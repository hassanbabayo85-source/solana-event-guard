use std::time::Duration;

use crate::error::{GuardError, Result};
use crate::event::{Checkpoint, Event};
use crate::handler::Handler;
use crate::source::Source;
use crate::store::CheckpointStore;

/// The engine: Source -> dedupe -> Handler -> checkpoint, with reconnect + backfill.
///
/// Order per event (this is what makes it at-least-once):
///   1. record_event   (row exists, handled = false)
///   2. skip if already handled
///   3. handler.handle
///   4. mark_handled
///   5. advance checkpoint
/// A crash anywhere leaves the event "not handled", so it is retried later.
pub struct Guard<S, C, H> {
    stream_id: String,
    source: S,
    store: C,
    handler: H,
    checkpoint: Option<Checkpoint>,
    retry_delay: Duration,
}

impl<S: Source, C: CheckpointStore, H: Handler> Guard<S, C, H> {
    pub fn new(stream_id: impl Into<String>, source: S, store: C, handler: H) -> Self {
        Self {
            stream_id: stream_id.into(),
            source,
            store,
            handler,
            checkpoint: None,
            retry_delay: Duration::from_secs(2),
        }
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Process one event. Returns `true` if the handler ran, `false` if it was a duplicate.
    pub async fn process(&mut self, event: Event) -> Result<bool> {
        self.store.record_event(&self.stream_id, &event).await?;
        if self.store.is_handled(&self.stream_id, &event).await? {
            return Ok(false);
        }

        self.handler.handle(&event).await?;
        self.store.mark_handled(&self.stream_id, &event).await?;

        // Never move the checkpoint backwards (backfill re-reads older events).
        let advance = match &self.checkpoint {
            Some(cp) => event.slot >= cp.last_slot,
            None => true,
        };
        if advance {
            let cp = Checkpoint {
                last_signature: event.signature.clone(),
                last_slot: event.slot,
            };
            self.store.save(&self.stream_id, &cp).await?;
            self.checkpoint = Some(cp);
        }
        Ok(true)
    }

    /// Subscribe first, then backfill from the checkpoint (if there is one).
    async fn recover(&mut self) -> Result<()> {
        self.source.reconnect().await?;
        if let Some(cp) = self.checkpoint.clone() {
            let events = self.source.backfill(Some(cp.last_signature.as_str())).await?;
            for event in events {
                self.process(event).await?;
            }
        }
        Ok(())
    }

    /// Load the checkpoint, connect and catch up.
    pub async fn start(&mut self) -> Result<()> {
        self.checkpoint = self.store.load(&self.stream_id).await?;
        self.recover().await
    }

    /// Handle one live event, or recover if the connection dropped.
    pub async fn step(&mut self) -> Result<()> {
        match self.source.next().await? {
            Some(event) => {
                self.process(event).await?;
                Ok(())
            }
            None => self.recover().await,
        }
    }

    /// Run forever. Source errors are retried after `retry_delay`;
    /// store and handler errors stop the engine.
    pub async fn run(&mut self) -> Result<()> {
        self.checkpoint = self.store.load(&self.stream_id).await?;
        let mut need_recover = true;
        loop {
            let res = if need_recover {
                self.recover().await
            } else {
                self.step().await
            };
            match res {
                Ok(()) => need_recover = false,
                Err(GuardError::Source(msg)) => {
                    tracing::warn!("source error: {msg}; will reconnect");
                    need_recover = true;
                    tokio::time::sleep(self.retry_delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
