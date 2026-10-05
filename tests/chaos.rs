use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use solana_event_guard::{
    CheckpointStore, Event, Guard, GuardError, Handler, MemStore, Result, Source,
};

fn make_chain(n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| Event {
            signature: format!("s{i}"),
            ix_index: 0,
            slot: i as u64,
            payload: json!({}),
        })
        .collect()
}

/// A fake chain + connection that we can break on purpose.
struct FakeSource {
    chain: Vec<Event>,
    pos: usize,        // next chain index delivered live
    drops: Vec<usize>, // positions where the connection drops
    outage: usize,     // events missed live during each outage
    overlap: usize,    // backfill re-sends this many already-seen events
    connected: bool,
}

impl FakeSource {
    fn new(chain: Vec<Event>, drops: Vec<usize>, outage: usize, overlap: usize) -> Self {
        Self { chain, pos: 0, drops, outage, overlap, connected: false }
    }
}

#[async_trait]
impl Source for FakeSource {
    async fn next(&mut self) -> Result<Option<Event>> {
        if self.pos >= self.chain.len() {
            return Err(GuardError::Source("eof".into()));
        }
        if !self.connected {
            return Err(GuardError::Source("not connected".into()));
        }
        if let Some(i) = self.drops.iter().position(|d| *d == self.pos) {
            self.drops.remove(i);
            self.connected = false;
            // Events during the outage exist on chain but are never delivered live.
            self.pos = (self.pos + self.outage).min(self.chain.len());
            return Ok(None);
        }
        let e = self.chain[self.pos].clone();
        self.pos += 1;
        Ok(Some(e))
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.connected = true;
        Ok(())
    }

    async fn backfill(&mut self, after: Option<&str>) -> Result<Vec<Event>> {
        let start = match after {
            Some(sig) => self
                .chain
                .iter()
                .position(|e| e.signature == sig)
                .map(|i| i + 1)
                .unwrap_or(0),
            None => 0,
        };
        let start = start.saturating_sub(self.overlap);
        Ok(self.chain[start..self.pos].to_vec())
    }
}

#[derive(Clone, Default)]
struct RecordingHandler {
    handled: Arc<Mutex<Vec<String>>>,
    fail_once: Arc<Mutex<Option<String>>>,
}

impl RecordingHandler {
    fn fail_once_on(&self, sig: &str) {
        *self.fail_once.lock().unwrap() = Some(sig.to_string());
    }
    fn handled(&self) -> Vec<String> {
        self.handled.lock().unwrap().clone()
    }
}

#[async_trait]
impl Handler for RecordingHandler {
    async fn handle(&self, event: &Event) -> Result<()> {
        {
            let mut f = self.fail_once.lock().unwrap();
            if f.as_deref() == Some(event.signature.as_str()) {
                *f = None;
                return Err(GuardError::Handler("injected failure".into()));
            }
        }
        self.handled.lock().unwrap().push(event.signature.clone());
        Ok(())
    }
}

async fn drive<S: Source, C: CheckpointStore, H: Handler>(g: &mut Guard<S, C, H>) -> GuardError {
    for _ in 0..1000 {
        if let Err(e) = g.step().await {
            return e;
        }
    }
    panic!("guard did not stop");
}

fn all_sigs(chain: &[Event]) -> Vec<String> {
    chain.iter().map(|e| e.signature.clone()).collect()
}

#[tokio::test]
async fn survives_disconnects_without_losing_or_repeating() {
    let chain = make_chain(20);
    let handler = RecordingHandler::default();
    // Connection drops at event 5 and 12; 3 events are missed live each time.
    let source = FakeSource::new(chain.clone(), vec![5, 12], 3, 2);
    let mut guard = Guard::new("chaos", source, MemStore::default(), handler.clone());

    guard.start().await.unwrap();
    let end = drive(&mut guard).await;
    assert!(matches!(end, GuardError::Source(ref m) if m == "eof"), "got {end:?}");

    // Every event handled exactly once, in order, despite gaps and overlap.
    assert_eq!(handler.handled(), all_sigs(&chain));
}

#[tokio::test]
async fn handler_failure_then_restart_does_not_lose_the_event() {
    let chain = make_chain(10);
    let store = MemStore::default();
    let handler = RecordingHandler::default();
    handler.fail_once_on("s4");

    // First run: the handler fails on s4 (simulates a crash mid-handling).
    let src1 = FakeSource::new(chain.clone(), vec![], 0, 2);
    let mut g1 = Guard::new("restart", src1, store.clone(), handler.clone());
    g1.start().await.unwrap();
    let end = drive(&mut g1).await;
    assert!(matches!(end, GuardError::Handler(_)), "got {end:?}");

    // Restart with the same store while the chain moved on to position 7.
    let mut src2 = FakeSource::new(chain.clone(), vec![], 0, 2);
    src2.pos = 7;
    let mut g2 = Guard::new("restart", src2, store.clone(), handler.clone());
    g2.start().await.unwrap();
    let end = drive(&mut g2).await;
    assert!(matches!(end, GuardError::Source(ref m) if m == "eof"), "got {end:?}");

    assert_eq!(handler.handled(), all_sigs(&chain));
}
