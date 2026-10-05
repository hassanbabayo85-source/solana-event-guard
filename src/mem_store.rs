use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::Result;
use crate::event::{Checkpoint, Event};
use crate::store::CheckpointStore;

type EventKey = (String, String, u32);

#[derive(Default)]
struct Inner {
    checkpoints: HashMap<String, Checkpoint>,
    /// value = handled?
    events: HashMap<EventKey, bool>,
}

/// In-memory store for tests and examples. Clones share the same data
/// (handy for simulating a restart with the same "database").
#[derive(Clone, Default)]
pub struct MemStore {
    inner: Arc<Mutex<Inner>>,
}

fn key(stream_id: &str, e: &Event) -> EventKey {
    (stream_id.to_string(), e.signature.clone(), e.ix_index)
}

#[async_trait]
impl CheckpointStore for MemStore {
    async fn load(&self, stream_id: &str) -> Result<Option<Checkpoint>> {
        Ok(self.inner.lock().unwrap().checkpoints.get(stream_id).cloned())
    }

    async fn save(&self, stream_id: &str, cp: &Checkpoint) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .checkpoints
            .insert(stream_id.to_string(), cp.clone());
        Ok(())
    }

    async fn record_event(&self, stream_id: &str, e: &Event) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        match inner.events.entry(key(stream_id, e)) {
            Entry::Vacant(v) => {
                v.insert(false);
                Ok(true)
            }
            Entry::Occupied(_) => Ok(false),
        }
    }

    async fn is_handled(&self, stream_id: &str, e: &Event) -> Result<bool> {
        let inner = self.inner.lock().unwrap();
        Ok(*inner.events.get(&key(stream_id, e)).unwrap_or(&false))
    }

    async fn mark_handled(&self, stream_id: &str, e: &Event) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(h) = inner.events.get_mut(&key(stream_id, e)) {
            *h = true;
        }
        Ok(())
    }
}
