use async_trait::async_trait;

use crate::error::Result;
use crate::event::Event;

/// Where events come from. WebSocket first, Yellowstone gRPC later.
#[async_trait]
pub trait Source: Send {
    /// Wait for the next live event. `Ok(None)` means the connection dropped
    /// and the engine should call `reconnect` and then `backfill`.
    async fn next(&mut self) -> Result<Option<Event>>;

    /// (Re)establish the live subscription. The engine calls this BEFORE
    /// `backfill`, so no event can fall into a gap between the two.
    async fn reconnect(&mut self) -> Result<()>;

    /// Fetch events newer than `after_signature`, OLDEST FIRST.
    /// Implementations should re-read a small overlap window; the dedupe
    /// layer removes the repeats. Simpler and safer than finding the exact boundary.
    async fn backfill(&mut self, after_signature: Option<&str>) -> Result<Vec<Event>>;
}
