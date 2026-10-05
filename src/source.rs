use async_trait::async_trait;

use crate::error::Result;
use crate::event::Event;

/// Where events come from. WebSocket first, Yellowstone gRPC later.
#[async_trait]
pub trait Source: Send {
    /// Wait for the next live event. `Ok(None)` means the connection dropped
    /// and the engine should reconnect and backfill.
    async fn next(&mut self) -> Result<Option<Event>>;

    /// Fetch events newer than `after_signature` (e.g. via getSignaturesForAddress).
    /// Called after a reconnect. Implementations should re-read a small overlap
    /// window; the dedupe layer removes the repeats. This is simpler and safer
    /// than trying to find the exact boundary.
    async fn backfill(&mut self, after_signature: Option<&str>) -> Result<Vec<Event>>;
}
