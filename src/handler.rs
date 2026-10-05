use async_trait::async_trait;

use crate::error::Result;
use crate::event::Event;

/// Your logic. Called once per new (non-duplicate) event.
/// Because delivery is at-least-once, make your handler idempotent.
#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, event: &Event) -> Result<()>;
}

/// Where events are forwarded (webhook with retry and backoff comes in week 3).
#[async_trait]
pub trait Sink: Send + Sync {
    async fn deliver(&self, event: &Event) -> Result<()>;
}
