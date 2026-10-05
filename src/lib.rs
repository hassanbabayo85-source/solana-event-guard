//! solana-event-guard: reliable Solana event delivery.
//!
//! RPC/WebSocket -> Source -> Gap Detector -> Dedupe -> Handler -> (Webhook + Delivery Log)
//!                                  ^              ^
//!                           Checkpoint Store (Postgres)
//!
//! Guarantee: at-least-once, deduplicated, replayable. Not exactly-once.

pub mod error;
pub mod event;
pub mod handler;
pub mod source;
pub mod store;

pub use error::{GuardError, Result};
pub use event::{Checkpoint, Event};
pub use handler::{Handler, Sink};
pub use source::Source;
pub use store::CheckpointStore;
#[cfg(feature = "postgres")]
pub use store::PgStore;

pub mod ws_source;
pub use ws_source::WsSource;
