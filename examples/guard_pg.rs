//! End-to-end example: WsSource -> Guard -> PgStore, with a printing handler.
//!
//! Env vars (all optional):
//!   DATABASE_URL   default: postgres://postgres:postgres@localhost:55432/guard
//!   SOLANA_WS_URL  default: wss://api.devnet.solana.com
//!   WATCH_ADDRESS  default: 11111111111111111111111111111111 (System Program)
//!   STREAM_ID      default: devnet-system
//!   MAX_EVENTS     default: 10  (exit after N handled events; 0 = run forever)
//!
//! Requires: `docker compose up -d --wait` for the Postgres instance.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

use solana_event_guard::event::Event;
use solana_event_guard::{Guard, GuardError, Handler, PgStore, Result, WsSource};

struct PrintingHandler {
    count: Arc<AtomicUsize>,
    stop_after: usize,
}

#[async_trait]
impl Handler for PrintingHandler {
    async fn handle(&self, event: &Event) -> Result<()> {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        let payload_bytes = serde_json::to_string(&event.payload)
            .map(|s| s.len())
            .unwrap_or(0);
        println!(
            "#{n} slot={} sig={} payload={}B",
            event.slot, event.signature, payload_bytes
        );
        if self.stop_after > 0 && n >= self.stop_after {
            println!("reached {n} handled events; exiting cleanly");
            std::process::exit(0);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:55432/guard".into());
    let ws_url = std::env::var("SOLANA_WS_URL")
        .unwrap_or_else(|_| "wss://api.devnet.solana.com".into());
    let address = std::env::var("WATCH_ADDRESS")
        .unwrap_or_else(|_| "11111111111111111111111111111111".into());
    let stream_id = std::env::var("STREAM_ID").unwrap_or_else(|_| "devnet-system".into());
    let stop_after: usize = std::env::var("MAX_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let pool = PgPool::connect(&db_url)
        .await
        .map_err(|e| GuardError::Store(format!("db connect: {e}")))?;
    let store = PgStore::new(pool);
    store.migrate().await?;

    let source = WsSource::new(&ws_url, &address, "confirmed");
    let handler = PrintingHandler {
        count: Arc::new(AtomicUsize::new(0)),
        stop_after,
    };

    let mut guard =
        Guard::new(&stream_id, source, store, handler).with_retry_delay(Duration::from_secs(2));

    println!("stream_id  = {stream_id}");
    println!("address    = {address}");
    println!("ws         = {ws_url}");
    println!("db         = {db_url}");
    if stop_after > 0 {
        println!("will exit after {stop_after} handled events (Ctrl+C to stop early)");
    } else {
        println!("running forever (Ctrl+C to stop)");
    }

    guard.run().await
}
