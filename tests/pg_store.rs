use serde_json::json;
use solana_event_guard::{Checkpoint, CheckpointStore, Event, PgStore};
use sqlx::PgPool;
use std::time::{SystemTime, UNIX_EPOCH};

async fn store() -> PgStore {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:55432/guard".into());
    let pool = PgPool::connect(&url)
        .await
        .expect("cannot connect to Postgres (is docker compose up?)");
    let store = PgStore::new(pool);
    store.migrate().await.expect("migration failed");
    store
}

// Every test uses its own stream_id so tests never collide.
fn unique_stream(name: &str) -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}-{n}")
}

fn event(sig: &str, ix: u32, slot: u64) -> Event {
    Event {
        signature: sig.to_string(),
        ix_index: ix,
        slot,
        payload: json!({ "test": true }),
    }
}

#[tokio::test]
async fn record_event_detects_duplicates() {
    let store = store().await;
    let stream = unique_stream("dedupe");
    let e = event("sig-A", 0, 100);

    assert!(store.record_event(&stream, &e).await.unwrap(), "first insert must be new");
    assert!(!store.record_event(&stream, &e).await.unwrap(), "second insert must be a duplicate");
}

#[tokio::test]
async fn different_ix_index_is_a_different_event() {
    let store = store().await;
    let stream = unique_stream("ix");

    assert!(store.record_event(&stream, &event("sig-B", 0, 100)).await.unwrap());
    assert!(store.record_event(&stream, &event("sig-B", 1, 100)).await.unwrap());
}

#[tokio::test]
async fn same_signature_in_different_streams_is_not_a_duplicate() {
    let store = store().await;
    let s1 = unique_stream("s1");
    let s2 = unique_stream("s2");
    let e = event("sig-C", 0, 100);

    assert!(store.record_event(&s1, &e).await.unwrap());
    assert!(store.record_event(&s2, &e).await.unwrap());
}

#[tokio::test]
async fn checkpoint_roundtrip_and_update() {
    let store = store().await;
    let stream = unique_stream("cp");

    assert!(store.load(&stream).await.unwrap().is_none());

    store
        .save(&stream, &Checkpoint { last_signature: "sig-1".into(), last_slot: 10 })
        .await
        .unwrap();
    let cp = store.load(&stream).await.unwrap().unwrap();
    assert_eq!(cp.last_signature, "sig-1");
    assert_eq!(cp.last_slot, 10);

    store
        .save(&stream, &Checkpoint { last_signature: "sig-2".into(), last_slot: 20 })
        .await
        .unwrap();
    let cp = store.load(&stream).await.unwrap().unwrap();
    assert_eq!(cp.last_signature, "sig-2");
    assert_eq!(cp.last_slot, 20);
}
