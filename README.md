# solana-event-guard

Reliable Solana event delivery in Rust: reconnect, backfill, dedupe, checkpoint and replay.

**Status:** skeleton (week 1). Not usable yet.

## Guarantee

At-least-once and deduplicated. Not exactly-once. Make your handler idempotent.

## Roadmap

- Week 1: crate skeleton, `Source` (WebSocket), `CheckpointStore` (Postgres)
- Week 2: reconnect, backfill with overlap window, dedupe, chaos test
- Week 3: webhook sink, retry, delivery log, `replay`
- Week 4: docs, docker-compose example, CI, `cargo publish`
