use async_trait::async_trait;

use crate::error::Result;
use crate::event::{Checkpoint, Event};

/// Persistence for checkpoints, dedupe and "handled" tracking.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, stream_id: &str) -> Result<Option<Checkpoint>>;

    async fn save(&self, stream_id: &str, checkpoint: &Checkpoint) -> Result<()>;

    /// Record an event. Returns `true` if it is new, `false` if it was already seen.
    async fn record_event(&self, stream_id: &str, event: &Event) -> Result<bool>;

    /// Has the handler finished for this event? (false if unknown)
    async fn is_handled(&self, stream_id: &str, event: &Event) -> Result<bool>;

    /// Mark the event as fully handled.
    async fn mark_handled(&self, stream_id: &str, event: &Event) -> Result<()>;
}

#[cfg(feature = "postgres")]
pub use pg::PgStore;

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use sqlx::PgPool;

    pub struct PgStore {
        pool: PgPool,
    }

    impl PgStore {
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }

        /// Apply the bundled migrations.
        pub async fn migrate(&self) -> Result<()> {
            sqlx::migrate!("./migrations")
                .run(&self.pool)
                .await
                .map_err(|e| crate::error::GuardError::Store(e.to_string()))
        }
    }

    #[async_trait]
    impl CheckpointStore for PgStore {
        async fn load(&self, stream_id: &str) -> Result<Option<Checkpoint>> {
            let row: Option<(String, i64)> = sqlx::query_as(
                "SELECT last_signature, last_slot FROM checkpoints WHERE stream_id = $1",
            )
            .bind(stream_id)
            .fetch_optional(&self.pool)
            .await?;

            Ok(row.map(|(sig, slot)| Checkpoint {
                last_signature: sig,
                last_slot: slot as u64,
            }))
        }

        async fn save(&self, stream_id: &str, cp: &Checkpoint) -> Result<()> {
            sqlx::query(
                "INSERT INTO checkpoints (stream_id, last_signature, last_slot)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (stream_id) DO UPDATE
                 SET last_signature = EXCLUDED.last_signature,
                     last_slot = EXCLUDED.last_slot,
                     updated_at = now()",
            )
            .bind(stream_id)
            .bind(&cp.last_signature)
            .bind(cp.last_slot as i64)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn record_event(&self, stream_id: &str, e: &Event) -> Result<bool> {
            let res = sqlx::query(
                "INSERT INTO events (stream_id, signature, ix_index, slot, payload)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (stream_id, signature, ix_index) DO NOTHING",
            )
            .bind(stream_id)
            .bind(&e.signature)
            .bind(e.ix_index as i32)
            .bind(e.slot as i64)
            .bind(&e.payload)
            .execute(&self.pool)
            .await?;
            Ok(res.rows_affected() == 1)
        }

        async fn is_handled(&self, stream_id: &str, e: &Event) -> Result<bool> {
            let row: Option<(bool,)> = sqlx::query_as(
                "SELECT handled_at IS NOT NULL FROM events
                 WHERE stream_id = $1 AND signature = $2 AND ix_index = $3",
            )
            .bind(stream_id)
            .bind(&e.signature)
            .bind(e.ix_index as i32)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row.map(|(h,)| h).unwrap_or(false))
        }

        async fn mark_handled(&self, stream_id: &str, e: &Event) -> Result<()> {
            sqlx::query(
                "UPDATE events SET handled_at = now()
                 WHERE stream_id = $1 AND signature = $2 AND ix_index = $3",
            )
            .bind(stream_id)
            .bind(&e.signature)
            .bind(e.ix_index as i32)
            .execute(&self.pool)
            .await?;
            Ok(())
        }
    }
}
