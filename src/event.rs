use serde::{Deserialize, Serialize};

/// A single Solana event.
///
/// Dedupe key = (stream_id, signature, ix_index). We never use the slot
/// number to detect gaps: some slots have no block (skipped leader), so a
/// "missing" slot is not necessarily a missed event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub signature: String,
    /// Instruction index inside the transaction (0 if the whole tx is one event).
    pub ix_index: u32,
    pub slot: u64,
    pub payload: serde_json::Value,
}

/// Last position we fully processed for a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub last_signature: String,
    pub last_slot: u64,
}
