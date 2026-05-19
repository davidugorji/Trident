use serde::{Deserialize, Serialize};

/// Distinguishes the three event categories emitted by the Soroban runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    /// Emitted explicitly by contract code via `env.events().publish(...)`.
    Contract,
    /// Emitted by the Soroban host itself (e.g. fee events).
    System,
    /// Emitted only when diagnostic mode is enabled; never stored by default.
    Diagnostic,
}

/// Normalised representation of a single Soroban event as stored in PostgreSQL
/// and published onto Redis Streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SorobanEvent {
    /// Strkey-encoded contract address (C...).
    pub contract_id: String,
    /// Ordered list of topic values, XDR-decoded to their string representations.
    pub topics: Vec<String>,
    /// Decoded event body. Scalar XDR types are coerced to JSON primitives;
    /// map/vec types become JSON objects/arrays.
    pub data: serde_json::Value,
    /// Ledger sequence number in which this event was emitted.
    pub ledger_sequence: u64,
    /// Hash of the transaction that emitted this event.
    pub transaction_hash: String,
    /// Zero-based index of this event within its transaction.
    pub event_index: u32,
    /// Category of event as reported by the Soroban host.
    pub event_type: EventType,
}
