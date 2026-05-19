//! # Parser
//!
//! Owns XDR decoding and event normalisation. Responsibilities:
//!
//! - Decoding raw base64-encoded XDR strings as returned by the Soroban RPC
//!   `getEvents` method into typed Rust values using the `stellar-xdr` crate.
//! - Normalising decoded XDR values into the canonical `SorobanEvent` shape:
//!   converting `ScVal` topics to their string representations, coercing the
//!   event body to `serde_json::Value`, and extracting ledger/tx metadata.
//! - Type coercion rules: ScVal::Symbol → plain string, ScVal::Address → strkey,
//!   ScVal::I128/U128 → JSON number (with overflow to string for values outside
//!   i64 range), ScVal::Map/Vec → JSON object/array recursively.
//! - Returning a typed `TridentError::ParseError` for any input that cannot be
//!   decoded, so the caller can decide whether to skip, retry, or halt.

use trident_common::{SorobanEvent, TridentError};

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Self
    }

    /// Decode a raw XDR event string (as returned by `getEvents` RPC) into a
    /// normalised `SorobanEvent`. Returns `TridentError::ParseError` if the
    /// input cannot be decoded or required fields are missing.
    pub fn parse_event(&self, raw_xdr: &str) -> Result<SorobanEvent, TridentError> {
        let _ = raw_xdr; // suppress unused warning until implemented
        // TODO: base64-decode raw_xdr
        // TODO: XDR-decode using stellar-xdr ContractEvent
        // TODO: normalise topics via ScVal → string conversion
        // TODO: normalise data body via ScVal → serde_json::Value
        // TODO: construct and return SorobanEvent
        Err(TridentError::ParseError("not yet implemented".into()))
    }
}
