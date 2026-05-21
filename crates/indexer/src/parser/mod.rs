//! # Parser
//!
//! Owns XDR decoding and event normalisation. Responsibilities:
//!
//! - Decoding raw base64-encoded XDR strings as returned by the Soroban RPC
//!   `getEvents` method into typed Rust values via the `stellar-xdr` crate.
//! - Normalising decoded `ScVal` topics into human-readable string representations
//!   and the event body into a `serde_json::Value` for storage and forwarding.
//! - Type coercion: Symbol/String → plain string, Address → strkey, I128/U128 →
//!   decimal string, Bool → "true"/"false", Bytes → hex, Map/Vec → JSON object/array.
//! - Returning `TridentError::ParseError` for any input that cannot be decoded so
//!   the caller (Streamer) can decide whether to skip or halt.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value as Json;
use stellar_strkey::{ed25519, Contract};
use stellar_xdr::curr::{
    AccountId, ContractId, Limited, Limits, PublicKey, ReadXdr, ScAddress, ScVal,
};
use trident_common::{EventType, SorobanEvent, TridentError};

use crate::rpc::RawEvent;

pub struct Parser {
    pub index_diagnostic: bool,
}

impl Parser {
    pub fn new(index_diagnostic: bool) -> Self {
        Self { index_diagnostic }
    }

    /// Decode a raw RPC event into a normalised `SorobanEvent`.
    ///
    /// Returns `None` if the event type is `diagnostic` and `index_diagnostic`
    /// is false — the caller should silently skip `None` returns.
    pub fn parse_event(&self, raw: &RawEvent) -> Result<Option<SorobanEvent>, TridentError> {
        let event_type = parse_event_type(&raw.event_type)?;

        if event_type == EventType::Diagnostic && !self.index_diagnostic {
            return Ok(None);
        }

        // Skip events emitted by failed contract calls — they have no observable effect.
        if !raw.in_successful_contract_call {
            return Ok(None);
        }

        let contract_id = raw.contract_id.clone().unwrap_or_default();

        let topics: Vec<String> = raw
            .topic
            .iter()
            .map(|xdr| decode_scval(xdr).map(|v| scval_to_string(&v)))
            .collect::<Result<_, _>>()?;

        let data = if raw.value.is_empty() {
            Json::Null
        } else {
            decode_scval(&raw.value).map(|v| scval_to_json(&v))?
        };

        let ledger_sequence: u64 = raw
            .ledger
            .parse()
            .map_err(|_| TridentError::ParseError(format!("invalid ledger: {}", raw.ledger)))?;

        // event_index is the second component of the opaque id string: "{encoded}-{index}"
        let event_index: u32 = raw
            .id
            .split('-')
            .next_back()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Ok(Some(SorobanEvent {
            contract_id,
            topics,
            data,
            ledger_sequence,
            ledger_timestamp: raw.ledger_closed_at.clone(),
            transaction_hash: raw.tx_hash.clone(),
            event_index,
            event_type,
        }))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn parse_event_type(raw: &str) -> Result<EventType, TridentError> {
    match raw {
        "contract" => Ok(EventType::Contract),
        "system" => Ok(EventType::System),
        "diagnostic" => Ok(EventType::Diagnostic),
        other => Err(TridentError::ParseError(format!(
            "unknown event type: {other}"
        ))),
    }
}

fn decode_scval(b64: &str) -> Result<ScVal, TridentError> {
    let bytes = STANDARD
        .decode(b64)
        .map_err(|e| TridentError::ParseError(format!("base64 decode: {e}")))?;
    let mut cursor = std::io::Cursor::new(bytes);
    ScVal::read_xdr(&mut Limited::new(&mut cursor, Limits::none()))
        .map_err(|e| TridentError::ParseError(format!("XDR decode ScVal: {e}")))
}

/// Convert a topic `ScVal` to a compact string representation.
pub fn scval_to_string(val: &ScVal) -> String {
    match val {
        ScVal::Symbol(s) => s.to_utf8_string_lossy(),
        ScVal::String(s) => s.to_utf8_string_lossy(),
        ScVal::Bool(b) => b.to_string(),
        ScVal::Void => "void".into(),
        ScVal::U32(n) => n.to_string(),
        ScVal::I32(n) => n.to_string(),
        ScVal::U64(n) => n.to_string(),
        ScVal::I64(n) => n.to_string(),
        ScVal::U128(parts) => {
            let val = ((parts.hi as u128) << 64) | (parts.lo as u128);
            val.to_string()
        }
        ScVal::I128(parts) => {
            let val = ((parts.hi as i128) << 64) | (parts.lo as i128);
            val.to_string()
        }
        ScVal::U256(parts) => format!(
            "u256({:x}{:x}{:x}{:x})",
            parts.hi_hi, parts.hi_lo, parts.lo_hi, parts.lo_lo
        ),
        ScVal::I256(parts) => format!(
            "i256({:x}{:x}{:x}{:x})",
            parts.hi_hi, parts.hi_lo, parts.lo_hi, parts.lo_lo
        ),
        ScVal::Bytes(b) => hex::encode(b.as_slice()),
        ScVal::Address(addr) => scaddress_to_string(addr),
        // For complex types in topic position, fall back to debug representation
        other => format!("{other:?}"),
    }
}

/// Recursively convert a `ScVal` to a `serde_json::Value` for the event body.
pub fn scval_to_json(val: &ScVal) -> Json {
    match val {
        ScVal::Void => Json::Null,
        ScVal::Bool(b) => Json::Bool(*b),
        ScVal::Symbol(s) => Json::String(s.to_utf8_string_lossy()),
        ScVal::String(s) => Json::String(s.to_utf8_string_lossy()),
        ScVal::U32(n) => Json::from(*n),
        ScVal::I32(n) => Json::from(*n),
        ScVal::U64(n) => Json::from(*n),
        ScVal::I64(n) => Json::from(*n),
        ScVal::U128(parts) => {
            let v = ((parts.hi as u128) << 64) | (parts.lo as u128);
            // Use string for values that overflow JSON's safe integer range
            if v <= u64::MAX as u128 {
                Json::from(v as u64)
            } else {
                Json::String(v.to_string())
            }
        }
        ScVal::I128(parts) => {
            let v = ((parts.hi as i128) << 64) | (parts.lo as i128);
            if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
                Json::from(v as i64)
            } else {
                Json::String(v.to_string())
            }
        }
        ScVal::Bytes(b) => Json::String(hex::encode(b.as_slice())),
        ScVal::Address(addr) => Json::String(scaddress_to_string(addr)),
        ScVal::Vec(Some(items)) => Json::Array(items.iter().map(scval_to_json).collect()),
        ScVal::Vec(None) => Json::Array(vec![]),
        ScVal::Map(Some(entries)) => {
            let obj: serde_json::Map<String, Json> = entries
                .iter()
                .map(|e| (scval_to_string(&e.key), scval_to_json(&e.val)))
                .collect();
            Json::Object(obj)
        }
        ScVal::Map(None) => Json::Object(serde_json::Map::new()),
        other => Json::String(format!("{other:?}")),
    }
}

fn scaddress_to_string(addr: &ScAddress) -> String {
    match addr {
        ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(bytes))) => {
            // stellar-strkey 0.0.16+ returns heapless::String — convert to std::String
            ed25519::PublicKey(bytes.0).to_string().as_str().to_owned()
        }
        // stellar-xdr 26.x wraps the hash in ContractId; the inner Hash holds [u8; 32]
        ScAddress::Contract(ContractId(hash)) => Contract(hash.0).to_string().as_str().to_owned(),
        // stellar-xdr 26.x added MuxedAccount, ClaimableBalance, LiquidityPool variants;
        // these do not appear in Soroban contract events but the match must be exhaustive.
        other => format!("{other:?}"),
    }
}
