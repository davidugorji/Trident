use chrono::{DateTime, Utc};
use sqlx::PgPool;
use trident_common::{EventType, SorobanEvent, TridentError};
use uuid::Uuid;

// Stable namespace for deterministic event UUIDs (UUIDv5).
// Using the DNS namespace is arbitrary; what matters is that it is fixed.
const EVENT_NS: Uuid = Uuid::NAMESPACE_DNS;

/// Derive a deterministic UUID for an event from its natural key.
/// Using the same inputs will always produce the same UUID, so duplicate
/// events produce the same `id` and `ON CONFLICT (id) DO NOTHING` fires.
fn event_uuid(contract_id: &str, ledger_sequence: u64, event_index: u32) -> Uuid {
    let key = format!("{contract_id}:{ledger_sequence}:{event_index}");
    Uuid::new_v5(&EVENT_NS, key.as_bytes())
}

/// Insert a normalised event. Silently ignores duplicates via `ON CONFLICT (id) DO NOTHING`.
/// The `id` is a deterministic UUIDv5 derived from `(contract_id, ledger_sequence, event_index)`,
/// so replaying the same event always produces the same primary key.
pub async fn insert_event(pool: &PgPool, event: &SorobanEvent) -> Result<(), TridentError> {
    let id = event_uuid(&event.contract_id, event.ledger_sequence, event.event_index);
    let event_type = match event.event_type {
        EventType::Contract => "contract",
        EventType::System => "system",
        EventType::Diagnostic => "diagnostic",
    };
    let topics = serde_json::to_value(&event.topics)
        .map_err(|e| TridentError::StorageError(format!("topics serialise: {e}")))?;
    let ledger_ts: DateTime<Utc> = event
        .ledger_timestamp
        .parse()
        .map_err(|e| TridentError::StorageError(format!("ledger timestamp parse: {e}")))?;

    sqlx::query(
        r#"
        INSERT INTO soroban_events
            (id, contract_id, ledger_sequence, ledger_timestamp, transaction_hash,
             event_index, event_type, topics, data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(id)
    .bind(&event.contract_id)
    .bind(event.ledger_sequence as i64)
    .bind(ledger_ts)
    .bind(&event.transaction_hash)
    .bind(event.event_index as i32)
    .bind(event_type)
    .bind(&topics)
    .bind(&event.data)
    .execute(pool)
    .await
    .map_err(|e| TridentError::StorageError(format!("insert_event: {e}")))?;

    Ok(())
}

/// Read the latest processed ledger cursor from system_state.
pub async fn get_cursor(pool: &PgPool) -> Result<u64, TridentError> {
    let row: (String,) =
        sqlx::query_as("SELECT value FROM system_state WHERE key = 'latest_ledger_cursor'")
            .fetch_one(pool)
            .await
            .map_err(|e| TridentError::StorageError(format!("get_cursor: {e}")))?;

    row.0
        .parse::<u64>()
        .map_err(|e| TridentError::StorageError(format!("cursor parse: {e}")))
}

/// Persist the latest processed ledger sequence so the streamer can resume
/// from the correct position after a restart.
pub async fn set_cursor(pool: &PgPool, ledger: u64) -> Result<(), TridentError> {
    sqlx::query(
        "UPDATE system_state SET value = $1, updated_at = NOW() WHERE key = 'latest_ledger_cursor'",
    )
    .bind(ledger.to_string())
    .execute(pool)
    .await
    .map_err(|e| TridentError::StorageError(format!("set_cursor: {e}")))?;

    Ok(())
}

/// Record a processed ledger in ledger_metadata for gap detection.
pub async fn insert_ledger_metadata(
    pool: &PgPool,
    ledger_sequence: u64,
    ledger_hash: &str,
    ledger_timestamp: &str,
    event_count: i32,
) -> Result<(), TridentError> {
    let ts: DateTime<Utc> = ledger_timestamp
        .parse()
        .map_err(|e| TridentError::StorageError(format!("ledger timestamp parse: {e}")))?;

    sqlx::query(
        r#"
        INSERT INTO ledger_metadata (ledger_sequence, ledger_hash, ledger_timestamp, event_count)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (ledger_sequence) DO NOTHING
        "#,
    )
    .bind(ledger_sequence as i64)
    .bind(ledger_hash)
    .bind(ts)
    .bind(event_count)
    .execute(pool)
    .await
    .map_err(|e| TridentError::StorageError(format!("insert_ledger_metadata: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::PgPool;
    use trident_common::{EventType, SorobanEvent};

    fn make_event(contract_id: &str, ledger_sequence: u64, event_index: u32) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            ledger_sequence,
            ledger_timestamp: "2024-01-01T00:00:00Z".to_string(),
            transaction_hash: "txhash_abc123".to_string(),
            event_index,
            event_type: EventType::Contract,
            topics: vec![],
            data: json!({}),
        }
    }

    /// Deterministic UUID: same inputs must produce the same id.
    #[test]
    fn event_uuid_is_deterministic() {
        let a = event_uuid("CABC", 100, 0);
        let b = event_uuid("CABC", 100, 0);
        assert_eq!(a, b);
    }

    /// Different natural keys must produce different UUIDs.
    #[test]
    fn event_uuid_varies_with_inputs() {
        let a = event_uuid("CABC", 100, 0);
        let b = event_uuid("CABC", 100, 1);
        assert_ne!(a, b);
    }

    /// Calling `insert_event` twice with the same event must not error and
    /// the row count in `soroban_events` must remain 1.
    #[sqlx::test(migrations = "../../../database/migrations")]
    async fn insert_event_is_idempotent(pool: PgPool) {
        let event = make_event("CABC_CONTRACT_001", 42, 0);

        insert_event(&pool, &event).await.expect("first insert failed");
        insert_event(&pool, &event).await.expect("second insert must not error");

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM soroban_events")
            .fetch_one(&pool)
            .await
            .expect("count query failed");

        assert_eq!(count.0, 1, "duplicate insert should be silently ignored");
    }
}
