//! # Streamer
//!
//! Owns the RPC polling loop. Responsibilities:
//!
//! - Maintaining the ledger cursor: reading the last processed sequence from
//!   `system_state`, advancing it after each successful batch, and persisting
//!   it atomically with the events it covers.
//! - Calling `getEvents` on the Stellar Soroban RPC node on a configurable
//!   interval (`POLL_INTERVAL_MS`), handling pagination across large ledger
//!   ranges by following the `cursor` field in each response.
//! - Fault tolerance and retry logic: transient RPC failures should be retried
//!   with exponential backoff; persistent failures must be logged and surfaced
//!   without crashing the process or losing cursor position.
//! - Handing each raw event to the `Parser` and forwarding normalised
//!   `SorobanEvent` values to both PostgreSQL and Redis Streams.

use trident_common::TridentError;

pub struct Streamer {
    // TODO: rpc_url: String
    // TODO: db: sqlx::PgPool
    // TODO: redis: redis::aio::ConnectionManager
    // TODO: poll_interval: std::time::Duration
}

impl Streamer {
    /// Construct a new Streamer. All dependencies are injected so they can be
    /// shared with other components or replaced in tests.
    pub fn new() -> Self {
        Self {}
    }

    /// Start the polling loop. This future runs indefinitely — it should be
    /// spawned with `tokio::spawn` or driven directly from `main`.
    pub async fn run(&self) -> Result<(), TridentError> {
        tracing::info!("Streamer started");
        // TODO: read cursor from system_state
        // TODO: loop: call getEvents RPC, parse, write to DB + Redis, advance cursor
        Ok(())
    }
}
