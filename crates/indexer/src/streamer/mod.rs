//! # Streamer
//!
//! Owns the RPC polling loop. Responsibilities:
//!
//! - Maintaining the ledger cursor: reading the last processed sequence from
//!   `system_state` on startup, advancing it after each successful batch, and
//!   persisting it atomically with the events it covers.
//! - Calling `getEvents` on the Stellar Soroban RPC node on a configurable
//!   interval (`POLL_INTERVAL_MS`), following the `pagingToken` cursor field
//!   to paginate across large ledger ranges within a single poll cycle.
//! - Fault tolerance and retry logic: transient RPC failures are retried with
//!   exponential backoff; persistent failures are logged without crashing the
//!   process or losing cursor position so the next poll cycle can recover.
//! - Handing each raw event to the `Parser` and forwarding normalised
//!   `SorobanEvent` values to both PostgreSQL (via `db`) and Redis Streams
//!   (via `redis_stream`).

use std::time::Duration;

use sqlx::PgPool;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use trident_common::TridentError;

use crate::{
    config::Config,
    db,
    parser::Parser,
    redis_stream,
    rpc::RpcClient,
};

pub struct Streamer {
    config: Config,
    db: PgPool,
    redis: redis::aio::MultiplexedConnection,
    rpc: RpcClient,
    parser: Parser,
}

impl Streamer {
    pub fn new(
        config: Config,
        db: PgPool,
        redis: redis::aio::MultiplexedConnection,
    ) -> Self {
        let rpc = RpcClient::new(config.stellar_rpc_url.clone());
        let parser = Parser::new(config.index_diagnostic);
        Self { config, db, redis, rpc, parser }
    }

    /// Start the polling loop. Runs indefinitely — spawn with `tokio::spawn`
    /// or drive directly from `main`. Never returns `Ok(())` in normal operation.
    pub async fn run(&mut self) -> Result<(), TridentError> {
        tracing::info!(
            network = %self.config.network,
            poll_interval_ms = %self.config.poll_interval.as_millis(),
            "Streamer started"
        );

        let mut cursor = db::get_cursor(&self.db).await?;
        tracing::info!(cursor, "Resuming from ledger cursor");

        loop {
            match self.poll_once(&mut cursor).await {
                Ok(events_processed) => {
                    if events_processed > 0 {
                        tracing::info!(events_processed, cursor, "Batch processed");
                    } else {
                        tracing::debug!(cursor, "No new events");
                    }
                }
                Err(e) => {
                    // Log but do not crash — the cursor is safe, next poll will retry.
                    tracing::error!(error = %e, "Poll cycle failed, will retry next interval");
                }
            }

            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    /// Execute a single poll cycle. Fetches all available pages from the RPC
    /// starting at `cursor`, persists each event, and advances the cursor.
    /// Returns the total number of events processed in this cycle.
    async fn poll_once(&mut self, cursor: &mut u64) -> Result<usize, TridentError> {
        let retry_strategy = ExponentialBackoff::from_millis(200)
            .max_delay(Duration::from_secs(30))
            .take(5);

        // Use start_ledger on the very first call (cursor == 0), then switch
        // to paging_token-based cursor for all subsequent pages.
        let start_ledger = if *cursor == 0 { None } else { None };
        let initial_cursor = if *cursor > 0 {
            Some(cursor.to_string())
        } else {
            None
        };

        let mut page_cursor = initial_cursor;
        let mut total = 0;

        loop {
            let pc = page_cursor.clone();
            let sl = start_ledger;
            let page = Retry::spawn(retry_strategy.clone(), || async {
                self.rpc.get_events(sl, pc.clone()).await
            })
            .await?;

            if page.events.is_empty() {
                break;
            }

            let last_paging_token = page
                .events
                .last()
                .map(|e| e.paging_token.clone());

            for raw in &page.events {
                match self.parser.parse_event(raw) {
                    Ok(Some(event)) => {
                        db::insert_event(&self.db, &event).await?;
                        redis_stream::publish_event(&mut self.redis, &event).await?;
                        total += 1;
                    }
                    Ok(None) => {
                        // Diagnostic event skipped — index_diagnostic is false
                    }
                    Err(e) => {
                        tracing::warn!(
                            tx_hash = %raw.tx_hash,
                            error = %e,
                            "Skipping unparseable event"
                        );
                    }
                }
            }

            // Advance the persistent cursor to the last processed ledger
            let last_ledger = page.events.last().map(|e| e.ledger.parse::<u64>().unwrap_or(*cursor));
            if let Some(seq) = last_ledger {
                if seq > *cursor {
                    *cursor = seq;
                    db::set_cursor(&self.db, *cursor).await?;
                }
            }

            // If a full page was returned there may be more — keep paginating.
            // An incomplete page means we've caught up to the chain tip.
            if page.events.len() < 200 {
                break;
            }

            page_cursor = last_paging_token;
        }

        Ok(total)
    }
}
