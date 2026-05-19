use tracing_subscriber::EnvFilter;

mod parser;
mod streamer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    tracing::info!("Trident indexer starting");

    // TODO: load config from environment
    // TODO: initialise database pool (sqlx::PgPool)
    // TODO: initialise Redis connection
    // TODO: construct Streamer and Parser, wire them together
    // TODO: call streamer.run().await

    Ok(())
}
