use tracing_subscriber::EnvFilter;

mod config;
mod db;
mod parser;
mod redis_stream;
mod rpc;
mod streamer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    tracing::info!("Trident indexer starting");

    let cfg = config::Config::from_env()?;

    let db_pool = sqlx::PgPool::connect(&cfg.database_url).await?;
    tracing::info!("Database connected");

    let redis_client = redis::Client::open(cfg.redis_url.as_str())?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;
    tracing::info!("Redis connected");

    let mut s = streamer::Streamer::new(cfg, db_pool, redis_conn);
    s.run().await?;

    Ok(())
}
