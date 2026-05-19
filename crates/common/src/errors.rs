use thiserror::Error;

#[derive(Debug, Error)]
pub enum TridentError {
    /// Failure communicating with or parsing a response from Stellar RPC.
    #[error("RPC error: {0}")]
    RpcError(String),

    /// Failure decoding or normalising raw XDR event data.
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Failure reading from or writing to PostgreSQL or Redis.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Missing or invalid configuration value.
    #[error("Config error: {0}")]
    ConfigError(String),
}
