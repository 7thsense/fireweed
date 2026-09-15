use thiserror::Error;

/// Errors returned by the object-log engine, storage adapters, and sequencer.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObjectLogError {
    /// An object key was empty or contained a `..` path component or `\0`.
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),
    /// The storage layer was unreachable or returned an I/O error.
    #[error("storage unavailable: {0}")]
    StorageUnavailable(String),
    /// A `get_range` request named a byte range outside the object, or inverted.
    #[error("byte range out of bounds: {0}")]
    RangeOutOfBounds(String),
    /// The index referenced an object that is no longer present in storage.
    #[error("missing object: {0}")]
    MissingObject(String),
    /// A produce batch was malformed (e.g. empty, or below the configured floor).
    #[error("invalid produce batch: {0}")]
    InvalidBatch(String),
    /// The sequencer rejected or failed to process a commit/lookup.
    #[error("sequencer error: {0}")]
    Sequencer(String),
    /// Durable-ops budget admission failed (`BudgetMode::FailClosed` or timed out).
    #[error("durable-ops budget exceeded: {0}")]
    BudgetExceeded(String),
    /// Invalid engine or budget configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<std::io::Error> for ObjectLogError {
    fn from(value: std::io::Error) -> Self {
        Self::StorageUnavailable(value.to_string())
    }
}
