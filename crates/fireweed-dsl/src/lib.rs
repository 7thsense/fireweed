//! Fireweed's data-plane DSL (API-008).
//!
//! A [`Schema`] declares a space's queues, tables and reads. A [`Program`]
//! is a list of [`Statement`]s run as one transaction. Both are read from
//! YAML or JSON. [`check::resolve`] checks a program against a schema and
//! returns the resolved statements an executor lowers to SQL.

pub mod check;
mod doc;

pub use doc::*;

/// Errors that change nothing and are not recorded (API-008 §Transactions).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The document doesn't parse, or the program doesn't check against the schema.
    Invalid(String),
    SpaceNotFound,
    /// `apply_schema` with an expected version that isn't the stored one.
    SchemaConflict {
        current: Option<i64>,
    },
    /// A request id reused with a different program.
    RequestIdConflict,
    /// The request was issued before the space's horizon: its record may be
    /// gone, so it is refused rather than run twice (ADR-027 D61).
    RequestExpired,
    /// A guarded program kept failing to serialize.
    Contention,
    /// A database error.
    Unavailable(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Invalid(m) => write!(f, "invalid: {m}"),
            Error::SpaceNotFound => write!(f, "space not found"),
            Error::SchemaConflict { current } => {
                write!(f, "schema conflict (current version {current:?})")
            }
            Error::RequestIdConflict => write!(f, "request id reused with a different program"),
            Error::RequestExpired => write!(f, "request expired"),
            Error::Contention => write!(f, "contention"),
            Error::Unavailable(m) => write!(f, "unavailable: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(msg.into()))
}

/// Reads a YAML or JSON document (JSON is YAML). The YAML is read into JSON
/// first, so a statement or predicate is a single-key map in both.
pub fn from_yaml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    let v: serde_json::Value =
        serde_norway::from_str(text).map_err(|e| Error::Invalid(e.to_string()))?;
    serde_json::from_value(v).map_err(|e| Error::Invalid(e.to_string()))
}
