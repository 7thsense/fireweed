//! Driver-neutral SQL values shared by SQLite-family projection adapters.

use fireweed_engine::{EngineError, EngineResult};

/// One bound parameter or result cell. Matches the SQLite type affinity both rusqlite and Turso use.
#[derive(Debug, Clone, PartialEq)]
pub enum RelValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl RelValue {
    pub fn opt_text(value: Option<String>) -> Self {
        value.map_or(Self::Null, Self::Text)
    }

    pub fn opt_int(value: Option<i64>) -> Self {
        value.map_or(Self::Null, Self::Integer)
    }

    pub fn opt_blob(value: Option<Vec<u8>>) -> Self {
        value.map_or(Self::Null, Self::Blob)
    }
}

impl From<i64> for RelValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<u64> for RelValue {
    fn from(value: u64) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<bool> for RelValue {
    fn from(value: bool) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<String> for RelValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for RelValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<&String> for RelValue {
    fn from(value: &String) -> Self {
        Self::Text(value.clone())
    }
}

impl From<Vec<u8>> for RelValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<&[u8]> for RelValue {
    fn from(value: &[u8]) -> Self {
        Self::Blob(value.to_vec())
    }
}

impl From<Option<String>> for RelValue {
    fn from(value: Option<String>) -> Self {
        Self::opt_text(value)
    }
}

impl From<Option<i64>> for RelValue {
    fn from(value: Option<i64>) -> Self {
        Self::opt_int(value)
    }
}

impl From<Option<Vec<u8>>> for RelValue {
    fn from(value: Option<Vec<u8>>) -> Self {
        Self::opt_blob(value)
    }
}

impl From<Option<&str>> for RelValue {
    fn from(value: Option<&str>) -> Self {
        value.map_or(Self::Null, |text| Self::Text(text.to_string()))
    }
}

/// One result row owned independently of the driver cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct RelRow(pub Vec<RelValue>);

impl RelRow {
    pub fn get<T: FromRelValue>(&self, index: usize) -> EngineResult<T> {
        let value = self.0.get(index).ok_or_else(|| {
            EngineError::Storage(format!("relational row missing column {index}"))
        })?;
        T::from_rel(value)
    }
}

pub trait FromRelValue: Sized {
    fn from_rel(value: &RelValue) -> EngineResult<Self>;
}

impl FromRelValue for RelValue {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        Ok(value.clone())
    }
}

impl FromRelValue for String {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        match value {
            RelValue::Text(text) => Ok(text.clone()),
            other => Err(EngineError::Storage(format!(
                "expected text, got {other:?}"
            ))),
        }
    }
}

impl FromRelValue for i64 {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        match value {
            RelValue::Integer(value) => Ok(*value),
            other => Err(EngineError::Storage(format!(
                "expected integer, got {other:?}"
            ))),
        }
    }
}

impl FromRelValue for Vec<u8> {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        match value {
            RelValue::Blob(value) => Ok(value.clone()),
            other => Err(EngineError::Storage(format!(
                "expected blob, got {other:?}"
            ))),
        }
    }
}

impl FromRelValue for bool {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        Ok(i64::from_rel(value)? != 0)
    }
}

impl<T: FromRelValue> FromRelValue for Option<T> {
    fn from_rel(value: &RelValue) -> EngineResult<Self> {
        match value {
            RelValue::Null => Ok(None),
            other => T::from_rel(other).map(Some),
        }
    }
}
