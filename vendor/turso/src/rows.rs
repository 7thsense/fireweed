use crate::{assert_send_sync, Column, Error, Result, Statement, Value};
use std::fmt::Debug;
use std::future::Future;

/// Results of a prepared statement query.
pub struct Rows {
    inner: Statement,
}

impl Rows {
    pub(crate) fn new(inner: Statement) -> Self {
        Self { inner }
    }

    /// Returns the number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.inner.column_count()
    }

    /// Returns the name of the column at the given index.
    pub fn column_name(&self, idx: usize) -> Result<String> {
        self.inner.column_name(idx)
    }

    /// Returns the names of all columns in the result set.
    pub fn column_names(&self) -> Vec<String> {
        self.inner.column_names()
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.inner.column_index(name)
    }

    /// Returns columns of the result set.
    pub fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }

    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        struct Next {
            columns: usize,
            stmt: Statement,
        }

        impl Future for Next {
            type Output = Result<Option<Row>>;

            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                self.stmt.step(Some(self.columns), cx)
            }
        }

        assert_send_sync!(Next);

        let next = Next {
            columns: self.inner.inner.lock().unwrap().column_count(),
            stmt: self.inner.clone(),
        };

        next.await
    }
}

/// Query result row.
#[derive(Debug, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<turso_sdk_kit::rsapi::Value>,
}

impl Row {
    /// Consume this row, transferring its text and blob buffers to the caller.
    /// Values remain owned independently of the statement and subsequent rows.
    pub fn into_values(self) -> impl ExactSizeIterator<Item = Value> {
        self.values.into_iter().map(Value::from)
    }

    pub fn get_value(&self, idx: usize) -> Result<Value> {
        let val = self.values.get(idx).ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })?;
        match val {
            turso_sdk_kit::rsapi::Value::Numeric(turso_sdk_kit::rsapi::Numeric::Integer(i)) => {
                Ok(Value::Integer(*i))
            }
            turso_sdk_kit::rsapi::Value::Numeric(turso_sdk_kit::rsapi::Numeric::Float(f)) => {
                Ok(Value::Real(f64::from(*f)))
            }
            turso_sdk_kit::rsapi::Value::Null => Ok(Value::Null),
            turso_sdk_kit::rsapi::Value::Text(text) => {
                Ok(Value::Text(text.value.clone().into_owned()))
            }
            turso_sdk_kit::rsapi::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
    }

    pub fn get<T>(&self, idx: usize) -> Result<T>
    where
        T: turso_sdk_kit::rsapi::FromValue,
    {
        let val = self.values.get(idx).ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })?;
        T::from_sql(val.clone()).map_err(|err| Error::ConversionFailure(err.to_string()))
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod owned_row_tests {
    use super::*;

    #[test]
    fn consuming_row_preserves_types_and_transfers_variable_buffers() {
        let text = "owned metadata".repeat(100);
        let blob = vec![0, 255, 31, 128].repeat(1024);
        let text_pointer = text.as_ptr();
        let blob_pointer = blob.as_ptr();
        let row = Row {
            values: vec![
                Value::Null.into(),
                Value::Integer(i64::MIN).into(),
                Value::Real(1.25).into(),
                Value::Text(text).into(),
                Value::Blob(blob).into(),
                Value::Text(String::new()).into(),
                Value::Blob(Vec::new()).into(),
            ],
        };
        let mut values = row.into_values();
        assert_eq!(values.len(), 7);
        assert_eq!(values.next(), Some(Value::Null));
        assert_eq!(values.next(), Some(Value::Integer(i64::MIN)));
        assert_eq!(values.next(), Some(Value::Real(1.25)));
        let Some(Value::Text(text)) = values.next() else {
            panic!("missing text")
        };
        assert_eq!(text, "owned metadata".repeat(100));
        assert_eq!(
            text.as_ptr(),
            text_pointer,
            "text buffer must transfer without cloning"
        );
        let Some(Value::Blob(blob)) = values.next() else {
            panic!("missing blob")
        };
        assert_eq!(blob, vec![0, 255, 31, 128].repeat(1024));
        assert_eq!(
            blob.as_ptr(),
            blob_pointer,
            "blob buffer must transfer without cloning"
        );
        assert_eq!(values.next(), Some(Value::Text(String::new())));
        assert_eq!(values.next(), Some(Value::Blob(Vec::new())));
        assert_eq!(values.len(), 0);
        assert_eq!(values.next(), None);
    }
}
