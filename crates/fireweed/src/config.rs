use std::fmt;

/// An owned secret used by queue configuration. Its value is redacted from `Debug` and has no
/// public accessor; the composition root consumes it internally.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SecretValue(pub(crate) String);

impl SecretValue {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// A configuration secret (S3 credential, PostgreSQL URL). Redacted from `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigSecret(pub(crate) SecretValue);

impl ConfigSecret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretValue::new(value))
    }
}

impl fmt::Debug for ConfigSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfigSecret(<redacted>)")
    }
}
