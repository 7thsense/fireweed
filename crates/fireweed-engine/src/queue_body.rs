//! Exact opaque-body write intent, independent of read materialization.
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedBody {
    Preserve,
    Replace(Bytes),
}
impl ResolvedBody {
    pub fn then(self, after: Self) -> Self {
        match after {
            Self::Preserve => self,
            replacement => replacement,
        }
    }
    pub fn apply_to_view(self, body: &mut Bytes, loaded: &mut bool) {
        if let Self::Replace(value) = self {
            *body = value;
            *loaded = true;
        }
    }
}
