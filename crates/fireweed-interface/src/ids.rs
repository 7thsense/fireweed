//! Identifier newtypes (API-006 §Names). The interface owns every id type
//! (crate rule 1); none is borrowed from `fireweed-core`.

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($($(#[$doc:meta])* $name:ident),* $(,)?) => {
        $(
            $(#[$doc])*
            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
            #[serde(transparent)]
            pub struct $name(pub String);

            impl $name {
                pub fn new(s: impl Into<String>) -> Self {
                    Self(s.into())
                }
                pub fn as_str(&self) -> &str {
                    &self.0
                }
            }

            impl From<&str> for $name {
                fn from(s: &str) -> Self {
                    Self(s.to_owned())
                }
            }

            impl std::fmt::Display for $name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(&self.0)
                }
            }
        )*
    };
}

string_id!(
    TenantId,
    /// `[a-z0-9][a-z0-9_-]{0,62}`
    SpaceId,
    /// `[a-z][a-z0-9_]{0,39}`; prefix `fw_` reserved.
    ObjectName,
    /// `[a-z][a-z0-9_]{0,39}`; prefix `fw_` reserved for system fields.
    FieldName,
    /// `[a-z][a-z0-9_]{0,39}`; prefix `fw_` reserved for built-in reads.
    ReadName,
    ParamName,
    /// RFC 6901 JSON pointer.
    JsonPointer,
    ClientItemKey,
    RequestId,
    LeaseToken,
    WorkerId,
    CohortId,
    GroupKey,
    /// Opaque.
    PageToken,
    /// Opaque; bound to the request that produced it.
    Continuation,
);

/// Item id, unique and never reused within a space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemId(pub u64);

/// Milliseconds since the Unix epoch, UTC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UtcMillis(pub i64);

/// One atomic and ordering domain, owned by one tenant.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SpaceKey {
    pub tenant_id: TenantId,
    pub space_id: SpaceId,
}

impl SpaceKey {
    pub fn new(tenant_id: impl Into<String>, space_id: impl Into<String>) -> Self {
        Self {
            tenant_id: TenantId::new(tenant_id),
            space_id: SpaceId::new(space_id),
        }
    }
}

fn is_lower_ident(s: &str, max_len: usize) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    s.len() <= max_len && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Object, field, read and parameter names: `[a-z][a-z0-9_]{0,39}`.
pub fn is_valid_name(s: &str) -> bool {
    is_lower_ident(s, 40)
}

/// True if a user-declared name uses the reserved `fw_` prefix.
pub fn is_reserved(s: &str) -> bool {
    s.starts_with("fw_")
}

/// Space ids: `[a-z0-9][a-z0-9_-]{0,62}`.
pub fn is_valid_space_id(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {}
        _ => return false,
    }
    s.len() <= 63
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}
