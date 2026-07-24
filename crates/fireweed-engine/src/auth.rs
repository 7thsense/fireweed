//! Authorization domain rules and lease-token redaction (migrated from the HTTP service, Phase 2 §4a).
//!
//! These are pure domain rules: the *principal* is authenticated by the driving adapter (RESP
//! `HELLO`/ACL or library caller context) and supplied here; the engine only *decides*. Auth is not
//! durable state (TD-007 §4 covers the durable ones — idempotency/fences/pause).

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::error::{EngineError, EngineResult};

/// An authenticated principal and the tenants it may act on. Construct it from whatever the driving
/// adapter authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    principal_id: String,
    tenants: BTreeSet<String>,
}

impl AuthContext {
    pub fn new(
        principal_id: impl Into<String>,
        tenants: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            principal_id: principal_id.into(),
            tenants: tenants.into_iter().map(Into::into).collect(),
        }
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    /// The principal must be authorized for `tenant`, else `Forbidden` (adapter → `-NOPERM`).
    pub fn authorize_tenant(&self, tenant: impl AsRef<str>) -> EngineResult<()> {
        if self.tenants.contains(tenant.as_ref()) {
            Ok(())
        } else {
            Err(EngineError::Forbidden(
                "principal is not authorized for the requested tenant",
            ))
        }
    }

    /// The operator plane (repair/redrive/purge/pause/…) is deny-by-default; only `operator-*`
    /// principals may use it (API-002).
    pub fn authorize_operator(&self) -> EngineResult<()> {
        if self.principal_id.starts_with("operator-") {
            Ok(())
        } else {
            Err(EngineError::Forbidden(
                "principal lacks operator privileges",
            ))
        }
    }
}

/// SHA-256 of a lease token, for audit trails that must not log the plaintext token.
pub fn hash_lease_token(token: &str) -> [u8; 32] {
    let digest = Sha256::digest(token.as_bytes());
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    hash
}

/// A lease token wrapper whose `Debug`/`Display` never reveal the plaintext.
#[derive(Clone, Copy)]
pub struct RedactedLeaseToken<'a> {
    token: &'a str,
}

impl<'a> RedactedLeaseToken<'a> {
    pub fn new(token: &'a str) -> Self {
        Self { token }
    }

    pub fn hash(self) -> [u8; 32] {
        hash_lease_token(self.token)
    }
}

impl std::fmt::Debug for RedactedLeaseToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeaseToken([redacted])")
    }
}

impl std::fmt::Display for RedactedLeaseToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> AuthContext {
        AuthContext::new("svc-1", ["t1", "t2"])
    }

    #[test]
    fn authorize_tenant_allows_member_denies_others() {
        let c = ctx();
        assert!(c.authorize_tenant("t1").is_ok());
        assert!(c.authorize_tenant("t2").is_ok());
        assert_eq!(
            c.authorize_tenant("t3"),
            Err(EngineError::Forbidden(
                "principal is not authorized for the requested tenant"
            ))
        );
    }

    #[test]
    fn authorize_operator_requires_operator_prefix() {
        // deny-by-default for a normal principal
        assert!(ctx().authorize_operator().is_err());
        let op = AuthContext::new("operator-root", ["t1"]);
        assert!(op.authorize_operator().is_ok());
    }

    #[test]
    fn lease_token_is_hashed_and_redacted() {
        let h1 = hash_lease_token("secret-lease");
        let h2 = hash_lease_token("secret-lease");
        let h3 = hash_lease_token("other-lease");
        assert_eq!(h1, h2, "hash is deterministic");
        assert_ne!(h1, h3);
        // Redaction never reveals the plaintext.
        let r = RedactedLeaseToken::new("secret-lease");
        assert_eq!(format!("{r}"), "[redacted]");
        assert_eq!(format!("{r:?}"), "LeaseToken([redacted])");
        assert_eq!(r.hash(), h1);
    }
}
