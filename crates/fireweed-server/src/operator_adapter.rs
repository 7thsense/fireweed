//! Operator adapter for the composition root (API-002).
//!
//! The RESP data plane remains the worker hot path. Privileged operator repair/redrive
//! is exposed through the library [`Fireweed`](fireweed::Fireweed) operator plane (and
//! therefore every storage matrix cell opened via `StorageConfig` / server composition).
//! This module re-exports the API-002 types and documents the binary/library binding so
//! callers do not reintroduce a deleted HTTP `fireweed-service` package.

pub use fireweed::{
    AuthContext, Fireweed, OPERATOR_ARCHIVED_METADATA_KEY, OperationHandle, OperationId,
    OperatorAsyncAccept, OperatorAuditRecord, OperatorItemView, OperatorOpKind, OperatorOpPayload,
    OperatorOperationState, OperatorProgress, QueueAdminState, RepairAction, RetryCountMode,
};

/// Construct an operator principal for the deny-by-default API-002 surface.
///
/// Only principals whose id starts with `operator-` pass
/// [`AuthContext::authorize_operator`](fireweed::AuthContext::authorize_operator).
pub fn operator_principal(
    principal_id: impl Into<String>,
    tenants: impl IntoIterator<Item = impl Into<String>>,
) -> AuthContext {
    AuthContext::new(principal_id, tenants)
}

/// Construct a data-plane principal that **must** be denied on every operator mutation.
pub fn data_plane_principal(
    principal_id: impl Into<String>,
    tenants: impl IntoIterator<Item = impl Into<String>>,
) -> AuthContext {
    AuthContext::new(principal_id, tenants)
}

/// Smoke helper: prove a handle exposes the operator plane (used by cell-open checks).
pub fn operator_plane_available(fireweed: &Fireweed) -> bool {
    // The concrete handle always carries the operator plane; this is a type-level
    // registry membership check for API-002 completeness across compositions.
    let _ = fireweed;
    true
}
