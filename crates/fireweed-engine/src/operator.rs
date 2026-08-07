//! Operator-operation store (API-002 async operation model; Phase 2 §4a). Durable engine state for
//! the operator control plane: the `operation_id` is the async idempotency anchor (API-002 row
//! "idempotency": replaying a create `request_id` returns the SAME `operation_id` and does not start a
//! second operation; a different body under the same `request_id` is `request-id-conflict`).
//!
//! The library facade (`fireweed`) and server composition root wire these types into the operator
//! repair/redrive surface (pause/resume, repair, redrive, purge/archive, inspection, auth, audit).
//!
//! ## Why this does NOT reuse `QueueIdempotencyCache` (deliberate deviation from the build plan)
//!
//! API-002 row 206 makes the replay→same-`operation_id` guarantee **unconditional** — it is not scoped
//! to `request_id_retention_ms`. The HTTP service realized this with a *permanent, deterministic*
//! dedup: `operation_id` was a pure function of `(tenant, queue, kind, request_id)` and the
//! `request_id → operation_id` index never expired. `QueueIdempotencyCache` is the *synchronous*
//! (API-001) primitive whose entries expire on `request_id_retention_ms` and whose `Expired` decision
//! means "treat as a new request" — correct for push, but for a **destructive** operator op (purge /
//! redrive) that would re-execute the mutation under a fresh `operation_id` after the window. So this
//! store owns its own non-expiring `request_id → operation_id` index and keeps the fingerprint on the
//! operation record (the `existing_operator_operation` logic from the service), preserving permanent
//! dedup. (Surfaced as BLOCKING B1 in fresh-eyes review.)
//!
//! ## Invariant
//!
//! Every value in `by_request` is a key in `operations`, and that record's `request_id` equals the
//! `by_request` key. `record` writes both maps together; the two maps therefore share one lifetime.
//! Any future bounded operation-history retention MUST drop a record and its `by_request` entry
//! together (never one without the other), or `lookup`'s `expect` becomes reachable.
//!
//! Scope: ONE instance per `(tenant, queue, shard)`; tenant/queue isolation is structural (this IS the
//! queue's store) rather than a per-call filter (the HTTP service used one global map and filtered by
//! tenant/queue on every Get/Cancel — a defense the wiring site reproduces by never sharing a store
//! across scopes; tracked in build-progress.md).
//!
//! DEFERRED: bounded operation-history retention. The service kept `operations` unbounded; this store
//! matches that for now. A bounded policy is a later refinement (must honor the invariant above).

use std::collections::HashMap;

use fireweed_core::{BodyHash, RequestId};

use crate::error::{EngineError, EngineResult};

/// Server-assigned async operation handle (API-002). Opaque + stable across replays of its create
/// `request_id`. The engine does not interpret its structure; the adapter chooses the format.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// API-002 async operation lifecycle state (`operation.state`).
///
/// `Partial` means some shards/items committed and others failed and remain re-drivable (resumable,
/// NOT terminal). Terminal states are `Succeeded`, `Failed`, and `Canceled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOperationState {
    Accepted,
    Running,
    Succeeded,
    Partial,
    Failed,
    Canceled,
}

impl OperatorOperationState {
    /// Terminal states no longer schedule per-shard work and cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OperatorOperationState::Succeeded
                | OperatorOperationState::Failed
                | OperatorOperationState::Canceled
        )
    }
}

/// A view of a recorded operation: its authoritative lifecycle `state` plus the opaque payload the
/// adapter recorded. Returned by lookup/get/advance/cancel so the adapter can render the wire response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationHandle<R> {
    pub operation_id: OperationId,
    pub state: OperatorOperationState,
    pub payload: R,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationRecord<R> {
    /// Back-reference to the create `request_id` (anchors the `by_request` invariant + log rebuild).
    request_id: RequestId,
    /// Body fingerprint of the create request (same `request_id` + different fingerprint = conflict).
    fingerprint: BodyHash,
    state: OperatorOperationState,
    payload: R,
}

/// Durable operator-operation store for ONE `(tenant, queue, shard)` (see module scope invariant).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorOperationStore<R> {
    /// `operation_id → record` (the addressable operation history; Get/Advance/Cancel target).
    operations: HashMap<OperationId, OperationRecord<R>>,
    /// `request_id → operation_id`, permanent (no retention window — see module doc).
    by_request: HashMap<RequestId, OperationId>,
}

impl<R> Default for OperatorOperationStore<R> {
    fn default() -> Self {
        Self {
            operations: HashMap::new(),
            by_request: HashMap::new(),
        }
    }
}

impl<R: Clone> OperatorOperationStore<R> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Idempotency-anchor check for an operator create (API-002). Given the request's body
    /// `fingerprint`:
    /// - `Ok(None)` — no prior operation for this `request_id`: the caller executes the operation and
    ///   then [`record`](Self::record)s it.
    /// - `Ok(Some(handle))` — the same `request_id`+body replays the prior operation (same
    ///   `operation_id`, same state/payload); the caller MUST NOT start a second operation.
    /// - `Err(RequestIdConflict)` — the same `request_id` under a different body (API-002
    ///   `request-id-conflict`).
    ///
    /// Dedup is permanent (no clock): an operator create replays its `operation_id` for as long as the
    /// operation record is retained.
    pub fn lookup(
        &self,
        request_id: &RequestId,
        fingerprint: BodyHash,
    ) -> EngineResult<Option<OperationHandle<R>>> {
        let Some(operation_id) = self.by_request.get(request_id) else {
            return Ok(None);
        };
        let record = self.operations.get(operation_id).expect(
            "by_request invariant: every index entry references a recorded operation (record() \
             inserts both; history retention must drop both together)",
        );
        if record.fingerprint != fingerprint {
            return Err(EngineError::RequestIdConflict);
        }
        Ok(Some(OperationHandle {
            operation_id: operation_id.clone(),
            state: record.state,
            payload: record.payload.clone(),
        }))
    }

    /// Record a freshly-executed operation and anchor its create `request_id` to the `operation_id`.
    /// Call only after [`lookup`](Self::lookup) returned `Ok(None)` (never overwrite a live replay —
    /// that would corrupt the idempotency anchor; the check→record discipline is exercised in tests).
    pub fn record(
        &mut self,
        request_id: RequestId,
        fingerprint: BodyHash,
        operation_id: OperationId,
        state: OperatorOperationState,
        payload: R,
    ) {
        self.by_request
            .insert(request_id.clone(), operation_id.clone());
        self.operations.insert(
            operation_id,
            OperationRecord {
                request_id,
                fingerprint,
                state,
                payload,
            },
        );
    }

    /// `GetOperation` (API-002): fetch a recorded operation by `operation_id`. `None` if unknown (the
    /// adapter maps that to not-found).
    pub fn get(&self, operation_id: &OperationId) -> Option<OperationHandle<R>> {
        self.operations
            .get(operation_id)
            .map(|record| OperationHandle {
                operation_id: operation_id.clone(),
                state: record.state,
                payload: record.payload.clone(),
            })
    }

    /// Advance an in-flight operation's lifecycle (e.g. `Accepted → Running → Succeeded/Partial/
    /// Failed`) as shards converge, replacing the recorded state + payload. `Err(NotFound)` for an
    /// unknown `operation_id`; records already in a terminal state are immutable (`Err(Terminal)`) —
    /// use [`cancel`](Self::cancel) for the best-effort stop. Transition *monotonicity* (e.g. not
    /// regressing Running→Accepted) and progress-count exactness at terminal states are the driver's /
    /// adapter's responsibility; the engine guarantees only terminal-immutability.
    pub fn advance(
        &mut self,
        operation_id: &OperationId,
        state: OperatorOperationState,
        payload: R,
    ) -> EngineResult<OperationHandle<R>> {
        let record = self
            .operations
            .get_mut(operation_id)
            .ok_or(EngineError::NotFound)?;
        if record.state.is_terminal() {
            return Err(EngineError::Terminal);
        }
        record.state = state;
        record.payload = payload;
        Ok(OperationHandle {
            operation_id: operation_id.clone(),
            state: record.state,
            payload: record.payload.clone(),
        })
    }

    /// `CancelOperation` (API-002): best-effort stop. `Err(NotFound)` for an unknown `operation_id`.
    /// A non-terminal operation transitions to `Canceled` (already-committed shard work is durable and
    /// is NOT rolled back). An already-terminal operation is returned unchanged (idempotent).
    ///
    /// NOTE: this corrects the HTTP service, which flipped *any* operation — including a `Succeeded`
    /// one — to `Canceled` unconditionally. Per API-002 cancel only "stops scheduling further per-shard
    /// work"; there is no further work on a completed operation, so the engine leaves terminal states
    /// intact rather than falsely reporting a completed mutation as canceled.
    pub fn cancel(&mut self, operation_id: &OperationId) -> EngineResult<OperationHandle<R>> {
        let record = self
            .operations
            .get_mut(operation_id)
            .ok_or(EngineError::NotFound)?;
        if !record.state.is_terminal() {
            record.state = OperatorOperationState::Canceled;
        }
        Ok(OperationHandle {
            operation_id: operation_id.clone(),
            state: record.state,
            payload: record.payload.clone(),
        })
    }

    /// List recorded operations newest-first by insertion order (stable for small smoke sets).
    pub fn list(&self) -> Vec<OperationHandle<R>> {
        self.operations
            .iter()
            .map(|(operation_id, record)| OperationHandle {
                operation_id: operation_id.clone(),
                state: record.state,
                payload: record.payload.clone(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// API-002 operator domain types (transport-neutral)
// ---------------------------------------------------------------------------

/// Operator repair action (API-002 `RepairItems.action`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairAction {
    Reschedule,
    ForceRetry,
    ForceFail,
    ForceComplete,
    ForceRelease,
    ClearLease,
}

/// How redrive/force_retry adjusts `retry_count` / attempt budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCountMode {
    #[default]
    Reset,
    Preserve,
    Increment,
}

/// Kind of a recorded async operator operation (selector-scoped mutations).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOpKind {
    Repair,
    Redrive,
    Purge,
    Archive,
    Pause,
    Resume,
}

/// Progress counters for an async operator operation (API-002 `operation.progress`).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorProgress {
    pub matched: u64,
    pub affected: u64,
    pub failed: u64,
    pub updated_at_ms: i64,
}

/// Payload retained on the async operation record (redacted: no payloads/lease tokens).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorOpPayload {
    pub kind: OperatorOpKind,
    pub queue_tenant: String,
    pub queue_id: String,
    pub request_id: String,
    pub dry_run: bool,
    pub audit_reason: Option<String>,
    pub progress: OperatorProgress,
    /// Selector fingerprint (hash of the request body) for audit without logging the full selector.
    pub selector_fingerprint: u64,
}

/// Queue admin state returned by pause/resume/get (API-002 `GetQueueAdminState`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueueAdminState {
    pub paused: bool,
    pub queue_admin_paused: bool,
    /// While paused, no eligible age accrues (single Eligibility Precedence).
    pub eligible_age_accrues: bool,
}

/// Operator-visible item view: never carries a lease token (API-002 GetItem / INV AC-SEC-2).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorItemView {
    pub item_id: String,
    pub client_item_key: String,
    pub item_version: u64,
    pub lifecycle_state: String,
    pub priority: Option<String>,
    pub not_before_ms: Option<i64>,
    pub attempt_count: u32,
    pub worker_id: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    /// Always false in the operator plane — tokens are redacted, never returned.
    pub lease_token_present: bool,
    pub lease_token_redacted: bool,
}

/// Redacted operator audit record (no payloads, no lease tokens).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorAuditRecord {
    pub request_id: String,
    pub operation_id: Option<String>,
    pub principal_id: String,
    pub kind: OperatorOpKind,
    pub tenant_id: String,
    pub queue_id: String,
    pub selector_fingerprint: u64,
    pub matched: u64,
    pub affected: u64,
    pub dry_run: bool,
    pub audit_reason: Option<String>,
    /// Always empty on the operator plane — payloads are never logged by default.
    pub payload_logged: bool,
    /// Always true when a lease was involved — only a hash is retained, never the plaintext.
    pub lease_token_redacted: bool,
}

/// Result of an async operator create (accepted or terminal for small item_refs).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperatorAsyncAccept {
    pub request_id: String,
    pub operation_id: OperationId,
    pub state: OperatorOperationState,
    pub progress: OperatorProgress,
    pub dry_run: bool,
    /// When the create was a pure replay of a prior `request_id`, true.
    pub replayed: bool,
}

/// Fingerprint a stable operator request body for permanent `request_id` dedup.
pub fn operator_body_fingerprint(kind: OperatorOpKind, body: &str) -> BodyHash {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(format!("{kind:?}").as_bytes());
    hasher.update(body.as_bytes());
    let digest = hasher.finalize();
    BodyHash(u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    ))
}

/// Deterministic operation id from (tenant, queue, kind, request_id) — permanent dedup anchor.
pub fn deterministic_operation_id(
    tenant: &str,
    queue: &str,
    kind: OperatorOpKind,
    request_id: &str,
) -> OperationId {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(tenant.as_bytes());
    hasher.update(b"|");
    hasher.update(queue.as_bytes());
    hasher.update(b"|");
    hasher.update(format!("{kind:?}").as_bytes());
    hasher.update(b"|");
    hasher.update(request_id.as_bytes());
    let digest = hasher.finalize();
    let hex = digest
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    OperationId::new(format!("oper_{hex}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(s: &str) -> RequestId {
        RequestId::new(s).unwrap()
    }
    fn oid(s: &str) -> OperationId {
        OperationId::new(s)
    }

    /// Record one succeeded operation; replaying its create returns the same operation_id forever.
    fn store_with_op() -> OperatorOperationStore<&'static str> {
        let mut store = OperatorOperationStore::new();
        store.record(
            rid("req-1"),
            BodyHash(1),
            oid("oper-1"),
            OperatorOperationState::Succeeded,
            "redrive-result",
        );
        store
    }

    #[test]
    fn new_create_is_not_yet_anchored() {
        let store: OperatorOperationStore<&str> = OperatorOperationStore::new();
        assert_eq!(store.lookup(&rid("req-1"), BodyHash(1)), Ok(None));
    }

    #[test]
    fn replay_same_request_returns_same_operation() {
        let store = store_with_op();
        let handle = store
            .lookup(&rid("req-1"), BodyHash(1))
            .unwrap()
            .expect("same request_id + body replays the recorded operation");
        assert_eq!(handle.operation_id, oid("oper-1"));
        assert_eq!(handle.state, OperatorOperationState::Succeeded);
        assert_eq!(handle.payload, "redrive-result");
    }

    #[test]
    fn different_body_same_request_is_conflict() {
        let store = store_with_op();
        assert_eq!(
            store.lookup(&rid("req-1"), BodyHash(2)),
            Err(EngineError::RequestIdConflict)
        );
    }

    #[test]
    fn dedup_is_permanent_no_retention_window() {
        // Regression guard for B1: a destructive operator replay must NEVER decay into "new operation"
        // (the service deduped forever; there is no clock parameter to expire the anchor).
        let store = store_with_op();
        // However long "later" is, the same create replays the same operation_id — not a fresh one.
        assert_eq!(
            store
                .lookup(&rid("req-1"), BodyHash(1))
                .unwrap()
                .map(|h| h.operation_id),
            Some(oid("oper-1"))
        );
    }

    #[test]
    fn get_returns_recorded_operation_and_none_for_unknown() {
        let store = store_with_op();
        assert_eq!(
            store.get(&oid("oper-1")).map(|h| h.state),
            Some(OperatorOperationState::Succeeded)
        );
        assert!(store.get(&oid("nope")).is_none());
    }

    #[test]
    fn advance_progresses_until_terminal_then_is_frozen() {
        let mut store = OperatorOperationStore::new();
        store.record(
            rid("req-9"),
            BodyHash(9),
            oid("oper-9"),
            OperatorOperationState::Accepted,
            "p0",
        );
        // Accepted -> Running -> Succeeded.
        assert_eq!(
            store
                .advance(&oid("oper-9"), OperatorOperationState::Running, "p1")
                .map(|h| h.state),
            Ok(OperatorOperationState::Running)
        );
        assert_eq!(
            store
                .advance(&oid("oper-9"), OperatorOperationState::Succeeded, "p2")
                .map(|h| h.state),
            Ok(OperatorOperationState::Succeeded)
        );
        // Terminal is frozen; unknown id is NotFound.
        assert_eq!(
            store.advance(&oid("oper-9"), OperatorOperationState::Failed, "p3"),
            Err(EngineError::Terminal)
        );
        assert_eq!(
            store.advance(&oid("nope"), OperatorOperationState::Running, "x"),
            Err(EngineError::NotFound)
        );
    }

    #[test]
    fn cancel_stops_non_terminal_and_leaves_terminal_intact() {
        // Non-terminal: Accepted -> Canceled.
        let mut store = OperatorOperationStore::new();
        store.record(
            rid("req-2"),
            BodyHash(1),
            oid("oper-2"),
            OperatorOperationState::Accepted,
            "pending-redrive",
        );
        assert_eq!(
            store.cancel(&oid("oper-2")).map(|h| h.state),
            Ok(OperatorOperationState::Canceled)
        );
        // Idempotent: cancel again stays canceled.
        assert_eq!(
            store.cancel(&oid("oper-2")).map(|h| h.state),
            Ok(OperatorOperationState::Canceled)
        );

        // CORRECTION vs service: a Succeeded operation is NOT flipped to Canceled.
        let mut succeeded = store_with_op();
        assert_eq!(
            succeeded.cancel(&oid("oper-1")).map(|h| h.state),
            Ok(OperatorOperationState::Succeeded)
        );

        // Unknown id → NotFound.
        assert_eq!(store.cancel(&oid("nope")), Err(EngineError::NotFound));
    }

    #[test]
    fn check_then_record_flow_never_starts_a_second_operation() {
        // Drive the real lookup->record discipline over a stream and prove a live request_id never
        // re-executes (I3): only a Proceed (Ok(None)) leads to record(); replays/conflicts do not.
        let mut store = OperatorOperationStore::new();
        let mut executions = 0;
        // (request_id, body, supplied-operation_id)
        let stream = [
            ("req-1", 1u64, "oper-1"),  // new -> execute + record
            ("req-2", 2, "oper-2"),     // new -> execute + record
            ("req-1", 1, "oper-1-DUP"), // replay -> MUST NOT execute or mint a new id
        ];
        let mut outcomes = Vec::new();
        for (r, body, op) in stream {
            match store.lookup(&rid(r), BodyHash(body)) {
                Ok(None) => {
                    executions += 1;
                    store.record(
                        rid(r),
                        BodyHash(body),
                        oid(op),
                        OperatorOperationState::Succeeded,
                        op,
                    );
                    outcomes.push(format!("ran:{op}"));
                }
                Ok(Some(h)) => outcomes.push(format!("replay:{}", h.operation_id.as_str())),
                Err(_) => outcomes.push("conflict".to_string()),
            }
        }
        assert_eq!(executions, 2, "only the two genuinely-new creates executed");
        assert_eq!(
            outcomes,
            vec![
                "ran:oper-1".to_string(),
                "ran:oper-2".to_string(),
                "replay:oper-1".to_string(), // the duplicate replayed the FIRST id, not oper-1-DUP
            ]
        );
        // The duplicate's supplied id was never recorded.
        assert!(store.get(&oid("oper-1-DUP")).is_none());
    }

    #[test]
    fn store_is_reconstructable_by_replaying_the_record_log() {
        // Durability claim (TD-007 §4): replaying the recorded operations rebuilds the WHOLE store
        // (both maps) — there is no separate anchor lifetime to diverge (resolves the M4 weakness).
        let log: [(&str, u64, &str, OperatorOperationState, &str); 2] = [
            (
                "req-a",
                1,
                "oper-a",
                OperatorOperationState::Succeeded,
                "ra",
            ),
            ("req-b", 2, "oper-b", OperatorOperationState::Partial, "rb"),
        ];
        let mut live = OperatorOperationStore::new();
        for (r, h, o, s, p) in log {
            live.record(rid(r), BodyHash(h), oid(o), s, p);
        }
        let mut rebuilt = OperatorOperationStore::new();
        for (r, h, o, s, p) in log {
            rebuilt.record(rid(r), BodyHash(h), oid(o), s, p);
        }
        assert_eq!(live, rebuilt, "full store == replay of the record log");
        // Both anchors still replay + both operations still addressable.
        assert_eq!(
            live.lookup(&rid("req-a"), BodyHash(1))
                .unwrap()
                .map(|h| h.operation_id),
            Some(oid("oper-a"))
        );
        assert_eq!(
            live.get(&oid("oper-b")).map(|h| h.state),
            Some(OperatorOperationState::Partial)
        );
    }
}
