#![forbid(unsafe_code)]
//! # fireweed-conformance
//!
//! The backend-conformance harness: the behavioral **no-stub** suite (plan §6) factored out of any one
//! adapter so every driven backend runs the SAME tests. Each scenario fails if a port returns a
//! default/no-op, proving the adapter actually implements the behavior.
//!
//! Usage from an adapter's tests (one line, full per-scenario granularity):
//!
//! ```ignore
//! fireweed_conformance::conformance_suite!(MyBackend::new);
//! ```
//!
//! or a single aggregate test:
//!
//! ```ignore
//! #[tokio::test]
//! async fn conformance() { fireweed_conformance::run_conformance(MyBackend::new).await; }
//! ```
//!
//! Each scenario takes a `make: impl Fn() -> B` factory (not a constructed backend) because some
//! scenarios build a SECOND fresh backend — e.g. log-replay reconstruction.
//!
//! Scope: this harness exercises only the engine **ports** ([`ConformanceBackend`]); white-box tests
//! that inspect an adapter's private state (e.g. item_version straight from the projection, or
//! simulating log compaction) stay in that adapter's own crate.
//!
//! Rule for new engine ports that are intended for more than one backend: they MUST add a
//! capability-gated conformance scenario here, not a bespoke per-backend test file. That scenario
//! should prove the advertised behavior when the capability is present, and prove explicit decline
//! plus capability-flag consistency when it is absent. The CommitTransitionPort gap in this epic is
//! the concrete precedent for why this rule exists.
//!
//! ## Conformance matrix (BQ-13 / ADR-008 §2 / TD-001 capability classes)
//!
//! Every backend runs the classes its durability + recovery model supports. The CORE class is the SAME
//! scenario set for ALL backends (`core_suite!(@atomic)` or `core_suite!(@eventual)`), including upsert on
//! both durability classes; only operations that still require an atomic projection boundary differ.
//!
//! | Backend | Projection family | Durability | Class wiring | Where |
//! |---|---|---|---|---|
//! | `fireweed_memory::composed_memory_backend` | in-memory log-replay | atomic | `conformance_suite!` = core@atomic + log-replay | `fireweed-memory/src/tests.rs` |
//! | `fireweed_sqlite::composed_sqlite_backend` (log) | in-memory log-replay | atomic | `conformance_suite!` + `relational_reconnect_suite!` | `fireweed-sqlite/tests/{conformance,reconnect_smoke}.rs` |
//! | `fireweed_postgres::PostgresBackend` (log) | in-memory log-replay | atomic | core@atomic + log (env-gated `pg_conformance!`) | `fireweed-postgres/tests/conformance.rs` |
//! | `fireweed_objectlog::composed_objectlog_backend` | log-bearing, in-memory projection | atomic | `conformance_suite!` = core@atomic + log-replay | `fireweed-objectlog/tests/conformance.rs` |
//! | `fireweed_sqlite::SqliteRelationalBackend` | relational (DB-authoritative) | atomic | `core_suite!(@atomic)` + `relational_reconnect_suite!` | `fireweed-sqlite/tests/relational_{conformance,reconnect}.rs` |
//! | `fireweed_postgres::PostgresRelationalBackend` | relational (DB-authoritative) | atomic | core@atomic + relational-reconnect (env-gated) | `fireweed-postgres/tests/relational_conformance.rs` |
//!
//! Relational-only features (`fireweed_group_summary`, `fireweed_item_key_retention`) are deliberately OUT of
//! the shared CORE class so the families stay identical on it. The two families are additionally held
//! identical HEAD-TO-HEAD by [`scenarios::cross_family_core_parity`] (run sqlite-relational vs in-memory in
//! `fireweed-sqlite/tests/cross_family_parity.rs`). PARITY EVIDENCE STATUS: sqlite-relational-vs-in-memory is
//! validated locally; the postgres-relational half runs the identical class wiring but its live-DB
//! evidence is env-gated on `FIREWEED_PG_TEST_URL` and deferred-with-reason where no database is present
//! (convergence-review I3).
//!
//! ## Product durability classes (Class A / Class B)
//!
//! The public 5×3 log × projection matrix (matrix brief) uses a separate product
//! **durability class** axis from engine `DurabilityClass` (Atomic / EventualApply):
//!
//! | Product class | Logs | Durable log-replay after process death? |
//! |---|---|---|
//! | **Class A** | `sqlite`, `postgres`, `filesystem`, `s3` | Yes — log is system of record |
//! | **Class B** | `memory` | **No** — projection-only reopen when projection is durable |
//!
//! See [`matrix_classes`] for the per-cell claim table (unit-tested: Class B never
//! claims `durable_log_replay`). Full CI / suite map:
//! `docs/helix/04-build/storage-matrix-conformance-classes.md`.
//!
//! Note: in-process `log_replay_suite!` on `memory` × `memory` exercises live-process
//! `LogRead` only; it is **not** a product Class A recovery claim.

use std::collections::BTreeMap;

use fireweed_core::{
    ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, Metadata, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, CommandChecksum, CommandEnvelope,
    CommandId, CommitTransitionPort, ControlPlaneStore, FinalizePort, HotProjectionQueryPort,
    IndexQueryPort, LogRead, ProjectionRead, PurgePort, PushItem, PushPort, QueueCommand, QueueKey,
    RawCommitRequest, ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort,
    RenewLeasePort, SnapshotStore, UpdateFieldsPort, UpsertPort,
};

pub mod fault;
pub mod matrix_classes;
pub mod scenarios;

#[cfg(test)]
pub mod hybrid_async;

/// The **core** conformance bound: the engine ports the substrate-independent scenarios exercise. Every
/// projection family implements these — ordering, eligibility, claim atomicity, idempotency, lease/epoch
/// fencing, and the per-queue progress bound (ADR-008 §2 / TD-001 capability classes). It does **not**
/// require `LogRead`/`SnapshotStore`, so a **log-optional, DB-authoritative relational backend** qualifies
/// and runs the core class (and the relational-reconnect class) without a command log.
///
/// A blanket impl means any adapter implementing the listed ports is a `ConformanceCore` for free.
pub trait ConformanceCore:
    Backend
    + ControlPlaneStore
    + ProjectionRead
    + PushPort
    + ClaimPort
    + UpsertPort
    + UpdateFieldsPort
    + FinalizePort
    + RenewLeasePort
    + ReassignLeasePort
    + ReclaimPort
    + PurgePort
    + ReclaimDriver
{
}

impl<T> ConformanceCore for T where
    T: Backend
        + ControlPlaneStore
        + ProjectionRead
        + PushPort
        + ClaimPort
        + UpsertPort
        + UpdateFieldsPort
        + FinalizePort
        + RenewLeasePort
        + ReassignLeasePort
        + ReclaimPort
        + PurgePort
        + ReclaimDriver
{
}

/// A **log-bearing** conformance backend: [`ConformanceCore`] plus the durable-log ports the log-replay
/// class exercises — `LogRead` (replay reconstruction) and `SnapshotStore` (snapshots). The committed
/// log-bearing backends (memory, sqlite, objectlog, and any log-bearing relational backend) run the
/// `log_replay_suite!`; a truly log-less relational backend implements only `ConformanceCore`.
pub trait ConformanceBackend: ConformanceCore + SnapshotStore + LogRead {}

impl<T> ConformanceBackend for T where T: ConformanceCore + SnapshotStore + LogRead {}

/// A backend that participates in the shared Snorri commit-transition scenarios:
/// [`ConformanceCore`] plus the authoritative commit and recovery read ports.
pub trait ConformanceCommitTransition:
    ConformanceCore + CommitTransitionPort + RecoveryReadPort
{
}

impl<T> ConformanceCommitTransition for T where
    T: ConformanceCore + CommitTransitionPort + RecoveryReadPort
{
}

/// ADR-011 typed-schema/index conformance backend. This is separate from [`ConformanceCore`] so
/// ADR-010-only backends can still satisfy the core queue contract without typed index lookup.
pub trait Adr011ConformanceBackend: ConformanceCore + IndexQueryPort {}

impl<T> Adr011ConformanceBackend for T where T: ConformanceCore + IndexQueryPort {}

/// Backend class for the public filtered lifecycle metrics query.
pub trait FilteredMetricsConformanceBackend: ConformanceCore + HotProjectionQueryPort {}

impl<T> FilteredMetricsConformanceBackend for T where T: ConformanceCore + HotProjectionQueryPort {}

// ---------------------------------------------------------------------------
// Shared fixtures (public so adapters' own white-box tests can reuse them too)
// ---------------------------------------------------------------------------

pub fn tenant() -> TenantId {
    TenantId::new("t1").unwrap()
}
pub fn queue() -> QueueId {
    QueueId::new("q1").unwrap()
}
pub fn qkey() -> QueueKey {
    QueueKey::new(tenant(), queue())
}
pub fn shard() -> QueueKey {
    QueueKey::new(tenant(), queue())
}
pub fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

pub fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: tenant(),
        queue_id: queue(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

pub fn item(id: &str, key: &str, priority: i64) -> PushItem {
    item_max(id, key, priority, 3)
}

/// Like [`item`] but with an explicit retry bound (for the retry-exhaustion scenario).
pub fn item_max(id: &str, key: &str, priority: i64, max_attempts: u32) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: None,
        max_attempts,
        payload: None,
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity_document: None,
    }
}

pub fn envelope(command: QueueCommand, item_ids: Vec<ItemId>) -> CommandEnvelope {
    static NEXT_COMMAND_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    CommandEnvelope {
        command_id: CommandId::new(format!(
            "conformance-{}",
            NEXT_COMMAND_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

pub fn claim_req(max_items: usize, lease_expires_at: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(lease_expires_at),
        now: ts(now),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

/// A claim that resolves due-ness at an explicit `eligibility_time` while `now` / `lease_expires_at` stay
/// on the operational clock (`ClaimRequest::eligibility_at`). Selecting scheduled work at one epoch must
/// never back-date the lease taken at another, so the two are passed separately.
pub fn claim_req_at(
    max_items: usize,
    lease_expires_at: i64,
    now: i64,
    eligibility_time: i64,
) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: Some(ts(eligibility_time)),
        ..claim_req(max_items, lease_expires_at, now)
    }
}

/// Apply a command through the atomic unit of work (append + apply together). Stamps the queue's current
/// `assignment_epoch` (the in-process owner is always current — never self-fences; BQ-20).
pub async fn commit<B: Backend + ControlPlaneStore>(backend: &B, env: CommandEnvelope) {
    let epoch = backend
        .current_epoch(&shard())
        .await
        .expect("current epoch");
    backend
        .commit_raw(RawCommitRequest::new(shard(), vec![env], epoch))
        .await
        .expect("commit");
}

// ---------------------------------------------------------------------------
// Aggregate runner + macro
// ---------------------------------------------------------------------------

/// Run every conformance scenario against fresh backends built by `make`. Panics on the first failure.
pub async fn run_conformance<B: ConformanceBackend>(make: impl Fn() -> B) {
    let gate_capable = make().supports_gates();
    scenarios::push_then_select_eligible_in_priority_order(&make).await;
    scenarios::claim_then_complete_lifecycle(&make).await;
    scenarios::replace_pending_supersedes_old(&make).await;
    scenarios::high_water_is_monotonic(&make).await;
    scenarios::claim_returns_priority_ordered_rich_items(&make).await;
    scenarios::claim_empty_when_nothing_eligible(&make).await;
    scenarios::claim_with_explicit_eligibility_time(&make).await;
    if gate_capable {
        scenarios::claimed_item_shape_includes_payload_fields_and_gate_keys(&make).await;
    }
    claimed_item_shape_reflects_update_fields_after_reclaim_if_supported(&make).await;
    scenarios::claimed_item_shape_omits_empty_conditionals(&make).await;
    scenarios::structured_live_items_are_ordered_and_only_live(&make).await;
    scenarios::upsert_inserts_then_replaces_pending(&make).await;
    scenarios::upsert_rejects_claimed_and_terminal(&make).await;
    scenarios::upsert_preserves_group_delay_and_payload_in_claim_shape(&make).await;
    scenarios::tick_reclaims_expired_lease_with_no_client_traffic(&make).await;
    scenarios::tick_lease_boundary_is_half_open(&make).await;
    scenarios::paused_queue_yields_no_claims(&make).await;
    scenarios::fenced_lease_finalize_is_stale(&make).await;
    scenarios::renew_extends_lease_and_rejects(&make).await;
    scenarios::reassign_swaps_token_and_charges_attempt(&make).await;
    scenarios::claimed_view_renders_leased_items(&make).await;
    scenarios::purge_removes_present_items_and_gates_leased(&make).await;
    scenarios::retry_beyond_max_attempts_goes_terminal(&make).await;
    scenarios::retry_with_backoff_defers_eligibility(&make).await;
    scenarios::finalize_of_nonleased_item_is_rejected_without_appending(&make).await;
    scenarios::pause_and_fence_reconstruct_from_log(&make).await;
    scenarios::high_water_advances_on_each_commit(&make).await;
    scenarios::peek_is_priority_ordered_and_nondestructive(&make).await;
    scenarios::pending_lists_leased_items(&make).await;
    scenarios::snapshots_write_read_latest(&make).await;
    scenarios::rejected_mutations_do_not_append_commands(&make).await;
    scenarios::claim_compatibility_is_resolved_and_gated(&make).await;
    scenarios::stale_epoch_append_is_fenced(&make).await;
    scenarios::epoch_fence_closes_pre_segment_window(&make).await;
}

pub async fn claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported<
    B: ConformanceCore,
>(
    make: impl Fn() -> B,
) {
    if make().supports_gates() {
        scenarios::claimed_item_shape_includes_payload_fields_and_gate_keys(&make).await;
    }
}

pub async fn claimed_item_shape_reflects_update_fields_after_reclaim_if_supported<
    B: ConformanceCore,
>(
    make: impl Fn() -> B,
) -> bool {
    let caps = make().commit_capabilities();
    if !caps.is_atomic() {
        return false;
    }

    scenarios::claimed_item_shape_reflects_update_fields_after_reclaim(&make).await;
    true
}

// ---------------------------------------------------------------------------
// Conformance classes (ADR-008 §2 / TD-001): core (every family) + log-replay
// (log-bearing) + relational-reconnect (DB-authoritative). Backends compose the
// classes that match their durability + recovery model.
// ---------------------------------------------------------------------------

/// API-001 claimed-item response-shape conformance. The `@core` arm applies to every claim-capable
/// backend; the `@whole_cohort` arm is opt-in because log-replay backends intentionally reject
/// non-item claim units until cohort compatibility is implemented there.
#[macro_export]
macro_rules! claimed_item_shape_conformance_tests {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            claimed_item_shape_omits_empty_conditionals,
        );
        #[tokio::test]
        async fn claimed_item_shape_reflects_update_fields_after_reclaim() {
            $crate::claimed_item_shape_reflects_update_fields_after_reclaim_if_supported($make).await;
        }
        #[tokio::test]
        async fn claimed_item_shape_includes_payload_fields_and_gate_keys() {
            $crate::claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported($make).await;
        }
    };
    (@core $make:expr) => {
        $crate::claimed_item_shape_conformance_tests!($make);
    };
    (@whole_cohort $make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            claimed_item_shape_whole_cohort_omits_per_item_lease_token,
        );
    };
}

/// **Core** scenario class — substrate-independent behavior every projection family must satisfy
/// (ordering, eligibility, claim atomicity, idempotency, lease/epoch fencing, the per-queue progress
/// bound). Both durability variants exercise the three `UpsertPort` scenarios and the raw
/// `ReplacePending` command and field-mutation scenarios. Bounded by [`ConformanceCore`] — no
/// `LogRead`/`SnapshotStore` required.
#[macro_export]
macro_rules! core_suite {
    (@atomic $make:expr) => {
        $crate::claimed_item_shape_conformance_tests!($make);
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            claim_with_explicit_eligibility_time,
            structured_live_items_are_ordered_and_only_live,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            claimed_view_renders_leased_items,
            retry_beyond_max_attempts_goes_terminal,
            retry_with_backoff_defers_eligibility,
            peek_is_priority_ordered_and_nondestructive,
            pending_lists_leased_items,
            renew_extends_lease_and_rejects,
            reassign_swaps_token_and_charges_attempt,
            purge_removes_present_items_and_gates_leased,
            finalize_of_nonleased_item_is_rejected_without_appending,
            replace_pending_supersedes_old,
            upsert_inserts_then_replaces_pending,
            upsert_rejects_claimed_and_terminal,
            upsert_preserves_group_delay_and_payload_in_claim_shape,
            update_fields_merges_and_cas,
            reclaim_expired_sweeps_per_queue,
            claim_compatibility_is_resolved_and_gated,
            successful_push_is_visible_before_response_returns,
            rejected_finalize_leaves_visible_state_unchanged,
            request_id_push_replays_once_and_conflicts_on_body_change,
            stale_epoch_append_is_fenced,
            epoch_fence_closes_pre_segment_window,
        );
    };
    (@eventual $make:expr) => {
        $crate::claimed_item_shape_conformance_tests!($make);
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            claim_with_explicit_eligibility_time,
            structured_live_items_are_ordered_and_only_live,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            claimed_view_renders_leased_items,
            retry_beyond_max_attempts_goes_terminal,
            retry_with_backoff_defers_eligibility,
            peek_is_priority_ordered_and_nondestructive,
            pending_lists_leased_items,
            renew_extends_lease_and_rejects,
            reassign_swaps_token_and_charges_attempt,
            purge_removes_present_items_and_gates_leased,
            finalize_of_nonleased_item_is_rejected_without_appending,
            replace_pending_supersedes_old,
            upsert_inserts_then_replaces_pending,
            upsert_rejects_claimed_and_terminal,
            upsert_preserves_group_delay_and_payload_in_claim_shape,
            update_fields_merges_and_cas,
            reclaim_expired_sweeps_per_queue,
            claim_compatibility_is_resolved_and_gated,
            successful_push_is_visible_before_response_returns,
            rejected_finalize_leaves_visible_state_unchanged,
            request_id_push_replays_once_and_conflicts_on_body_change,
            stale_epoch_append_is_fenced,
            epoch_fence_closes_pre_segment_window,
        );
    };
}

/// **Log-replay** scenario class — for log-bearing backends ([`ConformanceBackend`]). Replay
/// reconstruction, snapshot round-trip, command-position high-water, and the log-tail "rejected mutation
/// appends nothing" durability guarantee (`rejected_mutations_do_not_append_commands`).
///
/// The *behavioral* lease/purge/finalize obligations themselves — renew, reassign, purge, finalize, and
/// their structured rejections — are **core** (TD-001: lease/epoch fencing is the core class, every
/// family), asserted via projection-read ports in `core_suite!`. Only the log-tail no-append check (which
/// needs `LogRead`) lives here.
#[macro_export]
macro_rules! log_replay_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            high_water_is_monotonic,
            high_water_advances_on_each_commit,
            snapshots_write_read_latest,
            pause_and_fence_reconstruct_from_log,
            rejected_mutations_do_not_append_commands,
        );
    };
}

/// Durable reconnect scenario class: the external transaction-integrity contract every durable profile
/// must present after reopen. Accepted mutations survive restart; rejected mutations do not become phantom
/// durable commits; lifecycle state remains observable through the same ports.
///
/// Bounded by [`ConformanceCore`]; invoked only by a durable backend whose `make` reopens shared state.
#[macro_export]
macro_rules! durable_reconnect_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            reconnect_after_crash_preserves_committed_state,
            reconnect_preserves_terminal_and_pending_state,
            reconnect_preserves_leased_item_state,
            reconnect_after_rejected_mutation_has_no_phantom_commit,
        );
    };
}

/// Backward-compatible name for DB-authoritative relational backends. New durable backends should prefer
/// [`durable_reconnect_suite`].
#[macro_export]
macro_rules! relational_reconnect_suite {
    ($make:expr) => {
        $crate::durable_reconnect_suite!($make);
    };
}

/// ADR-011 typed entity-schema and typed secondary-index scenario class. Backends that persist typed
/// entity documents and implement `IndexQueryPort` should run this alongside their core suite. Durable
/// reopen/replay mechanics remain backend-specific because each adapter owns how its factory reopens
/// shared state.
#[macro_export]
macro_rules! adr011_typed_conformance_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            adr011_schema_validation_rejects_before_visible_state,
            adr011_typed_scalar_and_compound_indexes_work,
            adr011_typed_missing_fields_remain_sparse,
            adr011_typed_unique_conflicts_are_atomic,
            adr011_typed_update_fields_unique_conflict_is_atomic,
            adr011_typed_update_fields_and_replace_rekey,
            adr011_typed_purge_frees_unique_key,
            adr011_typed_upsert_insert_unique_conflict_is_atomic,
            adr011_typed_schema_less_queue_unaffected,
        );
    };
}

/// ADR-011 log-replay scenario class. Log-bearing typed-index backends run this to prove typed index
/// rows are reconstructed from committed commands, while DB-authoritative relational reconnect remains
/// covered by adapter-specific durable-reopen tests.
#[macro_export]
macro_rules! adr011_typed_log_replay_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            adr011_typed_log_replay_reconstructs_index_rows,
        );
    };
}

/// Generate one `#[tokio::test]` per conformance scenario for the backend built by `$make`. The atomic
/// log-bearing suite = `core_suite!(@atomic)` + `log_replay_suite!`; this composes them so existing
/// adapters invoke the same one-liner unchanged. Invoke at module scope:
/// `fireweed_conformance::conformance_suite!(MyBackend::new);`.
#[macro_export]
macro_rules! conformance_suite {
    ($make:expr) => {
        $crate::core_suite!(@atomic $make);
        $crate::log_replay_suite!($make);
    };
    (@scenarios $make:expr, $($name:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                $crate::scenarios::$name($make).await;
            }
        )+
    };
}

/// The conformance suite for an **eventual-apply** log-bearing backend (e.g. objectlog):
/// `core_suite!(@eventual)` (the same inherent operation surface, including upsert and field mutation) +
/// `log_replay_suite!`. Invoke at module scope:
/// `fireweed_conformance::eventual_apply_suite!(MyBackend::new);`.
#[macro_export]
macro_rules! eventual_apply_suite {
    ($make:expr) => {
        $crate::core_suite!(@eventual $make);
        $crate::log_replay_suite!($make);
    };
}

#[cfg(test)]
mod storage_conformance {
    use fireweed_engine::{EngineError, compile_entity_schema, validate_entity};
    use serde_json::json;

    #[test]
    fn storage_conformance_entity_schema_rejects_missing_required() {
        let schema = json!({"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}});
        let cs = compile_entity_schema(&schema).expect("compiles");
        let doc = json!({"age": 42});
        let err = validate_entity(Some(&cs), Some(&doc)).unwrap_err();
        assert!(
            matches!(err, EngineError::EntitySchemaViolation(_)),
            "expected EntitySchemaViolation, got {err:?}"
        );
    }

    #[test]
    fn storage_conformance_entity_schema_accepts_valid_document() {
        let schema = json!({"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}});
        let cs = compile_entity_schema(&schema).expect("compiles");
        let doc = json!({"name": "alice"});
        validate_entity(Some(&cs), Some(&doc)).expect("valid document should pass");
    }

    #[test]
    fn storage_conformance_no_schema_accepts_anything() {
        let doc = json!({"whatever": true});
        validate_entity(None, Some(&doc)).expect("no schema — always ok");
        validate_entity(None, None).expect("no schema, no doc — always ok");
    }

    #[test]
    fn storage_conformance_entity_schema_error_has_resp_token() {
        let schema = json!({"type": "object", "required": ["x"]});
        let cs = compile_entity_schema(&schema).expect("compiles");
        let err = validate_entity(Some(&cs), Some(&json!({}))).unwrap_err();
        assert_eq!(
            err.resp_token(),
            Some("-ERR fireweed entity_schema_violation"),
            "RESP token must match ADR-011"
        );
    }

    #[test]
    fn storage_conformance_schema_compile_error_on_invalid_schema() {
        let bad = json!({"type": "not-a-valid-type"});
        let result = compile_entity_schema(&bad);
        assert!(result.is_err(), "invalid schema must fail compilation");
    }
}

#[cfg(test)]
mod capability_gate_tests {
    use super::*;
    use bytes::Bytes;
    use fireweed_engine::{PayloadUpdate, PushPort, UpdateFieldsPort};

    fn objectlog_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "fireweed-conformance-capability-gate-{}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn test_composed_atomic_update_fields_executes() {
        let b = fireweed_memory::composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(
                &shard(),
                vec![fireweed_engine::PushSpec {
                    client_item_key: Some(
                        fireweed_core::ClientItemKey::new("ka").expect("client item key"),
                    ),
                    priority: Some(fireweed_core::PriorityValue::Int64(5)),
                    payload: Some(Bytes::from_static(b"opaque-payload")),
                    fields: Default::default(),
                    ..Default::default()
                }],
                ts(0),
                None,
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);

        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let item_id = claimed.items[0].item_id;
        let version = claimed.items[0].item_version;

        let updated_version = b
            .update_fields(
                &shard(),
                item_id,
                std::collections::BTreeMap::from([(
                    "field-a".to_string(),
                    Some(Bytes::from_static(b"value-a-2")),
                )]),
                PayloadUpdate::Set(Some(Bytes::from_static(b"opaque-payload-v2"))),
                None,
                Some(version),
                ts(120),
                None,
            )
            .await
            .unwrap();
        assert!(updated_version > version);

        let live_claim = b.claimed_view(&shard(), &[item_id]).await.unwrap();
        assert_eq!(live_claim.len(), 1);
        assert_eq!(
            live_claim[0].payload.as_deref(),
            Some(&b"opaque-payload-v2"[..])
        );
    }

    #[tokio::test]
    async fn test_capability_gate_uses_advertised_atomicity() {
        let composed = fireweed_memory::composed_memory_backend();
        assert!(composed.commit_capabilities().is_atomic());

        let _ = std::fs::remove_dir_all(objectlog_root());
        let ran = claimed_item_shape_reflects_update_fields_after_reclaim_if_supported(|| {
            fireweed_objectlog::composed_objectlog_backend(objectlog_root())
                .expect("compose objectlog backend")
        })
        .await;
        assert!(
            ran,
            "the native object-log strict response barrier must run atomic-only scenarios"
        );
    }
}
