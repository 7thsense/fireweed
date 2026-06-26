#![forbid(unsafe_code)]
//! # pqueue-conformance
//!
//! The backend-conformance harness: the behavioral **no-stub** suite (plan §6) factored out of any one
//! adapter so every driven backend runs the SAME tests. Each scenario fails if a port returns a
//! default/no-op, proving the adapter actually implements the behavior.
//!
//! Usage from an adapter's tests (one line, full per-scenario granularity):
//!
//! ```ignore
//! pqueue_conformance::conformance_suite!(MyBackend::new);
//! ```
//!
//! or a single aggregate test:
//!
//! ```ignore
//! #[tokio::test]
//! async fn conformance() { pqueue_conformance::run_conformance(MyBackend::new).await; }
//! ```
//!
//! Each scenario takes a `make: impl Fn() -> B` factory (not a constructed backend) because some
//! scenarios build a SECOND fresh backend — e.g. log-replay reconstruction.
//!
//! Scope: this harness exercises only the engine **ports** ([`ConformanceBackend`]); white-box tests
//! that inspect an adapter's private state (e.g. item_version straight from the projection, or
//! simulating log compaction) stay in that adapter's own crate.
//!
//! ## Conformance matrix (BQ-13 / ADR-008 §2 / TD-001 capability classes)
//!
//! Every backend runs the classes its durability + recovery model supports. The CORE class is the SAME
//! scenario set for ALL backends (`core_suite!(@atomic)` or its `@eventual` upsert variant), which is what
//! holds the two projection families behaviorally identical on core.
//!
//! | Backend | Projection family | Durability | Class wiring | Where |
//! |---|---|---|---|---|
//! | `pqueue_memory::MemoryBackend` | in-memory log-replay | atomic | `conformance_suite!` = core@atomic + log-replay | `pqueue-memory/src/tests.rs` |
//! | `pqueue_sqlite::SqliteBackend` (log) | in-memory log-replay | atomic | `conformance_suite!` + `relational_reconnect_suite!` | `pqueue-sqlite/tests/{conformance,reconnect_smoke}.rs` |
//! | `pqueue_postgres::PostgresBackend` (log) | in-memory log-replay | atomic | core@atomic + log (env-gated `pg_conformance!`) | `pqueue-postgres/tests/conformance.rs` |
//! | `pqueue_objectlog` | log-bearing | eventual-apply | `eventual_apply_suite!` = core@eventual + log-replay | `pqueue-objectlog/tests/conformance.rs` |
//! | `pqueue_sqlite::SqliteRelationalBackend` | relational (DB-authoritative) | atomic | `core_suite!(@atomic)` + `relational_reconnect_suite!` | `pqueue-sqlite/tests/relational_{conformance,reconnect}.rs` |
//! | `pqueue_postgres::PostgresRelationalBackend` | relational (DB-authoritative) | atomic | core@atomic + relational-reconnect (env-gated) | `pqueue-postgres/tests/relational_conformance.rs` |
//!
//! Relational-only features (`pqueue_group_summary`, `pqueue_item_key_retention`) are deliberately OUT of
//! the shared CORE class so the families stay identical on it. The two families are additionally held
//! identical HEAD-TO-HEAD by [`scenarios::cross_family_core_parity`] (run sqlite-relational vs in-memory in
//! `pqueue-sqlite/tests/cross_family_parity.rs`). PARITY EVIDENCE STATUS: sqlite-relational-vs-in-memory is
//! validated locally; the postgres-relational half runs the identical class wiring but its live-DB
//! evidence is env-gated on `PQUEUE_PG_TEST_URL` and deferred-with-reason where no database is present
//! (convergence-review I3).

use pqueue_core::{
    ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, CommandChecksum, CommandEnvelope,
    CommandId, ControlPlaneStore, FinalizePort, LogRead, ProjectionRead, PurgePort, PushItem,
    QueueCommand, QueueKey, ReassignLeasePort, ReclaimDriver, RenewLeasePort, SnapshotStore,
    UpsertPort,
};

pub mod scenarios;

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
    + ClaimPort
    + UpsertPort
    + FinalizePort
    + RenewLeasePort
    + ReassignLeasePort
    + PurgePort
    + ReclaimDriver
{
}

impl<T> ConformanceCore for T where
    T: Backend
        + ControlPlaneStore
        + ProjectionRead
        + ClaimPort
        + UpsertPort
        + FinalizePort
        + RenewLeasePort
        + ReassignLeasePort
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
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
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
        cohort_size: None,
        gate_keys: Vec::new(),
    }
}

pub fn envelope(command: QueueCommand, item_ids: Vec<ItemId>) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("c"),
        request_id: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

pub fn claim_req(max_items: usize, lease_expires_at: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(lease_expires_at),
        now: ts(now),
        compatibility: ClaimCompatibility::default(),
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
        .write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .expect("commit");
}

// ---------------------------------------------------------------------------
// Aggregate runner + macro
// ---------------------------------------------------------------------------

/// Run every conformance scenario against fresh backends built by `make`. Panics on the first failure.
pub async fn run_conformance<B: ConformanceBackend>(make: impl Fn() -> B) {
    scenarios::push_then_select_eligible_in_priority_order(&make).await;
    scenarios::claim_then_complete_lifecycle(&make).await;
    scenarios::replace_pending_supersedes_old(&make).await;
    scenarios::high_water_is_monotonic(&make).await;
    scenarios::claim_returns_priority_ordered_rich_items(&make).await;
    scenarios::claim_empty_when_nothing_eligible(&make).await;
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

// ---------------------------------------------------------------------------
// Conformance classes (ADR-008 §2 / TD-001): core (every family) + log-replay
// (log-bearing) + relational-reconnect (DB-authoritative). Backends compose the
// classes that match their durability + recovery model.
// ---------------------------------------------------------------------------

/// **Core** scenario class — substrate-independent behavior every projection family must satisfy
/// (ordering, eligibility, claim atomicity, idempotency, lease/epoch fencing, the per-queue progress
/// bound). Two durability variants on the one upsert axis (TD-007 §2.3): `@atomic` includes the three
/// `UpsertPort` scenarios + the raw `ReplacePending` command; `@eventual` substitutes
/// `upsert_is_unavailable`. Bounded by [`ConformanceCore`] — no `LogRead`/`SnapshotStore` required.
#[macro_export]
macro_rules! core_suite {
    (@atomic $make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            claimed_view_renders_leased_items,
            retry_beyond_max_attempts_goes_terminal,
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
            claim_compatibility_is_resolved_and_gated,
            stale_epoch_append_is_fenced,
            epoch_fence_closes_pre_segment_window,
        );
    };
    (@eventual $make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            claimed_view_renders_leased_items,
            retry_beyond_max_attempts_goes_terminal,
            peek_is_priority_ordered_and_nondestructive,
            pending_lists_leased_items,
            renew_extends_lease_and_rejects,
            reassign_swaps_token_and_charges_attempt,
            purge_removes_present_items_and_gates_leased,
            finalize_of_nonleased_item_is_rejected_without_appending,
            upsert_is_unavailable,
            claim_compatibility_is_resolved_and_gated,
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

/// **Relational-reconnect** scenario class — the relational substitute for log-replay (ADR-008 §2): a
/// DB-authoritative projection that survives a process restart via reopen-the-store, no log replay.
/// Bounded by [`ConformanceCore`]; invoked only by a durable backend whose `make` reopens shared state.
#[macro_export]
macro_rules! relational_reconnect_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            reconnect_after_crash_preserves_committed_state,
            reconnect_preserves_terminal_and_pending_state,
            reconnect_preserves_leased_item_state,
        );
    };
}

/// Generate one `#[tokio::test]` per conformance scenario for the backend built by `$make`. The atomic
/// log-bearing suite = `core_suite!(@atomic)` + `log_replay_suite!`; this composes them so existing
/// adapters invoke the same one-liner unchanged. Invoke at module scope:
/// `pqueue_conformance::conformance_suite!(MyBackend::new);`.
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
/// `core_suite!(@eventual)` (upsert refused) + `log_replay_suite!`. Invoke at module scope:
/// `pqueue_conformance::eventual_apply_suite!(MyBackend::new);`.
#[macro_export]
macro_rules! eventual_apply_suite {
    ($make:expr) => {
        $crate::core_suite!(@eventual $make);
        $crate::log_replay_suite!($make);
    };
}
