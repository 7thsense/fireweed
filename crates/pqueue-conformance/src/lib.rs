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

use pqueue_core::{
    ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimPort, ClaimRequest, CommandChecksum, CommandEnvelope, CommandId,
    ControlPlaneStore, FinalizePort, LogRead, ProjectionRead, PushItem, QueueCommand, QueueKey,
    ReassignLeasePort, ReclaimDriver, RenewLeasePort, ShardId, ShardKey, SnapshotStore, UpsertPort,
};

pub mod scenarios;

/// The umbrella bound for a conformance-testable backend: every engine port the suite exercises.
/// A blanket impl means any adapter implementing the listed ports is a `ConformanceBackend` for free.
pub trait ConformanceBackend:
    Backend
    + ControlPlaneStore
    + ProjectionRead
    + ClaimPort
    + UpsertPort
    + FinalizePort
    + RenewLeasePort
    + ReassignLeasePort
    + ReclaimDriver
    + SnapshotStore
    + LogRead
{
}

impl<T> ConformanceBackend for T where
    T: Backend
        + ControlPlaneStore
        + ProjectionRead
        + ClaimPort
        + UpsertPort
        + FinalizePort
        + RenewLeasePort
        + ReassignLeasePort
        + ReclaimDriver
        + SnapshotStore
        + LogRead
{
}

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
pub fn shard() -> ShardKey {
    ShardKey::new(tenant(), queue(), ShardId::ZERO)
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
        group_co_residency: false,
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
        shard_count: 1,
    }
}

pub fn item(id: &str, key: &str, priority: i64) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: None,
        max_attempts: 3,
        payload: None,
    }
}

pub fn envelope(command: QueueCommand, item_ids: Vec<ItemId>) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("c"),
        request_id: None,
        shard_id: ShardId::ZERO,
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
    }
}

/// Apply a command through the atomic unit of work (append + apply together).
pub async fn commit<B: Backend>(backend: &B, env: CommandEnvelope) {
    backend
        .write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env))?;
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
    scenarios::finalize_of_nonleased_item_is_rejected_without_appending(&make).await;
    scenarios::pause_and_fence_reconstruct_from_log(&make).await;
    scenarios::high_water_advances_on_each_commit(&make).await;
    scenarios::peek_is_priority_ordered_and_nondestructive(&make).await;
    scenarios::pending_lists_leased_items(&make).await;
    scenarios::snapshots_write_read_latest(&make).await;
}

/// Generate one `#[tokio::test]` per conformance scenario for the backend built by `$make`. Invoke at
/// module scope in an adapter's test target: `pqueue_conformance::conformance_suite!(MyBackend::new);`.
#[macro_export]
macro_rules! conformance_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            replace_pending_supersedes_old,
            high_water_is_monotonic,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            upsert_inserts_then_replaces_pending,
            upsert_rejects_claimed_and_terminal,
            upsert_preserves_group_delay_and_payload_in_claim_shape,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            renew_extends_lease_and_rejects,
            reassign_swaps_token_and_charges_attempt,
            claimed_view_renders_leased_items,
            finalize_of_nonleased_item_is_rejected_without_appending,
            pause_and_fence_reconstruct_from_log,
            high_water_advances_on_each_commit,
            peek_is_priority_ordered_and_nondestructive,
            pending_lists_leased_items,
            snapshots_write_read_latest,
        );
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

/// Generate the conformance suite for an **eventual-apply** backend (e.g. objectlog): the atomic-class
/// scenarios MINUS the three `UpsertPort` scenarios and the raw `ReplacePending`-command scenario (the
/// atomic XDEL+XADD upsert is not offered on this class), PLUS [`scenarios::upsert_is_unavailable`]
/// asserting the refusal. Everything else (push/claim/finalize/reclaim/pause/fence/reads + log replay)
/// is identical to the atomic suite. Invoke at module scope:
/// `pqueue_conformance::eventual_apply_suite!(MyBackend::new);`.
#[macro_export]
macro_rules! eventual_apply_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@scenarios $make,
            push_then_select_eligible_in_priority_order,
            claim_then_complete_lifecycle,
            high_water_is_monotonic,
            claim_returns_priority_ordered_rich_items,
            claim_empty_when_nothing_eligible,
            upsert_is_unavailable,
            tick_reclaims_expired_lease_with_no_client_traffic,
            tick_lease_boundary_is_half_open,
            paused_queue_yields_no_claims,
            fenced_lease_finalize_is_stale,
            renew_extends_lease_and_rejects,
            reassign_swaps_token_and_charges_attempt,
            claimed_view_renders_leased_items,
            finalize_of_nonleased_item_is_rejected_without_appending,
            pause_and_fence_reconstruct_from_log,
            high_water_advances_on_each_commit,
            peek_is_priority_ordered_and_nondestructive,
            pending_lists_leased_items,
            snapshots_write_read_latest,
        );
    };
}
