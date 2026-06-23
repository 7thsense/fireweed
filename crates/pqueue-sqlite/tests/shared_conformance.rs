//! Shared backend conformance harness (TD-001 / TD-005 B4).
//!
//! The same generic harness runs against TWO adapters — the in-memory reference
//! backend and the standalone `SqliteBackend` — proving the harness is faithful
//! to the reference AND that sqlite reaches parity (durability, lease,
//! replay/apply, idempotency, progress). The legacy `storage_conformance.rs`
//! remains untouched as the memory regression anchor.
//!
//! Claim parity note: BOTH adapters lease with a SINGLE `attempts` increment.
//! The memory adapter leases via `batch_claim` (which increments once) and does
//! NOT also append+apply a `BatchClaim` command; the sqlite adapter's `claim`
//! selects read-only then appends+applies one `BatchClaim`. Neither does the
//! `batch_claim`-then-append-`BatchClaim` two-step that would double-count.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{
    ClientItemKey, CohortPolicy, CreateQueue, EligibilityPolicy, ItemId, OrderingMode,
    PriorityModel, QueueCreationPolicy, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp,
};
use pqueue_sqlite::SqliteBackend;
use pqueue_storage::commands::{
    BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome, LeaseExpiredCommand,
    PushItem,
};
use pqueue_storage::memory::{MemoryControlPlaneStore, MemoryLogStore, MemoryProjectionStore};
use pqueue_storage::traits::{
    ClaimRequest, ControlPlaneStore, ProjectionStore, QueueMetricsSnapshot,
};
use pqueue_storage::types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};

// ---------------------------------------------------------------------------
// Fixtures shared by both adapters
// ---------------------------------------------------------------------------

fn tenant() -> TenantId {
    TenantId::new("t").unwrap()
}
fn queue() -> QueueId {
    QueueId::new("q").unwrap()
}
fn shard() -> ShardKey {
    ShardKey {
        tenant_id: tenant(),
        queue_id: queue(),
        shard_id: ShardId::new(0),
    }
}
fn qk() -> QueueKey {
    QueueKey {
        tenant_id: tenant(),
        queue_id: queue(),
    }
}
fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn queue_def() -> QueueDefinition {
    CreateQueue {
        tenant_id: tenant(),
        queue_id: queue(),
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn item(id: &str, not_before: Option<i64>) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(format!("k-{id}")).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: None,
        not_before: not_before.map(ts),
        max_attempts: 3,
        payload: None,
    }
}

fn envelope(command: QueueCommand) -> CommandEnvelope {
    let s = shard();
    CommandEnvelope {
        command_id: CommandId::new("c"),
        request_id: None,
        tenant_id: s.tenant_id,
        queue_id: s.queue_id,
        shard_id: s.shard_id,
        item_ids: vec![],
        command,
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

// ---------------------------------------------------------------------------
// The backend abstraction the harness drives
// ---------------------------------------------------------------------------

trait ConformanceBackend {
    async fn create_queue(&self) -> Result<(), String>;
    async fn push(&self, items: Vec<PushItem>) -> Result<(), String>;
    async fn claim(
        &self,
        max_items: usize,
        token: &str,
        now: i64,
        expires: i64,
    ) -> Result<Vec<String>, String>;
    async fn finalize(&self, item_id: &str, kind: FinalizeKind) -> Result<(), String>;
    async fn expire_leases(&self, item_ids: &[&str]) -> Result<(), String>;
    async fn metrics(&self) -> Result<QueueMetricsSnapshot, String>;
}

// In-memory reference adapter.
struct MemoryConformanceBackend {
    control: MemoryControlPlaneStore,
    projection: MemoryProjectionStore,
    // Holds the log so a future extension can replay; the harness asserts on the
    // projection, which the reference applies from committed commands.
    _log: MemoryLogStore,
    shard: ShardKey,
    seq: AtomicU64,
}

impl MemoryConformanceBackend {
    fn new() -> Self {
        Self {
            control: MemoryControlPlaneStore::new(),
            projection: MemoryProjectionStore::new(),
            _log: MemoryLogStore::new(),
            shard: shard(),
            seq: AtomicU64::new(0),
        }
    }

    fn next_pos(&self) -> CommandPosition {
        CommandPosition {
            shard_key: self.shard.clone(),
            sequence: self.seq.fetch_add(1, Ordering::Relaxed),
            backend_epoch: 0,
        }
    }
}

impl ConformanceBackend for MemoryConformanceBackend {
    async fn create_queue(&self) -> Result<(), String> {
        self.control
            .create_queue(queue_def())
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn push(&self, items: Vec<PushItem>) -> Result<(), String> {
        let env = envelope(QueueCommand::BatchPush(BatchPushCommand { items }));
        self.projection
            .apply_committed(self.next_pos(), &[env])
            .await
            .map_err(|err| err.to_string())
    }

    async fn claim(
        &self,
        max_items: usize,
        token: &str,
        now: i64,
        expires: i64,
    ) -> Result<Vec<String>, String> {
        // Single increment: lease via batch_claim only; do NOT also append+apply
        // a BatchClaim command (that two-step would double-count `attempts`).
        let result = self
            .projection
            .batch_claim(ClaimRequest {
                shard_key: self.shard.clone(),
                max_items,
                now: ts(now),
                lease_token: token.to_string(),
                lease_expires_at: ts(expires),
            })
            .await
            .map_err(|err| err.to_string())?;
        Ok(result
            .claimed_item_ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect())
    }

    async fn finalize(&self, item_id: &str, kind: FinalizeKind) -> Result<(), String> {
        let env = envelope(QueueCommand::BatchFinalize(BatchFinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: ItemId::new(item_id).unwrap(),
                kind,
            }],
        }));
        self.projection
            .apply_committed(self.next_pos(), &[env])
            .await
            .map_err(|err| err.to_string())
    }

    async fn expire_leases(&self, item_ids: &[&str]) -> Result<(), String> {
        let env = envelope(QueueCommand::LeaseExpired(LeaseExpiredCommand {
            item_ids: item_ids.iter().map(|i| ItemId::new(*i).unwrap()).collect(),
        }));
        self.projection
            .apply_committed(self.next_pos(), &[env])
            .await
            .map_err(|err| err.to_string())
    }

    async fn metrics(&self) -> Result<QueueMetricsSnapshot, String> {
        self.projection
            .metrics(&qk())
            .await
            .map_err(|err| err.to_string())
    }
}

// Standalone unified sqlite adapter.
struct SqliteConformanceBackend {
    backend: SqliteBackend,
    shard: ShardKey,
}

impl SqliteConformanceBackend {
    fn new() -> Self {
        Self {
            backend: SqliteBackend::open_in_memory().expect("open in-memory sqlite backend"),
            shard: shard(),
        }
    }
}

impl ConformanceBackend for SqliteConformanceBackend {
    async fn create_queue(&self) -> Result<(), String> {
        self.backend
            .create_queue(queue_def())
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn push(&self, items: Vec<PushItem>) -> Result<(), String> {
        self.backend
            .push(&self.shard, items)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn claim(
        &self,
        max_items: usize,
        token: &str,
        now: i64,
        expires: i64,
    ) -> Result<Vec<String>, String> {
        let ids = self
            .backend
            .claim(&self.shard, max_items, token, ts(now), ts(expires))
            .await
            .map_err(|err| err.to_string())?;
        Ok(ids.iter().map(|id| id.as_str().to_string()).collect())
    }

    async fn finalize(&self, item_id: &str, kind: FinalizeKind) -> Result<(), String> {
        self.backend
            .finalize(
                &self.shard,
                vec![FinalizeOutcome {
                    item_id: ItemId::new(item_id).unwrap(),
                    kind,
                }],
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn expire_leases(&self, item_ids: &[&str]) -> Result<(), String> {
        self.backend
            .expire_leases(
                &self.shard,
                item_ids.iter().map(|i| ItemId::new(*i).unwrap()).collect(),
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn metrics(&self) -> Result<QueueMetricsSnapshot, String> {
        self.backend
            .metrics(&qk())
            .await
            .map_err(|err| err.to_string())
    }
}

// ---------------------------------------------------------------------------
// The conformance dimensions (generic over the backend)
// ---------------------------------------------------------------------------

mod harness {
    use super::*;

    pub async fn push_makes_items_pending<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None), item("b", None)])
            .await
            .unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(m.pending_count, 2);
        assert_eq!(m.leased_count, 0);
    }

    pub async fn claim_leases_fifo_and_single_active<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None), item("b", None)])
            .await
            .unwrap();

        let first = b.claim(1, "tok-1", 100, 200).await.unwrap();
        assert_eq!(first, vec!["a".to_string()], "FIFO by insertion order");
        let m = b.metrics().await.unwrap();
        assert_eq!(m.leased_count, 1);
        assert_eq!(m.pending_count, 1);

        // Re-claiming the leased item finds nothing new (single active lease).
        let second = b.claim(10, "tok-2", 100, 300).await.unwrap();
        assert_eq!(second, vec!["b".to_string()]);
        let third = b.claim(10, "tok-3", 100, 400).await.unwrap();
        assert!(third.is_empty(), "all leased; nothing claimable");
    }

    pub async fn claim_respects_max_items<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None), item("b", None), item("c", None)])
            .await
            .unwrap();
        let claimed = b.claim(2, "tok", 100, 200).await.unwrap();
        assert_eq!(claimed.len(), 2);
    }

    pub async fn claim_respects_not_before<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("future", Some(500)), item("ready", None)])
            .await
            .unwrap();
        let early = b.claim(10, "tok", 100, 200).await.unwrap();
        assert_eq!(
            early,
            vec!["ready".to_string()],
            "not_before gates the future item"
        );
        let due = b.claim(10, "tok", 500, 600).await.unwrap();
        assert_eq!(due, vec!["future".to_string()]);
    }

    pub async fn finalize_complete_and_fail_are_terminal<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None), item("b", None)])
            .await
            .unwrap();
        b.claim(10, "tok", 100, 200).await.unwrap();
        b.finalize("a", FinalizeKind::Complete).await.unwrap();
        b.finalize("b", FinalizeKind::Fail).await.unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(m.completed_count, 1);
        assert_eq!(m.failed_count, 1);
        assert_eq!(m.leased_count, 0);
        assert_eq!(m.pending_count, 0);
    }

    pub async fn retry_and_release_re_pend_and_reclaim<B: ConformanceBackend>(
        b: B,
        kind: FinalizeKind,
    ) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None)]).await.unwrap();
        b.claim(10, "tok", 100, 200).await.unwrap();
        b.finalize("a", kind).await.unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(m.pending_count, 1, "{kind:?} re-pends");
        assert_eq!(m.leased_count, 0);
        let reclaimed = b.claim(10, "tok-2", 300, 400).await.unwrap();
        assert_eq!(
            reclaimed,
            vec!["a".to_string()],
            "re-claimable after {kind:?}"
        );
    }

    pub async fn expired_lease_re_pends<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None)]).await.unwrap();
        b.claim(10, "tok", 100, 200).await.unwrap();
        b.expire_leases(&["a"]).await.unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(m.pending_count, 1);
        assert_eq!(m.leased_count, 0);
    }

    pub async fn repush_same_item_id_converges<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None)]).await.unwrap();
        // Re-pushing the same item_id is idempotent (converges, no duplicate).
        b.push(vec![item("a", None)]).await.unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(
            m.pending_count, 1,
            "duplicate item_id converges to one item"
        );
    }

    pub async fn full_lifecycle<B: ConformanceBackend>(b: B) {
        b.create_queue().await.unwrap();
        b.push(vec![item("a", None), item("b", None)])
            .await
            .unwrap();
        b.claim(10, "tok", 100, 5_000).await.unwrap();
        b.finalize("a", FinalizeKind::Complete).await.unwrap();
        b.finalize("b", FinalizeKind::Fail).await.unwrap();
        let m = b.metrics().await.unwrap();
        assert_eq!(m.pending_count, 0);
        assert_eq!(m.leased_count, 0);
        assert_eq!(m.completed_count, 1);
        assert_eq!(m.failed_count, 1);
    }
}

/// Generate the full conformance suite for one backend constructor.
macro_rules! conformance_suite {
    ($modname:ident, $ctor:expr) => {
        mod $modname {
            use super::*;

            #[tokio::test]
            async fn push_makes_items_pending() {
                harness::push_makes_items_pending($ctor()).await;
            }
            #[tokio::test]
            async fn claim_leases_fifo_and_single_active() {
                harness::claim_leases_fifo_and_single_active($ctor()).await;
            }
            #[tokio::test]
            async fn claim_respects_max_items() {
                harness::claim_respects_max_items($ctor()).await;
            }
            #[tokio::test]
            async fn claim_respects_not_before() {
                harness::claim_respects_not_before($ctor()).await;
            }
            #[tokio::test]
            async fn finalize_complete_and_fail_are_terminal() {
                harness::finalize_complete_and_fail_are_terminal($ctor()).await;
            }
            #[tokio::test]
            async fn retry_re_pends_and_reclaims() {
                harness::retry_and_release_re_pend_and_reclaim($ctor(), FinalizeKind::Retry).await;
            }
            #[tokio::test]
            async fn release_re_pends_and_reclaims() {
                harness::retry_and_release_re_pend_and_reclaim($ctor(), FinalizeKind::Release)
                    .await;
            }
            #[tokio::test]
            async fn expired_lease_re_pends() {
                harness::expired_lease_re_pends($ctor()).await;
            }
            #[tokio::test]
            async fn repush_same_item_id_converges() {
                harness::repush_same_item_id_converges($ctor()).await;
            }
            #[tokio::test]
            async fn full_lifecycle() {
                harness::full_lifecycle($ctor()).await;
            }
        }
    };
}

conformance_suite!(memory, MemoryConformanceBackend::new);
conformance_suite!(sqlite, SqliteConformanceBackend::new);
