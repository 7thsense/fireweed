//! Exact log-read, head-read and fence deltas. Never timings.
use super::*;
use fireweed_conformance::queue_contract::{self as suite, TestClock};
use fireweed_engine::queue_contract::QueueStore;

pub(super) struct Env {
    pub(super) root: std::path::PathBuf,
    pub(super) prefix: Option<String>,
    pub(super) clock: Arc<TestClock>,
    pub(super) ownership: TestOwnership,
    pub(super) nonce: u128,
}
impl Env {
    pub(super) fn new(name: &str, s3: bool) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("fireweed-counts-{name}-{s3}-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root,
            prefix: s3.then(|| format!("api007-counts-{name}-{nonce}")),
            clock: Arc::new(TestClock::default()),
            ownership: TestOwnership::new(),
            nonce,
        }
    }
    /// A handle named after its projection file; every handle shares the control plane.
    pub(super) async fn open(&self, projection: &str) -> NativeQueueStore {
        self.open_with(projection, self.ownership.handle(projection))
            .await
    }
    pub(super) async fn open_with(
        &self,
        projection: &str,
        ownership: QueueOwnership,
    ) -> NativeQueueStore {
        native_port_fixture_owned(
            &self.root,
            projection,
            self.prefix.as_deref(),
            self.clock.clone(),
            ownership,
        )
        .await
    }
    pub(super) fn address(&self, name: &str) -> QueueAddress {
        QueueAddress {
            tenant: "api007-counts".into(),
            queue: format!("{}-{name}", self.nonce),
        }
    }
}
impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
pub(super) fn now(store: &NativeQueueStore) -> RecoverySnapshot {
    store.recovery_diagnostics().snapshot()
}
/// (log records read, head reads, fences) since `before`.
pub(super) fn delta(store: &NativeQueueStore, before: RecoverySnapshot) -> (u64, u64, u64) {
    let after = now(store);
    (
        after.admitted_raw_records - before.admitted_raw_records,
        after.head_reads - before.head_reads,
        after.fence_acquires - before.fence_acquires,
    )
}
pub(super) async fn insert(store: &NativeQueueStore, queue: &QueueAddress, n: usize) {
    let outcome = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: format!("insert-{n}"),
            mode: InsertMode::InsertOnly,
            items: vec![suite::item(&format!("k-{n}"), "counts", 0, n as i64)],
        })
        .await
        .unwrap();
    assert!(matches!(outcome[0], InsertOutcome::Inserted { .. }));
}
pub(super) async fn commit(store: &NativeQueueStore, queue: &QueueAddress, n: usize) {
    let mut group = suite::group(&format!("commit-{n}"));
    group
        .work_inserts
        .push(suite::item(&format!("c-{n}"), "counts", 0, n as i64));
    let receipts = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![group],
        })
        .await
        .unwrap();
    assert!(matches!(
        receipts[0].outcome,
        GroupOutcome::Committed { .. }
    ));
}
/// Provision, then alternate inserts and commits, `len` operations in all. Returns
/// the number of log records that took: operations queued together share a
/// record, so it is at most `len`.
pub(super) async fn grow(store: &NativeQueueStore, queue: &QueueAddress, len: usize) -> u64 {
    let pipeline = store.pipeline_for_test();
    let published = &pipeline.published;
    let before = published.load(std::sync::atomic::Ordering::SeqCst);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    // Queued together the operations share loop iterations and records.
    let numbers: Vec<usize> = (1..len).collect();
    for chunk in numbers.chunks(50) {
        futures::future::join_all(chunk.iter().map(|&n| async move {
            if n % 2 == 0 {
                insert(store, queue, n).await;
            } else {
                commit(store, queue, n).await;
            }
        }))
        .await;
    }
    published.load(std::sync::atomic::Ordering::SeqCst) - before
}
pub(super) async fn reads(store: &NativeQueueStore, queue: &QueueAddress) {
    store
        .stats(StatsRequest {
            buckets: None,
            queue: queue.clone(),
            filter: suite::filter("counts"),
            group_by: vec![],
            eligible_at: 0,
        })
        .await
        .unwrap();
    store
        .query(QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "by_score".into(),
            filter: suite::filter("counts"),
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    store
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["absent".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["commit-1".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
}

async fn owner_write_reads_no_log(s3: bool, writes: usize) {
    let env = Env::new("write", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let before = now(&store);
    for n in 0..writes {
        if n % 2 == 0 {
            insert(&store, &queue, 1000 + n).await;
        } else {
            commit(&store, &queue, 1000 + n).await;
        }
    }
    assert_eq!(delta(&store, before), (0, 0, 0));
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_write_reads_no_log_local() {
    owner_write_reads_no_log(false, 1000).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_write_reads_no_log_real_s3() {
    owner_write_reads_no_log(true, 20).await;
}

/// After entry, neither writes nor the reads that follow them touch the log: no
/// record read, no head read, no fence. The first append of a tenure reads the
/// right spine of the log's index once, which these counters do not see.
async fn owner_writes_then_reads_cost_nothing(s3: bool, writes: usize) {
    let env = Env::new("write-read", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let before = now(&store);
    for n in 0..writes {
        if n % 2 == 0 {
            insert(&store, &queue, 3000 + n).await;
        } else {
            commit(&store, &queue, 3000 + n).await;
        }
        if n % 5 == 4 {
            reads(&store, &queue).await;
        }
    }
    reads(&store, &queue).await;
    let pending = super::owner_safety::pending_items(&store, &queue).await;
    assert_eq!(pending, 1 + writes as u64);
    assert_eq!(delta(&store, before), (0, 0, 0));
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_writes_then_reads_cost_nothing_local() {
    owner_writes_then_reads_cost_nothing(false, 100).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_writes_then_reads_cost_nothing_real_s3() {
    owner_writes_then_reads_cost_nothing(true, 20).await;
}

async fn owner_read_reads_no_log(s3: bool, lengths: &[usize]) {
    for &len in lengths {
        let env = Env::new("read", s3);
        let store = env.open("a.db").await;
        let queue = env.address("q");
        grow(&store, &queue, len).await;
        let before = now(&store);
        reads(&store, &queue).await;
        assert_eq!(delta(&store, before), (0, 0, 0), "log length {len}");
        store.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_read_reads_no_log_local() {
    owner_read_reads_no_log(false, &[10, 1000, 5000]).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_read_reads_no_log_real_s3() {
    owner_read_reads_no_log(true, &[10, 5000]).await;
}

async fn reopen_rebuilds_from_log(s3: bool, len: usize) {
    let env = Env::new("reopen", s3);
    let queue = env.address("q");
    let first = env.open("a.db").await;
    let log = grow(&first, &queue, len).await;
    first.shutdown().await.unwrap();
    drop(first);
    let second = env.open("a.db").await;
    let before = now(&second);
    reads(&second, &queue).await;
    let (records, _, _) = delta(&second, before);
    assert_eq!(records, log, "reopen reads the whole log once");
    let before = now(&second);
    reads(&second, &queue).await;
    insert(&second, &queue, 9999).await;
    assert_eq!(delta(&second, before), (0, 0, 0));
    second.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_rebuilds_from_log_local() {
    reopen_rebuilds_from_log(false, 200).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_rebuilds_from_log_real_s3() {
    reopen_rebuilds_from_log(true, 20).await;
}

async fn concurrent_operations_share_one_iteration(s3: bool, operations: usize) {
    let env = Env::new("concurrent", s3);
    let store = env.open("a.db").await;
    let queue = env.address("q");
    grow(&store, &queue, 2).await;
    let before = now(&store);
    let mut tasks = Vec::new();
    for n in 0..operations {
        let store = store.clone();
        let queue = queue.clone();
        tasks.push(tokio::spawn(async move {
            if n % 2 == 0 {
                insert(&store, &queue, 2000 + n).await;
            } else {
                reads(&store, &queue).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(delta(&store, before), (0, 0, 0));
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_operations_share_one_iteration_local() {
    concurrent_operations_share_one_iteration(false, 64).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_operations_share_one_iteration_real_s3() {
    concurrent_operations_share_one_iteration(true, 8).await;
}
