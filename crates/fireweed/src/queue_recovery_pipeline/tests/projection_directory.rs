//! One projection database file per owned queue: queues publish in parallel, a
//! file lives exactly as long as its tenure, and nothing a previous process left
//! is read.
use super::owner_safety::{pending_items, stats, wait_for};
use super::ownership::TTL;
use super::record_counts::{Env, grow, insert};
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_engine::queue_contract::QueueStore;
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

/// The queue's projection file name stem, as the owner derives it.
fn stem(queue: &QueueAddress) -> String {
    owner::queue_key(queue)
        .unwrap()
        .queue_id
        .as_str()
        .to_owned()
}
/// Every file name in `directory`, sorted, with the directory lock's target.
fn all_names(directory: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}
/// As `all_names`, without the `.lock` file, which is not a projection file.
fn names(directory: &Path) -> Vec<String> {
    let mut names = all_names(directory);
    names.retain(|name| name != ".lock");
    names
}
fn projection_dir(env: &Env, name: &str) -> PathBuf {
    env.root.join(name)
}

/// While one queue's batch is held at its publish, an insert on another queue of
/// the same pipeline completes. Each queue has its own writer.
async fn queues_publish_in_parallel(s3: bool) {
    let env = Env::new("parallel", s3);
    let store = env.open("a.db").await;
    let (a, b) = (env.address("a"), env.address("b"));
    grow(&store, &a, 2).await;
    grow(&store, &b, 2).await;
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
    let fired = AtomicBool::new(false);
    *store.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        // Only the first publish is held: queue A's, which starts first.
        if stage == owner::Stage::BeforeAppend && !fired.swap(true, Ordering::SeqCst) {
            entered_tx.lock().unwrap().send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    }));
    let held = tokio::spawn({
        let (store, a) = (store.clone(), a.clone());
        async move { insert(&store, &a, 1000).await }
    });
    tokio::task::spawn_blocking(move || entered.recv().unwrap())
        .await
        .unwrap();
    // A holds its open transaction. B must not wait for it.
    tokio::time::timeout(Duration::from_secs(60), insert(&store, &b, 2000))
        .await
        .expect("an insert on another queue completes while A's batch is held");
    assert_eq!(pending_items(&store, &b).await, 2);
    assert!(!held.is_finished(), "A is still held at its publish");
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(60), held)
        .await
        .expect("A finishes once released")
        .unwrap();
    assert_eq!(pending_items(&store, &a).await, 2);
    *store.pipeline_for_test().on_stage.lock().unwrap() = None;
    store.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queues_publish_in_parallel_local() {
    queues_publish_in_parallel(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queues_publish_in_parallel_real_s3() {
    queues_publish_in_parallel(true).await;
}

/// Entering creates exactly the queue's file; exit, release and shutdown remove it.
async fn a_file_lives_exactly_as_long_as_its_tenure(s3: bool) {
    let env = Env::new("lifecycle", s3);
    let store = env.open("a.db").await;
    let dir = projection_dir(&env, "a.db");
    assert!(
        names(&dir).is_empty(),
        "construction creates the directory only"
    );
    let (a, b) = (env.address("a"), env.address("b"));
    // Release: a queue that does not exist holds nothing, not even a file.
    assert_eq!(
        stats(&store, &a).await.err(),
        Some(QueueError::QueueNotFound)
    );
    assert!(names(&dir).is_empty(), "the absent queue's file is gone");
    // Entry: one file for the queue, nothing for any other.
    grow(&store, &a, 2).await;
    let belongs = |name: &String, stem: &str| name.starts_with(&format!("{stem}.db"));
    let (sa, sb) = (stem(&a), stem(&b));
    let held = names(&dir);
    assert!(held.contains(&format!("{sa}.db")), "{held:?}");
    assert!(held.iter().all(|name| belongs(name, &sa)), "{held:?}");
    // A second queue gets a second file of its own.
    grow(&store, &b, 2).await;
    let held = names(&dir);
    assert!(held.contains(&format!("{sa}.db")) && held.contains(&format!("{sb}.db")));
    assert!(
        held.iter()
            .all(|name| belongs(name, &sa) || belongs(name, &sb)),
        "{held:?}"
    );
    // Exit: the lease lapses with the clock; the owner leaves and takes its files.
    env.ownership.advance(TTL + Duration::from_secs(1));
    wait_for("both files to go on exit", async || names(&dir).is_empty()).await;
    // Entry again rebuilds into a new file.
    assert_eq!(pending_items(&store, &a).await, 1);
    assert!(names(&dir).contains(&format!("{sa}.db")));
    // Shutdown: the directory holds no projection file.
    store.shutdown().await.unwrap();
    assert!(names(&dir).is_empty(), "{:?}", names(&dir));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_lives_exactly_as_long_as_its_tenure_local() {
    a_file_lives_exactly_as_long_as_its_tenure(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_lives_exactly_as_long_as_its_tenure_real_s3() {
    a_file_lives_exactly_as_long_as_its_tenure(true).await;
}

/// A failed entry takes its file with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_entry_leaves_no_file() {
    let env = Env::new("failed-entry", false);
    let store = env.open("a.db").await;
    let dir = projection_dir(&env, "a.db");
    let a = env.address("a");
    grow(&store, &a, 2).await;
    // The lease lapses and the control plane is down: re-entry fails after its
    // file was opened or before it, and either way nothing is left.
    env.ownership.advance(TTL + Duration::from_secs(1));
    env.ownership.plane.down.store(true, Ordering::SeqCst);
    wait_for("the lapsed owner to leave", async || names(&dir).is_empty()).await;
    assert!(stats(&store, &a).await.is_err());
    assert!(names(&dir).is_empty());
    env.ownership.plane.down.store(false, Ordering::SeqCst);
    store.shutdown().await.unwrap();
}

/// Files an earlier process left are deleted at construction and never read.
async fn leftovers_are_deleted_and_never_read(s3: bool) {
    let env = Env::new("leftovers", s3);
    let (a, b) = (env.address("a"), env.address("b"));
    let dir = projection_dir(&env, "a.db");
    let first = env.open("a.db").await;
    grow(&first, &a, 4).await;
    let expected = pending_items(&first, &a).await;
    first.shutdown().await.unwrap();
    assert!(names(&dir).is_empty());
    // Garbage at the queue's own name and at another name that matches.
    let other = format!("q7-{}", "ab".repeat(32));
    let mut planted = Vec::new();
    for stem in [stem(&a), other] {
        for suffix in [".db", ".db-wal", ".db-shm", ".db-journal"] {
            planted.push(format!("{stem}{suffix}"));
        }
    }
    for name in &planted {
        std::fs::write(dir.join(name), b"not a database").unwrap();
    }
    // Files that do not match our naming survive.
    let kept = ["notes.txt", "projection.db", "q7-short.db"];
    for name in kept {
        std::fs::write(dir.join(name), b"keep").unwrap();
    }
    let second = env.open("a.db").await;
    assert_eq!(
        names(&dir),
        kept.to_vec(),
        "construction deleted the leftovers"
    );
    // The queue rebuilds from the log, not from anything left behind.
    assert_eq!(pending_items(&second, &a).await, expected);
    assert_eq!(
        stats(&second, &b).await.err(),
        Some(QueueError::QueueNotFound)
    );
    second.shutdown().await.unwrap();
    assert_eq!(names(&dir), kept.to_vec());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leftovers_are_deleted_and_never_read_local() {
    leftovers_are_deleted_and_never_read(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leftovers_are_deleted_and_never_read_real_s3() {
    leftovers_are_deleted_and_never_read(true).await;
}

/// The arguments of a pipeline over a local log, but for the directory.
async fn construct(root: &Path, directory: PathBuf) -> QueueResult<Arc<QueueRecoveryPipeline>> {
    let log = native_log_fixture(root, None, None);
    let workers = ScratchWorkers::new(2, 8).unwrap();
    let scratch = fireweed_queue_spool::ScratchRoot::open(root.join("scratch"), &workers)
        .await
        .unwrap();
    QueueRecoveryPipeline::new(
        Arc::new(log),
        directory,
        workers,
        QueuePipelineOptions {
            operations: 8,
            hard_record_bytes: 1 << 20,
        },
        scratch,
        fireweed_queue_spool::ScratchDiskQuota::new(64 << 20).unwrap(),
        super::ownership::TestOwnership::new().handle("only"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_projection_directory_is_validated_before_any_io() {
    let env = Env::new("validation", false);
    // Relative: refused, and nothing is created for it.
    let relative = PathBuf::from("fireweed-relative-projections-must-not-exist");
    let refused = construct(&env.root, relative.clone()).await;
    assert!(matches!(refused, Err(QueueError::Invalid(_))));
    assert!(!relative.exists());
    // A regular file: refused, and left as it was.
    let file = env.root.join("a-file");
    std::fs::write(&file, b"data").unwrap();
    let refused = construct(&env.root, file.clone()).await;
    assert!(matches!(refused, Err(QueueError::Invalid(_))));
    assert_eq!(std::fs::read(&file).unwrap(), b"data");
    // Missing, even nested: created.
    let missing = env.root.join("not").join("yet");
    let pipeline = construct(&env.root, missing.clone()).await.unwrap();
    assert!(missing.is_dir());
    drop(pipeline);
    // An existing directory is accepted.
    let existing = env.root.join("existing");
    std::fs::create_dir(&existing).unwrap();
    construct(&env.root, existing).await.unwrap();
}

/// After a committed write the write-ahead log is at most its bound plus the one
/// transaction that grew it. The bound is 1 MiB (`WAL_TRUNCATE_BYTES` in
/// fireweed-turso). One insert of a 4 KiB body writes a few dozen pages at most
/// (the record row, its body and field rows, the index leaves and their
/// ancestors, the cursor), well under 64 pages of 4 KiB each; so 256 KiB covers
/// the transaction, and the log can never rest above 1 MiB + 256 KiB.
const WAL_LIMIT: u64 = (1 << 20) + (256 << 10);

fn wal_size(dir: &Path, queue: &QueueAddress) -> u64 {
    std::fs::metadata(dir.join(format!("{}.db-wal", stem(queue))))
        .map(|meta| meta.len())
        .unwrap_or(0)
}
fn heavy(n: usize) -> NewWork {
    let mut work = suite::item(&format!("wal-{n}"), "counts", 0, n as i64);
    work.body = bytes::Bytes::from(vec![n as u8; 4096]);
    work
}

async fn the_wal_stays_bounded(s3: bool) {
    const RECORDS: usize = 400;
    let env = Env::new("wal", s3);
    let queue = env.address("q");
    let dir = projection_dir(&env, "a.db");
    let store = env.open("a.db").await;
    grow(&store, &queue, 1).await;
    let (mut peak, mut truncations, mut last) = (0, 0, 0);
    for n in 0..RECORDS {
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: format!("wal-{n}"),
                mode: InsertMode::InsertOnly,
                items: vec![heavy(n)],
            })
            .await
            .unwrap();
        let size = wal_size(&dir, &queue);
        assert!(size <= WAL_LIMIT, "record {n}: wal is {size} bytes");
        truncations += usize::from(size < last);
        (peak, last) = (peak.max(size), size);
    }
    // The workload passed the bound several times over, so the log was truncated
    // several times; without truncation it would have grown past all of them.
    assert!(
        truncations >= 3,
        "truncated {truncations} times, peak {peak}"
    );
    store.shutdown().await.unwrap();
    // A rebuild applies the same records one at a time and is bounded the same way.
    let rebuilt = env.open("a.db").await;
    let rebuilding = tokio::spawn({
        let (store, queue) = (rebuilt.clone(), queue.clone());
        async move { pending_items(&store, &queue).await }
    });
    let mut rebuild_peak = 0;
    while !rebuilding.is_finished() {
        rebuild_peak = rebuild_peak.max(wal_size(&dir, &queue));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(rebuilding.await.unwrap(), RECORDS as u64);
    assert!(
        rebuild_peak <= WAL_LIMIT,
        "rebuild wal peaked at {rebuild_peak}"
    );
    assert!(wal_size(&dir, &queue) <= WAL_LIMIT);
    rebuilt.shutdown().await.unwrap();
}

/// Large-scale tier: selected in CI by the `::large::` path segment.
mod large {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_wal_stays_bounded_local() {
        the_wal_stays_bounded(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_wal_stays_bounded_real_s3() {
        the_wal_stays_bounded(true).await;
    }
}

/// A pipeline over `env`'s local log and the projection directory `name`.
async fn pipeline_on(env: &Env, name: &str) -> QueueResult<Arc<QueueRecoveryPipeline>> {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let scratch = fireweed_queue_spool::ScratchRoot::open(env.root.join("scratch-lock"), &workers)
        .await
        .unwrap();
    QueueRecoveryPipeline::new(
        Arc::new(super::native_log_fixture(&env.root, None, None)),
        env.root.join(name),
        workers,
        QueuePipelineOptions {
            operations: 1,
            hard_record_bytes: 1 << 20,
        },
        scratch,
        fireweed_queue_spool::ScratchDiskQuota::new(1 << 20).unwrap(),
        env.ownership.handle("second"),
    )
}

#[tokio::test]
async fn a_second_pipeline_on_a_directory_in_use_is_refused_until_the_first_shuts_down() {
    let env = Env::new("directory-lock", false);
    let first = env.open("shared").await;
    let live = projection_dir(&env, "shared");
    let before = all_names(&live);
    assert!(before.contains(&".lock".to_owned()), "{before:?}");
    assert!(matches!(
        pipeline_on(&env, "shared").await,
        Err(QueueError::Invalid(_))
    ));
    assert_eq!(
        all_names(&live),
        before,
        "the refused pipeline changed the directory"
    );
    first.shutdown().await.unwrap();
    // The lock goes with the pipeline, and the lock file itself stays.
    let second = pipeline_on(&env, "shared").await.unwrap();
    assert_eq!(all_names(&live), vec![".lock".to_owned()]);
    drop(second);
}

/// Dropping the last handle without `shutdown` must not release the directory
/// while a queue loop still holds a tenure: that loop deletes its projection
/// files when it ends, and would delete a new handle's files of the same name.
/// The current-thread runtime makes this deterministic: the loop task cannot run
/// between the drop and the assertions, so it is still parked in its wait.
#[tokio::test]
async fn the_directory_stays_locked_until_every_tenure_has_ended() {
    let env = Env::new("directory-lock-tenure", false);
    let store = env.open("shared").await;
    let live = projection_dir(&env, "shared");
    let a = env.address("a");
    grow(&store, &a, 1).await;
    let file = format!("{}.db", stem(&a));
    assert!(names(&live).contains(&file), "the tenure's file exists");
    drop(store);
    // The loop task has not run since the drop (no await in between): its tenure
    // is alive. Take the lock directly, since building a pipeline awaits.
    let refused = projection_files::prepare_directory(&live);
    assert!(
        matches!(refused, Err(QueueError::Invalid(_))),
        "the directory was released while a tenure still held a projection: {:?} {:?}",
        refused.as_ref().err(),
        names(&live)
    );
    assert!(
        names(&live).contains(&file),
        "the refused pipeline must not delete the live tenure's file"
    );
    drop(refused);
    // Once the loop ends and its files are gone, the directory is free.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let second = loop {
        match pipeline_on(&env, "shared").await {
            Ok(second) => break second,
            Err(QueueError::Invalid(_)) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("directory never freed: {error:?}"),
        }
    };
    assert_eq!(all_names(&live), vec![".lock".to_owned()]);
    drop(second);
}
