use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_admission::QueueAdmission;
use fireweed_objectlog::queue_native_log::{
    QueueNativeLog, QueueNativeLogOptions, QueueNativeS3Connection, QueueNativeS3TransportOptions,
};

mod batching;
mod counting_proxy;
mod native_transport;
mod owner_safety;
pub(super) mod ownership;
mod projection_directory;
mod record_counts;
mod store_lifecycle;
use ownership::TestOwnership;

#[test]
fn pipeline_configuration_rejects_semaphore_overflow_before_resource_construction() {
    let mut options = QueuePipelineOptions {
        operations: 1,
        hard_record_bytes: 1 << 20,
    };
    options.validate().unwrap();
    for operations in [0, Semaphore::MAX_PERMITS + 1] {
        options.operations = operations;
        assert!(matches!(options.validate(), Err(QueueError::Invalid(_))));
    }
    options.operations = 1;
    // A bound that leaves no room for a record inside an entry-list record.
    let overhead = fireweed_engine::queue_log_record::standalone_record_bound(u64::MAX)
        .map(|bound| u64::MAX - bound)
        .unwrap();
    for hard_record_bytes in [0, 1, overhead] {
        options.hard_record_bytes = hard_record_bytes;
        assert!(matches!(options.validate(), Err(QueueError::Invalid(_))));
    }
    options.hard_record_bytes = overhead + 1;
    options.validate().unwrap();
}

#[test]
fn operation_completion_releases_permit_before_terminal_notification_on_every_drop_path() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    for path in 0..3 {
        let slots = Arc::new(Semaphore::new(1));
        let admission = QueueAdmission::new(1, 100).unwrap();
        let observed = Arc::new(std::sync::Mutex::new(None));
        let counter = admission.clone();
        let permits = slots.clone();
        let signal = observed.clone();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || {
                    *signal.lock().unwrap() =
                        Some((permits.available_permits(), counter.usage().unwrap()));
                })
                .unwrap(),
        );
        let completion = OperationCompletion {
            _slot: slots.clone().try_acquire_owned().unwrap(),
            _job: job,
        };
        match path {
            0 => drop(completion),
            1 => {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                        let _completion = completion;
                        panic!("completion unwind control");
                    }))
                    .is_err()
                );
            }
            _ => {
                let mut pending = Box::pin(async move {
                    let _completion = completion;
                    std::future::pending::<()>().await;
                });
                assert!(
                    pending
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert!(observed.lock().unwrap().is_none());
                drop(pending);
            }
        }
        assert_eq!(
            *observed.lock().unwrap(),
            Some((1, (0, 0))),
            "drop path {path}"
        );
    }
}

/// The log every store in a test shares, through `prefix` on S3 or `root` on disk.
/// `retained_bytes` is left to the default, which covers a retained spine for a
/// thousand owned queues.
fn native_log_fixture(
    root: &std::path::Path,
    s3_prefix: Option<&str>,
    endpoint: Option<&str>,
) -> QueueNativeLog {
    let options =
        QueueNativeLogOptions::for_queue_limits(&QueueLimits::default(), 128, 2, 4096, None)
            .unwrap();
    if let Some(prefix) = s3_prefix {
        let env = fireweed_objectlog::test_s3::shared_s3_test_env();
        QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: endpoint.unwrap_or(&env.endpoint),
                region: &env.region,
                bucket: &env.bucket,
                access_key: &env.access_key,
                secret_key: &env.secret_key,
            },
            prefix.to_owned(),
            options,
            QueueNativeS3TransportOptions::default(),
        )
        .unwrap()
    } else {
        QueueNativeLog::local(&root.join("log"), "native".to_owned(), options).unwrap()
    }
}
async fn native_port_fixture(
    root: &std::path::Path,
    projection_directory: &str,
    s3_prefix: Option<&str>,
    clock: Arc<fireweed_conformance::queue_contract::TestClock>,
) -> NativeQueueStore {
    native_port_fixture_owned(
        root,
        projection_directory,
        s3_prefix,
        clock,
        TestOwnership::new().handle("only"),
    )
    .await
}
/// A handle that shares its control plane and time with others through `ownership`.
async fn native_port_fixture_owned(
    root: &std::path::Path,
    projection_directory: &str,
    s3_prefix: Option<&str>,
    clock: Arc<fireweed_conformance::queue_contract::TestClock>,
    ownership: QueueOwnership,
) -> NativeQueueStore {
    native_port_fixture_sized(
        root,
        projection_directory,
        s3_prefix,
        clock,
        ownership,
        256 << 20,
    )
    .await
}
/// As `native_port_fixture_owned`, with the pipeline's hard record bound.
async fn native_port_fixture_sized(
    root: &std::path::Path,
    projection_directory: &str,
    s3_prefix: Option<&str>,
    clock: Arc<fireweed_conformance::queue_contract::TestClock>,
    ownership: QueueOwnership,
    hard_record_bytes: u64,
) -> NativeQueueStore {
    native_port_fixture_routed(
        root,
        projection_directory,
        s3_prefix,
        clock,
        ownership,
        hard_record_bytes,
        None,
    )
    .await
}
/// As `native_port_fixture_sized`, with the S3 client pointed at `endpoint` (a
/// counting proxy) instead of the shared test endpoint.
async fn native_port_fixture_routed(
    root: &std::path::Path,
    projection_directory: &str,
    s3_prefix: Option<&str>,
    clock: Arc<fireweed_conformance::queue_contract::TestClock>,
    ownership: QueueOwnership,
    hard_record_bytes: u64,
    endpoint: Option<&str>,
) -> NativeQueueStore {
    let store = native_log_fixture(root, s3_prefix, endpoint);
    let workers = ScratchWorkers::new(2, 8).unwrap();
    let admission = QueueAdmission::new(1, 512 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    drop(job);
    let options = QueuePipelineOptions {
        operations: 128,
        hard_record_bytes,
    };
    options.validate().unwrap();
    let scratch = fireweed_queue_spool::ScratchRoot::open(root.join("scratch"), &workers)
        .await
        .unwrap();
    let quota = fireweed_queue_spool::ScratchDiskQuota::new(64 << 20).unwrap();
    let pipeline = QueueRecoveryPipeline::new(
        Arc::new(store),
        root.join(projection_directory),
        workers,
        options,
        scratch,
        quota,
        ownership,
    )
    .unwrap();
    NativeQueueStore::new(pipeline, QueueLimits::default(), 512 << 20, clock).unwrap()
}
async fn native_port_identical_contract(s3: bool) {
    use fireweed_conformance::queue_contract::{self as suite, TestClock};
    use fireweed_engine::queue_contract::QueueStore;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-native-port-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-native-port-{nonce}"));
    let clock = Arc::new(TestClock::default());
    let ownership = TestOwnership::new();
    let queue = native_port_fixture_owned(
        &root,
        "primary-projections",
        prefix.as_deref(),
        clock.clone(),
        ownership.handle("primary"),
    )
    .await;
    let other = native_port_fixture_owned(
        &root,
        "independent-projections",
        prefix.as_deref(),
        clock.clone(),
        ownership.handle("other"),
    )
    .await;
    let address = |name: &str| QueueAddress {
        tenant: "api007-native-port".into(),
        queue: format!("{nonce}-{name}"),
    };
    let missing = ReceiptReadRequest {
        queue: address("missing"),
        request_ids: vec![ReceiptReadId {
            request_id: "none".into(),
            issued_at: 0,
        }],
        max_bytes: 1 << 20,
    };
    assert_eq!(
        queue.read_commit(missing).await,
        Err(QueueError::QueueNotFound)
    );
    let primary = address("contract");
    eprintln!("native contract {s3}: run");
    suite::run(&queue, primary.clone()).await;
    let original_ids = || {
        suite::read_ids([
            "transition",
            "claim-and-absence-checks",
            "present-and-absent-state-checks",
        ])
    };
    let expected = queue
        .read_commit(ReceiptReadRequest {
            queue: primary.clone(),
            request_ids: original_ids(),
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    eprintln!("native contract {s3}: competing_claims");
    // One owner per queue: the other handle is Busy until ownership moves.
    suite::competing_claims_with_handoff(&queue, &other, address("competition"), &|| {
        let ownership = ownership.clone();
        Box::pin(
            async move { ownership.advance(ownership::TTL + std::time::Duration::from_secs(1)) },
        )
    })
    .await;
    eprintln!("native contract {s3}: expiry_and_administration");
    suite::expiry_and_administration(&queue, &clock, address("expiry")).await;
    eprintln!("native contract {s3}: ordered_long_values_and_cursor_scope");
    suite::ordered_long_values_and_cursor_scope(&queue, address("long")).await;
    eprintln!("native contract {s3}: predicate_and_order_oracles");
    suite::predicate_and_order_oracles(&queue, address("predicates")).await;
    eprintln!("native contract {s3}: ordered_groups_sparse_stats_and_replay_precedence");
    suite::ordered_groups_sparse_stats_and_replay_precedence(&queue, &clock, address("groups"))
        .await;
    eprintln!("native contract {s3}: ordered_insert_uniqueness");
    suite::ordered_insert_uniqueness(&queue, address("insert-uniqueness")).await;
    eprintln!("native contract {s3}: shrinking_upsert_index_amplification");
    suite::shrinking_upsert_index_amplification(&queue, address("shrinking-upsert")).await;
    eprintln!("native contract {s3}: indexed_additional_attributes");
    suite::indexed_additional_attributes(&queue, address("additional-attributes")).await;
    eprintln!("native contract {s3}: numeric_bucket_stats");
    suite::numeric_bucket_stats(&queue, &clock, address("numeric-buckets")).await;
    eprintln!("native contract {s3}: retention");
    suite::retention_floor_refused_at_provisioning(&queue, address("retention-floor")).await;
    suite::issued_at_skew_and_conflict(&queue, &clock, address("issued-at")).await;
    suite::retired_keys_refuse_reinsert_within_window(&queue, &clock, address("retired-keys"))
        .await;
    suite::reusable_keys_reinsert_at_once(&queue, &clock, address("reusable-keys")).await;
    suite::retired_keys_cover_every_deletion_route(
        &queue,
        &clock,
        address("retired-routes"),
        cfg!(target_os = "linux"),
    )
    .await;
    eprintln!("native contract {s3}: unrestricted_names_obey_response_bounds");
    suite::unrestricted_names_obey_response_bounds(&queue, address("names-bounds")).await;
    eprintln!("native contract {s3}: atomic_administration_preview_and_replay");
    suite::atomic_administration_preview_and_replay(&queue, address("administration-preview"))
        .await;
    eprintln!("native contract {s3}: administrative_size_siblings_and_preflight");
    suite::administrative_size_siblings_and_preflight(&queue, address("administration-sizes"))
        .await;
    // Broad administration spools through Linux-only scratch (the projection
    // rejects it elsewhere), so this one step is Linux-only.
    #[cfg(target_os = "linux")]
    {
        eprintln!("native contract {s3}: broad_administration_exceeds_input_chunk");
        suite::broad_administration_exceeds_input_chunk(&queue, address("administration-broad"))
            .await;
    }
    let acknowledged = suite::prepare_process_loss(&queue, address("acknowledged-reopen")).await;
    let receipts = |name: &str, ids: Vec<ReceiptReadId>| ReceiptReadRequest {
        queue: address(name),
        request_ids: ids,
        max_bytes: 1 << 20,
    };
    let administrative_expected = queue
        .read_commit(receipts(
            "administration-preview",
            suite::read_ids(["admin-transition"]),
        ))
        .await
        .unwrap();
    let wide_expected = queue
        .read_commit(receipts(
            "administration-sizes",
            suite::read_ids(["wide-accepted", "middle"]),
        ))
        .await
        .unwrap();
    let clone = queue.clone();
    queue.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    assert_eq!(
        clone
            .read_commit(receipts("contract", original_ids()))
            .await,
        Err(QueueError::Busy)
    );
    // Cold projection rebuild is part of the same suite, not cached DB reuse.
    let reopened =
        native_port_fixture(&root, "cold-rebuild-projections", prefix.as_deref(), clock).await;
    assert_eq!(
        reopened
            .read_commit(receipts("contract", original_ids()))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        reopened
            .read_commit(receipts(
                "administration-preview",
                suite::read_ids(["admin-transition"])
            ))
            .await
            .unwrap(),
        administrative_expected
    );
    assert_eq!(
        reopened
            .read_commit(receipts(
                "administration-sizes",
                suite::read_ids(["wide-accepted", "middle"])
            ))
            .await
            .unwrap(),
        wide_expected
    );
    eprintln!("native contract {s3}: verify_process_loss");
    suite::verify_process_loss(&reopened, acknowledged).await;
    reopened.shutdown().await.unwrap();
    drop(reopened);
    drop(clone);
    drop(other);
    drop(queue);
    std::fs::remove_dir_all(root).unwrap();
}

async fn native_condition_checks_contract(s3: bool) {
    use fireweed_conformance::queue_contract::{self as suite, TestClock};
    use fireweed_engine::queue_contract::QueueStore;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-check-contract-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-check-contract-{nonce}"));
    let clock = Arc::new(TestClock::default());
    let queue = native_port_fixture(
        &root,
        "primary-projections",
        prefix.as_deref(),
        clock.clone(),
    )
    .await;
    let address = QueueAddress {
        tenant: "api007-check-contract".into(),
        queue: nonce.to_string(),
    };
    let original = suite::run(&queue, address.clone()).await;
    let receipts = ReceiptReadRequest {
        queue: address.clone(),
        request_ids: suite::read_ids([
            original.groups[0].request_id.as_str(),
            "present-and-absent-state-checks",
            "transition",
        ]),
        max_bytes: 65536,
    };
    let expected = queue.read_commit(receipts.clone()).await.unwrap();
    let ReceiptLookup::Found(original_receipt) = &expected[0] else {
        panic!("missing original Check")
    };
    clock.set(2_000_000_000);
    assert_eq!(
        queue.commit(original.clone()).await.unwrap(),
        vec![original_receipt.clone()]
    );
    suite::atomic_administration_preview_and_replay(
        &queue,
        QueueAddress {
            tenant: address.tenant.clone(),
            queue: format!("{nonce}-administration"),
        },
    )
    .await;
    queue.shutdown().await.unwrap();
    let rebuilt =
        native_port_fixture(&root, "cold-rebuild-projections", prefix.as_deref(), clock).await;
    assert_eq!(rebuilt.read_commit(receipts).await.unwrap(), expected);
    assert_eq!(
        rebuilt.commit(original).await.unwrap(),
        vec![original_receipt.clone()]
    );
    rebuilt.shutdown().await.unwrap();
    drop(rebuilt);
    drop(queue);
    std::fs::remove_dir_all(root).unwrap();
}

/// A `Retired` deletion time lives in the log and the projection row: a cold
/// rebuild must refuse and accept exactly where the live owner did.
async fn native_retired_deletion_contract(s3: bool) {
    use fireweed_conformance::queue_contract::{self as suite, TestClock};
    use fireweed_engine::queue_contract::QueueStore;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-retired-contract-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-retired-contract-{nonce}"));
    let clock = Arc::new(TestClock::default());
    let live = native_port_fixture(
        &root,
        "primary-projections",
        prefix.as_deref(),
        clock.clone(),
    )
    .await;
    let prepared = suite::prepare_retired_deletion(
        &live,
        &clock,
        QueueAddress {
            tenant: "api007-retired-contract".into(),
            queue: nonce.to_string(),
        },
    )
    .await;
    live.shutdown().await.unwrap();
    drop(live);
    let rebuilt = native_port_fixture(
        &root,
        "cold-rebuild-projections",
        prefix.as_deref(),
        clock.clone(),
    )
    .await;
    suite::verify_retired_deletion(&rebuilt, &clock, prepared).await;
    rebuilt.shutdown().await.unwrap();
    drop(rebuilt);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn native_store_retired_deletion_survives_rebuild_local() {
    native_retired_deletion_contract(false).await;
}

#[tokio::test]
async fn native_store_retired_deletion_survives_rebuild_real_s3() {
    native_retired_deletion_contract(true).await;
}

#[tokio::test]
async fn native_store_condition_checks_replay_and_rebuild_local() {
    native_condition_checks_contract(false).await;
}

#[tokio::test]
async fn native_store_condition_checks_replay_and_rebuild_real_s3() {
    native_condition_checks_contract(true).await;
}

/// Large-scale tier: selected in CI by the `::large::` path segment.
mod large {
    use super::*;

    #[tokio::test]
    async fn native_store_runs_identical_contract_local() {
        native_port_identical_contract(false).await;
    }

    #[tokio::test]
    async fn native_store_runs_identical_contract_real_s3() {
        native_port_identical_contract(true).await;
    }
}
