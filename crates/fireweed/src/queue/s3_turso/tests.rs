//! Validation, redaction and ephemeral-store tests, plus the S3/Turso/PostgreSQL
//! end-to-end tests, which are `#[ignore]`d until their environment is set:
//! `FIREWEED_API007_PG_TEST_URL` for PostgreSQL and `FIREWEED_S3_TEST_*` for S3.
use super::*;
use crate::queue::*;
use fireweed_conformance::queue_contract::{self as suite, TestClock};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
fn scratch_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-s3t-{label}-{}-{}",
        std::process::id(),
        nonce()
    ))
}
fn address(nonce: u128, name: &str) -> QueueAddress {
    QueueAddress {
        tenant: "api007-s3-turso".into(),
        queue: format!("{nonce}-{name}"),
    }
}

/// Valid options that point nowhere: nothing listens on port 1.
fn unreachable_options(cache: &Path) -> S3TursoOptions {
    S3TursoOptions::new(
        S3LogOptions {
            endpoint: "http://127.0.0.1:1".into(),
            region: "us-east-1".into(),
            bucket: "bucket".into(),
            namespace: "ns".into(),
            access_key_id: ConfigSecret::new("TOPSECRET-ACCESS"),
            secret_access_key: ConfigSecret::new("TOPSECRET-SECRET"),
            allow_insecure_http: true,
        },
        TursoCacheOptions {
            directory: cache.to_owned(),
        },
        PostgresControlPlaneOptions {
            url: ConfigSecret::new("postgres://user:TOPSECRET-PASSWORD@127.0.0.1:1/db"),
            schema: "fireweed_queue".into(),
            instance_id: OwnerId::new("instance-1").unwrap(),
            heartbeat_ttl_ms: 5_000,
            lease_ttl_ms: 15_000,
            ownership_clock: None,
        },
    )
}

#[tokio::test]
async fn invalid_options_fail_before_any_io() {
    let cache = scratch_dir("validation");
    type Mutation = Box<dyn Fn(&mut S3TursoOptions)>;
    let cases: Vec<(&str, Mutation)> = vec![
        ("empty endpoint", Box::new(|o| o.log.endpoint = "".into())),
        ("blank region", Box::new(|o| o.log.region = " ".into())),
        ("empty bucket", Box::new(|o| o.log.bucket = "".into())),
        ("empty namespace", Box::new(|o| o.log.namespace = "".into())),
        (
            "invalid namespace",
            Box::new(|o| o.log.namespace = "a/../b".into()),
        ),
        (
            "empty access key",
            Box::new(|o| o.log.access_key_id = ConfigSecret::new("")),
        ),
        (
            "empty secret key",
            Box::new(|o| o.log.secret_access_key = ConfigSecret::new("")),
        ),
        (
            "http without opt-in",
            Box::new(|o| o.log.allow_insecure_http = false),
        ),
        (
            "unknown scheme",
            Box::new(|o| o.log.endpoint = "ftp://host".into()),
        ),
        (
            "endpoint credentials",
            Box::new(|o| o.log.endpoint = "https://user:pw@host".into()),
        ),
        (
            "endpoint without host",
            Box::new(|o| o.log.endpoint = "https://".into()),
        ),
        (
            "relative cache directory",
            Box::new(|o| o.cache.directory = PathBuf::from("relative/cache")),
        ),
        (
            "zero heartbeat",
            Box::new(|o| o.control_plane.heartbeat_ttl_ms = 0),
        ),
        (
            "zero lease",
            Box::new(|o| {
                o.control_plane.heartbeat_ttl_ms = 0;
                o.control_plane.lease_ttl_ms = 0
            }),
        ),
        (
            "lease shorter than heartbeat",
            Box::new(|o| o.control_plane.lease_ttl_ms = 4_999),
        ),
        (
            "empty schema",
            Box::new(|o| o.control_plane.schema = "".into()),
        ),
        (
            "schema with dash",
            Box::new(|o| o.control_plane.schema = "a-b".into()),
        ),
        (
            "schema too long",
            Box::new(|o| o.control_plane.schema = "a".repeat(64)),
        ),
        (
            "empty url",
            Box::new(|o| o.control_plane.url = ConfigSecret::new(" ")),
        ),
        (
            "unparseable url",
            Box::new(|o| o.control_plane.url = ConfigSecret::new("postgres://h:notaport/db")),
        ),
        ("invalid limits", Box::new(|o| o.limits.max_records = 0)),
        ("zero operations", Box::new(|o| o.runtime.operations = 0)),
        (
            "more workers than operations",
            Box::new(|o| {
                o.runtime.operations = 2;
                o.runtime.scratch_workers = Some(3)
            }),
        ),
        (
            "zero retained bytes",
            Box::new(|o| o.runtime.retained_bytes = Some(0)),
        ),
        (
            "relative scratch root",
            Box::new(|o| o.runtime.scratch_root = Some(PathBuf::from("scratch"))),
        ),
    ];
    for (name, mutate) in cases {
        let mut options = unreachable_options(&cache);
        mutate(&mut options);
        let started = Instant::now();
        let result = Queue::s3_turso(options, Arc::new(SystemClock)).await;
        assert!(
            matches!(result, Err(QueueError::Invalid(_))),
            "{name}: {:?}",
            result.err()
        );
        // A refused connection is instant too, so the absence of the cache
        // directory (made by construction after validation) is what proves it.
        assert!(started.elapsed() < Duration::from_secs(2), "{name}");
        assert!(!cache.exists(), "{name}: touched the filesystem");
    }
}

#[tokio::test]
async fn unreachable_infrastructure_is_a_sanitized_outcome_unknown() {
    let cache = scratch_dir("unreachable");
    let result = Queue::s3_turso(unreachable_options(&cache), Arc::new(SystemClock)).await;
    let Err(QueueError::OutcomeUnknown(message)) = result else {
        panic!("expected OutcomeUnknown")
    };
    for secret in ["TOPSECRET", "127.0.0.1", "user"] {
        assert!(!message.contains(secret), "{message}");
    }
    let _ = std::fs::remove_dir_all(cache);
}

#[test]
fn debug_output_redacts_secrets_and_the_ownership_clock() {
    struct Loud;
    impl Clock for Loud {
        fn now(&self) -> UtcTimestamp {
            unreachable!()
        }
    }
    impl fmt::Debug for Loud {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("CLOCK-INTERNALS")
        }
    }
    let mut options = unreachable_options(Path::new("/cache"));
    options.control_plane.ownership_clock = Some(Arc::new(Loud));
    for text in [
        format!("{options:?}"),
        format!("{options:#?}"),
        format!("{:?}", options.log),
        format!("{:?}", options.control_plane),
        format!("{:?}", options.cache),
    ] {
        for secret in ["TOPSECRET", "CLOCK-INTERNALS"] {
            assert!(!text.contains(secret), "{text}");
        }
    }
    let text = format!("{:?}", options.control_plane);
    assert!(text.contains("separate_ownership_clock: true"), "{text}");
    assert!(text.contains("redacted"), "{text}");
}

#[test]
fn defaults_match_postgres_options() {
    let options = unreachable_options(Path::new("/cache"));
    assert_eq!(options.limits, QueueLimits::default());
    assert_eq!(options.runtime, QueueRuntimeOptions::default());
}

/// Provision, insert, claim, commit, stats and query through a public handle.
async fn round_trip(queue: &Queue, queue_address: QueueAddress) {
    queue
        .provision(ProvisionRequest {
            queue: queue_address.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    let outcomes = queue
        .insert(InsertRequest {
            queue: queue_address.clone(),
            request_id: "seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![
                suite::item("a", "job", 1, 10),
                suite::item("b", "job", 2, 20),
            ],
            issued_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    let claimed = queue
        .claim(suite::claim(&queue_address, "claim-1", "job"))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    let mut group = suite::group("finish");
    group.claimed_updates = claimed.iter().map(suite::update).collect();
    let receipts = queue
        .commit(CommitBatch {
            queue: queue_address.clone(),
            groups: vec![group],
        })
        .await
        .unwrap();
    assert!(matches!(
        receipts[0].outcome,
        GroupOutcome::Committed { .. }
    ));
    let stats = queue
        .stats(StatsRequest {
            buckets: None,
            queue: queue_address.clone(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(stats.totals.completed, 2);
    assert_eq!(stats.totals.pending, 0);
    let page = queue
        .query(QueryRequest {
            queue: queue_address.clone(),
            source: RecordSource::Work,
            index: "by_score".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 10,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    let found = queue
        .read_commit(ReceiptReadRequest {
            queue: queue_address,
            request_ids: vec![ReceiptReadId {
                request_id: "finish".into(),
                issued_at: 0,
            }],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(matches!(found[0], ReceiptLookup::Found(_)));
}

fn entries(directory: &Path) -> Vec<String> {
    std::fs::read_dir(directory)
        .map(|read| {
            read.map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn ephemeral_disk_round_trips_and_shutdown_removes_its_directory() {
    let parent = scratch_dir("ephemeral");
    std::fs::create_dir_all(&parent).unwrap();
    let queue = Queue::ephemeral_disk_in(
        &parent,
        QueueLimits::default(),
        QueueRuntimeOptions::default(),
        Arc::new(TestClock::default()),
    )
    .await
    .unwrap();
    let created = entries(&parent);
    assert_eq!(created.len(), 1, "{created:?}");
    let own = parent.join(&created[0]);
    round_trip(&queue, address(nonce(), "round-trip")).await;
    assert!(own.join("projections").exists());
    assert!(
        own.join("projections").join(".lock").exists(),
        "the ephemeral cache takes the same directory lock"
    );
    let clone = queue.clone();
    queue.shutdown().await.unwrap();
    assert!(!own.exists(), "shutdown left {:?}", entries(&parent));
    assert_eq!(
        clone
            .stats(StatsRequest {
                buckets: None,
                queue: address(1, "x"),
                filter: Filter::default(),
                group_by: vec![],
                eligible_at: 0,
            })
            .await,
        Err(QueueError::Busy)
    );
    std::fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn ephemeral_disk_drop_removes_its_directory_and_handles_are_separate() {
    let parent = scratch_dir("ephemeral-drop");
    std::fs::create_dir_all(&parent).unwrap();
    let first = Queue::ephemeral_disk_in(
        &parent,
        QueueLimits::default(),
        QueueRuntimeOptions::default(),
        Arc::new(TestClock::default()),
    )
    .await
    .unwrap();
    let second = Queue::ephemeral_disk_in(
        &parent,
        QueueLimits::default(),
        QueueRuntimeOptions::default(),
        Arc::new(TestClock::default()),
    )
    .await
    .unwrap();
    let names = entries(&parent);
    assert_eq!(names.len(), 2, "two handles, two directories");
    // The same address is a different queue in each handle.
    let shared = address(nonce(), "separate");
    round_trip(&first, shared.clone()).await;
    round_trip(&second, shared).await;
    drop(first);
    assert_eq!(entries(&parent).len(), 1, "the drop removed only its own");
    second.shutdown().await.unwrap();
    drop(second);
    assert!(entries(&parent).is_empty());
    std::fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn ephemeral_disk_public_constructor_uses_the_temp_directory() {
    let queue = Queue::ephemeral_disk(Arc::new(TestClock::default()))
        .await
        .unwrap();
    round_trip(&queue, address(nonce(), "public")).await;
    queue.shutdown().await.unwrap();
}

#[tokio::test]
async fn ephemeral_disk_limits_round_trip_and_are_applied() {
    let default = Queue::ephemeral_disk(Arc::new(TestClock::default()))
        .await
        .unwrap();
    assert_eq!(default.limits(), QueueLimits::default());
    let clone = default.clone();
    assert_eq!(clone.limits(), QueueLimits::default());
    default.shutdown().await.unwrap();

    let custom = QueueLimits {
        max_records: 16,
        ..QueueLimits::default()
    };
    let queue = Queue::ephemeral_disk_with(
        custom,
        QueueRuntimeOptions::default(),
        Arc::new(TestClock::default()),
    )
    .await
    .unwrap();
    assert_eq!(queue.limits(), custom);
    let queue_address = address(nonce(), "custom-limits");
    queue
        .provision(ProvisionRequest {
            queue: queue_address.clone(),
            schema: suite::schema(),
        })
        .await
        .unwrap();
    let refused = queue
        .insert(InsertRequest {
            queue: queue_address,
            request_id: "too-many".into(),
            mode: InsertMode::InsertOnly,
            items: (0..17)
                .map(|n| suite::item(&format!("i{n}"), "job", n, 10))
                .collect(),
            issued_at: 0,
        })
        .await;
    assert!(
        matches!(
            refused,
            Err(QueueError::Invalid(_)) | Err(QueueError::TooLarge)
        ),
        "{refused:?}"
    );
    queue.shutdown().await.unwrap();
}

#[tokio::test]
async fn ephemeral_disk_with_refuses_invalid_input_before_creating_a_directory() {
    let parent = scratch_dir("ephemeral-invalid");
    std::fs::create_dir_all(&parent).unwrap();
    let bad_limits = QueueLimits {
        max_records: 0,
        ..QueueLimits::default()
    };
    let bad_runtime = QueueRuntimeOptions {
        operations: 0,
        ..QueueRuntimeOptions::default()
    };
    for (limits, runtime) in [
        (bad_limits, QueueRuntimeOptions::default()),
        (QueueLimits::default(), bad_runtime),
    ] {
        let result =
            Queue::ephemeral_disk_in(&parent, limits, runtime, Arc::new(TestClock::default()))
                .await;
        assert!(matches!(result, Err(QueueError::Invalid(_))), "{result:?}");
        assert!(entries(&parent).is_empty(), "{:?}", entries(&parent));
    }
    std::fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn ephemeral_disk_with_refuses_unsupported_native_options_before_creating_a_directory() {
    // The parent does not exist, so any attempt to create the directory would give
    // `Unavailable`; an error handle removes what it created, so an existing empty
    // parent could not tell the two orders apart.
    let parent = scratch_dir("ephemeral-native-invalid");
    // Consistent admission limits whose largest log record exceeds `u32::MAX`:
    // only `native_options` refuses them.
    let limits = QueueLimits {
        max_request_bytes: 1 << 30,
        ..QueueLimits::default()
    };
    limits.validate().unwrap();
    let runtime = QueueRuntimeOptions::default();
    resolve_runtime(&limits, &runtime).unwrap();
    let result =
        Queue::ephemeral_disk_in(&parent, limits, runtime, Arc::new(TestClock::default())).await;
    assert!(matches!(result, Err(QueueError::Invalid(_))), "{result:?}");
    assert!(!parent.exists());
}

/// Everything after `run` that a rebuild or reopen must preserve.
struct Prepared {
    receipts: Vec<(QueueAddress, Vec<String>, Vec<ReceiptLookup>)>,
    acknowledged: (CommitBatch, Vec<CommitReceipt>),
}

/// The API-007 scenarios common to every handle, except those needing a second
/// handle. Returns what a fresh handle over the same storage must reproduce.
async fn contract(queue: &Queue, clock: &TestClock, id: u128) -> Prepared {
    let a = |name: &str| address(id, name);
    suite::run(queue, a("contract")).await;
    suite::expiry_and_administration(queue, clock, a("expiry")).await;
    suite::ordered_long_values_and_cursor_scope(queue, a("long")).await;
    suite::predicate_and_order_oracles(queue, a("predicates")).await;
    suite::presence_and_absence_predicates(queue, a("presence")).await;
    suite::ordered_groups_sparse_stats_and_replay_precedence(queue, clock, a("groups")).await;
    suite::ordered_insert_uniqueness(queue, a("insert-uniqueness")).await;
    suite::shrinking_upsert_index_amplification(queue, a("shrinking-upsert")).await;
    suite::indexed_additional_attributes(queue, a("additional-attributes")).await;
    suite::numeric_bucket_stats(queue, clock, a("numeric-buckets")).await;
    suite::retention_floor_refused_at_provisioning(queue, a("retention-floor")).await;
    suite::issued_at_skew_and_conflict(queue, clock, a("issued-at")).await;
    suite::retired_keys_refuse_reinsert_within_window(queue, clock, a("retired-keys")).await;
    suite::reusable_keys_reinsert_at_once(queue, clock, a("reusable-keys")).await;
    suite::request_horizon_expires_old_requests(queue, clock, a("request-horizon")).await;
    suite::horizon_never_passes_a_live_claim(queue, clock, a("live-claim-horizon")).await;
    suite::removed_deleted_keys_keep_versions_rising(queue, clock, a("removed-keys")).await;
    suite::retired_keys_cover_every_deletion_route(
        queue,
        clock,
        a("retired-routes"),
        cfg!(target_os = "linux"),
    )
    .await;
    suite::unrestricted_names_obey_response_bounds(queue, a("names-bounds")).await;
    suite::atomic_administration_preview_and_replay(queue, a("administration-preview")).await;
    suite::administrative_size_siblings_and_preflight(queue, a("administration-sizes")).await;
    // Broad administration spools through Linux-only scratch.
    #[cfg(target_os = "linux")]
    suite::broad_administration_exceeds_input_chunk(queue, a("administration-broad")).await;
    let acknowledged = suite::prepare_process_loss(queue, a("acknowledged-reopen")).await;
    let mut receipts = Vec::new();
    for (name, ids) in [
        (
            "contract",
            vec![
                "transition".to_owned(),
                "claim-and-absence-checks".into(),
                "present-and-absent-state-checks".into(),
            ],
        ),
        ("administration-preview", vec!["admin-transition".into()]),
        (
            "administration-sizes",
            vec!["wide-accepted".into(), "middle".into()],
        ),
    ] {
        let found = queue
            .read_commit(ReceiptReadRequest {
                queue: a(name),
                request_ids: fireweed_conformance::queue_contract::read_ids(ids.clone()),
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        receipts.push((a(name), ids, found));
    }
    Prepared {
        receipts,
        acknowledged,
    }
}

mod large {
    use super::*;

    #[tokio::test]
    async fn ephemeral_disk_runs_the_api007_contract() {
        let clock = Arc::new(TestClock::default());
        let queue = Queue::ephemeral_disk(clock.clone()).await.unwrap();
        contract(&queue, &clock, nonce()).await;
        queue.shutdown().await.unwrap();
    }
}

// ---- S3 + Turso + PostgreSQL ----------------------------------------------

/// A clock both handles stamp the control plane with, moved by hand.
struct SharedClock(AtomicI64);
impl SharedClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(
            i64::try_from(nonce()).unwrap_or(i64::MAX),
        )))
    }
    fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_nanos() as i64, Ordering::SeqCst);
    }
}
impl Clock for SharedClock {
    fn now(&self) -> UtcTimestamp {
        timestamp_from_nanos(self.0.load(Ordering::SeqCst))
    }
}

struct Fixture {
    id: u128,
    schema: String,
    namespace: String,
    url: String,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let id = nonce();
        Self {
            id,
            schema: format!("fw_s3t_{label}_{id}"),
            namespace: format!("api007-s3-turso-{label}-{id}"),
            url: std::env::var("FIREWEED_API007_PG_TEST_URL")
                .expect("isolated PostgreSQL fixture: FIREWEED_API007_PG_TEST_URL"),
        }
    }
    fn options(
        &self,
        instance: &str,
        cache: &Path,
        ttl_ms: u64,
        ownership_clock: Option<Arc<dyn Clock>>,
    ) -> S3TursoOptions {
        let s3 = fireweed_objectlog::test_s3::shared_s3_test_env();
        S3TursoOptions::new(
            S3LogOptions {
                endpoint: s3.endpoint.clone(),
                region: s3.region.clone(),
                bucket: s3.bucket.clone(),
                namespace: self.namespace.clone(),
                access_key_id: ConfigSecret::new(s3.access_key.clone()),
                secret_access_key: ConfigSecret::new(s3.secret_key.clone()),
                allow_insecure_http: s3.endpoint.starts_with("http://"),
            },
            TursoCacheOptions {
                directory: cache.to_owned(),
            },
            PostgresControlPlaneOptions {
                url: ConfigSecret::new(self.url.clone()),
                schema: self.schema.clone(),
                instance_id: OwnerId::new(instance).unwrap(),
                heartbeat_ttl_ms: ttl_ms,
                lease_ttl_ms: ttl_ms,
                ownership_clock,
            },
        )
    }
}

#[tokio::test]
#[ignore = "requires FIREWEED_API007_PG_TEST_URL and FIREWEED_S3_TEST_*"]
async fn s3_turso_round_trips_and_leaves_no_cache_files() {
    let fixture = Fixture::new("round");
    let cache = scratch_dir("round").join("cache");
    let queue = Queue::s3_turso(
        fixture.options("one", &cache, 3_000, None),
        Arc::new(TestClock::default()),
    )
    .await
    .unwrap();
    let second = Queue::s3_turso(
        fixture.options("two", &cache, 3_000, None),
        Arc::new(TestClock::default()),
    )
    .await;
    assert!(
        matches!(second, Err(QueueError::Invalid(_))),
        "a second handle on a directory in use is refused: {:?}",
        second.err()
    );
    round_trip(&queue, address(fixture.id, "round-trip")).await;
    assert!(
        entries(&cache).iter().any(|name| name.starts_with("q7-")),
        "the owned queue has a projection file while the handle is alive"
    );
    queue.shutdown().await.unwrap();
    assert!(
        !entries(&cache).iter().any(|name| name.starts_with("q7-")),
        "{:?}",
        entries(&cache)
    );
    let again = Queue::s3_turso(
        fixture.options("three", &cache, 3_000, None),
        Arc::new(TestClock::default()),
    )
    .await
    .expect("the directory is free once the first handle shut down");
    again.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(cache.parent().unwrap());
}

#[tokio::test]
#[ignore = "requires FIREWEED_API007_PG_TEST_URL and FIREWEED_S3_TEST_*"]
async fn s3_turso_shutdown_releases_leases_for_another_handle() {
    let fixture = Fixture::new("release");
    let root = scratch_dir("release");
    let clock = Arc::new(TestClock::default());
    // A 10 s lease: waiting it out would take 10 s, and entry waits 2 s of skew.
    let ttl = 10_000;
    let first = Queue::s3_turso(
        fixture.options("first", &root.join("first"), ttl, None),
        clock.clone(),
    )
    .await
    .unwrap();
    let second = Queue::s3_turso(
        fixture.options("second", &root.join("second"), ttl, None),
        clock.clone(),
    )
    .await
    .unwrap();
    let queue = address(fixture.id, "release");
    round_trip(&first, queue.clone()).await;
    let stats = StatsRequest {
        buckets: None,
        queue: queue.clone(),
        filter: Filter::default(),
        group_by: vec![],
        eligible_at: 0,
    };
    assert_eq!(
        second.stats(stats.clone()).await.map(|_| ()),
        Err(QueueError::Busy),
        "the first handle holds the lease"
    );
    first.shutdown().await.unwrap();
    let started = Instant::now();
    let seen = second.stats(stats).await.unwrap();
    assert_eq!(seen.totals.completed, 2, "nothing lost across the handoff");
    assert!(
        started.elapsed() < Duration::from_millis(ttl - 1_000),
        "took {:?}: waited out the lease",
        started.elapsed()
    );
    second.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
#[ignore = "requires FIREWEED_API007_PG_TEST_URL and FIREWEED_S3_TEST_*"]
async fn s3_turso_two_handles_share_a_queue_and_run_the_api007_contract() {
    let fixture = Fixture::new("contract");
    let root = scratch_dir("contract");
    let clock = Arc::new(TestClock::default());
    let ownership = SharedClock::new();
    // Large requests take seconds in a debug build, and renewal happens between
    // batches, so the lease must outlast them; entry waits a fifth of it.
    let ttl = 60_000;
    let open = |instance: &'static str, cache: PathBuf| {
        let options = fixture.options(instance, &cache, ttl, Some(ownership.clone()));
        let clock = clock.clone();
        async move { Queue::s3_turso(options, clock).await.unwrap() }
    };
    let queue = open("primary", root.join("primary")).await;
    let other = open("other", root.join("other")).await;
    assert_ne!(root.join("primary"), root.join("other"));

    // ConcurrentClaims: the second handle is Busy until ownership moves, then
    // continues with disjoint work; all 100 items are claimed exactly once.
    let handoff_clock = ownership.clone();
    suite::competing_claims_with_handoff(
        &queue,
        &other,
        address(fixture.id, "competition"),
        &move || {
            let clock = handoff_clock.clone();
            Box::pin(async move { clock.advance(Duration::from_millis(ttl + 1_000)) })
        },
    )
    .await;

    let prepared = contract(&queue, &clock, fixture.id).await;
    queue.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    assert!(
        !entries(&root.join("primary"))
            .iter()
            .any(|n| n.starts_with("q7-"))
    );
    assert!(
        !entries(&root.join("other"))
            .iter()
            .any(|n| n.starts_with("q7-"))
    );

    // Cold rebuild from S3 in a third handle with a new cache directory.
    let reopened = Queue::s3_turso(
        fixture.options("reopened", &root.join("reopened"), ttl, None),
        clock.clone(),
    )
    .await
    .unwrap();
    for (queue_address, ids, expected) in prepared.receipts {
        let found = reopened
            .read_commit(ReceiptReadRequest {
                queue: queue_address,
                request_ids: fireweed_conformance::queue_contract::read_ids(ids),
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        assert_eq!(found, expected);
    }
    suite::verify_process_loss(&reopened, prepared.acknowledged).await;
    reopened.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}
