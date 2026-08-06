//! P5aN — non-S3 Class A reopen and recovery-replay parity.
//!
//! The nine Class A cells owned here (no S3; Turso projection is out of this leaf):
//!   log ∈ {sqlite, postgres, filesystem}
//!   × projection ∈ {memory, sqlite, postgres}
//!
//! Aggregate contract (P5a): after process death + reopen, definitions (including
//! typed_indexes), counters/metrics, item IDs, request_id replay, lifecycle claim
//! surface, and typed-index queries match the pre-crash snapshot. Live Postgres
//! cells fail closed when `FIREWEED_PG_TEST_URL` is unset (zero silent skips).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ClientItemKey, CompoundIndexDef, CompoundIndexField, ConfigSecret, EligibilityPolicy, Fireweed,
    GateKeyPolicy, IndexDeclaration, IndexType, LogConfig, NewItem, ObjectLogAuthority,
    ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, PostgresMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig,
    ProjectionStoreConfig, QueueDefinition, QueueId, QueueIndex, QueueKey, RecoveryAction,
    RecoveryPolicy, RecurrencePolicy, RequestId, ResponseBarrier, RetryPolicy, SegmentConfig,
    StorageConfig, SystemClock, TenantId,
};

static ORD: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let n = ORD.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-p5an-{}-{}-{n}",
            label,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn segments() -> SegmentConfig {
    SegmentConfig {
        target_bytes: 1024 * 1024,
        max_latency_ms: 5,
    }
}

fn pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL").unwrap_or_else(|_| {
        panic!("FIREWEED_PG_TEST_URL must be set for P5aN live Postgres Class A cells (zero skips)")
    })
}

fn definition(cell: &str) -> QueueDefinition {
    // Cell slug as queue_id keeps shared Postgres DSNs isolated across parallel cells.
    let slug = cell.replace(['-', '×', 'x', '/'], "_");
    QueueDefinition {
        tenant_id: TenantId::new("p5an").unwrap(),
        queue_id: QueueId::new(format!("q_{slug}")).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(4),
        },
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        // Prefer ADR-011 typed indexes (Postgres projection resolves IndexQueryPort via
        // typed_indexes only; legacy IndexSpec secondary is not required for the P5a bar).
        secondary_indexes: vec![],
        entity_schema: None,
        // Typed indexes must survive reopen/rebuild (fireweed-e6ae8137 root ownership).
        typed_indexes: vec![
            QueueIndex {
                name: "by_kind_suppressed".into(),
                declaration: IndexDeclaration::Compound(CompoundIndexDef {
                    fields: vec![
                        CompoundIndexField {
                            field: "kind".into(),
                            index_type: IndexType::String,
                        },
                        CompoundIndexField {
                            field: "suppressed".into(),
                            index_type: IndexType::Boolean,
                        },
                    ],
                    unique: false,
                }),
            },
            QueueIndex {
                name: "by_customer_region".into(),
                declaration: IndexDeclaration::Compound(CompoundIndexDef {
                    fields: vec![
                        CompoundIndexField {
                            field: "customer".into(),
                            index_type: IndexType::String,
                        },
                        CompoundIndexField {
                            field: "region".into(),
                            index_type: IndexType::String,
                        },
                    ],
                    unique: true,
                }),
            },
        ],
        emit_change_records: true,
    }
}

fn queue_key(cell: &str) -> QueueKey {
    let def = definition(cell);
    QueueKey::new(def.tenant_id, def.queue_id)
}

fn primary_item() -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new("primary").unwrap()),
        // High priority so ascending claim selects fillers first and leaves primary pending.
        priority: Some(PriorityValue::Int64(10_000)),
        payload: Some(bytes::Bytes::from_static(b"seed")),
        // Entity carries both typed-index field sets (kind/suppressed and customer/region).
        entity: Some(serde_json::json!({
            "kind": "effect",
            "suppressed": false,
            "customer": "acme",
            "region": "east"
        })),
        gate_keys: vec![],
        ..NewItem::default()
    }
}

fn filler_item(i: u64) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(format!("filler-{i}")).unwrap()),
        // Low priorities so claim/complete operate on fillers, not the primary.
        priority: Some(PriorityValue::Int64(i as i64)),
        entity: Some(serde_json::json!({"kind": "filler", "suppressed": false})),
        ..NewItem::default()
    }
}

struct Snapshot {
    definition: QueueDefinition,
    primary_id: fireweed::ItemId,
    pending: u64,
    leased: u64,
    complete: u64,
    failed: u64,
}

/// Seed durable Class A state, then drop the handle (process death).
async fn seed(cell: &str, fireweed: &Fireweed) -> Snapshot {
    let q = queue_key(cell);
    let def = definition(cell);
    assert!(
        fireweed
            .create_queue(def.clone())
            .await
            .unwrap_or_else(|e| panic!("{cell}: create_queue: {e}"))
            .created,
        "{cell}: first create_queue must report created"
    );

    let push_rid = RequestId::new("p5an-push-primary").unwrap();
    let (primary_id, disp) = fireweed
        .push_with_request_id(&q, push_rid.clone(), primary_item())
        .await
        .unwrap_or_else(|e| panic!("{cell}: push primary: {e}"));
    assert_eq!(disp, fireweed::PushDisposition::Fresh);
    let (replay_id, replay_disp) = fireweed
        .push_with_request_id(&q, push_rid, primary_item())
        .await
        .unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replay_id, primary_id);

    // Fillers: complete 3, leave 2 leased, leave rest pending.
    for i in 0..8 {
        fireweed
            .push(&q, filler_item(i))
            .await
            .unwrap_or_else(|e| panic!("{cell}: push filler {i}: {e}"));
    }
    let to_complete = fireweed.claim(&q, 3, 60_000).await.unwrap();
    assert_eq!(to_complete.len(), 3, "{cell}: complete batch");
    // Prefer completing fillers, not primary (primary has lowest priority 10).
    fireweed
        .complete(&q, to_complete.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let still_leased = fireweed.claim(&q, 2, 60_000).await.unwrap();
    assert_eq!(still_leased.len(), 2, "{cell}: leased batch");
    let _ = still_leased; // leave leased across crash

    let m = fireweed
        .metrics(&q)
        .await
        .unwrap_or_else(|e| panic!("{cell}: metrics before crash: {e}"));
    let accounted = m.pending + m.leased + m.complete + m.failed;
    assert_eq!(
        accounted, 9,
        "{cell}: every accepted item accounted before crash (got {accounted})"
    );

    // Typed index must answer pre-crash (proves index materialization before reopen).
    let typed = fireweed
        .query_index_typed(
            &q,
            "by_kind_suppressed",
            &[serde_json::json!("effect"), serde_json::json!(false)],
        )
        .await
        .unwrap_or_else(|e| panic!("{cell}: typed index pre-crash: {e}"));
    assert_eq!(
        typed.len(),
        1,
        "{cell}: typed index finds primary before crash"
    );
    assert_eq!(typed[0].item_id, primary_id);

    Snapshot {
        definition: def,
        primary_id,
        pending: m.pending,
        leased: m.leased,
        complete: m.complete,
        failed: m.failed,
    }
}

/// Reopen recovery assertions for the P5a aggregate contract.
async fn verify(cell: &str, fireweed: &Fireweed, snap: Snapshot) {
    let q = queue_key(cell);

    // Definition + typed_indexes must round-trip (control-plane catalog rehydration).
    let recovered = fireweed
        .queue_definition(&q)
        .await
        .unwrap_or_else(|e| panic!(
            "{cell}: queue_definition after reopen failed — control-plane registry not rehydrated: {e}"
        ));
    assert_eq!(
        recovered, snap.definition,
        "{cell}: recovered definition (incl. typed_indexes) must equal seeded definition"
    );
    assert!(
        !recovered.typed_indexes.is_empty(),
        "{cell}: typed_indexes must not be dropped on recovery"
    );

    // Metrics without re-create_queue — the worker_crash_recovery_e2e root proof.
    let m = fireweed
        .metrics(&q)
        .await
        .unwrap_or_else(|e| panic!(
            "{cell}: metrics after reopen without create_queue failed — registry/rehydration gap: {e}"
        ));
    assert_eq!(
        m.complete, snap.complete,
        "{cell}: complete survived reopen"
    );
    assert_eq!(m.pending, snap.pending, "{cell}: pending survived reopen");
    assert_eq!(m.leased, snap.leased, "{cell}: leased survived reopen");
    assert_eq!(m.failed, snap.failed, "{cell}: failed survived reopen");
    assert_eq!(
        m.pending + m.leased + m.complete + m.failed,
        9,
        "{cell}: no accepted item lost across reopen"
    );

    // request_id replay converges on the same primary id.
    let (replayed, disp) = fireweed
        .push_with_request_id(
            &q,
            RequestId::new("p5an-push-primary").unwrap(),
            primary_item(),
        )
        .await
        .unwrap_or_else(|e| panic!("{cell}: request_id replay after reopen: {e}"));
    assert_eq!(disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed, snap.primary_id);

    // Live item identity + secondary unique index.
    let live = fireweed
        .live_item(&q, ClientItemKey::new("primary").unwrap())
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{cell}: primary live_item missing after reopen"));
    assert_eq!(live.item_id, snap.primary_id);

    // Typed unique index after reopen (customer/region).
    let unique = fireweed
        .query_index_unique_typed(
            &q,
            "by_customer_region",
            &[serde_json::json!("acme"), serde_json::json!("east")],
        )
        .await
        .unwrap_or_else(|e| panic!("{cell}: typed unique index after reopen: {e}"))
        .unwrap_or_else(|| panic!("{cell}: typed unique miss after reopen"));
    assert_eq!(unique.item_id, snap.primary_id);

    // Typed multi-field index after reopen — fireweed-e6ae8137 / P5aN typed-index ownership.
    let typed = fireweed
        .query_index_typed(
            &q,
            "by_kind_suppressed",
            &[serde_json::json!("effect"), serde_json::json!(false)],
        )
        .await
        .unwrap_or_else(|e| panic!("{cell}: typed index after reopen: {e}"));
    assert_eq!(
        typed.len(),
        1,
        "{cell}: typed index must find primary after reopen/rebuild"
    );
    assert_eq!(typed[0].item_id, snap.primary_id);

    // Lifecycle: claim a pending filler and complete it.
    if snap.pending > 0 {
        let claimed = fireweed
            .claim(&q, 1, 60_000)
            .await
            .unwrap_or_else(|e| panic!("{cell}: claim after reopen: {e}"));
        assert_eq!(claimed.len(), 1, "{cell}: claim after reopen");
        fireweed
            .complete(&q, claimed.iter().map(|c| c.item_id))
            .await
            .unwrap_or_else(|e| panic!("{cell}: complete after reopen: {e}"));
    }
}

async fn assert_reopen(cell: &str, open: impl Fn() -> Fireweed) {
    let fireweed = open();
    let snap = seed(cell, &fireweed).await;
    drop(fireweed);
    let reopened = open();
    verify(cell, &reopened, snap).await;
    drop(reopened);
}

/// Seed on `first`, drop it, then open `reopen` only after process death so recovery
/// rehydrates from durable state (not a concurrent second handle opened on an empty catalog).
async fn assert_reopen_after_seed<F, Fut>(cell: &str, first: Fireweed, reopen: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Fireweed>,
{
    let snap = seed(cell, &first).await;
    drop(first);
    let second = reopen().await;
    verify(cell, &second, snap).await;
    drop(second);
}

// ---------------------------------------------------------------------------
// Local deterministic Class A cells (sqlite / filesystem × memory|sqlite)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn p5an_sqlite_memory_reopen() {
    let root = FixtureRoot::new("sqlite_memory");
    let path = root.path().join("log.sqlite");
    assert_reopen("sqlite--memory", || {
        fireweed::open_sqlite(path.to_str().unwrap(), Arc::new(SystemClock))
            .expect("open sqlite×memory")
    })
    .await;
}

#[tokio::test]
async fn p5an_sqlite_sqlite_reopen() {
    let root = FixtureRoot::new("sqlite_sqlite");
    let log = root.path().join("log.sqlite");
    let proj = root.path().join("projection.sqlite");
    assert_reopen("sqlite--sqlite", || {
        fireweed::open_sqlite_sqlite_projection(
            log.to_str().unwrap(),
            proj.to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite×sqlite")
    })
    .await;
}

#[tokio::test]
async fn p5an_filesystem_memory_reopen() {
    let root = FixtureRoot::new("filesystem_memory");
    let ol = root.path().join("object-log");
    assert_reopen("filesystem--memory", || {
        fireweed::open_objectlog(&ol, Arc::new(SystemClock)).expect("open filesystem×memory")
    })
    .await;
}

#[tokio::test]
async fn p5an_filesystem_sqlite_reopen() {
    let root = FixtureRoot::new("filesystem_sqlite");
    let cfg = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.path().join("object-log"),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: segments(),
        namespace: format!("p5an-fs-sqlite-{}", std::process::id()),
        recovery: RecoveryPolicy {
            // Rebuild path is the sanctioned typed-index recovery route (e6ae8137).
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    };
    assert_reopen("filesystem--sqlite", || {
        fireweed::open_objectlog_sqlite(cfg.clone(), Arc::new(SystemClock))
            .expect("open filesystem×sqlite")
    })
    .await;
}

// ---------------------------------------------------------------------------
// Live Postgres Class A cells — fail closed without FIREWEED_PG_TEST_URL
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres_cells {
    use super::*;

    fn storage_cfg_sqlite_postgres(log: PathBuf, url: &str, schema: &str) -> StorageConfig {
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Sqlite { path: log };
        cfg.projection = ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(url.to_string()),
        };
        cfg.namespace = schema.to_string();
        cfg
    }

    fn storage_cfg_postgres_memory(url: &str, schema: &str) -> StorageConfig {
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Postgres {
            url: ConfigSecret::new(url.to_string()),
            schema: Some(schema.to_string()),
            mode: PostgresMode::LogReplay,
            node_id: None,
            coordination: None,
        };
        cfg.projection = ProjectionStoreConfig::Memory;
        cfg.namespace = schema.to_string();
        cfg
    }

    fn storage_cfg_postgres_sqlite(url: &str, schema: &str, proj: PathBuf) -> StorageConfig {
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Postgres {
            url: ConfigSecret::new(url.to_string()),
            schema: Some(schema.to_string()),
            mode: PostgresMode::LogReplay,
            node_id: None,
            coordination: None,
        };
        cfg.projection = ProjectionStoreConfig::Sqlite { path: proj };
        cfg.namespace = schema.to_string();
        cfg
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5an_sqlite_postgres_reopen() {
        let url = pg_url();
        let root = FixtureRoot::new("sqlite_postgres");
        let log = root.path().join("log.sqlite");
        let schema = format!("p5an_sqlite_pg_{}", std::process::id());
        let first = fireweed::open_async(
            storage_cfg_sqlite_postgres(log.clone(), &url, &schema),
            Arc::new(SystemClock),
        )
        .await
        .expect("open sqlite×postgres");
        assert_reopen_after_seed("sqlite--postgres", first, || {
            let log = log.clone();
            let url = url.clone();
            let schema = schema.clone();
            async move {
                fireweed::open_async(
                    storage_cfg_sqlite_postgres(log, &url, &schema),
                    Arc::new(SystemClock),
                )
                .await
                .expect("reopen sqlite×postgres")
            }
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5an_postgres_memory_reopen() {
        let url = pg_url();
        let schema = format!("p5an_pg_mem_{}", std::process::id());
        let first = fireweed::open_async(
            storage_cfg_postgres_memory(&url, &schema),
            Arc::new(SystemClock),
        )
        .await
        .expect("open postgres×memory");
        assert_reopen_after_seed("postgres--memory", first, || {
            let url = url.clone();
            let schema = schema.clone();
            async move {
                fireweed::open_async(
                    storage_cfg_postgres_memory(&url, &schema),
                    Arc::new(SystemClock),
                )
                .await
                .expect("reopen postgres×memory")
            }
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5an_postgres_sqlite_reopen() {
        let url = pg_url();
        let root = FixtureRoot::new("postgres_sqlite");
        let schema = format!("p5an_pg_sqlite_{}", std::process::id());
        let proj = root.path().join("projection.sqlite");
        let first = fireweed::open_async(
            storage_cfg_postgres_sqlite(&url, &schema, proj.clone()),
            Arc::new(SystemClock),
        )
        .await
        .expect("open postgres×sqlite");
        assert_reopen_after_seed("postgres--sqlite", first, || {
            let url = url.clone();
            let schema = schema.clone();
            let proj = proj.clone();
            async move {
                fireweed::open_async(
                    storage_cfg_postgres_sqlite(&url, &schema, proj),
                    Arc::new(SystemClock),
                )
                .await
                .expect("reopen postgres×sqlite")
            }
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5an_postgres_postgres_reopen() {
        let url = pg_url();
        let schema = format!("p5an_pg_pg_{}", std::process::id());
        let first = fireweed::open_postgres_runtime_async(
            fireweed::PostgresRuntimeConfig {
                url: ConfigSecret::new(url.clone()),
                schema: Some(schema.clone()),
                mode: PostgresMode::Relational,
                node_id: None,
                coordination: None,
            },
            Arc::new(SystemClock),
        )
        .await
        .expect("open postgres×postgres");
        assert_reopen_after_seed("postgres--postgres", first, || {
            let url = url.clone();
            let schema = schema.clone();
            async move {
                fireweed::open_postgres_runtime_async(
                    fireweed::PostgresRuntimeConfig {
                        url: ConfigSecret::new(url),
                        schema: Some(schema),
                        mode: PostgresMode::Relational,
                        node_id: None,
                        coordination: None,
                    },
                    Arc::new(SystemClock),
                )
                .await
                .expect("reopen postgres×postgres")
            }
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5an_filesystem_postgres_reopen() {
        let url = pg_url();
        let root = FixtureRoot::new("filesystem_postgres");
        let cfg = ObjectLogRuntimeConfig {
            object_log: ObjectLogStorage::Local {
                root: root.path().join("object-log"),
            },
            authority: ObjectLogAuthority::NativeConditionalWrite,
            projection: ProjectionConfig::Postgres {
                url: ConfigSecret::new(url),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: format!("p5an-fs-pg-{}", std::process::id()),
            recovery: RecoveryPolicy {
                incompatible_projection: RecoveryAction::RebuildProjection,
                verify_checksums: true,
                max_tail_commands: 1_000_000,
            },
        };
        let first = fireweed::open_objectlog_postgres_async(cfg.clone(), Arc::new(SystemClock))
            .await
            .expect("open filesystem×postgres");
        assert_reopen_after_seed("filesystem--postgres", first, || {
            let cfg = cfg.clone();
            async move {
                fireweed::open_objectlog_postgres_async(cfg, Arc::new(SystemClock))
                    .await
                    .expect("reopen filesystem×postgres")
            }
        })
        .await;
    }
}
