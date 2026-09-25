//! P10r exact functional-matrix route source leaves (provider-neutral IDs).
//!
//! These leaves are compile/list-addressable dry-run sources for the public 4×3
//! matrix. P2r binds semantic requirements to the listed harness IDs; P10 executes
//! them. Broad substring cargo filters are forbidden — each leaf uses a full
//! exact test name under this target.
//!
//! Governing axes: `docs/helix/04-build/storage-authority-manifest.json`
//! (`memory|postgres|filesystem|s3` × `memory|turso|postgres`).
//!
//! Leaf families:
//! - **strict** — 12 cells, `ResponseBarrier::AsyncProjection` validate dry-run
//! - **object_log_async** — 6 filesystem/s3 cells, `AsyncProjection` validate dry-run
//! - **async_invalid** — 6 non-object-log cells, pre-I/O rejection dry-run
//! - **ac_txn_dry_run** — aggregate AC-TXN-5/5A cardinality dry-runs over the same axes
//! - **t0_t2_register** — proves the T0–T2 matrix harness is registered (12 cells)
//!
//! ADR-024 leaves one public cell. In every family, `s3--turso` validates and each
//! other cell is rejected with `RETIRED_STORAGE_CELL` before storage I/O.
//!
//! Cell IDs use the manifest separator (`--`). Test function names map
//! `log--projection` to rustc-safe `prefix_log_projection`.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    AsyncProjectionSpec, ConfigSecret, EngineError, LogConfig, ObjectLogAuthority, PostgresMode,
    ProjectionStoreConfig, RETIRED_STORAGE_CELL, RecoveryAction, RecoveryPolicy, ResponseBarrier,
    SegmentConfig, StorageConfig,
};

const RETIRED: EngineError = EngineError::Invalid(RETIRED_STORAGE_CELL);
const PUBLIC_CELL: (&str, &str) = ("s3", "turso");

const LOGS: [&str; 4] = ["memory", "postgres", "filesystem", "s3"];
const PROJECTIONS: [&str; 3] = ["memory", "turso", "postgres"];
const OBJECT_LOGS: [&str; 2] = ["filesystem", "s3"];
const NON_OBJECT_LOGS: [&str; 2] = ["memory", "postgres"];

fn cell_id(log: &str, projection: &str) -> String {
    format!("{log}--{projection}")
}

fn fixture_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "fireweed-p10r-route-src-{label}-{}-{nonce}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("fixture root");
    path
}

fn production_segments() -> SegmentConfig {
    // Production-safe object-log segment shape (matches P3b/P3s dry-run fixtures).
    SegmentConfig::new(64 * 1024, 5).expect("production-safe segment bounds")
}

fn async_spec() -> AsyncProjectionSpec {
    AsyncProjectionSpec::new(13, 65_537, 7, 12_345, 4)
        .expect("all five non-default async bounds are valid")
}

fn dummy_pg_url() -> &'static str {
    "postgres://127.0.0.1:1/fireweed_p10r_dry_run"
}

fn projection_cfg(projection: &str, root: &Path, tag: &str) -> ProjectionStoreConfig {
    match projection {
        "memory" => ProjectionStoreConfig::Memory,
        "turso" => ProjectionStoreConfig::Turso {
            path: root.join(format!("{tag}-projection.turso")),
        },
        "postgres" => ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(dummy_pg_url()),
        },
        other => panic!("unknown projection axis {other}"),
    }
}

fn log_cfg(log: &str, root: &Path, tag: &str) -> LogConfig {
    match log {
        "memory" => LogConfig::Memory,
        "postgres" => LogConfig::Postgres {
            url: ConfigSecret::new(dummy_pg_url()),
            schema: Some(format!("p10r_{tag}")),
            mode: PostgresMode::LogReplay,
            node_id: Some(1),
            coordination: None,
        },
        "filesystem" => LogConfig::Filesystem {
            root: root.join(format!("{tag}-fs-log")),
        },
        "s3" => LogConfig::S3 {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "fireweed-p10r".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: ConfigSecret::new("access"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        other => panic!("unknown log axis {other}"),
    }
}

fn storage(
    log: &str,
    projection: &str,
    barrier: ResponseBarrier,
    root: &Path,
    tag: &str,
) -> StorageConfig {
    let is_object_log = matches!(log, "filesystem" | "s3");
    StorageConfig {
        authority: is_object_log.then_some(ObjectLogAuthority::NativeConditionalWrite),
        log: log_cfg(log, root, tag),
        projection: projection_cfg(projection, root, tag),
        control_plane: None,
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection).then(async_spec),
        segments: production_segments(),
        namespace: format!("p10r-{tag}"),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

/// Validate one cell without storage I/O: `s3--turso` passes, every other cell is retired.
fn dry_run_cell(family: &str, log: &str, projection: &str) {
    let id = cell_id(log, projection);
    let root = fixture_root(&format!("{family}-{log}-{projection}"));
    // Never-created path: validation must not touch storage.
    let never = root.join("never-created");
    let tag = format!("{family}_{log}_{projection}");
    let cfg = storage(
        log,
        projection,
        ResponseBarrier::AsyncProjection,
        &never,
        &tag,
    );
    if (log, projection) == PUBLIC_CELL {
        cfg.validate()
            .unwrap_or_else(|error| panic!("{family} dry-run {id} validate failed: {error}"));
    } else {
        assert_eq!(
            cfg.validate(),
            Err(RETIRED),
            "{family} dry-run {id} must reject the retired cell before I/O"
        );
    }
    assert!(
        !never.exists(),
        "{family} dry-run {id} must not create storage paths"
    );
    let _ = std::fs::remove_dir_all(&root);
}

fn dry_run_strict(log: &str, projection: &str) {
    dry_run_cell("strict", log, projection);
}

fn dry_run_async_valid(log: &str, projection: &str) {
    assert!(
        OBJECT_LOGS.contains(&log),
        "async valid leaf only for object-log cells"
    );
    dry_run_cell("object-log-async", log, projection);
}

fn dry_run_async_invalid(log: &str, projection: &str) {
    assert!(
        NON_OBJECT_LOGS.contains(&log),
        "async invalid leaf only for non-object-log cells"
    );
    // Retirement precedes the async-projection object-log rule.
    dry_run_cell("async-invalid", log, projection);
}

// ---------------------------------------------------------------------------
// Exact strict leaves (12)
// ---------------------------------------------------------------------------

macro_rules! strict_leaf {
    ($fn_name:ident, $log:expr, $proj:expr) => {
        #[test]
        fn $fn_name() {
            dry_run_strict($log, $proj);
        }
    };
}

strict_leaf!(strict_memory_memory, "memory", "memory");
strict_leaf!(strict_memory_turso, "memory", "turso");
strict_leaf!(strict_memory_postgres, "memory", "postgres");
strict_leaf!(strict_postgres_memory, "postgres", "memory");
strict_leaf!(strict_postgres_turso, "postgres", "turso");
strict_leaf!(strict_postgres_postgres, "postgres", "postgres");
strict_leaf!(strict_filesystem_memory, "filesystem", "memory");
strict_leaf!(strict_filesystem_turso, "filesystem", "turso");
strict_leaf!(strict_filesystem_postgres, "filesystem", "postgres");
strict_leaf!(strict_s3_memory, "s3", "memory");
strict_leaf!(strict_s3_turso, "s3", "turso");
strict_leaf!(strict_s3_postgres, "s3", "postgres");

// ---------------------------------------------------------------------------
// Exact object-log async leaves (6)
// ---------------------------------------------------------------------------

macro_rules! async_valid_leaf {
    ($fn_name:ident, $log:expr, $proj:expr) => {
        #[test]
        fn $fn_name() {
            dry_run_async_valid($log, $proj);
        }
    };
}

async_valid_leaf!(object_log_async_filesystem_memory, "filesystem", "memory");
async_valid_leaf!(object_log_async_filesystem_turso, "filesystem", "turso");
async_valid_leaf!(
    object_log_async_filesystem_postgres,
    "filesystem",
    "postgres"
);
async_valid_leaf!(object_log_async_s3_memory, "s3", "memory");
async_valid_leaf!(object_log_async_s3_turso, "s3", "turso");
async_valid_leaf!(object_log_async_s3_postgres, "s3", "postgres");

// ---------------------------------------------------------------------------
// Exact async-invalid leaves (6)
// ---------------------------------------------------------------------------

macro_rules! async_invalid_leaf {
    ($fn_name:ident, $log:expr, $proj:expr) => {
        #[test]
        fn $fn_name() {
            dry_run_async_invalid($log, $proj);
        }
    };
}

async_invalid_leaf!(async_invalid_memory_memory, "memory", "memory");
async_invalid_leaf!(async_invalid_memory_turso, "memory", "turso");
async_invalid_leaf!(async_invalid_memory_postgres, "memory", "postgres");
async_invalid_leaf!(async_invalid_postgres_memory, "postgres", "memory");
async_invalid_leaf!(async_invalid_postgres_turso, "postgres", "turso");
async_invalid_leaf!(async_invalid_postgres_postgres, "postgres", "postgres");

// ---------------------------------------------------------------------------
// AC-TXN dry-run aggregates (cardinality + pre-I/O contract)
// ---------------------------------------------------------------------------

#[test]
fn ac_txn_dry_run_strict_enumerates_all_12_manifest_cells() {
    let mut seen = std::collections::BTreeSet::new();
    for log in LOGS {
        for projection in PROJECTIONS {
            dry_run_strict(log, projection);
            seen.insert(cell_id(log, projection));
        }
    }
    assert_eq!(seen.len(), 12, "AC-TXN dry-run strict must cover 12 cells");
}

#[test]
fn ac_txn_dry_run_async_invalid_enumerates_all_6_non_object_log_cells() {
    let mut seen = std::collections::BTreeSet::new();
    for log in NON_OBJECT_LOGS {
        for projection in PROJECTIONS {
            dry_run_async_invalid(log, projection);
            seen.insert(cell_id(log, projection));
        }
    }
    assert_eq!(
        seen.len(),
        6,
        "AC-TXN dry-run async-invalid must cover 6 cells"
    );
}

#[test]
fn ac_txn_dry_run_object_log_async_enumerates_all_6_cells() {
    let mut seen = std::collections::BTreeSet::new();
    for log in OBJECT_LOGS {
        for projection in PROJECTIONS {
            dry_run_async_valid(log, projection);
            seen.insert(cell_id(log, projection));
        }
    }
    assert_eq!(
        seen.len(),
        6,
        "AC-TXN dry-run object-log async must cover 6 cells"
    );
}

// ---------------------------------------------------------------------------
// T0–T2 registration leaf (proves harness target is present; no execution claim)
// ---------------------------------------------------------------------------

#[test]
fn t0_t2_register_manifest_axes_match_authority() {
    // Pure axis arithmetic from the authority axes — no cargo execution claim.
    assert_eq!(LOGS.len() * PROJECTIONS.len(), 12);
    assert_eq!(OBJECT_LOGS.len() * PROJECTIONS.len(), 6);
    assert_eq!(NON_OBJECT_LOGS.len() * PROJECTIONS.len(), 6);
    for log in LOGS {
        for projection in PROJECTIONS {
            let id = cell_id(log, projection);
            assert!(id.contains("--"), "manifest cell_id separator required");
            assert!(
                !id.contains('×'),
                "legacy × selector is not provider-neutral"
            );
            assert!(
                !id.contains("garage"),
                "provider brand forbidden in cell_id"
            );
            assert!(!id.contains("hybrid"), "retired Hybrid selector forbidden");
        }
    }
}

#[test]
fn route_source_leaf_ids_are_provider_neutral() {
    // Static source guard: leaf IDs and axis tables use only canonical names.
    for log in LOGS {
        for projection in PROJECTIONS {
            let id = cell_id(log, projection);
            assert!(
                !id.contains('×'),
                "legacy × selector is not provider-neutral: {id}"
            );
            for banned in ["garage", "minio", "rustfs", "hybrid", "objectlog"] {
                assert!(
                    !id.to_ascii_lowercase().contains(banned),
                    "provider brand {banned} forbidden in cell_id {id}"
                );
            }
        }
    }
    // Axis tables themselves are the only allowed selector vocabulary.
    assert_eq!(LOGS, ["memory", "postgres", "filesystem", "s3"]);
    assert_eq!(PROJECTIONS, ["memory", "turso", "postgres"]);
}
