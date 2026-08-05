//! Provider-neutral shared API-005 public-interface suite (local cells).
//!
//! Cell IDs use the authority-manifest separator (`log--projection[--variant]`).
//! Provider brand strings are forbidden in fixtures; live S3 provenance is P4s.
//! Method coverage is discovery-derived via `scripts/ci/api005_suite_ownership.py`.

#[path = "support/public_interface.rs"]
mod public_interface;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ConfigSecret, Fireweed, ObjectLogAuthority, ObjectLogRuntimeConfig, ObjectLogStorage,
    ProjectionConfig, RecoveryAction, RecoveryPolicy, ResponseBarrier, SegmentConfig, SystemClock,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(cell: &str) -> Self {
        let ordinal = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-public-interface-{cell}-{}-{ordinal}",
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

async fn assert_cell(
    cell: &str,
    expect_projection_control: bool,
    expect_atomic_commit: bool,
    build: impl FnOnce(&Path) -> Fireweed,
) {
    let root = FixtureRoot::new(cell);
    let fireweed = build(root.path());
    if expect_atomic_commit {
        public_interface::run(cell, &fireweed, expect_projection_control).await;
    } else {
        public_interface::run_with_commit_boundary(
            cell,
            &fireweed,
            expect_projection_control,
            expect_atomic_commit,
        )
        .await;
    }
    drop(fireweed);
}

fn filesystem_sqlite_config(
    root: &Path,
    barrier: ResponseBarrier,
    namespace: &str,
) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.join("object-log"),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Sqlite {
            path: root.join("projection.sqlite"),
        },
        response_barrier: barrier,
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace: namespace.into(),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    }
}

fn filesystem_sqlite(root: &Path, barrier: ResponseBarrier, namespace: &str) -> Fireweed {
    fireweed::open_objectlog_sqlite(
        filesystem_sqlite_config(root, barrier, namespace),
        Arc::new(SystemClock),
    )
    .unwrap()
}

#[test]
fn objectlog_authority_validation_accepts_native_conditional_write() {
    let root = FixtureRoot::new("authority-validation");
    let mut local =
        filesystem_sqlite_config(root.path(), ResponseBarrier::Strict, "authority-validation");
    local.authority = ObjectLogAuthority::NativeConditionalWrite;
    local.validate().unwrap();

    let mut s3 = local.clone();
    s3.object_log = ObjectLogStorage::S3Compatible {
        endpoint: "http://127.0.0.1:9".into(),
        bucket: "fixture".into(),
        region: "us-east-1".into(),
        access_key_id: ConfigSecret::new("fixture-key"),
        secret_access_key: ConfigSecret::new("fixture-secret"),
        allow_insecure_http: true,
    };
    s3.authority = ObjectLogAuthority::NativeConditionalWrite;
    s3.validate().unwrap();
}

#[tokio::test]
async fn memory_memory_public_interface() {
    assert_cell("memory--memory", false, true, |_| {
        fireweed::open_memory(Arc::new(SystemClock))
    })
    .await;
}

#[tokio::test]
async fn sqlite_memory_public_interface() {
    assert_cell("sqlite--memory", false, true, |root| {
        fireweed::open_sqlite(
            root.join("log.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn sqlite_sqlite_public_interface() {
    assert_cell("sqlite--sqlite", false, true, |root| {
        fireweed::open_sqlite_relational(
            root.join("relational.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn filesystem_memory_public_interface() {
    assert_cell("filesystem--memory", false, true, |root| {
        fireweed::open_objectlog(root.join("object-log"), Arc::new(SystemClock)).unwrap()
    })
    .await;
}

#[tokio::test]
async fn filesystem_sqlite_strict_public_interface() {
    assert_cell("filesystem--sqlite--strict", true, true, |root| {
        filesystem_sqlite(root, ResponseBarrier::Strict, "filesystem-sqlite-strict")
    })
    .await;
}

#[tokio::test]
async fn filesystem_sqlite_async_public_interface() {
    assert_cell("filesystem--sqlite--async", true, false, |root| {
        filesystem_sqlite(
            root,
            ResponseBarrier::AsyncProjection,
            "filesystem-sqlite-async",
        )
    })
    .await;
}
