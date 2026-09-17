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
    ConfigSecret, Fireweed, LogConfig, ObjectLogAuthority, ProjectionStoreConfig, RecoveryPolicy,
    ResponseBarrier, SegmentConfig, StorageConfig, SystemClock,
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

fn objectlog_storage(
    log: LogConfig,
    projection: ProjectionStoreConfig,
    namespace: &str,
) -> StorageConfig {
    StorageConfig {
        log,
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace: namespace.into(),
        recovery: RecoveryPolicy::default(),
    }
}

#[test]
fn objectlog_authority_validation_accepts_native_conditional_write() {
    let root = FixtureRoot::new("authority-validation");
    objectlog_storage(
        LogConfig::Filesystem {
            root: root.path().join("object-log"),
        },
        ProjectionStoreConfig::Memory,
        "authority-validation",
    )
    .validate()
    .unwrap();

    objectlog_storage(
        LogConfig::S3 {
            endpoint: "http://127.0.0.1:9".into(),
            bucket: "fixture".into(),
            region: "us-east-1".into(),
            access_key_id: ConfigSecret::new("fixture-key"),
            secret_access_key: ConfigSecret::new("fixture-secret"),
            allow_insecure_http: true,
        },
        ProjectionStoreConfig::Memory,
        "authority-validation-s3",
    )
    .validate()
    .unwrap();
}

#[tokio::test]
async fn memory_memory_public_interface() {
    assert_cell("memory--memory", false, true, |_| {
        fireweed::open_memory(Arc::new(SystemClock))
    })
    .await;
}

#[cfg(all(feature = "memory", feature = "turso"))]
#[tokio::test]
async fn memory_turso_public_interface() {
    assert_cell("memory--turso", false, true, |root| {
        let mut config = StorageConfig::memory();
        config.projection = ProjectionStoreConfig::Turso {
            path: root.join("projection.db"),
        };
        fireweed::open(config, Arc::new(SystemClock)).unwrap()
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

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_strict_public_interface() {
    assert_cell("filesystem--turso--strict", false, true, |root| {
        filesystem_turso(root, ResponseBarrier::Strict, "filesystem-turso-strict")
    })
    .await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_async_public_interface() {
    assert_cell("filesystem--turso--async", false, true, |root| {
        filesystem_turso(
            root,
            ResponseBarrier::AsyncProjection,
            "filesystem-turso-async",
        )
    })
    .await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
fn filesystem_turso(root: &Path, barrier: ResponseBarrier, namespace: &str) -> Fireweed {
    let mut storage = objectlog_storage(
        LogConfig::Filesystem {
            root: root.join("log"),
        },
        ProjectionStoreConfig::Turso {
            path: root.join("projection.db"),
        },
        namespace,
    );
    storage.response_barrier = barrier;
    if barrier == ResponseBarrier::AsyncProjection {
        storage.async_projection = Some(fireweed::AsyncProjectionSpec::default());
    }
    fireweed::open(storage, Arc::new(SystemClock)).unwrap()
}
