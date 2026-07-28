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
    build: impl FnOnce(&Path) -> Fireweed,
) {
    let root = FixtureRoot::new(cell);
    let fireweed = build(root.path());
    public_interface::run(cell, &fireweed, expect_projection_control).await;
    drop(fireweed);
}

fn objectlog_sqlite_config(
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

fn objectlog_sqlite(root: &Path, barrier: ResponseBarrier, namespace: &str) -> Fireweed {
    fireweed::open_objectlog_sqlite(
        objectlog_sqlite_config(root, barrier, namespace),
        Arc::new(SystemClock),
    )
    .unwrap()
}

#[test]
fn objectlog_authority_validation_accepts_native_conditional_write() {
    let root = FixtureRoot::new("authority-validation");
    let mut local =
        objectlog_sqlite_config(root.path(), ResponseBarrier::Strict, "authority-validation");
    local.authority = ObjectLogAuthority::NativeConditionalWrite;
    local.validate().unwrap();

    let mut s3 = local;
    s3.object_log = ObjectLogStorage::S3Compatible {
        endpoint: "https://objects.example.test".into(),
        bucket: "fireweed".into(),
        region: "us-east-1".into(),
        access_key_id: ConfigSecret::new("access"),
        secret_access_key: ConfigSecret::new("secret"),
        allow_insecure_http: false,
    };
    s3.authority = ObjectLogAuthority::NativeConditionalWrite;
    s3.validate().unwrap();
}

#[tokio::test]
async fn memory_public_interface() {
    assert_cell("memory", false, |_| {
        fireweed::open_memory(Arc::new(SystemClock))
    })
    .await;
}

#[tokio::test]
async fn sqlite_log_public_interface() {
    assert_cell("sqlite-log", false, |root| {
        fireweed::open_sqlite(
            root.join("log.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn sqlite_relational_public_interface() {
    assert_cell("sqlite-relational", false, |root| {
        fireweed::open_sqlite_relational(
            root.join("relational.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn objectlog_local_direct_public_interface() {
    assert_cell("objectlog-local-direct", false, |root| {
        fireweed::open_objectlog(root.join("object-log"), Arc::new(SystemClock)).unwrap()
    })
    .await;
}

#[tokio::test]
async fn objectlog_sqlite_strict_public_interface() {
    assert_cell("objectlog-sqlite-strict", true, |root| {
        objectlog_sqlite(root, ResponseBarrier::Strict, "public-interface-strict")
    })
    .await;
}

#[tokio::test]
async fn objectlog_sqlite_async_public_interface() {
    assert_cell("objectlog-sqlite-async", true, |root| {
        objectlog_sqlite(
            root,
            ResponseBarrier::AsyncProjection,
            "public-interface-async",
        )
    })
    .await;
}
