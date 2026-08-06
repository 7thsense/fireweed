//! P9N — transaction parity (commit, multi-claim, explain, side records, atomic
//! rejection) on the 12 non-S3 cells.
//!
//! Public matrix without S3 (turso projection is exercised elsewhere / default axis):
//!   log ∈ {memory, sqlite, postgres, filesystem}
//!   × projection ∈ {memory, sqlite, postgres}
//! = 12 cells.
//!
//! Method coverage is the P9 contract set, exercised via
//! `support/public_interface::run_p9_surface`. Live Postgres cells
//! fail closed when `FIREWEED_PG_TEST_URL` is unset (zero silent skips).

#[path = "support/public_interface.rs"]
mod public_interface;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    Fireweed, ProjectionStoreConfig, ResponseBarrier, SegmentConfig, StorageConfig, SystemClock,
    open,
};

#[cfg(feature = "postgres")]
use fireweed::{ConfigSecret, LogConfig, PostgresMode};

static ORD: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let n = ORD.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-p9n-{}-{}-{n}",
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

#[cfg(feature = "postgres")]
fn pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL").unwrap_or_else(|_| {
        panic!(
            "FIREWEED_PG_TEST_URL must be set for P9N live Postgres cells (zero silent skips)"
        )
    })
}

async fn run_cell(cell: &str, expect_atomic_commit: bool, open_fw: impl FnOnce() -> Fireweed) {
    let fw = open_fw();
    public_interface::run_p9_surface(cell, &fw, expect_atomic_commit).await;
    drop(fw);
}

async fn run_cell_async(
    cell: &str,
    expect_atomic_commit: bool,
    open_fw: impl std::future::Future<Output = Fireweed>,
) {
    let fw = open_fw.await;
    public_interface::run_p9_surface(cell, &fw, expect_atomic_commit).await;
    drop(fw);
}

// ---------------------------------------------------------------------------
// Local deterministic cells (no live Postgres)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn p9n_memory_memory_query_parity() {
    run_cell("memory--memory", true, || {
        fireweed::open_memory(Arc::new(SystemClock))
    })
    .await;
}

#[tokio::test]
async fn p9n_memory_sqlite_query_parity() {
    let root = FixtureRoot::new("memory_sqlite");
    run_cell("memory--sqlite", true, || {
        let mut cfg = StorageConfig::memory();
        cfg.projection = ProjectionStoreConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        };
        open(cfg, Arc::new(SystemClock)).expect("open memory×sqlite")
    })
    .await;
}

#[tokio::test]
async fn p9n_sqlite_memory_query_parity() {
    let root = FixtureRoot::new("sqlite_memory");
    run_cell("sqlite--memory", true, || {
        fireweed::open_sqlite(
            root.path().join("log.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite×memory")
    })
    .await;
}

#[tokio::test]
async fn p9n_sqlite_sqlite_query_parity() {
    let root = FixtureRoot::new("sqlite_sqlite");
    run_cell("sqlite--sqlite", true, || {
        fireweed::open_sqlite_relational(
            root.path().join("relational.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite×sqlite")
    })
    .await;
}

#[tokio::test]
async fn p9n_filesystem_memory_query_parity() {
    let root = FixtureRoot::new("filesystem_memory");
    run_cell("filesystem--memory", true, || {
        fireweed::open_objectlog(root.path().join("object-log"), Arc::new(SystemClock))
            .expect("open filesystem×memory")
    })
    .await;
}

#[tokio::test]
async fn p9n_filesystem_sqlite_query_parity() {
    let root = FixtureRoot::new("filesystem_sqlite");
    run_cell("filesystem--sqlite", true, || {
        let cfg = fireweed::ObjectLogRuntimeConfig {
            object_log: fireweed::ObjectLogStorage::Local {
                root: root.path().join("object-log"),
            },
            authority: fireweed::ObjectLogAuthority::NativeConditionalWrite,
            projection: fireweed::ProjectionConfig::Sqlite {
                path: root.path().join("projection.sqlite"),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: format!("p9n-filesystem-sqlite-{}", std::process::id()),
            recovery: fireweed::RecoveryPolicy::default(),
        };
        fireweed::open_objectlog_sqlite(cfg, Arc::new(SystemClock)).expect("open filesystem×sqlite")
    })
    .await;
}

// ---------------------------------------------------------------------------
// Live Postgres cells — require `--features postgres` + FIREWEED_PG_TEST_URL
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres_cells {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_memory_postgres_query_parity() {
        let url = pg_url();
        let mut cfg = StorageConfig::memory();
        cfg.projection = ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(url),
        };
        cfg.namespace = format!("p9n_mem_pg_{}", std::process::id());
        run_cell_async("memory--postgres", true, async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open memory×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_sqlite_postgres_query_parity() {
        let url = pg_url();
        let root = FixtureRoot::new("sqlite_postgres");
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Sqlite {
            path: root.path().join("log.sqlite"),
        };
        cfg.projection = ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(url),
        };
        cfg.namespace = format!("p9n_sqlite_pg_{}", std::process::id());
        run_cell_async("sqlite--postgres", true, async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open sqlite×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_postgres_memory_query_parity() {
        let url = pg_url();
        // Isolate log schema so concurrent matrix runs do not collide on create_queue/push.
        let schema = format!("p9n_pg_mem_{}", std::process::id());
        run_cell_async("postgres--memory", true, async {
            fireweed::open_postgres_runtime_async(
                fireweed::PostgresRuntimeConfig {
                    url: ConfigSecret::new(url),
                    schema: Some(schema),
                    mode: PostgresMode::LogReplay,
                    node_id: None,
                    coordination: None,
                },
                Arc::new(SystemClock),
            )
            .await
            .expect("open postgres×memory")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_postgres_sqlite_query_parity() {
        let url = pg_url();
        let root = FixtureRoot::new("postgres_sqlite");
        let schema = format!("p9n_pg_sqlite_{}", std::process::id());
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Postgres {
            url: ConfigSecret::new(url),
            schema: Some(schema.clone()),
            mode: PostgresMode::LogReplay,
            node_id: None,
            coordination: None,
        };
        cfg.projection = ProjectionStoreConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        };
        cfg.namespace = schema;
        run_cell_async("postgres--sqlite", true, async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open postgres×sqlite")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_postgres_postgres_query_parity() {
        let url = pg_url();
        // Isolate relational schema so concurrent matrix runs do not collide.
        let schema = format!("p9n_pg_pg_{}", std::process::id());
        run_cell_async("postgres--postgres", true, async {
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
            .expect("open postgres×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9n_filesystem_postgres_query_parity() {
        let url = pg_url();
        let root = FixtureRoot::new("filesystem_postgres");
        let cfg = fireweed::ObjectLogRuntimeConfig {
            object_log: fireweed::ObjectLogStorage::Local {
                root: root.path().join("object-log"),
            },
            authority: fireweed::ObjectLogAuthority::NativeConditionalWrite,
            projection: fireweed::ProjectionConfig::Postgres {
                url: ConfigSecret::new(url),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: format!("p9n-fs-pg-{}", std::process::id()),
            recovery: fireweed::RecoveryPolicy::default(),
        };
        run_cell_async("filesystem--postgres", true, async {
            fireweed::open_objectlog_postgres_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open filesystem×postgres")
        })
        .await;
    }
}
