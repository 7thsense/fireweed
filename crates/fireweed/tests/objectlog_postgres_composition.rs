#![cfg(all(feature = "objectlog", feature = "postgres"))]
#![allow(dead_code, unused_imports)]

use postgres::{Client, NoTls};
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::*;
use fireweed_memory::ManualClock;

fn unique_fixture(name: &str) -> (PathBuf, String) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    (
        std::env::temp_dir().join(format!("fireweed-{name}-{nonce}")),
        format!("fireweed_{name}_{nonce}"),
    )
}

fn public_config(root: &Path, schema: &str, url: &str) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: root.to_path_buf(),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Postgres {
            url: ConfigSecret::new(url),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: schema.to_owned(),
        recovery: RecoveryPolicy::default(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_open_inside_tokio_returns_typed_error() {
    let (root, schema) = unique_fixture("tokio_sync_open");
    let error = match fireweed::open_objectlog_postgres(
        public_config(&root, &schema, "postgres://127.0.0.1:1/postgres"),
        Arc::new(ManualClock::at(1_000)),
    ) {
        Ok(_) => panic!("the synchronous constructor must reject an ambient Tokio runtime"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        EngineError::Invalid(
            "open_objectlog_postgres cannot run inside a Tokio runtime; use open_objectlog_postgres_async"
        )
    );
}
