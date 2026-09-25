//! Product storage fixtures: S3 (local RustFS) × Turso.
#![allow(dead_code)]
use fireweed::{Clock, EngineResult, Fireweed, ObjectLogAuthority, ResponseBarrier, StorageConfig};
use fireweed_objectlog::shared_s3_test_env;
use std::{path::Path, path::PathBuf, sync::Arc};

pub fn unique_namespace(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

pub fn product_config(root: &Path, namespace: &str) -> StorageConfig {
    let s3 = shared_s3_test_env();
    std::fs::create_dir_all(root).ok();
    let mut config = StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        root.join("projection.db"),
    );
    config.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
    config.namespace = namespace.to_owned();
    config
}

fn config(path: &str) -> StorageConfig {
    let root = PathBuf::from(format!("{path}.store"));
    product_config(&root, path)
}

pub fn open_log_memory(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    // Memory projection is retired; the product cell is s3 × turso.
    let _ = path;
    let _ = clock;
    Err(fireweed::EngineError::Invalid(
        fireweed::RETIRED_STORAGE_CELL,
    ))
}

pub fn open_log_turso(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    fireweed::open(config(path), clock)
}

pub async fn open_log_turso_async(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    fireweed::open_async(config(path), clock).await
}

pub fn open_product_async(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    let mut cfg = config(path);
    cfg.response_barrier = ResponseBarrier::AsyncProjection;
    cfg.async_projection = Some(fireweed::AsyncProjectionSpec::default());
    fireweed::open(cfg, clock)
}

pub fn cleanup(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    std::fs::remove_dir_all(format!("{}.store", path.as_ref().display()))
}

/// Own a disposable local fixture even when the test panics.
pub struct Fixture(PathBuf);
impl Fixture {
    pub fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "fireweed-fixture-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )))
    }
    pub fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = cleanup(&self.0);
    }
}
