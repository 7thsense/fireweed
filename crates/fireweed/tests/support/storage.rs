//! Current local storage fixtures shared by public API tests.
#![allow(dead_code)]
use fireweed::{
    Clock, EngineResult, Fireweed, LogConfig, ObjectLogAuthority, ProjectionStoreConfig,
    StorageConfig,
};
use std::{path::PathBuf, sync::Arc};

fn config(path: &str, projection: bool) -> StorageConfig {
    let root = PathBuf::from(format!("{path}.store"));
    let mut config = StorageConfig::memory();
    config.log = LogConfig::Filesystem {
        root: root.join("log"),
    };
    config.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
    if projection {
        config.projection = ProjectionStoreConfig::Turso {
            path: root.join("projection.db"),
        };
    }
    config
}

pub fn open_log_memory(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    fireweed::open(config(path, false), clock)
}

pub fn open_log_turso(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed> {
    fireweed::open(config(path, true), clock)
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
