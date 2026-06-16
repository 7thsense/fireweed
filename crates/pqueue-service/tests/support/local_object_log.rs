use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pqueue_core::QueueDefinition;
use pqueue_objectlog::{
    DeploymentProfile, FjordObjectLogStore, ManifestMode, MemoryBlobStore, MemoryCoordinator,
    PqueueObjectLogConfig,
};
use pqueue_sqlite::SqliteProjection;
use pqueue_storage::types::ShardKey;

#[derive(Clone)]
pub struct LocalObjectLogProfile {
    pub backend_profile: String,
    pub object_log_root: PathBuf,
    pub sqlite_projection_root: PathBuf,
    pub segment_max_commands: usize,
    coordinator: Arc<MemoryCoordinator>,
    blob: Arc<MemoryBlobStore>,
}

pub struct LocalObjectLogConnection {
    pub store: FjordObjectLogStore,
    pub object_log_root: PathBuf,
    pub sqlite_projection_root: PathBuf,
}

impl LocalObjectLogProfile {
    pub fn from_fixture(path: impl AsRef<Path>) -> Self {
        let text = std::fs::read_to_string(path).expect("profile fixture should be readable");
        let backend_profile = parse_string_field(&text, "backend_profile");
        let segment_max_commands = parse_usize_field(&text, "segment_max_commands");
        let runtime_root = runtime_root();
        let object_log_root = runtime_root.join(parse_string_field(&text, "object_log_root"));
        let sqlite_projection_root =
            runtime_root.join(parse_string_field(&text, "sqlite_projection_root"));
        std::fs::create_dir_all(&object_log_root).expect("object-log root should be created");
        std::fs::create_dir_all(&sqlite_projection_root)
            .expect("sqlite projection root should be created");

        Self {
            backend_profile,
            object_log_root,
            sqlite_projection_root,
            segment_max_commands,
            coordinator: Arc::new(MemoryCoordinator::new()),
            blob: Arc::new(MemoryBlobStore::new()),
        }
    }

    pub fn connect(&self) -> LocalObjectLogConnection {
        assert_eq!(self.backend_profile, "object_log_sqlite_projection");
        let config = PqueueObjectLogConfig {
            deployment_profile: DeploymentProfile::Production,
            manifest_mode: ManifestMode::ObjectStoreCas,
            max_commands_per_segment: self.segment_max_commands,
            dev_unsafe_one_command_segments: false,
        };
        let store = FjordObjectLogStore::new_with_config(
            self.coordinator.clone(),
            self.blob.clone(),
            config,
        )
        .unwrap();
        LocalObjectLogConnection {
            store,
            object_log_root: self.object_log_root.clone(),
            sqlite_projection_root: self.sqlite_projection_root.clone(),
        }
    }

    pub fn persist_queue_manifest(&self, definition: &QueueDefinition) -> PathBuf {
        let path = self.object_log_root.join(format!(
            "{}__{}.queue.json",
            definition.tenant_id.as_str(),
            definition.queue_id.as_str()
        ));
        let manifest = serde_json::json!({
            "backend_profile": self.backend_profile,
            "tenant_id": definition.tenant_id.as_str(),
            "queue_id": definition.queue_id.as_str(),
            "shard_count": definition.shard_count,
            "segment_max_commands": self.segment_max_commands
        });
        std::fs::write(&path, format!("{manifest}\n")).expect("queue manifest should be writable");
        path
    }
}

impl LocalObjectLogConnection {
    pub fn projection(&self, shard_key: ShardKey) -> SqliteProjection {
        SqliteProjection::new_in_memory(shard_key).unwrap()
    }

    pub fn snapshot_path(&self, name: &str) -> PathBuf {
        self.sqlite_projection_root
            .join(format!("{name}.snapshot.json"))
    }
}

fn runtime_root() -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    std::env::temp_dir()
        .join("pqueue-object-log-local")
        .join(format!("{}-{millis}", std::process::id()))
}

fn parse_string_field(text: &str, field: &str) -> String {
    let prefix = format!("{field} = ");
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|raw| raw.strip_prefix('"')?.strip_suffix('"'))
        .map(str::to_string)
        .unwrap_or_else(|| panic!("missing string field `{field}`"))
}

fn parse_usize_field(text: &str, field: &str) -> usize {
    let prefix = format!("{field} = ");
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("missing usize field `{field}`"))
}
