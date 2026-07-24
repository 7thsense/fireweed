use std::sync::Mutex;

use fireweed_engine::{EngineError, EngineResult};
use fireweed_objectlog::segmented::{ManifestHeadBlob, ManifestPointerStore, VersionedHead};
use postgres::{Client, NoTls};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pqueue_objectlog_manifest_pointer (
    pointer_key TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    assignment_epoch BIGINT NOT NULL,
    head_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pqueue_objectlog_immutable_claim (
    object_key TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL
)";

/// Postgres-held TD-004 manifest pointer for object stores without conditional writes.
/// `version`, `assignment_epoch`, and the serialized head change in one row update and one transaction.
pub struct PostgresManifestPointer {
    client: Mutex<Client>,
}

impl PostgresManifestPointer {
    pub fn open(connection_string: &str) -> EngineResult<Self> {
        let mut client = Client::connect(connection_string, NoTls).map_err(storage)?;
        client.batch_execute(SCHEMA).map_err(storage)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

impl ManifestPointerStore for PostgresManifestPointer {
    fn read(&self, pointer_key: &str) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
        let mut client = self
            .client
            .lock()
            .expect("manifest pointer client poisoned");
        let row = client
            .query_opt(
                "SELECT version, assignment_epoch, head_json \
                 FROM pqueue_objectlog_manifest_pointer WHERE pointer_key=$1",
                &[&pointer_key],
            )
            .map_err(storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let version: i64 = row.get(0);
        let assignment_epoch: i64 = row.get(1);
        let value: ManifestHeadBlob = serde_json::from_str(row.get::<_, &str>(2))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        if version < 0 || assignment_epoch < 0 || value.current_epoch != assignment_epoch as u64 {
            return Err(EngineError::Storage(
                "Postgres manifest pointer epoch/head mismatch".into(),
            ));
        }
        Ok(Some(VersionedHead {
            version: version as u64,
            value,
        }))
    }

    fn compare_and_swap(
        &self,
        pointer_key: &str,
        expected_version: Option<u64>,
        value: &ManifestHeadBlob,
    ) -> EngineResult<bool> {
        let expected = expected_version
            .map(i64::try_from)
            .transpose()
            .map_err(|_| EngineError::Storage("manifest pointer version overflow".into()))?;
        let epoch = i64::try_from(value.current_epoch)
            .map_err(|_| EngineError::Storage("assignment epoch overflow".into()))?;
        let head_json = serde_json::to_string(value)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let mut client = self
            .client
            .lock()
            .expect("manifest pointer client poisoned");
        let mut transaction = client.transaction().map_err(storage)?;
        let changed = match expected {
            None => transaction
                .execute(
                    "INSERT INTO pqueue_objectlog_manifest_pointer \
                     (pointer_key,version,assignment_epoch,head_json) VALUES($1,0,$2,$3) \
                     ON CONFLICT(pointer_key) DO NOTHING",
                    &[&pointer_key, &epoch, &head_json],
                )
                .map_err(storage)?,
            Some(expected) => transaction
                .execute(
                    "UPDATE pqueue_objectlog_manifest_pointer \
                     SET version=$2+1, assignment_epoch=$3, head_json=$4 \
                     WHERE pointer_key=$1 AND version=$2",
                    &[&pointer_key, &expected, &epoch, &head_json],
                )
                .map_err(storage)?,
        };
        transaction.commit().map_err(storage)?;
        Ok(changed == 1)
    }

    fn claim_immutable(&self, key: &str, content_sha256: &str) -> EngineResult<bool> {
        let mut client = self
            .client
            .lock()
            .expect("manifest pointer client poisoned");
        let changed = client
            .execute(
                "INSERT INTO pqueue_objectlog_immutable_claim(object_key,content_sha256) \
                 VALUES($1,$2) ON CONFLICT(object_key) DO NOTHING",
                &[&key, &content_sha256],
            )
            .map_err(storage)?;
        if changed == 1 {
            return Ok(true);
        }
        let existing: String = client
            .query_one(
                "SELECT content_sha256 FROM pqueue_objectlog_immutable_claim WHERE object_key=$1",
                &[&key],
            )
            .map_err(storage)?
            .get(0);
        Ok(existing == content_sha256)
    }
}

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}
