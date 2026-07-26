use std::sync::Mutex;

use fireweed_engine::{EngineError, EngineResult};
use fireweed_objectlog::segmented::{
    BlobStore, ManifestHeadBlob, ManifestPointerStore, TransactionalDeleteOutcome,
    TransactionalPublishOutcome, VersionedHead,
};
use postgres::Client;
use sha2::{Digest, Sha256};

use crate::{PostgresConnectConfig, connect};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS fireweed_objectlog_manifest_pointer (
    pointer_key TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    assignment_epoch BIGINT NOT NULL,
    head_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fireweed_objectlog_immutable_claim (
    object_key TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    present BOOLEAN NOT NULL DEFAULT TRUE
);
ALTER TABLE fireweed_objectlog_immutable_claim
    ADD COLUMN IF NOT EXISTS present BOOLEAN NOT NULL DEFAULT TRUE
";

/// Postgres-held TD-004 manifest pointer and create-only object lifecycle authority for object stores
/// without conditional writes.
pub struct PostgresManifestPointer {
    client: Mutex<Option<Client>>,
}

impl PostgresManifestPointer {
    pub fn open(connection_string: &str) -> EngineResult<Self> {
        let connection_string = connection_string.to_owned();
        let client = std::thread::spawn(move || {
            let mut client = connect(PostgresConnectConfig::new(connection_string))?;
            client.batch_execute(SCHEMA).map_err(storage)?;
            Ok::<Client, EngineError>(client)
        })
        .join()
        .map_err(|_| {
            EngineError::Storage("Postgres manifest pointer open thread panicked".into())
        })??;
        Ok(Self {
            client: Mutex::new(Some(client)),
        })
    }

    fn with_client<T: Send>(
        &self,
        operation: impl FnOnce(&mut Client) -> EngineResult<T> + Send,
    ) -> EngineResult<T> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut guard = self.client.lock().map_err(|_| {
                        EngineError::Storage("manifest pointer client poisoned".into())
                    })?;
                    let client = guard.as_mut().ok_or_else(|| {
                        EngineError::Storage("manifest pointer client is closed".into())
                    })?;
                    operation(client)
                })
                .join()
                .map_err(|_| {
                    EngineError::Storage(
                        "Postgres manifest pointer operation thread panicked".into(),
                    )
                })?
        })
    }
}

impl Drop for PostgresManifestPointer {
    fn drop(&mut self) {
        let client = self
            .client
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(client) = client {
            let _ = std::thread::spawn(move || drop(client)).join();
        }
    }
}

impl ManifestPointerStore for PostgresManifestPointer {
    fn read(&self, pointer_key: &str) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "SELECT version, assignment_epoch, head_json \
                     FROM fireweed_objectlog_manifest_pointer WHERE pointer_key=$1",
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
            if version < 0 || assignment_epoch < 0 || value.current_epoch != assignment_epoch as u64
            {
                return Err(EngineError::Storage(
                    "Postgres manifest pointer epoch/head mismatch".into(),
                ));
            }
            Ok(Some(VersionedHead {
                version: version as u64,
                value,
            }))
        })
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
        self.with_client(move |client| {
            let mut transaction = client.transaction().map_err(storage)?;
            let changed = match expected {
                None => transaction
                    .execute(
                        "INSERT INTO fireweed_objectlog_manifest_pointer \
                         (pointer_key,version,assignment_epoch,head_json) VALUES($1,0,$2,$3) \
                         ON CONFLICT(pointer_key) DO NOTHING",
                        &[&pointer_key, &epoch, &head_json],
                    )
                    .map_err(storage)?,
                Some(expected) => transaction
                    .execute(
                        "UPDATE fireweed_objectlog_manifest_pointer \
                         SET version=$2+1, assignment_epoch=$3, head_json=$4 \
                         WHERE pointer_key=$1 AND version=$2",
                        &[&pointer_key, &expected, &epoch, &head_json],
                    )
                    .map_err(storage)?,
            };
            transaction.commit().map_err(storage)?;
            Ok(changed == 1)
        })
    }

    fn publish_if_absent(
        &self,
        key: &str,
        content_sha256: &str,
        body: &[u8],
        mirror: &dyn BlobStore,
    ) -> EngineResult<TransactionalPublishOutcome> {
        if sha256_hex(body) != content_sha256 {
            return Err(EngineError::Invalid(
                "transactional publication body hash mismatch",
            ));
        }
        self.with_client(|client| {
            let mut transaction = client.transaction().map_err(storage)?;
            lock_object_key(&mut transaction, key)?;
            let lifecycle = read_lifecycle(&mut transaction, key)?;
            let mirrored = mirror.get(key)?;

            let outcome = match lifecycle {
                Some((claimed, true)) => match mirrored {
                    None if claimed == content_sha256 => {
                        mirror.put(key, body)?;
                        TransactionalPublishOutcome::Repaired
                    }
                    None => TransactionalPublishOutcome::Conflict,
                    Some(existing) if sha256_hex(&existing) != claimed => {
                        return Err(EngineError::Storage(
                            "object mirror content disagrees with transactional publication authority"
                                .into(),
                        ));
                    }
                    Some(_) if claimed != content_sha256 => TransactionalPublishOutcome::Conflict,
                    Some(existing) if existing == body => TransactionalPublishOutcome::Existing,
                    Some(_) => {
                        return Err(EngineError::Storage(
                            "object mirror bytes disagree with their claimed content hash".into(),
                        ));
                    }
                },
                Some((_, false)) => {
                    let repaired = mirrored.as_deref() == Some(body);
                    if !repaired {
                        mirror.put(key, body)?;
                    }
                    transaction
                        .execute(
                            "UPDATE fireweed_objectlog_immutable_claim \
                             SET content_sha256=$2,present=TRUE WHERE object_key=$1",
                            &[&key, &content_sha256],
                        )
                        .map_err(storage)?;
                    if repaired {
                        TransactionalPublishOutcome::Repaired
                    } else {
                        TransactionalPublishOutcome::Created
                    }
                }
                None => match mirrored {
                    Some(existing) if existing != body => TransactionalPublishOutcome::Conflict,
                    existing => {
                        transaction
                            .execute(
                                "INSERT INTO fireweed_objectlog_immutable_claim \
                                 (object_key,content_sha256,present) VALUES($1,$2,TRUE)",
                                &[&key, &content_sha256],
                            )
                            .map_err(storage)?;
                        if existing.is_some() {
                            TransactionalPublishOutcome::Repaired
                        } else {
                            mirror.put(key, body)?;
                            TransactionalPublishOutcome::Created
                        }
                    }
                },
            };
            transaction.commit().map_err(storage)?;
            Ok(outcome)
        })
    }

    fn delete_object(
        &self,
        key: &str,
        mirror: &dyn BlobStore,
    ) -> EngineResult<TransactionalDeleteOutcome> {
        self.with_client(|client| {
            let mut transaction = client.transaction().map_err(storage)?;
            lock_object_key(&mut transaction, key)?;
            let lifecycle = read_lifecycle(&mut transaction, key)?;
            let mirrored = mirror.get(key)?;

            let outcome = match (lifecycle, mirrored) {
                (None, None) | (Some((_, false)), None) => TransactionalDeleteOutcome::Missing,
                (None, Some(existing)) => {
                    let digest = sha256_hex(&existing);
                    mirror.delete(key)?;
                    transaction
                        .execute(
                            "INSERT INTO fireweed_objectlog_immutable_claim \
                             (object_key,content_sha256,present) VALUES($1,$2,FALSE)",
                            &[&key, &digest],
                        )
                        .map_err(storage)?;
                    TransactionalDeleteOutcome::Repaired
                }
                (Some((claimed, false)), Some(existing)) if sha256_hex(&existing) == claimed => {
                    mirror.delete(key)?;
                    TransactionalDeleteOutcome::Repaired
                }
                (Some((_, false)), Some(_)) => TransactionalDeleteOutcome::Conflict,
                (Some((claimed, true)), Some(existing)) if sha256_hex(&existing) != claimed => {
                    TransactionalDeleteOutcome::Conflict
                }
                (Some((_, true)), Some(_)) => {
                    mirror.delete(key)?;
                    transaction
                        .execute(
                            "UPDATE fireweed_objectlog_immutable_claim SET present=FALSE WHERE object_key=$1",
                            &[&key],
                        )
                        .map_err(storage)?;
                    TransactionalDeleteOutcome::Deleted
                }
                (Some((_, true)), None) => {
                    transaction
                        .execute(
                            "UPDATE fireweed_objectlog_immutable_claim SET present=FALSE WHERE object_key=$1",
                            &[&key],
                        )
                        .map_err(storage)?;
                    TransactionalDeleteOutcome::Repaired
                }
            };
            transaction.commit().map_err(storage)?;
            Ok(outcome)
        })
    }
}

fn lock_object_key(transaction: &mut postgres::Transaction<'_>, key: &str) -> EngineResult<()> {
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .map(|_| ())
        .map_err(storage)
}

fn read_lifecycle(
    transaction: &mut postgres::Transaction<'_>,
    key: &str,
) -> EngineResult<Option<(String, bool)>> {
    transaction
        .query_opt(
            "SELECT content_sha256,present FROM fireweed_objectlog_immutable_claim \
             WHERE object_key=$1 FOR UPDATE",
            &[&key],
        )
        .map(|row| row.map(|row| (row.get(0), row.get(1))))
        .map_err(storage)
}

fn sha256_hex(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::{SystemTime, UNIX_EPOCH};

    use fireweed_objectlog::segmented::{BlobStore, InMemoryBlobStore, PointerFencedBlobStore};

    use super::*;

    struct ClobberingStore {
        inner: InMemoryBlobStore,
        fail_after_put: AtomicBool,
        fail_after_delete: AtomicBool,
    }

    impl ClobberingStore {
        fn new() -> Self {
            Self {
                inner: InMemoryBlobStore::new(),
                fail_after_put: AtomicBool::new(false),
                fail_after_delete: AtomicBool::new(false),
            }
        }
    }

    impl BlobStore for ClobberingStore {
        fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
            self.inner.put(key, body)?;
            if self.fail_after_put.swap(false, Ordering::SeqCst) {
                return Err(EngineError::Storage("effect-then-error put".into()));
            }
            Ok(())
        }

        fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
            self.inner.put(key, body)?;
            Ok(true)
        }

        fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
            self.inner.get(key)
        }

        fn delete(&self, key: &str) -> EngineResult<bool> {
            let deleted = self.inner.delete(key)?;
            if self.fail_after_delete.swap(false, Ordering::SeqCst) {
                return Err(EngineError::Storage("effect-then-error delete".into()));
            }
            Ok(deleted)
        }

        fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    #[test]
    fn postgres_pointer_serializes_clobbering_store_lifecycle_and_repairs_crash_gaps() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!(
                "SKIP postgres_pointer_serializes_clobbering_store_lifecycle_and_repairs_crash_gaps: FIREWEED_PG_TEST_URL unset"
            );
            return;
        };
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("test/transactional-publication/{nonce}");
        let objects = Arc::new(ClobberingStore::new());
        let owner_a = Arc::new(PostgresManifestPointer::open(&url).unwrap());
        let owner_b = Arc::new(PostgresManifestPointer::open(&url).unwrap());
        let adapter_a = Arc::new(PointerFencedBlobStore::new(objects.clone(), owner_a));
        let adapter_b = Arc::new(PointerFencedBlobStore::new(objects.clone(), owner_b));

        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for (adapter, body) in [
            (adapter_a.clone(), b"alpha".as_slice()),
            (adapter_b.clone(), b"bravo".as_slice()),
        ] {
            let barrier = barrier.clone();
            let key = key.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                adapter.put_if_absent(&key, body).unwrap()
            }));
        }
        barrier.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|created| **created).count(), 1);

        objects.fail_after_delete.store(true, Ordering::SeqCst);
        assert!(adapter_a.delete(&key).is_err());
        assert!(objects.get(&key).unwrap().is_none());
        assert!(
            !adapter_b
                .put_if_absent(&key, b"after-delete-crash")
                .unwrap()
        );
        assert!(adapter_b.delete(&key).unwrap());
        assert!(
            adapter_b
                .put_if_absent(&key, b"after-delete-crash")
                .unwrap()
        );

        assert!(adapter_b.delete(&key).unwrap());
        objects.fail_after_put.store(true, Ordering::SeqCst);
        assert!(adapter_a.put_if_absent(&key, b"after-put-crash").is_err());
        assert_eq!(objects.get(&key).unwrap().unwrap(), b"after-put-crash");
        assert!(adapter_b.put_if_absent(&key, b"after-put-crash").unwrap());
        assert!(adapter_b.delete(&key).unwrap());
        assert!(adapter_a.put_if_absent(&key, b"transient-reuse").unwrap());
        assert_eq!(objects.get(&key).unwrap().unwrap(), b"transient-reuse");
        assert!(adapter_a.delete(&key).unwrap());
    }
}
