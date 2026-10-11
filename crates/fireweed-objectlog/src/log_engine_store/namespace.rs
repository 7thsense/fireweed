//! Admitted compact discovery buckets with immutable, verified collision slots.
//! API-007 factories must isolate roots; this module never migrates legacy keys.
use super::*;
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_namespace::{address_bucket, encode_address, framed_address_len, view_address},
};

/// Operation-scoped: retains the originating reservation until dropped.
/// A QueueStore must not cache this handle across operations/shutdown.
pub struct VerifiedApi007Namespace {
    shard: QueueKey,
    address: Bytes,
    origin: Weak<()>,
    _charge: OwnedQueueBufferCharge,
}
impl VerifiedApi007Namespace {
    pub fn address(&self) -> QueueResult<fireweed_engine::queue_namespace::AddressRef<'_>> {
        if self.origin.upgrade().is_none() {
            return Err(unknown("namespace authority is gone"));
        }
        view_address(&self.address)
    }
}
struct AdmittedBytes {
    bytes: Bytes,
    _charge: OwnedQueueBufferCharge,
}
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
fn storage_queue(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
fn component(bucket: &[u8; 32], slot: u64) -> String {
    let mut value = String::with_capacity(85);
    value.push_str("q7-");
    for byte in bucket {
        write!(&mut value, "{byte:02x}").expect("String write");
    }
    write!(&mut value, "-{slot:016x}").expect("String write");
    value
}
fn worker_headers(root: &Path, key: &str) -> QueueResult<u64> {
    let root_bytes =
        u64::try_from(root.as_os_str().as_encoded_bytes().len()).map_err(storage_queue)?;
    let key_bytes = u64::try_from(key.len()).map_err(storage_queue)?;
    root_bytes
        .checked_add(key_bytes)
        .and_then(|n| n.checked_add(256))
        .and_then(|n| n.checked_mul(16))
        .and_then(|n| n.checked_add(65536))
        .ok_or(QueueError::TooLarge)
}
impl<S: Sequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    pub async fn bind_api007_namespace(
        &self,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<VerifiedApi007Namespace> {
        self.bind_namespace_with_bucket(address, job, address_bucket)
            .await
    }
    async fn bind_namespace_with_bucket(
        &self,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
        hash: fn(&[u8]) -> QueueResult<[u8; 32]>,
    ) -> QueueResult<VerifiedApi007Namespace> {
        address.validate()?;
        let size = u64::try_from(framed_address_len(address)?).map_err(storage_queue)?;
        let charge = job.owned_buffer(
            size.checked_mul(4)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let raw = Bytes::from(encode_address(address)?);
        let bucket = hash(&raw)?;
        let mut slot = 0u64;
        loop {
            let name = component(&bucket, slot);
            let key = format!("{}api007_addresses/{name}.bin", self.meta_prefix);
            let headers = match &self.definition_authority {
                DefinitionAuthority::Local { root } => worker_headers(root, &key)?,
                _ => worker_headers(Path::new(""), &key)?,
            };
            let publication_charge = charge.child_buffer(
                size.checked_mul(2)
                    .and_then(|n| n.checked_add(headers))
                    .ok_or(QueueError::TooLarge)?,
            )?;
            self.publish_immutable_bytes_owned(&key, raw.to_vec(), Some(publication_charge))
                .await
                .map_err(storage_queue)?;
            let winner = self
                .read_immutable_admitted(&key, job)
                .await?
                .ok_or_else(|| unknown("namespace authority vanished after publication"))?;
            // Corrupt slots are never interpreted as collisions or skipped.
            view_address(&winner.bytes)?;
            if hash(&winner.bytes)? != bucket {
                return Err(unknown(
                    "namespace authority is in the wrong discovery bucket",
                ));
            }
            if winner.bytes == raw {
                let shard = QueueKey::new(
                    fireweed_core::TenantId::new("api007").map_err(storage_queue)?,
                    fireweed_core::QueueId::new(name).map_err(storage_queue)?,
                );
                return Ok(VerifiedApi007Namespace {
                    shard,
                    address: raw,
                    origin: Arc::downgrade(&self.namespace_origin),
                    _charge: charge,
                });
            }
            slot = slot
                .checked_add(1)
                .ok_or_else(|| unknown("namespace collision slots exhausted"))?;
        }
    }
    pub fn validate_api007_namespace(
        &self,
        namespace: &VerifiedApi007Namespace,
    ) -> QueueResult<()> {
        self.checked_api007_shard(namespace).map(|_| ())
    }
    fn checked_api007_shard<'a>(
        &self,
        namespace: &'a VerifiedApi007Namespace,
    ) -> QueueResult<&'a QueueKey> {
        let origin = namespace
            .origin
            .upgrade()
            .ok_or_else(|| unknown("namespace authority is gone"))?;
        if !Arc::ptr_eq(&origin, &self.namespace_origin) {
            return Err(unknown("namespace belongs to another authority"));
        }
        Ok(&namespace.shard)
    }
    async fn read_immutable_admitted(
        &self,
        key: &str,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<AdmittedBytes>> {
        match &self.definition_authority {
            DefinitionAuthority::Local { root } => {
                // Admit the queued closure/path before copying either; caller drop
                // cannot release these bytes while a blocking worker is queued.
                struct Worker {
                    path: std::path::PathBuf,
                    job: Arc<QueueReservation>,
                    _charge: OwnedQueueBufferCharge,
                }
                let header_charge = job.owned_buffer(worker_headers(root, key)?)?;
                let worker = Worker {
                    path: root.join(key),
                    job: Arc::clone(job),
                    _charge: header_charge,
                };
                tokio::task::spawn_blocking(move || {
                    let worker = worker;
                    use std::io::Read;
                    let mut file = match std::fs::File::open(&worker.path) {
                        Ok(file) => file,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(None);
                        }
                        Err(error) => return Err(storage_queue(error)),
                    };
                    let size = file.metadata().map_err(storage_queue)?.len();
                    let charge = worker.job.owned_buffer(
                        size.checked_mul(4)
                            .and_then(|n| n.checked_add(65536))
                            .ok_or(QueueError::TooLarge)?,
                    )?;
                    let mut bytes =
                        Vec::with_capacity(usize::try_from(size).map_err(storage_queue)?);
                    (&mut file)
                        .take(size.checked_add(1).ok_or(QueueError::TooLarge)?)
                        .read_to_end(&mut bytes)
                        .map_err(storage_queue)?;
                    if bytes.len() as u64 != size
                        || file.metadata().map_err(storage_queue)?.len() != size
                    {
                        return Err(unknown("immutable namespace authority length changed"));
                    }
                    Ok(Some(AdmittedBytes {
                        bytes: bytes.into(),
                        _charge: charge,
                    }))
                })
                .await
                .map_err(storage_queue)?
            }
            DefinitionAuthority::ProcessLocal => {
                let Some(bytes) = self.blob.get(key).await.map_err(storage_queue)? else {
                    return Ok(None);
                };
                // MemoryBlobStore returns a shared Bytes handle, not a payload copy.
                let charge = job.owned_buffer(
                    (bytes.len() as u64)
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                Ok(Some(AdmittedBytes {
                    bytes,
                    _charge: charge,
                }))
            }
            DefinitionAuthority::S3CreateOnly { put } => {
                let Some((size, etag)) =
                    put.immutable_metadata(key).await.map_err(storage_queue)?
                else {
                    return Ok(None);
                };
                let charge = job.owned_buffer(
                    size.checked_mul(4)
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                let bytes = put
                    .read_immutable_exact(key, size, &etag)
                    .await
                    .map_err(storage_queue)?;
                Ok(Some(AdmittedBytes {
                    bytes,
                    _charge: charge,
                }))
            }
            DefinitionAuthority::ConditionalCreateUnavailable => {
                Err(unknown("native immutable namespace authority unavailable"))
            }
        }
    }
}

#[cfg(test)]
type PublicationHook = Arc<dyn Fn(&Path, bool, bool) + Send + Sync>;
#[cfg(test)]
static PUBLICATION_HOOK: Mutex<Option<PublicationHook>> = Mutex::new(None);
#[cfg(test)]
pub(super) fn publication_hook(path: &Path, created: bool, synced: bool) {
    let hook = PUBLICATION_HOOK.lock().unwrap().clone();
    if let Some(hook) = hook {
        hook(path, created, synced);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    static HOOK_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    fn collision(raw: &[u8]) -> QueueResult<[u8; 32]> {
        view_address(raw)?;
        Ok([0; 32])
    }
    fn address(value: &str) -> QueueAddress {
        QueueAddress {
            tenant: "tenant/\0β".into(),
            queue: value.into(),
        }
    }
    fn job() -> Arc<QueueReservation> {
        Arc::new(
            QueueAdmission::new(1, 16 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        )
    }
    #[tokio::test]
    async fn local_loser_syncs_directory_while_winner_is_paused_after_link() {
        let _serial = HOOK_TESTS.lock().await;
        let root = super::super::tests::temp_root("loser-fsync");
        let key = "new/ancestors/identity.bin";
        let expected_path = root.join(key);
        let (linked_tx, linked_rx) = std::sync::mpsc::channel();
        let (synced_tx, synced_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let gate = Arc::clone(&release);
        *PUBLICATION_HOOK.lock().unwrap() = Some(Arc::new(move |path, created, synced| {
            if path != expected_path {
                return;
            }
            if created && !synced {
                linked_tx.send(()).unwrap();
                let (lock, cv) = &*gate;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = cv.wait(ready).unwrap();
                }
            }
            if !created && synced {
                synced_tx.send(()).unwrap();
            }
        }));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                *PUBLICATION_HOOK.lock().unwrap() = None;
            }
        }
        let _reset = Reset;
        let winner_root = root.clone();
        let winner = tokio::task::spawn_blocking(move || {
            publish_local_definition(&winner_root, key, b"winner")
        });
        let linked = linked_rx.recv_timeout(Duration::from_secs(5));
        let loser_root = root.clone();
        let loser =
            std::thread::spawn(move || publish_local_definition(&loser_root, key, b"loser"));
        let synced = synced_rx.recv_timeout(Duration::from_secs(5));
        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        assert!(winner.await.unwrap().unwrap());
        assert!(!loser.join().unwrap().unwrap());
        linked.unwrap();
        synced.unwrap();
        assert_eq!(std::fs::read(root.join(key)).unwrap(), b"winner");
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_concurrent_collision_creators_converge_before_winner_returns() {
        let _serial = HOOK_TESTS.lock().await;
        let root = super::super::tests::temp_root("concurrent-collision");
        let store = Arc::new(
            ObjectLogEngineStore::open_local(&root, FlushConfig::default())
                .await
                .unwrap(),
        );
        let path = root.join(format!(
            "{}api007_addresses/{}.bin",
            store.meta_prefix,
            component(&[0; 32], 0)
        ));
        let (linked_tx, linked_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let gate = Arc::clone(&release);
        *PUBLICATION_HOOK.lock().unwrap() = Some(Arc::new(move |candidate, created, synced| {
            if candidate == path && created && !synced {
                linked_tx.send(()).unwrap();
                let (lock, cv) = &*gate;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = cv.wait(ready).unwrap();
                }
            }
        }));
        struct Release(Arc<(Mutex<bool>, std::sync::Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                let (lock, cv) = &*self.0;
                *lock.lock().unwrap() = true;
                cv.notify_all();
                *PUBLICATION_HOOK.lock().unwrap() = None;
            }
        }
        let reset = Release(Arc::clone(&release));
        let owner = Arc::clone(&store);
        let first = tokio::spawn(async move {
            owner
                .bind_namespace_with_bucket(&address("same"), &job(), collision)
                .await
        });
        tokio::task::spawn_blocking(move || linked_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let j1 = job();
        let j2 = job();
        let same = address("same");
        let other = address(&"β\0/".repeat(20_000));
        let (same_result, other_result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                store.bind_namespace_with_bucket(&same, &j1, collision),
                store.bind_namespace_with_bucket(&other, &j2, collision)
            )
        })
        .await
        .unwrap();
        let same_result = same_result.unwrap();
        let other_result = other_result.unwrap();
        drop(reset);
        let original = first.await.unwrap().unwrap();
        assert_eq!(
            store.checked_api007_shard(&original).unwrap(),
            store.checked_api007_shard(&same_result).unwrap()
        );
        assert_ne!(
            store.checked_api007_shard(&original).unwrap(),
            store.checked_api007_shard(&other_result).unwrap()
        );
        assert!(
            store
                .checked_api007_shard(&original)
                .unwrap()
                .queue_id
                .as_str()
                .ends_with("-0000000000000000")
        );
        assert!(
            store
                .checked_api007_shard(&other_result)
                .unwrap()
                .queue_id
                .as_str()
                .ends_with("-0000000000000001")
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn concurrent_collision_slots_converge_and_verify_origin() {
        let store = ObjectLogEngineStore::open_memory(FlushConfig::default())
            .await
            .unwrap();
        let a = address("a");
        let b = address("b\0/".repeat(20_000).as_str());
        let ja = job();
        let jb = job();
        let jc = job();
        let (na, nb, nc) = tokio::join!(
            store.bind_namespace_with_bucket(&a, &ja, collision),
            store.bind_namespace_with_bucket(&b, &jb, collision),
            store.bind_namespace_with_bucket(&a, &jc, collision)
        );
        let na = na.unwrap();
        let nb = nb.unwrap();
        let nc = nc.unwrap();
        assert_eq!(
            store.checked_api007_shard(&na).unwrap(),
            store.checked_api007_shard(&nc).unwrap()
        );
        assert_ne!(
            store.checked_api007_shard(&na).unwrap(),
            store.checked_api007_shard(&nb).unwrap()
        );
        assert_eq!(nb.address().unwrap().queue, b.queue);
        let other = ObjectLogEngineStore::open_memory(FlushConfig::default())
            .await
            .unwrap();
        assert!(other.validate_api007_namespace(&na).is_err());
        drop(store);
        assert!(na.address().is_err());
    }
    #[tokio::test]
    async fn same_blob_different_prefix_cannot_accept_another_authority_mapping() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let first = ObjectLogEngineStore::open_with_blob_and_authority(
            Arc::clone(&blob),
            "one/fwlog/",
            "one/fwmeta/",
            FlushConfig::default(),
            DefinitionAuthority::ProcessLocal,
        )
        .await
        .unwrap();
        let second = ObjectLogEngineStore::open_with_blob_and_authority(
            Arc::clone(&blob),
            "two/fwlog/",
            "two/fwmeta/",
            FlushConfig::default(),
            DefinitionAuthority::ProcessLocal,
        )
        .await
        .unwrap();
        let mapping = first
            .bind_api007_namespace(&address("same"), &job())
            .await
            .unwrap();
        first.validate_api007_namespace(&mapping).unwrap();
        assert!(
            matches!(second.validate_api007_namespace(&mapping),Err(QueueError::OutcomeUnknown(ref message)) if message.contains("another authority"))
        );
        drop(first);
        assert!(mapping.address().is_err());
        assert!(second.validate_api007_namespace(&mapping).is_err());
        assert!(Arc::strong_count(&blob) > 1);
    }
    #[tokio::test]
    async fn compact_long_address_reopens_and_corruption_never_skips_a_slot() {
        let root = super::super::tests::temp_root("compact-address");
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let address = address(&"β\0/".repeat(20_000));
        let guard = job();
        let physical = {
            let store = ObjectLogEngineStore::open_local(&root, FlushConfig::default())
                .await
                .unwrap();
            let namespace = store.bind_api007_namespace(&address, &guard).await.unwrap();
            let shard = store.checked_api007_shard(&namespace).unwrap();
            // Existing manifest transport hex-expands the entire partition.
            assert!(partition_component(shard).len() + 32 < 255);
            assert!(component(&[0; 32], u64::MAX).len() + 7 <= 127);
            shard.queue_id.as_str().to_owned()
        };
        let store = ObjectLogEngineStore::open_local(&root, FlushConfig::default())
            .await
            .unwrap();
        let namespace = store.bind_api007_namespace(&address, &guard).await.unwrap();
        assert_eq!(
            store
                .checked_api007_shard(&namespace)
                .unwrap()
                .queue_id
                .as_str(),
            physical
        );
        assert_eq!(namespace.address().unwrap().queue, address.queue);
        let key = format!(
            "{}api007_addresses/{}.bin",
            store.meta_prefix,
            component(&[0; 32], 0)
        );
        store
            .blob
            .put(&key, Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        assert!(matches!(
            store
                .bind_namespace_with_bucket(&address, &guard, collision)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let keys = store
            .blob
            .list(&format!("{}api007_addresses/", store.meta_prefix))
            .await
            .unwrap();
        assert!(
            !keys
                .iter()
                .any(|key| key.ends_with(&format!("{}.bin", component(&[0; 32], 1))))
        );
        assert_eq!(
            store.blob.get(&key).await.unwrap().unwrap(),
            Bytes::from_static(b"malformed")
        );
    }
    #[tokio::test]
    async fn compact_physical_keys_preserve_long_and_colliding_queue_logs() {
        use fireweed_core::UtcTimestamp;
        use fireweed_engine::{CommandChecksum, CommandId, QueueCommand};
        let root = super::super::tests::temp_root("compact-real-log");
        let addresses = [
            address(&"β\0/".repeat(20_000)),
            address("a\0b/c"),
            address("a/b\0c"),
        ];
        let guard = job();
        let mut positions = vec![];
        {
            let store = ObjectLogEngineStore::open_local(
                &root,
                FlushConfig {
                    linger: Duration::ZERO,
                    ..FlushConfig::default()
                },
            )
            .await
            .unwrap();
            for (index, address) in addresses.iter().enumerate() {
                let mapping = store
                    .bind_namespace_with_bucket(address, &guard, collision)
                    .await
                    .unwrap();
                let shard = store.checked_api007_shard(&mapping).unwrap().clone();
                let mut definition = super::super::tests::qdef();
                definition.tenant_id = shard.tenant_id.clone();
                definition.queue_id = shard.queue_id.clone();
                store.create_or_read_definition(definition).await.unwrap();
                store.ensure_shard(shard.clone()).await.unwrap();
                let mut epoch = 0;
                for _ in 0..=index {
                    epoch = store.acquire_epoch(shard.clone()).await.unwrap();
                }
                let command = CommandEnvelope {
                    command_id: CommandId::new(format!("exact-{index}")),
                    request_id: None,
                    request_fingerprint: None,
                    request_outcome: None,
                    item_ids: vec![],
                    command: QueueCommand::PauseQueue(Default::default()),
                    checksum: CommandChecksum(0),
                    created_at: UtcTimestamp::new(1, 0).unwrap(),
                };
                let result = store
                    .append(shard.clone(), vec![command], epoch)
                    .await
                    .unwrap();
                store
                    .set_emission_cursor(&shard, result[0].clone())
                    .await
                    .unwrap();
                positions.push(result[0].clone());
            }
        }
        let store = ObjectLogEngineStore::open_local(
            &root,
            FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            },
        )
        .await
        .unwrap();
        for (index, address) in addresses.iter().enumerate() {
            let mapping = store
                .bind_namespace_with_bucket(address, &guard, collision)
                .await
                .unwrap();
            let shard = store.checked_api007_shard(&mapping).unwrap().clone();
            store.ensure_shard(shard.clone()).await.unwrap();
            assert_eq!(
                store.current_epoch(shard.clone()).await.unwrap(),
                index as u64 + 1
            );
            assert_eq!(
                store.high_water(shard.clone()).await.unwrap(),
                Some(positions[index].clone())
            );
            assert_eq!(
                store.emission_cursor(&shard).await.unwrap(),
                Some(positions[index].clone())
            );
            let page = store.read_from(shard.clone(), None, 1).await.unwrap();
            assert_eq!(page.entries.len(), 1);
            assert_eq!(page.entries[0].0, positions[index]);
            assert_eq!(page.entries[0].1.command_id.0, format!("exact-{index}"));
            let end = store.read_from(shard, page.next, 1).await.unwrap();
            assert!(end.entries.is_empty());
            assert!(end.next.is_none());
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn queued_local_read_keeps_header_charge_after_owner_drop() {
        let root = super::super::tests::temp_root("queued-namespace-read");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = Arc::new(
                ObjectLogEngineStore::open_local(&root, FlushConfig::default())
                    .await
                    .unwrap(),
            );
            let key = "queued.bin";
            std::fs::write(root.join(key), vec![17u8; 65536]).unwrap();
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let owner = Arc::clone(&store);
            let worker_job = Arc::clone(&job);
            let reader =
                tokio::spawn(async move { owner.read_immutable_admitted(key, &worker_job).await });
            tokio::time::timeout(Duration::from_secs(5), async {
                while admission.usage().unwrap().1 == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            reader.abort();
            let _ = reader.await;
            drop(job);
            let queued = admission.usage().unwrap();
            assert_eq!(queued.0, 1);
            assert!(queued.1 >= 65536);
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while admission.usage().unwrap() != (0, 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn winner_bytes_are_admitted_before_owned_local_read() {
        let root = super::super::tests::temp_root("compact-admission");
        let store = ObjectLogEngineStore::open_local(&root, FlushConfig::default())
            .await
            .unwrap();
        let large = address(&"x".repeat(1 << 20));
        let roomy = job();
        store
            .bind_namespace_with_bucket(&large, &roomy, collision)
            .await
            .unwrap();
        let small = Arc::new(
            QueueAdmission::new(1, 256 << 10)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        assert!(
            matches!(store.bind_namespace_with_bucket(&address("small"),&small,collision).await,Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
        );
        assert!(small.owned_buffer(1).is_ok());
        let _ = std::fs::remove_dir_all(root);
    }
}
