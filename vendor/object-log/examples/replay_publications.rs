//! Replay captured Fireweed immutable publications through LocalBlobStore.
//! This isolates publication cost; it is never workflow qualification.
use bytes::Bytes;
use object_log::{BlobStore, IndexEntry, LocalBlobStore, PartitionKey};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Deserialize)]
struct Manifest {
    entries: Vec<(PartitionKey, IndexEntry)>,
}
struct Group {
    data: Vec<(String, Bytes)>,
    chunks: BTreeMap<String, Vec<Bytes>>,
    manifest_key: String,
    manifest: Bytes,
}
struct Store {
    relative_root: PathBuf,
    groups: Vec<Group>,
    publications: usize,
    bytes: u64,
}
struct Capture {
    items: u64,
    stores: Vec<Arc<Store>>,
    omitted_files: u64,
    omitted_bytes: u64,
}
fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}
fn numeric_files(directory: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("non-UTF8 object key"))?;
        if !entry.file_type()?.is_file()
            || name.len() != 20
            || !name.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid(format!(
                "unexpected/incomplete object: {}",
                entry.path().display()
            )));
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}
fn file_totals(root: &Path) -> Result<(u64, u64)> {
    let (mut files, mut bytes) = (0, 0);
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let (n, b) = file_totals(&entry.path())?;
            files += n;
            bytes += b;
        } else if entry.file_type()?.is_file() {
            files += 1;
            bytes += entry.metadata()?.len();
        } else {
            return Err(invalid(
                "capture must not contain symlinks or special files",
            ));
        }
    }
    Ok((files, bytes))
}
fn load_store(source: &Path, relative_root: PathBuf) -> Result<(Store, u64, u64)> {
    let root = source.join(&relative_root);
    let keys = numeric_files(&root.join("fwmeta/manifest"))?;
    if keys.is_empty() {
        return Err(invalid("empty manifest history"));
    }
    let mut objects: BTreeMap<String, Bytes> = BTreeMap::new();
    let mut chunk_ranges: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    let mut next_offsets = BTreeMap::new();
    let mut groups = Vec::new();
    let (mut publications, mut bytes) = (0, 0);
    for key in keys {
        let manifest_key = format!("fwmeta/manifest/{key}");
        let manifest = Bytes::from(std::fs::read(root.join(&manifest_key))?);
        let record: Manifest = serde_json::from_slice(&manifest)?;
        if record.entries.is_empty() {
            return Err(invalid("empty manifest entries"));
        }
        let mut data = Vec::new();
        for (partition, entry) in record.entries {
            let name = entry
                .location
                .object_id
                .strip_prefix("fwlog/")
                .ok_or_else(|| invalid("manifest references a non-data key"))?;
            if name.len() != 20 || !name.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid("invalid data object key"));
            }
            let next = next_offsets.entry(partition.0).or_insert(0_i64);
            if entry.record_count <= 0 || entry.base_offset != *next {
                return Err(invalid(
                    "manifest history has a missing/reordered partition range",
                ));
            }
            *next = next
                .checked_add(i64::from(entry.record_count))
                .ok_or_else(|| invalid("offset overflow"))?;
            if !objects.contains_key(&entry.location.object_id) {
                let content = Bytes::from(std::fs::read(root.join(&entry.location.object_id))?);
                bytes += content.len() as u64;
                publications += 1;
                data.push((entry.location.object_id.clone(), content.clone()));
                objects.insert(entry.location.object_id.clone(), content);
            }
            let content = &objects[&entry.location.object_id];
            let end = entry
                .location
                .byte_start
                .checked_add(entry.location.byte_len)
                .ok_or_else(|| invalid("byte range overflow"))?;
            if entry.location.byte_len == 0 || u64::from(end) > content.len() as u64 {
                return Err(invalid("manifest range exceeds captured data"));
            }
            chunk_ranges
                .entry(entry.location.object_id.clone())
                .or_default()
                .push((entry.location.byte_start, entry.location.byte_len));
        }
        publications += 1;
        bytes += manifest.len() as u64;
        groups.push(Group {
            data,
            chunks: BTreeMap::new(),
            manifest_key,
            manifest,
        });
    }
    let disk_keys: BTreeSet<_> = numeric_files(&root.join("fwlog"))?
        .into_iter()
        .map(|k| format!("fwlog/{k}"))
        .collect();
    if disk_keys != objects.keys().cloned().collect() {
        return Err(invalid("unreferenced captured data would be omitted"));
    }
    for group in &mut groups {
        for (key, content) in &group.data {
            let ranges = chunk_ranges
                .get_mut(key)
                .ok_or_else(|| invalid("missing chunk ranges"))?;
            ranges.sort_unstable();
            let mut next = 0usize;
            let mut chunks = Vec::new();
            for &(start, len) in ranges.iter() {
                if start as usize != next {
                    return Err(invalid("data chunks overlap or leave a gap"));
                }
                next += len as usize;
                chunks.push(content.slice(start as usize..next));
            }
            if next != content.len() {
                return Err(invalid(
                    "manifest chunks do not cover the whole data object",
                ));
            }
            group.chunks.insert(key.clone(), chunks);
        }
    }
    let (all_files, all_bytes) = file_totals(&root)?;
    let omitted_files = all_files
        .checked_sub(publications as u64)
        .ok_or_else(|| invalid("file accounting mismatch"))?;
    let omitted_bytes = all_bytes
        .checked_sub(bytes)
        .ok_or_else(|| invalid("byte accounting mismatch"))?;
    Ok((
        Store {
            relative_root,
            groups,
            publications,
            bytes,
        },
        omitted_files,
        omitted_bytes,
    ))
}
fn load_capture(capture: &Path) -> Result<Capture> {
    let report: Value = serde_json::from_slice(&std::fs::read(capture.join("capture.json"))?)?;
    let r = &report["result"];
    if report["exit_code"] != 0 || r["schema"] != "campaign-capacity/v3" || r["cycles"] != 1 {
        return Err(invalid(
            "require one successfully completed campaign capture",
        ));
    }
    let items = r["items"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("missing recipient count"))?;
    let count = r["physical_shards"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("missing store count"))?;
    let source = capture.join("source");
    let children = std::fs::read_dir(&source)?.collect::<std::io::Result<Vec<_>>>()?;
    if children.len() as u64 != count
        || children
            .iter()
            .any(|entry| !entry.file_type().is_ok_and(|kind| kind.is_dir()))
    {
        return Err(invalid("capture store count/type differs from its report"));
    }
    let (mut stores, mut omitted_files, mut omitted_bytes) = (Vec::new(), 0, 0);
    for shard in 0..count {
        let log = source.join(format!("shard-{shard}/log"));
        let mut tenants = Vec::new();
        for entry in std::fs::read_dir(&log)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(invalid("unexpected log-root entry"));
            }
            tenants.push(entry.file_name());
        }
        if tenants.len() != 1 {
            return Err(invalid(
                "capture must have one tenant log per physical store",
            ));
        }
        let relative = PathBuf::from(format!("shard-{shard}/log")).join(&tenants[0]);
        let (store, n, b) = load_store(&source, relative)?;
        stores.push(Arc::new(store));
        omitted_files += n;
        omitted_bytes += b;
    }
    Ok(Capture {
        items,
        stores,
        omitted_files,
        omitted_bytes,
    })
}
async fn publish_history(store: &Store, blob: &dyn BlobStore) -> Result<()> {
    for group in &store.groups {
        // Conservative single stream per store: every referenced data object
        // completes durable publication before its manifest is published.
        for (key, _) in &group.data {
            blob.put_chunks(key, group.chunks[key].clone()).await?;
        }
        blob.put(&group.manifest_key, group.manifest.clone())
            .await?;
    }
    Ok(())
}
async fn publish_store(store: Arc<Store>, destination: PathBuf) -> Result<Value> {
    let started = Instant::now();
    let blob = LocalBlobStore::new(destination.join(&store.relative_root));
    publish_history(&store, &blob).await?;
    let elapsed = started.elapsed().as_secs_f64();
    let stats = blob
        .take_media_op_stats()
        .ok_or_else(|| invalid("missing native media accounting"))?;
    if stats.bytes != store.bytes {
        return Err(invalid("publication byte accounting mismatch"));
    }
    Ok(
        json!({"store":store.relative_root,"wall_s":elapsed,"publications":store.publications,"bytes":stats.bytes,"media_ops":stats.media_ops}),
    )
}
async fn verify(capture: &Capture, destination: &Path, cycles: usize) -> Result<u64> {
    let mut verified = 0;
    for cycle in 0..cycles {
        for store in &capture.stores {
            let blob = LocalBlobStore::new(
                destination
                    .join(format!("cycle-{cycle}"))
                    .join(&store.relative_root),
            );
            let mut expected = BTreeSet::new();
            for group in &store.groups {
                for (key, content) in group
                    .data
                    .iter()
                    .map(|(k, v)| (k, v))
                    .chain(std::iter::once((&group.manifest_key, &group.manifest)))
                {
                    if blob.get(key).await?.as_ref() != Some(content) {
                        return Err(invalid(format!("destination bytes differ: {key}")));
                    }
                    expected.insert(key.clone());
                    verified += 1;
                }
            }
            let actual: BTreeSet<_> = blob.list("").await?.into_iter().collect();
            if actual != expected {
                return Err(invalid("destination key set differs"));
            }
        }
    }
    Ok(verified)
}
fn unix_s() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() == 2 && args[0] == "--inspect" {
        let capture = load_capture(Path::new(&args[1]))?;
        println!(
            "{}",
            serde_json::to_string_pretty(
                &json!({"captured_recipients":capture.items,"physical_stores":capture.stores.len(),"publications":capture.stores.iter().map(|s|s.publications).sum::<usize>(),"publication_bytes":capture.stores.iter().map(|s|s.bytes).sum::<u64>(),"omitted_other_snapshot_files":capture.omitted_files,"omitted_other_snapshot_bytes":capture.omitted_bytes})
            )?
        );
        return Ok(());
    }
    if args.len() != 3 {
        return Err(invalid(
            "Usage: replay_publications CAPTURE_DIRECTORY NEW_DESTINATION CYCLES",
        ));
    }
    let source = Path::new(&args[0]).canonicalize()?;
    let dest_arg = Path::new(&args[1]);
    let destination = dest_arg
        .parent()
        .unwrap_or(Path::new("."))
        .canonicalize()?
        .join(
            dest_arg
                .file_name()
                .ok_or_else(|| invalid("missing destination name"))?,
        );
    if destination.starts_with(&source) {
        return Err(invalid("destination must be outside the capture"));
    }
    let cycles: usize = args[2].parse()?;
    if cycles == 0 {
        return Err(invalid("cycles must be positive"));
    }
    let preloading = Instant::now();
    let capture = load_capture(&source)?;
    let preload_s = preloading.elapsed().as_secs_f64();
    std::fs::create_dir(&destination)?; // Refuse to overwrite any previous evidence.
    let mut reports = Vec::new();
    for cycle in 0..cycles {
        let started_unix_s = unix_s();
        let started = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for store in &capture.stores {
            tasks.spawn(publish_store(
                Arc::clone(store),
                destination.join(format!("cycle-{cycle}")),
            ));
        }
        let mut stores = Vec::new();
        while let Some(result) = tasks.join_next().await {
            stores.push(result??);
        }
        let wall_s = started.elapsed().as_secs_f64();
        let finished_unix_s = unix_s();
        stores.sort_by(|a, b| a["store"].as_str().cmp(&b["store"].as_str()));
        let report = json!({"cycle":cycle,"wall_s":wall_s,"started_unix_s":started_unix_s,"finished_unix_s":finished_unix_s,"publication_only_equivalent_recipients_per_s":capture.items as f64/wall_s,"stores":stores});
        eprintln!("publication_replay_cycle_complete {report}");
        reports.push(report);
    }
    let verification = Instant::now();
    let verified_objects = verify(&capture, &destination, cycles).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema":"publication-replay/v1","not_workflow_qualification":true,"capture":source,"destination":destination,"captured_recipients":capture.items,"physical_stores":capture.stores.len(),"cycles":reports,"preload_s":preload_s,"verification_s":verification.elapsed().as_secs_f64(),"verified_objects":verified_objects,"omitted_other_snapshot_files":capture.omitted_files,"omitted_other_snapshot_bytes":capture.omitted_bytes,"exclusions":["application and Turso work","command serialization and manifest planning","online workflow stage/reporting barriers","other mutable/catalog log files and their unknown overwritten publication history","original engine runtime/queue topology"],"runtime_worker_threads":16,"per_store_publication_streams":1,"publication_model":"one ordered stream per store; all stores concurrent; original data chunk boundaries; data durable before referencing manifest; new destination per cycle; source reads and final byte verification outside timed cycles"})
        )?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join("source/shard-0/log/tenant");
        std::fs::create_dir_all(store.join("fwlog")).unwrap();
        std::fs::create_dir_all(store.join("fwmeta/manifest")).unwrap();
        std::fs::write(store.join("fwlog/00000000000000000001"), b"abcd").unwrap();
        let manifest = json!({"entries":[["a",{"location":{"object_id":"fwlog/00000000000000000001","byte_start":0,"byte_len":2},"base_offset":0,"record_count":1}],["b",{"location":{"object_id":"fwlog/00000000000000000001","byte_start":2,"byte_len":2},"base_offset":0,"record_count":1}]]});
        std::fs::write(
            store.join("fwmeta/manifest/00000000000000000001"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(root.path().join("capture.json"), serde_json::to_vec(&json!({"exit_code":0,"result":{"schema":"campaign-capacity/v3","cycles":1,"items":2,"physical_shards":1}})).unwrap()).unwrap();
        root
    }
    fn mutate_manifest(root: &Path, change: impl FnOnce(&mut Value)) {
        let path = root.join("source/shard-0/log/tenant/fwmeta/manifest/00000000000000000001");
        let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        change(&mut value);
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
    #[test]
    fn shared_data_object_is_published_once_and_accounted_exactly() {
        let root = fixture();
        std::fs::write(
            root.path().join("source/shard-0/log/tenant/other-metadata"),
            b"omitted",
        )
        .unwrap();
        let capture = load_capture(root.path()).unwrap();
        assert_eq!(capture.items, 2);
        assert_eq!(capture.stores[0].groups[0].data.len(), 1);
        assert_eq!(
            capture.stores[0].groups[0].chunks["fwlog/00000000000000000001"]
                .iter()
                .map(Bytes::len)
                .collect::<Vec<_>>(),
            vec![2, 2]
        );
        assert_eq!(capture.stores[0].publications, 2);
        assert_eq!(
            capture.stores[0].bytes,
            4 + capture.stores[0].groups[0].manifest.len() as u64
        );
        assert_eq!((capture.omitted_files, capture.omitted_bytes), (1, 7));
    }
    #[test]
    fn missing_referenced_data_is_rejected() {
        let root = fixture();
        std::fs::remove_file(
            root.path()
                .join("source/shard-0/log/tenant/fwlog/00000000000000000001"),
        )
        .unwrap();
        assert!(load_capture(root.path()).is_err());
    }
    #[test]
    fn invalid_range_and_missing_partition_prefix_are_rejected() {
        let root = fixture();
        mutate_manifest(root.path(), |m| {
            m["entries"][0][1]["location"]["byte_len"] = json!(5)
        });
        assert!(load_capture(root.path()).is_err());
        mutate_manifest(root.path(), |m| {
            m["entries"][0][1]["location"]["byte_len"] = json!(2);
            m["entries"][0][1]["base_offset"] = json!(1);
        });
        assert!(load_capture(root.path()).is_err());
        mutate_manifest(root.path(), |m| {
            m["entries"][0][1]["base_offset"] = json!(0);
            m["entries"][0][1]["location"]["byte_len"] = json!(1);
        });
        assert!(load_capture(root.path()).is_err(), "chunk gap must fail");
        mutate_manifest(root.path(), |m| {
            m["entries"][0][1]["location"]["byte_len"] = json!(3)
        });
        assert!(
            load_capture(root.path()).is_err(),
            "overlapping chunks must fail"
        );
    }
    #[test]
    fn unreferenced_data_cannot_silently_reduce_the_replayed_work() {
        let root = fixture();
        std::fs::write(
            root.path()
                .join("source/shard-0/log/tenant/fwlog/00000000000000000002"),
            b"orphan",
        )
        .unwrap();
        assert!(load_capture(root.path()).is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replay_verifies_all_bytes_and_detects_destination_corruption() {
        let root = fixture();
        let capture = load_capture(root.path()).unwrap();
        let destination = tempfile::tempdir().unwrap();
        for cycle in 0..2 {
            let report = publish_store(
                Arc::clone(&capture.stores[0]),
                destination.path().join(format!("cycle-{cycle}")),
            )
            .await
            .unwrap();
            assert_eq!(report["publications"], 2);
            assert_eq!(report["media_ops"], 4);
        }
        assert_eq!(verify(&capture, destination.path(), 2).await.unwrap(), 4);
        std::fs::write(
            destination
                .path()
                .join("cycle-1/shard-0/log/tenant/fwlog/00000000000000000001"),
            b"bad!",
        )
        .unwrap();
        assert!(verify(&capture, destination.path(), 2).await.is_err());
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    use object_log::{MemoryBlobStore, ObjectLogError};
    use std::ops::Range;
    use std::sync::Mutex;
    struct RecordingStore {
        inner: MemoryBlobStore,
        calls: Mutex<Vec<String>>,
        fail_data: bool,
    }
    #[async_trait::async_trait]
    impl BlobStore for RecordingStore {
        async fn put(&self, key: &str, value: Bytes) -> std::result::Result<(), ObjectLogError> {
            self.calls.lock().unwrap().push(key.into());
            if self.fail_data && key.starts_with("fwlog/") {
                return Err(ObjectLogError::StorageUnavailable(
                    "injected data failure".into(),
                ));
            }
            self.inner.put(key, value).await
        }
        async fn get(&self, key: &str) -> std::result::Result<Option<Bytes>, ObjectLogError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> std::result::Result<Option<Bytes>, ObjectLogError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, ObjectLogError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> std::result::Result<(), ObjectLogError> {
            self.inner.delete(key).await
        }
    }
    #[tokio::test]
    async fn manifests_follow_successful_data_and_stop_after_data_failure() {
        let store = Store {
            relative_root: "unused".into(),
            publications: 4,
            bytes: 4,
            groups: vec![
                Group {
                    data: vec![("fwlog/one".into(), Bytes::from_static(b"1"))],
                    chunks: BTreeMap::from([("fwlog/one".into(), vec![Bytes::from_static(b"1")])]),
                    manifest_key: "fwmeta/manifest/one".into(),
                    manifest: Bytes::from_static(b"a"),
                },
                Group {
                    data: vec![("fwlog/two".into(), Bytes::from_static(b"2"))],
                    chunks: BTreeMap::from([("fwlog/two".into(), vec![Bytes::from_static(b"2")])]),
                    manifest_key: "fwmeta/manifest/two".into(),
                    manifest: Bytes::from_static(b"b"),
                },
            ],
        };
        let success = RecordingStore {
            inner: MemoryBlobStore::new(),
            calls: Mutex::new(Vec::new()),
            fail_data: false,
        };
        publish_history(&store, &success).await.unwrap();
        assert_eq!(
            *success.calls.lock().unwrap(),
            [
                "fwlog/one",
                "fwmeta/manifest/one",
                "fwlog/two",
                "fwmeta/manifest/two"
            ]
        );
        let failure = RecordingStore {
            inner: MemoryBlobStore::new(),
            calls: Mutex::new(Vec::new()),
            fail_data: true,
        };
        assert!(publish_history(&store, &failure).await.is_err());
        assert_eq!(*failure.calls.lock().unwrap(), ["fwlog/one"]);
        assert!(
            failure
                .inner
                .list("fwmeta/manifest/")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
