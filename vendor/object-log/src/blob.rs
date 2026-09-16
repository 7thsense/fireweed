//! The object-storage port and the bundled adapters.

use crate::ObjectLogError;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

/// Durable-media accounting for the flush budget controller (TD-004).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MediaOpStats {
    /// File/dir fsyncs or billable durable requests performed since last take.
    pub media_ops: u64,
    /// Bytes written through durable (or memory) put paths since last take.
    pub bytes: u64,
}

/// Suffix for in-flight temp files written by [`LocalBlobStore`]; keys ending in
/// it are skipped by [`list`](BlobStore::list).
const TMP_SUFFIX: &str = ".olog-tmp";

/// A minimal async object store over immutable, string-keyed blobs.
///
/// This is the storage *port* the log engine is built on. Implement it for your
/// backend (e.g. S3) to store the log there. The engine writes each flushed
/// object under a unique key, so no conditional writes are needed.
///
/// Durability: [`put`](BlobStore::put) is **durable-on-return** for crash-durable
/// adapters ([`LocalBlobStore`], an S3 adapter) — once it resolves `Ok`, the bytes
/// survive a crash, so a caller may treat `Ok` as a durability barrier.
/// [`MemoryBlobStore`] is a test/dev backend and is **not** crash-durable.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Durably store `value` at `key` (see the trait-level durability note).
    /// `value` may be arbitrarily large; a network adapter should chunk it
    /// (e.g. S3 multipart) rather than rely on a single request.
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError>;

    /// Durably store a logical object assembled from immutable byte chunks.
    ///
    /// The default implementation concatenates chunks and delegates to
    /// [`put`](BlobStore::put). Object-store adapters can override this to stream
    /// or multipart-upload chunks without materializing the full object twice.
    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        match chunks.len() {
            0 => self.put(key, Bytes::new()).await,
            1 => self.put(key, chunks.into_iter().next().unwrap()).await,
            _ => {
                let total = chunks.iter().map(Bytes::len).sum();
                let mut value = BytesMut::with_capacity(total);
                for chunk in chunks {
                    value.extend_from_slice(&chunk);
                }
                self.put(key, value.freeze()).await
            }
        }
    }

    /// Fetch the whole object at `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError>;

    /// Read a byte sub-range of an object without fetching the whole thing.
    /// `None` if the key is absent. `range.end > len` or `range.start > range.end`
    /// is [`ObjectLogError::RangeOutOfBounds`]; an empty `n..n` returns
    /// `Ok(Some(<empty>))`. No integrity check is performed — payloads are opaque.
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError>;

    /// List keys beginning with `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError>;

    /// Delete an object; deleting a missing key is a no-op success.
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError>;

    /// Snapshot and reset media-op counters since the previous take.
    ///
    /// Adapters that can count exact durable units (fsyncs, S3 PUTs) should
    /// implement this. The default returns [`None`]; the engine then treats each
    /// successful [`put`](BlobStore::put) / [`put_chunks`](BlobStore::put_chunks)
    /// as **one** media op (documented fallback).
    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        None
    }
}

/// Slice `bytes` by `range`, applying the [`BlobStore::get_range`] bounds rules.
fn slice_range(bytes: &Bytes, range: Range<u64>) -> Result<Bytes, ObjectLogError> {
    if range.start > range.end {
        return Err(ObjectLogError::RangeOutOfBounds(format!(
            "start {} > end {}",
            range.start, range.end
        )));
    }
    let len = bytes.len() as u64;
    if range.end > len {
        return Err(ObjectLogError::RangeOutOfBounds(format!(
            "end {} > len {len}",
            range.end
        )));
    }
    Ok(bytes.slice(range.start as usize..range.end as usize))
}

/// In-process [`BlobStore`] backed by a map. For tests and development; state is
/// lost on drop and is **not** crash-durable.
#[derive(Clone, Default)]
pub struct MemoryBlobStore {
    objects: Arc<Mutex<BTreeMap<String, Bytes>>>,
    bytes_written: Arc<AtomicU64>,
}

impl MemoryBlobStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored objects (lets tests assert PUT-count / cost invariants).
    pub fn object_count(&self) -> usize {
        self.objects.lock().expect("poisoned").len()
    }

    /// Total stored bytes across all objects.
    pub fn total_bytes(&self) -> usize {
        self.objects
            .lock()
            .expect("poisoned")
            .values()
            .map(|v| v.len())
            .sum()
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        self.objects
            .lock()
            .expect("poisoned")
            .insert(key.to_string(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        Ok(self.objects.lock().expect("poisoned").get(key).cloned())
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        let object = self.objects.lock().expect("poisoned").get(key).cloned();
        match object {
            Some(bytes) => Ok(Some(slice_range(&bytes, range)?)),
            None => Ok(None),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        Ok(self
            .objects
            .lock()
            .expect("poisoned")
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.objects.lock().expect("poisoned").remove(key);
        Ok(())
    }

    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        // Memory has no crash-durable media cost.
        Some(MediaOpStats {
            media_ops: 0,
            bytes: self.bytes_written.swap(0, Ordering::Relaxed),
        })
    }
}

/// Concurrent publishers may share a directory barrier, but only for renames
/// completed before that barrier starts. Failed syncs never advance coverage.
#[derive(Default)]
struct DirectorySync {
    renamed: AtomicU64,
    durable: Mutex<u64>,
}

impl DirectorySync {
    fn register_rename(&self) -> std::io::Result<u64> {
        self.renamed
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map(|previous| previous + 1)
            .map_err(|_| std::io::Error::other("directory rename generation exhausted"))
    }

    /// Returns whether this caller performed the sync. The mutex stays held
    /// through completion so followers cannot acknowledge an in-progress sync.
    fn sync_through(
        &self,
        ticket: u64,
        sync: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<bool> {
        let mut durable = self
            .durable
            .lock()
            .map_err(|_| std::io::Error::other("directory sync poisoned"))?;
        if *durable >= ticket {
            return Ok(false);
        }
        let covered = self.renamed.load(Ordering::Acquire);
        sync()?;
        *durable = covered;
        Ok(true)
    }
}

/// Filesystem-backed [`BlobStore`] rooted at a directory.
///
/// Single-node. Writes are **durable-on-return**: each `put` / `put_chunks`:
/// 1. write a temp file in the same directory as the final key  
/// 2. [`std::fs::File::sync_data`] (fdatasync on Unix) so payload bytes are durable  
/// 3. `rename` into place  
/// 4. `fsync` the parent directory so the directory entry survives power loss  
/// Concurrent writes to the same parent may share step 4, without added linger.
///
/// (On macOS, true device durability may need `F_FULLFSYNC`; Linux `sync_data` /
/// dir `sync_all` is the intended contract.)
#[derive(Clone)]
pub struct LocalBlobStore {
    root: Arc<PathBuf>,
    media_ops: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    directory_syncs: Arc<Mutex<BTreeMap<PathBuf, Weak<DirectorySync>>>>,
}

impl LocalBlobStore {
    /// Create a store rooted at `root` (created lazily on first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            media_ops: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            directory_syncs: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn directory_sync(&self, parent: &Path) -> std::io::Result<Arc<DirectorySync>> {
        let mut syncs = self
            .directory_syncs
            .lock()
            .map_err(|_| std::io::Error::other("directory sync registry poisoned"))?;
        if let Some(sync) = syncs.get(parent).and_then(Weak::upgrade) {
            return Ok(sync);
        }
        // Keep only in-flight owners; distinct historical object directories
        // must not grow a permanent registry.
        syncs.retain(|_, sync| sync.strong_count() > 0);
        let sync = Arc::new(DirectorySync::default());
        syncs.insert(parent.to_path_buf(), Arc::downgrade(&sync));
        Ok(sync)
    }

    fn path_for(&self, key: &str) -> Result<PathBuf, ObjectLogError> {
        if key.is_empty() || key.contains('\0') || key.split('/').any(|p| p == "..") {
            return Err(ObjectLogError::InvalidObjectKey(key.to_string()));
        }
        Ok(self.root.join(key))
    }

    /// Durable publish: temp → sync_data → rename → dir fsync. No full pre-merge
    /// of `chunks` (streams them to the temp file).
    fn durable_publish_chunks(
        path: PathBuf,
        chunks: Vec<Bytes>,
        sync: Arc<DirectorySync>,
    ) -> std::io::Result<(u64, bool)> {
        // Opt-in diagnostic only: observe publication without adding barriers.
        // Directory sync time includes waiting for a shared barrier. Failed puts remain errors;
        // phase records below describe successful publications only.
        static TRACE: OnceLock<bool> = OnceLock::new();
        let trace =
            *TRACE.get_or_init(|| std::env::var_os("OBJECT_LOG_LOCAL_PUBLISH_TRACE").is_some());
        let started = trace.then(Instant::now);
        let mut phase = started;
        let mut elapsed = || {
            phase.map_or(0, |before| {
                let now = Instant::now();
                phase = Some(now);
                now.duration_since(before).as_micros()
            })
        };
        let parent = path.parent().expect("object path has a parent");
        std::fs::create_dir_all(parent)?;
        let mkdir_us = elapsed();
        let mut tmp = path.clone().into_os_string();
        tmp.push(TMP_SUFFIX);
        let tmp = PathBuf::from(tmp);
        let mut byte_len = 0u64;
        let (create_us, write_us, data_sync_us);
        {
            let mut f = std::fs::File::create(&tmp)?;
            create_us = elapsed();
            for chunk in &chunks {
                f.write_all(chunk)?;
                byte_len += chunk.len() as u64;
            }
            write_us = elapsed();
            // fdatasync on Unix: data durable; dir fsync below covers the name.
            f.sync_data()?;
            data_sync_us = elapsed();
        }
        std::fs::rename(&tmp, &path)?;
        let ticket = sync.register_rename()?;
        let rename_us = elapsed(); // Includes closing the data file.
        let directory = std::fs::File::open(parent)?;
        let dir_open_us = elapsed();
        let dir_sync_performed = sync.sync_through(ticket, || directory.sync_all())?;
        let dir_sync_us = elapsed();
        if let Some(started) = started {
            eprintln!(
                "local_publish bytes={byte_len} mkdir_us={mkdir_us} create_us={create_us} write_us={write_us} data_sync_us={data_sync_us} rename_us={rename_us} dir_open_us={dir_open_us} dir_sync_us={dir_sync_us} dir_sync_ops={} total_us={}",
                u8::from(dir_sync_performed),
                started.elapsed().as_micros()
            );
        }
        Ok((byte_len, dir_sync_performed))
    }

    fn record_put(&self, (byte_len, dir_sync_performed): (u64, bool)) {
        // Every successful write synced its data; count shared directory syncs once.
        self.media_ops
            .fetch_add(1 + u64::from(dir_sync_performed), Ordering::Relaxed);
        self.bytes_written.fetch_add(byte_len, Ordering::Relaxed);
    }

    async fn publish_async(
        &self,
        path: PathBuf,
        chunks: Vec<Bytes>,
    ) -> Result<(u64, bool), ObjectLogError> {
        let sync = self
            .directory_sync(path.parent().expect("object path has a parent"))
            .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))?;
        let run = move || {
            Self::durable_publish_chunks(path, chunks, sync)
                .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))
        };
        match tokio::runtime::Handle::try_current() {
            // Multi-thread worker: avoid spawn_blocking queue (flush path uses this).
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            // Current-thread runtime (many unit tests): must not block_in_place.
            Ok(_) => tokio::task::spawn_blocking(run)
                .await
                .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))?,
            Err(_) => run(),
        }
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let path = self.path_for(key)?;
        let publication = self.publish_async(path, vec![value]).await?;
        self.record_put(publication);
        Ok(())
    }

    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        match chunks.len() {
            0 => self.put(key, Bytes::new()).await,
            1 => self.put(key, chunks.into_iter().next().unwrap()).await,
            _ => {
                let path = self.path_for(key)?;
                let publication = self.publish_async(path, chunks).await?;
                self.record_put(publication);
                Ok(())
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        let path = self.path_for(key)?;
        match tokio::fs::read(&path).await {
            Ok(v) => Ok(Some(Bytes::from(v))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        if range.start > range.end {
            return Err(ObjectLogError::RangeOutOfBounds(format!(
                "start {} > end {}",
                range.start, range.end
            )));
        }
        let path = self.path_for(key)?;
        tokio::task::spawn_blocking(move || -> Result<Option<Bytes>, ObjectLogError> {
            let mut f = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let len = f.metadata()?.len();
            if range.end > len {
                return Err(ObjectLogError::RangeOutOfBounds(format!(
                    "end {} > len {len}",
                    range.end
                )));
            }
            let count = (range.end - range.start) as usize;
            let mut buf = vec![0u8; count];
            f.seek(SeekFrom::Start(range.start))?;
            f.read_exact(&mut buf)?;
            Ok(Some(Bytes::from(buf)))
        })
        .await
        .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))?
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        let root = self.root.clone();
        let prefix = prefix.to_string();
        let keys = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<String>> {
            let mut out = Vec::new();
            collect_files(&root, &root, &mut out)?;
            Ok(out)
        })
        .await
        .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))??;
        Ok(keys
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        let path = self.path_for(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        Some(MediaOpStats {
            media_ops: self.media_ops.swap(0, Ordering::Relaxed),
            bytes: self.bytes_written.swap(0, Ordering::Relaxed),
        })
    }
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out)?;
        } else if !path.to_string_lossy().ends_with(TMP_SUFFIX)
            && let Ok(rel) = path.strip_prefix(root)
        {
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod directory_sync_tests {
    use super::*;

    #[test]
    fn one_successful_sync_covers_all_preexisting_renames() {
        let sync = DirectorySync::default();
        let first = sync.register_rename().unwrap();
        let second = sync.register_rename().unwrap();
        assert!(sync.sync_through(first, || Ok(())).unwrap());
        assert!(
            !sync
                .sync_through(second, || panic!(
                    "already covered rename must not sync twice"
                ))
                .unwrap()
        );
    }

    #[test]
    fn rename_during_sync_requires_another_successful_barrier() {
        let sync = DirectorySync::default();
        let first = sync.register_rename().unwrap();
        let mut later = None;
        assert!(
            sync.sync_through(first, || {
                later = Some(sync.register_rename().unwrap());
                Ok(())
            })
            .unwrap()
        );
        assert!(sync.sync_through(later.unwrap(), || Ok(())).unwrap());
    }

    #[test]
    fn failed_sync_cannot_acknowledge_any_covered_rename() {
        let sync = DirectorySync::default();
        let first = sync.register_rename().unwrap();
        let second = sync.register_rename().unwrap();
        let failed = sync.sync_through(first, || {
            Err(std::io::Error::other("injected fsync failure"))
        });
        assert!(failed.is_err());
        assert!(sync.sync_through(second, || Ok(())).unwrap());
        assert!(
            !sync
                .sync_through(first, || panic!("second sync covered both"))
                .unwrap()
        );
    }

    #[test]
    fn distinct_directories_have_independent_barriers_and_retire() {
        let store = LocalBlobStore::new("unused-test-root");
        let a = store.directory_sync(Path::new("a")).unwrap();
        let a_again = store.clone().directory_sync(Path::new("a")).unwrap();
        let b = store.directory_sync(Path::new("b")).unwrap();
        assert!(Arc::ptr_eq(&a, &a_again));
        let a_ticket = a.register_rename().unwrap();
        let b_ticket = b.register_rename().unwrap();
        assert!(a.sync_through(a_ticket, || Ok(())).unwrap());
        assert!(b.sync_through(b_ticket, || Ok(())).unwrap());
        let weak = Arc::downgrade(&a);
        drop(a);
        drop(a_again);
        drop(b);
        assert!(weak.upgrade().is_none());
        let _next = store.directory_sync(Path::new("next")).unwrap();
        assert_eq!(store.directory_syncs.lock().unwrap().len(), 1);
    }
}
