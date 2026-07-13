//! # Segmented S3 object-log group-commit substrate (TD-004 production form)
//!
//! This is the **production** object-log substrate, distinct from the in-process [`LocalObjectLog`]
//! file smoke reference. Where `LocalObjectLog` writes ONE object per command, this substrate buffers
//! commands per `(tenant, queue)` and seals **segments** (many commands per durable object) onto an
//! S3-compatible object store, realizing the TD-004 group-commit pipeline:
//!
//! 1. **Buffer** commands per queue in arrival order.
//! 2. **Seal** a segment when EITHER the buffered byte size reaches `target_bytes` OR the oldest
//!    buffered command's age reaches `max_latency_ms` (whichever fires first — TD-004 step 2).
//! 3. **Write segment** — one immutable, checksummed object per sealed segment (TD-004 step 3).
//! 4. **Commit manifest head** — append a manifest-head entry naming the attempt segment via a conditional
//!    (create-only) object write that is the CAS boundary AND the epoch fence (TD-004 step 4).
//! 5. **Ack** — a command's positions are returned to the caller ONLY after its segment's manifest
//!    entry is durably committed (TD-004 step 5). A buffered-but-unsealed command is NOT acked, and a
//!    segment whose manifest-head commit was fenced is an orphan that no reader ever observes.
//!
//! **Manifest-CAS epoch fence (reused from the `pqueue-e5c6d6fc` pattern).** Each manifest entry records
//! the writer's `assignment_epoch`. The manifest is an append-only series of immutable objects
//! `manifest/{index:020}.json`; a commit is a create-only PUT at the next index (the CAS) gated on the
//! writer's `expected_epoch` still equalling the queue's current epoch (the highest epoch any committed
//! manifest entry records). The durable head namespace is `manifest_head/{index:020}.json`; successful
//! commits are also mirrored to the legacy `manifest/{index:020}.json` namespace for older readers and
//! existing tooling. An epoch handoff publishes a **fence entry** (TD-004 implementation (b)) into the
//! manifest head BEFORE the new owner writes data; a stale-epoch writer that tries to seal then observes the
//! higher epoch on the manifest tail and is rejected [`EngineError::EpochFenced`] — no torn segment is
//! committed (the fence is checked before the segment object is written).
//!
//! **Object store seam.** The substrate is generic over [`BlobStore`], whose only required primitive beyond
//! plain `get`/`put`/`list` is `put_if_absent` (create-only PUT = the CAS). [`InMemoryBlobStore`] backs the
//! unit tests with no network; [`S3BlobStore`] is a minimal hand-rolled SigV4 S3 client (PUT/GET/LIST +
//! create-only conditional PUT) that runs the SAME substrate against MinIO / any S3-compatible store.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::QueueDefinition;
use pqueue_engine::{
    CommandEnvelope, CommandPosition, EngineError, EngineResult, QueueCommand, QueueKey,
    validate_gate_command,
};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Object-store seam (the minimal S3 surface the substrate needs)
// ---------------------------------------------------------------------------

/// The minimal S3-compatible object surface the segmented substrate drives. Implemented in-memory (unit
/// tests, no network) and over a real S3 endpoint ([`S3BlobStore`], tested against MinIO).
pub trait BlobStore: Send + Sync {
    /// Unconditional PUT (used for immutable, deterministically-keyed segment objects; a retried write
    /// re-puts identical bytes, so it is idempotent at a stable key — TD-004 "idempotent segment PUT").
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()>;

    /// Conditional create-only PUT — the CAS primitive. `Ok(true)` if the object was created, `Ok(false)`
    /// if an object already exists at `key` (the conditional failed). This is the manifest commit's
    /// compare-and-set: two writers racing to extend the manifest from the same tail cannot both win.
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool>;

    /// GET an object. `Ok(None)` if it does not exist.
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>>;

    /// Delete an object. `Ok(true)` if the object existed and was removed, `Ok(false)` otherwise.
    fn delete(&self, key: &str) -> EngineResult<bool>;

    /// List keys under `prefix` (lexical order not required; the caller sorts).
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>>;

    /// Range-LIST: keys under `prefix` that sort strictly AFTER `start_after` (bead pqueue-8928baec). This
    /// is the read-cost primitive behind the durable read-horizon watermark: manifest keys are fixed-width
    /// `manifest/{index:020}.json`, so lexicographic order == numeric index order, and
    /// `start_after = "{prefix}manifest/{W:020}.json"` returns exactly the indices `> W` (the LIVE,
    /// above-floor manifest entries). The default filters after a full `list`, which is CORRECT for every
    /// impl but does NOT reduce enumeration cost; [`S3BlobStore`] OVERRIDES it to pass `StartAfter` NATIVELY
    /// so the real read-cost win lands at scale (only `> W` keys are enumerated/paginated).
    fn list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        self.list_from_with_request_count(prefix, start_after)
            .map(|(keys, _)| keys)
    }

    /// Range-LIST reporting the number of billable LIST-class API requests consumed (the `list_from`
    /// counterpart of [`Self::list_with_request_count`]). The default is one request (filter-after-list);
    /// [`S3BlobStore`] overrides it to report every `ListObjectsV2` page it pages through, so a >1000-live-key
    /// ranged list bills accurately in the cost ledger.
    fn list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> EngineResult<(Vec<String>, u64)> {
        let keys = self
            .list(prefix)?
            .into_iter()
            .filter(|k| k.as_str() > start_after)
            .collect();
        Ok((keys, 1))
    }

    /// List keys and report the number of billable LIST-class API requests consumed.
    ///
    /// Most in-process/local implementations satisfy one logical list with one
    /// request. S3-compatible stores may page `ListObjectsV2`, so one logical
    /// list can consume several billable LIST operations.
    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        self.list(prefix).map(|keys| (keys, 1))
    }

    /// Current object-count and byte-size stats under `prefix`. This is an
    /// evidence/introspection helper, not part of the hot append path.
    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        let mut stats = ObjectStoreStats::default();
        for key in self.list(prefix)? {
            if let Some(bytes) = self.get(&key)? {
                stats.object_count += 1;
                stats.total_bytes += bytes.len() as u64;
                stats.max_object_bytes = stats.max_object_bytes.max(bytes.len() as u64);
            }
        }
        Ok(stats)
    }
}

/// Share one store between several owners (e.g. two competing epoch holders) — delegates through the `Arc`.
impl<T: BlobStore + ?Sized> BlobStore for std::sync::Arc<T> {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        (**self).put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        (**self).put_if_absent(key, body)
    }
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        (**self).get(key)
    }
    fn delete(&self, key: &str) -> EngineResult<bool> {
        (**self).delete(key)
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        (**self).list(prefix)
    }
    fn list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        (**self).list_from(prefix, start_after)
    }
    fn list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> EngineResult<(Vec<String>, u64)> {
        (**self).list_from_with_request_count(prefix, start_after)
    }
    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        (**self).list_with_request_count(prefix)
    }
    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        (**self).stats(prefix)
    }
}

/// In-memory [`BlobStore`] for unit tests — no network. `put_if_absent` is a genuine compare-and-set under
/// the map lock, so the manifest-CAS / epoch-fence path is exercised without an S3 server.
#[derive(Default)]
pub struct InMemoryBlobStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of objects currently stored (test introspection).
    pub fn object_count(&self) -> usize {
        self.objects.lock().expect("blobstore poisoned").len()
    }
}

impl BlobStore for InMemoryBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.objects
            .lock()
            .expect("blobstore poisoned")
            .insert(key.to_string(), body.to_vec());
        Ok(())
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let mut g = self.objects.lock().expect("blobstore poisoned");
        if g.contains_key(key) {
            return Ok(false);
        }
        g.insert(key.to_string(), body.to_vec());
        Ok(true)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        Ok(self
            .objects
            .lock()
            .expect("blobstore poisoned")
            .get(key)
            .cloned())
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        Ok(self
            .objects
            .lock()
            .expect("blobstore poisoned")
            .remove(key)
            .is_some())
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .expect("blobstore poisoned")
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        let mut stats = ObjectStoreStats::default();
        for (key, bytes) in self.objects.lock().expect("blobstore poisoned").iter() {
            if key.starts_with(prefix) {
                stats.object_count += 1;
                stats.total_bytes += bytes.len() as u64;
                stats.max_object_bytes = stats.max_object_bytes.max(bytes.len() as u64);
            }
        }
        Ok(stats)
    }
}

/// Durable local-filesystem [`BlobStore`] over a directory tree (the production-shaped, no-network store
/// used by the `object_log_sqlite_projection` segmented backend). Each object key maps to a file under
/// `root` (the key's `/` separators become directory levels). Few large segment objects + an append-only
/// manifest, so the per-object file overhead is amortized across a whole group-commit batch — unlike the
/// per-command `LocalObjectLog`, which writes one object file PER command.
///
/// - `put` is atomic (write a sibling temp file, then `rename` over the target — a reader never sees a
///   half-written object).
/// - `put_if_absent` is the manifest-CAS primitive: `create_new(true)` (`O_EXCL`) — exactly one racing
///   writer creates the manifest entry, the rest observe `AlreadyExists` and lose the CAS.
/// - `get` returns `None` for a missing file; `list(prefix)` walks the tree and returns matching keys.
pub struct LocalFsBlobStore {
    root: PathBuf,
}

/// Monotonic suffix source so concurrent `put`s never collide on the same temp filename.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static SEGMENT_ATTEMPT_COUNTER: AtomicU64 = AtomicU64::new(0);

impl LocalFsBlobStore {
    /// Open a store rooted at `root` (created on first write).
    pub fn open(root: impl Into<PathBuf>) -> EngineResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(store_err)?;
        Ok(Self { root })
    }

    /// Map an object key (`a/b/c.json`) to its on-disk path under `root`.
    fn key_path(&self, key: &str) -> PathBuf {
        let mut p = self.root.clone();
        for comp in key.split('/') {
            if !comp.is_empty() {
                p.push(comp);
            }
        }
        p
    }

    /// A unique sibling temp path for an atomic `put` (same parent dir → `rename` is atomic).
    fn tmp_path(target: &Path) -> PathBuf {
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let parent = target.parent().unwrap_or(Path::new("."));
        parent.join(format!(".tmp-{pid}-{n}"))
    }
}

/// Recursively collect file keys (relative to `root`, `/`-joined) under `dir`, skipping temp files.
fn walk_keys(root: &Path, dir: &Path, out: &mut Vec<String>) -> EngineResult<()> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(store_err(e)),
    };
    for entry in rd {
        let entry = entry.map_err(store_err)?;
        let ft = entry.file_type().map_err(store_err)?;
        let path = entry.path();
        if ft.is_dir() {
            walk_keys(root, &path, out)?;
        } else if ft.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with(".tmp-")
            {
                continue;
            }
            if let Ok(rel) = path.strip_prefix(root) {
                let key = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push(key);
            }
        }
    }
    Ok(())
}

impl BlobStore for LocalFsBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(store_err)?;
        }
        let tmp = Self::tmp_path(&path);
        fs::write(&tmp, body).map_err(store_err)?;
        fs::rename(&tmp, &path).map_err(store_err)?;
        Ok(())
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(store_err)?;
        }
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut f) => {
                f.write_all(body).map_err(store_err)?;
                f.flush().map_err(store_err)?;
                Ok(true)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(store_err(e)),
        }
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        match fs::read(self.key_path(key)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(store_err(e)),
        }
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        match fs::remove_file(self.key_path(key)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(store_err(e)),
        }
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        let mut out = Vec::new();
        // Scope the directory walk to the prefix's subtree when the prefix names a directory boundary
        // (ends in `/`, e.g. `…/manifest/`): walking only that subtree keeps a per-seal manifest list O(its
        // own entries) instead of O(every object under root) — a sustained push writes one seg object per
        // seal, so a whole-root walk per seal would itself be O(n^2). Every key under `root/prefix` starts
        // with `prefix` by construction, so the result is identical to a full walk + `starts_with` filter.
        if let Some(dir) = prefix.strip_suffix('/') {
            walk_keys(&self.root, &self.root.join(dir), &mut out)?;
        } else {
            walk_keys(&self.root, &self.root, &mut out)?;
            out.retain(|k| k.starts_with(prefix));
        }
        Ok(out)
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        let mut stats = ObjectStoreStats::default();
        for key in self.list(prefix)? {
            let len = fs::metadata(self.key_path(&key)).map_err(store_err)?.len();
            stats.object_count += 1;
            stats.total_bytes += len;
            stats.max_object_bytes = stats.max_object_bytes.max(len);
        }
        Ok(stats)
    }
}

// ---------------------------------------------------------------------------
// Configuration (TD-004 §Configuration Validation)
// ---------------------------------------------------------------------------

/// Group-commit segment-sizing configuration. Two independent seal triggers are supported (TD-004 step 2):
/// a byte-size threshold (`target_bytes`) AND a latency cap (`max_latency_ms`); a segment seals on
/// whichever fires first. `>= 2` distinct configurations are therefore expressible (e.g. a small-latency
/// profile and a large-batch profile), satisfying the substrate's "configurable segment sizing" contract.
#[derive(Debug, Clone, Copy)]
pub struct SegmentConfig {
    /// Seal when the buffered serialized byte size reaches this value (TD-004 `segment_target_bytes`).
    pub target_bytes: usize,
    /// Seal when the oldest buffered command's age reaches this many ms (TD-004 `segment_max_latency_ms`).
    pub max_latency_ms: u64,
    /// Dev/test escape hatch that allows sealing one command per segment. MUST be false in production
    /// (TD-004 "Reject 1-object-per-command"); a production config seals many commands per segment.
    pub dev_unsafe_one_command_segments: bool,
}

impl SegmentConfig {
    /// Validate (`max_latency_ms > 0`, `target_bytes > 0`; TD-004 §Window sanity). Returns the config.
    pub fn new(target_bytes: usize, max_latency_ms: u64) -> EngineResult<Self> {
        if max_latency_ms == 0 {
            return Err(EngineError::Invalid("segment_max_latency_ms must be > 0"));
        }
        if target_bytes == 0 {
            return Err(EngineError::Invalid("segment_target_bytes must be > 0"));
        }
        Ok(Self {
            target_bytes,
            max_latency_ms,
            dev_unsafe_one_command_segments: false,
        })
    }
}

// ---------------------------------------------------------------------------
// Measured counters surface (release-ledger harness)
// ---------------------------------------------------------------------------

/// Current object-store utilization under an object prefix.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreStats {
    pub object_count: u64,
    pub total_bytes: u64,
    pub max_object_bytes: u64,
}

/// Release-measurable counters: how many segments sealed, how many objects were PUT to the store (segment
/// objects + manifest objects + fence entries), how many commands committed, and the per-segment
/// group-commit batch sizes. Surfaced to the release ledger harness as the object-log cost evidence
/// (segments, not commands, are the durable-commit unit).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentCounters {
    /// Sealed-and-committed data segments (the durable-commit unit; cost scales with this, not commands).
    pub segments_sealed: u64,
    /// Total objects PUT to the store (segment objects + committed manifest objects + epoch-fence entries).
    pub objects_put: u64,
    /// Commands durably committed (acked) through sealed segments.
    pub commands_committed: u64,
    /// Per-segment group-commit batch size (commands per sealed segment), in seal order.
    pub group_commit_batches: Vec<usize>,
    /// Current object/file count under the object-log store prefix.
    pub object_count: u64,
    /// Current total bytes retained under the object-log store prefix.
    pub total_bytes: u64,
    /// Bytes retained in sealed data segment objects only.
    pub segment_bytes: u64,
    /// Largest current object size in bytes.
    pub max_object_bytes: u64,
    /// Count of object-store PUT-class API calls issued by this process.
    pub put_count: u64,
    /// Count of object-store GET API calls issued by this process.
    pub get_count: u64,
    /// Count of object-store LIST API calls issued by this process.
    pub list_count: u64,
    /// Count of object-store DELETE API calls issued by this process. The current append-only log does not
    /// delete objects during the release run, but retention cleanup must be visible when it is added.
    pub delete_count: u64,
}

impl SegmentCounters {
    /// Mean commands-per-segment (the group-commit amortization factor); `0.0` if nothing sealed.
    pub fn mean_batch_size(&self) -> f64 {
        if self.group_commit_batches.is_empty() {
            return 0.0;
        }
        self.commands_committed as f64 / self.group_commit_batches.len() as f64
    }

    /// Largest sealed group-commit batch (`0` if nothing sealed).
    pub fn max_batch_size(&self) -> usize {
        self.group_commit_batches.iter().copied().max().unwrap_or(0)
    }

    pub fn mean_object_bytes(&self) -> f64 {
        if self.object_count == 0 {
            return 0.0;
        }
        self.total_bytes as f64 / self.object_count as f64
    }

    pub fn storage_utilization_ratio(&self, target_segment_bytes: usize) -> f64 {
        if self.segments_sealed == 0 || target_segment_bytes == 0 {
            return 0.0;
        }
        self.segment_bytes as f64 / (self.segments_sealed as f64 * target_segment_bytes as f64)
    }
}

// ---------------------------------------------------------------------------
// On-store object formats
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Sealed-segment binary frame (Fix A)
// ---------------------------------------------------------------------------
//
// A sealed segment object is a small fixed header followed by a length-prefixed concatenation of the
// per-command records that were buffered for it. Each record's bytes are the `postcard` encoding of one
// `CommandEnvelope`, produced ONCE when the command was buffered (`enqueue`) and stored verbatim — the seal
// never re-serializes. This replaces the prior format, which JSON-serialized every envelope a second time on
// seal (and a THIRD throwaway time per command just to measure its buffered size). The on-store layout is:
//
//   magic   : b"PQSG"          (4 bytes)
//   version : u8  = SEG_VERSION (segment-format marker; bumped from the JSON form)
//   epoch   : u64 little-endian (the assignment epoch the run committed under)
//   first_seq: u64 little-endian (the sequence of the first record)
//   records : [ u32 count ][ for each: u32 len, len bytes ]   (the "records blob")
//
// The per-segment checksum stored in the manifest entry is the FNV-1a of the records-blob bytes only (the
// header is reconstructable and excluded), validated on read before any record is decoded.

/// Segment object magic + version. The version is bumped from the previous JSON segment form (pre-release,
/// so no on-disk back-compat is owed — a stale object simply fails to parse rather than mis-decoding).
const SEG_MAGIC: [u8; 4] = *b"PQSG";
const SEG_VERSION: u8 = 2;
const SEG_HEADER_LEN: usize = 4 + 1 + 8 + 8;

/// Build a sealed-segment object from already-encoded per-command record bytes (no re-serialize). Returns
/// `(object_bytes, checksum)` where `checksum` is the FNV-1a over the records-blob region only.
fn build_segment_object(epoch: u64, first_seq: u64, records: &[Vec<u8>]) -> (Vec<u8>, u64) {
    let records_len: usize = records.iter().map(|r| 4 + r.len()).sum();
    let mut blob = Vec::with_capacity(4 + records_len);
    blob.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for r in records {
        blob.extend_from_slice(&(r.len() as u32).to_le_bytes());
        blob.extend_from_slice(r);
    }
    let checksum = checksum(&blob);
    let mut object = Vec::with_capacity(SEG_HEADER_LEN + blob.len());
    object.extend_from_slice(&SEG_MAGIC);
    object.push(SEG_VERSION);
    object.extend_from_slice(&epoch.to_le_bytes());
    object.extend_from_slice(&first_seq.to_le_bytes());
    object.extend_from_slice(&blob);
    (object, checksum)
}

/// Parse a sealed-segment object: validate the header, verify the records-blob checksum against the
/// manifest entry, then `postcard`-decode each framed record. Returns `(epoch, first_seq, commands)`.
fn parse_segment_object(
    bytes: &[u8],
    seg_key: &str,
    expected_checksum: u64,
) -> EngineResult<(u64, u64, Vec<CommandEnvelope>)> {
    if bytes.len() < SEG_HEADER_LEN || bytes[..4] != SEG_MAGIC {
        return Err(EngineError::Storage(format!(
            "segment {seg_key} has a bad header"
        )));
    }
    if bytes[4] != SEG_VERSION {
        return Err(EngineError::Storage(format!(
            "segment {seg_key} has unsupported format version {}",
            bytes[4]
        )));
    }
    let epoch = u64::from_le_bytes(bytes[5..13].try_into().expect("8 bytes"));
    let first_seq = u64::from_le_bytes(bytes[13..21].try_into().expect("8 bytes"));
    let blob = &bytes[SEG_HEADER_LEN..];
    if checksum(blob) != expected_checksum {
        return Err(EngineError::Storage(format!(
            "segment checksum mismatch at {seg_key}"
        )));
    }
    let mut cursor = 0usize;
    let read_u32 = |buf: &[u8], cur: &mut usize| -> EngineResult<u32> {
        if *cur + 4 > buf.len() {
            return Err(EngineError::Storage(format!("segment {seg_key} truncated")));
        }
        let v = u32::from_le_bytes(buf[*cur..*cur + 4].try_into().expect("4 bytes"));
        *cur += 4;
        Ok(v)
    };
    let count = read_u32(blob, &mut cursor)? as usize;
    let mut commands = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(blob, &mut cursor)? as usize;
        if cursor + len > blob.len() {
            return Err(EngineError::Storage(format!("segment {seg_key} truncated")));
        }
        let env: CommandEnvelope =
            serde_json::from_slice(&blob[cursor..cursor + len]).map_err(store_err)?;
        commands.push(env);
        cursor += len;
    }
    Ok((epoch, first_seq, commands))
}

/// One append-only manifest entry. A data entry names a segment; a `fence` entry records an epoch handoff
/// and names no segment (TD-004 implementation (b): epoch fence published to the manifest before handoff).
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestEntry {
    index: u64,
    epoch: u64,
    #[serde(default)]
    fence: bool,
    segment_key: Option<String>,
    first_seq: u64,
    last_seq: u64,
    /// For branched views, the same immutable segment object may be shared while only a prefix of the
    /// commands is visible. `None` means the full segment is visible.
    #[serde(default)]
    visible_last_seq: Option<u64>,
    /// Per-segment checksum over the serialized commands (TD-004 step 2 segment checksum), validated on read.
    checksum: u64,
    committed_at_ms: i64,
    /// A RETENTION-FLOOR-ADVANCE entry (bead pqueue-b5cc2bc7 bug 3): names no segment (`segment_key: None`,
    /// `fence: false`) and records the highest command sequence whose segment objects are reclaimed, at this
    /// entry's `epoch`. The AUTHORITATIVE floor is the max of these across the manifest. The advance is an
    /// epoch-fenced, create-only manifest CAS at the next index — EXACTLY like a data/fence commit — so a
    /// superseded owner cannot atomically-lose-the-CAS-and-still-regress the floor (closing the racy
    /// read-then-overwrite TOCTOU of the old `retention_floor.json` blob). `None` for data/fence entries; a
    /// pre-existing manifest (written before this field existed) defaults every entry to `None`.
    #[serde(default)]
    retention_floor_through: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct BranchMetadata {
    source: QueueKey,
    branch: QueueKey,
    #[serde(default)]
    source_epoch: u64,
    cut_sequence: u64,
    ttl_ms: u64,
    created_at_ms: i64,
    expires_at_ms: i64,
    emit_change_records: bool,
}

/// FNV-1a 64-bit checksum (small, dependency-free) over a segment's serialized bytes.
fn checksum(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn store_err<E: std::fmt::Display>(e: E) -> EngineError {
    EngineError::Storage(e.to_string())
}

fn to_json<T: serde::Serialize>(v: &T) -> EngineResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(store_err)
}

/// Epoch-milliseconds of a command envelope's `created_at` (bead pqueue-b5cc2bc7 bug 1). Mirrors
/// `pqueue_engine`'s internal `ts_to_ms`; used to stamp a sealed segment's `committed_at_ms` as an upper bound
/// on the `created_at` of every envelope it holds.
fn created_at_ms(env: &CommandEnvelope) -> i64 {
    env.created_at
        .seconds
        .saturating_mul(1000)
        .saturating_add((env.created_at.nanoseconds / 1_000_000) as i64)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Path-safe, collision-free per-queue key prefix: `t/{hex(tenant)}/q/{hex(queue)}/` (TD-004 §Object Layout:
/// tenant/queue leading; hex-encoded so arbitrary id bytes are S3-key-safe).
fn shard_prefix(shard: &QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex_lower(shard.tenant_id.as_str().as_bytes()),
        hex_lower(shard.queue_id.as_str().as_bytes())
    )
}

fn branch_registry_key(source: &QueueKey, branch: &QueueKey) -> String {
    format!(
        "{}branches/{}/{}.json",
        shard_prefix(source),
        hex_lower(branch.tenant_id.as_str().as_bytes()),
        hex_lower(branch.queue_id.as_str().as_bytes())
    )
}

fn branch_metadata_key(branch: &QueueKey) -> String {
    format!("{}branch.json", shard_prefix(branch))
}

/// The "branch creation in progress" sentinel (bead pqueue-b5cc2bc7): written EARLY (right after the source
/// pin) and dropped when the `branch.json` commit marker lands. Its presence WITHOUT the commit marker means a
/// PARTIAL/uncommitted branch — treated as non-existent by every segment-reading path, so a failed/partial
/// branch is never readable and can never GET a (source or its own) object, regardless of pin/TTL/cleanup.
fn branch_pending_key(branch: &QueueKey) -> String {
    format!("{}branch.pending", shard_prefix(branch))
}

/// The outcome of a SINGLE branch-creation attempt ([`SegmentedObjectLog::branch_attempt`]). This is a
/// crate-PRIVATE signal that NEVER escapes: `FloorAdvanced` is the ONLY retryable outcome (a concurrent
/// source-floor advance detected by validate-after-copy, AFTER a full rollback), so the bounded retry in
/// [`SegmentedObjectLog::branch_with_emission`] retries on it and ONLY it. Every `EngineError` — a cut<=floor
/// `Invalid`, an `acquire_epoch` `Conflict`, a rollback-cleanup failure, any store error — is propagated
/// immediately and can NEVER be mistaken for a floor advance (which the public `EngineError::Conflict`
/// discriminator previously could be).
enum BranchAttempt {
    /// The branch committed; carries its acquired epoch.
    Committed(u64),
    /// The source floor advanced concurrently during the copy; the partial branch was fully rolled back
    /// (objects-first / pin-last), so a re-attempt against the advanced floor starts from a clean slate.
    FloorAdvanced,
}

// ---------------------------------------------------------------------------
// Internal fault-injection seam (TP-003 §3.10 AC-TXN-4)
// ---------------------------------------------------------------------------
//
// The only commit-pipeline seam the engine exposes to a driver is `Backend::write` (append/apply as one
// unit), which cannot strike the instants INSIDE this substrate's own group-commit pipeline: durable
// segment write, durable manifest CAS commit, durable epoch-fence commit (owner reassignment), and durable
// snapshot write are all internal to `SegmentedObjectLog::seal` / `acquire_epoch` / `write_snapshot`. This
// seam is a test-only hook (never driven in production — no caller outside a test sets one) that lets a
// test strike a "process died right here" fault at each of those named instants and observe the durable
// footprint the crash leaves behind, so recovery/replay correctness can be asserted for real instead of
// documented as an unreachable gap.

/// The object-log-internal commit-pipeline instants a test can strike (TP-003 §3.10 AC-TXN-4). Each
/// variant names a point strictly INSIDE the durable pipeline that the public `Backend::write` seam cannot
/// reach because it treats a whole `append` (or `acquire_epoch`/`write_snapshot`) as one opaque call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultCutPoint {
    /// Kill before the sealed segment object is durably written. Nothing durable exists yet — equivalent
    /// in spirit to the public-seam `BeforeAppend`, but internal to this substrate's own pipeline.
    BeforeSegmentWrite,
    /// Kill after the segment object is durably written but before the manifest CAS is attempted. The
    /// segment is now an ORPHAN: durable on the store but named by no committed manifest entry, so replay
    /// (which only trusts the manifest) must never surface it.
    AfterSegmentWriteBeforeManifest,
    /// Kill after the manifest CAS durably commits (the TD-004 ack boundary — the manifest entry names the
    /// segment and is now the durable source of truth) but before the caller receives the acked positions.
    /// This is strictly before the composed backend's projection apply, since `ComposedBackend` only
    /// applies a batch after its `LogStore::append` call returns `Ok`; recovery must replay the
    /// manifest-committed segment exactly once even though the ack (and therefore the apply) was lost.
    AfterManifestBeforeAck,
    /// Kill after an epoch-fence entry durably commits to the manifest (owner reassignment /
    /// `acquire_epoch`) but before the acquirer's local bookkeeping observes the new epoch. A stale-epoch
    /// writer's next commit must still be rejected from the durable manifest tail, not from in-memory state.
    DuringOwnerReassignment,
    /// Kill before a projection snapshot blob is durably written. The command log remains the sole source
    /// of truth, so a lost snapshot write must not lose or corrupt any committed command.
    DuringSnapshotWrite,
    /// Kill DURING segment-object reclamation (bead pqueue-b5cc2bc7): struck before each below-floor segment
    /// object delete inside [`SegmentedObjectLog::expire_segments_through`], AFTER the durable retention floor
    /// was already advanced (the crash-safe order writes the floor first). A crash here leaves floor=F with
    /// some below-F segment objects still present; recovery reads from F+1 and skips them (no "missing
    /// segment" error), and re-running the trim with an advanced horizon reaches the same consistent state.
    DuringSegmentExpiry,
    /// Struck DURING branch creation (bead pqueue-b5cc2bc7 HOLE B), AFTER the branch has published its source
    /// pin and read the source floor but BEFORE it copies the retained manifest entries. A test uses this to
    /// interleave a concurrent peer trim (advance the source floor + reclaim), exercising the pin-first +
    /// validate-after-copy cross-owner guard.
    DuringBranchCopy,
    /// Struck INSIDE [`SegmentedObjectLog::gc_orphaned_branches`] AFTER a branch has been classified as an
    /// orphan (its `branch.json` commit marker was observed ABSENT) but BEFORE its objects are deleted — the
    /// exact instant a concurrent branch creation could commit the marker. A test uses this to deterministically
    /// prove the create/GC guard excludes a concurrent creation (without the guard, GC struck here would go on
    /// to delete a branch that committed during the block).
    GcAfterOrphanClassified,
}

/// A test-only fault hook: called at each [`FaultCutPoint`] the pipeline passes through. Returning `Err`
/// simulates a process death at that instant (the in-flight operation aborts there); returning `Ok(())`
/// (the default no-op behavior of not installing a hook at all) lets the pipeline run normally.
pub trait FaultHook: Send + Sync {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()>;
}

// ---------------------------------------------------------------------------
// The segmented object log
// ---------------------------------------------------------------------------

struct ShardBuf {
    /// Per-command `postcard` record bytes in arrival order (serialized ONCE here at buffer time). On seal
    /// these are concatenated into the segment frame with no re-serialize (Fix A).
    /// Per-command `(serialized record bytes, created_at ms)` in arrival order (bead pqueue-b5cc2bc7 bug 1).
    /// Carrying each envelope's OWN `created_at` here — rather than a resettable running max — makes the seal's
    /// `committed_at_ms` computation race-free: the seal takes the max over the ACTUALLY-DRAINED batch it holds
    /// in hand, so a command still buffered when another batch seals keeps its own `created_at` and its later
    /// seal computes ITS batch's max (no shared counter to clobber across the drain-then-release-mutex window).
    buffered: Vec<(Vec<u8>, i64)>,
    buffered_bytes: usize,
    /// `now` (ms) of the oldest buffered command, for the latency seal trigger.
    oldest_buffered_ms: Option<i64>,
    /// Next sequence to assign (recovered from the manifest on open).
    next_seq: u64,
    /// Next manifest object index to write (recovered from the manifest on open).
    next_manifest_index: u64,
    /// Highest epoch any committed manifest entry records (the queue's current epoch).
    committed_epoch: u64,
}

struct Inner {
    shards: BTreeMap<QueueKey, ShardBuf>,
    counters: SegmentCounters,
    object_sizes: BTreeMap<String, u64>,
}

/// The outcome of an [`SegmentedObjectLog::enqueue`]: any positions that were acked because a size-triggered
/// seal fired during this call, plus how many commands remain buffered (un-acked) for the queue.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOutcome {
    /// Positions acked by a seal that fired during this enqueue (empty if the command was only buffered).
    pub committed: Vec<CommandPosition>,
    /// Commands still buffered (not yet sealed → not yet acked) for this queue.
    pub pending: usize,
}

/// Segmented, group-committing object log over an S3-compatible [`BlobStore`].
pub struct SegmentedObjectLog<S: BlobStore> {
    store: S,
    config: SegmentConfig,
    inner: Mutex<Inner>,
    /// Test-only fault-injection hook (TP-003 §3.10 AC-TXN-4). `None` in every production path.
    fault_hook: Mutex<Option<Arc<dyn FaultHook>>>,
    /// CREATE-vs-GC mutual exclusion (bead pqueue-74f03d0e). Branch creation ([`Self::branch_with_emission`])
    /// holds this for its WHOLE duration — every attempt, the commit-marker write, and any rollback — and
    /// orphan GC ([`Self::gc_orphaned_branches`]) holds it across its WHOLE classify+delete critical section.
    /// So on one log instance (one owner) GC can NEVER observe a branch whose creation is concurrently in
    /// flight: a marker-absent branch seen under this guard is DEFINITIVELY a failed/abandoned creation. This is
    /// a real exclusion (not a timing heuristic) that closes the classify-then-delete TOCTOU vs a marker write.
    /// It is ALWAYS the OUTERMOST lock (taken before `inner`), so it introduces no lock-order inversion.
    create_gc_guard: Mutex<()>,
}

impl<S: BlobStore> SegmentedObjectLog<S> {
    /// Open a segmented object log over `store` with `config`.
    pub fn open(store: S, config: SegmentConfig) -> Self {
        Self {
            store,
            config,
            inner: Mutex::new(Inner {
                shards: BTreeMap::new(),
                counters: SegmentCounters::default(),
                object_sizes: BTreeMap::new(),
            }),
            fault_hook: Mutex::new(None),
            create_gc_guard: Mutex::new(()),
        }
    }

    /// Install (or clear, with `None`) a test-only fault hook (TP-003 §3.10 AC-TXN-4). Never called from
    /// production code paths.
    pub fn set_fault_hook(&self, hook: Option<Arc<dyn FaultHook>>) {
        *self.fault_hook.lock().expect("fault hook poisoned") = hook;
    }

    /// Invoke the installed fault hook (if any) at `cut`. `Ok(())` when no hook is installed.
    fn fault(&self, cut: FaultCutPoint) -> EngineResult<()> {
        let hook = self.fault_hook.lock().expect("fault hook poisoned").clone();
        match hook {
            Some(h) => h.fault_point(cut),
            None => Ok(()),
        }
    }

    fn register_object(g: &mut Inner, key: &str, len: u64) {
        let old = g.object_sizes.insert(key.to_string(), len);
        match old {
            Some(old_len) => {
                g.counters.total_bytes = g.counters.total_bytes.saturating_sub(old_len) + len;
            }
            None => {
                g.counters.object_count += 1;
                g.counters.total_bytes += len;
            }
        }
        g.counters.max_object_bytes = g.counters.max_object_bytes.max(len);
    }

    fn store_put(&self, key: &str, body: &[u8], count_object_put: bool) -> EngineResult<()> {
        self.store.put(key, body)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.counters.put_count += 1;
        if count_object_put {
            g.counters.objects_put += 1;
        }
        Self::register_object(&mut g, key, body.len() as u64);
        Ok(())
    }

    fn store_put_segment(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.store.put(key, body)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.counters.put_count += 1;
        g.counters.objects_put += 1;
        g.counters.segment_bytes += body.len() as u64;
        Self::register_object(&mut g, key, body.len() as u64);
        Ok(())
    }

    fn store_put_if_absent(
        &self,
        key: &str,
        body: &[u8],
        count_object_put: bool,
    ) -> EngineResult<bool> {
        let created = self.store.put_if_absent(key, body)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.counters.put_count += 1;
        if created {
            if count_object_put {
                g.counters.objects_put += 1;
            }
            Self::register_object(&mut g, key, body.len() as u64);
        }
        Ok(created)
    }

    fn manifest_prefix(shard: &QueueKey) -> String {
        format!("{}manifest/", shard_prefix(shard))
    }

    fn manifest_head_prefix(shard: &QueueKey) -> String {
        format!("{}manifest_head/", shard_prefix(shard))
    }

    fn manifest_key(shard: &QueueKey, index: u64) -> String {
        format!("{}{index:020}.json", Self::manifest_prefix(shard))
    }

    fn manifest_head_key(shard: &QueueKey, index: u64) -> String {
        format!("{}{index:020}.json", Self::manifest_head_prefix(shard))
    }

    fn list_commit_keys_at(&self, prefix: &str, horizon: Option<u64>) -> EngineResult<Vec<String>> {
        match horizon {
            Some(w) => self.store_list_from(prefix, &format!("{prefix}{w:020}.json")),
            None => self.store_list(prefix),
        }
    }

    fn list_authoritative_manifest_keys_at(
        &self,
        shard: &QueueKey,
        horizon: Option<u64>,
    ) -> EngineResult<Vec<String>> {
        let head_prefix = Self::manifest_head_prefix(shard);
        let head_keys = self.list_commit_keys_at(&head_prefix, horizon)?;
        if head_keys.is_empty() {
            self.list_commit_keys_at(&Self::manifest_prefix(shard), horizon)
        } else {
            Ok(head_keys)
        }
    }

    fn commit_manifest_entry(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
        count_object_put: bool,
    ) -> EngineResult<bool> {
        let body = to_json(entry)?;
        let head_key = Self::manifest_head_key(shard, entry.index);
        let won = self.store_put_if_absent(&head_key, &body, count_object_put)?;
        if won {
            let legacy_key = Self::manifest_key(shard, entry.index);
            let _ = self.store_put_if_absent(&legacy_key, &body, false)?;
        }
        Ok(won)
    }

    fn delete_manifest_entry(&self, shard: &QueueKey, index: u64) -> EngineResult<()> {
        // Delete the legacy key first so a partial failure leaves the authoritative head entry visible.
        let legacy_key = Self::manifest_key(shard, index);
        let _ = self.store_delete(&legacy_key)?;
        let head_key = Self::manifest_head_key(shard, index);
        let _ = self.store_delete(&head_key)?;
        Ok(())
    }

    fn store_get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        let out = self.store.get(key)?;
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .counters
            .get_count += 1;
        Ok(out)
    }

    fn store_delete(&self, key: &str) -> EngineResult<bool> {
        let deleted = self.store.delete(key)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.counters.delete_count += 1;
        if deleted {
            g.counters.object_count = g.counters.object_count.saturating_sub(1);
            if let Some(len) = g.object_sizes.remove(key) {
                g.counters.total_bytes = g.counters.total_bytes.saturating_sub(len);
                if g.object_sizes.is_empty() {
                    g.counters.max_object_bytes = 0;
                } else if len == g.counters.max_object_bytes {
                    g.counters.max_object_bytes =
                        g.object_sizes.values().copied().max().unwrap_or(0);
                }
            }
        }
        Ok(deleted)
    }

    fn store_list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        let (out, request_count) = self.store.list_with_request_count(prefix)?;
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .counters
            .list_count += request_count.max(1);
        Ok(out)
    }

    /// Range-LIST wrapper mirroring [`Self::store_list`] (bead pqueue-8928baec): enumerate only the keys
    /// strictly after `start_after`, and bill EVERY LIST-class request the store paged through (an S3 ranged
    /// list of >1000 live keys spans several `ListObjectsV2` pages — each is billable, so the cost ledger must
    /// count them, not a flat 1).
    fn store_list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        let (out, request_count) = self
            .store
            .list_from_with_request_count(prefix, start_after)?;
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .counters
            .list_count += request_count.max(1);
        Ok(out)
    }

    /// Register a queue and recover its committed position + epoch from the manifest (idempotent).
    pub fn create_queue(&self, def: &QueueDefinition) -> EngineResult<()> {
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let (next_seq, next_index, epoch) = self.recover_manifest(&shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.shards.entry(shard).or_insert(ShardBuf {
            buffered: Vec::new(),
            buffered_bytes: 0,
            oldest_buffered_ms: None,
            next_seq,
            next_manifest_index: next_index,
            committed_epoch: epoch,
        });
        Ok(())
    }

    fn visible_last_seq(entry: &ManifestEntry) -> u64 {
        entry.visible_last_seq.unwrap_or(entry.last_seq)
    }

    /// Read the manifest from the store and derive `(next_seq, next_manifest_index, current_epoch)`.
    ///
    /// HOT PATH: every seal (and epoch fence) calls this to read the authoritative tail. Manifest objects
    /// are an append-only, contiguous series keyed `manifest/{index:020}.json`, so the zero-padded name sorts
    /// lexicographically by index — the LAST key is the tail. Deriving the tuple from only that one entry is
    /// exact (indices are contiguous so `tail.index + 1` is the next index; epoch is monotonically
    /// non-decreasing so the tail carries the max; `next_seq` is the tail's `last_seq + 1`, or for a fence
    /// entry — which names no segment — its `first_seq`, which already records the live next seq). This makes
    /// a seal O(1) manifest reads instead of re-reading + re-parsing the whole O(n) manifest each time
    /// (the previous full scan made a sustained push O(n^2)). `read_all` still does a full scan for recovery.
    fn recover_manifest(&self, shard: &QueueKey) -> EngineResult<(u64, u64, u64)> {
        // Range-list from the durable read-horizon so recovery enumerates only LIVE (above-floor) entries
        // (bead pqueue-8928baec). The tail is ALWAYS > floor > W, so it is never below the horizon; deriving
        // the tuple from the MAX ranged key is exact. The +1 GET for the horizon here is off the O(1) seal hot
        // path (recover_manifest is reached only on open/acquire/CAS-lost/advance-floor/ensure_shard).
        let keys = match self.read_read_horizon(shard)? {
            Some(w) => {
                let ranged = self.list_authoritative_manifest_keys_at(shard, Some(w))?;
                // Defensive: a horizon can never legitimately reach/exceed the tail (it is derived strictly
                // below the floor, which is strictly below the tail), but if a ranged list ever came back
                // empty for a non-empty manifest, fall back to the full list rather than reset a live tail to
                // genesis. A genuinely empty manifest (fresh queue) has no horizon object, so this branch is
                // not even reached for it — no double-list for the common fresh-open case.
                if ranged.is_empty() {
                    self.list_authoritative_manifest_keys_at(shard, None)?
                } else {
                    ranged
                }
            }
            None => self.list_authoritative_manifest_keys_at(shard, None)?,
        };
        let Some(tail_key) = keys.into_iter().max() else {
            return Ok((0, 0, 0));
        };
        let Some(bytes) = self.store_get(&tail_key)? else {
            return Ok((0, 0, 0));
        };
        let tail: ManifestEntry = serde_json::from_slice(&bytes).map_err(store_err)?;
        let next_index = tail.index + 1;
        // A fence entry AND a retention-floor-advance entry both name no segment and carry the LIVE next-seq in
        // `first_seq` (they don't add commands), so the tail's next-seq is `first_seq`; a data entry's is
        // `visible_last_seq + 1`.
        let next_seq = if tail.fence || tail.retention_floor_through.is_some() {
            tail.first_seq
        } else {
            Self::visible_last_seq(&tail) + 1
        };
        Ok((next_seq, next_index, tail.epoch))
    }

    /// The LIVE manifest keys for `shard` above a GIVEN `horizon` — indices strictly ABOVE the durable
    /// read-horizon watermark, so every read/recovery/fold enumerates O(live) entries instead of O(total
    /// lifetime seals) (bead pqueue-8928baec). Manifest keys are fixed-width `manifest/{index:020}.json`
    /// (lexicographic order == numeric index order), so `start_after = "{prefix}manifest/{W:020}.json"`
    /// returns exactly indices `> W`. BACKWARD-COMPATIBLE: `horizon == None` (a queue with NO
    /// `read_horizon.json` object — never trimmed, or written before this watermark existed) lists the whole
    /// manifest prefix exactly as before. Callers that also fail-closed on the floor MUST capture `horizon`
    /// ONCE (before reading the floor) and pass the SAME snapshot here, so a concurrent trim cannot advance
    /// the watermark between the guard and this enumeration (see [`Self::fail_closed_below_floor`]).
    fn list_manifest_keys_at(
        &self,
        shard: &QueueKey,
        horizon: Option<u64>,
    ) -> EngineResult<Vec<String>> {
        self.list_authoritative_manifest_keys_at(shard, horizon)
    }

    /// All LIVE manifest entries for `shard` above a GIVEN `horizon` snapshot, sorted by index. Consumers that
    /// pair this with the fail-closed floor guard pass a horizon they captured BEFORE reading the floor so the
    /// guard decision and the enumeration are consistent under a concurrent trim.
    fn read_manifest_at(
        &self,
        shard: &QueueKey,
        horizon: Option<u64>,
    ) -> EngineResult<Vec<ManifestEntry>> {
        let mut entries = Vec::new();
        for key in self.list_manifest_keys_at(shard, horizon)? {
            if let Some(bytes) = self.store_get(&key)? {
                let entry: ManifestEntry = serde_json::from_slice(&bytes).map_err(store_err)?;
                entries.push(entry);
            }
        }
        entries.sort_by_key(|e| e.index);
        Ok(entries)
    }

    /// All LIVE manifest entries for `shard`, sorted by index (range-listed from the CURRENT read-horizon).
    /// Every consumer (read_all, read_from_limited, read_retention_floor, expire_segments_through,
    /// lowest_branch_pinned_below, max_trimmable_seq_before, branch copy) folds ONLY live/needed entries:
    /// below-horizon entries are strictly below the epoch-fenced retention floor (reclaimed data tombstones,
    /// superseded floor-advance entries, old fences) that none of them needs (reads resume at floor+1; the
    /// authoritative floor entry and every live/pinned segment are above W). Readers that ALSO fail-closed on
    /// the floor use [`Self::read_manifest_at`] with a pre-captured horizon instead.
    fn read_manifest(&self, shard: &QueueKey) -> EngineResult<Vec<ManifestEntry>> {
        let horizon = self.read_read_horizon(shard)?;
        self.read_manifest_at(shard, horizon)
    }

    /// The queue's current `assignment_epoch` (highest epoch any committed manifest entry records).
    pub fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .get(shard)
            .map(|buf| buf.committed_epoch)
            .ok_or(EngineError::NotFound)
    }

    /// Acquire the queue at a NEW, strictly-greater epoch by publishing a **fence entry** to the manifest
    /// via the create-only CAS (TD-003 durable-fence-before-use; TD-004 implementation (b)). After it
    /// commits, a prior-epoch writer's next seal observes the higher epoch and self-fences.
    ///
    /// This does not rely on the deferred pqueue-c33c367e owner-fence wiring to bound stale writers inside
    /// retention; the durable head CAS is the fence, and the code only uses that wiring if a later proof
    /// establishes the bounded-window invariant there.
    pub fn acquire_epoch(&self, shard: &QueueKey, now_ms: i64) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        // Bounded retry against concurrent acquirers (no consensus; the store CAS is the only primitive).
        for _ in 0..16 {
            let (next_seq, next_index, cur_epoch) = self.recover_manifest(shard)?;
            let new_epoch = cur_epoch + 1;
            let entry = ManifestEntry {
                index: next_index,
                epoch: new_epoch,
                fence: true,
                segment_key: None,
                first_seq: next_seq,
                last_seq: next_seq.saturating_sub(1),
                visible_last_seq: None,
                checksum: 0,
                committed_at_ms: now_ms,
                retention_floor_through: None,
            };
            if self.commit_manifest_entry(shard, &entry, true)? {
                // The fence entry just won its CAS: the epoch handoff is now durably committed to the
                // manifest, even though this acquirer's own in-memory bookkeeping has not yet observed it.
                self.fault(FaultCutPoint::DuringOwnerReassignment)?;
                let mut g = self.inner.lock().expect("segmented log poisoned");
                if let Some(buf) = g.shards.get_mut(shard) {
                    buf.committed_epoch = new_epoch;
                    buf.next_manifest_index = next_index + 1;
                }
                return Ok(new_epoch);
            }
            // Lost the CAS race; re-read and retry at the new tail.
        }
        Err(EngineError::Conflict)
    }

    /// Buffer `commands` for `shard` (TD-004 step 1). If the buffered byte size reaches `target_bytes`, a
    /// segment seals synchronously and its positions are acked in [`EnqueueOutcome::committed`]; otherwise
    /// the commands stay buffered and are NOT acked (TD-004 step 5: no ack before manifest commit).
    pub fn enqueue(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<EnqueueOutcome> {
        // Class ban + gate validation BEFORE buffering (parity with the file reference write path).
        for env in commands {
            validate_gate_command(false, &env.command)?;
            if matches!(env.command, QueueCommand::ReplacePending(_)) {
                return Err(EngineError::Unavailable);
            }
        }
        let should_seal = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            for env in commands {
                // Serialize ONCE, here; keep the bytes. `buffered_bytes` is the size of the kept bytes (free)
                // rather than a throwaway serialize-just-to-measure (Fix A: kills the double serialization).
                let bytes = serde_json::to_vec(env).map_err(store_err)?;
                buf.buffered_bytes += bytes.len();
                // Keep each command's OWN created_at alongside its bytes (bug 1): the seal derives
                // committed_at_ms from the drained batch, so there is no shared running max to race.
                buf.buffered.push((bytes, created_at_ms(env)));
                buf.oldest_buffered_ms.get_or_insert(now_ms);
            }
            let one_command_seal = self.config.dev_unsafe_one_command_segments;
            buf.buffered_bytes >= self.config.target_bytes
                || (one_command_seal && !buf.buffered.is_empty())
        };
        let committed = if should_seal {
            self.seal(shard, expected_epoch, now_ms)?
        } else {
            Vec::new()
        };
        let pending = self.pending(shard);
        Ok(EnqueueOutcome { committed, pending })
    }

    /// Seal the buffered commands for `shard` if the oldest has aged past `max_latency_ms` (TD-004 step 2
    /// latency trigger). Returns acked positions (empty if nothing was due).
    pub fn flush_due(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let due = {
            let g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get(shard).ok_or(EngineError::NotFound)?;
            match buf.oldest_buffered_ms {
                Some(oldest) if !buf.buffered.is_empty() => {
                    now_ms.saturating_sub(oldest) >= self.config.max_latency_ms as i64
                }
                _ => false,
            }
        };
        if due {
            self.seal(shard, expected_epoch, now_ms)
        } else {
            Ok(Vec::new())
        }
    }

    /// Force-seal whatever is buffered for `shard` into one segment, commit its manifest entry, and ack.
    ///
    /// The manifest commit is the ack boundary AND the epoch fence: the writer's `expected_epoch` MUST equal
    /// the queue's current epoch (read authoritatively from the manifest). A stale epoch is rejected
    /// [`EngineError::EpochFenced`] BEFORE the segment object is written (no torn/orphan segment), and the
    /// buffer is discarded (the fenced writer surrenders; the new owner re-drives on retry).
    pub fn seal(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let prefix = shard_prefix(shard);
        // 1. Snapshot+drain the buffer under the lock; nothing buffered → nothing to do.
        let drained: Vec<(Vec<u8>, i64)> = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            if buf.buffered.is_empty() {
                return Ok(Vec::new());
            }
            std::mem::take(&mut buf.buffered)
        };
        let n = drained.len();
        // `committed_at_ms >= every sealed envelope's created_at` MUST hold for the retention-floor trim to be
        // AC-TXN-3-safe. `now_ms` alone is NOT sufficient (a size-seal is stamped with the TRIGGERING push's
        // now, which can be smaller than an earlier buffered push's created_at). Compute the max over THIS
        // drained batch's own `created_at` values (held in hand, so no reset race with a concurrent enqueue).
        // A larger committed_at_ms is always safe (it only delays age-trimming).
        let batch_max_created_ms = drained.iter().map(|(_, c)| *c).max().unwrap_or(0);
        let committed_at_ms = now_ms.max(batch_max_created_ms);
        // The segment frame is the concatenation of the per-command record bytes (no re-serialize).
        let drained_bytes: Vec<Vec<u8>> = drained.into_iter().map(|(b, _)| b).collect();

        // 2. Epoch fence from the recovered in-memory tail. Recovery/acquire/CAS-lost paths refresh this tail
        //    from the manifest; the normal single-writer hot path must not list the manifest before every seal.
        let (cur_seq, cur_index, cur_epoch) = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            if expected_epoch != buf.committed_epoch {
                // Fenced: discard the buffer (the commands are unacked; no segment, no manifest entry).
                buf.buffered_bytes = 0;
                buf.oldest_buffered_ms = None;
                return Err(EngineError::EpochFenced);
            }
            (buf.next_seq, buf.next_manifest_index, buf.committed_epoch)
        };

        // Reclaim-time fence: if compaction has already advanced the durable read-horizon beyond this
        // cached manifest index, the index was reclaimed and this stale writer must self-fence before any
        // segment PUT. This stays on the O(1) seal hot path: one GET of `read_horizon.json`, not a manifest
        // LIST or post-CAS rollback substitute (docs/perf/design/manifest-compaction-hotpath.md:359 and
        // pqueue-c33c367e).
        if let Some(horizon) = self.read_read_horizon(shard)?
            && cur_index <= horizon
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
            return Err(EngineError::EpochFenced);
        }

        self.fault(FaultCutPoint::BeforeSegmentWrite)?;

        // 3. Write the immutable, checksummed segment object (idempotent at its first-seq key). The segment
        //    is the framed concatenation of the per-command bytes serialized once at buffer time — no
        //    re-serialize on seal (Fix A). The checksum covers the records-blob region.
        let first_seq = cur_seq;
        let last_seq = first_seq + n as u64 - 1;
        let (seg_bytes, seg_checksum) = build_segment_object(cur_epoch, first_seq, &drained_bytes);
        let attempt = SEGMENT_ATTEMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let seg_key = format!(
            "{prefix}seg_attempt/e{cur_epoch:020}/i{cur_index:020}/s{first_seq:020}-{pid}-{attempt}.seg"
        );
        self.store_put_segment(&seg_key, &seg_bytes)?;

        self.fault(FaultCutPoint::AfterSegmentWriteBeforeManifest)?;

        // 4. Commit the manifest entry via the create-only CAS at the next index.
        let entry = ManifestEntry {
            index: cur_index,
            epoch: cur_epoch,
            fence: false,
            segment_key: Some(seg_key),
            first_seq,
            last_seq,
            visible_last_seq: None,
            checksum: seg_checksum,
            committed_at_ms,
            retention_floor_through: None,
        };
        let won = self.commit_manifest_entry(shard, &entry, true)?;
        if !won {
            // CAS lost: a peer extended the manifest from the same tail. Re-read to learn the new epoch.
            let observed_epoch = self.recover_manifest(shard)?.2;
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
            if observed_epoch > expected_epoch {
                return Err(EngineError::EpochFenced);
            }
            // Same-epoch transient race: the segment object is an orphan (no manifest entry); the caller
            // retries. Surface as a conflict so it is not mistaken for an ack.
            return Err(EngineError::Conflict);
        }

        // The manifest CAS just won: the segment is now named by a durably committed manifest entry (the
        // TD-004 ack boundary). A fault struck here models a crash after that durable commit but before the
        // ack (and therefore before any projection apply, which only ever runs after this call returns
        // `Ok`) reaches the caller.
        self.fault(FaultCutPoint::AfterManifestBeforeAck)?;

        // 5. Ack: the manifest entry is durable. Advance state + counters, then return positions.
        let mut positions = Vec::with_capacity(n);
        for i in 0..n {
            positions.push(CommandPosition::new(
                shard.clone(),
                cur_epoch,
                first_seq + i as u64,
            ));
        }
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            g.counters.segments_sealed += 1;
            g.counters.commands_committed += n as u64;
            g.counters.group_commit_batches.push(n);
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.next_seq = last_seq + 1;
            buf.next_manifest_index = cur_index + 1;
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
        }
        Ok(positions)
    }

    /// Commands buffered (un-acked) for `shard`.
    pub fn pending(&self, shard: &QueueKey) -> usize {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .get(shard)
            .map(|b| b.buffered.len())
            .unwrap_or(0)
    }

    /// Replay every **manifest-committed** command for `shard` in sequence order (recovery / read path).
    /// Only segments named by a committed manifest entry are visible — a buffered-but-unsealed command or a
    /// fenced orphan segment is NOT returned, which is what makes "ack only after manifest commit"
    /// observable. Per-segment checksums are validated.
    /// Whether `shard` is an UNCOMMITTED branch (bead pqueue-b5cc2bc7 — atomic branch existence). Branch
    /// creation writes a `branch.pending` sentinel FIRST and its `branch.json` commit marker LAST (after the
    /// pin, floor seed, ALL manifest copies, validate-after-copy, and acquire_epoch). A shard with the sentinel
    /// but WITHOUT the commit marker is a partial/failed branch and MUST be treated as non-existent by every
    /// segment-reading path, so it can never GET a reclaimed source object ("missing segment") — regardless of
    /// the pin/TTL/cleanup outcome. A source queue (no sentinel — fast path, one GET) and a committed branch
    /// (marker present) read normally.
    fn branch_uncommitted(&self, shard: &QueueKey) -> EngineResult<bool> {
        if self.store_get(&branch_pending_key(shard))?.is_none() {
            return Ok(false); // source queue OR committed branch (sentinel dropped at commit)
        }
        Ok(self.store_get(&branch_metadata_key(shard))?.is_none())
    }

    /// Fail-closed floor guard (bead pqueue-8928baec step 5). Once reads range-list past the durable
    /// read-horizon, the below-floor tombstones are NO LONGER ENUMERATED, so a read whose `from_seq` dips
    /// to/below the reclaimed floor would silently return a TRUNCATED prefix instead of the pre-horizon
    /// "missing segment" Storage error. Reproduce that fail-closed with the SAME `EngineError::Storage`
    /// class. Boundary: the floor is an EXCLUSIVE lower bound (last-reclaimed seq), so `from_seq == floor+1`
    /// still SUCCEEDS and `from_seq <= floor` FAILS CLOSED.
    ///
    /// GATED on a horizon EXISTING so a branch's legitimate `seq == f` seed (design §5(ii); branch creation
    /// seeds a floor entry but writes NO horizon) is never suppressed: with no horizon the full manifest list
    /// still enumerates every entry, so the natural behavior stands — a genuinely-reclaimed range still
    /// errors organically on the missing-segment GET, while a branch reading its own present seed reads it.
    /// This is behavior-preserving: a below-floor read errors today (organic missing-segment) exactly when
    /// `from_seq <= floor`, and every production recovery/idempotency fold resumes at `floor + 1`.
    ///
    /// CONCURRENCY: `horizon` is the caller's snapshot captured BEFORE this call, and the SAME snapshot drives
    /// the subsequent range-list ([`Self::read_manifest_at`]). Reading the horizon before the floor guarantees
    /// the horizon corresponds to a floor `<= floor_now`, so every below-horizon (hidden) entry is `<= floor`
    /// here — a concurrent trim that advances the watermark after the snapshot can therefore never hide a
    /// tombstone this guard would have let slip (it would have raised the floor this guard reads too).
    fn fail_closed_below_floor(
        &self,
        shard: &QueueKey,
        from_seq: u64,
        horizon: Option<u64>,
    ) -> EngineResult<()> {
        if horizon.is_some()
            && let Some(floor) = self.read_retention_floor(shard)?
            && from_seq <= floor.sequence
        {
            return Err(EngineError::Storage(format!(
                "read below retention floor: from_seq {from_seq} <= reclaimed floor {} \
                 (segments reclaimed; recovery resumes at floor+1)",
                floor.sequence
            )));
        }
        Ok(())
    }

    pub fn read_all(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        // A partial/uncommitted branch is NON-EXISTENT: return empty rather than GET a (possibly reclaimed)
        // shared source segment.
        if self.branch_uncommitted(shard)? {
            return Ok(Vec::new());
        }
        // Capture the horizon ONCE (before the floor) and reuse it for BOTH the fail-closed guard and the
        // range-list, so a concurrent trim cannot advance the watermark between the two (bead pqueue-8928baec).
        let horizon = self.read_read_horizon(shard)?;
        // Genesis read: from_seq == 0 dips to/below any floor, so this fails closed on a trimmed queue with a
        // read-horizon (equivalent to today's organic missing-segment error over the reclaimed prefix).
        self.fail_closed_below_floor(shard, 0, horizon)?;
        let mut out = Vec::new();
        for entry in self.read_manifest_at(shard, horizon)? {
            if entry.fence {
                continue;
            }
            let Some(seg_key) = entry.segment_key.as_ref() else {
                continue;
            };
            let visible_last_seq = Self::visible_last_seq(&entry);
            let bytes = self
                .store_get(seg_key)?
                .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
            let (epoch, first_seq, commands) =
                parse_segment_object(&bytes, seg_key, entry.checksum)?;
            for (i, env) in commands.into_iter().enumerate() {
                let seq = first_seq + i as u64;
                if seq > visible_last_seq {
                    continue;
                }
                let pos = CommandPosition::new(shard.clone(), epoch, seq);
                out.push((pos, env));
            }
        }
        Ok(out)
    }

    /// Bounded-tail recovery read (bead pqueue-8a76daad): replay only the **manifest-committed** commands at
    /// sequence `>= from_seq`, the snapshot-tail counterpart to [`read_all`]. A segment whose `last_seq <
    /// from_seq` lies entirely in the snapshot the projection has already materialized, so its object is
    /// NEVER fetched or decoded — only the manifest (already an O(entries) list) is scanned and the tail
    /// segments are GET + checksum-verified + decoded. A segment straddling the boundary is fetched once and
    /// its already-applied prefix records are filtered out, so the returned positions are contiguous from
    /// `from_seq`. With `from_seq == 0` this is exactly [`read_all`] (full-genesis replay fallback).
    pub fn read_from(
        &self,
        shard: &QueueKey,
        from_seq: u64,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        self.read_from_limited(shard, from_seq, usize::MAX)
    }

    /// Like [`Self::read_from`], but stops fetching/parsing segment objects as soon as `limit` commands have
    /// been returned. The `LogStore` adapter uses this for recovery paging; without it, each page would
    /// deserialize the entire remaining tail and then truncate in memory.
    pub fn read_from_limited(
        &self,
        shard: &QueueKey,
        from_seq: u64,
        limit: usize,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // A partial/uncommitted branch is NON-EXISTENT (see `branch_uncommitted`): never GET its shared source
        // segments.
        if self.branch_uncommitted(shard)? {
            return Ok(Vec::new());
        }
        // Capture the horizon ONCE (before the floor) and reuse it for BOTH the fail-closed guard and the
        // range-list, so a concurrent trim cannot advance the watermark between the two (bead pqueue-8928baec).
        let horizon = self.read_read_horizon(shard)?;
        // Fail closed if the requested range dips to/below the reclaimed floor on a range-listed (horizon)
        // queue — the below-floor tombstones are no longer enumerated, so return the same missing-segment
        // Storage error today's full-list read produces rather than a silently-truncated prefix.
        self.fail_closed_below_floor(shard, from_seq, horizon)?;
        let mut out = Vec::new();
        for entry in self.read_manifest_at(shard, horizon)? {
            if entry.fence {
                continue;
            }
            // The bounded-tail saving: a fully-applied segment is skipped without a GET/parse of its object.
            let visible_last_seq = Self::visible_last_seq(&entry);
            if visible_last_seq < from_seq {
                continue;
            }
            let Some(seg_key) = entry.segment_key.as_ref() else {
                continue;
            };
            let bytes = self
                .store_get(seg_key)?
                .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
            let (epoch, first_seq, commands) =
                parse_segment_object(&bytes, seg_key, entry.checksum)?;
            for (i, env) in commands.into_iter().enumerate() {
                let seq = first_seq + i as u64;
                if seq < from_seq || seq > visible_last_seq {
                    continue;
                }
                out.push((CommandPosition::new(shard.clone(), epoch, seq), env));
                if out.len() == limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    /// Read the committed command prefix at or before `position`.
    pub fn read_as_of(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        let mut out = Vec::new();
        for (pos, env) in self.read_all(shard)? {
            if pos.sequence <= position.sequence {
                out.push((pos, env));
            }
        }
        Ok(out)
    }

    fn read_branch_registry(&self, source: &QueueKey) -> EngineResult<Vec<BranchMetadata>> {
        let prefix = format!("{}branches/", shard_prefix(source));
        let mut out = Vec::new();
        for key in self.store_list(&prefix)? {
            if let Some(bytes) = self.store_get(&key)? {
                out.push(serde_json::from_slice(&bytes).map_err(store_err)?);
            }
        }
        Ok(out)
    }

    fn live_branch_registry(
        &self,
        source: &QueueKey,
        now_ms: i64,
    ) -> EngineResult<Vec<BranchMetadata>> {
        Ok(self
            .read_branch_registry(source)?
            .into_iter()
            .filter(|meta| now_ms < meta.expires_at_ms)
            .collect())
    }

    fn branch_pins_segment(
        &self,
        source: &QueueKey,
        first_seq: u64,
        now_ms: i64,
    ) -> EngineResult<bool> {
        Ok(self
            .live_branch_registry(source, now_ms)?
            .into_iter()
            .any(|meta| first_seq <= meta.cut_sequence))
    }

    fn delete_prefix(&self, prefix: &str) -> EngineResult<u64> {
        let mut deleted = 0u64;
        for key in self.store_list(prefix)? {
            if self.store_delete(&key)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Create a copy-on-write branch rooted at `source` and cut at `position`.
    pub fn branch(
        &self,
        source: &QueueKey,
        branch_def: &QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        self.branch_with_emission(source, branch_def, position, ttl_ms, now_ms, false)
    }

    /// Same as [`Self::branch`], but allows opting in to change-record emission for the branch metadata.
    ///
    /// BOUNDED TRANSPARENT RETRY (bead pqueue-9dcec223): a single attempt ([`Self::branch_attempt`]) reports
    /// the crate-private [`BranchAttempt::FloorAdvanced`] signal when a peer CONCURRENTLY advances the source
    /// retention floor DURING creation (the validate-after-copy guard), having FIRST fully rolled back its own
    /// partial state (branch objects FIRST, source pin LAST). Re-attempting is therefore SAFE: the next
    /// attempt re-reads the ADVANCED floor and either (a) succeeds against the now-retained range, or (b) is
    /// cleanly REJECTED with `Invalid` if the cut is now at/below the advanced floor (a genuine "whole view
    /// reclaimed"). The retry fires ONLY on the private `FloorAdvanced` signal — EVERY `EngineError` (the
    /// cut<=floor `Invalid`, an `acquire_epoch` `Conflict`, a rollback-cleanup failure, any store error) is
    /// propagated immediately and is NEVER mistaken for a floor advance. Bounded to `MAX_BRANCH_ATTEMPTS` so
    /// CONTINUOUS trimming cannot livelock: after the cap a still-`FloorAdvanced` outcome is mapped to a clean
    /// terminal `EngineError::Conflict` for the caller (its rollback already released the pin).
    pub fn branch_with_emission(
        &self,
        source: &QueueKey,
        branch_def: &QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
        emit_change_records: bool,
    ) -> EngineResult<u64> {
        // CREATE-vs-GC exclusion (bead pqueue-74f03d0e): hold the create/GC guard for the ENTIRE creation —
        // every attempt, the commit-marker write, and any rollback — so orphan GC can never run concurrently
        // with (and thus never mis-classify or destroy) an in-flight creation on this log instance. Outermost
        // lock: taken before any `inner` acquisition, so no lock-order inversion.
        // POISON-TOLERANT: this mutex guards CREATE-vs-GC coordination, not an in-memory invariant, so a panic
        // that unwinds through a creation (or GC) while it holds the guard must NOT wedge all future GC (and
        // creation) forever. Recover the guard from a poisoned lock instead of propagating the panic.
        let _create_guard = self
            .create_gc_guard
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // A small fixed cap. Each attempt is a FULL clean single-shot creation that rolls back its own
        // partial state before the next runs, so no pin/objects leak across attempts. `?` propagates EVERY
        // EngineError (never retried); only the private `FloorAdvanced` signal loops.
        const MAX_BRANCH_ATTEMPTS: u32 = 5;
        for _ in 1..MAX_BRANCH_ATTEMPTS {
            match self.branch_attempt(
                source,
                branch_def,
                position,
                ttl_ms,
                now_ms,
                emit_change_records,
            )? {
                BranchAttempt::Committed(epoch) => return Ok(epoch),
                // A genuine concurrent source-floor advance (already rolled back): re-read + re-attempt.
                BranchAttempt::FloorAdvanced => continue,
            }
        }
        // Final attempt: a still-`FloorAdvanced` outcome is the bounded give-up — map it to a clean terminal
        // public `Conflict` (no livelock; its rollback already released the pin so the source stays
        // reclaimable). A `Committed` succeeds; any `EngineError` propagates verbatim.
        match self.branch_attempt(
            source,
            branch_def,
            position,
            ttl_ms,
            now_ms,
            emit_change_records,
        )? {
            BranchAttempt::Committed(epoch) => Ok(epoch),
            BranchAttempt::FloorAdvanced => Err(EngineError::Conflict),
        }
    }

    /// A SINGLE branch-creation attempt (pin-first + validate-after-copy). Reports
    /// [`BranchAttempt::FloorAdvanced`] (NOT a bare `EngineError::Conflict`) if the source retention floor
    /// MOVED during the copy, having fully rolled back its partial state; the bounded retry loop in
    /// [`Self::branch_with_emission`] re-attempts against the advanced floor on that signal ALONE. Every real
    /// failure is returned as an `Err(EngineError)` and is never retried.
    fn branch_attempt(
        &self,
        source: &QueueKey,
        branch_def: &QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
        emit_change_records: bool,
    ) -> EngineResult<BranchAttempt> {
        let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
        if branch == *source {
            return Err(EngineError::Invalid("branch queue must differ from source"));
        }

        // CROSS-OWNER SAFETY (bead pqueue-b5cc2bc7 HOLE B): a peer owner may CONCURRENTLY advance the source
        // retention floor + reclaim segments while this branch is being created. Guard with (1) PIN-FIRST:
        // publish the branch's source pin BEFORE reading the floor / copying manifests, so a concurrent trim
        // whose `branch_pins_segment` check runs after the pin is published SKIPS the branched range; and
        // (2) VALIDATE-AFTER-COPY: re-read the AUTHORITATIVE (epoch-fenced manifest) floor after copying and, if
        // it MOVED, roll back and fail cleanly (`Conflict`) so a retry re-reads the advanced floor — NEVER
        // leaving a branch that GETs a reclaimed object.
        // SOURCE-OWNERSHIP FENCE (cross-instance superseded-owner safety): snapshot the durable source epoch
        // before copying, then re-read it after the copy and before the final commit marker write. If a newer
        // owner has taken the source in the meantime, the branch commit must fail cleanly (`Conflict`) and
        // roll back the partial branch rather than publishing a branch on a source it no longer owns.
        let (_, _, source_epoch) = self.recover_manifest(source)?;
        let metadata = BranchMetadata {
            source: source.clone(),
            branch: branch.clone(),
            source_epoch,
            cut_sequence: position.sequence,
            ttl_ms,
            created_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl_ms as i64),
            emit_change_records,
        };
        // (1) Publish the source PIN first (the registry entry `branch_pins_segment` consults). If THIS fails,
        // no pin was published, so there is nothing to roll back — just surface the error.
        self.store_put(
            &branch_registry_key(source, &branch),
            &to_json(&metadata)?,
            true,
        )?;

        // Roll back a partially-created branch (bead pqueue-b5cc2bc7 error-path safety) by the SAFETY-CRITICAL
        // ordering in [`Self::cleanup_uncommitted_branch`]: branch OBJECTS + in-memory shard FIRST, source PIN
        // LAST. If branch-object cleanup FAILS, the pin is RETAINED (a safe, TTL-bounded, retryable LEAK that
        // keeps the source segments protected) and the cleanup error is surfaced — NEVER an unpinned partial
        // branch that would let reclamation delete a still-referenced segment ("missing segment"). The exact
        // same routine is what `gc_orphaned_branches` reuses so the two stay consistent.
        let rollback =
            |this: &Self| -> EngineResult<()> { this.cleanup_uncommitted_branch(source, &branch) };

        // FAILURE FUNNEL: ALL post-pin work runs here. Any `?`-propagated failure returns from this closure and
        // is routed through `rollback` below, so there is NO early-return between "pin published" and "branch
        // fully committed" that leaves the pin (or an unpinned partial branch) behind.
        // `Ok(Some(epoch))` = committed; `Ok(None)` = the source floor moved during the copy (a retryable
        // FloorAdvanced, routed through rollback below); `Err` = a genuine failure (also rolled back).
        let committed: EngineResult<Option<u64>> = (|| {
            // ATOMIC BRANCH EXISTENCE (bead pqueue-b5cc2bc7): write the `branch.pending` sentinel FIRST so any
            // failure/crash before the `branch.json` commit marker (written LAST, below) leaves an UNCOMMITTED
            // branch that every segment-reading path treats as NON-EXISTENT — never a readable partial branch.
            self.store_put(&branch_pending_key(&branch), b"1", true)?;
            // The source retention floor: the below-floor segment OBJECTS are already reclaimed, so the branch
            // must INHERIT the floor — it can only view commands at/above `floor + 1`. Read it AFTER the pin is
            // published so the copy baseline is consistent with the pin.
            let source_floor = self.read_retention_floor(source)?.map(|p| p.sequence);
            // Reject a cut at or below the floor CLEANLY (its whole view is reclaimed).
            if let Some(f) = source_floor
                && position.sequence <= f
            {
                return Err(EngineError::Invalid(
                    "branch cut at or below the source retention floor: those segments were reclaimed",
                ));
            }

            // Test seam (never armed in production): interleave a concurrent peer trim / inject a store failure
            // here, between the floor read and the copy.
            self.fault(FaultCutPoint::DuringBranchCopy)?;

            self.create_queue(branch_def)?;

            let mut next_index = 0u64;
            // Seed the branch with the INHERITED floor as its FIRST manifest entry, so the branch's effective
            // genesis is `floor + 1`: `read_retention_floor(branch)` returns it and the branch's recovery /
            // read / idempotency folds resume above the trimmed prefix and never GET a reclaimed object.
            if let Some(f) = source_floor {
                let floor_entry = ManifestEntry {
                    index: next_index,
                    epoch: 0,
                    fence: false,
                    segment_key: None,
                    first_seq: f,
                    last_seq: f,
                    visible_last_seq: None,
                    checksum: 0,
                    committed_at_ms: 0,
                    retention_floor_through: Some(f),
                };
                self.commit_manifest_entry(&branch, &floor_entry, true)?;
                next_index += 1;
            }
            let entries = self.read_manifest(source)?;
            for entry in entries {
                // Do NOT copy the source's own retention-floor-advance entries verbatim.
                if entry.retention_floor_through.is_some() {
                    continue;
                }
                if entry.fence {
                    if entry.first_seq > position.sequence + 1 {
                        break;
                    }
                    let mut copied = entry.clone();
                    copied.index = next_index;
                    self.commit_manifest_entry(&branch, &copied, true)?;
                    next_index += 1;
                    continue;
                }

                // Skip a data segment entirely at/below the source floor — its object is RECLAIMED, so copying
                // the tombstone would make the branch's read GET a deleted object. A straddling segment
                // (visible_last_seq > floor) is retained and IS copied.
                if let Some(f) = source_floor
                    && Self::visible_last_seq(&entry) <= f
                {
                    continue;
                }

                if entry.first_seq > position.sequence {
                    break;
                }

                let mut copied = entry.clone();
                copied.index = next_index;
                if entry.last_seq > position.sequence {
                    copied.visible_last_seq = Some(position.sequence);
                }
                self.commit_manifest_entry(&branch, &copied, true)?;
                next_index += 1;
                if entry.last_seq >= position.sequence {
                    break;
                }
            }

            // (2) VALIDATE-AFTER-COPY: re-read the AUTHORITATIVE source floor. If it MOVED during the copy, a
            // peer concurrently reclaimed part of the branched range — signal the RETRYABLE floor-advance (a
            // private `Ok(None)`, routed through rollback below) so a retry re-reads the advanced floor. This is
            // NOT a bare `EngineError::Conflict`: it must be distinguishable from a Conflict that a store /
            // `acquire_epoch` / cleanup could raise, which must NEVER be retried.
            let floor_after = self.read_retention_floor(source)?.map(|p| p.sequence);
            if floor_after != source_floor {
                return Ok(None);
            }

            let (next_seq, next_manifest_index, committed_epoch) =
                self.recover_manifest(&branch)?;
            {
                let mut g = self.inner.lock().expect("segmented log poisoned");
                let buf = g.shards.get_mut(&branch).ok_or(EngineError::NotFound)?;
                buf.next_seq = next_seq;
                buf.next_manifest_index = next_manifest_index;
                buf.committed_epoch = committed_epoch;
            }

            // Own lease / epoch: the branch gets its own fence entry without mutating the parent queue.
            let epoch = self.acquire_epoch(&branch, now_ms)?;
            let (_, _, current_source_epoch) = self.recover_manifest(source)?;
            if current_source_epoch != source_epoch {
                return Err(EngineError::Conflict);
            }

            // COMMIT MARKER — the LAST durable write (bead pqueue-b5cc2bc7 atomic branch existence). Only now,
            // after the pin, floor seed, ALL manifest copies + objects, validate-after-copy, and acquire_epoch,
            // does `branch.json` land — the atomic boundary that makes the branch READABLE (mirrors the
            // manifest-CAS ack boundary for segments). A crash/failure at ANY point before this leaves an
            // unreadable (non-existent) branch.
            self.store_put(&branch_metadata_key(&branch), &to_json(&metadata)?, true)?;
            // Drop the "in progress" sentinel now that the commit marker is authoritative (best-effort — a
            // leftover sentinel is harmless because the commit marker wins the readability gate; a leftover is
            // just non-blocking garbage, same GC class as the deferred manifest-tombstone compaction).
            let _ = self.store_delete(&branch_pending_key(&branch));
            Ok(Some(epoch))
        })();

        match committed {
            // Committed cleanly — surface the epoch.
            Ok(Some(epoch)) => Ok(BranchAttempt::Committed(epoch)),
            // Concurrent floor advance during the copy: roll back the partial branch FIRST, then report the
            // private `FloorAdvanced` retry signal so the next attempt starts from a clean slate. If the
            // rollback CLEANUP itself fails, that `EngineError` is surfaced immediately (`?`) and is NEVER
            // retried over the deliberately-RETAINED pin/objects — the safe-leak invariant is preserved.
            Ok(None) => {
                rollback(self)?;
                Ok(BranchAttempt::FloorAdvanced)
            }
            // A genuine failure (cut<=floor `Invalid`, `acquire_epoch` `Conflict`, any store error): roll back
            // and surface the ORIGINAL error, never retried.
            Err(original) => match rollback(self) {
                // Clean rollback (branch objects + pin gone) — surface the original failure.
                Ok(()) => Err(original),
                // Cleanup itself failed: the branch objects could not all be removed, so the pin is RETAINED
                // (source stays protected). Surface the cleanup error so the leak is visible + retryable.
                Err(cleanup) => Err(cleanup),
            },
        }
    }

    /// Whether a live branch defaults to emitting change records.
    pub fn branch_emits_change_records(&self, branch: &QueueKey) -> EngineResult<bool> {
        let key = branch_metadata_key(branch);
        let Some(bytes) = self.store_get(&key)? else {
            return Err(EngineError::NotFound);
        };
        let meta: BranchMetadata = serde_json::from_slice(&bytes).map_err(store_err)?;
        Ok(meta.emit_change_records)
    }

    /// Discard a branch and release its pins.
    pub fn discard_branch(&self, source: &QueueKey, branch: &QueueKey) -> EngineResult<()> {
        let _ = self.store_delete(&branch_registry_key(source, branch))?;
        let _ = self.delete_prefix(&shard_prefix(branch))?;
        Ok(())
    }

    /// Delete ALL durable objects of an UNCOMMITTED/partial `branch` and drop its in-memory shard, then release
    /// the source PIN (its `branch_registry_key` entry) LAST. This is the single cleanup routine shared by the
    /// branch-creation rollback ([`Self::branch_attempt`]) and orphan GC ([`Self::gc_orphaned_branches`]) so the
    /// two stay consistent.
    ///
    /// ORDER IS SAFETY-CRITICAL. Delete every branch object EXCEPT the `branch.pending` sentinel FIRST: while ANY
    /// manifest entry survives, the sentinel MUST survive too so the readability gate keeps the branch
    /// non-existent (a plain prefix delete would drop the sentinel before the manifest — lexically
    /// `branch.pending` < `manifest/` — momentarily leaving readable manifest entries with no sentinel). Only
    /// once the branch's manifest/objects are provably gone is the sentinel dropped, and the source PIN LAST. If
    /// a delete errors partway, the sentinel + pin are RETAINED (the branch stays non-readable and the source
    /// segments stay protected) and the error is surfaced — NEVER an unpinned partial branch. Idempotent: a
    /// re-run finishes any partial cleanup (already-deleted objects delete cleanly).
    fn cleanup_uncommitted_branch(&self, source: &QueueKey, branch: &QueueKey) -> EngineResult<()> {
        let pending = branch_pending_key(branch);
        for key in self.store_list(&shard_prefix(branch))? {
            if key == pending {
                continue;
            }
            self.store_delete(&key)?;
        }
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .remove(branch);
        self.store_delete(&pending)?;
        self.store_delete(&branch_registry_key(source, branch))?;
        Ok(())
    }

    /// Reclaim the durable objects of ORPHANED uncommitted branches of `source` — the space leak a failed branch
    /// creation (or a rollback whose own cleanup failed) can leave behind (bead pqueue-74f03d0e, follow-up to
    /// pqueue-b5cc2bc7). Branch creation writes the source pin + `branch.pending` sentinel + branch manifest
    /// objects and lands the `branch.json` commit marker LAST; a partial/failed attempt therefore leaves durable
    /// GARBAGE (a leftover sentinel, partial manifest copies, and a still-registered source pin) that no read
    /// path can ever see (they gate on the marker) but that is never reclaimed — a slow leak proportional to
    /// failed-creation attempts, and a source pin that keeps the source's own segments un-reclaimable.
    ///
    /// The source's branch registry (`{source}branches/`) is the index of every pin — and a pin is published
    /// FIRST and released LAST, so an orphan ALWAYS still has its registry entry (if the pin is gone, the
    /// objects it protected are already gone too). For each registered branch this reclaims it IFF the
    /// `branch.json` commit marker is ABSENT (an uncommitted branch); a COMMITTED branch (marker present) is a
    /// live, readable branch protected by its own TTL/pin and is NEVER touched.
    ///
    /// CORRECTNESS — SINGLE INSTANCE (the shipped guarantee, why marker-absent is safe to reclaim, not a timing
    /// guess): this whole classify+delete runs under the [`Self::create_gc_guard`] that branch creation ALSO
    /// holds for its whole duration (including the commit-marker write and any rollback). So on ONE log instance
    /// NO creation can be in flight while GC runs — the classification is EXACT: a marker-absent branch is
    /// DEFINITIVELY a failed/abandoned creation (its creation already released the guard without landing the
    /// marker), never a creation about to write its marker. This closes the classify-then-delete TOCTOU (a
    /// concurrent creation on this instance cannot slip a `branch.json` in between the marker check and the
    /// delete) with a REAL mutual-exclusion argument, not a "should be long enough" window.
    ///
    /// SCOPE — CROSS INSTANCE (shared-store owners): the guard is a per-instance field, so it does NOT exclude
    /// a branch creation running on a DIFFERENT `SegmentedObjectLog` instance sharing the same store. Cross-
    /// instance safety therefore depends on the creation protocol itself fencing the final commit on the source
    /// ownership epoch: a superseded owner re-checks the durable source epoch before the `branch.json` marker is
    /// written, so a newer owner's `acquire_epoch(source)` forces the older creator to roll back cleanly instead
    /// of committing a branch on a source it no longer owns. GC still reuses the same cleanup routine; the
    /// commit fence is what closes the residual TOCTOU.
    ///
    /// Reclamation reuses [`Self::cleanup_uncommitted_branch`] (objects first, source pin LAST), so it also
    /// RELEASES the orphaned source pin and the source becomes fully reclaimable again. Store-failure-tolerant +
    /// idempotent: a delete that fails surfaces its error and leaves the rest for the NEXT pass (never corrupts,
    /// and a re-run over an already-cleaned orphan is a clean no-op). Branches copy no segment OBJECTS of their
    /// own (their manifest entries reference the SOURCE's shared segments via the pin), so no source segment is
    /// ever deleted here — only branch-local manifest/sentinel/queue objects and the source pin. Returns the
    /// number of orphans reclaimed.
    pub fn gc_orphaned_branches(&self, source: &QueueKey) -> EngineResult<u64> {
        // Exclude concurrent branch creation for the WHOLE classify+delete (see the doc comment + the guard's
        // definition). Outermost lock: taken before any `inner` acquisition, so no lock-order inversion.
        // POISON-TOLERANT: this mutex guards CREATE-vs-GC coordination, not an in-memory invariant, so a panic
        // that unwinds through a creation (or GC) while it holds the guard must NOT wedge all future GC (and
        // creation) forever. Recover the guard from a poisoned lock instead of propagating the panic.
        let _create_guard = self
            .create_gc_guard
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut reclaimed = 0u64;
        for meta in self.read_branch_registry(source)? {
            let branch = &meta.branch;
            // COMMITTED (marker present) => a live, readable branch protected by its own TTL/pin. NEVER GC.
            if self.store_get(&branch_metadata_key(branch))?.is_some() {
                continue;
            }
            // Test seam (never armed in production): strike the classify→delete window a concurrent creation's
            // marker write could race, so the create/GC exclusion can be proven deterministically.
            self.fault(FaultCutPoint::GcAfterOrphanClassified)?;
            // ORPHAN: marker absent AND — under the create/GC guard — provably not an in-flight creation, so it
            // is a failed/abandoned attempt. Reclaim ALL its objects and release the source pin. A failing
            // delete surfaces here (`?`) and leaves the remainder for the next pass.
            self.cleanup_uncommitted_branch(source, branch)?;
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    /// Expire parent segments at or before `through_seq`, skipping any segment pinned by a live branch.
    ///
    /// This deletes the expired segment OBJECT and its manifest entry pair once the branch-pin check says the
    /// source segment is no longer needed. While a live branch still pins the source segment, reclamation
    /// leaves both objects in place so the pin remains effective; after the pin is released, the next pass
    /// reclaims the previously pinned segment and its manifest records together.
    pub fn expire_segments_through(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        let entries = self.read_manifest(source)?;
        let mut deleted = 0u64;
        for entry in &entries {
            if entry.fence {
                continue;
            }
            if Self::visible_last_seq(entry) > through_seq {
                continue;
            }
            if self.branch_pins_segment(source, entry.first_seq, now_ms)? {
                continue;
            }
            if let Some(seg_key) = entry.segment_key.as_ref() {
                // Test-only crash seam (never armed in production): a fault here models a process death mid-
                // reclamation, after the durable floor advanced but before this object is deleted.
                self.fault(FaultCutPoint::DuringSegmentExpiry)?;
                if self.store_delete(seg_key)? {
                    deleted += 1;
                }
                self.delete_manifest_entry(source, entry.index)?;
            }
        }
        // Advance the durable read-horizon now that the below-floor segments are reclaimed (bead
        // pqueue-8928baec). BEST-EFFORT: a transient failure here must NOT fail the completed reclamation — the
        // horizon is a monotonic read-cost optimization that catches up on the next trim / (re)open expiry, and
        // reads stay correct via the full-list fallback + fail-closed floor guard. The updater consumes the
        // same manifest snapshot used for deletion, so physical reclamation can happen without losing sight of
        // the reclaimed prefix before the watermark is persisted.
        //
        // Protocol note: the deferred pqueue-c33c367e owner-fence wiring does not change this watermark path.
        // The permanent head CAS stays the stale-writer fence; we keep below-floor manifest addresses occupied
        // so the collision fence stays intact. The current index-CAS manifest protocol still cannot support
        // delete-only compaction safely; a cheaper delete-only variant would need the post-head-CAS protocol
        // redesign, not this code path.
        let _ = self.advance_read_horizon_from_entries(source, through_seq, now_ms, &entries);
        Ok(deleted)
    }

    /// The LOWEST `first_seq` among data segments at or below `through_seq` that `expire_segments_through`
    /// would SKIP because a live branch pins them (bead pqueue-b5cc2bc7 bug 2b). A branch pin is a TRANSIENT
    /// condition — once the branch is discarded the segment becomes reclaimable again — so the trim caller must
    /// NOT record "fully reclaimed up to the floor" past a pinned segment, or a released pin would leave the
    /// object leaked forever. `None` when nothing at/below the horizon is branch-pinned.
    pub fn lowest_branch_pinned_below(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<Option<u64>> {
        let mut lowest: Option<u64> = None;
        for entry in self.read_manifest(source)? {
            if entry.fence || entry.segment_key.is_none() {
                continue;
            }
            if Self::visible_last_seq(&entry) > through_seq {
                continue;
            }
            if self.branch_pins_segment(source, entry.first_seq, now_ms)? {
                lowest = Some(lowest.map_or(entry.first_seq, |l| l.min(entry.first_seq)));
            }
        }
        Ok(lowest)
    }

    /// A snapshot of the measured segment/object counters (release-ledger harness surface).
    pub fn counters(&self) -> SegmentCounters {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .counters
            .clone()
    }

    // -- high-water + snapshots (ADR-012 LogStore facets stored as blobs in the object store) ----------
    //
    // The orthogonal `LogStore` axis (compose.rs) requires a durable high-water mark and projection
    // snapshots. The manifest tail is the authoritative command position, but the engine also drives an
    // EXPLICIT high-water (snapshot truncation, TD-007 §4) and writes projection snapshots — both stored
    // here as small JSON blobs alongside the segments, exactly as the per-file `ObjectLogBackend` keeps a
    // `high_water.json`.

    /// Read the durable high-water mark blob (`None` if no commit/set has advanced it yet).
    pub fn read_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let key = format!("{}high_water.json", shard_prefix(shard));
        match self.store_get(&key)? {
            Some(bytes) => {
                let hw: HighWaterBlob = serde_json::from_slice(&bytes).map_err(store_err)?;
                Ok(Some(CommandPosition::new(shard.clone(), hw.epoch, hw.seq)))
            }
            None => Ok(None),
        }
    }

    /// Unconditionally advance the high-water blob to `position` (called by the append path after a seal,
    /// where the new position always advances — no monotonic re-check needed on the hot path).
    pub fn advance_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        let key = format!("{}high_water.json", shard_prefix(shard));
        let blob = HighWaterBlob {
            epoch: position.backend_epoch,
            seq: position.sequence,
        };
        self.store_put(&key, &to_json(&blob)?, false)
    }

    /// Monotonically set the high-water blob (snapshot-truncation setter): reject a position that does not
    /// advance the stored one (TD-007 §4), else persist it.
    pub fn set_high_water(&self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        if let Some(cur) = self.read_high_water(shard)?
            && !cur.precedes(&position)
            && cur != position
        {
            return Err(EngineError::Invalid("high-water regression"));
        }
        self.advance_high_water(shard, position)
    }

    // -- retention floor (bounded-recovery segment-object reclamation, bead pqueue-b5cc2bc7) -------------
    //
    // The AUTHORITATIVE retention floor is a MANIFEST ENTRY (`retention_floor_through`), advanced by the SAME
    // atomic, create-only, epoch-fenced manifest CAS the substrate uses for data segments and epoch fences —
    // NOT a racy read-then-overwrite blob (bug 3). It records the highest command sequence whose segment
    // OBJECTS have been trimmed (`expire_segments_through`), an EXCLUSIVE lower bound: recovery + the
    // idempotency folds resume at `floor + 1`, mirroring `recovery_high_water`'s "resume at next_seq". The
    // floor entry is committed BEFORE the segment objects are deleted (crash-safe order): a crash after the
    // floor commit but before the delete leaves floor=F with some below-F segments still present; recovery
    // reads from F+1 and skips them (no "missing segment" error). Because the advance is a manifest CAS, a
    // superseded owner either LOSES the CAS (its index is already taken) or is `EpochFenced`, so it can never
    // regress a newer owner's floor and strand recovery at a reclaimed segment.

    /// The durable per-shard read-horizon watermark object key (OUTSIDE the `manifest/` prefix).
    fn read_horizon_key(shard: &QueueKey) -> String {
        format!("{}read_horizon.json", shard_prefix(shard))
    }

    /// Read the durable READ-HORIZON watermark `W` (bead pqueue-8928baec): the highest manifest index below
    /// which every entry is a below-floor entry no reader needs. `None` when no trim has advanced it yet
    /// (backward-compatible: reads then fall back to the full manifest list).
    pub fn read_read_horizon(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        match self.store_get(&Self::read_horizon_key(shard))? {
            Some(bytes) => {
                let blob: ReadHorizonBlob = serde_json::from_slice(&bytes).map_err(store_err)?;
                Ok(Some(blob.index))
            }
            None => Ok(None),
        }
    }

    /// Read the AUTHORITATIVE durable retention floor: the highest `retention_floor_through` recorded across the
    /// manifest (`None` if no trim has advanced it yet). The returned position is the EXCLUSIVE lower bound
    /// (last-trimmed seq), carrying the epoch of the entry that set it; recovery/idempotency folds resume at
    /// `sequence + 1`.
    pub fn read_retention_floor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        // A partial/uncommitted branch is NON-EXISTENT: it has no resolvable floor.
        if self.branch_uncommitted(shard)? {
            return Ok(None);
        }
        let mut best: Option<(u64, u64)> = None; // (seq, epoch)
        for entry in self.read_manifest(shard)? {
            if let Some(seq) = entry.retention_floor_through
                && best.is_none_or(|(bs, _)| seq >= bs)
            {
                best = Some((seq, entry.epoch));
            }
        }
        Ok(best.map(|(seq, epoch)| CommandPosition::new(shard.clone(), epoch, seq)))
    }

    /// Advance the durable retention floor to `position` by appending a retention-floor-advance MANIFEST ENTRY
    /// via the create-only, epoch-fenced manifest CAS (bead pqueue-b5cc2bc7 bug 3 — atomic, not a racy blob
    /// overwrite). `expected_epoch` is the writing owner's currently-held assignment epoch.
    ///
    /// - **Epoch fence + atomic CAS.** The authoritative current epoch is read from the manifest tail; a
    ///   superseded writer (`current > expected_epoch`) is rejected [`EngineError::EpochFenced`]. The floor
    ///   entry is then committed with `put_if_absent` at the next index — so even if a newer owner takes over
    ///   BETWEEN the epoch read and the write, this writer LOSES the CAS (the index is taken) and re-checks the
    ///   epoch, returning `EpochFenced`/`Conflict` rather than regressing the newer owner's floor.
    /// - **Monotonic.** A `position.sequence` below the current authoritative floor is rejected; equal is an
    ///   idempotent no-op (no new entry). The trim caller only ever calls this to strictly advance.
    pub fn advance_retention_floor(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        expected_epoch: u64,
    ) -> EngineResult<()> {
        let (cur_seq, cur_index, cur_epoch) = self.recover_manifest(shard)?;
        if cur_epoch > expected_epoch {
            return Err(EngineError::EpochFenced);
        }
        if let Some(cur_floor) = self.read_retention_floor(shard)? {
            if position.sequence < cur_floor.sequence {
                return Err(EngineError::Invalid("retention floor regression"));
            }
            if position.sequence == cur_floor.sequence {
                return Ok(()); // already at/above this floor — idempotent no-op, no redundant entry
            }
        }
        // A floor-advance entry: no segment, carries the LIVE next-seq in `first_seq` (like a fence) so
        // `recover_manifest` derives the tail's next-seq correctly, and the trim-through in
        // `retention_floor_through`, committed at the current epoch.
        let entry = ManifestEntry {
            index: cur_index,
            epoch: cur_epoch,
            fence: false,
            segment_key: None,
            first_seq: cur_seq,
            last_seq: cur_seq,
            visible_last_seq: None,
            checksum: 0,
            committed_at_ms: 0, // audit-only; floor entries are skipped by every age/segment scanner
            retention_floor_through: Some(position.sequence),
        };
        let won = self.commit_manifest_entry(shard, &entry, false)?;
        if !won {
            // CAS lost: a peer extended the manifest between our read and our write. If a newer epoch is now
            // present we are fenced; otherwise surface a transient conflict (the trim caller skips this tick).
            let observed_epoch = self.recover_manifest(shard)?.2;
            if observed_epoch > expected_epoch {
                return Err(EngineError::EpochFenced);
            }
            return Err(EngineError::Conflict);
        }
        // Keep the in-memory tail bookkeeping in sync so the next seal's CAS uses the right index.
        let mut g = self.inner.lock().expect("segmented log poisoned");
        if let Some(buf) = g.shards.get_mut(shard) {
            buf.next_manifest_index = buf.next_manifest_index.max(cur_index + 1);
        }
        Ok(())
    }

    /// Advance the durable READ-HORIZON watermark `W` for `shard` (bead pqueue-8928baec). Folded into the trim
    /// path at the END of [`Self::expire_segments_through`] — AFTER the below-floor segment objects are
    /// actually reclaimed — so the horizon advances whenever the floor advances (the trim always runs
    /// `advance_retention_floor` then `expire_segments_through`, and a (re)open re-runs the expiry up to the
    /// durable floor). Placing it after the delete is load-bearing: advancing it BEFORE would hide the very
    /// below-floor entries `expire_segments_through` must enumerate to find the segment keys to delete. Also
    /// callable standalone (tests / an explicit compaction tick). `now_ms` gates the branch-pin check.
    ///
    /// `W` = the highest index of the OLDEST CONTIGUOUS PREFIX of manifest entries that are ALL strictly below
    /// the durable retention floor AND already RECLAIMED — a reclaimed DATA tombstone
    /// (`visible_last_seq <= reclaimed_through`, not branch-pinned), a SUPERSEDED floor-advance entry
    /// (`retention_floor_through == Some(v) && v < floor`), or an old FENCE at/below the floor
    /// (`first_seq <= floor`). The walk STOPS at the first entry that is NOT provably reclaimed: a LIVE or
    /// not-yet-reclaimed DATA entry (`visible_last_seq > reclaimed_through`), the AUTHORITATIVE floor entry
    /// (`retention_floor_through == Some(floor)`), a still-branch-PINNED below-floor data segment (its object
    /// is NOT yet reclaimed — a future expire once the pin releases must still find it), or the tail — so `W`
    /// is ALWAYS strictly below every entry any read / read_retention_floor / recover-tail / branch-copy /
    /// FUTURE-EXPIRE needs. MONOTONIC: a candidate that would lower the stored watermark is a no-op.
    ///
    /// `reclaimed_through` is the boundary [`Self::expire_segments_through`] just deleted up to — bounding the
    /// DATA check by it (not the possibly-higher durable `floor`) is load-bearing: a partial expire
    /// (`through < floor`) must NOT hide an unpinned, NOT-yet-deleted below-floor segment from a future expire
    /// (a storage leak). Non-data entries (fences / superseded floor markers) name no segment, so advancing
    /// past them below the floor leaks nothing. In the production trim path `through == floor`, so `W` advances
    /// fully; a caller passing `through < floor` simply advances `W` more conservatively.
    ///
    /// SAFETY (never hides a live entry, even under races): every writer derives `W` strictly below the value
    /// it reads for the durable, MONOTONE retention floor (`read_retention_floor` returns the max
    /// `retention_floor_through` across the authoritative manifest — no writer can observe a floor above the
    /// true durable floor), and the lowest LIVE entry is above that floor. So the MAX `W` any racing writer
    /// can persist is still below the lowest live index; a stale writer that regresses `W` to a lower value is
    /// harmless (it only widens enumeration, never hides live data). It NEVER deletes/renames/marks a manifest
    /// object — below-`W` addresses stay OCCUPIED, so a stale writer's `put_if_absent` there still COLLIDES and
    /// the epoch-fence is intact byte-for-byte.
    fn advance_read_horizon_from_entries(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
        entries: &[ManifestEntry],
    ) -> EngineResult<()> {
        let Some(floor) = self.read_retention_floor(shard)? else {
            return Ok(()); // no durable floor => no read-horizon
        };
        let mut new_w: Option<u64> = None;
        for entry in entries {
            // STOP at the AUTHORITATIVE floor entry — `read_retention_floor` needs it, so W must stay below it.
            if entry.retention_floor_through == Some(floor.sequence) {
                break;
            }
            let reclaimed = match entry.retention_floor_through {
                // A SUPERSEDED floor-advance entry (names no segment; strictly below the authoritative floor).
                Some(v) => v < floor.sequence,
                // An old epoch FENCE published at/below the floor (names no segment; no reader needs it).
                None if entry.fence => entry.first_seq <= floor.sequence,
                // A DATA tombstone whose object is provably reclaimed: its whole visible range is at/below what
                // this (or a prior) expire actually deleted. Bounded by `reclaimed_through`, NOT the floor.
                None => Self::visible_last_seq(entry) <= reclaimed_through,
            };
            if !reclaimed {
                break; // first LIVE / not-yet-reclaimed / needed entry — W must stay STRICTLY below it
            }
            // A still-branch-PINNED below-floor DATA segment has NOT been reclaimed (expire_segments_through
            // skipped its delete): a future trim after the pin releases must still enumerate it, so do NOT
            // hide it behind the horizon. Stop here (keeps W strictly below the pinned index).
            if entry.segment_key.is_some()
                && self.branch_pins_segment(shard, entry.first_seq, now_ms)?
            {
                break;
            }
            new_w = Some(entry.index);
        }
        if let Some(w) = new_w
            && self.read_read_horizon(shard)?.is_none_or(|cur| w > cur)
        {
            // Best-effort monotonic PUT. A candidate is always derived strictly below the durable floor (see
            // SAFETY above), so even a non-atomic read-check-then-put that a racing writer interleaves can only
            // regress W to another below-floor value — never above a live entry.
            let blob = ReadHorizonBlob { index: w };
            self.store_put(&Self::read_horizon_key(shard), &to_json(&blob)?, false)?;
        }
        Ok(())
    }

    pub fn advance_read_horizon(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
    ) -> EngineResult<()> {
        let entries = self.read_manifest(shard)?;
        self.advance_read_horizon_from_entries(shard, reclaimed_through, now_ms, &entries)
    }

    /// The highest `visible_last_seq` over the CONTIGUOUS PREFIX of DATA manifest segments whose
    /// `committed_at_ms <= cutoff_ms` (bead pqueue-b5cc2bc7). This is the "all request_ids in these segments
    /// are past retention" horizon: `created_at <= committed_at_ms` by causality, so a segment committed at or
    /// before `cutoff_ms = now - request_id_retention_ms - SKEW_MARGIN` holds ONLY expired request_ids.
    ///
    /// The scan STOPS at the first data segment newer than `cutoff_ms` (rather than taking a global max), so
    /// the returned seq never spans a still-fresh middle segment even if `committed_at_ms` were ever
    /// non-monotonic across seals — every data segment at or below the returned seq is provably expired, which
    /// is exactly the precondition `expire_segments_through` needs (it deletes ALL data segments up to the
    /// horizon). Fence entries (which name no segment) are skipped, not treated as a boundary. `None` when no
    /// data segment is old enough (nothing to trim).
    pub fn max_trimmable_seq_before(
        &self,
        shard: &QueueKey,
        cutoff_ms: i64,
    ) -> EngineResult<Option<u64>> {
        let mut best: Option<u64> = None;
        for entry in self.read_manifest(shard)? {
            if entry.fence || entry.segment_key.is_none() {
                continue;
            }
            // A non-positive `committed_at_ms` is NOT a trustworthy seal-time upper bound on the segment's
            // command `created_at` (bead pqueue-b5cc2bc7 bug 1): e.g. a legacy raw-append segment written
            // before the seal-timestamp fix stamped 0. Treat it as NOT-yet-expired and STOP the prefix scan
            // (conservative — never age-trim a segment whose age we cannot bound), so a within-retention
            // request_id in such a segment is never reclaimed. The current write paths always stamp a real
            // upper bound (group-commit: the push `now`; raw append: max `created_at` over the batch).
            if entry.committed_at_ms <= 0 {
                break;
            }
            if entry.committed_at_ms <= cutoff_ms {
                best = Some(Self::visible_last_seq(&entry));
            } else {
                break;
            }
        }
        Ok(best)
    }

    /// Write a projection snapshot blob and return its distinct ref id (`snap-{n}`).
    pub fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        payload: &[u8],
    ) -> EngineResult<String> {
        let prefix = format!("{}snap/", shard_prefix(shard));
        let n = self.store_list(&prefix)?.len();
        let ref_id = format!("snap-{n}");
        let blob = SnapshotBlob {
            epoch: position.backend_epoch,
            seq: position.sequence,
            payload: payload.to_vec(),
        };
        let key = format!("{prefix}{ref_id}.json");
        self.fault(FaultCutPoint::DuringSnapshotWrite)?;
        self.store_put(&key, &to_json(&blob)?, false)?;
        Ok(ref_id)
    }

    /// The most-recently written snapshot's `(ref_id, position)`, or `None`. Ordered by the numeric suffix
    /// (so `snap-10` is newer than `snap-9` — a lexical max would be wrong).
    pub fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Option<(String, CommandPosition)>> {
        let prefix = format!("{}snap/", shard_prefix(shard));
        let mut best: Option<(u64, String)> = None;
        for key in self.store_list(&prefix)? {
            let ref_id = key
                .rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".json"))
                .unwrap_or("");
            if let Some(n) = ref_id
                .strip_prefix("snap-")
                .and_then(|s| s.parse::<u64>().ok())
                && best.as_ref().is_none_or(|(bn, _)| n > *bn)
            {
                best = Some((n, ref_id.to_string()));
            }
        }
        match best {
            Some((_, ref_id)) => {
                let blob = self.read_snapshot_blob(shard, &ref_id)?;
                Ok(Some((
                    ref_id,
                    CommandPosition::new(shard.clone(), blob.epoch, blob.seq),
                )))
            }
            None => Ok(None),
        }
    }

    /// The newest snapshot at or before `position`, or `None`.
    pub fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<(String, CommandPosition)>> {
        let prefix = format!("{}snap/", shard_prefix(shard));
        let mut best: Option<(u64, String, CommandPosition)> = None;
        for key in self.store_list(&prefix)? {
            let ref_id = key
                .rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".json"))
                .unwrap_or("");
            let Some(n) = ref_id
                .strip_prefix("snap-")
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            let blob = self.read_snapshot_blob(shard, ref_id)?;
            let snapshot_position = CommandPosition::new(shard.clone(), blob.epoch, blob.seq);
            if (snapshot_position.precedes(position) || snapshot_position == *position)
                && best.as_ref().is_none_or(|(bn, _, _)| n > *bn)
            {
                best = Some((n, ref_id.to_string(), snapshot_position));
            }
        }
        Ok(best.map(|(_, ref_id, position)| (ref_id, position)))
    }

    /// Read a snapshot's payload by ref id.
    pub fn read_snapshot(&self, shard: &QueueKey, ref_id: &str) -> EngineResult<Vec<u8>> {
        Ok(self.read_snapshot_blob(shard, ref_id)?.payload)
    }

    fn read_snapshot_blob(&self, shard: &QueueKey, ref_id: &str) -> EngineResult<SnapshotBlob> {
        let key = format!("{}snap/{ref_id}.json", shard_prefix(shard));
        let bytes = self
            .store_get(&key)?
            .ok_or_else(|| EngineError::Storage(format!("missing snapshot {ref_id}")))?;
        serde_json::from_slice(&bytes).map_err(store_err)
    }

    /// Register a shard by key alone (the ADR-012 `LogStore::ensure_shard` seam — the control plane owns the
    /// queue DEFINITION, so the log axis only needs the key to recover its manifest tail). Idempotent.
    pub fn ensure_shard(&self, shard: &QueueKey) -> EngineResult<()> {
        let (next_seq, next_index, epoch) = self.recover_manifest(shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.shards.entry(shard.clone()).or_insert(ShardBuf {
            buffered: Vec::new(),
            buffered_bytes: 0,
            oldest_buffered_ms: None,
            next_seq,
            next_manifest_index: next_index,
            committed_epoch: epoch,
        });
        Ok(())
    }

    /// Persist a queue definition as a durable per-shard `queue.json` object (ADR-012 P2 recovery-on-open).
    /// The composition's in-process control plane is not durable, so the object log catalogs definitions
    /// here; a reopened composition enumerates them ([`Self::recover_definitions`]) to rebuild WITHOUT a
    /// re-create_queue. Unconditional PUT (idempotent at a stable key — a compatible re-create re-writes
    /// identical bytes).
    pub fn persist_definition(&self, def: &QueueDefinition) -> EngineResult<()> {
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let key = format!("{}queue.json", shard_prefix(&shard));
        self.store_put(&key, &to_json(def)?, false)
    }

    /// Enumerate every durable queue definition catalogued under the store root (the `queue.json` objects).
    pub fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        let mut out = Vec::new();
        for key in self.store_list("t/")? {
            if !key.ends_with("/queue.json") {
                continue;
            }
            if let Some(bytes) = self.store_get(&key)? {
                out.push(serde_json::from_slice(&bytes).map_err(store_err)?);
            }
        }
        Ok(out)
    }
}

/// Durable high-water blob (the explicit command-position high-water; TD-007 §4).
#[derive(serde::Serialize, serde::Deserialize)]
struct HighWaterBlob {
    epoch: u64,
    seq: u64,
}

/// Durable per-shard READ-HORIZON watermark blob (bead pqueue-8928baec): the highest manifest `index` below
/// which every entry is a reclaimed/superseded below-floor entry that no live read, recovery tail,
/// authoritative-floor read, or branch copy needs. Stored at `{shard_prefix}read_horizon.json` — OUTSIDE the
/// `manifest/` prefix — so `recover_manifest`/`read_manifest` LISTs never enumerate it. Monotonic (a set that
/// would lower it is a no-op). It NEVER frees a manifest address or becomes an ownership fence: below-horizon
/// objects still EXIST, so a stale writer's `put_if_absent` at a below-horizon cached index still COLLIDES →
/// the permanent head CAS stays the stale-writer fence.
#[derive(serde::Serialize, serde::Deserialize)]
struct ReadHorizonBlob {
    index: u64,
}

/// A projection snapshot blob (payload + the command position it was taken at).
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotBlob {
    epoch: u64,
    seq: u64,
    payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Minimal hand-rolled S3 client (SigV4 PUT/GET/LIST + create-only conditional PUT)
// ---------------------------------------------------------------------------

/// A minimal S3-compatible [`BlobStore`] over plain HTTP/1.1 + SigV4. Deliberately dependency-light: the
/// only crate it pulls beyond the workspace baseline is `sha2` (already in-tree for the relational
/// projection); HMAC-SHA256, the SigV4 canonical request, the HTTP/1.1 framing, and the (small) ListObjects
/// XML scrape are hand-rolled. Targets MinIO / any S3-compatible store over `http://host:port` (path-style
/// addressing). The manifest CAS uses `If-None-Match: *` (create-only conditional PUT), which MinIO and S3
/// both support and which needs no ETag round-trip.
pub struct S3BlobStore {
    host: String,
    port: u16,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl S3BlobStore {
    /// Build a client. `endpoint` is `http://host:port` (the orbstack container IP form). The bucket must
    /// exist (or call [`S3BlobStore::create_bucket`]).
    pub fn new(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> EngineResult<Self> {
        let rest = endpoint
            .strip_prefix("http://")
            .ok_or(EngineError::Invalid("endpoint must be http://host:port"))?;
        let (host, port) = match rest.split_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.trim_end_matches('/')
                    .parse::<u16>()
                    .map_err(|_| EngineError::Invalid("bad endpoint port"))?,
            ),
            None => (rest.trim_end_matches('/').to_string(), 80),
        };
        Ok(Self {
            host,
            port,
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: region.to_string(),
        })
    }

    /// Create the bucket (idempotent: an existing bucket's 409/200 is treated as success).
    pub fn create_bucket(&self) -> EngineResult<()> {
        let path = format!("/{}", self.bucket);
        let (status, _) = self.request("PUT", &path, &[], &[], &[])?;
        if status == 200 || status == 204 || status == 409 {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "create_bucket failed: HTTP {status}"
            )))
        }
    }

    fn object_path(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, key)
    }

    /// Sign and send one HTTP/1.1 request over a fresh `Connection: close` TCP stream. Returns
    /// `(status, body)`. `query` is the (unencoded) query params; `extra_headers` are additional
    /// signed headers (e.g. the conditional `If-None-Match`).
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> EngineResult<(u16, Vec<u8>)> {
        let (amz_date, datestamp) = amz_dates();
        let payload_hash = hex_lower(&sha256(body));
        let host_header = format!("{}:{}", self.host, self.port);

        // Canonical headers (lowercased, sorted): host, x-amz-content-sha256, x-amz-date, + extras.
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), host_header.clone()),
            ("x-amz-content-sha256".into(), payload_hash.clone()),
            ("x-amz-date".into(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            headers.push((k.to_lowercase(), v.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{}\n", v.trim()))
            .collect::<String>();

        let canonical_uri = uri_encode(path, false);
        let mut q = query.to_vec();
        q.sort();
        let canonical_query = q
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{datestamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex_lower(&sha256(canonical_request.as_bytes()))
        );
        let signing_key = self.signing_key(&datestamp);
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        // Build the raw HTTP/1.1 request.
        let target = if canonical_query.is_empty() {
            canonical_uri.clone()
        } else {
            format!("{canonical_uri}?{canonical_query}")
        };
        let mut req = format!("{method} {target} HTTP/1.1\r\n");
        req.push_str(&format!("Host: {host_header}\r\n"));
        req.push_str(&format!("x-amz-date: {amz_date}\r\n"));
        req.push_str(&format!("x-amz-content-sha256: {payload_hash}\r\n"));
        for (k, v) in extra_headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str(&format!("Authorization: {authorization}\r\n"));
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        req.push_str("Connection: close\r\n\r\n");

        let mut stream = TcpStream::connect((self.host.as_str(), self.port)).map_err(store_err)?;
        stream.write_all(req.as_bytes()).map_err(store_err)?;
        stream.write_all(body).map_err(store_err)?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).map_err(store_err)?;
        parse_http_response(&raw)
    }

    fn signing_key(&self, datestamp: &str) -> [u8; 32] {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            datestamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        let (status, resp) = self.request("PUT", &self.object_path(key), &[], body, &[])?;
        if status == 200 || status == 204 {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "S3 PUT {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            )))
        }
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        // Create-only conditional PUT: `If-None-Match: *` succeeds only if the object does not yet exist.
        let extra = vec![("If-None-Match".to_string(), "*".to_string())];
        let (status, resp) = self.request("PUT", &self.object_path(key), &[], body, &extra)?;
        match status {
            200 | 204 => Ok(true),
            // 412 Precondition Failed (object exists) / 409 Conflict → the CAS lost.
            409 | 412 => Ok(false),
            _ => Err(EngineError::Storage(format!(
                "S3 conditional PUT {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            ))),
        }
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        let (status, body) = self.request("GET", &self.object_path(key), &[], &[], &[])?;
        match status {
            200 => Ok(Some(body)),
            404 => Ok(None),
            _ => Err(EngineError::Storage(format!(
                "S3 GET {key} failed: HTTP {status}"
            ))),
        }
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        let (status, resp) = self.request("DELETE", &self.object_path(key), &[], &[], &[])?;
        match status {
            204 | 200 => Ok(true),
            404 => Ok(false),
            _ => Err(EngineError::Storage(format!(
                "S3 DELETE {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            ))),
        }
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.list_with_request_count(prefix).map(|(keys, _)| keys)
    }

    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        // ListObjectsV2 returns at most `MaxKeys` (default + cap 1000) keys per response. A queue that has
        // sealed more than 1000 segments therefore has a >1000-entry manifest, so a SINGLE-page list would
        // silently truncate it — returning a stale tail to `recover_manifest` (→ the next seal's manifest CAS
        // collides with an existing index = a spurious `Conflict`) AND dropping segments from `read_all` /
        // `read_from` (→ silent data loss on recovery). So follow the `NextContinuationToken` until the result
        // is no longer truncated, accumulating every page's keys. The returned request count feeds release
        // cost evidence: each page is a billable S3 LIST-class API request. (Exercised by the TP-002 E3
        // 10M-item live recovery run, whose recovery queue exceeds 1000 sealed segments.)
        let path = format!("/{}", self.bucket);
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        let mut request_count = 0u64;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(token) = &continuation {
                query.push(("continuation-token".to_string(), token.clone()));
            }
            let (status, body) = self.request("GET", &path, &query, &[], &[])?;
            request_count += 1;
            if status != 200 {
                return Err(EngineError::Storage(format!(
                    "S3 LIST {prefix} failed: HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                )));
            }
            let xml = String::from_utf8_lossy(&body);
            keys.extend(scrape_keys(&xml));
            match next_continuation_token(&xml) {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
        Ok((keys, request_count))
    }

    fn list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> EngineResult<(Vec<String>, u64)> {
        // NATIVE `StartAfter`: ListObjectsV2 begins enumeration strictly AFTER `start_after` (exclusive), so
        // the server never even scans the below-horizon manifest keys — this is where the read-cost win lands
        // at scale (the filter-after-list default would still page every key). `start-after` applies only to
        // the FIRST page; once a `continuation-token` takes over it is ignored (and must be dropped, else some
        // S3 implementations reject start-after + continuation-token together). Pagination is otherwise
        // identical to `list_with_request_count`: follow `NextContinuationToken` until no longer truncated,
        // billing each page as a LIST-class request.
        let path = format!("/{}", self.bucket);
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        let mut request_count = 0u64;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            match &continuation {
                Some(token) => query.push(("continuation-token".to_string(), token.clone())),
                None => query.push(("start-after".to_string(), start_after.to_string())),
            }
            let (status, body) = self.request("GET", &path, &query, &[], &[])?;
            request_count += 1;
            if status != 200 {
                return Err(EngineError::Storage(format!(
                    "S3 LIST {prefix} (start-after {start_after}) failed: HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                )));
            }
            let xml = String::from_utf8_lossy(&body);
            keys.extend(scrape_keys(&xml));
            match next_continuation_token(&xml) {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
        Ok((keys, request_count))
    }
}

/// Scrape `<Key>…</Key>` values out of an S3 ListObjectsV2 XML body. The substrate pages the whole result
/// ([`S3BlobStore::list`]), so this scrapes one page's keys; the caller concatenates the pages.
fn scrape_keys(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        if let Some(end) = after.find("</Key>") {
            keys.push(after[..end].to_string());
            rest = &after[end + 6..];
        } else {
            break;
        }
    }
    keys
}

/// The `NextContinuationToken` of a truncated ListObjectsV2 page, or `None` when the listing is complete.
/// Only honored when `<IsTruncated>true</IsTruncated>` (a non-truncated page carries no further token).
fn next_continuation_token(xml: &str) -> Option<String> {
    let truncated = scrape_tag(xml, "IsTruncated").as_deref() == Some("true");
    if !truncated {
        return None;
    }
    scrape_tag(xml, "NextContinuationToken").filter(|t| !t.is_empty())
}

/// Extract the text of the first `<tag>…</tag>` element from an XML body (small, dependency-free).
fn scrape_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Parse `(status_code, body)` from a raw HTTP/1.1 response (headers terminated by `\r\n\r\n`).
fn parse_http_response(raw: &[u8]) -> EngineResult<(u16, Vec<u8>)> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(EngineError::Storage("malformed HTTP response".into()))?;
    let head = &raw[..split];
    let body = raw[split + 4..].to_vec();
    let head_str = String::from_utf8_lossy(head);
    let status_line = head_str
        .lines()
        .next()
        .ok_or(EngineError::Storage("empty HTTP response".into()))?;
    // "HTTP/1.1 200 OK"
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or(EngineError::Storage("no HTTP status code".into()))?;
    Ok((status, body))
}

// ---------------------------------------------------------------------------
// Crypto + SigV4 helpers (hand-rolled over `sha2`; no extra crates)
// ---------------------------------------------------------------------------

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// HMAC-SHA256 (RFC 2104) over `sha2::Sha256`; block size 64.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

/// Percent-encode per SigV4 rules: unreserved `A-Za-z0-9-_.~` pass through; `/` passes through unless
/// `encode_slash`; everything else is `%XX` (uppercase hex).
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if keep || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Current UTC as `(amz_date "YYYYMMDDTHHMMSSZ", datestamp "YYYYMMDD")` from the system clock — no chrono.
fn amz_dates() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days);
    (
        format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z"),
        format!("{y:04}{mo:02}{d:02}"),
    )
}

/// Days-since-1970-01-01 → `(year, month, day)` (Howard Hinnant's civil-from-days algorithm).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// Unit tests for the local-filesystem BlobStore (mirrors InMemoryBlobStore behavior)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod fs_blob_store_tests {
    use super::*;

    /// A unique scratch directory under the system temp dir, removed on drop.
    struct TmpDir {
        path: PathBuf,
    }
    impl TmpDir {
        fn new() -> Self {
            let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = std::env::temp_dir()
                .join(format!("pqueue-fsblob-{}-{n}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// The same get/put/list/put_if_absent contract the substrate relies on, asserted identically against
    /// both stores so the local-filesystem store is a drop-in for the in-memory one.
    fn assert_blob_store_contract(store: &dyn BlobStore) {
        // get of a missing key is None.
        assert_eq!(store.get("t/a/q/b/seg/x").unwrap(), None);
        // put then get round-trips bytes.
        store.put("t/a/q/b/seg/00000.seg", b"hello").unwrap();
        assert_eq!(
            store.get("t/a/q/b/seg/00000.seg").unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        // put overwrites (idempotent re-put at a stable key).
        store.put("t/a/q/b/seg/00000.seg", b"world").unwrap();
        assert_eq!(
            store.get("t/a/q/b/seg/00000.seg").unwrap().as_deref(),
            Some(&b"world"[..])
        );
        // put_if_absent creates once, then loses the CAS.
        assert!(
            store
                .put_if_absent("t/a/q/b/manifest/0.json", b"first")
                .unwrap()
        );
        assert!(
            !store
                .put_if_absent("t/a/q/b/manifest/0.json", b"second")
                .unwrap()
        );
        assert_eq!(
            store.get("t/a/q/b/manifest/0.json").unwrap().as_deref(),
            Some(&b"first"[..]),
            "the CAS loser does not overwrite the winner's bytes"
        );
        // list returns keys under a prefix (and only those).
        store.put("t/a/q/b/manifest/1.json", b"e1").unwrap();
        store.put("t/other/q/z/seg/0.seg", b"z").unwrap();
        let mut manifest = store.list("t/a/q/b/manifest/").unwrap();
        manifest.sort();
        assert_eq!(
            manifest,
            vec![
                "t/a/q/b/manifest/0.json".to_string(),
                "t/a/q/b/manifest/1.json".to_string(),
            ]
        );
        let all_a = store.list("t/a/").unwrap();
        assert_eq!(
            all_a.len(),
            3,
            "all three keys under t/a/ (one seg + two manifest)"
        );
        assert!(store.list("t/nope/").unwrap().is_empty());
    }

    #[test]
    fn local_fs_blob_store_mirrors_in_memory() {
        let tmp = TmpDir::new();
        let fs_store = LocalFsBlobStore::open(&tmp.path).unwrap();
        assert_blob_store_contract(&fs_store);

        let mem = InMemoryBlobStore::new();
        assert_blob_store_contract(&mem);
    }

    #[test]
    fn local_fs_blob_store_persists_across_reopen() {
        let tmp = TmpDir::new();
        {
            let s = LocalFsBlobStore::open(&tmp.path).unwrap();
            s.put("t/a/q/b/seg/0.seg", b"durable").unwrap();
            assert!(s.put_if_absent("t/a/q/b/manifest/0.json", b"m0").unwrap());
        }
        // Reopen: the durable objects survive (the substrate recovers its manifest from these).
        let s = LocalFsBlobStore::open(&tmp.path).unwrap();
        assert_eq!(
            s.get("t/a/q/b/seg/0.seg").unwrap().as_deref(),
            Some(&b"durable"[..])
        );
        assert_eq!(
            s.list("t/a/q/b/manifest/").unwrap(),
            vec!["t/a/q/b/manifest/0.json".to_string()]
        );
        // A second create-only PUT at the recovered manifest tail still loses the CAS (durable fence).
        assert!(!s.put_if_absent("t/a/q/b/manifest/0.json", b"dup").unwrap());
    }
}

// ---------------------------------------------------------------------------
// ListObjectsV2 pagination scraping (the >1000-object correctness fix)
// ---------------------------------------------------------------------------
#[cfg(test)]
mod list_pagination_tests {
    use super::{next_continuation_token, scrape_keys, scrape_tag};

    #[test]
    fn scrape_keys_reads_every_key_on_a_page() {
        let xml = "<ListBucketResult><Contents><Key>m/0.json</Key></Contents>\
                   <Contents><Key>m/1.json</Key></Contents></ListBucketResult>";
        assert_eq!(scrape_keys(xml), vec!["m/0.json", "m/1.json"]);
    }

    #[test]
    fn truncated_page_yields_the_continuation_token() {
        // A truncated ListObjectsV2 page (>1000 objects) carries the token to fetch the next page.
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
                   <NextContinuationToken>abc123==</NextContinuationToken>\
                   <Contents><Key>m/0999.json</Key></Contents></ListBucketResult>";
        assert_eq!(scrape_tag(xml, "IsTruncated").as_deref(), Some("true"));
        assert_eq!(next_continuation_token(xml).as_deref(), Some("abc123=="));
    }

    #[test]
    fn final_page_has_no_continuation_token() {
        // The last page is NOT truncated, so listing stops (no token honored even if one were present).
        let complete = "<ListBucketResult><IsTruncated>false</IsTruncated>\
                        <Contents><Key>m/0.json</Key></Contents></ListBucketResult>";
        assert_eq!(next_continuation_token(complete), None);
        let empty = "<ListBucketResult></ListBucketResult>";
        assert_eq!(next_continuation_token(empty), None);
    }
}
