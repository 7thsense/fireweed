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
//! 4. **Commit manifest** — append a manifest entry naming the segment via a conditional
//!    (create-only) object write that is the CAS boundary AND the epoch fence (TD-004 step 4).
//! 5. **Ack** — a command's positions are returned to the caller ONLY after its segment's manifest
//!    entry is durably committed (TD-004 step 5). A buffered-but-unsealed command is NOT acked, and a
//!    segment whose manifest commit was fenced is an orphan that no reader ever observes.
//!
//! **Manifest-CAS epoch fence (reused from the `pqueue-e5c6d6fc` pattern).** Each manifest entry records
//! the writer's `assignment_epoch`. The manifest is an append-only series of immutable objects
//! `manifest/{index:020}.json`; a commit is a create-only PUT at the next index (the CAS) gated on the
//! writer's `expected_epoch` still equalling the queue's current epoch (the highest epoch any committed
//! manifest entry records). An epoch handoff publishes a **fence entry** (TD-004 implementation (b)) into
//! the manifest BEFORE the new owner writes data; a stale-epoch writer that tries to seal then observes the
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
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct BranchMetadata {
    source: QueueKey,
    branch: QueueKey,
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
    buffered: Vec<Vec<u8>>,
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
        if deleted && let Some(len) = g.object_sizes.remove(key) {
            g.counters.object_count = g.counters.object_count.saturating_sub(1);
            g.counters.total_bytes = g.counters.total_bytes.saturating_sub(len);
            if g.object_sizes.is_empty() {
                g.counters.max_object_bytes = 0;
            } else if len == g.counters.max_object_bytes {
                g.counters.max_object_bytes = g.object_sizes.values().copied().max().unwrap_or(0);
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
        let prefix = format!("{}manifest/", shard_prefix(shard));
        let Some(tail_key) = self.store_list(&prefix)?.into_iter().max() else {
            return Ok((0, 0, 0));
        };
        let Some(bytes) = self.store_get(&tail_key)? else {
            return Ok((0, 0, 0));
        };
        let tail: ManifestEntry = serde_json::from_slice(&bytes).map_err(store_err)?;
        let next_index = tail.index + 1;
        let next_seq = if tail.fence {
            tail.first_seq
        } else {
            Self::visible_last_seq(&tail) + 1
        };
        Ok((next_seq, next_index, tail.epoch))
    }

    /// All manifest entries for `shard`, sorted by index.
    fn read_manifest(&self, shard: &QueueKey) -> EngineResult<Vec<ManifestEntry>> {
        let prefix = format!("{}manifest/", shard_prefix(shard));
        let mut entries = Vec::new();
        for key in self.store_list(&prefix)? {
            if let Some(bytes) = self.store_get(&key)? {
                let entry: ManifestEntry = serde_json::from_slice(&bytes).map_err(store_err)?;
                entries.push(entry);
            }
        }
        entries.sort_by_key(|e| e.index);
        Ok(entries)
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
    pub fn acquire_epoch(&self, shard: &QueueKey, now_ms: i64) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        let prefix = shard_prefix(shard);
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
            };
            let key = format!("{prefix}manifest/{next_index:020}.json");
            if self.store_put_if_absent(&key, &to_json(&entry)?, true)? {
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
                buf.buffered.push(bytes);
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
        let drained = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            if buf.buffered.is_empty() {
                return Ok(Vec::new());
            }
            std::mem::take(&mut buf.buffered)
        };
        let n = drained.len();

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

        self.fault(FaultCutPoint::BeforeSegmentWrite)?;

        // 3. Write the immutable, checksummed segment object (idempotent at its first-seq key). The segment
        //    is the framed concatenation of the per-command bytes serialized once at buffer time — no
        //    re-serialize on seal (Fix A). The checksum covers the records-blob region.
        let first_seq = cur_seq;
        let last_seq = first_seq + n as u64 - 1;
        let (seg_bytes, seg_checksum) = build_segment_object(cur_epoch, first_seq, &drained);
        let seg_key = format!("{prefix}seg/{first_seq:020}.seg");
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
            committed_at_ms: now_ms,
        };
        let manifest_key = format!("{prefix}manifest/{cur_index:020}.json");
        let won = self.store_put_if_absent(&manifest_key, &to_json(&entry)?, true)?;
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
    pub fn read_all(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        let mut out = Vec::new();
        for entry in self.read_manifest(shard)? {
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
        let mut out = Vec::new();
        for entry in self.read_manifest(shard)? {
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
    pub fn branch_with_emission(
        &self,
        source: &QueueKey,
        branch_def: &QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
        emit_change_records: bool,
    ) -> EngineResult<u64> {
        let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
        if branch == *source {
            return Err(EngineError::Invalid("branch queue must differ from source"));
        }

        // Reject a cut at or below the durable retention floor CLEANLY (bead pqueue-b5cc2bc7): the segments at
        // and below the floor may already be reclaimed, so a branch cut there would surface a later "missing
        // segment" on the branch's first read. Failing fast turns that into an explicit, actionable error.
        if let Some(floor) = self.read_retention_floor(source)?
            && position.sequence <= floor.sequence
        {
            return Err(EngineError::Invalid(
                "branch cut at or below the retention floor: the source segments were reclaimed",
            ));
        }

        self.create_queue(branch_def)?;

        let branch_prefix = shard_prefix(&branch);
        let mut next_index = 0u64;
        let entries = self.read_manifest(source)?;
        for entry in entries {
            if entry.fence {
                if entry.first_seq > position.sequence + 1 {
                    break;
                }
                let mut copied = entry.clone();
                copied.index = next_index;
                let key = format!("{branch_prefix}manifest/{next_index:020}.json");
                self.store_put(&key, &to_json(&copied)?, true)?;
                next_index += 1;
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
            let key = format!("{branch_prefix}manifest/{next_index:020}.json");
            self.store_put(&key, &to_json(&copied)?, true)?;
            next_index += 1;
            if entry.last_seq >= position.sequence {
                break;
            }
        }

        let (next_seq, next_manifest_index, committed_epoch) = self.recover_manifest(&branch)?;
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(&branch).ok_or(EngineError::NotFound)?;
            buf.next_seq = next_seq;
            buf.next_manifest_index = next_manifest_index;
            buf.committed_epoch = committed_epoch;
        }

        let metadata = BranchMetadata {
            source: source.clone(),
            branch: branch.clone(),
            cut_sequence: position.sequence,
            ttl_ms,
            created_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl_ms as i64),
            emit_change_records,
        };
        self.store_put(
            &format!("{branch_prefix}branch.json"),
            &to_json(&metadata)?,
            true,
        )?;
        self.store_put(
            &branch_registry_key(source, &branch),
            &to_json(&metadata)?,
            true,
        )?;

        // Own lease / epoch: the branch gets its own fence entry without mutating the parent queue.
        self.acquire_epoch(&branch, now_ms)
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

    /// Expire parent segments at or before `through_seq`, skipping any segment pinned by a live branch.
    ///
    /// This deletes only segment OBJECTS (`store_delete`, counted in `delete_count`); the manifest entries are
    /// kept as TOMBSTONES that `read_from`/`read_from_limited` skip (their `visible_last_seq < from_seq`). The
    /// tombstones are deliberately NOT deleted here — manifest-tombstone accumulation over a long-lived queue
    /// is risk R5, deferred to a follow-up (compacting the manifest prefix needs its own CAS-safe rewrite so
    /// it does not race the append-only manifest invariant). Trimming the segment objects reclaims the bulk of
    /// the durable bytes; the tombstone JSON is small.
    pub fn expire_segments_through(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        let mut deleted = 0u64;
        for entry in self.read_manifest(source)? {
            if entry.fence {
                continue;
            }
            if Self::visible_last_seq(&entry) > through_seq {
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
            }
        }
        Ok(deleted)
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
    // The retention floor is a SEPARATE small overwrite-able blob (`retention_floor.json`), modeled exactly
    // on the high-water blob above. It records the highest command position whose segment OBJECTS have been
    // trimmed (`expire_segments_through`), as an EXCLUSIVE lower bound — recovery resumes at floor+1, mirroring
    // `recovery_high_water`'s "resume at next_seq" semantics. It is written BEFORE the segment objects are
    // deleted (crash-safe order): a crash after the floor write but before the delete leaves floor=F with some
    // below-F segments still present; recovery reads from F+1 and skips them (no "missing segment" error). The
    // reverse order would leave the floor pointing past a deleted segment. This blob does NOT touch the
    // append-only manifest CAS / epoch-fence invariants — it is an independent overwrite blob.

    /// Read the durable retention-floor blob (`None` if no trim has advanced it yet). Mirrors
    /// [`Self::read_high_water`]. The returned position is the EXCLUSIVE lower bound (last-trimmed seq);
    /// recovery/idempotency folds resume at `sequence + 1`.
    pub fn read_retention_floor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let key = format!("{}retention_floor.json", shard_prefix(shard));
        match self.store_get(&key)? {
            Some(bytes) => {
                let floor: RetentionFloorBlob =
                    serde_json::from_slice(&bytes).map_err(store_err)?;
                Ok(Some(CommandPosition::new(
                    shard.clone(),
                    floor.epoch,
                    floor.seq,
                )))
            }
            None => Ok(None),
        }
    }

    /// Monotonically advance the retention-floor blob to `position` (the trim caller's durable "written first"
    /// step). Mirrors [`Self::set_high_water`]: a position that REGRESSES the stored floor is rejected (the
    /// floor is monotonic — it must never point below already-trimmed segments), an equal one is a no-op, and
    /// an advancing one is persisted.
    pub fn advance_retention_floor(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        if let Some(cur) = self.read_retention_floor(shard)?
            && !cur.precedes(&position)
            && cur != position
        {
            return Err(EngineError::Invalid("retention floor regression"));
        }
        let key = format!("{}retention_floor.json", shard_prefix(shard));
        let blob = RetentionFloorBlob {
            epoch: position.backend_epoch,
            seq: position.sequence,
        };
        self.store_put(&key, &to_json(&blob)?, false)
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

/// Durable retention-floor blob (bounded-recovery segment-object reclamation, bead pqueue-b5cc2bc7): the
/// highest command position whose segment objects have been trimmed, an EXCLUSIVE lower bound (recovery
/// resumes at `seq + 1`). Same shape as [`HighWaterBlob`]; a distinct type so the two blobs never alias.
#[derive(serde::Serialize, serde::Deserialize)]
struct RetentionFloorBlob {
    epoch: u64,
    seq: u64,
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
