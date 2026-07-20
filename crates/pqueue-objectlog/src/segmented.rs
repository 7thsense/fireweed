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

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::object_store_observability::ObservedBlobCall;
pub use crate::segment_integrity::WriterFormat as SegmentWriterFormat;
use crate::segment_integrity::{
    CRC32C_ALGORITHM, ManifestIntegrity, SHA256_ALGORITHM, object_locator,
};
use pqueue_core::QueueDefinition;
use pqueue_engine::{
    CommandEnvelope, CommandPosition, EngineError, EngineResult, QueueCommand, QueueKey,
    validate_gate_command,
};
use sha2::{Digest, Sha256};

use pqueue_engine::sequenced_metadata::{
    AssignmentEpoch, CommandSequence, CreateOnlyPublication, CreateOnlyResolution,
    DeleteThenAdvance, DeletionWatermarkClass, FreeAddress, ManifestHeadClass, ManifestIndex,
    RetainedAddress,
};

// ---------------------------------------------------------------------------
// Object-store seam (the minimal S3 surface the substrate needs)
// ---------------------------------------------------------------------------

/// The minimal S3-compatible object surface the segmented substrate drives. Implemented in-memory (unit
/// tests, no network) and over a real S3 endpoint ([`S3BlobStore`], tested against MinIO).
pub trait BlobStore: Send + Sync {
    /// Declared hard upper bound on hidden physical attempts for each primitive call. Bounded maintenance
    /// fails closed when a provider cannot declare a bound; shipped providers perform exactly one attempt.
    fn max_physical_attempts_per_primitive(&self) -> Option<std::num::NonZeroUsize> {
        match self.backend_kind() {
            crate::object_store_observability::BlobBackendKind::Memory
            | crate::object_store_observability::BlobBackendKind::LocalFs
            | crate::object_store_observability::BlobBackendKind::S3 => {
                std::num::NonZeroUsize::new(1)
            }
            crate::object_store_observability::BlobBackendKind::Other => None,
        }
    }

    fn observed_put(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<()>,
    > {
        self.put(key, body)
            .map(|value| {
                crate::object_store_observability::ObservedBlobCall::new(
                    value,
                    1,
                    body.len() as u64,
                    0,
                )
            })
            .map_err(|error| {
                let mut error =
                    crate::object_store_observability::ClassifiedBlobError::fallback(self, error);
                error.request_bytes = body.len() as u64;
                error
            })
    }

    fn observed_put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<bool>,
    > {
        self.put_if_absent(key, body)
            .map(|value| {
                crate::object_store_observability::ObservedBlobCall::new(
                    value,
                    1,
                    body.len() as u64,
                    0,
                )
            })
            .map_err(|error| {
                let mut error =
                    crate::object_store_observability::ClassifiedBlobError::fallback(self, error);
                error.request_bytes = body.len() as u64;
                error
            })
    }

    fn observed_get(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<Option<Vec<u8>>>,
    > {
        self.get(key)
            .map(|value| {
                let bytes = value.as_ref().map_or(0, |v| v.len() as u64);
                crate::object_store_observability::ObservedBlobCall::new(value, 1, 0, bytes)
            })
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn observed_delete(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<bool>,
    > {
        self.delete(key)
            .map(|value| crate::object_store_observability::ObservedBlobCall::new(value, 1, 0, 0))
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn observed_list_with_request_count(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<Vec<String>>,
    > {
        self.list_with_request_count(prefix)
            .map(|(value, attempts)| {
                crate::object_store_observability::ObservedBlobCall::new(
                    value,
                    attempts.max(1),
                    0,
                    0,
                )
            })
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn observed_list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<Vec<String>>,
    > {
        self.list_from_with_request_count(prefix, start_after)
            .map(|(value, attempts)| {
                crate::object_store_observability::ObservedBlobCall::new(
                    value,
                    attempts.max(1),
                    0,
                    0,
                )
            })
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn observed_list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<Vec<String>>,
    > {
        self.list_page(prefix, start_after, limit)
            .map(|value| {
                crate::object_store_observability::ObservedBlobCall::new(
                    value,
                    u64::from(limit != 0),
                    0,
                    0,
                )
            })
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn observed_stats(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<
        crate::object_store_observability::ObservedBlobCall<ObjectStoreStats>,
    > {
        self.stats(prefix)
            .map(|value| crate::object_store_observability::ObservedBlobCall::new(value, 1, 0, 0))
            .map_err(|error| {
                crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
            })
    }

    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        crate::object_store_observability::BlobBackendKind::Other
    }

    fn instrumentation_depth(&self) -> u8 {
        0
    }

    fn instrumentation_recorder(
        &self,
    ) -> Option<Arc<crate::object_store_observability::BlobMetricsRecorder>> {
        None
    }

    /// Classify a provider error without parsing its display text. Providers with richer native errors
    /// override this at the boundary; the default is a conservative structured projection.
    fn classify_fault(
        &self,
        error: &EngineError,
    ) -> crate::object_store_observability::BlobStoreFault {
        crate::object_store_observability::BlobStoreFault::from_engine_error(error)
    }

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

    /// Return at most `limit` keys after an optional exclusive cursor. Remote stores override this with a
    /// single bounded LIST request so maintenance work cannot accidentally enumerate an unbounded prefix.
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        let mut keys = self.list(prefix)?;
        keys.sort();
        Ok(keys
            .into_iter()
            .filter(|key| start_after.is_none_or(|cursor| key.as_str() > cursor))
            .take(limit)
            .collect())
    }

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

    /// Read the latest versioned manifest-head record under `prefix`.
    ///
    /// The head is modeled as an append-only, versioned series of immutable objects, so readers recover
    /// the current state from the highest numbered version key. The returned `version` is an opaque token
    /// callers can feed back into [`Self::update_manifest_head_if_version`] to conditionally advance the
    /// head without overwriting the previous value.
    fn read_manifest_head(
        &self,
        prefix: &str,
    ) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
        read_manifest_head_via(self, prefix)
    }

    /// Conditionally advance the versioned manifest head.
    ///
    /// The update is linearizable because the next version key is created with the store's existing
    /// create-only CAS primitive. Concurrent writers that race from the same observed `expected_version`
    /// target the same next key; exactly one wins and the previous version remains readable for losers.
    fn update_manifest_head_if_version(
        &self,
        prefix: &str,
        expected_version: Option<u64>,
        value: &ManifestHeadBlob,
    ) -> EngineResult<bool> {
        update_manifest_head_via(self, prefix, expected_version, value)
    }
}

pub(crate) fn read_manifest_head_via<S: BlobStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
    let mut versions = Vec::new();
    for key in store.list(prefix)? {
        let Some(version) = parse_versioned_manifest_head_key(prefix, &key) else {
            continue;
        };
        let Some(bytes) = store.get(&key)? else {
            return Err(EngineError::Conflict);
        };
        let value: ManifestHeadBlob =
            serde_json::from_slice(&bytes).map_err(|_| EngineError::DurableDataCorrupt {
                stage: pqueue_engine::DurableIntegrityStage::Manifest,
                manifest_index: version,
                locator: object_locator(&key),
            })?;
        versions.push(VersionedHead { version, value });
    }
    versions.sort_by_key(|head| head.version);
    if versions
        .iter()
        .enumerate()
        .any(|(expected, head)| head.version != expected as u64)
    {
        return Err(EngineError::Conflict);
    }
    Ok(versions.pop())
}

pub(crate) fn update_manifest_head_via<S: BlobStore + ?Sized>(
    store: &S,
    prefix: &str,
    expected_version: Option<u64>,
    value: &ManifestHeadBlob,
) -> EngineResult<bool> {
    // The immutable successor key IS the compare-and-set location. A caller can only hold version `v`
    // after reading its permanent object; racing writers from `v` both create `v + 1`, so exactly one wins,
    // while a stale writer observes the already-created successor and loses. Re-reading the whole
    // append-only version prefix here made the Nth head update read and parse all N prior heads, turning a
    // sustained segmented append into O(N^2) object reads. Recovery still validates the complete contiguous
    // version chain; the steady-state CAS only verifies its immutable predecessor and creates its successor.
    let next_version = match expected_version {
        Some(version) => {
            let predecessor = versioned_manifest_head_key(prefix, version);
            if store.get(&predecessor)?.is_none() {
                return Ok(false);
            }
            version
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("manifest head version overflow".into()))?
        }
        None => 0,
    };
    let key = versioned_manifest_head_key(prefix, next_version);
    let body = serde_json::to_vec(value).map_err(store_err)?;
    // Unlike immutable data publication, an identical pre-existing successor is still a LOST CAS: another
    // writer advanced from the same observed version and this caller must not acknowledge a second append.
    // Therefore do not collapse an equal body into idempotent success here.
    store.put_if_absent(&key, &body)
}

/// Share one store between several owners (e.g. two competing epoch holders) — delegates through the `Arc`.
impl<T: BlobStore + ?Sized> BlobStore for std::sync::Arc<T> {
    fn observed_put(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<()>> {
        (**self).observed_put(key, body)
    }
    fn observed_put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        (**self).observed_put_if_absent(key, body)
    }
    fn observed_get(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Option<Vec<u8>>>>
    {
        (**self).observed_get(key)
    }
    fn observed_delete(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        (**self).observed_delete(key)
    }
    fn observed_list_with_request_count(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        (**self).observed_list_with_request_count(prefix)
    }
    fn observed_list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        (**self).observed_list_from_with_request_count(prefix, start_after)
    }
    fn observed_list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        (**self).observed_list_page(prefix, start_after, limit)
    }
    fn observed_stats(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<ObjectStoreStats>>
    {
        (**self).observed_stats(prefix)
    }
    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        (**self).backend_kind()
    }

    fn instrumentation_depth(&self) -> u8 {
        (**self).instrumentation_depth()
    }

    fn instrumentation_recorder(
        &self,
    ) -> Option<Arc<crate::object_store_observability::BlobMetricsRecorder>> {
        (**self).instrumentation_recorder()
    }
    fn classify_fault(
        &self,
        error: &EngineError,
    ) -> crate::object_store_observability::BlobStoreFault {
        (**self).classify_fault(error)
    }
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
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        (**self).list_page(prefix, start_after, limit)
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
    fn read_manifest_head(
        &self,
        prefix: &str,
    ) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
        (**self).read_manifest_head(prefix)
    }
    fn update_manifest_head_if_version(
        &self,
        prefix: &str,
        expected_version: Option<u64>,
        value: &ManifestHeadBlob,
    ) -> EngineResult<bool> {
        (**self).update_manifest_head_if_version(prefix, expected_version, value)
    }
}

/// A logical object namespace over a shared blob store. Every key/list cursor is prefixed on the backing
/// store and stripped on return, so independent embedded deployments may safely share one local root or S3
/// bucket without observing or fencing each other's manifests.
pub struct NamespacedBlobStore {
    inner: std::sync::Arc<dyn BlobStore>,
    prefix: String,
}

impl NamespacedBlobStore {
    pub fn new(inner: std::sync::Arc<dyn BlobStore>, namespace: &str) -> EngineResult<Self> {
        let namespace = namespace.trim_matches('/');
        if namespace.is_empty() {
            return Err(EngineError::Invalid("object namespace must not be empty"));
        }
        Ok(Self {
            inner,
            prefix: format!("{namespace}/"),
        })
    }

    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    fn strip_keys(&self, keys: Vec<String>) -> EngineResult<Vec<String>> {
        keys.into_iter()
            .map(|key| {
                key.strip_prefix(&self.prefix)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        EngineError::Storage("blob namespace returned foreign key".into())
                    })
            })
            .collect()
    }
}

impl BlobStore for NamespacedBlobStore {
    fn observed_put(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<()>> {
        self.inner.observed_put(&self.key(key), body)
    }
    fn observed_put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        self.inner.observed_put_if_absent(&self.key(key), body)
    }
    fn observed_get(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Option<Vec<u8>>>>
    {
        self.inner.observed_get(&self.key(key))
    }
    fn observed_delete(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        self.inner.observed_delete(&self.key(key))
    }
    fn observed_list_with_request_count(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        let call = self
            .inner
            .observed_list_with_request_count(&self.key(prefix))?;
        let value = self.strip_keys(call.value).map_err(|error| {
            crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
        })?;
        Ok(ObservedBlobCall::new(
            value,
            call.attempts,
            call.request_bytes,
            call.response_bytes,
        ))
    }
    fn observed_list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        let call = self
            .inner
            .observed_list_from_with_request_count(&self.key(prefix), &self.key(start_after))?;
        let value = self.strip_keys(call.value).map_err(|error| {
            crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
        })?;
        Ok(ObservedBlobCall::new(
            value,
            call.attempts,
            call.request_bytes,
            call.response_bytes,
        ))
    }
    fn observed_list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        let cursor = start_after.map(|value| self.key(value));
        let call = self
            .inner
            .observed_list_page(&self.key(prefix), cursor.as_deref(), limit)?;
        let value = self.strip_keys(call.value).map_err(|error| {
            crate::object_store_observability::ClassifiedBlobError::fallback(self, error)
        })?;
        Ok(ObservedBlobCall::new(
            value,
            call.attempts,
            call.request_bytes,
            call.response_bytes,
        ))
    }
    fn observed_stats(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<ObjectStoreStats>>
    {
        self.inner.observed_stats(&self.key(prefix))
    }
    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        self.inner.backend_kind()
    }

    fn instrumentation_depth(&self) -> u8 {
        self.inner.instrumentation_depth()
    }

    fn instrumentation_recorder(
        &self,
    ) -> Option<Arc<crate::object_store_observability::BlobMetricsRecorder>> {
        self.inner.instrumentation_recorder()
    }
    fn classify_fault(
        &self,
        error: &EngineError,
    ) -> crate::object_store_observability::BlobStoreFault {
        self.inner.classify_fault(error)
    }
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.inner.put(&self.key(key), body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        self.inner.put_if_absent(&self.key(key), body)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        self.inner.get(&self.key(key))
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        self.inner.delete(&self.key(key))
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.strip_keys(self.inner.list(&self.key(prefix))?)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        let cursor = start_after.map(|value| self.key(value));
        self.strip_keys(
            self.inner
                .list_page(&self.key(prefix), cursor.as_deref(), limit)?,
        )
    }

    fn list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        self.strip_keys(
            self.inner
                .list_from(&self.key(prefix), &self.key(start_after))?,
        )
    }

    fn list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> EngineResult<(Vec<String>, u64)> {
        let (keys, requests) = self
            .inner
            .list_from_with_request_count(&self.key(prefix), &self.key(start_after))?;
        Ok((self.strip_keys(keys)?, requests))
    }

    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        let (keys, requests) = self.inner.list_with_request_count(&self.key(prefix))?;
        Ok((self.strip_keys(keys)?, requests))
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(&self.key(prefix))
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
    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        crate::object_store_observability::BlobBackendKind::Memory
    }
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
    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        crate::object_store_observability::BlobBackendKind::LocalFs
    }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentConfig {
    /// Seal when the buffered serialized byte size reaches this value (TD-004 `segment_target_bytes`).
    pub target_bytes: usize,
    /// Seal when the oldest buffered command's age reaches this many ms (TD-004 `segment_max_latency_ms`).
    pub max_latency_ms: u64,
    /// Dev/test escape hatch that allows sealing one command per segment. MUST be false in production
    /// (TD-004 "Reject 1-object-per-command"); a production config seals many commands per segment.
    pub dev_unsafe_one_command_segments: bool,
    /// Durable format emitted by new seals. Readers always accept v2 and v3.
    /// Release N defaults to v2; operators may opt into v3 for soak testing.
    writer_format: SegmentWriterFormat,
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
        if target_bytes > crate::segment_integrity::MAX_SEGMENT_BYTES {
            return Err(EngineError::Invalid(
                "segment_target_bytes exceeds maximum writable segment size",
            ));
        }
        Ok(Self {
            target_bytes,
            max_latency_ms,
            dev_unsafe_one_command_segments: false,
            writer_format: SegmentWriterFormat::V2,
        })
    }

    pub const fn writer_format(&self) -> SegmentWriterFormat {
        self.writer_format
    }

    pub const fn with_writer_format(mut self, writer_format: SegmentWriterFormat) -> Self {
        self.writer_format = writer_format;
        self
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
    /// Physical request payload bytes from the production recorder for an explicitly baselined interval.
    pub request_bytes: u64,
    /// Physical response payload bytes from the production recorder for an explicitly baselined interval.
    pub response_bytes: u64,
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
// A sealed segment object is a small fixed header followed by a length-prefixed concatenation of canonical
// JSON `CommandEnvelope` bytes produced once when buffered. V2 stores `len + JSON` and protects the records
// blob with manifest FNV-1a. V3 stores `len + record CRC32C + JSON`, then a full-frame CRC32C trailer; its
// manifest also carries SHA-256 over the complete stored bytes.
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
/// Parse a sealed-segment object under manifest-authoritative version and integrity metadata, then decode
/// each framed canonical-JSON record. Returns `(epoch, first_seq, commands)`.
fn parse_segment_object(
    bytes: &[u8],
    entry: &ManifestEntry,
    locator: &str,
) -> EngineResult<(u64, u64, Vec<CommandEnvelope>)> {
    let integrity = entry.manifest_integrity()?;
    crate::segment_integrity::decode(bytes, entry.index, locator, &integrity)
}

/// One append-only manifest entry. A data entry names a segment; a `fence` entry records an epoch handoff
/// and names no segment (TD-004 implementation (b): epoch fence published to the manifest before handoff).
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ManifestEntryKind {
    Data,
    Fence,
    RetentionFloor,
    DeletionWatermark,
    Reclaimed,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestEntry {
    index: u64,
    epoch: u64,
    /// Additive discriminator for entries written by current releases. Legacy
    /// v2 entries may omit it and are accepted only when their fields form one
    /// of the exact historical shapes below. V3 data always requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_kind: Option<ManifestEntryKind>,
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
    /// Physical segment epoch. Branch manifests retain the source value even
    /// when their logical authority `epoch` is rewritten.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment_epoch: Option<u64>,
    /// Explicit only for v3 data entries; absence on data means v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment_format: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frame_crc32c: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    record_checksum_algorithm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frame_checksum_algorithm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_hash_algorithm: Option<String>,
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
    /// Durable manifest reclamation marker / deletion watermark marker. Current manifests can carry it as an
    /// append-only, read-only marker entry; legacy manifests deserialize it as `None` and continue to
    /// bootstrap from the compatibility `read_horizon.json` cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compacted_through_index: Option<u64>,
}

impl ManifestEntry {
    fn validate_kind(&self) -> EngineResult<()> {
        let has_integrity = self.checksum != 0
            || self.segment_epoch.is_some()
            || self.segment_format.is_some()
            || self.frame_crc32c.is_some()
            || self.content_sha256.is_some()
            || self.record_checksum_algorithm.is_some()
            || self.frame_checksum_algorithm.is_some()
            || self.content_hash_algorithm.is_some();
        let kind = match self.entry_kind {
            Some(kind) => kind,
            None if self.segment_format.is_some() => {
                return Err(self.corrupt_manifest("v3-entry-kind-required"));
            }
            None if self.segment_key.is_some()
                && !self.fence
                && self.retention_floor_through.is_none()
                && self.compacted_through_index.is_none() =>
            {
                ManifestEntryKind::Data
            }
            None if self.segment_key.is_some()
                && !self.fence
                && self.retention_floor_through.is_none()
                && self.compacted_through_index == Some(self.index) =>
            {
                ManifestEntryKind::Reclaimed
            }
            None if self.segment_key.is_none()
                && self.fence
                && self.retention_floor_through.is_none()
                && self.compacted_through_index.is_none() =>
            {
                ManifestEntryKind::Fence
            }
            None if self.segment_key.is_none()
                && !self.fence
                && self.retention_floor_through.is_some()
                && self.compacted_through_index.is_none() =>
            {
                ManifestEntryKind::RetentionFloor
            }
            None if self.segment_key.is_none()
                && !self.fence
                && self.retention_floor_through.is_none()
                && self.compacted_through_index.is_some() =>
            {
                ManifestEntryKind::DeletionWatermark
            }
            None => return Err(self.corrupt_manifest("invalid-legacy-entry-shape")),
        };
        match kind {
            ManifestEntryKind::Data
                if self.segment_key.is_some()
                    && !self.fence
                    && self.retention_floor_through.is_none()
                    && self.compacted_through_index.is_none()
                    && self.first_seq <= self.last_seq
                    && self.visible_last_seq.is_none_or(|visible| {
                        (self.first_seq..=self.last_seq).contains(&visible)
                    }) =>
            {
                self.manifest_integrity().map(|_| ())
            }
            ManifestEntryKind::Reclaimed
                if self.segment_key.is_some()
                    && !self.fence
                    && self.retention_floor_through.is_none()
                    && self.compacted_through_index == Some(self.index)
                    && self.first_seq <= self.last_seq
                    && self.visible_last_seq.is_none_or(|visible| {
                        (self.first_seq..=self.last_seq).contains(&visible)
                    }) =>
            {
                self.manifest_integrity().map(|_| ())
            }
            ManifestEntryKind::Fence
                if self.segment_key.is_none()
                    && self.fence
                    && self.retention_floor_through.is_none()
                    && self.compacted_through_index.is_none()
                    && self.visible_last_seq.is_none()
                    && self.last_seq == self.first_seq.saturating_sub(1)
                    && !has_integrity =>
            {
                Ok(())
            }
            ManifestEntryKind::RetentionFloor
                if self.segment_key.is_none()
                    && !self.fence
                    && self.retention_floor_through.is_some()
                    && self.compacted_through_index.is_none()
                    && self.visible_last_seq.is_none()
                    && self.first_seq == self.last_seq
                    && self
                        .retention_floor_through
                        .is_some_and(|floor| floor <= self.first_seq)
                    && !has_integrity =>
            {
                Ok(())
            }
            ManifestEntryKind::DeletionWatermark
                if self.segment_key.is_none()
                    && !self.fence
                    && self.retention_floor_through.is_none()
                    && self.compacted_through_index.is_some()
                    && self.visible_last_seq.is_none()
                    && self.first_seq == self.last_seq
                    && !has_integrity =>
            {
                Ok(())
            }
            _ => Err(self.corrupt_manifest("entry-kind-field-mismatch")),
        }
    }

    fn manifest_integrity(&self) -> EngineResult<ManifestIntegrity> {
        if self.segment_key.is_none() {
            if self.segment_format.is_some()
                || self.frame_crc32c.is_some()
                || self.content_sha256.is_some()
                || self.record_checksum_algorithm.is_some()
                || self.frame_checksum_algorithm.is_some()
                || self.content_hash_algorithm.is_some()
            {
                return Err(EngineError::DurableDataCorrupt {
                    stage: pqueue_engine::DurableIntegrityStage::Manifest,
                    manifest_index: self.index,
                    locator: "non-data".to_owned(),
                });
            }
            return Err(EngineError::DurableDataCorrupt {
                stage: pqueue_engine::DurableIntegrityStage::Manifest,
                manifest_index: self.index,
                locator: "non-data".to_owned(),
            });
        }
        match self.segment_format {
            None => {
                if self.frame_crc32c.is_some()
                    || self.content_sha256.is_some()
                    || self.record_checksum_algorithm.is_some()
                    || self.frame_checksum_algorithm.is_some()
                    || self.content_hash_algorithm.is_some()
                {
                    return Err(self.corrupt_manifest("partial-v2-metadata"));
                }
                Ok(ManifestIntegrity::V2 {
                    checksum_fnv1a: self.checksum,
                })
            }
            Some(3)
                if self.record_checksum_algorithm.as_deref() == Some(CRC32C_ALGORITHM)
                    && self.frame_checksum_algorithm.as_deref() == Some(CRC32C_ALGORITHM)
                    && self.content_hash_algorithm.as_deref() == Some(SHA256_ALGORITHM)
                    && self.segment_epoch.is_some()
                    && self.checksum == 0 =>
            {
                Ok(ManifestIntegrity::V3 {
                    frame_crc32c: self
                        .frame_crc32c
                        .ok_or_else(|| self.corrupt_manifest("missing-frame-crc32c"))?,
                    content_sha256: self
                        .content_sha256
                        .clone()
                        .ok_or_else(|| self.corrupt_manifest("missing-content-sha256"))?,
                })
            }
            _ => Err(self.corrupt_manifest("unsupported-or-incomplete-format")),
        }
    }

    fn corrupt_manifest(&self, _detail: &'static str) -> EngineError {
        EngineError::DurableDataCorrupt {
            stage: pqueue_engine::DurableIntegrityStage::Manifest,
            manifest_index: self.index,
            locator: self
                .segment_key
                .as_deref()
                .map(object_locator)
                .unwrap_or_else(|| "non-data".to_owned()),
        }
    }
}

/// Immutable prepared manifest record. It becomes authoritative only when the queue's versioned authority
/// head CAS names its key. The parent link forms the post-migration committed chain; losing candidates are
/// never traversed by recovery or reads.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestCandidate {
    entry: ManifestEntry,
    previous_candidate_key: Option<String>,
    expected_head_version: u64,
}

/// Private proof that every address in the contiguous prefix is safe for deletion-watermark publication.
/// No public constructor exists; only `prove_completed_manifest_deletion_prefix` can mint it after checking
/// segment absence and compatibility/authority metadata.
#[derive(Debug, Clone, Copy)]
struct CompletedManifestDeletionPrefix(ManifestIndex);

/// A manifest entry that is eligible for below-floor reclamation bookkeeping.
///
/// The candidate set is intentionally narrow: only entries strictly below the current durable floor,
/// whose data segment is already reclaimed for the requested pass, and that are not still branch-pinned
/// at the time of enumeration. That gives compaction callers a stable, bounded surface to consume
/// without freeing the manifest address that the write-once CAS fence depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestReclamationCandidate {
    pub index: u64,
    pub first_seq: u64,
    pub segment_key: Option<String>,
    pub retention_floor_through: Option<u64>,
}

/// Visibility decision for a manifest entry during partial-expire enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialExpireVisibility {
    /// Entry must remain visible — above-floor live data, authoritative floor entry, etc.
    Visible,
    /// Entry can be hidden as proven reclaimed by the durable manifest deletion watermark.
    HiddenAsReclaimed,
    /// Entry is below-floor and not yet durably deleted — stops the hidden prefix.
    StopHiddenPrefix,
}

/// Shared pure eligibility result used by partial-expiry visibility, contiguous watermark derivation, and
/// candidate selection. Keeping this classification in one place prevents the three actual metadata walks
/// from drifting on floor/fence/data semantics while leaving branch-pin I/O to their callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReclamationEligibility {
    AuthoritativeFloor,
    Reclaimed,
    Required,
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
    /// Size-bearing cleanup inventory for restart-safe bounded orphan reclamation.
    #[serde(default)]
    object_sizes: BTreeMap<String, u64>,
}

fn manifest_index_from_key(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let base = name.strip_suffix(".json")?;
    let digits = base.strip_suffix("~watermark").unwrap_or(base);
    digits.parse().ok()
}

fn manifest_index_from_any_key(key: &str) -> Option<u64> {
    manifest_index_from_key(key).or_else(|| {
        key.split('/').find_map(|component| {
            let digits = component.strip_prefix('i')?;
            (digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| digits.parse().ok())
                .flatten()
        })
    })
}

fn store_err<E: std::fmt::Display>(e: E) -> EngineError {
    EngineError::Storage(e.to_string())
}

/// Construct the distinct fail-closed deleted-prefix error. Downstream callers (SQLite projection,
/// engine compose recovery) can use this to build or identify the same deleted-manifest-prefix signal.
pub fn deleted_manifest_prefix_error(from_seq: u64, floor_seq: u64) -> EngineError {
    EngineError::Storage(format!(
        "read below retention floor: from_seq {from_seq} <= reclaimed floor {floor_seq} \
         (segments reclaimed; recovery resumes at floor+1)"
    ))
}

/// Returns `true` when `err` is the distinct deleted-manifest-prefix fail-closed signal
/// (as produced by [`fail_closed_below_floor`] / [`deleted_manifest_prefix_error`]).
pub fn is_deleted_manifest_prefix_error(err: &EngineError) -> bool {
    matches!(err, EngineError::Storage(msg) if msg.starts_with("read below retention floor"))
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

fn branch_registry_branch_key(key: &str) -> Option<String> {
    let branch = key.split_once("/branches/")?.1;
    let (tenant_hex, queue_json) = branch.split_once('/')?;
    let queue_hex = queue_json.strip_suffix(".json")?;
    Some(format!("t/{tenant_hex}/q/{queue_hex}/branch.json"))
}

fn branch_segment_key(
    branch: &QueueKey,
    index: u64,
    first_seq: u64,
    content_sha256: Option<&str>,
) -> String {
    let identity = content_sha256.map_or_else(String::new, |digest| format!("-{digest}"));
    format!(
        "{}branch-seg/e{index:020}/s{first_seq:020}{identity}.seg",
        shard_prefix(branch)
    )
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
    /// Kill after the immutable manifest candidate is durable but before the single authoritative-head CAS.
    /// The candidate and its segment are unreferenced and therefore invisible after recovery.
    AfterManifestCandidateBeforeHead,
    /// Pause after the irreversible authority-protocol marker is durable but before the genesis head CAS.
    /// Concurrent data commits must fail closed throughout this initialization window.
    BeforeAuthorityHeadInitialize,
    /// Fail immediately before the conditional authoritative-head update for an already initialized queue.
    /// Used to prove a control-plane lease remains non-serving when storage fencing cannot commit.
    BeforeAuthorityHeadUpdate,
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
    /// Struck INSIDE [`SegmentedObjectLog::gc_orphaned_branches_bounded`] AFTER a branch has been classified as an
    /// orphan (its `branch.json` commit marker was observed ABSENT) but BEFORE its objects are deleted — the
    /// exact instant a concurrent branch creation could commit the marker. A test uses this to deterministically
    /// prove the create/GC guard excludes a concurrent creation (without the guard, GC struck here would go on
    /// to delete a branch that committed during the block).
    GcAfterOrphanClassified,
    /// Struck after one orphan object delete but before sentinel/pin release; used to prove a remote owner
    /// fence preserves both remaining authorities while retaining the partial-effect report.
    GcAfterOrphanObjectDeleted,
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
    /// Per-command canonical-JSON record bytes in arrival order (serialized once here at buffer time). On seal
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
    /// Exact shared-authority head observed during recovery/fencing and advanced only after its immutable
    /// successor CAS wins. Normal single-writer seals use this token instead of re-listing and re-reading the
    /// complete append-only head history. A competing owner still fences this cache at the successor CAS.
    authority_head: Option<VersionedHead<ManifestHeadBlob>>,
    /// Cached durable manifest-deletion watermark (read-horizon). This is a conservative local copy of
    /// the persisted object; the permanent head CAS remains the stale-writer fence.
    manifest_deletion_watermark: Option<u64>,
}

struct Inner {
    shards: BTreeMap<QueueKey, ShardBuf>,
    counters: SegmentCounters,
    object_sizes: BTreeMap<String, u64>,
}

#[derive(Clone)]
struct MaintenanceOwnerToken {
    epoch: u64,
    authority_key: String,
    authority_digest: [u8; 32],
    authority_body: Vec<u8>,
    successor_prefix: String,
}

#[derive(Clone, Default)]
struct SegmentGcProgress {
    target_through: Option<u64>,
    candidate_index: Option<u64>,
    reclaimed_through: Option<u64>,
    branch_cursor: Option<String>,
    max_live_branch_cut: Option<u64>,
    branch_scan_complete: bool,
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

/// One canonical serialized command. Keeping envelope and record together prevents count/order drift across
/// admission, actor submission, and segment framing.
pub struct SerializedCommandEnvelope {
    pub(crate) envelope: CommandEnvelope,
    pub(crate) record: Vec<u8>,
}

impl SerializedCommandEnvelope {
    pub fn new(envelope: CommandEnvelope) -> EngineResult<Self> {
        let record = serde_json::to_vec(&envelope).map_err(store_err)?;
        if record.len() > crate::segment_integrity::MAX_RECORD_BYTES {
            return Err(EngineError::RequestTooLarge {
                requested: record.len(),
                limit: crate::segment_integrity::MAX_RECORD_BYTES,
            });
        }
        Ok(Self { envelope, record })
    }

    pub fn record_len(&self) -> usize {
        self.record.len()
    }

    pub(crate) fn from_parts(envelope: CommandEnvelope, record: Vec<u8>) -> Self {
        Self { envelope, record }
    }

    pub fn into_parts(self) -> (CommandEnvelope, Vec<u8>) {
        (self.envelope, self.record)
    }
}

/// Segmented, group-committing object log over an S3-compatible [`BlobStore`].
pub struct SegmentedObjectLog<S: BlobStore> {
    store: crate::object_store_observability::InstrumentedBlobStore<S>,
    config: SegmentConfig,
    inner: Mutex<Inner>,
    /// Test-only fault-injection hook (TP-003 §3.10 AC-TXN-4). `None` in every production path.
    fault_hook: Mutex<Option<Arc<dyn FaultHook>>>,
    /// CREATE-vs-GC mutual exclusion (bead pqueue-74f03d0e). Branch creation ([`Self::branch_with_emission`])
    /// holds this for its WHOLE duration — every attempt, the commit-marker write, and any rollback — and
    /// orphan GC ([`Self::gc_orphaned_branches_bounded`]) holds it across its WHOLE classify+delete critical section.
    /// So on one log instance (one owner) GC can NEVER observe a branch whose creation is concurrently in
    /// flight: a marker-absent branch seen under this guard is DEFINITIVELY a failed/abandoned creation. This is
    /// a real exclusion (not a timing heuristic) that closes the classify-then-delete TOCTOU vs a marker write.
    /// It is ALWAYS the OUTERMOST lock (taken before `inner`), so it introduces no lock-order inversion.
    create_gc_guard: Mutex<()>,
    candidate_gc_cursors: Mutex<BTreeMap<QueueKey, String>>,
    branch_gc_cursors: Mutex<BTreeMap<QueueKey, String>>,
    segment_gc_cursors: Mutex<BTreeMap<QueueKey, String>>,
    segment_gc_progress: Mutex<BTreeMap<QueueKey, SegmentGcProgress>>,
    /// Epochs this instance completed `acquire_epoch` for. Durable current epoch alone is not proof that this
    /// process is the serving owner, so background maintenance requires this explicit local claim.
    maintenance_owned_epochs: Mutex<BTreeMap<QueueKey, MaintenanceOwnerToken>>,
}

impl<S: BlobStore> SegmentedObjectLog<S> {
    fn decode_manifest_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
        bytes: &[u8],
        index_hint: Option<u64>,
    ) -> EngineResult<T> {
        serde_json::from_slice(bytes).map_err(|_| {
            let error = EngineError::DurableDataCorrupt {
                stage: pqueue_engine::DurableIntegrityStage::Manifest,
                manifest_index: index_hint
                    .or_else(|| manifest_index_from_any_key(key))
                    .unwrap_or(0),
                locator: object_locator(key),
            };
            if self.store.effective_recorder().is_enabled() {
                let result: EngineResult<()> = Err(error.clone());
                self.store
                    .effective_recorder()
                    .record_segment_validation(self.store.backend_kind(), &result);
            }
            error
        })
    }

    fn canonical_v3_segment_keys(
        shard: &QueueKey,
        entry: &ManifestEntry,
    ) -> Option<(String, String)> {
        let digest = entry.content_sha256.as_deref()?;
        let prefix = shard_prefix(shard);
        let segment_epoch = entry.segment_epoch?;
        Some((
            format!(
                "{prefix}seg_candidates/e{segment_epoch:020}/i{:020}/s{:020}-{digest}.seg",
                entry.index, entry.first_seq
            ),
            format!(
                "{prefix}branch-seg/e{:020}/s{:020}-{digest}.seg",
                entry.index, entry.first_seq
            ),
        ))
    }

    fn validate_manifest_entries(
        &self,
        shard: &QueueKey,
        entries: &[ManifestEntry],
    ) -> EngineResult<()> {
        for entry in entries {
            let result = entry.validate_kind().and_then(|()| {
                if let Some((candidate, branch)) = Self::canonical_v3_segment_keys(shard, entry)
                    && entry.segment_key.as_deref() != Some(candidate.as_str())
                    && entry.segment_key.as_deref() != Some(branch.as_str())
                {
                    return Err(EngineError::DurableDataCorrupt {
                        stage: pqueue_engine::DurableIntegrityStage::Manifest,
                        manifest_index: entry.index,
                        locator: object_locator(
                            entry
                                .segment_key
                                .as_deref()
                                .unwrap_or("missing-segment-key"),
                        ),
                    });
                }
                Ok(())
            });
            if result.is_err() {
                if self.store.effective_recorder().is_enabled() {
                    self.store
                        .effective_recorder()
                        .record_segment_validation(self.store.backend_kind(), &result);
                }
                return result;
            }
        }
        Ok(())
    }

    /// Open a segmented object log over `store` with `config`.
    pub fn open(store: S, config: SegmentConfig) -> Self {
        let backend = store.backend_kind();
        Self {
            store: crate::object_store_observability::InstrumentedBlobStore::production(
                store, backend,
            ),
            config,
            inner: Mutex::new(Inner {
                shards: BTreeMap::new(),
                counters: SegmentCounters::default(),
                object_sizes: BTreeMap::new(),
            }),
            fault_hook: Mutex::new(None),
            create_gc_guard: Mutex::new(()),
            candidate_gc_cursors: Mutex::new(BTreeMap::new()),
            branch_gc_cursors: Mutex::new(BTreeMap::new()),
            segment_gc_cursors: Mutex::new(BTreeMap::new()),
            segment_gc_progress: Mutex::new(BTreeMap::new()),
            maintenance_owned_epochs: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn object_store_metrics(&self) -> crate::object_store_observability::BlobMetricsSnapshot {
        self.store.effective_recorder().snapshot()
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

    fn known_object_size(&self, key: &str) -> Option<u64> {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .object_sizes
            .get(key)
            .copied()
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

    fn store_put_segment_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let created = self.store.put_if_absent(key, body)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.counters.put_count += 1;
        if created {
            g.counters.objects_put += 1;
            g.counters.segment_bytes += body.len() as u64;
            Self::register_object(&mut g, key, body.len() as u64);
        }
        Ok(created)
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

    fn authoritative_head_prefix(shard: &QueueKey) -> String {
        format!("{}authority_head/", shard_prefix(shard))
    }

    fn authority_protocol_marker_key(shard: &QueueKey) -> String {
        format!("{}authority_protocol_v1", shard_prefix(shard))
    }

    fn authority_initialized_marker_key(shard: &QueueKey) -> String {
        format!("{}authority_initialized_v1", shard_prefix(shard))
    }

    fn read_authoritative_head(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Option<VersionedHead<ManifestHeadBlob>>> {
        let head = self
            .store
            .read_manifest_head(&Self::authoritative_head_prefix(shard))?;
        let marker_key = Self::authority_protocol_marker_key(shard);
        let marker_present = self.store.get(&marker_key)?.is_some();
        let initialized_key = Self::authority_initialized_marker_key(shard);
        let initialized = self.store.get(&initialized_key)?.is_some();
        match (head, marker_present, initialized) {
            (Some(head), false, _) => {
                // Backfill after a crash between the first head CAS and marker publication. Once present,
                // this immutable marker makes total authority-prefix loss fail closed forever.
                let _ = self
                    .store
                    .put_if_absent(&marker_key, b"authority-head-v1")?;
                let _ = self.store.put_if_absent(&initialized_key, b"initialized")?;
                Ok(Some(head))
            }
            (Some(head), true, false) => {
                let _ = self.store.put_if_absent(&initialized_key, b"initialized")?;
                Ok(Some(head))
            }
            (Some(head), true, true) => Ok(Some(head)),
            // The pre-CAS protocol marker is an irreversible choice of authority protocol. In particular,
            // marker-without-head is NOT legacy/genesis: it is either an initializer in flight or a failed
            // initialization that requires operator recovery. Every other path must fail closed so no legacy
            // append can be acknowledged and then hidden by the first authority head.
            (None, true, _) | (None, false, true) => Err(EngineError::Conflict),
            (None, false, false) => Ok(None),
        }
    }

    fn manifest_candidate_prefix(shard: &QueueKey) -> String {
        format!("{}manifest_candidates/", shard_prefix(shard))
    }

    fn manifest_key(shard: &QueueKey, index: u64) -> String {
        format!("{}{index:020}.json", Self::manifest_prefix(shard))
    }

    fn manifest_head_key(shard: &QueueKey, index: u64) -> String {
        format!("{}{index:020}.json", Self::manifest_head_prefix(shard))
    }

    fn manifest_watermark_head_key(shard: &QueueKey, index: u64) -> String {
        format!(
            "{}{index:020}~watermark.json",
            Self::manifest_head_prefix(shard)
        )
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
        let head_keys = self
            .list_commit_keys_at(&head_prefix, horizon)?
            .into_iter()
            .filter(|key| !key.ends_with("~watermark.json"))
            .collect::<Vec<_>>();
        if head_keys.is_empty() {
            self.list_commit_keys_at(&Self::manifest_prefix(shard), horizon)
        } else {
            Ok(head_keys)
        }
    }

    fn commit_authoritative_entry<F>(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
        count_object_put: bool,
        after_candidate: F,
    ) -> EngineResult<Option<bool>>
    where
        F: FnOnce() -> EngineResult<()>,
    {
        if let Some(head) = self.read_authoritative_head(shard)? {
            return self
                .commit_authoritative_entry_from_head(
                    shard,
                    entry,
                    count_object_put,
                    &head,
                    after_candidate,
                )
                .map(|(won, _)| Some(won));
        }
        Ok(None)
    }

    /// Commit against an already-observed immutable authority head. The returned head is the exact durable
    /// successor when `won` is true. Keeping this seam separate lets the normal seal path reuse its per-shard
    /// recovered/CAS-advanced token while recovery, fencing, and maintenance can retain their fresh-read
    /// behavior.
    fn commit_authoritative_entry_from_head<F>(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
        count_object_put: bool,
        head: &VersionedHead<ManifestHeadBlob>,
        after_candidate: F,
    ) -> EngineResult<(bool, VersionedHead<ManifestHeadBlob>)>
    where
        F: FnOnce() -> EngineResult<()>,
    {
        if entry.index != head.value.next_manifest_index || entry.epoch != head.value.current_epoch
        {
            return Ok((false, head.clone()));
        }
        let candidate = ManifestCandidate {
            entry: entry.clone(),
            previous_candidate_key: head.value.tail_candidate_key.clone(),
            expected_head_version: head.version,
        };
        let candidate_body = to_json(&candidate)?;
        let candidate_digest = hex_lower(&Sha256::digest(&candidate_body));
        let candidate_key = format!(
            "{}e{:020}/i{:020}/{candidate_digest}.json",
            Self::manifest_candidate_prefix(shard),
            entry.epoch,
            entry.index,
        );
        let _ = self.store_put_if_absent(&candidate_key, &candidate_body, count_object_put)?;
        self.fault(FaultCutPoint::AfterManifestCandidateBeforeHead)?;
        after_candidate()?;
        if self.store.get(&candidate_key)?.is_none() {
            // A concurrent fence may have made this candidate a permanent loser and GC may already have
            // removed it while preparation was paused. Never publish a head that names a missing record.
            return Ok((false, head.clone()));
        }
        let next_seq = if entry.fence || entry.retention_floor_through.is_some() {
            entry.first_seq
        } else {
            Self::visible_last_seq(entry) + 1
        };
        let next_head = ManifestHeadBlob {
            current_epoch: entry.epoch,
            next_seq,
            next_manifest_index: entry.index + 1,
            retention_floor_through: entry
                .retention_floor_through
                .or(head.value.retention_floor_through),
            tail_candidate_key: Some(candidate_key),
            legacy_next_manifest_index: head.value.legacy_next_manifest_index,
        };
        let won = self.store.update_manifest_head_if_version(
            &Self::authoritative_head_prefix(shard),
            Some(head.version),
            &next_head,
        )?;
        Ok((
            won,
            VersionedHead {
                version: head.version + 1,
                value: next_head,
            },
        ))
    }

    fn commit_manifest_entry(
        &self,
        shard: &QueueKey,
        index: ManifestIndex,
        epoch: AssignmentEpoch,
        entry: &ManifestEntry,
        count_object_put: bool,
    ) -> EngineResult<bool> {
        if entry.index != index.0 || entry.epoch != epoch.0 {
            return Err(EngineError::Conflict);
        }
        if let Some(won) =
            self.commit_authoritative_entry(shard, entry, count_object_put, || Ok(()))?
        {
            return Ok(won);
        }
        let body = to_json(entry)?;
        let head_key = Self::manifest_head_key(shard, entry.index);
        let resolution = CreateOnlyPublication::<ManifestHeadClass, RetainedAddress>::publish(
            &body,
            || self.store_put_if_absent(&head_key, &body, count_object_put),
            || self.store_get(&head_key),
        )?;
        let won = match resolution {
            resolution if resolution.applied() => true,
            CreateOnlyResolution::PreconditionLost => false,
            CreateOnlyResolution::Ambiguous(source) => return Err(source),
            _ => unreachable!("all applied resolutions handled by the guard"),
        };
        if won {
            let legacy_key = Self::manifest_key(shard, entry.index);
            let _ = self.store_put_if_absent(&legacy_key, &body, false)?;
        }
        Ok(won)
    }

    fn commit_manifest_watermark_marker(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
    ) -> EngineResult<bool> {
        self.commit_manifest_watermark_marker_counted(shard, entry)
            .map(|(applied, _)| applied)
            .map_err(|failure| failure.error)
    }

    fn commit_manifest_watermark_marker_counted(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
    ) -> Result<(bool, usize), crate::maintenance::MaintenanceExecutionFailure> {
        let attempts = std::cell::Cell::new(0usize);
        let body =
            to_json(entry).map_err(|error| crate::maintenance::MaintenanceExecutionFailure {
                effect: crate::maintenance::MaintenanceEffect {
                    objects: 0,
                    bytes: 0,
                    requests: 0,
                },
                fault: None,
                error,
            })?;
        let marker_index = entry.compacted_through_index.unwrap_or(entry.index);
        let head_key = Self::manifest_watermark_head_key(shard, marker_index);
        let resolution = CreateOnlyPublication::<DeletionWatermarkClass, RetainedAddress>::publish(
            &body,
            || {
                attempts.set(attempts.get() + 1);
                self.store_put_if_absent(&head_key, &body, true)
            },
            || {
                attempts.set(attempts.get() + 1);
                self.store_get(&head_key)
            },
        )
        .map_err(|error| crate::maintenance::MaintenanceExecutionFailure {
            effect: crate::maintenance::MaintenanceEffect {
                objects: 0,
                bytes: 0,
                requests: attempts.get(),
            },
            fault: Some(self.store.classify_fault(&error)),
            error,
        })?;
        match resolution {
            resolution if resolution.applied() => Ok((true, attempts.get())),
            CreateOnlyResolution::PreconditionLost => Ok((false, attempts.get())),
            CreateOnlyResolution::Ambiguous(error) => {
                Err(crate::maintenance::MaintenanceExecutionFailure {
                    effect: crate::maintenance::MaintenanceEffect {
                        objects: 0,
                        bytes: 0,
                        requests: attempts.get(),
                    },
                    fault: Some(self.store.classify_fault(&error)),
                    error,
                })
            }
            _ => unreachable!("all applied resolutions handled by the guard"),
        }
    }

    fn mark_manifest_entry_reclaimed(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
    ) -> EngineResult<()> {
        // Overwrite the authoritative head entry with a reclaimed marker so range-list recovery can still
        // reconstruct the live tail from the head namespace after a trim. The legacy compatibility copy is
        // deleted separately once the segment reclaim has fully succeeded.
        let marker = ManifestEntry {
            index: entry.index,
            epoch: entry.epoch,
            entry_kind: Some(ManifestEntryKind::Reclaimed),
            fence: false,
            segment_key: entry.segment_key.clone(),
            first_seq: entry.first_seq,
            last_seq: entry.last_seq,
            visible_last_seq: None,
            checksum: entry.checksum,
            segment_epoch: entry.segment_epoch,
            segment_format: entry.segment_format,
            frame_crc32c: entry.frame_crc32c,
            content_sha256: entry.content_sha256.clone(),
            record_checksum_algorithm: entry.record_checksum_algorithm.clone(),
            frame_checksum_algorithm: entry.frame_checksum_algorithm.clone(),
            content_hash_algorithm: entry.content_hash_algorithm.clone(),
            committed_at_ms: entry.committed_at_ms,
            retention_floor_through: None,
            compacted_through_index: Some(entry.index),
        };
        let head_key = Self::manifest_head_key(shard, entry.index);
        self.store_put(&head_key, &to_json(&marker)?, false)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn delete_manifest_entry(&self, shard: &QueueKey, index: u64) -> EngineResult<()> {
        // Delete the legacy compatibility copy after the authoritative head entry has already been updated
        // to a reclaimed marker. A partial failure here only leaves the compatibility object behind for the
        // next trim pass; it never frees the authoritative head slot.
        let legacy_key = Self::manifest_key(shard, index);
        let _ = self.store_delete(&legacy_key)?;
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
        let buf = self.load_shard_buf(&shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.shards.entry(shard).or_insert(buf);
        Ok(())
    }

    fn visible_last_seq(entry: &ManifestEntry) -> u64 {
        entry.visible_last_seq.unwrap_or(entry.last_seq)
    }

    fn is_reclaimed_manifest_marker(entry: &ManifestEntry) -> bool {
        entry.compacted_through_index.is_some()
    }

    fn classify_reclamation_eligibility(
        entry_first_seq: u64,
        entry_visible_last_seq: u64,
        entry_fence: bool,
        entry_retention_floor_through: Option<u64>,
        floor_seq: Option<u64>,
        reclaimed_through: u64,
    ) -> ReclamationEligibility {
        if entry_retention_floor_through == floor_seq && floor_seq.is_some() {
            return ReclamationEligibility::AuthoritativeFloor;
        }
        let reclaimed = match floor_seq {
            Some(floor) => match entry_retention_floor_through {
                Some(value) => value < floor,
                None if entry_fence => entry_first_seq <= floor,
                None => entry_visible_last_seq <= reclaimed_through,
            },
            None => {
                !entry_fence
                    && entry_retention_floor_through.is_none()
                    && entry_visible_last_seq <= reclaimed_through
            }
        };
        if reclaimed {
            ReclamationEligibility::Reclaimed
        } else {
            ReclamationEligibility::Required
        }
    }

    /// Decide whether a manifest entry is visible during a partial-expire enumeration pass.
    ///
    /// Accepts the durable manifest deletion watermark (`durable_watermark` index W), the caller's
    /// active `reclaimed_through` sequence boundary, and the current `floor_seq`.
    ///
    /// Returns a `PartialExpireVisibility` variant:
    /// - `Visible`: entry above the retention floor (live data), authoritative floor entry, or
    ///   no retention floor is configured — the entry must appear in enumerated output.
    /// - `HiddenAsReclaimed`: entry proven reclaimed by the durable manifest deletion watermark
    ///   and at or below `W` — the entry can be hidden during manifest compaction.
    /// - `StopHiddenPrefix`: entry is below the retention floor and NOT yet durably deleted
    ///   (either not reclaimed at all, or reclaimed but above the durable watermark `W`).
    ///   This entry stops the hidden prefix from extending further in a contiguous prefix scan.
    ///
    /// Below-floor entries whose index is above `W` remain visible even when the caller's
    /// `reclaimed_through` has advanced past their `visible_last_seq` — the durable watermark
    /// is the definitive record of what has been proven reclaimed, and a partial expire must
    /// not hide not-yet-deleted below-floor entries.
    ///
    /// Internal helper for partial-expiry enumeration; exported for fixture-level testing.
    #[allow(clippy::too_many_arguments)]
    pub fn partial_expire_entry_visible(
        entry_index: u64,
        entry_first_seq: u64,
        entry_visible_last_seq: u64,
        entry_fence: bool,
        entry_retention_floor_through: Option<u64>,
        entry_compacted_through_index: Option<u64>,
        durable_watermark: Option<u64>,
        reclaimed_through: u64,
        floor_seq: Option<u64>,
    ) -> PartialExpireVisibility {
        if entry_compacted_through_index.is_some() {
            return PartialExpireVisibility::HiddenAsReclaimed;
        }
        let Some(floor_seq) = floor_seq else {
            return PartialExpireVisibility::Visible;
        };
        let eligibility = Self::classify_reclamation_eligibility(
            entry_first_seq,
            entry_visible_last_seq,
            entry_fence,
            entry_retention_floor_through,
            Some(floor_seq),
            reclaimed_through,
        );
        if eligibility != ReclamationEligibility::Reclaimed {
            if eligibility == ReclamationEligibility::AuthoritativeFloor
                || entry_retention_floor_through.is_some()
                || entry_fence
            {
                return PartialExpireVisibility::Visible;
            }
            if entry_visible_last_seq > floor_seq {
                return PartialExpireVisibility::Visible;
            }
            return PartialExpireVisibility::StopHiddenPrefix;
        }
        if let Some(w) = durable_watermark
            && entry_index <= w
        {
            return PartialExpireVisibility::HiddenAsReclaimed;
        }
        PartialExpireVisibility::StopHiddenPrefix
    }

    fn contiguous_manifest_deletion_watermark_from_entries(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
        entries: &[ManifestEntry],
    ) -> EngineResult<Option<u64>> {
        let Some(floor) = self.read_retention_floor(shard)? else {
            return Ok(None); // no durable floor => no read-horizon
        };
        // Capture the complete live-pin registry once for this watermark proof. Re-reading it for every
        // manifest entry multiplies remote registry I/O by the manifest length and also makes one logical
        // proof observe several different pin states.
        let max_live_branch_cut = self.max_live_branch_cut_snapshot(shard, now_ms)?;
        let mut new_w: Option<u64> = None;
        for entry in entries {
            if entry.compacted_through_index.is_some() {
                continue;
            }
            // STOP at the AUTHORITATIVE floor entry — `read_retention_floor` needs it, so W must stay below it.
            let eligibility = Self::classify_reclamation_eligibility(
                entry.first_seq,
                Self::visible_last_seq(entry),
                entry.fence,
                entry.retention_floor_through,
                Some(floor.sequence),
                reclaimed_through,
            );
            if eligibility == ReclamationEligibility::AuthoritativeFloor {
                break;
            }
            if eligibility != ReclamationEligibility::Reclaimed {
                break; // first LIVE / not-yet-reclaimed / needed entry — W must stay STRICTLY below it
            }
            // The caller's sequence boundary is only a candidate. A standalone or retried watermark pass
            // must prove the data object is physically absent before it can record completed deletion; a
            // stale cached/high claimed boundary can therefore widen work but can never suppress it.
            if let Some(segment_key) = entry.segment_key.as_deref()
                && self.store_get(segment_key)?.is_some()
            {
                break;
            }
            // A still-branch-PINNED below-floor DATA segment has NOT been reclaimed (expire_segments_through
            // skipped its delete): a future trim after the pin releases must still enumerate it, so do NOT
            // hide it behind the horizon. Stop here (keeps W strictly below the pinned index).
            if entry.segment_key.is_some()
                && max_live_branch_cut.is_some_and(|cut| entry.first_seq <= cut)
            {
                break;
            }
            new_w = Some(entry.index);
        }
        Ok(new_w)
    }

    fn prove_completed_manifest_deletion_prefix(
        &self,
        shard: &QueueKey,
        through: ManifestIndex,
        entries: &[ManifestEntry],
        authority_mode: bool,
    ) -> EngineResult<CompletedManifestDeletionPrefix> {
        let targets = entries.iter().filter(|entry| entry.index <= through.0);
        DeleteThenAdvance::<DeletionWatermarkClass, FreeAddress>::delete_all_then_advance(
            targets,
            |entry| {
                if let Some(segment_key) = entry.segment_key.as_deref()
                    && self.store_get(segment_key)?.is_some()
                {
                    return Err(EngineError::Conflict);
                }

                // The legacy compatibility mirror is the freeable metadata address. Delete is idempotent;
                // the retained manifest-head/authority address below remains the stale-writer fence.
                self.delete_manifest_entry(shard, entry.index)?;
                if !authority_mode
                    && self
                        .store_get(&Self::manifest_head_key(shard, entry.index))?
                        .is_none()
                {
                    return Err(EngineError::Conflict);
                }
                Ok(())
            },
            || Ok(()),
        )?;
        Ok(CompletedManifestDeletionPrefix(through))
    }

    /// Read the manifest from the store and derive `(next_seq, next_manifest_index, current_epoch)`.
    ///
    /// HOT PATH: every seal (and epoch fence) calls this to read the authoritative tail. New queues recover
    /// from the permanent `manifest_head/{index:020}.json` namespace when it exists; legacy append-only
    /// queues bootstrap from `manifest/{index:020}.json` when the head is absent. The recovered namespace is
    /// still an append-only, contiguous series keyed by zero-padded index, so the lexicographically-last key
    /// is the tail. Deriving the tuple from only that one entry is exact (indices are contiguous so
    /// `tail.index + 1` is the next index; epoch is monotonically non-decreasing so the tail carries the max;
    /// `next_seq` is the tail's `last_seq + 1`, or for a fence entry — which names no segment — its
    /// `first_seq`, which already records the live next seq). This makes a seal O(1) manifest reads instead
    /// of re-reading + re-parsing the whole O(n) manifest each time (the previous full scan made a sustained
    /// push O(n^2)). `read_all` still does a full scan for recovery.
    fn recover_manifest_from_keys(
        &self,
        keys: Vec<String>,
        has_horizon: bool,
    ) -> EngineResult<(u64, u64, u64, Option<u64>)> {
        let mut keys = keys;
        keys.sort();
        if !has_horizon
            && let Some(first_key) = keys.first()
            && parse_manifest_index_from_key(first_key) != Some(0)
        {
            // A legacy-only prefix that begins above genesis has lost the retained collision history for
            // reclaimed addresses. A compatibility cache is not authority, so fail closed rather than let
            // a stale writer recreate one of those missing create-only addresses.
            return Err(EngineError::Conflict);
        }
        for tail_key in keys.into_iter().rev() {
            let Some(bytes) = self.store_get(&tail_key)? else {
                return Err(EngineError::Conflict);
            };
            let tail: ManifestEntry = self.decode_manifest_json(
                &tail_key,
                &bytes,
                parse_manifest_index_from_key(&tail_key),
            )?;
            if Self::is_reclaimed_manifest_marker(&tail) {
                continue;
            }
            let next_index = tail.index + 1;
            // A fence entry AND a retention-floor-advance entry both name no segment and carry the LIVE
            // next-seq in `first_seq` (they don't add commands), so the tail's next-seq is `first_seq`; a
            // data entry's is `visible_last_seq + 1`.
            let next_seq = if tail.fence || tail.retention_floor_through.is_some() {
                tail.first_seq
            } else {
                Self::visible_last_seq(&tail) + 1
            };
            return Ok((
                next_seq,
                next_index,
                tail.epoch,
                tail.compacted_through_index,
            ));
        }
        if has_horizon {
            Err(EngineError::Conflict)
        } else {
            Ok((0, 0, 0, None))
        }
    }

    fn recover_manifest(&self, shard: &QueueKey) -> EngineResult<(u64, u64, u64, Option<u64>)> {
        if let Some(head) = self.read_authoritative_head(shard)? {
            return Ok((
                head.value.next_seq,
                head.value.next_manifest_index,
                head.value.current_epoch,
                None,
            ));
        }
        // Range-list from the durable read-horizon so recovery enumerates only LIVE (above-floor) entries
        // (bead pqueue-8928baec). The tail is ALWAYS > floor > W, so it is never below the horizon; deriving
        // the tuple from the MAX ranged key is exact. The +1 GET for the horizon here is off the O(1) seal hot
        // path (recover_manifest is reached only on open/acquire/CAS-lost/advance-floor/ensure_shard).
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        let has_horizon = horizon.is_some();
        let keys = match horizon {
            Some(w) => {
                let ranged = self.list_authoritative_manifest_keys_at(shard, Some(w))?;
                // A trimmed queue must still have at least the authoritative floor entry above the horizon.
                // If the live range is unexpectedly empty, a manifest hole was deleted or hidden above the
                // durable floor. Fail closed instead of silently falling back to the reclaimed prefix and
                // treating it as genesis.
                if ranged.is_empty() {
                    return Err(EngineError::Conflict);
                }
                ranged
            }
            None => self.list_authoritative_manifest_keys_at(shard, None)?,
        };
        self.recover_manifest_from_keys(keys, has_horizon)
    }

    fn recover_manifest_without_horizon(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<(u64, u64, u64, Option<u64>)> {
        if let Some(head) = self.read_authoritative_head(shard)? {
            return Ok((
                head.value.next_seq,
                head.value.next_manifest_index,
                head.value.current_epoch,
                None,
            ));
        }
        let keys = self.list_authoritative_manifest_keys_at(shard, None)?;
        self.recover_manifest_from_keys(keys, false)
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
    /// guard decision and the enumeration are consistent under a concurrent trim. A key that is listed but can
    /// no longer be fetched is treated as corruption, not reclaimed history.
    fn read_manifest_at_with_authority(
        &self,
        shard: &QueueKey,
        horizon: Option<u64>,
    ) -> EngineResult<(Vec<ManifestEntry>, bool)> {
        if let Some(head) = self.read_authoritative_head(shard)? {
            let mut entries = Vec::new();
            // The legacy prefix is frozen at migration. Any later fixed-index write from a stale legacy
            // process is excluded by this bound and cannot become visible.
            for key in self.list_manifest_keys_at(shard, horizon)? {
                let Some(bytes) = self.store_get(&key)? else {
                    return Err(EngineError::Conflict);
                };
                let entry: ManifestEntry =
                    self.decode_manifest_json(&key, &bytes, parse_manifest_index_from_key(&key))?;
                if entry.index < head.value.legacy_next_manifest_index {
                    entries.push(entry);
                }
            }
            let mut chain = Vec::new();
            let mut cursor = head.value.tail_candidate_key.clone();
            let live_start_index = horizon
                .map(|index| index.saturating_add(1))
                .unwrap_or(head.value.legacy_next_manifest_index)
                .max(head.value.legacy_next_manifest_index);
            let mut remaining = head
                .value
                .next_manifest_index
                .saturating_sub(live_start_index);
            while remaining > 0 {
                let Some(key) = cursor else {
                    return Err(EngineError::Conflict);
                };
                let Some(bytes) = self.store_get(&key)? else {
                    return Err(EngineError::Conflict);
                };
                let candidate: ManifestCandidate =
                    self.decode_manifest_json(&key, &bytes, manifest_index_from_any_key(&key))?;
                cursor = candidate.previous_candidate_key.clone();
                chain.push(candidate.entry);
                remaining -= 1;
            }
            chain.reverse();
            entries.extend(chain);
            entries.sort_by_key(|entry| entry.index);
            if entries
                .iter()
                .filter(|entry| entry.index >= live_start_index)
                .enumerate()
                .any(|(offset, entry)| entry.index != live_start_index + offset as u64)
            {
                return Err(EngineError::Conflict);
            }
            self.validate_manifest_entries(shard, &entries)?;
            return Ok((entries, true));
        }
        let mut entries = Vec::new();
        for key in self.list_manifest_keys_at(shard, horizon)? {
            let Some(bytes) = self.store_get(&key)? else {
                return Err(EngineError::Conflict);
            };
            let entry: ManifestEntry =
                self.decode_manifest_json(&key, &bytes, parse_manifest_index_from_key(&key))?;
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.index);
        self.validate_manifest_entries(shard, &entries)?;
        Ok((entries, false))
    }

    fn read_manifest_at(
        &self,
        shard: &QueueKey,
        horizon: Option<u64>,
    ) -> EngineResult<Vec<ManifestEntry>> {
        self.read_manifest_at_with_authority(shard, horizon)
            .map(|(entries, _)| entries)
    }

    /// All LIVE manifest entries for `shard`, sorted by index (range-listed from the CURRENT read-horizon).
    /// Every consumer (read_all, read_from_limited, read_retention_floor, expire_segments_through,
    /// lowest_branch_pinned_below, max_trimmable_seq_before, branch copy) folds ONLY live/needed entries:
    /// below-horizon entries are strictly below the epoch-fenced retention floor (reclaimed data tombstones,
    /// superseded floor-advance entries, old fences) that none of them needs (reads resume at floor+1; the
    /// authoritative floor entry and every live/pinned segment are above W). Readers that ALSO fail-closed on
    /// the floor use [`Self::read_manifest_at`] with a pre-captured horizon instead.
    fn read_manifest(&self, shard: &QueueKey) -> EngineResult<Vec<ManifestEntry>> {
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        self.read_manifest_at(shard, horizon)
    }

    fn read_manifest_with_authority(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<(Vec<ManifestEntry>, bool)> {
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        self.read_manifest_at_with_authority(shard, horizon)
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

    /// Bounded proof that the locally acquired owner token still names the latest durable authority head.
    fn maintenance_authority_is_current(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
    ) -> EngineResult<bool> {
        if self.maintenance_owner_epoch(shard) != Some(expected_epoch) {
            return Ok(false);
        }
        self.maintenance_authority_check_counted(shard, expected_epoch)
            .map(|(current, _)| current)
            .map_err(|(error, _, _)| error)
    }

    fn maintenance_authority_check_counted(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
    ) -> Result<
        (bool, usize),
        (
            EngineError,
            crate::object_store_observability::BlobStoreFault,
            usize,
        ),
    > {
        let token = self
            .maintenance_owned_epochs
            .lock()
            .expect("maintenance owner epochs poisoned")
            .get(shard)
            .filter(|token| token.epoch == expected_epoch)
            .cloned()
            .ok_or_else(|| {
                let error = EngineError::EpochFenced;
                (
                    error,
                    self.store.classify_fault(&EngineError::EpochFenced),
                    0,
                )
            })?;
        let (body, get_attempts) = match self.store.observed_get(&token.authority_key) {
            Ok(call) => match call.value {
                Some(body) => (body, call.attempts as usize),
                None => return Ok((false, call.attempts as usize)),
            },
            Err(error) => {
                return Err((error.outward, error.fault, error.attempts as usize));
            }
        };
        if Sha256::digest(&body).as_slice() != token.authority_digest {
            return Ok((false, get_attempts));
        }
        match self
            .store
            .observed_list_page(&token.successor_prefix, Some(&token.authority_key), 1)
        {
            Ok(call) => Ok((call.value.is_empty(), get_attempts + call.attempts as usize)),
            Err(error) => Err((
                error.outward,
                error.fault,
                get_attempts + error.attempts as usize,
            )),
        }
    }

    pub fn maintenance_owner_epoch(&self, shard: &QueueKey) -> Option<u64> {
        self.maintenance_owned_epochs
            .lock()
            .expect("maintenance owner epochs poisoned")
            .get(shard)
            .map(|token| token.epoch)
    }

    fn refresh_maintenance_authority_token(
        &self,
        shard: &QueueKey,
        epoch: u64,
    ) -> EngineResult<()> {
        let authority_head = self.read_authoritative_head(shard)?;
        let (authority_key, successor_prefix) = if let Some(head) = authority_head {
            let prefix = Self::authoritative_head_prefix(shard);
            (versioned_manifest_head_key(&prefix, head.version), prefix)
        } else {
            let version = self
                .inner
                .lock()
                .expect("segmented log poisoned")
                .shards
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .next_manifest_index
                .saturating_sub(1);
            let prefix = Self::manifest_head_prefix(shard);
            (versioned_manifest_head_key(&prefix, version), prefix)
        };
        let body = self
            .store_get(&authority_key)?
            .ok_or_else(|| EngineError::Storage("acquired authority token is missing".into()))?;
        let authority_digest: [u8; 32] = Sha256::digest(&body).into();
        self.maintenance_owned_epochs
            .lock()
            .expect("maintenance owner epochs poisoned")
            .insert(
                shard.clone(),
                MaintenanceOwnerToken {
                    epoch,
                    authority_key,
                    authority_digest,
                    authority_body: body,
                    successor_prefix,
                },
            );
        Ok(())
    }

    /// Refresh the local maintenance fence from the exact head this instance just published. The serialized
    /// body is identical to the immutable authority object, so no post-seal prefix scan or GET is needed.
    fn refresh_maintenance_authority_token_from_head(
        &self,
        shard: &QueueKey,
        epoch: u64,
        head: &VersionedHead<ManifestHeadBlob>,
    ) -> EngineResult<()> {
        let prefix = Self::authoritative_head_prefix(shard);
        let authority_key = versioned_manifest_head_key(&prefix, head.version);
        let authority_body = to_json(&head.value)?;
        let authority_digest: [u8; 32] = Sha256::digest(&authority_body).into();
        self.maintenance_owned_epochs
            .lock()
            .expect("maintenance owner epochs poisoned")
            .insert(
                shard.clone(),
                MaintenanceOwnerToken {
                    epoch,
                    authority_key,
                    authority_digest,
                    authority_body,
                    successor_prefix: prefix,
                },
            );
        Ok(())
    }

    fn load_shard_buf(&self, shard: &QueueKey) -> EngineResult<ShardBuf> {
        let (next_seq, next_index, epoch, _) = self.recover_manifest(shard)?;
        let authority_head = self.read_authoritative_head(shard)?;
        // Only append-only marker history is durable fencing authority. The legacy read_horizon blob may be
        // stale or forged high and is never loaded into the seal/recovery fence cache.
        let manifest_deletion_watermark = self.read_manifest_deletion_watermark(shard)?;
        Ok(ShardBuf {
            buffered: Vec::new(),
            buffered_bytes: 0,
            oldest_buffered_ms: None,
            next_seq,
            next_manifest_index: next_index,
            committed_epoch: epoch,
            authority_head,
            manifest_deletion_watermark,
        })
    }

    fn cached_manifest_deletion_watermark(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        Ok({
            let g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get(shard).ok_or(EngineError::NotFound)?;
            buf.manifest_deletion_watermark
        })
    }

    /// The manifest-deletion watermark used by read/recovery visibility. If durable watermark markers
    /// exist, they alone define the visible horizon. The cached `read_horizon.json` blob is intentionally
    /// ignored here: legacy shards can still have physically present below-floor manifests, so cache-only
    /// state must not hide them during recovery or manifest enumeration.
    fn visible_manifest_deletion_watermark(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        self.read_manifest_deletion_watermark(shard)
    }

    /// Acquire the queue at a NEW, strictly-greater epoch by publishing a **fence entry** to the manifest
    /// via the create-only CAS (TD-003 durable-fence-before-use; TD-004 implementation (b)). After it
    /// commits, a prior-epoch writer's next seal observes the higher epoch and self-fences.
    ///
    /// This does not rely on the deferred pqueue-c33c367e owner-fence wiring to bound stale writers inside
    /// retention; the durable head CAS is the fence, and the code only uses that wiring if a later proof
    /// establishes the bounded-window invariant there.
    pub fn acquire_epoch(&self, shard: &QueueKey, now_ms: i64) -> EngineResult<u64> {
        if !self.store.effective_recorder().is_enabled() {
            let result = self.acquire_epoch_inner(shard, now_ms, &mut 0);
            if let Ok(epoch) = &result {
                self.refresh_maintenance_authority_token(shard, *epoch)?;
            }
            return result;
        }
        let started = std::time::Instant::now();
        let mut attempts = 0;
        let result = self.acquire_epoch_inner(shard, now_ms, &mut attempts);
        self.store.effective_recorder().record_protocol(
            crate::object_store_observability::BlobOperation::AcquireEpoch,
            self.store.backend_kind(),
            crate::object_store_observability::BlobObjectClass::ManifestHead,
            attempts,
            started.elapsed(),
            &result,
        );
        if let Ok(epoch) = &result {
            self.refresh_maintenance_authority_token(shard, *epoch)?;
        }
        result
    }

    fn acquire_epoch_inner(
        &self,
        shard: &QueueKey,
        now_ms: i64,
        attempts: &mut u64,
    ) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        if let Some(head) = self.read_authoritative_head(shard)? {
            return self.fence_epoch(shard, head.value.current_epoch + 1, now_ms);
        }
        // Bounded retry against concurrent acquirers (no consensus; the store CAS is the only primitive).
        for _ in 0..16 {
            *attempts += 1;
            let (next_seq, next_index, cur_epoch, _) = self.recover_manifest(shard)?;
            let new_epoch = cur_epoch + 1;
            let entry = ManifestEntry {
                index: next_index,
                epoch: new_epoch,
                entry_kind: Some(ManifestEntryKind::Fence),
                fence: true,
                segment_key: None,
                first_seq: next_seq,
                last_seq: next_seq.saturating_sub(1),
                visible_last_seq: None,
                checksum: 0,
                segment_epoch: None,
                segment_format: None,
                frame_crc32c: None,
                content_sha256: None,
                record_checksum_algorithm: None,
                frame_checksum_algorithm: None,
                content_hash_algorithm: None,
                committed_at_ms: now_ms,
                retention_floor_through: None,
                compacted_through_index: None,
            };
            if self.commit_manifest_entry(
                shard,
                ManifestIndex(entry.index),
                AssignmentEpoch(entry.epoch),
                &entry,
                true,
            )? {
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

    /// Establish or advance the single shared authority head to an exact externally allocated epoch.
    /// Equality is idempotent only after the shared head exists; lower targets are fenced. A caller that
    /// loses a later-target race after its own CAS is rejected by the final version check and cannot treat
    /// the superseded epoch as usable.
    pub fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
        _now_ms: i64,
    ) -> EngineResult<u64> {
        if !self.store.effective_recorder().is_enabled() {
            let result = self.fence_epoch_inner(shard, target_epoch, &mut 0);
            if let Ok(epoch) = result {
                self.refresh_maintenance_authority_token(shard, epoch)?;
            }
            return result;
        }
        let started = std::time::Instant::now();
        let mut attempts = 0;
        let result = self.fence_epoch_inner(shard, target_epoch, &mut attempts);
        self.store.effective_recorder().record_protocol(
            crate::object_store_observability::BlobOperation::FenceEpoch,
            self.store.backend_kind(),
            crate::object_store_observability::BlobObjectClass::ManifestHead,
            attempts,
            started.elapsed(),
            &result,
        );
        if let Ok(epoch) = result {
            self.refresh_maintenance_authority_token(shard, epoch)?;
        }
        result
    }

    fn fence_epoch_inner(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
        attempts: &mut u64,
    ) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        let prefix = Self::authoritative_head_prefix(shard);
        for _ in 0..16 {
            *attempts += 1;
            let head = self.read_authoritative_head(shard)?;
            let (expected_version, next_head) = match head.as_ref() {
                None => {
                    let (next_seq, next_index, legacy_epoch, _) =
                        self.recover_manifest_without_horizon(shard)?;
                    // Adopting an already-written legacy tail cannot be made atomic with a concurrently
                    // running old writer. Fail closed; an offline migration tool can be designed separately.
                    if next_index != 0 || next_seq != 0 || legacy_epoch != 0 {
                        return Err(EngineError::Unavailable);
                    }
                    let _ = self.store.put_if_absent(
                        &Self::authority_protocol_marker_key(shard),
                        b"authority-head-v1",
                    )?;
                    self.fault(FaultCutPoint::BeforeAuthorityHeadInitialize)?;
                    (
                        None,
                        ManifestHeadBlob {
                            current_epoch: target_epoch,
                            next_seq,
                            next_manifest_index: next_index,
                            retention_floor_through: None,
                            tail_candidate_key: None,
                            legacy_next_manifest_index: next_index,
                        },
                    )
                }
                Some(head) if target_epoch < head.value.current_epoch => {
                    return Err(EngineError::EpochFenced);
                }
                Some(head) if target_epoch == head.value.current_epoch => {
                    let observed_version = head.version;
                    self.fault(FaultCutPoint::DuringOwnerReassignment)?;
                    let final_head = self
                        .read_authoritative_head(shard)?
                        .ok_or(EngineError::Conflict)?;
                    if final_head.version != observed_version
                        || final_head.value.current_epoch != target_epoch
                    {
                        return Err(EngineError::EpochFenced);
                    }
                    let mut g = self.inner.lock().expect("segmented log poisoned");
                    let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
                    buf.committed_epoch = target_epoch;
                    buf.next_seq = final_head.value.next_seq;
                    buf.next_manifest_index = final_head.value.next_manifest_index;
                    buf.authority_head = Some(final_head);
                    return Ok(target_epoch);
                }
                Some(head) => {
                    let mut value = head.value.clone();
                    value.current_epoch = target_epoch;
                    (Some(head.version), value)
                }
            };
            if !self
                .fault(FaultCutPoint::BeforeAuthorityHeadUpdate)
                .and_then(|()| {
                    self.store.update_manifest_head_if_version(
                        &prefix,
                        expected_version,
                        &next_head,
                    )
                })?
            {
                continue;
            }
            let won_version = expected_version.map_or(0, |version| version + 1);
            let _ = self.store.put_if_absent(
                &Self::authority_initialized_marker_key(shard),
                b"initialized",
            )?;
            self.fault(FaultCutPoint::DuringOwnerReassignment)?;
            let final_head = self
                .read_authoritative_head(shard)?
                .ok_or(EngineError::Conflict)?;
            if final_head.version != won_version || final_head.value.current_epoch != target_epoch {
                return Err(EngineError::EpochFenced);
            }
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.committed_epoch = target_epoch;
            buf.next_seq = final_head.value.next_seq;
            buf.next_manifest_index = final_head.value.next_manifest_index;
            buf.authority_head = Some(final_head);
            return Ok(target_epoch);
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
        let serialized = commands
            .iter()
            .cloned()
            .map(SerializedCommandEnvelope::new)
            .collect::<EngineResult<Vec<_>>>()?;
        self.enqueue_serialized(shard, serialized, expected_epoch, now_ms)
            .map(|(outcome, _)| outcome)
    }

    /// Buffer envelopes with their canonical, already-serialized records. The async admission path calls
    /// this after charging the exact retained bytes, eliminating both measure-only and seal-time encoding.
    pub fn enqueue_serialized(
        &self,
        shard: &QueueKey,
        commands: Vec<SerializedCommandEnvelope>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<(EnqueueOutcome, Vec<CommandEnvelope>)> {
        // Class ban + gate validation BEFORE buffering (parity with the file reference write path).
        for command in &commands {
            validate_gate_command(false, &command.envelope.command)?;
            if command.record.len() > crate::segment_integrity::MAX_RECORD_BYTES {
                return Err(EngineError::RequestTooLarge {
                    requested: command.record.len(),
                    limit: crate::segment_integrity::MAX_RECORD_BYTES,
                });
            }
            if matches!(command.envelope.command, QueueCommand::ReplacePending(_)) {
                return Err(EngineError::Unavailable);
            }
        }
        let incoming_frame_len = crate::segment_integrity::encoded_len(
            self.config.writer_format(),
            commands.iter().map(|command| command.record.len()),
        )
        .ok_or(EngineError::RequestTooLarge {
            requested: usize::MAX,
            limit: crate::segment_integrity::MAX_SEGMENT_BYTES,
        })?;
        if incoming_frame_len > crate::segment_integrity::MAX_SEGMENT_BYTES
            || commands.len() > crate::segment_integrity::MAX_RECORDS
        {
            return Err(EngineError::RequestTooLarge {
                requested: incoming_frame_len,
                limit: crate::segment_integrity::MAX_SEGMENT_BYTES,
            });
        }
        // A valid request must not become permanently "too large" merely
        // because a prior batch is buffered. Seal that prefix first, keeping
        // its positions ordered ahead of any positions produced below.
        let should_preseal = {
            let g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get(shard).ok_or(EngineError::NotFound)?;
            !buf.buffered.is_empty()
                && crate::segment_integrity::encoded_len(
                    self.config.writer_format(),
                    buf.buffered
                        .iter()
                        .map(|(record, _)| record.len())
                        .chain(commands.iter().map(|command| command.record.len())),
                )
                .is_none_or(|len| len > crate::segment_integrity::MAX_SEGMENT_BYTES)
        };
        let mut presealed = if should_preseal {
            self.seal(shard, expected_epoch, now_ms)?
        } else {
            Vec::new()
        };
        let (should_seal, envelopes) = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            let frame_len = crate::segment_integrity::encoded_len(
                self.config.writer_format(),
                buf.buffered
                    .iter()
                    .map(|(record, _)| record.len())
                    .chain(commands.iter().map(|command| command.record.len())),
            )
            .ok_or(EngineError::RequestTooLarge {
                requested: usize::MAX,
                limit: crate::segment_integrity::MAX_SEGMENT_BYTES,
            })?;
            if frame_len > crate::segment_integrity::MAX_SEGMENT_BYTES
                || buf.buffered.len().saturating_add(commands.len())
                    > crate::segment_integrity::MAX_RECORDS
            {
                return Err(EngineError::Backpressure {
                    resource: "segment frame rollover",
                });
            }
            let mut envelopes = Vec::with_capacity(commands.len());
            for command in commands {
                let (env, bytes) = command.into_parts();
                buf.buffered_bytes += bytes.len();
                // Keep each command's OWN created_at alongside its bytes (bug 1): the seal derives
                // committed_at_ms from the drained batch, so there is no shared running max to race.
                buf.buffered.push((bytes, created_at_ms(&env)));
                buf.oldest_buffered_ms.get_or_insert(now_ms);
                envelopes.push(env);
            }
            let one_command_seal = self.config.dev_unsafe_one_command_segments;
            (
                buf.buffered_bytes >= self.config.target_bytes
                    || (one_command_seal && !buf.buffered.is_empty()),
                envelopes,
            )
        };
        let mut committed = if should_seal {
            self.seal(shard, expected_epoch, now_ms)?
        } else {
            Vec::new()
        };
        presealed.append(&mut committed);
        let pending = self.pending(shard);
        Ok((
            EnqueueOutcome {
                committed: presealed,
                pending,
            },
            envelopes,
        ))
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
        let (cur_seq, cur_index, cur_epoch, authority_head) = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            if expected_epoch != buf.committed_epoch {
                // Fenced: discard the buffer (the commands are unacked; no segment, no manifest entry).
                buf.buffered_bytes = 0;
                buf.oldest_buffered_ms = None;
                return Err(EngineError::EpochFenced);
            }
            (
                buf.next_seq,
                buf.next_manifest_index,
                buf.committed_epoch,
                buf.authority_head.clone(),
            )
        };

        // Reclaim-time fence: if compaction has already advanced the durable read-horizon beyond this
        // cached manifest index, the index was reclaimed and this stale writer must self-fence before any
        // segment PUT. The cached watermark is refreshed on open and after successful trim; the permanent
        // head CAS remains the stale-writer fence (docs/perf/design/manifest-compaction-hotpath.md:359,
        // :365, and pqueue-c33c367e). This is intentionally not a tail-validate/delete-rollback substitute:
        // the rejection happens before the manifest CAS, so a stale writer cannot externally observe an ack
        // and then be "corrected" later by deleting the entry. pqueue-c33c367e is not a dependency here
        // unless a separate bounded-window proof shows that wiring bounds stale writers independently.
        if let Some(horizon) = self.cached_manifest_deletion_watermark(shard)?
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
        let encoded = crate::segment_integrity::encode(
            self.config.writer_format(),
            cur_epoch,
            first_seq,
            &drained_bytes,
        )?;
        let seg_bytes = encoded.bytes;
        let seg_checksum = encoded.legacy_checksum;
        let shared_authority = authority_head.is_some();
        let content_sha256 = encoded
            .content_sha256
            .or_else(|| shared_authority.then(|| hex_lower(&Sha256::digest(&seg_bytes))));
        let seg_key = if shared_authority || self.config.writer_format() == SegmentWriterFormat::V3
        {
            let digest = content_sha256
                .as_deref()
                .expect("shared and v3 segments have content identity");
            format!(
                "{prefix}seg_candidates/e{cur_epoch:020}/i{cur_index:020}/s{first_seq:020}-{digest}.seg"
            )
        } else {
            let attempt = SEGMENT_ATTEMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            format!(
                "{prefix}seg_attempt/e{cur_epoch:020}/i{cur_index:020}/s{first_seq:020}-{pid}-{attempt}.seg"
            )
        };
        if !shared_authority {
            self.store_put_segment(&seg_key, &seg_bytes)?;
            self.fault(FaultCutPoint::AfterSegmentWriteBeforeManifest)?;
        }

        // 4. Commit the manifest entry via the create-only CAS at the next index.
        let entry = ManifestEntry {
            index: cur_index,
            epoch: cur_epoch,
            entry_kind: Some(ManifestEntryKind::Data),
            fence: false,
            segment_key: Some(seg_key.clone()),
            first_seq,
            last_seq,
            visible_last_seq: None,
            checksum: seg_checksum,
            segment_epoch: Some(cur_epoch),
            segment_format: (self.config.writer_format() == SegmentWriterFormat::V3).then_some(3),
            frame_crc32c: encoded.frame_crc32c,
            content_sha256: (self.config.writer_format() == SegmentWriterFormat::V3)
                .then(|| content_sha256.expect("v3 content identity")),
            record_checksum_algorithm: (self.config.writer_format() == SegmentWriterFormat::V3)
                .then(|| CRC32C_ALGORITHM.to_owned()),
            frame_checksum_algorithm: (self.config.writer_format() == SegmentWriterFormat::V3)
                .then(|| CRC32C_ALGORITHM.to_owned()),
            content_hash_algorithm: (self.config.writer_format() == SegmentWriterFormat::V3)
                .then(|| SHA256_ALGORITHM.to_owned()),
            committed_at_ms,
            retention_floor_through: None,
            compacted_through_index: None,
        };
        let (won, next_authority_head) = if let Some(head) = authority_head.as_ref() {
            let (won, next_head) =
                self.commit_authoritative_entry_from_head(shard, &entry, true, head, || {
                    let _ = self.store_put_segment_if_absent(
                        entry.segment_key.as_deref().expect("data entry segment"),
                        &seg_bytes,
                    )?;
                    self.fault(FaultCutPoint::AfterSegmentWriteBeforeManifest)
                })?;
            (won, won.then_some(next_head))
        } else {
            (
                self.commit_manifest_entry(
                    shard,
                    ManifestIndex(entry.index),
                    AssignmentEpoch(entry.epoch),
                    &entry,
                    true,
                )?,
                None,
            )
        };
        if !won {
            // Content-addressed candidates can be shared by an identical winning
            // append. A CAS loser must not delete the key; bounded orphan GC
            // reclaims only candidates proven unreachable from authority.
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
            if let Some(head) = next_authority_head.clone() {
                buf.authority_head = Some(head);
            }
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
        }
        if self.maintenance_owner_epoch(shard) == Some(expected_epoch) {
            if let Some(head) = next_authority_head.as_ref() {
                self.refresh_maintenance_authority_token_from_head(shard, expected_epoch, head)?;
            } else {
                self.refresh_maintenance_authority_token(shard, expected_epoch)?;
            }
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

    /// Reclaim at most `limit` immutable manifest candidates that are no longer needed by the authoritative
    /// manifest. This includes candidates that provably lost their head CAS and winning candidates strictly
    /// below the durable read horizon. The latter are safe to remove because readers stop their backward walk
    /// at the horizon root and therefore never dereference its parent. A rotating cursor keeps every pass
    /// bounded while ensuring an old candidate cannot be hidden forever behind retained winners.
    pub fn gc_unreferenced_candidates(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> EngineResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let Some(_head) = self.read_authoritative_head(shard)? else {
            return Ok(0);
        };
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        let mut reclaimed = 0usize;
        let scan_limit = limit.saturating_mul(16).max(limit);
        let prefix = Self::manifest_candidate_prefix(shard);
        let cursor = self
            .candidate_gc_cursors
            .lock()
            .expect("candidate gc cursors poisoned")
            .get(shard)
            .cloned();
        let mut keys = self
            .store
            .list_page(&prefix, cursor.as_deref(), scan_limit)?;
        if keys.is_empty() && cursor.is_some() {
            keys = self.store.list_page(&prefix, None, scan_limit)?;
        }
        let page_full = keys.len() == scan_limit;
        let mut last_resolved = cursor.clone();
        let mut stopped_unresolved = false;
        let authority = pqueue_engine::MaintenanceAuthoritySnapshot {
            queue: shard.clone(),
            current_epoch: self.current_epoch(shard)?,
            observed_at_ms: i64::MAX,
            retention_may_advance: true,
            complete_frontier_required: false,
            lineage_validated: true,
            committed_snapshot_through: None,
            recovery_window_through: None,
            manifest_tail: pqueue_engine::FrontierRequirement::NotRequired,
            request_ids: pqueue_engine::FrontierRequirement::NotRequired,
            item_keys: pqueue_engine::FrontierRequirement::NotRequired,
            async_projection_through: None,
            in_memory_claim_replay: pqueue_engine::FrontierRequirement::NotRequired,
            durable_floor: None,
            branch_pins: BTreeSet::new(),
        };
        for key in keys {
            if reclaimed == limit {
                stopped_unresolved = true;
                break;
            }
            let Some(bytes) = self.store_get(&key)? else {
                last_resolved = Some(key);
                continue;
            };
            let candidate: ManifestCandidate =
                self.decode_manifest_json(&key, &bytes, manifest_index_from_any_key(&key))?;
            let decision_key = versioned_manifest_head_key(
                &Self::authoritative_head_prefix(shard),
                candidate.expected_head_version + 1,
            );
            let Some(decision_bytes) = self.store_get(&decision_key)? else {
                // Its expected successor version does not exist yet: the candidate may still be in flight.
                stopped_unresolved = true;
                break;
            };
            let decision: ManifestHeadBlob = self.decode_manifest_json(
                &decision_key,
                &decision_bytes,
                Some(candidate.entry.index),
            )?;
            if decision.tail_candidate_key.as_deref() == Some(&key) {
                // A winning candidate remains authoritative, but a durable horizon makes every winner
                // strictly below it unreachable. Preserve the candidate AT the horizon as the live chain's
                // physical root; its now-dangling parent is intentionally never followed.
                if horizon.is_none_or(|horizon| candidate.entry.index >= horizon) {
                    last_resolved = Some(key);
                    continue;
                }
                if self.store_delete(&key)? {
                    reclaimed += 1;
                }
                last_resolved = Some(key);
                continue;
            }
            let orphan = pqueue_engine::MaintenanceCandidate {
                queue: shard.clone(),
                stable_id: key.clone(),
                class: pqueue_engine::MaintenanceObjectClass::OrphanManifestCandidate,
                first_sequence: Some(candidate.entry.first_seq),
                last_sequence: Some(Self::visible_last_seq(&candidate.entry)),
                manifest_index: Some(candidate.entry.index),
                bytes: Some(bytes.len() as u64),
                created_at_ms: candidate.entry.committed_at_ms,
                unreferenced_proven: true,
                loser_proven: true,
            };
            let planned = pqueue_engine::MaintenancePolicy::new(0)
                .plan(
                    &authority,
                    &[orphan],
                    &pqueue_engine::MaintenanceFilter::default(),
                )
                .into_iter()
                .next()
                .expect("one losing candidate");
            if planned.disposition != pqueue_engine::MaintenanceDisposition::Delete {
                stopped_unresolved = true;
                break;
            }
            // Compare against the winner at the exact CAS decision. Identical
            // bodies share a digest key and must be preserved; a distinct key
            // is proven unreachable at these epoch/index/sequence coordinates
            // and can be reclaimed before its only candidate proof is removed.
            let winner_segment_key =
                if let Some(winner_key) = decision.tail_candidate_key.as_deref() {
                    let Some(winner_bytes) = self.store_get(winner_key)? else {
                        stopped_unresolved = true;
                        break;
                    };
                    let winner: ManifestCandidate = self.decode_manifest_json(
                        winner_key,
                        &winner_bytes,
                        manifest_index_from_any_key(winner_key),
                    )?;
                    winner.entry.segment_key
                } else {
                    None
                };
            if candidate.entry.segment_key != winner_segment_key
                && let Some(segment_key) = candidate.entry.segment_key.as_deref()
            {
                let _ = self.store_delete(segment_key)?;
            }
            if self.store_delete(&key)? {
                reclaimed += 1;
            }
            last_resolved = Some(key);
        }
        let mut cursors = self
            .candidate_gc_cursors
            .lock()
            .expect("candidate gc cursors poisoned");
        if page_full || stopped_unresolved {
            if let Some(cursor) = last_resolved {
                cursors.insert(shard.clone(), cursor);
            }
        } else {
            cursors.remove(shard);
        }
        Ok(reclaimed)
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
    /// The distinct fail-closed deleted-prefix signal: returned when a read's `from_seq` dips to or below
    /// the reclaimed retention floor after the manifest deletion watermark has advanced. Callers can
    /// distinguish this from generic storage errors via [`is_deleted_manifest_prefix_error`] or by
    /// constructing/reporting it through [`deleted_manifest_prefix_error`].
    pub fn fail_closed_below_floor(
        &self,
        shard: &QueueKey,
        from_seq: u64,
        horizon: Option<u64>,
    ) -> EngineResult<()> {
        if horizon.is_some()
            && let Some(floor) = self.read_retention_floor(shard)?
            && from_seq <= floor.sequence
        {
            return Err(deleted_manifest_prefix_error(from_seq, floor.sequence));
        }
        Ok(())
    }

    fn decode_manifest_segment(
        &self,
        entry: &ManifestEntry,
        segment_key: &str,
        bytes: &[u8],
        is_committed_branch: bool,
    ) -> EngineResult<(u64, u64, Vec<CommandEnvelope>)> {
        let locator = object_locator(segment_key);
        let decoded = parse_segment_object(bytes, entry, &locator);
        let result = decoded.and_then(|(epoch, first_seq, commands)| {
            // Pre-field v2 branch manifests rewrote the logical epoch and cannot
            // be repaired in place. Only that narrow compatibility case may use
            // the physical header epoch without an explicit segment_epoch.
            let expected_epoch = entry.segment_epoch.or_else(|| {
                (!is_committed_branch || entry.segment_format.is_some()).then_some(entry.epoch)
            });
            let count =
                u64::try_from(commands.len()).map_err(|_| EngineError::DurableDataCorrupt {
                    stage: pqueue_engine::DurableIntegrityStage::Position,
                    manifest_index: entry.index,
                    locator: locator.clone(),
                })?;
            let decoded_last = first_seq.checked_add(count.saturating_sub(1));
            if expected_epoch.is_some_and(|expected| epoch != expected)
                || first_seq != entry.first_seq
                || count == 0
                || decoded_last != Some(entry.last_seq)
            {
                return Err(EngineError::DurableDataCorrupt {
                    stage: pqueue_engine::DurableIntegrityStage::Position,
                    manifest_index: entry.index,
                    locator: locator.clone(),
                });
            }
            Ok((epoch, first_seq, commands))
        });
        if self.store.effective_recorder().is_enabled() {
            self.store
                .effective_recorder()
                .record_segment_validation(self.store.backend_kind(), &result);
        }
        result
    }

    fn validate_live_segment_locator(
        &self,
        shard: &QueueKey,
        entry: &ManifestEntry,
        segment_key: &str,
        is_committed_branch: bool,
    ) -> EngineResult<()> {
        let Some((candidate, branch)) = Self::canonical_v3_segment_keys(shard, entry) else {
            return Ok(());
        };
        let expected = if is_committed_branch {
            branch
        } else {
            candidate
        };
        let result = if segment_key == expected {
            Ok(())
        } else {
            Err(EngineError::DurableDataCorrupt {
                stage: pqueue_engine::DurableIntegrityStage::Manifest,
                manifest_index: entry.index,
                locator: object_locator(segment_key),
            })
        };
        if result.is_err() && self.store.effective_recorder().is_enabled() {
            self.store
                .effective_recorder()
                .record_segment_validation(self.store.backend_kind(), &result);
        }
        result
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
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        // Genesis read: from_seq == 0 dips to/below any floor, so this fails closed on a trimmed queue with a
        // read-horizon (equivalent to today's organic missing-segment error over the reclaimed prefix).
        self.fail_closed_below_floor(shard, 0, horizon)?;
        let entries = self.read_manifest_at(shard, horizon)?;
        let is_committed_branch = self.store_get(&branch_metadata_key(shard))?.is_some();
        let mut out = Vec::new();
        for entry in entries {
            if entry.fence || Self::is_reclaimed_manifest_marker(&entry) {
                continue;
            }
            let Some(seg_key) = entry.segment_key.as_ref() else {
                continue;
            };
            let visible_last_seq = Self::visible_last_seq(&entry);
            self.validate_live_segment_locator(shard, &entry, seg_key, is_committed_branch)?;
            let bytes = self
                .store_get(seg_key)?
                .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
            let (epoch, first_seq, commands) =
                self.decode_manifest_segment(&entry, seg_key, &bytes, is_committed_branch)?;
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
        let horizon = self.visible_manifest_deletion_watermark(shard)?;
        // Fail closed if the requested range dips to/below the reclaimed floor on a range-listed (horizon)
        // queue — the below-floor tombstones are no longer enumerated, so return the same missing-segment
        // Storage error today's full-list read produces rather than a silently-truncated prefix.
        self.fail_closed_below_floor(shard, from_seq, horizon)?;
        let entries = self.read_manifest_at(shard, horizon)?;
        let is_committed_branch = self.store_get(&branch_metadata_key(shard))?.is_some();
        let mut out = Vec::new();
        for entry in entries {
            if entry.fence || Self::is_reclaimed_manifest_marker(&entry) {
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
            self.validate_live_segment_locator(shard, &entry, seg_key, is_committed_branch)?;
            let bytes = self
                .store_get(seg_key)?
                .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
            let (epoch, first_seq, commands) =
                self.decode_manifest_segment(&entry, seg_key, &bytes, is_committed_branch)?;
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

    /// Read every registered source pin (bead pqueue-635500fb). This is the ONLY input the reclamation pin
    /// snapshots use to decide whether a below-floor source object may be deleted, so a
    /// listed-but-unfetchable entry MUST fail closed the same way [`Self::gc_orphaned_branches_bounded`] already does
    /// for its own registry read — silently skipping it here would let `expire_segments_through` and
    /// [`Self::contiguous_manifest_deletion_watermark_from_entries`] treat a still-registered (and possibly
    /// still-readable) branch as unpinned and reclaim an object it may need, on nothing more than a transient
    /// store inconsistency between `list` and `get`.
    fn read_branch_registry(&self, source: &QueueKey) -> EngineResult<Vec<BranchMetadata>> {
        let prefix = format!("{}branches/", shard_prefix(source));
        let mut out = Vec::new();
        for key in self.store_list(&prefix)? {
            let Some(bytes) = self.store_get(&key)? else {
                return Err(EngineError::Storage(format!(
                    "missing branch registry entry {key}"
                )));
            };
            out.push(serde_json::from_slice(&bytes).map_err(store_err)?);
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

    /// Capture the broadest live branch cut in one registry read. A segment is pinned exactly when its first
    /// sequence is at or below this cut, so callers can fold an arbitrary manifest without further registry
    /// I/O while retaining the fail-closed behavior of [`Self::read_branch_registry`].
    fn max_live_branch_cut_snapshot(
        &self,
        source: &QueueKey,
        now_ms: i64,
    ) -> EngineResult<Option<u64>> {
        Ok(self
            .live_branch_registry(source, now_ms)?
            .into_iter()
            .map(|meta| meta.cut_sequence)
            .max())
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
        if !self.store.effective_recorder().is_enabled() {
            return self.branch_with_emission_inner(
                source,
                branch_def,
                position,
                ttl_ms,
                now_ms,
                emit_change_records,
                &mut 0,
            );
        }
        let started = std::time::Instant::now();
        let mut attempts = 0;
        let result = self.branch_with_emission_inner(
            source,
            branch_def,
            position,
            ttl_ms,
            now_ms,
            emit_change_records,
            &mut attempts,
        );
        self.store.effective_recorder().record_protocol(
            crate::object_store_observability::BlobOperation::Branch,
            self.store.backend_kind(),
            crate::object_store_observability::BlobObjectClass::BranchPin,
            attempts,
            started.elapsed(),
            &result,
        );
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn branch_with_emission_inner(
        &self,
        source: &QueueKey,
        branch_def: &QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
        emit_change_records: bool,
        attempts: &mut u64,
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
            *attempts += 1;
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
        *attempts += 1;
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
        // whose live-pin snapshot runs after the pin is published SKIPS the branched range; and
        // (2) VALIDATE-AFTER-COPY: re-read the AUTHORITATIVE (epoch-fenced manifest) floor after copying and, if
        // it MOVED, roll back and fail cleanly (`Conflict`) so a retry re-reads the advanced floor — NEVER
        // leaving a branch that GETs a reclaimed object.
        // SOURCE-OWNERSHIP FENCE (cross-instance superseded-owner safety): snapshot the durable source epoch
        // before copying, then re-read it after the copy and before the final commit marker write. If a newer
        // owner has taken the source in the meantime, the branch commit must fail cleanly (`Conflict`) and
        // roll back the partial branch rather than publishing a branch on a source it no longer owns.
        let (_, _, source_epoch, _) = self.recover_manifest(source)?;
        let mut metadata = BranchMetadata {
            source: source.clone(),
            branch: branch.clone(),
            source_epoch,
            cut_sequence: position.sequence,
            ttl_ms,
            created_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl_ms as i64),
            emit_change_records,
            object_sizes: BTreeMap::new(),
        };
        // (1) Publish the source PIN first (the registry entry reclamation snapshots consult). If THIS fails,
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
        // same routine is what bounded orphan GC reuses so the two stay consistent.
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
            if self.read_authoritative_head(source)?.is_some() {
                self.fence_epoch(&branch, 0, now_ms)?;
            }

            let mut next_index = 0u64;
            // Seed the branch with the INHERITED floor as its FIRST manifest entry, so the branch's effective
            // genesis is `floor + 1`: `read_retention_floor(branch)` returns it and the branch's recovery /
            // read / idempotency folds resume above the trimmed prefix and never GET a reclaimed object.
            if let Some(f) = source_floor {
                let floor_entry = ManifestEntry {
                    index: next_index,
                    epoch: 0,
                    entry_kind: Some(ManifestEntryKind::RetentionFloor),
                    fence: false,
                    segment_key: None,
                    first_seq: f,
                    last_seq: f,
                    visible_last_seq: None,
                    checksum: 0,
                    segment_epoch: None,
                    segment_format: None,
                    frame_crc32c: None,
                    content_sha256: None,
                    record_checksum_algorithm: None,
                    frame_checksum_algorithm: None,
                    content_hash_algorithm: None,
                    committed_at_ms: 0,
                    retention_floor_through: Some(f),
                    compacted_through_index: None,
                };
                if !self.commit_manifest_entry(
                    &branch,
                    ManifestIndex(floor_entry.index),
                    AssignmentEpoch(floor_entry.epoch),
                    &floor_entry,
                    true,
                )? {
                    return Err(EngineError::Conflict);
                }
                next_index += 1;
            }
            let entries = self.read_manifest(source)?;
            for entry in entries {
                if Self::is_reclaimed_manifest_marker(&entry) {
                    continue;
                }
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
                    if self.read_authoritative_head(&branch)?.is_some() {
                        copied.epoch = 0;
                    }
                    if !self.commit_manifest_entry(
                        &branch,
                        ManifestIndex(copied.index),
                        AssignmentEpoch(copied.epoch),
                        &copied,
                        true,
                    )? {
                        return Err(EngineError::Conflict);
                    }
                    next_index += 1;
                    continue;
                }

                // Skip a data segment entirely at/below the source floor — its object is RECLAIMED, so copying
                // the tombstone would make the branch's read GET a deleted object. A straddling segment
                // (visible_last_seq > floor) is retained, but the branch gets its OWN copy of the segment
                // bytes so later source-prefix deletion cannot strand the branch on a deleted source object.
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
                if self.read_authoritative_head(&branch)?.is_some() {
                    copied.epoch = 0;
                }
                if entry.last_seq > position.sequence {
                    copied.visible_last_seq = Some(position.sequence);
                }
                if let Some(seg_key) = entry.segment_key.as_ref() {
                    let branch_seg_key = branch_segment_key(
                        &branch,
                        copied.index,
                        copied.first_seq,
                        copied.content_sha256.as_deref(),
                    );
                    let bytes = self
                        .store_get(seg_key)?
                        .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
                    self.store_put_segment(&branch_seg_key, &bytes)?;
                    copied.segment_key = Some(branch_seg_key);
                }
                if !self.commit_manifest_entry(
                    &branch,
                    ManifestIndex(copied.index),
                    AssignmentEpoch(copied.epoch),
                    &copied,
                    true,
                )? {
                    return Err(EngineError::Conflict);
                }
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

            let (next_seq, next_manifest_index, committed_epoch, _) =
                self.recover_manifest(&branch)?;
            let authority_head = self.read_authoritative_head(&branch)?;
            {
                let mut g = self.inner.lock().expect("segmented log poisoned");
                let buf = g.shards.get_mut(&branch).ok_or(EngineError::NotFound)?;
                buf.next_seq = next_seq;
                buf.next_manifest_index = next_manifest_index;
                buf.committed_epoch = committed_epoch;
                buf.authority_head = authority_head;
            }

            // Own lease / epoch: the branch gets its own fence entry without mutating the parent queue.
            let epoch = self.acquire_epoch(&branch, now_ms)?;
            let (_, _, current_source_epoch, _) = self.recover_manifest(source)?;
            if current_source_epoch != source_epoch {
                return Err(EngineError::Conflict);
            }

            metadata.object_sizes = self
                .inner
                .lock()
                .expect("segmented log poisoned")
                .object_sizes
                .iter()
                .filter(|(key, _)| key.starts_with(&shard_prefix(&branch)))
                .map(|(key, size)| (key.clone(), *size))
                .collect();
            self.store_put(
                &branch_registry_key(source, &branch),
                &to_json(&metadata)?,
                false,
            )?;

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
    /// branch-creation rollback ([`Self::branch_attempt`]) and orphan GC
    /// ([`Self::gc_orphaned_branches_bounded`]) so the
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

    #[allow(clippy::too_many_arguments)]
    fn cleanup_uncommitted_branch_bounded(
        &self,
        source: &QueueKey,
        branch: &QueueKey,
        expected_epoch: u64,
        persisted_sizes: &BTreeMap<String, u64>,
        pin_bytes: u64,
        object_budget: usize,
        byte_budget: u64,
        request_budget: usize,
        page_size: usize,
        deadline: std::time::Instant,
        dry_run: bool,
    ) -> Result<
        (crate::maintenance::MaintenanceEffect, bool),
        crate::maintenance::MaintenanceExecutionFailure,
    > {
        let mut effect = crate::maintenance::MaintenanceEffect {
            objects: 0,
            bytes: 0,
            requests: 0,
        };
        macro_rules! effect_try {
            ($expression:expr) => {{
                if std::time::Instant::now() >= deadline {
                    return Ok((effect, false));
                }
                match $expression {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(crate::maintenance::MaintenanceExecutionFailure {
                            effect,
                            error,
                            fault: None,
                        });
                    }
                }
            }};
        }
        macro_rules! effect_blob_try {
            ($expression:expr) => {{
                if std::time::Instant::now() >= deadline {
                    return Ok((effect, false));
                }
                match $expression {
                    Ok(call) => call.value,
                    Err(error) => {
                        return Err(crate::maintenance::MaintenanceExecutionFailure {
                            effect,
                            error: error.outward,
                            fault: Some(error.fault),
                        });
                    }
                }
            }};
        }
        macro_rules! object_size {
            ($key:expr) => {{
                if let Some(size) = persisted_sizes
                    .get($key)
                    .copied()
                    .or_else(|| self.known_object_size($key))
                {
                    size
                } else {
                    if effect.requests >= request_budget {
                        return Ok((effect, false));
                    }
                    effect.requests += 1;
                    let Some(body) = effect_blob_try!(self.store.observed_get($key)) else {
                        return Ok((effect, false));
                    };
                    body.len() as u64
                }
            }};
        }
        macro_rules! authority_current {
            () => {{
                if request_budget.saturating_sub(effect.requests) < 2 {
                    return Ok((effect, false));
                }
                match self.maintenance_authority_check_counted(source, expected_epoch) {
                    Ok((current, attempts)) => {
                        effect.requests += attempts;
                        current
                    }
                    Err((error, fault, attempts)) => {
                        effect.requests += attempts;
                        return Err(crate::maintenance::MaintenanceExecutionFailure {
                            effect,
                            error,
                            fault: Some(fault),
                        });
                    }
                }
            }};
        }
        if object_budget == 0 || request_budget == 0 {
            return Ok((effect, false));
        }
        let pending = branch_pending_key(branch);
        if dry_run {
            let prefix = shard_prefix(branch);
            let mut cursor: Option<String> = None;
            loop {
                if effect.requests >= request_budget {
                    return Ok((effect, false));
                }
                effect.requests += 1;
                let keys = effect_blob_try!(self.store.observed_list_page(
                    &prefix,
                    cursor.as_deref(),
                    page_size,
                ));
                let page_full = keys.len() == page_size;
                let next = keys.last().cloned();
                for key in keys {
                    if key == pending {
                        continue;
                    }
                    if effect.objects >= object_budget {
                        return Ok((effect, false));
                    }
                    let bytes = object_size!(&key);
                    if effect.bytes.saturating_add(bytes) > byte_budget {
                        return Ok((effect, false));
                    }
                    effect.objects += 1;
                    effect.bytes = effect.bytes.saturating_add(bytes);
                }
                if !page_full {
                    break;
                }
                cursor = next;
            }
            for key in [&pending, &branch_registry_key(source, branch)] {
                if effect.objects >= object_budget {
                    return Ok((effect, false));
                }
                let bytes = object_size!(key);
                if effect.bytes.saturating_add(bytes) > byte_budget {
                    return Ok((effect, false));
                }
                effect.objects += 1;
                effect.bytes = effect.bytes.saturating_add(bytes);
            }
            return Ok((effect, true));
        }
        let prefix = shard_prefix(branch);
        effect.requests += 1;
        let mut keys = effect_blob_try!(self.store.observed_list_page(&prefix, None, page_size));
        if keys.len() == page_size && keys.iter().all(|key| key == &pending) {
            if effect.requests >= request_budget {
                return Ok((effect, false));
            }
            effect.requests += 1;
            keys = effect_blob_try!(self.store.observed_list_page(
                &prefix,
                Some(&pending),
                page_size,
            ));
        }
        let page_full = keys.len() == page_size;
        let mut had_non_pending = false;
        for key in keys {
            if key == pending {
                continue;
            }
            had_non_pending = true;
            if effect.objects >= object_budget {
                return Ok((effect, false));
            }
            let bytes = object_size!(&key);
            if effect.bytes.saturating_add(bytes) > byte_budget {
                return Ok((effect, false));
            }
            if !authority_current!() {
                return Err(crate::maintenance::MaintenanceExecutionFailure {
                    effect,
                    error: EngineError::EpochFenced,
                    fault: None,
                });
            }
            if effect.requests >= request_budget {
                return Ok((effect, false));
            }
            effect.requests += 1;
            if effect_blob_try!(self.store.observed_delete(&key)) {
                effect.objects += 1;
                effect.bytes = effect.bytes.saturating_add(bytes);
                effect_try!(self.fault(FaultCutPoint::GcAfterOrphanObjectDeleted));
            }
        }
        if page_full && had_non_pending {
            return Ok((effect, false));
        }
        // Prove no non-sentinel branch object remains. This extra bounded page avoids releasing the source pin
        // after a short provider page or after deleting only the first page.
        if effect.requests >= request_budget {
            return Ok((effect, false));
        }
        effect.requests += 1;
        let remaining = effect_blob_try!(self.store.observed_list_page(
            &shard_prefix(branch),
            None,
            2,
        ));
        if remaining.iter().any(|key| key != &pending) {
            return Ok((effect, false));
        }
        // Sentinel first, source pin last. If the budget cannot cover both, leave both in place and resume.
        if object_budget.saturating_sub(effect.objects) < 2 {
            return Ok((effect, false));
        }
        let sentinel_bytes = object_size!(&pending);
        if request_budget.saturating_sub(effect.requests) < 6 {
            return Ok((effect, false));
        }
        let pin_key = branch_registry_key(source, branch);
        if effect
            .bytes
            .saturating_add(sentinel_bytes)
            .saturating_add(pin_bytes)
            > byte_budget
        {
            return Ok((effect, false));
        }
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .remove(branch);
        if !authority_current!() {
            return Err(crate::maintenance::MaintenanceExecutionFailure {
                effect,
                error: EngineError::EpochFenced,
                fault: None,
            });
        }
        effect.requests += 1;
        if effect_blob_try!(self.store.observed_delete(&pending)) {
            effect.objects += 1;
            effect.bytes = effect.bytes.saturating_add(sentinel_bytes);
        }
        if !authority_current!() {
            return Err(crate::maintenance::MaintenanceExecutionFailure {
                effect,
                error: EngineError::EpochFenced,
                fault: None,
            });
        }
        effect.requests += 1;
        if effect_blob_try!(self.store.observed_delete(&pin_key)) {
            effect.objects += 1;
            effect.bytes = effect.bytes.saturating_add(pin_bytes);
        }
        Ok((effect, true))
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
    /// and a re-run over an already-cleaned orphan is a clean no-op). Branches own their copied segment OBJECTS,
    /// so cleanup deletes only branch-local manifest/sentinel/queue/segment objects and the source pin — never
    /// the source's segment prefix. Returns the number of orphans reclaimed.
    /// Bounded, resumable orphan-branch maintenance. Discovery uses a soft in-memory cursor; cursor loss or
    /// version mismatch simply rescans. Classification and every delete stay under `create_gc_guard`, and the
    /// authoritative source epoch is checked before discovery and immediately before cleanup. A partially
    /// cleaned branch remains the first unresolved candidate: its sentinel and pin are retained, and the next
    /// pass idempotently resumes it rather than advancing the cursor past it.
    pub fn gc_orphaned_branches_bounded(
        &self,
        source: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
        grace_ms: u64,
        limits: crate::maintenance::MaintenanceLimits,
        dry_run: bool,
    ) -> EngineResult<crate::maintenance::MaintenanceReport> {
        self.gc_orphaned_branches_bounded_filtered(
            source,
            expected_epoch,
            now_ms,
            grace_ms,
            limits,
            &pqueue_engine::MaintenanceFilter::default(),
            dry_run,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn gc_orphaned_branches_bounded_filtered(
        &self,
        source: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
        grace_ms: u64,
        limits: crate::maintenance::MaintenanceLimits,
        filter: &pqueue_engine::MaintenanceFilter,
        dry_run: bool,
    ) -> EngineResult<crate::maintenance::MaintenanceReport> {
        let started = std::time::Instant::now();
        let mut report = crate::maintenance::MaintenanceReport::new(dry_run);
        macro_rules! report_try {
            ($operation:expr) => {
                match $operation {
                    Ok(value) => value,
                    Err(error) => {
                        match error {
                            EngineError::EpochFenced => {
                                report.fenced = true;
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::EpochChanged,
                                );
                            }
                            EngineError::Storage(_) | EngineError::Backpressure { .. } => {
                                let fault = self.store.classify_fault(&error);
                                report.failure_cause = Some(
                                    crate::maintenance::MaintenanceFailureCause::Provider(
                                        fault.result,
                                    ),
                                );
                                if fault.retryable {
                                    report.retryable_failures += 1;
                                    report.stopped_by = Some(
                                        crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                                    );
                                } else {
                                    report.permanent_failures += 1;
                                    report.stopped_by = Some(
                                        crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                                    );
                                }
                            }
                            _ => {
                                report.permanent_failures += 1;
                                report.failure_cause =
                                    Some(crate::maintenance::MaintenanceFailureCause::Internal);
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                                );
                            }
                        }
                        return Ok(report);
                    }
                }
            };
        }
        macro_rules! report_blob_try {
            ($operation:expr) => {
                match $operation {
                    Ok(call) => call.value,
                    Err(error) => {
                        report.failure_cause =
                            Some(crate::maintenance::MaintenanceFailureCause::Provider(
                                error.fault.result,
                            ));
                        if error.fault.retryable {
                            report.retryable_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                            );
                        } else {
                            report.permanent_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                            );
                        }
                        return Ok(report);
                    }
                }
            };
        }
        if self
            .store
            .max_physical_attempts_per_primitive()
            .is_none_or(|attempts| attempts.get() != 1)
        {
            report.permanent_failures = 1;
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
            return Ok(report);
        }
        if self.maintenance_owner_epoch(source) != Some(expected_epoch) {
            report.fenced = true;
            report.stopped_by = Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
            return Ok(report);
        }
        // Exclude concurrent branch creation for the WHOLE classify+delete (see the doc comment + the guard's
        // definition). Outermost lock: taken before any `inner` acquisition, so no lock-order inversion.
        // POISON-TOLERANT: this mutex guards CREATE-vs-GC coordination, not an in-memory invariant, so a panic
        // that unwinds through a creation (or GC) while it holds the guard must NOT wedge all future GC (and
        // creation) forever. Recover the guard from a poisoned lock instead of propagating the panic.
        let _create_guard = self
            .create_gc_guard
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if limits.requests.get().saturating_sub(report.requests) < 2 {
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
            return Ok(report);
        }
        let authority_current =
            match self.maintenance_authority_check_counted(source, expected_epoch) {
                Ok((current, attempts)) => {
                    report.requests += attempts;
                    current
                }
                Err((error, fault, attempts)) => {
                    report.requests += attempts;
                    report.failure_cause = Some(
                        crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                    );
                    if fault.retryable {
                        report.retryable_failures += 1;
                        report.stopped_by =
                            Some(crate::maintenance::MaintenanceExecutionReason::RetryableFailure);
                    } else {
                        report.permanent_failures += 1;
                        report.stopped_by =
                            Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                    }
                    let _ = error;
                    return Ok(report);
                }
            };
        if !authority_current {
            report.fenced = true;
            report.stopped_by = Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
            return Ok(report);
        }
        let prefix = format!("{}branches/", shard_prefix(source));
        let cursor = self
            .branch_gc_cursors
            .lock()
            .expect("branch gc cursors poisoned")
            .get(source)
            .cloned();
        if report.requests >= limits.requests.get() {
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
            return Ok(report);
        }
        report.requests += 1;
        let keys = report_blob_try!(self.store.observed_list_page(
            &prefix,
            cursor.as_deref(),
            limits.page_size.get()
        ));
        let mut last_resolved = cursor;
        for key in keys {
            if report.deleted >= limits.objects.get()
                || report.requests.saturating_add(4) > limits.requests.get()
                || started.elapsed() >= limits.elapsed
            {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                break;
            }
            report.scanned += 1;
            let Some(expected_marker_key) = branch_registry_branch_key(&key) else {
                report.permanent_failures += 1;
                report.failure_cause =
                    Some(crate::maintenance::MaintenanceFailureCause::InvalidInheritanceMetadata);
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                break;
            };
            report.requests += 1;
            let Some(bytes) = report_blob_try!(self.store.observed_get(&key)) else {
                report.permanent_failures += 1;
                report.failure_cause =
                    Some(crate::maintenance::MaintenanceFailureCause::MissingInheritanceMetadata);
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                break;
            };
            let meta: BranchMetadata = match serde_json::from_slice(&bytes) {
                Ok(meta) => meta,
                Err(_) => {
                    report.permanent_failures += 1;
                    report.failure_cause = Some(
                        crate::maintenance::MaintenanceFailureCause::InvalidInheritanceMetadata,
                    );
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                    break;
                }
            };
            if &meta.source != source || branch_metadata_key(&meta.branch) != expected_marker_key {
                report.permanent_failures += 1;
                report.failure_cause =
                    Some(crate::maintenance::MaintenanceFailureCause::InvalidInheritanceMetadata);
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                break;
            }
            let branch = &meta.branch;
            report.requests += 1;
            if report_blob_try!(self.store.observed_get(&expected_marker_key)).is_some() {
                report.retained += 1;
                *report
                    .retained_by_reason
                    .entry(crate::maintenance::MaintenanceExecutionReason::CommittedBranch)
                    .or_default() += 1;
                last_resolved = Some(key);
                continue;
            }
            let authority = pqueue_engine::MaintenanceAuthoritySnapshot {
                queue: source.clone(),
                current_epoch: expected_epoch,
                observed_at_ms: now_ms,
                retention_may_advance: true,
                complete_frontier_required: false,
                lineage_validated: true,
                committed_snapshot_through: None,
                recovery_window_through: None,
                manifest_tail: pqueue_engine::FrontierRequirement::NotRequired,
                request_ids: pqueue_engine::FrontierRequirement::NotRequired,
                item_keys: pqueue_engine::FrontierRequirement::NotRequired,
                async_projection_through: None,
                in_memory_claim_replay: pqueue_engine::FrontierRequirement::NotRequired,
                durable_floor: None,
                branch_pins: BTreeSet::new(),
            };
            let candidate = pqueue_engine::MaintenanceCandidate {
                queue: source.clone(),
                stable_id: key.clone(),
                class: pqueue_engine::MaintenanceObjectClass::OrphanBranch,
                first_sequence: Some(meta.cut_sequence),
                last_sequence: Some(meta.cut_sequence),
                manifest_index: None,
                bytes: Some(bytes.len() as u64),
                created_at_ms: meta.created_at_ms,
                unreferenced_proven: true,
                loser_proven: false,
            };
            let decision = pqueue_engine::MaintenancePolicy::new(grace_ms)
                .plan(&authority, &[candidate], filter)
                .into_iter()
                .next()
                .expect("one orphan candidate");
            if decision.disposition != pqueue_engine::MaintenanceDisposition::Delete {
                report.retained += 1;
                let reason =
                    if decision.reason == pqueue_engine::MaintenanceReason::InFlightWriterGrace {
                        crate::maintenance::MaintenanceExecutionReason::InFlightWriterGrace
                    } else {
                        crate::maintenance::MaintenanceExecutionReason::Filtered
                    };
                *report.retained_by_reason.entry(reason).or_default() += 1;
                last_resolved = Some(key);
                continue;
            }
            if !dry_run {
                if limits.requests.get().saturating_sub(report.requests) < 2 {
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                    break;
                }
                let authority_current =
                    match self.maintenance_authority_check_counted(source, expected_epoch) {
                        Ok((current, attempts)) => {
                            report.requests += attempts;
                            current
                        }
                        Err((error, fault, attempts)) => {
                            report.requests += attempts;
                            report.failure_cause = Some(
                                crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                            );
                            if fault.retryable {
                                report.retryable_failures += 1;
                                report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                            );
                            } else {
                                report.permanent_failures += 1;
                                report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                            );
                            }
                            let _ = error;
                            break;
                        }
                    };
                if !authority_current {
                    report.fenced = true;
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
                    break;
                }
            }
            // Test seam (never armed in production): strike the classify→delete window a concurrent creation's
            // marker write could race, so the create/GC exclusion can be proven deterministically.
            report_try!(self.fault(FaultCutPoint::GcAfterOrphanClassified));
            // ORPHAN: marker absent AND — under the create/GC guard — provably not an in-flight creation, so it
            // is a failed/abandoned attempt. Reclaim ALL its objects and release the source pin. A failing
            // delete surfaces here (`?`) and leaves the remainder for the next pass.
            let remaining_objects = limits.objects.get().saturating_sub(report.deleted);
            let remaining_requests = limits.requests.get().saturating_sub(report.requests);
            let cleanup = self.cleanup_uncommitted_branch_bounded(
                source,
                branch,
                expected_epoch,
                &meta.object_sizes,
                bytes.len() as u64,
                remaining_objects,
                limits.bytes.get().saturating_sub(report.bytes_deleted),
                remaining_requests,
                limits.page_size.get(),
                started + limits.elapsed,
                dry_run,
            );
            let (effect, complete, failure) = match cleanup {
                Ok((effect, complete)) => (effect, complete, None),
                Err(failure) => (failure.effect, false, Some((failure.error, failure.fault))),
            };
            if dry_run {
                report.would_delete += effect.objects;
                report.would_delete_bytes = report.would_delete_bytes.saturating_add(effect.bytes);
            } else {
                report.deleted += effect.objects;
                report.bytes_deleted = report.bytes_deleted.saturating_add(effect.bytes);
            }
            report.requests = report.requests.saturating_add(effect.requests);
            if let Some((error, structured_fault)) = failure {
                match error {
                    EngineError::EpochFenced => {
                        report.fenced = true;
                        report.stopped_by =
                            Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
                    }
                    EngineError::Storage(_) | EngineError::Backpressure { .. } => {
                        let fault =
                            structured_fault.unwrap_or_else(|| self.store.classify_fault(&error));
                        report.failure_cause = Some(
                            crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                        );
                        if fault.retryable {
                            report.retryable_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                            );
                        } else {
                            report.permanent_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                            );
                        }
                    }
                    _ => {
                        report.permanent_failures += 1;
                        report.stopped_by =
                            Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                    }
                }
                break;
            }
            if !complete {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                break;
            }
            if !dry_run {
                report.completed_candidates += 1;
            }
            last_resolved = Some(key);
        }
        if dry_run {
            return Ok(report);
        }
        let mut cursors = self
            .branch_gc_cursors
            .lock()
            .expect("branch gc cursors poisoned");
        if report.stopped_by.is_some() {
            if let Some(cursor) = last_resolved.clone() {
                cursors.insert(source.clone(), cursor.clone());
                report.cursor = Some(crate::maintenance::MaintenanceCursor {
                    version: crate::maintenance::MAINTENANCE_CURSOR_VERSION,
                    resume_after: Some(cursor),
                });
            }
        } else if report.scanned == limits.page_size.get() {
            if let Some(cursor) = last_resolved {
                cursors.insert(source.clone(), cursor.clone());
                report.cursor = Some(crate::maintenance::MaintenanceCursor {
                    version: crate::maintenance::MAINTENANCE_CURSOR_VERSION,
                    resume_after: Some(cursor),
                });
            }
        } else {
            cursors.remove(source);
        }
        Ok(report)
    }

    /// Expire parent segments at or before `through_seq`, skipping any segment pinned by a live branch.
    ///
    /// The segment object is deleted first; once that succeeds, the reclaimed-prefix watermark is advanced
    /// through the newly deleted entry. The legacy compatibility copy is freeable, the retained head or
    /// authority-chain address stays occupied, and the write-once CAS fence is preserved. The
    /// candidate set is still enumerated explicitly so follow-up deletion / watermark beads can consume it
    /// without re-deriving eligibility.
    pub fn expire_segments_through(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        let Some(owner_epoch) = self.maintenance_owner_epoch(source) else {
            return Err(EngineError::EpochFenced);
        };
        if !self.maintenance_authority_is_current(source, owner_epoch)?
            || self.recover_manifest(source)?.2 != owner_epoch
        {
            return Err(EngineError::EpochFenced);
        }
        let horizon_snapshot = self.visible_manifest_deletion_watermark(source)?;
        let (entries, authority_mode) =
            self.read_manifest_at_with_authority(source, horizon_snapshot)?;
        let max_live_branch_cut = self.max_live_branch_cut_snapshot(source, now_ms)?;
        let mut deleted = 0u64;
        let mut reclaimed_through: Option<u64> = None;
        let mut reclaimed_indices = Vec::new();
        let mut error: Option<EngineError> = None;
        for entry in &entries {
            if entry.fence {
                continue;
            }
            if Self::visible_last_seq(entry) > through_seq {
                continue;
            }
            if max_live_branch_cut.is_some_and(|cut| entry.first_seq <= cut) {
                continue;
            }
            if let Some(seg_key) = entry.segment_key.as_ref() {
                if self.maintenance_owner_epoch(source) != Some(owner_epoch)
                    || !self.maintenance_authority_is_current(source, owner_epoch)?
                    || self.recover_manifest(source)?.2 != owner_epoch
                {
                    error = Some(EngineError::EpochFenced);
                    break;
                }
                // Test-only crash seam (never armed in production): a fault here models a process death mid-
                // reclamation, after the durable floor advanced but before this object is deleted.
                if let Err(err) = self.fault(FaultCutPoint::DuringSegmentExpiry) {
                    error = Some(err);
                    break;
                }
                let deleted_now = match self.store_delete(seg_key) {
                    Ok(deleted_now) => deleted_now,
                    Err(err) => {
                        error = Some(err);
                        break;
                    }
                };
                if deleted_now {
                    deleted += 1;
                }
                if !self.maintenance_authority_is_current(source, owner_epoch)? {
                    error = Some(EngineError::EpochFenced);
                    break;
                }
                // Record reclaimed progress only after the object delete succeeds, so a fault can never
                // advance the watermark past an undeleted below-floor entry.
                if let Err(err) = self.mark_manifest_entry_reclaimed(source, entry) {
                    error = Some(err);
                    break;
                }
                if !self.maintenance_authority_is_current(source, owner_epoch)? {
                    error = Some(EngineError::EpochFenced);
                    break;
                }
                if let Err(err) = self.delete_manifest_entry(source, entry.index) {
                    error = Some(err);
                    break;
                }
                reclaimed_indices.push(entry.index);
                reclaimed_through = Some(Self::visible_last_seq(entry));
            }
        }
        // Persist the durable read-horizon only for the longest contiguous prefix we fully reclaimed. A later
        // delete failure must not let the watermark leap over an undeleted manifest entry; at the same time, a
        // partial failure after some successful reclaim work should still durably record the safe prefix so a
        // retry can resume from the last committed boundary.
        //
        // Protocol note: the deferred pqueue-c33c367e owner-fence wiring does not change this watermark path.
        // The permanent head/authority CAS stays the stale-writer fence; freeable legacy mirrors are not
        // authority. The current index-CAS manifest protocol still cannot support
        // delete-only compaction safely; a cheaper delete-only variant would need the post-head-CAS protocol
        // redesign, not this code path.
        if let Some(reclaimed_through) = reclaimed_through {
            let advance_entries = match horizon_snapshot {
                // Capture the scan input before this pass rewrites reclaimed entries. The no-horizon case can
                // reuse the pre-delete snapshot directly; later passes need one entry before the current
                // watermark so the already-reclaimed prefix remains visible to the fold.
                None | Some(0) => entries.clone(),
                Some(w) => self.read_manifest_at(source, Some(w - 1))?,
            };
            let mut advance_entry_map = BTreeMap::new();
            for entry in advance_entries {
                advance_entry_map.insert(entry.index, entry);
            }
            for entry in entries
                .iter()
                .filter(|entry| reclaimed_indices.contains(&entry.index))
            {
                advance_entry_map.insert(entry.index, entry.clone());
            }
            let advance_entries: Vec<_> = advance_entry_map.into_values().collect();
            let new_w = self.contiguous_manifest_deletion_watermark_from_entries(
                source,
                reclaimed_through,
                now_ms,
                &advance_entries,
            )?;
            if let Some(w) = new_w {
                if !self.maintenance_authority_is_current(source, owner_epoch)? {
                    return Err(EngineError::EpochFenced);
                }
                let completed = self.prove_completed_manifest_deletion_prefix(
                    source,
                    ManifestIndex(w),
                    &advance_entries,
                    authority_mode,
                )?;
                self.persist_manifest_deletion_watermark_entry(source, completed, now_ms)?;
                // Once the horizon is durable, old winning candidates are as unreachable as CAS losers.
                // Fold their reclamation into the same bounded, rotating candidate collector. Cleanup is
                // deliberately after watermark persistence: before that point a reader may still traverse
                // the historical parent chain.
                if !self.maintenance_authority_is_current(source, owner_epoch)? {
                    return Err(EngineError::EpochFenced);
                }
                let _ = self.gc_unreferenced_candidates(source, reclaimed_indices.len().max(1))?;
            }
        }
        match error {
            Some(err) => Err(err),
            None => Ok(deleted),
        }
    }

    /// Run one bounded, resumable segment-expiry page. Discovery cursors are soft: loss rescans immutable
    /// entries and idempotent deletes. Every candidate is admitted only when the remaining budget can cover
    /// its exact owner proofs and all destructive calls.
    pub fn expire_segments_through_bounded(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
        limits: crate::maintenance::MaintenanceLimits,
        dry_run: bool,
    ) -> EngineResult<crate::maintenance::MaintenanceReport> {
        let started = std::time::Instant::now();
        let mut report = crate::maintenance::MaintenanceReport::new(dry_run);
        macro_rules! partial_try {
            ($operation:expr) => {
                match $operation {
                    Ok(value) => value,
                    Err(error) => {
                        match error {
                            EngineError::EpochFenced => {
                                report.fenced = true;
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::EpochChanged,
                                );
                            }
                            EngineError::Storage(_) | EngineError::Backpressure { .. } => {
                                let fault = self.store.classify_fault(&error);
                                report.failure_cause = Some(
                                    crate::maintenance::MaintenanceFailureCause::Provider(
                                        fault.result,
                                    ),
                                );
                                if fault.retryable {
                                    report.retryable_failures += 1;
                                    report.stopped_by = Some(
                                        crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                                    );
                                } else {
                                    report.permanent_failures += 1;
                                    report.stopped_by = Some(
                                        crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                                    );
                                }
                            }
                            _ => {
                                report.permanent_failures += 1;
                                report.failure_cause =
                                    Some(crate::maintenance::MaintenanceFailureCause::Internal);
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                                );
                            }
                        }
                        return Ok(report);
                    }
                }
            };
        }
        macro_rules! partial_blob_try {
            ($operation:expr) => {
                match $operation {
                    Ok(call) => call.value,
                    Err(error) => {
                        report.failure_cause =
                            Some(crate::maintenance::MaintenanceFailureCause::Provider(
                                error.fault.result,
                            ));
                        if error.fault.retryable {
                            report.retryable_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                            );
                        } else {
                            report.permanent_failures += 1;
                            report.stopped_by = Some(
                                crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                            );
                        }
                        return Ok(report);
                    }
                }
            };
        }
        let Some(owner_epoch) = self.maintenance_owner_epoch(source) else {
            report.fenced = true;
            report.stopped_by = Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
            return Ok(report);
        };
        if self
            .store
            .max_physical_attempts_per_primitive()
            .is_none_or(|attempts| attempts.get() != 1)
        {
            report.permanent_failures = 1;
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
            return Ok(report);
        }
        let token = self
            .maintenance_owned_epochs
            .lock()
            .expect("maintenance owner epochs poisoned")
            .get(source)
            .filter(|token| token.epoch == owner_epoch)
            .cloned()
            .ok_or(EngineError::EpochFenced)?;
        if limits.requests.get() < 3 {
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
            return Ok(report);
        }
        let (current, attempts) =
            match self.maintenance_authority_check_counted(source, owner_epoch) {
                Ok(value) => value,
                Err((error, fault, attempts)) => {
                    report.requests += attempts;
                    report.failure_cause = Some(
                        crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                    );
                    partial_try!(Err::<(bool, usize), _>(error))
                }
            };
        report.requests += attempts;
        if !current {
            report.fenced = true;
            report.stopped_by = Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
            return Ok(report);
        }

        let head = if token.authority_key.contains("/authority_head/") {
            Some(partial_try!(self.decode_manifest_json::<ManifestHeadBlob>(
                &token.authority_key,
                &token.authority_body,
                None,
            )))
        } else {
            None
        };
        let mut cursor = self
            .segment_gc_cursors
            .lock()
            .expect("segment gc cursors poisoned")
            .get(source)
            .cloned()
            .unwrap_or_else(|| "legacy:".to_owned());
        let mut progress = self
            .segment_gc_progress
            .lock()
            .expect("segment gc progress poisoned")
            .get(source)
            .cloned()
            .unwrap_or_default();
        if progress.target_through != Some(through_seq) {
            cursor = "legacy:".to_owned();
            progress = SegmentGcProgress {
                target_through: Some(through_seq),
                ..SegmentGcProgress::default()
            };
        }
        let page_start_cursor = cursor.clone();
        let mut entries: Vec<(ManifestEntry, Option<String>)> = Vec::new();
        let mut next_cursor = None;
        if let Some(start) = cursor.strip_prefix("legacy:") {
            if report.requests >= limits.requests.get() {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                return Ok(report);
            }
            report.requests += 1;
            let prefix = Self::manifest_head_prefix(source);
            let start_after = if start.is_empty() { None } else { Some(start) };
            let keys = partial_blob_try!(self.store.observed_list_page(
                &prefix,
                start_after,
                limits.page_size.get()
            ));
            let legacy_limit = head
                .as_ref()
                .map(|head| head.legacy_next_manifest_index)
                .unwrap_or(u64::MAX);
            for key in &keys {
                let Some(index) = manifest_index_from_key(key) else {
                    continue;
                };
                if index >= legacy_limit {
                    break;
                }
                if report.requests >= limits.requests.get() {
                    next_cursor = Some(cursor.clone());
                    break;
                }
                report.requests += 1;
                let Some(bytes) = partial_blob_try!(self.store.observed_get(key)) else {
                    report.permanent_failures += 1;
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                    break;
                };
                let resume = Some(format!("legacy:{key}"));
                entries.push((
                    partial_try!(self.decode_manifest_json::<ManifestEntry>(
                        key,
                        &bytes,
                        parse_manifest_index_from_key(key),
                    )),
                    resume.clone(),
                ));
                next_cursor = resume;
            }
            let legacy_done = keys.len() < limits.page_size.get()
                || keys
                    .last()
                    .and_then(|key| manifest_index_from_key(key))
                    .is_some_and(|index| index.saturating_add(1) >= legacy_limit);
            if legacy_done {
                cursor = head
                    .as_ref()
                    .and_then(|head| head.tail_candidate_key.clone())
                    .map_or_else(|| "done".to_owned(), |key| format!("authority:{key}"));
                next_cursor = (cursor != "done").then_some(cursor.clone());
            }
        } else if let Some(mut key) = cursor.strip_prefix("authority:").map(str::to_owned) {
            for _ in 0..limits.page_size.get() {
                if report.requests >= limits.requests.get() {
                    next_cursor = Some(format!("authority:{key}"));
                    break;
                }
                report.requests += 1;
                let Some(bytes) = partial_blob_try!(self.store.observed_get(&key)) else {
                    return Err(EngineError::Conflict);
                };
                let candidate: ManifestCandidate = partial_try!(self.decode_manifest_json(
                    &key,
                    &bytes,
                    manifest_index_from_any_key(&key),
                ));
                let entry = candidate.entry;
                match candidate.previous_candidate_key {
                    Some(previous) => {
                        key = previous;
                        let resume = Some(format!("authority:{key}"));
                        entries.push((entry, resume.clone()));
                        next_cursor = resume;
                    }
                    None => {
                        entries.push((entry, None));
                        next_cursor = None;
                        break;
                    }
                }
            }
        }

        let branch_prefix = format!("{}branches/", shard_prefix(source));
        while !progress.branch_scan_complete {
            if started.elapsed() >= limits.elapsed || report.requests >= limits.requests.get() {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                break;
            }
            report.requests += 1;
            let page = partial_blob_try!(self.store.observed_list_page(
                &branch_prefix,
                progress.branch_cursor.as_deref(),
                limits.page_size.get(),
            ));
            for key in &page {
                if report.requests >= limits.requests.get() {
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                    break;
                }
                report.requests += 1;
                let Some(bytes) = partial_blob_try!(self.store.observed_get(key)) else {
                    report.permanent_failures += 1;
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure);
                    break;
                };
                let meta: BranchMetadata =
                    partial_try!(serde_json::from_slice(&bytes).map_err(store_err));
                if now_ms < meta.expires_at_ms {
                    progress.max_live_branch_cut = Some(
                        progress
                            .max_live_branch_cut
                            .map_or(meta.cut_sequence, |cut| cut.max(meta.cut_sequence)),
                    );
                }
                progress.branch_cursor = Some(key.clone());
            }
            if report.stopped_by.is_some() {
                break;
            }
            if page.len() < limits.page_size.get() {
                progress.branch_scan_complete = true;
            }
        }
        if report.stopped_by.is_some() {
            let mut cursors = self
                .segment_gc_cursors
                .lock()
                .expect("segment gc cursors poisoned");
            cursors.insert(source.clone(), page_start_cursor.clone());
            report.cursor = Some(crate::maintenance::MaintenanceCursor {
                version: crate::maintenance::MAINTENANCE_CURSOR_VERSION,
                resume_after: Some(page_start_cursor),
            });
            self.segment_gc_progress
                .lock()
                .expect("segment gc progress poisoned")
                .insert(source.clone(), progress);
            return Ok(report);
        }
        let mut committed_cursor = Some(page_start_cursor.clone());
        let mut refresh_branch_scan = false;
        for (entry, resume_after_entry) in entries {
            if started.elapsed() >= limits.elapsed {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                break;
            }
            report.scanned += 1;
            if Self::visible_last_seq(&entry) > through_seq {
                report.retained += 1;
                committed_cursor = resume_after_entry;
                refresh_branch_scan = true;
                continue;
            }
            if progress
                .max_live_branch_cut
                .is_some_and(|cut| entry.first_seq <= cut)
            {
                report.retained += 1;
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                refresh_branch_scan = true;
                break;
            }
            if entry.segment_key.is_none() {
                if entry.retention_floor_through.is_none() {
                    progress.candidate_index = Some(
                        progress
                            .candidate_index
                            .map_or(entry.index, |index| index.max(entry.index)),
                    );
                    progress.reclaimed_through = Some(
                        progress
                            .reclaimed_through
                            .map_or(Self::visible_last_seq(&entry), |through| {
                                through.max(Self::visible_last_seq(&entry))
                            }),
                    );
                }
                report.retained += 1;
                committed_cursor = resume_after_entry;
                refresh_branch_scan = true;
                continue;
            }
            let seg_key = entry.segment_key.as_ref().expect("checked segment key");
            let bytes = if let Some(bytes) = self.known_object_size(seg_key) {
                bytes
            } else {
                if limits.requests.get().saturating_sub(report.requests) < 10 {
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                    break;
                }
                report.requests += 1;
                partial_blob_try!(self.store.observed_get(seg_key))
                    .map_or(0, |body| body.len() as u64)
            };
            if report.deleted >= limits.objects.get()
                || report.bytes_deleted.saturating_add(bytes) > limits.bytes.get()
                || limits.requests.get().saturating_sub(report.requests) < 9
            {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                break;
            }
            if dry_run {
                report.would_delete += 1;
                report.would_delete_bytes = report.would_delete_bytes.saturating_add(bytes);
                committed_cursor = resume_after_entry;
                refresh_branch_scan = true;
                continue;
            }
            for phase in 0..3 {
                let (current, attempts) =
                    match self.maintenance_authority_check_counted(source, owner_epoch) {
                        Ok(value) => value,
                        Err((error, fault, attempts)) => {
                            report.requests += attempts;
                            report.failure_cause = Some(
                                crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                            );
                            partial_try!(Err::<(bool, usize), _>(error))
                        }
                    };
                report.requests += attempts;
                if !current {
                    report.fenced = true;
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
                    break;
                }
                report.requests += 1;
                match phase {
                    0 => {
                        partial_try!(self.fault(FaultCutPoint::DuringSegmentExpiry));
                        if partial_blob_try!(self.store.observed_delete(seg_key)) {
                            report.deleted += 1;
                            report.bytes_deleted = report.bytes_deleted.saturating_add(bytes);
                        }
                    }
                    1 => partial_try!(self.mark_manifest_entry_reclaimed(source, &entry)),
                    _ => partial_try!(self.delete_manifest_entry(source, entry.index)),
                }
            }
            if report.fenced {
                break;
            }
            report.completed_candidates += 1;
            progress.candidate_index = Some(
                progress
                    .candidate_index
                    .map_or(entry.index, |index| index.max(entry.index)),
            );
            progress.reclaimed_through = Some(
                progress
                    .reclaimed_through
                    .map_or(Self::visible_last_seq(&entry), |through| {
                        through.max(Self::visible_last_seq(&entry))
                    }),
            );
            committed_cursor = resume_after_entry;
            refresh_branch_scan = true;
        }
        if report.stopped_by.is_none() && next_cursor.is_some() {
            report.stopped_by =
                Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
        }
        if report.stopped_by
            == Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted)
        {
            next_cursor = committed_cursor;
        }
        if report.stopped_by.is_none() && next_cursor.is_none() {
            cursor = "finalize".to_owned();
        }
        if report.stopped_by.is_none()
            && (cursor == "finalize" || page_start_cursor == "finalize")
            && let (Some(index), Some(reclaimed_through)) =
                (progress.candidate_index, progress.reclaimed_through)
        {
            if limits.requests.get().saturating_sub(report.requests) < 5 {
                report.stopped_by =
                    Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted);
                next_cursor = Some("finalize".to_owned());
            } else {
                let (current, attempts) =
                    match self.maintenance_authority_check_counted(source, owner_epoch) {
                        Ok(value) => value,
                        Err((error, fault, attempts)) => {
                            report.requests += attempts;
                            report.failure_cause = Some(
                                crate::maintenance::MaintenanceFailureCause::Provider(fault.result),
                            );
                            partial_try!(Err::<(bool, usize), _>(error))
                        }
                    };
                report.requests += attempts;
                if !current {
                    report.fenced = true;
                    report.stopped_by =
                        Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged);
                } else {
                    match self.persist_bounded_manifest_deletion_watermark(
                        source,
                        index,
                        reclaimed_through,
                        owner_epoch,
                        now_ms,
                    ) {
                        Ok(watermark_requests) => report.requests += watermark_requests,
                        Err(failure) => {
                            report.requests += failure.effect.requests;
                            report.failure_cause = failure.fault.map(|fault| {
                                crate::maintenance::MaintenanceFailureCause::Provider(fault.result)
                            });
                            if failure.fault.is_some_and(|fault| fault.retryable) {
                                report.retryable_failures += 1;
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::RetryableFailure,
                                );
                            } else {
                                report.permanent_failures += 1;
                                report.failure_cause.get_or_insert(
                                    crate::maintenance::MaintenanceFailureCause::Internal,
                                );
                                report.stopped_by = Some(
                                    crate::maintenance::MaintenanceExecutionReason::PermanentFailure,
                                );
                            }
                            next_cursor = Some("finalize".to_owned());
                        }
                    }
                }
            }
        }
        let mut cursors = self
            .segment_gc_cursors
            .lock()
            .expect("segment gc cursors poisoned");
        if let Some(cursor) = next_cursor {
            if cursor != "finalize" && refresh_branch_scan {
                progress.branch_cursor = None;
                progress.max_live_branch_cut = None;
                progress.branch_scan_complete = false;
            }
            cursors.insert(source.clone(), cursor.clone());
            report.cursor = Some(crate::maintenance::MaintenanceCursor {
                version: crate::maintenance::MAINTENANCE_CURSOR_VERSION,
                resume_after: Some(cursor),
            });
        } else {
            cursors.remove(source);
            self.segment_gc_progress
                .lock()
                .expect("segment gc progress poisoned")
                .remove(source);
        }
        if report.cursor.is_some() {
            self.segment_gc_progress
                .lock()
                .expect("segment gc progress poisoned")
                .insert(source.clone(), progress);
        }
        Ok(report)
    }

    /// Production scheduler defaults for one segment-expiry page.
    pub fn expire_segments_through_bounded_default(
        &self,
        source: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<crate::maintenance::MaintenanceReport> {
        self.expire_segments_through_bounded(
            source,
            through_seq,
            now_ms,
            crate::maintenance::MaintenanceLimits::new(
                64,
                64 * 1024 * 1024,
                1_024,
                std::time::Duration::from_millis(50),
                64,
            )?,
            false,
        )
    }

    /// Enumerate the manifest entries that are currently eligible for below-floor reclamation bookkeeping.
    ///
    /// The pass is intentionally bounded to the current live manifest snapshot plus the recent retained
    /// history needed to identify already-reclaimed entries: it only inspects the current manifest range,
    /// then filters to entries that are strictly below the durable floor, already reclaimed by the requested
    /// pass, and not branch-pinned. That keeps the candidate set usable for deletion/watermark follow-up
    /// beads without weakening the write-once manifest fence.
    pub fn manifest_reclamation_candidates(
        &self,
        shard: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<ManifestReclamationCandidate>> {
        let entries = self.read_manifest(shard)?;
        let (candidates, _) = self.manifest_reclamation_candidates_from_entries(
            shard,
            through_seq,
            now_ms,
            &entries,
        )?;
        Ok(candidates)
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
        // Keep the entire fold on one immutable pin view and one remote registry read.
        let max_live_branch_cut = self.max_live_branch_cut_snapshot(source, now_ms)?;
        let mut lowest: Option<u64> = None;
        for entry in self.read_manifest(source)? {
            if entry.fence
                || entry.segment_key.is_none()
                || Self::is_reclaimed_manifest_marker(&entry)
            {
                continue;
            }
            if Self::visible_last_seq(&entry) > through_seq {
                continue;
            }
            if max_live_branch_cut.is_some_and(|cut| entry.first_seq <= cut) {
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

    /// Reconcile legacy release counters to the recorder source of truth for a caller-owned interval.
    /// A scoped recorder is required when multiple logs run concurrently.
    pub fn counters_reconciled_since(
        &self,
        baseline: &crate::object_store_observability::BlobMetricsSnapshot,
    ) -> SegmentCounters {
        let mut counters = self.counters();
        let totals = self
            .object_store_metrics()
            .delta(baseline)
            .physical_totals();
        counters.put_count = totals.puts;
        counters.get_count = totals.gets;
        counters.list_count = totals.lists;
        counters.delete_count = totals.deletes;
        counters.request_bytes = totals.request_bytes;
        counters.response_bytes = totals.response_bytes;
        counters
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

    fn read_horizon_cache(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        match self.store_get(&Self::read_horizon_key(shard))? {
            Some(bytes) => {
                let blob: ReadHorizonBlob = serde_json::from_slice(&bytes).map_err(store_err)?;
                Ok(Some(blob.index))
            }
            None => Ok(None),
        }
    }

    /// Read the durable READ-HORIZON watermark `W` (bead pqueue-8928baec): the highest manifest index below
    /// which every entry is a below-floor entry no reader needs. `None` when no trim has advanced it yet
    /// (backward-compatible: reads then fall back to the full manifest list).
    ///
    /// The `read_horizon.json` blob is a cache; the durable source of truth is the append-only
    /// `manifest_head/*~watermark.json` marker history. Reconstructing from both makes stale/blob-regressing
    /// writers harmless without relying on any owner-fence wiring.
    pub fn read_read_horizon(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        let blob = self.read_horizon_cache(shard)?;
        let durable = self.read_manifest_deletion_watermark(shard)?;
        Ok(match (durable, blob) {
            (Some(durable), Some(blob)) => Some(durable.max(blob)),
            (Some(durable), None) => Some(durable),
            (None, Some(blob)) => Some(blob),
            (None, None) => None,
        })
    }

    /// Read the durable manifest-deletion watermark history only. This is the append-only source of truth
    /// for the highest contiguous reclaimed manifest index; the `read_horizon.json` blob is only the
    /// compatibility cache used to bootstrap legacy shards that do not yet have marker history.
    pub fn read_manifest_deletion_watermark(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        let mut durable: Option<u64> = None;
        let mut saw_marker = false;
        for key in self.store_list(&Self::manifest_head_prefix(shard))? {
            if !key.ends_with("~watermark.json") {
                continue;
            }
            let Some(bytes) = self.store_get(&key)? else {
                continue;
            };
            let marker: ManifestEntry =
                self.decode_manifest_json(&key, &bytes, manifest_index_from_any_key(&key))?;
            if let Some(index) = marker.compacted_through_index {
                saw_marker = true;
                durable = Some(durable.map_or(index, |cur| cur.max(index)));
            }
        }
        Ok(if saw_marker { durable } else { None })
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
        if self.maintenance_owner_epoch(shard) != Some(expected_epoch)
            || !self.maintenance_authority_is_current(shard, expected_epoch)?
        {
            return Err(EngineError::EpochFenced);
        }
        // Floor advancement is not allowed to treat the deletion watermark as an authority. It still prefers
        // the ranged recovery path for the current tail, but if that range is unexpectedly empty it falls
        // back to a full manifest scan so a stale/corrupt read-horizon cannot block progress.
        let (cur_seq, cur_index, cur_epoch, _) = match self.recover_manifest(shard) {
            Ok(tuple) => tuple,
            Err(EngineError::Conflict) => self.recover_manifest_without_horizon(shard)?,
            Err(err) => return Err(err),
        };
        let cur_seq = CommandSequence(cur_seq);
        let cur_index = ManifestIndex(cur_index);
        let cur_epoch = AssignmentEpoch(cur_epoch);
        if cur_epoch.0 > expected_epoch {
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
            index: cur_index.0,
            epoch: cur_epoch.0,
            entry_kind: Some(ManifestEntryKind::RetentionFloor),
            fence: false,
            segment_key: None,
            first_seq: cur_seq.0,
            last_seq: cur_seq.0,
            visible_last_seq: None,
            checksum: 0,
            segment_epoch: None,
            segment_format: None,
            frame_crc32c: None,
            content_sha256: None,
            record_checksum_algorithm: None,
            frame_checksum_algorithm: None,
            content_hash_algorithm: None,
            committed_at_ms: 0, // audit-only; floor entries are skipped by every age/segment scanner
            retention_floor_through: Some(position.sequence),
            compacted_through_index: None,
        };
        let won = self.commit_manifest_entry(shard, cur_index, cur_epoch, &entry, false)?;
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
        let authority_head = self.read_authoritative_head(shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        if let Some(buf) = g.shards.get_mut(shard) {
            buf.next_manifest_index = buf.next_manifest_index.max(cur_index.0 + 1);
            buf.authority_head = authority_head.clone();
        }
        drop(g);
        if self.maintenance_owner_epoch(shard) == Some(expected_epoch) {
            if let Some(head) = authority_head.as_ref() {
                self.refresh_maintenance_authority_token_from_head(shard, expected_epoch, head)?;
            } else {
                self.refresh_maintenance_authority_token(shard, expected_epoch)?;
            }
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
    fn manifest_reclamation_candidates_from_entries(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
        entries: &[ManifestEntry],
    ) -> EngineResult<(Vec<ManifestReclamationCandidate>, Option<u64>)> {
        let floor = self.read_retention_floor(shard)?;
        let floor_seq = floor.as_ref().map(|f| f.sequence);
        let max_live_branch_cut = self.max_live_branch_cut_snapshot(shard, now_ms)?;
        let authority = pqueue_engine::MaintenanceAuthoritySnapshot {
            queue: shard.clone(),
            current_epoch: self.current_epoch(shard)?,
            observed_at_ms: now_ms,
            retention_may_advance: true,
            complete_frontier_required: false,
            lineage_validated: true,
            committed_snapshot_through: Some(reclaimed_through),
            recovery_window_through: Some(reclaimed_through),
            manifest_tail: pqueue_engine::FrontierRequirement::NotRequired,
            request_ids: pqueue_engine::FrontierRequirement::NotRequired,
            item_keys: pqueue_engine::FrontierRequirement::NotRequired,
            async_projection_through: None,
            in_memory_claim_replay: pqueue_engine::FrontierRequirement::NotRequired,
            durable_floor: floor_seq,
            branch_pins: BTreeSet::new(),
        };
        Ok(Self::plan_manifest_reclamation_candidates_from_entries(
            reclaimed_through,
            entries,
            floor_seq,
            max_live_branch_cut,
            &authority,
        ))
    }

    /// Pure ordered selector; its caller assembles floor, epoch, and the complete live pin registry once.
    fn plan_manifest_reclamation_candidates_from_entries(
        reclaimed_through: u64,
        entries: &[ManifestEntry],
        floor_seq: Option<u64>,
        max_live_branch_cut: Option<u64>,
        authority: &pqueue_engine::MaintenanceAuthoritySnapshot,
    ) -> (Vec<ManifestReclamationCandidate>, Option<u64>) {
        let mut new_w: Option<u64> = None;
        let mut pinned_prefix = false;
        let mut candidates = Vec::new();
        for entry in entries {
            // STOP at the AUTHORITATIVE floor entry — `read_retention_floor` needs it, so W must stay below it.
            let eligibility = Self::classify_reclamation_eligibility(
                entry.first_seq,
                Self::visible_last_seq(entry),
                entry.fence,
                entry.retention_floor_through,
                floor_seq,
                reclaimed_through,
            );
            if eligibility == ReclamationEligibility::AuthoritativeFloor {
                break;
            }
            if eligibility != ReclamationEligibility::Reclaimed {
                if floor_seq.is_some() {
                    break; // first LIVE / not-yet-reclaimed / needed entry — W must stay STRICTLY below it
                }
                continue;
            }
            // A still-branch-PINNED below-floor DATA segment has NOT been reclaimed (expire_segments_through
            // skipped its delete): a future trim after the pin releases must still enumerate it, so do NOT
            // include it in the candidate set. The pinned entry blocks the watermark, but later reclaimed
            // entries remain eligible for deletion within the same pass.
            if entry.segment_key.is_some()
                && max_live_branch_cut.is_some_and(|cut| entry.first_seq <= cut)
            {
                pinned_prefix = true;
                continue;
            }
            let planned = pqueue_engine::MaintenancePolicy::new(0)
                .plan(
                    authority,
                    &[pqueue_engine::MaintenanceCandidate {
                        queue: authority.queue.clone(),
                        stable_id: format!("manifest-entry-{}", entry.index),
                        class: pqueue_engine::MaintenanceObjectClass::ManifestEntry,
                        first_sequence: Some(entry.first_seq),
                        last_sequence: Some(Self::visible_last_seq(entry)),
                        manifest_index: Some(entry.index),
                        bytes: None,
                        created_at_ms: entry.committed_at_ms,
                        unreferenced_proven: false,
                        loser_proven: false,
                    }],
                    &pqueue_engine::MaintenanceFilter::default(),
                )
                .into_iter()
                .next()
                .expect("one manifest candidate");
            if planned.disposition != pqueue_engine::MaintenanceDisposition::Delete {
                break;
            }
            if floor_seq.is_some() && !pinned_prefix {
                new_w = Some(entry.index);
            }
            if entry.segment_key.is_some() {
                candidates.push(ManifestReclamationCandidate {
                    index: entry.index,
                    first_seq: entry.first_seq,
                    segment_key: entry.segment_key.clone(),
                    retention_floor_through: entry.retention_floor_through,
                });
            }
        }
        (candidates, new_w)
    }

    fn advance_read_horizon_from_entries(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
        entries: &[ManifestEntry],
        authority_mode: bool,
    ) -> EngineResult<()> {
        let new_w = self.contiguous_manifest_deletion_watermark_from_entries(
            shard,
            reclaimed_through,
            now_ms,
            entries,
        )?;
        if let Some(w) = new_w
            && self
                .read_manifest_deletion_watermark(shard)?
                .is_none_or(|cur| w > cur)
        {
            let completed = self.prove_completed_manifest_deletion_prefix(
                shard,
                ManifestIndex(w),
                entries,
                authority_mode,
            )?;
            self.persist_manifest_deletion_watermark_entry(shard, completed, now_ms)?;
        }
        Ok(())
    }

    pub fn advance_read_horizon(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
    ) -> EngineResult<()> {
        let (entries, authority_mode) = self.read_manifest_with_authority(shard)?;
        self.advance_read_horizon_from_entries(
            shard,
            reclaimed_through,
            now_ms,
            &entries,
            authority_mode,
        )
    }

    /// Persist the manifest-deletion watermark after a caller has already confirmed deletion progress.
    ///
    /// This is progress storage only: the caller must delete the manifest objects first, then call this
    /// helper to durably record the reclaimed prefix. It does not advance `read_retention_floor`, does not
    /// act as retention authority, and must not be used to hide still-present below-floor entries during a
    /// partial-expiry pass. If no below-floor manifest deletion made progress, the monotonic update is a
    /// no-op. Correctness here does not depend on the deferred pqueue-c33c367e owner-fence wiring; the
    /// permanent head CAS remains the stale-writer fence, and this helper only records already-reclaimed
    /// manifest history.
    pub fn persist_manifest_deletion_watermark(
        &self,
        shard: &QueueKey,
        reclaimed_through: u64,
        now_ms: i64,
    ) -> EngineResult<()> {
        if self
            .read_retention_floor(shard)?
            .is_some_and(|floor| reclaimed_through > floor.sequence)
        {
            return Ok(()); // ignore a stale candidate that would overrun the authoritative floor
        }
        self.advance_read_horizon(shard, reclaimed_through, now_ms)
    }

    fn persist_manifest_deletion_watermark_entry(
        &self,
        shard: &QueueKey,
        completed: CompletedManifestDeletionPrefix,
        now_ms: i64,
    ) -> EngineResult<()> {
        let reclaimed_through = completed.0.0;
        // Best-effort monotonic PUT. A candidate is always derived strictly below the durable floor (see
        // SAFETY above), and the append-only watermark marker history makes stale blob writes harmless.
        let (cur_seq, cur_index, cur_epoch, _) = self.recover_manifest(shard)?;
        let marker = ManifestEntry {
            index: cur_index.saturating_sub(1),
            epoch: cur_epoch,
            entry_kind: Some(ManifestEntryKind::DeletionWatermark),
            fence: false,
            segment_key: None,
            first_seq: cur_seq.saturating_sub(1),
            last_seq: cur_seq.saturating_sub(1),
            visible_last_seq: None,
            checksum: 0,
            segment_epoch: None,
            segment_format: None,
            frame_crc32c: None,
            content_sha256: None,
            record_checksum_algorithm: None,
            frame_checksum_algorithm: None,
            content_hash_algorithm: None,
            committed_at_ms: now_ms,
            retention_floor_through: None,
            compacted_through_index: Some(reclaimed_through),
        };
        let _ = self.commit_manifest_watermark_marker(shard, &marker)?;
        let blob = ReadHorizonBlob {
            index: reclaimed_through,
        };
        self.store_put(&Self::read_horizon_key(shard), &to_json(&blob)?, false)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        if let Some(buf) = g.shards.get_mut(shard) {
            buf.manifest_deletion_watermark = Some(
                buf.manifest_deletion_watermark
                    .map_or(reclaimed_through, |cur| cur.max(reclaimed_through)),
            );
        }
        Ok(())
    }

    fn persist_bounded_manifest_deletion_watermark(
        &self,
        shard: &QueueKey,
        index: u64,
        reclaimed_through: u64,
        epoch: u64,
        now_ms: i64,
    ) -> Result<usize, crate::maintenance::MaintenanceExecutionFailure> {
        let marker = ManifestEntry {
            index,
            epoch,
            entry_kind: Some(ManifestEntryKind::DeletionWatermark),
            fence: false,
            segment_key: None,
            first_seq: reclaimed_through,
            last_seq: reclaimed_through,
            visible_last_seq: None,
            checksum: 0,
            segment_epoch: None,
            segment_format: None,
            frame_crc32c: None,
            content_sha256: None,
            record_checksum_algorithm: None,
            frame_checksum_algorithm: None,
            content_hash_algorithm: None,
            committed_at_ms: now_ms,
            retention_floor_through: None,
            compacted_through_index: Some(index),
        };
        let (_, marker_attempts) = self.commit_manifest_watermark_marker_counted(shard, &marker)?;
        let horizon_body = to_json(&ReadHorizonBlob { index }).map_err(|error| {
            crate::maintenance::MaintenanceExecutionFailure {
                effect: crate::maintenance::MaintenanceEffect {
                    objects: 0,
                    bytes: 0,
                    requests: marker_attempts,
                },
                fault: None,
                error,
            }
        })?;
        let requests = marker_attempts + 1;
        self.store_put(&Self::read_horizon_key(shard), &horizon_body, false)
            .map_err(|error| crate::maintenance::MaintenanceExecutionFailure {
                effect: crate::maintenance::MaintenanceEffect {
                    objects: 0,
                    bytes: 0,
                    requests,
                },
                fault: Some(self.store.classify_fault(&error)),
                error,
            })?;
        if let Some(buf) = self
            .inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .get_mut(shard)
        {
            buf.manifest_deletion_watermark = Some(
                buf.manifest_deletion_watermark
                    .map_or(index, |current| current.max(index)),
            );
        }
        Ok(requests)
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
            if entry.fence
                || entry.segment_key.is_none()
                || Self::is_reclaimed_manifest_marker(&entry)
            {
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
        let buf = self.load_shard_buf(shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.shards.entry(shard.clone()).or_insert(buf);
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

/// Versioned manifest-head payload. The version token is carried by the object key; the body carries the
/// durable queue state that recovery needs to resume from the manifest tail.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ManifestHeadBlob {
    pub current_epoch: u64,
    pub next_seq: u64,
    pub next_manifest_index: u64,
    pub retention_floor_through: Option<u64>,
    /// Tail of the authoritative post-migration manifest-candidate chain.
    #[serde(default)]
    pub tail_candidate_key: Option<String>,
    /// Entries below this index belong to the immutable legacy manifest base captured at migration.
    #[serde(default)]
    pub legacy_next_manifest_index: u64,
}

/// The latest versioned manifest head object together with the opaque version token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedHead<T> {
    pub version: u64,
    pub value: T,
}

fn versioned_manifest_head_key(prefix: &str, version: u64) -> String {
    format!("{prefix}{version:020}.json")
}

fn parse_versioned_manifest_head_key(prefix: &str, key: &str) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?.strip_suffix(".json")?;
    if suffix.len() != 20 || !suffix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    suffix.parse().ok()
}

fn parse_manifest_index_from_key(key: &str) -> Option<u64> {
    let suffix = key.rsplit('/').next()?.strip_suffix(".json")?;
    if suffix.len() != 20 || !suffix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    suffix.parse().ok()
}

/// Durable per-shard READ-HORIZON watermark blob (bead pqueue-8928baec): the highest manifest `index` below
/// which every entry is a reclaimed/superseded below-floor entry that no live read, recovery tail,
/// authoritative-floor read, or branch copy needs. Stored at `{shard_prefix}read_horizon.json` — OUTSIDE the
/// `manifest/` prefix — so `recover_manifest`/`read_manifest` LISTs never enumerate it. Monotonic (a set that
/// would lower it is a no-op). It is a compatibility cache, never authority: append-only watermark markers
/// define visibility, retained manifest-head/authority history fences stale writers, and the legacy manifest
/// mirror may be physically deleted after the completed-prefix proof succeeds.
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

/// A minimal S3-compatible [`BlobStore`] over HTTP/1.1 + SigV4. Deliberately dependency-light: the
/// signing dependency it pulls beyond the workspace baseline is `sha2` (already in-tree for the relational
/// projection); HMAC-SHA256, the SigV4 canonical request, the HTTP/1.1 framing, and the (small) ListObjects
/// XML scrape are hand-rolled. Production endpoints use HTTPS via rustls; local MinIO may use HTTP. Both
/// use path-style addressing. The manifest CAS uses `If-None-Match: *` (create-only conditional PUT), which
/// MinIO and S3 support and which needs no ETag round-trip.
pub struct S3BlobStore {
    host: String,
    port: u16,
    tls: bool,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
}

struct S3RequestError {
    outward: EngineError,
    result: crate::object_store_observability::BlobResultClass,
    retryable: bool,
}

impl S3RequestError {
    fn transport(outward: EngineError) -> Self {
        Self {
            outward,
            result: crate::object_store_observability::BlobResultClass::Transport,
            retryable: true,
        }
    }

    fn corrupt(outward: EngineError) -> Self {
        Self {
            outward,
            result: crate::object_store_observability::BlobResultClass::Corrupt,
            retryable: false,
        }
    }

    fn other(outward: EngineError) -> Self {
        Self {
            outward,
            result: crate::object_store_observability::BlobResultClass::OtherError,
            retryable: false,
        }
    }
}

impl S3BlobStore {
    fn request_error(
        error: S3RequestError,
    ) -> crate::object_store_observability::ClassifiedBlobError {
        crate::object_store_observability::ClassifiedBlobError {
            outward: error.outward,
            fault: crate::object_store_observability::BlobStoreFault::new(
                error.result,
                error.retryable,
                false,
                false,
            ),
            attempts: 1,
            request_bytes: 0,
            response_bytes: 0,
        }
    }

    fn http_error(
        error: EngineError,
        status: u16,
    ) -> crate::object_store_observability::ClassifiedBlobError {
        use crate::object_store_observability::{BlobResultClass, BlobStoreFault};
        let fault = match status {
            429 | 503 => BlobStoreFault::new(BlobResultClass::Throttled, true, true, false),
            408 | 500..=599 => BlobStoreFault::new(BlobResultClass::Transport, true, false, false),
            _ => BlobStoreFault::new(BlobResultClass::OtherError, false, false, false),
        };
        crate::object_store_observability::ClassifiedBlobError {
            outward: error,
            fault,
            attempts: 1,
            request_bytes: 0,
            response_bytes: 0,
        }
    }

    fn request_observed(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> crate::object_store_observability::ClassifiedBlobResult<(u16, Vec<u8>)> {
        self.request_typed(method, path, query, body, extra_headers)
            .map_err(|error| {
                let mut error = Self::request_error(error);
                error.request_bytes = body.len() as u64;
                error
            })
    }

    /// Build a client. `endpoint` is `https://host[:port]` for production S3 or
    /// `http://host[:port]` for an explicitly permitted local S3-compatible fixture. The bucket must exist
    /// (or call [`S3BlobStore::create_bucket`]).
    pub fn new(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> EngineResult<Self> {
        let (rest, tls, default_port) = if let Some(rest) = endpoint.strip_prefix("https://") {
            (rest, true, 443)
        } else if let Some(rest) = endpoint.strip_prefix("http://") {
            (rest, false, 80)
        } else {
            return Err(EngineError::Invalid(
                "endpoint must be https://host[:port] or http://host[:port]",
            ));
        };
        let (host, port) = match rest.split_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.trim_end_matches('/')
                    .parse::<u16>()
                    .map_err(|_| EngineError::Invalid("bad endpoint port"))?,
            ),
            None => (rest.trim_end_matches('/').to_string(), default_port),
        };
        if host.is_empty() || host.contains('/') {
            return Err(EngineError::Invalid("bad endpoint host"));
        }
        Ok(Self {
            host,
            port,
            tls,
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
        self.request_typed(method, path, query, body, extra_headers)
            .map_err(|error| error.outward)
    }

    fn request_typed(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> Result<(u16, Vec<u8>), S3RequestError> {
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

        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .map_err(store_err)
            .map_err(S3RequestError::transport)?;
        let raw = if self.tls {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let server_name = rustls::pki_types::ServerName::try_from(self.host.clone())
                .map_err(|_| EngineError::Invalid("bad TLS endpoint host"))
                .map_err(S3RequestError::other)?;
            let connection =
                rustls::ClientConnection::new(std::sync::Arc::new(config), server_name)
                    .map_err(store_err)
                    .map_err(S3RequestError::transport)?;
            let stream = rustls::StreamOwned::new(connection, tcp);
            send_http_request(stream, req.as_bytes(), body).map_err(S3RequestError::transport)?
        } else {
            send_http_request(tcp, req.as_bytes(), body).map_err(S3RequestError::transport)?
        };
        parse_http_response(&raw).map_err(S3RequestError::corrupt)
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

    fn observed_list_impl(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        let path = format!("/{}", self.bucket);
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        let mut request_count = 0u64;
        let mut response_bytes = 0u64;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            match (&continuation, start_after) {
                (Some(token), _) => query.push(("continuation-token".to_string(), token.clone())),
                (None, Some(cursor)) => query.push(("start-after".to_string(), cursor.to_string())),
                (None, None) => {}
            }
            let (status, body) = self
                .request_observed("GET", &path, &query, &[], &[])
                .map_err(|mut error| {
                    error.attempts = request_count + 1;
                    // A later-page transport/TLS/HTTP-decode failure still belongs to this one
                    // logical LIST. Preserve all provider-known wire bytes from pages that completed
                    // before the terminal request; the terminal request keeps its own typed byte
                    // accounting from `request_observed`.
                    error.response_bytes = error.response_bytes.saturating_add(response_bytes);
                    error
                })?;
            request_count += 1;
            response_bytes += body.len() as u64;
            if status != 200 {
                let outward = EngineError::Storage(format!(
                    "S3 LIST {prefix} failed: HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                ));
                let mut error = Self::http_error(outward, status);
                error.attempts = request_count;
                error.response_bytes = response_bytes;
                return Err(error);
            }
            let xml = String::from_utf8_lossy(&body);
            keys.extend(scrape_keys(&xml));
            match next_continuation_token(&xml) {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
        Ok(ObservedBlobCall::new(
            keys,
            request_count,
            0,
            response_bytes,
        ))
    }
}

fn send_http_request(
    mut stream: impl Read + Write,
    headers: &[u8],
    body: &[u8],
) -> EngineResult<Vec<u8>> {
    stream.write_all(headers).map_err(store_err)?;
    stream.write_all(body).map_err(store_err)?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(store_err)?;
    Ok(raw)
}

impl BlobStore for S3BlobStore {
    fn backend_kind(&self) -> crate::object_store_observability::BlobBackendKind {
        crate::object_store_observability::BlobBackendKind::S3
    }

    fn observed_put(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<()>> {
        let (status, resp) =
            self.request_observed("PUT", &self.object_path(key), &[], body, &[])?;
        if status == 200 || status == 204 {
            Ok(ObservedBlobCall::new(
                (),
                1,
                body.len() as u64,
                resp.len() as u64,
            ))
        } else {
            let error = EngineError::Storage(format!(
                "S3 PUT {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            ));
            let mut error = Self::http_error(error, status);
            error.request_bytes = body.len() as u64;
            error.response_bytes = resp.len() as u64;
            Err(error)
        }
    }

    fn observed_put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        let extra = vec![("If-None-Match".to_string(), "*".to_string())];
        let (status, resp) =
            self.request_observed("PUT", &self.object_path(key), &[], body, &extra)?;
        match status {
            200 | 204 => Ok(ObservedBlobCall::new(
                true,
                1,
                body.len() as u64,
                resp.len() as u64,
            )),
            409 | 412 => Ok(ObservedBlobCall::new(
                false,
                1,
                body.len() as u64,
                resp.len() as u64,
            )),
            _ => {
                let error = EngineError::Storage(format!(
                    "S3 conditional PUT {key} failed: HTTP {status}: {}",
                    String::from_utf8_lossy(&resp)
                ));
                let mut error = Self::http_error(error, status);
                error.request_bytes = body.len() as u64;
                error.response_bytes = resp.len() as u64;
                Err(error)
            }
        }
    }

    fn observed_get(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Option<Vec<u8>>>>
    {
        let (status, body) = self.request_observed("GET", &self.object_path(key), &[], &[], &[])?;
        match status {
            200 => {
                let len = body.len() as u64;
                Ok(ObservedBlobCall::new(Some(body), 1, 0, len))
            }
            404 => Ok(ObservedBlobCall::new(None, 1, 0, body.len() as u64)),
            _ => {
                let error = EngineError::Storage(format!("S3 GET {key} failed: HTTP {status}"));
                let mut error = Self::http_error(error, status);
                error.response_bytes = body.len() as u64;
                Err(error)
            }
        }
    }

    fn observed_delete(
        &self,
        key: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<bool>> {
        let (status, resp) =
            self.request_observed("DELETE", &self.object_path(key), &[], &[], &[])?;
        match status {
            204 | 200 => Ok(ObservedBlobCall::new(true, 1, 0, resp.len() as u64)),
            404 => Ok(ObservedBlobCall::new(false, 1, 0, resp.len() as u64)),
            _ => {
                let error = EngineError::Storage(format!(
                    "S3 DELETE {key} failed: HTTP {status}: {}",
                    String::from_utf8_lossy(&resp)
                ));
                let mut error = Self::http_error(error, status);
                error.response_bytes = resp.len() as u64;
                Err(error)
            }
        }
    }

    fn observed_list_with_request_count(
        &self,
        prefix: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        self.observed_list_impl(prefix, None)
    }

    fn observed_list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        self.observed_list_impl(prefix, Some(start_after))
    }

    fn observed_list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> crate::object_store_observability::ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>
    {
        if limit == 0 {
            return Ok(ObservedBlobCall::new(Vec::new(), 0, 0, 0));
        }
        let path = format!("/{}", self.bucket);
        let mut query = vec![
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), prefix.to_string()),
            ("max-keys".to_string(), limit.min(1000).to_string()),
        ];
        if let Some(cursor) = start_after {
            query.push(("start-after".to_string(), cursor.to_string()));
        }
        let (status, body) = self.request_observed("GET", &path, &query, &[], &[])?;
        if status != 200 {
            let outward = EngineError::Storage(format!(
                "S3 bounded LIST {prefix} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ));
            let mut error = Self::http_error(outward, status);
            error.response_bytes = body.len() as u64;
            return Err(error);
        }
        let bytes = body.len() as u64;
        Ok(ObservedBlobCall::new(
            scrape_keys(&String::from_utf8_lossy(&body)),
            1,
            0,
            bytes,
        ))
    }

    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.observed_put(key, body)
            .map(|call| call.value)
            .map_err(|error| error.outward)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        self.observed_put_if_absent(key, body)
            .map(|call| call.value)
            .map_err(|error| error.outward)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        self.observed_get(key)
            .map(|call| call.value)
            .map_err(|error| error.outward)
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        self.observed_delete(key)
            .map(|call| call.value)
            .map_err(|error| error.outward)
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.list_with_request_count(prefix).map(|(keys, _)| keys)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        self.observed_list_page(prefix, start_after, limit)
            .map(|call| call.value)
            .map_err(|error| error.outward)
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
        self.observed_list_with_request_count(prefix)
            .map(|call| (call.value, call.attempts))
            .map_err(|error| error.outward)
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
        self.observed_list_from_with_request_count(prefix, start_after)
            .map(|call| (call.value, call.attempts))
            .map_err(|error| error.outward)
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
    use super::{
        BlobStore, EngineError, S3BlobStore, S3RequestError, next_continuation_token, scrape_keys,
        scrape_tag,
    };
    use crate::object_store_observability::{
        BlobBackendKind, BlobMetricsRecorder, BlobObjectClass, BlobOperation, BlobResultClass,
        InstrumentedBlobStore,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    fn loopback(responses: Vec<(u16, String)>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 8192];
                let _ = stream.read(&mut request).unwrap();
                let reason = if status == 200 { "OK" } else { "Error" };
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (endpoint, handle)
    }

    fn loopback_raw(responses: Vec<Vec<u8>>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 8192];
                let _ = stream.read(&mut request).unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        (endpoint, handle)
    }

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

    #[test]
    fn s3_status_and_request_failures_have_structured_bounded_classes() {
        for (status, result, retryable, throttled) in [
            (429, BlobResultClass::Throttled, true, true),
            (503, BlobResultClass::Throttled, true, true),
            (500, BlobResultClass::Transport, true, false),
            (408, BlobResultClass::Transport, true, false),
            (403, BlobResultClass::OtherError, false, false),
        ] {
            let error =
                S3BlobStore::http_error(EngineError::Storage("same outward".into()), status);
            assert_eq!(error.fault.result, result);
            assert_eq!(error.fault.retryable, retryable);
            assert_eq!(error.fault.throttled, throttled);
            assert!(
                !error.fault.timeout,
                "timeout is reserved-zero in this slice"
            );
        }
        let corrupt = S3BlobStore::request_error(S3RequestError::corrupt(EngineError::Storage(
            "malformed HTTP response".into(),
        )));
        assert_eq!(corrupt.fault.result, BlobResultClass::Corrupt);
        assert!(!corrupt.fault.retryable);
        let transport = S3BlobStore::request_error(S3RequestError::transport(
            EngineError::Storage("connection refused".into()),
        ));
        assert_eq!(transport.fault.result, BlobResultClass::Transport);
        assert!(transport.fault.retryable);
    }

    #[test]
    fn instrumented_s3_two_page_list_records_wire_body_bytes_and_no_retries() {
        let first = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>manifest/a</Key></Contents></ListBucketResult>".to_string();
        let second = "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>manifest/b</Key></Contents></ListBucketResult>".to_string();
        let expected_bytes = (first.len() + second.len()) as u64;
        let (endpoint, server) = loopback(vec![(200, first), (200, second)]);
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            S3BlobStore::new(&endpoint, "bucket", "access", "secret", "us-east-1").unwrap(),
            Arc::clone(&recorder),
            BlobBackendKind::S3,
        );
        let (keys, attempts) = store.list_with_request_count("manifest/").unwrap();
        server.join().unwrap();
        assert_eq!(keys, vec!["manifest/a", "manifest/b"]);
        assert_eq!(attempts, 2);
        let row = recorder.snapshot().row(
            BlobOperation::List,
            BlobObjectClass::Manifest,
            BlobResultClass::Success,
            false,
            BlobBackendKind::S3,
        );
        assert_eq!(
            (
                row.completions,
                row.attempts,
                row.retries,
                row.response_bytes
            ),
            (1, 2, 0, expected_bytes)
        );
    }

    #[test]
    fn instrumented_s3_terminal_list_error_keeps_prior_and_terminal_wire_bytes() {
        let first = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>".to_string();
        let terminal = "slow-down".to_string();
        let expected_bytes = (first.len() + terminal.len()) as u64;
        let (endpoint, server) = loopback(vec![(200, first), (429, terminal)]);
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            S3BlobStore::new(&endpoint, "bucket", "access", "secret", "us-east-1").unwrap(),
            Arc::clone(&recorder),
            BlobBackendKind::S3,
        );
        assert!(store.list_with_request_count("manifest/").is_err());
        server.join().unwrap();
        let row = recorder.snapshot().row(
            BlobOperation::List,
            BlobObjectClass::Manifest,
            BlobResultClass::Throttled,
            true,
            BlobBackendKind::S3,
        );
        assert_eq!(
            (
                row.completions,
                row.attempts,
                row.retries,
                row.response_bytes,
                row.errors
            ),
            (1, 2, 0, expected_bytes, 1)
        );
    }

    #[test]
    fn instrumented_s3_malformed_later_page_keeps_prior_wire_bytes() {
        let first = "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>";
        let first_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{first}",
            first.len()
        )
        .into_bytes();
        let (endpoint, server) = loopback_raw(vec![first_response, b"not-http".to_vec()]);
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            S3BlobStore::new(&endpoint, "bucket", "access", "secret", "us-east-1").unwrap(),
            Arc::clone(&recorder),
            BlobBackendKind::S3,
        );
        assert!(store.list_with_request_count("manifest/").is_err());
        server.join().unwrap();
        let row = recorder.snapshot().row(
            BlobOperation::List,
            BlobObjectClass::Manifest,
            BlobResultClass::Corrupt,
            false,
            BlobBackendKind::S3,
        );
        assert_eq!(
            (
                row.completions,
                row.attempts,
                row.retries,
                row.request_bytes,
                row.response_bytes,
                row.errors
            ),
            (1, 2, 0, 0, first.len() as u64, 1)
        );
    }

    #[test]
    fn s3_zero_limit_page_has_zero_attempts_without_connecting() {
        let store = S3BlobStore::new(
            "http://127.0.0.1:1",
            "bucket",
            "access",
            "secret",
            "us-east-1",
        )
        .unwrap();
        let call = store.observed_list_page("manifest/", None, 0).unwrap();
        assert!(call.value.is_empty());
        assert_eq!(
            (call.attempts, call.request_bytes, call.response_bytes),
            (0, 0, 0)
        );
    }
}

#[cfg(test)]
mod manifest_deletion_watermark_tests {
    use super::*;
    use pqueue_conformance::{
        envelope, item, qdef as conformance_qdef, shard as conformance_shard,
    };
    use pqueue_engine::PushCommand;
    use std::sync::atomic::{
        AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering, Ordering,
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn manifest_json_v3_golden_and_legacy_v2_inference_are_pinned() {
        const V3: &str = "{\"index\":4,\"epoch\":7,\"entry_kind\":\"data\",\"fence\":false,\"segment_key\":\"opaque\",\"first_seq\":11,\"last_seq\":11,\"visible_last_seq\":null,\"checksum\":0,\"segment_epoch\":7,\"segment_format\":3,\"frame_crc32c\":123,\"content_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"record_checksum_algorithm\":\"crc32c-castagnoli\",\"frame_checksum_algorithm\":\"crc32c-castagnoli\",\"content_hash_algorithm\":\"sha256\",\"committed_at_ms\":9,\"retention_floor_through\":null}";
        let v3: ManifestEntry = serde_json::from_str(V3).unwrap();
        v3.validate_kind().unwrap();
        assert_eq!(serde_json::to_string(&v3).unwrap(), V3);

        const LEGACY_V2: &str = "{\"index\":1,\"epoch\":2,\"fence\":false,\"segment_key\":\"legacy.seg\",\"first_seq\":3,\"last_seq\":3,\"visible_last_seq\":null,\"checksum\":5,\"committed_at_ms\":6,\"retention_floor_through\":null}";
        let legacy: ManifestEntry = serde_json::from_str(LEGACY_V2).unwrap();
        assert_eq!(legacy.entry_kind, None);
        assert_eq!(legacy.segment_format, None);
        legacy.validate_kind().unwrap();
        assert_eq!(
            manifest_index_from_any_key(
                "t/aa/q/bb/manifest_candidates/e00000000000000000007/i00000000000000000042/digest.json"
            ),
            Some(42)
        );
        assert_eq!(
            manifest_index_from_any_key(
                "t/aa/q/bb/manifest_head/00000000000000000041~watermark.json"
            ),
            Some(41)
        );
    }

    fn pushes(n: u64) -> Vec<CommandEnvelope> {
        (0..n)
            .map(|i| {
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item(&format!("{i}"), &format!("k{i}"), i as i64)],
                    }),
                    vec![],
                )
            })
            .collect()
    }

    fn expire_as_owner<S: BlobStore>(
        log: &SegmentedObjectLog<S>,
        shard: &QueueKey,
        through: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        if log.maintenance_owner_epoch(shard).is_none() {
            log.acquire_epoch(shard, now_ms)?;
        }
        log.expire_segments_through(shard, through, now_ms)
    }

    fn advance_floor_as_owner<S: BlobStore>(
        log: &SegmentedObjectLog<S>,
        shard: &QueueKey,
        through: u64,
        now_ms: i64,
    ) -> EngineResult<()> {
        let epoch = match log.maintenance_owner_epoch(shard) {
            Some(epoch) => epoch,
            None => log.acquire_epoch(shard, now_ms)?,
        };
        log.advance_retention_floor(
            shard,
            CommandPosition::new(shard.clone(), epoch, through),
            epoch,
        )
    }

    fn strip_manifest_head_namespace(store: &std::sync::Arc<InMemoryBlobStore>, shard: &QueueKey) {
        for key in store
            .list(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_prefix(shard))
            .unwrap()
        {
            assert!(
                store.delete(&key).unwrap(),
                "expected to remove manifest head key {key}"
            );
        }
    }

    struct PartialExpireFault {
        calls: AtomicUsize,
    }

    impl PartialExpireFault {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl FaultHook for PartialExpireFault {
        fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
            if cut != FaultCutPoint::DuringSegmentExpiry {
                return Ok(());
            }
            if self.calls.fetch_add(1, AtomicOrdering::SeqCst) >= 1 {
                Err(EngineError::Conflict)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Default)]
    struct RecordingDeleteBlobStore {
        inner: InMemoryBlobStore,
        delete_events: Mutex<Vec<String>>,
        fail_delete_key: Mutex<Option<String>>,
        failed_once: AtomicBool,
    }

    impl RecordingDeleteBlobStore {
        fn delete_events(&self) -> Vec<String> {
            self.delete_events.lock().unwrap().clone()
        }

        fn fail_delete_once(&self, key: &str) {
            *self.fail_delete_key.lock().unwrap() = Some(key.to_owned());
            self.failed_once.store(false, Ordering::Relaxed);
        }

        fn clear_delete_failure(&self) {
            *self.fail_delete_key.lock().unwrap() = None;
            self.failed_once.store(false, Ordering::Relaxed);
        }
    }

    impl BlobStore for RecordingDeleteBlobStore {
        fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
            self.inner.put(key, body)
        }

        fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
            self.inner.put_if_absent(key, body)
        }

        fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
            self.inner.get(key)
        }

        fn delete(&self, key: &str) -> EngineResult<bool> {
            self.delete_events.lock().unwrap().push(key.to_owned());
            let should_fail = self
                .fail_delete_key
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|fail_key| fail_key == key)
                && !self.failed_once.swap(true, Ordering::Relaxed);
            if should_fail {
                return Err(EngineError::Conflict);
            }
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
            self.inner.list(prefix)
        }

        fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
            self.inner.stats(prefix)
        }
    }

    #[derive(Default)]
    struct CountingBlobStore {
        inner: InMemoryBlobStore,
        list_count: AtomicU64,
        get_count: AtomicU64,
        delete_count: AtomicU64,
    }

    impl CountingBlobStore {
        fn list_count(&self) -> u64 {
            self.list_count.load(Ordering::Relaxed)
        }

        fn reset_list_count(&self) {
            self.list_count.store(0, Ordering::Relaxed);
        }

        fn request_counts(&self) -> (u64, u64, u64) {
            (
                self.list_count.load(Ordering::Relaxed),
                self.get_count.load(Ordering::Relaxed),
                self.delete_count.load(Ordering::Relaxed),
            )
        }

        fn reset_request_counts(&self) {
            self.list_count.store(0, Ordering::Relaxed);
            self.get_count.store(0, Ordering::Relaxed);
            self.delete_count.store(0, Ordering::Relaxed);
        }
    }

    impl BlobStore for CountingBlobStore {
        fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
            self.inner.put(key, body)
        }

        fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
            self.inner.put_if_absent(key, body)
        }

        fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
            self.get_count.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }

        fn delete(&self, key: &str) -> EngineResult<bool> {
            self.delete_count.fetch_add(1, Ordering::Relaxed);
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
            self.list_count.fetch_add(1, Ordering::Relaxed);
            self.inner.list(prefix)
        }

        fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
            self.inner.stats(prefix)
        }
    }

    fn authority_seal_request_counts(history: u64) -> (u64, u64, u64) {
        let store = Arc::new(CountingBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        assert_eq!(log.fence_epoch(&shard, 0, 0).unwrap(), 0);
        for i in 0..history {
            log.enqueue(&shard, &pushes(1), 0, i as i64 * 2 + 1)
                .unwrap();
            log.seal(&shard, 0, i as i64 * 2 + 2).unwrap();
        }
        store.reset_request_counts();
        log.enqueue(&shard, &pushes(1), 0, 10_000).unwrap();
        log.seal(&shard, 0, 10_001).unwrap();
        store.request_counts()
    }

    #[test]
    fn authority_seal_cost_is_constant_after_long_head_history() {
        let short = authority_seal_request_counts(2);
        let long = authority_seal_request_counts(512);
        assert_eq!(
            short, long,
            "seal object reads must not grow with head history"
        );
        assert_eq!(
            long.0, 0,
            "steady-state seal must not LIST authority history"
        );
        assert!(
            long.1 <= 2,
            "seal should GET only predecessor and candidate"
        );
    }

    #[test]
    fn reopened_authority_cache_makes_first_seal_constant_work() {
        let store = Arc::new(CountingBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        {
            let writer = SegmentedObjectLog::open(store.clone(), cfg);
            writer.create_queue(&conformance_qdef()).unwrap();
            writer.fence_epoch(&shard, 0, 0).unwrap();
            for i in 0..128 {
                writer
                    .enqueue(&shard, &pushes(1), 0, i as i64 * 2 + 1)
                    .unwrap();
                writer.seal(&shard, 0, i as i64 * 2 + 2).unwrap();
            }
        }
        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        store.reset_request_counts();
        reopened.enqueue(&shard, &pushes(1), 0, 1_000).unwrap();
        reopened.seal(&shard, 0, 1_001).unwrap();
        let counts = store.request_counts();
        assert_eq!(
            counts.0, 0,
            "recovery pays the scan once, not on the next seal"
        );
        assert!(counts.1 <= 2, "first post-reopen seal stays constant-work");
    }

    #[test]
    fn cached_stale_owner_loses_successor_cas_without_ack() {
        let store = Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let stale = SegmentedObjectLog::open(store.clone(), cfg);
        stale.create_queue(&conformance_qdef()).unwrap();
        stale.fence_epoch(&shard, 0, 0).unwrap();
        let winner = SegmentedObjectLog::open(store, cfg);
        winner.create_queue(&conformance_qdef()).unwrap();
        winner.fence_epoch(&shard, 1, 1).unwrap();

        stale.enqueue(&shard, &pushes(1), 0, 2).unwrap();
        assert_eq!(stale.seal(&shard, 0, 3), Err(EngineError::EpochFenced));
        assert!(winner.read_all(&shard).unwrap().is_empty());
    }

    #[test]
    fn successful_seal_refreshes_maintenance_token_without_rescan() {
        let store = Arc::new(CountingBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        log.fence_epoch(&shard, 0, 0).unwrap();
        store.reset_request_counts();
        log.enqueue(&shard, &pushes(1), 0, 1).unwrap();
        log.seal(&shard, 0, 2).unwrap();
        let counts = store.request_counts();
        assert_eq!(
            counts.0, 0,
            "maintenance token refresh must not scan head history"
        );
        assert!(counts.1 <= 2);

        let token = log
            .maintenance_owned_epochs
            .lock()
            .unwrap()
            .get(&shard)
            .cloned()
            .expect("serving owner token");
        let durable = store
            .inner
            .get(&token.authority_key)
            .unwrap()
            .expect("durable authority head");
        assert_eq!(token.authority_body, durable);
        assert_eq!(
            token.authority_digest.as_slice(),
            Sha256::digest(&durable).as_slice()
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestReclamationEligibilityStrictlyBelowFloor() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        let shard = conformance_shard();

        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }

        advance_floor_as_owner(&log, &shard, 3, 0).unwrap();
        let candidates = log.manifest_reclamation_candidates(&shard, 1, 31).unwrap();
        assert_eq!(
            candidates.iter().map(|c| c.first_seq).collect::<Vec<_>>(),
            vec![0],
            "the candidate set stays strictly below the durable floor and excludes the unreclaimed below-floor and live above-floor segments"
        );
        assert_eq!(
            candidates.len(),
            1,
            "the authoritative floor entry and the live tail at or above it are not eligible"
        );
        assert_eq!(
            expire_as_owner(&log, &shard, 1, 31).unwrap(),
            1,
            "only the reclaimed below-floor prefix is deleted on the partial pass"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestReclamationEligibilitySkipsBranchPinnedSegments() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        let shard = conformance_shard();

        log.create_queue(&conformance_qdef()).unwrap();
        log.enqueue(&shard, &pushes(2), 0, 10).unwrap();
        log.seal(&shard, 0, 11).unwrap();
        log.enqueue(&shard, &pushes(2), 0, 20).unwrap();
        log.seal(&shard, 0, 21).unwrap();

        let mut branch_def = conformance_qdef();
        branch_def.queue_id =
            pqueue_core::QueueId::new(format!("manifest-eligibility-{}", std::process::id()))
                .unwrap();
        let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
        log.branch(
            &shard,
            &branch_def,
            &CommandPosition::new(shard.clone(), 0, 1),
            60_000,
            30,
        )
        .unwrap();

        advance_floor_as_owner(&log, &shard, 1, 0).unwrap();
        assert!(
            log.manifest_reclamation_candidates(&shard, 1, 31)
                .unwrap()
                .is_empty(),
            "the branch-pinned below-floor segment is excluded while the pin is live"
        );

        log.discard_branch(&shard, &branch).unwrap();
        let candidates = log.manifest_reclamation_candidates(&shard, 1, 32).unwrap();
        assert_eq!(
            candidates.iter().map(|c| c.first_seq).collect::<Vec<_>>(),
            vec![0],
            "once the branch pin is released, the same below-floor segment becomes enumerable again"
        );
        assert_eq!(
            expire_as_owner(&log, &shard, 1, 32).unwrap(),
            1,
            "the later expiry pass can reclaim the formerly pinned segment"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkAdvancesOnlyAfterManifestDeleteProgress() {
        let store = std::sync::Arc::new(RecordingDeleteBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }
        let segment_keys: Vec<_> = log
            .read_manifest(&shard)
            .unwrap()
            .into_iter()
            .filter_map(|entry| entry.segment_key)
            .collect();
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        store.fail_delete_once(&segment_keys[0]);
        let err = expire_as_owner(&log, &shard, 5, 1_000).unwrap_err();
        assert!(
            matches!(err, EngineError::Conflict),
            "a failed first eligible delete must abort before any watermark persistence occurs"
        );
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::read_horizon_key(
                    &shard
                ))
                .unwrap()
                .is_none(),
            "no deletion watermark blob is written when the pass makes no manifest-delete progress"
        );
        assert!(
            store
                .list(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_prefix(&shard))
                .unwrap()
                .into_iter()
                .all(|key| !key.ends_with("~watermark.json")),
            "no watermark marker is persisted when the first eligible manifest delete fails"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkRestartPersistsHighestContiguous() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..4u64 {
            log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
                .unwrap();
            log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 7, 0).unwrap();
        assert_eq!(expire_as_owner(&log, &shard, 7, 1_000).unwrap(), 4);
        let first = log
            .read_read_horizon(&shard)
            .unwrap()
            .expect("watermark after trim/reclaim");
        assert_eq!(
            first, 3,
            "the highest contiguous reclaimed manifest index is retained"
        );

        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        assert_eq!(
            reopened.read_read_horizon(&shard).unwrap(),
            Some(first),
            "the persisted watermark survives a close/reopen cycle without advancing the read horizon"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestWatermarkFailClosedBelowFloor() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            writer
                .enqueue(&shard, &pushes(1), 0, 100 + i as i64 * 10)
                .unwrap();
            writer.seal(&shard, 0, 101 + i as i64 * 10).unwrap();
        }

        advance_floor_as_owner(&writer, &shard, 1, 0).unwrap();
        assert_eq!(expire_as_owner(&writer, &shard, 1, 1_000).unwrap(), 2);

        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        assert_eq!(
            reopened.read_read_horizon(&shard).unwrap(),
            Some(1),
            "reopen reloads the durable manifest-deletion watermark"
        );

        let err = reopened.read_all(&shard).unwrap_err();
        assert!(
            matches!(&err, EngineError::Storage(msg) if msg.contains("read below retention floor")),
            "reads below the durable floor must fail closed after reopen, got {err:?}"
        );

        let err = reopened.read_from(&shard, 1).unwrap_err();
        assert!(
            matches!(&err, EngineError::Storage(msg) if msg.contains("read below retention floor")),
            "a reopened reader must also fail closed when the requested start sequence is at the floor, got {err:?}"
        );

        let live = reopened.read_from(&shard, 2).unwrap();
        assert_eq!(
            live.iter().map(|(pos, _)| pos.sequence).collect::<Vec<_>>(),
            vec![2],
            "the reopened reader still returns live entries above the durable floor"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkMonotonic() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let writer_a = SegmentedObjectLog::open(store.clone(), cfg);
        writer_a.create_queue(&conformance_qdef()).unwrap();
        for i in 0..4u64 {
            writer_a
                .enqueue(&shard, &pushes(2), 0, 200 + i as i64 * 10)
                .unwrap();
            writer_a.seal(&shard, 0, 201 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&writer_a, &shard, 7, 0).unwrap();
        expire_as_owner(&writer_a, &shard, 7, 1_000).unwrap();

        let writer_b = SegmentedObjectLog::open(store.clone(), cfg);
        writer_b.create_queue(&conformance_qdef()).unwrap();
        assert_eq!(
            writer_b.read_read_horizon(&shard).unwrap(),
            Some(3),
            "restart reconstructs the durable watermark"
        );
        writer_b
            .persist_manifest_deletion_watermark(&shard, 1, 2_000)
            .unwrap();
        assert_eq!(
            writer_b.read_read_horizon(&shard).unwrap(),
            Some(3),
            "a stale lower candidate is ignored by the monotonic durable watermark"
        );

        let stale_blob = ReadHorizonBlob { index: 1 };
        writer_b
            .store
            .put(
                &SegmentedObjectLog::<InMemoryBlobStore>::read_horizon_key(&shard),
                &serde_json::to_vec(&stale_blob).unwrap(),
            )
            .unwrap();
        assert_eq!(
            writer_b.read_read_horizon(&shard).unwrap(),
            Some(3),
            "a late lower blob write cannot regress the durable watermark"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkCannotSkipPhysicallyPresentPrefix() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 300 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 301 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        log.persist_manifest_deletion_watermark(&shard, 4, 1_000)
            .unwrap();
        assert_eq!(
            log.read_read_horizon(&shard).unwrap(),
            None,
            "a claimed below-floor boundary cannot advance while its segment objects still exist"
        );
        assert!(
            !store
                .list(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_prefix(&shard))
                .unwrap()
                .into_iter()
                .any(|key| key.ends_with("~watermark.json")),
            "no durable watermark marker is published before physical deletion completes"
        );

        assert_eq!(expire_as_owner(&log, &shard, 4, 1_001).unwrap(), 2);
        assert_eq!(
            log.read_manifest_deletion_watermark(&shard).unwrap(),
            Some(1)
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkStorageMonotonicNoRegression() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 400 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 401 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        assert_eq!(expire_as_owner(&log, &shard, 4, 1_000).unwrap(), 2);
        log.persist_manifest_deletion_watermark(&shard, 4, 1_001)
            .unwrap();
        assert_eq!(log.read_read_horizon(&shard).unwrap(), Some(1));

        log.persist_manifest_deletion_watermark(&shard, 1, 2_000)
            .unwrap();
        assert_eq!(
            log.read_read_horizon(&shard).unwrap(),
            Some(1),
            "a stale or lower candidate cannot regress the durable deletion watermark"
        );
    }

    #[test]
    fn deletion_watermark_proof_request_budget_is_linear_and_bounded() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let log = SegmentedObjectLog::open(store, cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..4u64 {
            log.enqueue(&shard, &pushes(2), 0, 500 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 501 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 3, 0).unwrap();
        let before = log.counters();
        assert_eq!(expire_as_owner(&log, &shard, 3, 1_000).unwrap(), 2);
        let after = log.counters();
        let reclaimed = 2;
        assert!(after.get_count - before.get_count <= 8 * reclaimed + 8);
        assert!(after.delete_count - before.delete_count <= 4 * reclaimed + 4);
        assert!(after.list_count - before.list_count <= 8 * reclaimed + 16);
        assert!(after.put_count - before.put_count <= 2 * reclaimed + 4);
    }

    #[test]
    fn forged_high_cache_cannot_suppress_standalone_authoritative_marker() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..4u64 {
            log.enqueue(&shard, &pushes(2), 0, 700 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 701 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 3, 0).unwrap();
        for entry in log
            .read_manifest(&shard)
            .unwrap()
            .into_iter()
            .filter(|entry| entry.segment_key.is_some() && entry.last_seq <= 3)
        {
            assert!(store.delete(entry.segment_key.as_deref().unwrap()).unwrap());
        }
        store
            .put(
                &SegmentedObjectLog::<InMemoryBlobStore>::read_horizon_key(&shard),
                &serde_json::to_vec(&ReadHorizonBlob { index: 99 }).unwrap(),
            )
            .unwrap();
        assert_eq!(log.read_manifest_deletion_watermark(&shard).unwrap(), None);

        log.persist_manifest_deletion_watermark(&shard, 3, 2_000)
            .unwrap();
        assert_eq!(
            log.read_manifest_deletion_watermark(&shard).unwrap(),
            Some(1),
            "standalone persistence must compare against marker authority, not the forged cache"
        );
    }

    #[test]
    fn authority_mode_deletion_proof_cost_ignores_total_head_history() {
        fn proof_counts(history: u64) -> (u64, u64, u64) {
            let store = Arc::new(CountingBlobStore::default());
            let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
            let shard = conformance_shard();
            let log = SegmentedObjectLog::open(store.clone(), cfg);
            log.create_queue(&conformance_qdef()).unwrap();
            for i in 0..2u64 {
                log.enqueue(&shard, &pushes(1), 0, 800 + i as i64 * 10)
                    .unwrap();
                log.seal(&shard, 0, 801 + i as i64 * 10).unwrap();
            }
            let entries = log.read_manifest(&shard).unwrap();
            for entry in &entries {
                store
                    .inner
                    .delete(entry.segment_key.as_deref().unwrap())
                    .unwrap();
            }
            let prefix =
                SegmentedObjectLog::<Arc<CountingBlobStore>>::authoritative_head_prefix(&shard);
            for version in 0..history {
                let head = ManifestHeadBlob {
                    current_epoch: 0,
                    next_seq: 2,
                    next_manifest_index: 2,
                    retention_floor_through: None,
                    tail_candidate_key: None,
                    legacy_next_manifest_index: 2,
                };
                store
                    .inner
                    .put(
                        &versioned_manifest_head_key(&prefix, version),
                        &serde_json::to_vec(&head).unwrap(),
                    )
                    .unwrap();
            }

            store.reset_request_counts();
            log.prove_completed_manifest_deletion_prefix(&shard, ManifestIndex(1), &entries, true)
                .unwrap();
            store.request_counts()
        }

        let short = proof_counts(8);
        let long = proof_counts(128);
        assert_eq!(short, (0, 2, 2));
        assert_eq!(
            long, short,
            "proof cost must not scale with retained head history"
        );
    }

    #[test]
    fn typed_manifest_identity_mismatch_is_rejected_before_storage_io() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();
        let log = SegmentedObjectLog::open(store, cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        log.enqueue(&shard, &pushes(1), 0, 600).unwrap();
        log.seal(&shard, 0, 601).unwrap();
        let entry = log.read_manifest(&shard).unwrap().remove(0);

        let before = log.counters();
        assert_eq!(
            log.commit_manifest_entry(
                &shard,
                ManifestIndex(entry.index + 1),
                AssignmentEpoch(entry.epoch),
                &entry,
                true,
            ),
            Err(EngineError::Conflict)
        );
        assert_eq!(
            log.commit_manifest_entry(
                &shard,
                ManifestIndex(entry.index),
                AssignmentEpoch(entry.epoch + 1),
                &entry,
                true,
            ),
            Err(EngineError::Conflict)
        );
        assert_eq!(log.counters(), before);
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeletionWatermarkOwnerFenceIndependenceDocumented() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let owner_a = SegmentedObjectLog::open(store.clone(), cfg);
        owner_a.create_queue(&conformance_qdef()).unwrap();
        owner_a.enqueue(&shard, &pushes(2), 0, 10).unwrap();
        owner_a.seal(&shard, 0, 11).unwrap();

        let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
        owner_b.create_queue(&conformance_qdef()).unwrap();
        assert_eq!(owner_b.acquire_epoch(&shard, 100).unwrap(), 1);
        for i in 0..3u64 {
            owner_b
                .enqueue(&shard, &pushes(2), 1, 200 + i as i64 * 10)
                .unwrap();
            owner_b.seal(&shard, 1, 201 + i as i64 * 10).unwrap();
        }
        owner_b
            .advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 1, 5), 1)
            .unwrap();
        expire_as_owner(&owner_b, &shard, 5, 1_000).unwrap();

        let horizon = owner_b
            .read_read_horizon(&shard)
            .unwrap()
            .expect("watermark advanced");
        assert!(
            horizon >= 1,
            "the watermark advances, but it does not replace the ownership fence"
        );
        assert_eq!(
            owner_b.current_epoch(&shard).unwrap(),
            1,
            "permanent head remains the stale-writer fence"
        );
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_key(
                    &shard, 1
                ))
                .unwrap()
                .is_some(),
            "the durable head object still exists and continues to fence stale writers"
        );
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(
                    &shard, 1
                ))
                .unwrap()
                .is_none(),
            "the legacy compatibility mirror is freeable after the retained head proves the fence"
        );

        owner_a.enqueue(&shard, &pushes(3), 0, 5_000).unwrap();
        let err = owner_a.seal(&shard, 0, 5_001).unwrap_err();
        assert_eq!(
            err,
            EngineError::EpochFenced,
            "the stale writer is fenced by the permanent head CAS, not by the watermark"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestPermanentFenceSurvivesReopen() {
        let store = Arc::new(CountingBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        writer.enqueue(&shard, &pushes(1), 0, 10).unwrap();
        writer.seal(&shard, 0, 11).unwrap();
        writer.enqueue(&shard, &pushes(1), 0, 20).unwrap();
        writer.seal(&shard, 0, 21).unwrap();
        advance_floor_as_owner(&writer, &shard, 0, 0).unwrap();
        assert_eq!(
            expire_as_owner(&writer, &shard, 0, 1_000).unwrap(),
            1,
            "the first manifest index is reclaimed and the live tail remains available"
        );
        assert_eq!(writer.read_read_horizon(&shard).unwrap(), Some(0));

        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        {
            let mut g = reopened.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(&shard).expect("reopened shard");
            assert_eq!(
                buf.manifest_deletion_watermark,
                Some(0),
                "the reopened shard reloads the reclaimed-index fence before the stale owner tries to seal"
            );
            buf.next_manifest_index = 0;
        }
        reopened.enqueue(&shard, &pushes(1), 0, 30).unwrap();
        let object_count = store.inner.object_count();
        store.reset_list_count();

        let err = reopened.seal(&shard, 0, 31).unwrap_err();
        assert!(
            matches!(err, EngineError::EpochFenced | EngineError::Conflict),
            "a stale reopened owner must not seal against the reclaimed index"
        );
        assert_eq!(
            store.inner.object_count(),
            object_count,
            "the stale reopened writer is rejected before any new segment or manifest object is written"
        );
        assert_eq!(
            store.list_count(),
            0,
            "the normal seal path does not introduce a manifest LIST once the fence is reloaded"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestReopenFenceReloadsBeforeSeal() {
        let store = Arc::new(CountingBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        writer.enqueue(&shard, &pushes(1), 0, 10).unwrap();
        writer.seal(&shard, 0, 11).unwrap();
        writer.enqueue(&shard, &pushes(1), 0, 20).unwrap();
        writer.seal(&shard, 0, 21).unwrap();
        advance_floor_as_owner(&writer, &shard, 0, 0).unwrap();
        expire_as_owner(&writer, &shard, 0, 1_000).unwrap();

        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        {
            let mut g = reopened.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(&shard).expect("reopened shard");
            assert_eq!(
                buf.manifest_deletion_watermark,
                Some(0),
                "open/recovery reloads the durable reclaimed-index fence into the shard cache"
            );
            buf.next_manifest_index = 0;
        }

        reopened.enqueue(&shard, &pushes(1), 0, 30).unwrap();
        let object_count = store.inner.object_count();
        store.reset_list_count();

        let err = reopened.seal(&shard, 0, 31).unwrap_err();
        assert!(
            matches!(err, EngineError::EpochFenced | EngineError::Conflict),
            "the recovered fence is consulted before seal can commit against the cached next_manifest_index"
        );
        assert_eq!(
            store.inner.object_count(),
            object_count,
            "seal returns before writing a segment or manifest object"
        );
        assert_eq!(
            store.list_count(),
            0,
            "seal does not introduce a manifest LIST on the normal hot path"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestReclamationDoesNotHidePartialExpirySegments() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }
        let seg0_legacy_key = SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 0);
        let seg1_legacy_key = SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 1);
        let seg2_legacy_key = SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 2);
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        log.set_fault_hook(Some(std::sync::Arc::new(PartialExpireFault::new())));
        let err = expire_as_owner(&log, &shard, 5, 1_000).unwrap_err();
        assert_eq!(err, EngineError::Conflict);

        assert!(
            store.get(&seg1_legacy_key).unwrap().is_some(),
            "the not-yet-reclaimed below-floor legacy manifest entry stays visible until reclamation completes"
        );

        log.set_fault_hook(None);
        assert_eq!(
            expire_as_owner(&log, &shard, 5, 2_000).unwrap(),
            2,
            "a later full expiry still finds the remaining below-floor entries"
        );
        assert!(
            store.get(&seg0_legacy_key).unwrap().is_none(),
            "the first reclaimed legacy manifest copy is physically deleted after the full pass"
        );
        assert!(
            store.get(&seg1_legacy_key).unwrap().is_none(),
            "the second reclaimed legacy manifest copy is physically deleted after the full pass"
        );
        assert!(
            store.get(&seg2_legacy_key).unwrap().is_none(),
            "the fully reclaimed below-floor legacy manifest copy is physically deleted after the full pass"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestPartialExpireDoesNotHideUndeletedManifestEntries() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }
        let seg2_legacy_key = SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 2);
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        assert_eq!(
            expire_as_owner(&log, &shard, 4, 1_000).unwrap(),
            2,
            "a partial below-floor pass only reclaims the entries at or below the requested through_seq"
        );
        assert_eq!(
            log.read_read_horizon(&shard).unwrap(),
            Some(1),
            "the recovered watermark stays below the undeleted below-floor manifest entry"
        );
        assert!(
            store.get(&seg2_legacy_key).unwrap().is_some(),
            "the below-floor manifest entry whose segment object was not deleted stays visible"
        );

        assert_eq!(
            expire_as_owner(&log, &shard, 5, 2_000).unwrap(),
            1,
            "a later full pass still finds and reclaims the undeleted below-floor entry"
        );
        assert!(
            store.get(&seg2_legacy_key).unwrap().is_none(),
            "the final full pass physically deletes the previously undeleted below-floor manifest copy"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestExpireDeletesEligibleLegacyManifestObjectsAfterSegmentReclaim() {
        let store = std::sync::Arc::new(RecordingDeleteBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }
        let segment_keys: Vec<_> = log
            .read_manifest(&shard)
            .unwrap()
            .into_iter()
            .filter_map(|entry| entry.segment_key)
            .collect();
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        assert_eq!(expire_as_owner(&log, &shard, 5, 1_000).unwrap(), 3);

        let events = store.delete_events();
        for (index, segment_key) in segment_keys.iter().enumerate() {
            let segment_pos = events
                .iter()
                .position(|key| key == segment_key)
                .unwrap_or_else(|| panic!("segment delete should be recorded: {events:?}"));
            let legacy_pos = events
                .iter()
                .position(|key| {
                    key == &SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(
                        &shard,
                        index as u64,
                    )
                })
                .unwrap_or_else(|| panic!("legacy manifest delete should be recorded: {events:?}"));
            assert!(
                segment_pos < legacy_pos,
                "legacy manifest delete must follow its segment delete: {events:?}"
            );
        }
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestHeadFenceRemainsOccupiedAfterReclaim() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let stale_writer = SegmentedObjectLog::open(store.clone(), cfg);
        stale_writer.create_queue(&conformance_qdef()).unwrap();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        for i in 0..2u64 {
            writer
                .enqueue(&shard, &pushes(2), 0, 100 + i as i64 * 10)
                .unwrap();
            writer.seal(&shard, 0, 101 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&writer, &shard, 3, 0).unwrap();
        assert_eq!(expire_as_owner(&writer, &shard, 3, 1_000).unwrap(), 2);

        let head_key = SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_key(&shard, 1);
        let head_bytes = store
            .get(&head_key)
            .unwrap()
            .expect("authoritative manifest_head entry should remain occupied");
        let head: ManifestEntry = serde_json::from_slice(&head_bytes).unwrap();
        assert_eq!(
            head.compacted_through_index,
            Some(1),
            "the occupied head entry is rewritten as a reclaimed marker"
        );
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(
                    &shard, 1
                ))
                .unwrap()
                .is_none(),
            "the trim removes only the legacy compatibility copy, not the authoritative head"
        );

        stale_writer.enqueue(&shard, &pushes(1), 0, 2_000).unwrap();
        let err = stale_writer.seal(&shard, 0, 2_001).unwrap_err();
        assert!(
            matches!(err, EngineError::EpochFenced | EngineError::Conflict),
            "the stale writer still collides with the occupied manifest_head address"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestManifestDeleteFailureDoesNotSkipUndeletedPrefix() {
        let store = std::sync::Arc::new(RecordingDeleteBlobStore::default());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            log.enqueue(&shard, &pushes(2), 0, 200 + i as i64 * 10)
                .unwrap();
            log.seal(&shard, 0, 201 + i as i64 * 10).unwrap();
        }
        advance_floor_as_owner(&log, &shard, 5, 0).unwrap();

        let failing_legacy = SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 1);
        store.fail_delete_once(&failing_legacy);
        let err = expire_as_owner(&log, &shard, 5, 1_000).unwrap_err();
        assert!(
            matches!(err, EngineError::Conflict),
            "a legacy manifest delete failure should surface so the pass can be retried"
        );
        assert_eq!(
            log.read_read_horizon(&shard).unwrap(),
            Some(0),
            "the durable read horizon must stop before the undeleted legacy manifest gap"
        );
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(
                    &shard, 1
                ))
                .unwrap()
                .is_some(),
            "the undeleted legacy manifest entry must remain available for retry"
        );

        store.clear_delete_failure();
        assert_eq!(
            expire_as_owner(&log, &shard, 5, 2_000).unwrap(),
            1,
            "the retry resumes from the undeleted prefix and finishes reclaiming the remaining segment"
        );
        assert_eq!(log.read_read_horizon(&shard).unwrap(), Some(2));
        assert!(
            store
                .get(&SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(
                    &shard, 1
                ))
                .unwrap()
                .is_none(),
            "the retry physically removes the legacy manifest gap"
        );
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestLegacyManifestBootstrapFailsClosedWithoutReclaimedFenceHistory() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let stale_writer = SegmentedObjectLog::open(store.clone(), cfg);
        stale_writer.create_queue(&conformance_qdef()).unwrap();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        for i in 0..3u64 {
            writer
                .enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            writer.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }

        advance_floor_as_owner(&writer, &shard, 1, 0).unwrap();
        assert_eq!(expire_as_owner(&writer, &shard, 1, 1_000).unwrap(), 1);

        strip_manifest_head_namespace(&store, &shard);

        let head_prefix = SegmentedObjectLog::<InMemoryBlobStore>::manifest_head_prefix(&shard);
        assert!(
            store.list(&head_prefix).unwrap().is_empty(),
            "the fixture must recover without any authoritative head keys"
        );
        let legacy_prefix = SegmentedObjectLog::<InMemoryBlobStore>::manifest_prefix(&shard);
        assert_eq!(
            store.list(&legacy_prefix).unwrap(),
            vec![
                SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 1),
                SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 2),
                SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 3),
                SegmentedObjectLog::<InMemoryBlobStore>::manifest_key(&shard, 4),
            ],
            "only the legacy manifest namespace remains"
        );

        let reopened = SegmentedObjectLog::open(store, cfg);
        assert_eq!(
            reopened.create_queue(&conformance_qdef()),
            Err(EngineError::Conflict),
            "legacy fallback with a reclaimed genesis gap and no retained head/marker authority must fail closed"
        );

        // The stale process is not safe to serve once an external actor has deleted the retained head
        // namespace. The safety property is that no replacement process adopts this incomplete legacy tail.
        drop(stale_writer);
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestLegacyBootstrapNotRemoved() {
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let shard = conformance_shard();

        let writer = SegmentedObjectLog::open(store.clone(), cfg);
        writer.create_queue(&conformance_qdef()).unwrap();
        for i in 0..2u64 {
            writer
                .enqueue(&shard, &pushes(2), 0, 10 + i as i64 * 10)
                .unwrap();
            writer.seal(&shard, 0, 11 + i as i64 * 10).unwrap();
        }

        strip_manifest_head_namespace(&store, &shard);

        let reopened = SegmentedObjectLog::open(store.clone(), cfg);
        reopened.create_queue(&conformance_qdef()).unwrap();
        let recovered = reopened.read_manifest(&shard).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "a queue with only legacy manifest keys still recovers its entries"
        );
        assert_eq!(
            recovered.last().map(|entry| entry.last_seq),
            Some(3),
            "the tail state comes back from the legacy manifest namespace"
        );
        reopened.enqueue(&shard, &pushes(1), 0, 30).unwrap();
        let ack = reopened.seal(&shard, 0, 31).unwrap();
        assert_eq!(ack[0], CommandPosition::new(shard.clone(), 0, 4));
    }

    #[test]
    #[allow(non_snake_case)]
    fn TestPartialExpireVisibilityHelperKeepsUndeletedBelowFloorEntry() {
        // Build the lagging-partial-expire fixture (8 data segments, 2 commands each = seqs 0..15,
        // floor advanced to seq 15, first 2 segments physically deleted, durable watermark at index 1).
        let store = std::sync::Arc::new(InMemoryBlobStore::new());
        let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        let shard = conformance_shard();

        log.create_queue(&conformance_qdef()).unwrap();
        for i in 0..8u64 {
            log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
                .unwrap();
            log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
        }

        // Advance floor to seq 15, then delete the first 2 segment objects and persist watermark.
        advance_floor_as_owner(&log, &shard, 15, 0).unwrap();
        let entries = log.read_manifest(&shard).unwrap();
        let seg_keys: Vec<String> = entries
            .iter()
            .filter_map(|e| e.segment_key.clone())
            .collect();
        for seg_key in seg_keys.iter().take(2) {
            assert!(store.delete(seg_key).unwrap());
        }
        log.persist_manifest_deletion_watermark(&shard, 3, 1_000)
            .unwrap();
        assert_eq!(
            log.read_read_horizon(&shard).unwrap(),
            Some(1),
            "durable watermark at index 1 (only entries 0,1 reclaimed)"
        );

        // Entry at index 0: data, visible_last_seq=1, index 0 <= watermark 1 → HiddenAsReclaimed
        // when reclaimed_through >= visible_last_seq.  With reclaimed_through=3 (> 1) the data
        // check says reclaimed AND index <= watermark → HiddenAsReclaimed.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                0,        // entry_index
                0,        // first_seq
                1,        // visible_last_seq
                false,    // fence
                None,     // retention_floor_through
                None,     // compacted_through_index
                Some(1),  // durable_watermark
                3,        // reclaimed_through
                Some(15), // floor_seq
            ),
            PartialExpireVisibility::HiddenAsReclaimed,
            "entry at reclaimed index 0 must be HiddenAsReclaimed when reclaimed_through covers it \
             and index is at or below the durable watermark"
        );

        // Entry at index 1: data, visible_last_seq=3, index 1 <= watermark 1 → HiddenAsReclaimed
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                1,
                2,
                3,
                false,
                None,
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::HiddenAsReclaimed,
            "entry at reclaimed index 1 must be HiddenAsReclaimed when reclaimed_through covers it"
        );

        // Entry at index 2: data, visible_last_seq=5, index 2 > watermark 1.
        // With reclaimed_through=3 (< visible_last_seq=5): data check says NOT reclaimed
        // and visible_last_seq <= floor_seq → StopHiddenPrefix (first undeleted below-floor entry).
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                2,
                4,
                5,
                false,
                None,
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::StopHiddenPrefix,
            "below-floor data entry at index 2 must be StopHiddenPrefix when reclaimed_through \
             is below visible_last_seq"
        );

        // Entry at index 2 with reclaimed_through=7 (> visible_last_seq=5): data check says
        // reclaimed BUT index 2 > watermark 1 → StopHiddenPrefix (watermark defense stops prefix).
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                2,
                4,
                5,
                false,
                None,
                None,
                Some(1),
                7,
                Some(15),
            ),
            PartialExpireVisibility::StopHiddenPrefix,
            "below-floor data entry at index 2 must be StopHiddenPrefix when above the durable \
             watermark even if reclaimed_through advanced past visible_last_seq"
        );

        // Entry at index 6 (first_seq=12, visible_last_seq=13, floor_seq=15): below floor,
        // not reclaimed → StopHiddenPrefix (the hidden prefix cannot skip past it).
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                6,
                12,
                13,
                false,
                None,
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::StopHiddenPrefix,
            "below-floor undeleted data entry at index 6 must be StopHiddenPrefix"
        );

        // Floor-advance entry (superseded, below authoritative floor) at index 2 > watermark 1:
        // reclaimed by data check but index above watermark → StopHiddenPrefix.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                2,
                0,
                0,
                false,
                Some(3),
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::StopHiddenPrefix,
            "superseded floor-advance entry above the durable watermark must be StopHiddenPrefix"
        );

        // Same entry at index 0 <= watermark 1: reclaimed by data check AND index below
        // watermark → HiddenAsReclaimed.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                0,
                0,
                0,
                false,
                Some(3),
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::HiddenAsReclaimed,
            "superseded floor-advance entry at or below the durable watermark must be HiddenAsReclaimed"
        );

        // Authoritative floor entry (retention_floor_through == floor_seq): always Visible.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                3,
                0,
                0,
                false,
                Some(15),
                None,
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::Visible,
            "authoritative floor entry must always be Visible"
        );

        // Reclaimed manifest marker (compacted_through_index is Some): HiddenAsReclaimed.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                0,
                0,
                0,
                false,
                None,
                Some(0),
                Some(1),
                3,
                Some(15),
            ),
            PartialExpireVisibility::HiddenAsReclaimed,
            "reclaimed manifest marker must be HiddenAsReclaimed"
        );

        // No floor (None): every entry is Visible.
        assert_eq!(
            SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
                0,
                0,
                1,
                false,
                None,
                None,
                Some(1),
                3,
                None,
            ),
            PartialExpireVisibility::Visible,
            "every entry must be Visible when there is no durable floor"
        );
    }
}
