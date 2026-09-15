//! A `BlobStore`-persisted [`Sequencer`]: the offset→location index survives a
//! restart, so a standalone object-log is crash-durable end to end.
//!
//! Each `commit` durably writes one manifest object (≤1 PUT per group-commit,
//! co-amortized with the data object) recording the batches it assigned. On
//! [`open`](ManifestSequencer::open) the index is rebuilt by replaying the
//! manifest objects in order.

use crate::{
    BlobStore, CommitBatch, CommitOutcome, IndexEntry, ObjectLogError, PartitionKey, Sequencer,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Runtime};

#[derive(Serialize, Deserialize)]
struct ManifestRecord {
    entries: Vec<(PartitionKey, IndexEntry)>,
}

#[derive(Default)]
struct Part {
    next_offset: i64,
    log_start: i64,
    entries: Vec<IndexEntry>,
}

struct Inner {
    parts: HashMap<PartitionKey, Part>,
    counter: u64,
}

/// A [`Sequencer`] (`Meta = ()`) that persists its index to a [`BlobStore`].
pub struct ManifestSequencer {
    blob: Arc<dyn BlobStore>,
    prefix: String,
    // `Option` so `Drop` can `shutdown_background()` — dropping a `Runtime` inside
    // an async context (e.g. an `Arc` last-ref drop in a `#[tokio::test]`) panics.
    rt: Option<Runtime>,
    inner: Mutex<Inner>,
}

impl Drop for ManifestSequencer {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl ManifestSequencer {
    /// Open (or recover) a persisted sequencer. Manifest objects live under
    /// `manifest_prefix` (keep it disjoint from the engine's data-object prefix).
    /// Existing manifests are replayed to rebuild the index.
    pub async fn open(
        blob: Arc<dyn BlobStore>,
        manifest_prefix: impl Into<String>,
    ) -> Result<Self, ObjectLogError> {
        let prefix = manifest_prefix.into();
        let mut keys = blob.list(&prefix).await?;
        keys.sort();
        let mut parts: HashMap<PartitionKey, Part> = HashMap::new();
        for key in &keys {
            let bytes = blob
                .get(key)
                .await?
                .ok_or_else(|| ObjectLogError::MissingObject(key.clone()))?;
            let rec: ManifestRecord = serde_json::from_slice(&bytes)
                .map_err(|e| ObjectLogError::Sequencer(format!("manifest {key}: {e}")))?;
            for (pkey, entry) in rec.entries {
                let p = parts.entry(pkey).or_default();
                let end = entry.base_offset + entry.record_count as i64;
                if end > p.next_offset {
                    p.next_offset = end;
                }
                p.entries.push(entry);
            }
        }
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ObjectLogError::StorageUnavailable(e.to_string()))?;
        Ok(Self {
            blob,
            prefix,
            rt: Some(rt),
            inner: Mutex::new(Inner {
                parts,
                counter: keys.len() as u64,
            }),
        })
    }

    /// Object ids referenced by any live index entry (for orphan reaping).
    /// Does not include manifest object keys themselves.
    pub fn live_object_ids(&self) -> HashSet<String> {
        let st = self.inner.lock().expect("poisoned");
        let mut live = HashSet::new();
        for p in st.parts.values() {
            for e in &p.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        live
    }

    /// Point-in-time view of the rebuilt index (diagnostics / CLI).
    pub fn snapshot(&self) -> IndexSnapshot {
        let st = self.inner.lock().expect("poisoned");
        let mut partitions: Vec<PartitionSnapshot> = st
            .parts
            .iter()
            .map(|(k, p)| PartitionSnapshot {
                partition: k.0.clone(),
                log_start: p.log_start,
                high_watermark: p.next_offset,
                entry_count: p.entries.len(),
                entries: p.entries.clone(),
            })
            .collect();
        partitions.sort_by(|a, b| a.partition.cmp(&b.partition));
        IndexSnapshot {
            manifest_prefix: self.prefix.clone(),
            manifest_count: st.counter,
            partitions,
        }
    }
}

/// Full index view from [`ManifestSequencer::snapshot`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshot {
    /// Prefix used for manifest objects.
    pub manifest_prefix: String,
    /// Number of manifest objects replayed (commit counter).
    pub manifest_count: u64,
    /// Per-partition index state.
    pub partitions: Vec<PartitionSnapshot>,
}

/// One partition's index bounds and entries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    /// Partition key string.
    pub partition: String,
    /// First readable offset.
    pub log_start: i64,
    /// Next offset to assign (high watermark).
    pub high_watermark: i64,
    /// Number of index entries.
    pub entry_count: usize,
    /// Ordered index entries (may be large; CLI can omit via flag).
    pub entries: Vec<IndexEntry>,
}

impl Sequencer for ManifestSequencer {
    type Meta = ();

    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut st = self.inner.lock().expect("poisoned");
        // Plan the assignment without mutating the index yet.
        let mut local_next: HashMap<PartitionKey, i64> = HashMap::new();
        let mut planned: Vec<(PartitionKey, IndexEntry)> = Vec::with_capacity(batches.len());
        let mut outcomes = Vec::with_capacity(batches.len());
        for cb in batches {
            let base = *local_next
                .entry(cb.partition.clone())
                .or_insert_with(|| st.parts.get(&cb.partition).map_or(0, |p| p.next_offset));
            let entry = IndexEntry {
                location: cb.location.clone(),
                base_offset: base,
                record_count: cb.record_count,
            };
            planned.push((cb.partition.clone(), entry));
            *local_next.get_mut(&cb.partition).expect("inserted above") =
                base + cb.record_count as i64;
            outcomes.push(CommitOutcome::Assigned {
                base_offset: base,
                record_count: cb.record_count,
            });
        }

        // Persist the manifest durably BEFORE the index becomes visible.
        let counter = st.counter + 1;
        let key = format!("{}{:020}", self.prefix, counter);
        let bytes = serde_json::to_vec(&ManifestRecord {
            entries: planned.clone(),
        })
        .map_err(|e| ObjectLogError::Sequencer(e.to_string()))?;
        self.rt
            .as_ref()
            .expect("runtime present until drop")
            .block_on(self.blob.put(&key, Bytes::from(bytes)))?;

        // Apply.
        for (pkey, entry) in planned {
            let p = st.parts.entry(pkey).or_default();
            let end = entry.base_offset + entry.record_count as i64;
            if end > p.next_offset {
                p.next_offset = end;
            }
            p.entries.push(entry);
        }
        st.counter = counter;
        Ok(outcomes)
    }

    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError> {
        let st = self.inner.lock().expect("poisoned");
        let Some(p) = st.parts.get(partition) else {
            return Ok(Vec::new());
        };
        Ok(p.entries
            .iter()
            .filter(|e| e.base_offset + e.record_count as i64 > fetch_offset)
            .cloned()
            .collect())
    }

    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .parts
            .get(partition)
            .map_or(0, |p| p.next_offset))
    }

    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .inner
            .lock()
            .expect("poisoned")
            .parts
            .get(partition)
            .map_or(0, |p| p.log_start))
    }

    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        let mut st = self.inner.lock().expect("poisoned");
        let mut dropped: Vec<String> = Vec::new();
        match st.parts.get_mut(partition) {
            Some(p) => {
                for e in p.entries.iter() {
                    if e.base_offset + e.record_count as i64 <= offset {
                        dropped.push(e.location.object_id.clone());
                    }
                }
                p.entries
                    .retain(|e| e.base_offset + e.record_count as i64 > offset);
                if offset > p.log_start {
                    p.log_start = offset.min(p.next_offset);
                }
            }
            None => return Ok(Vec::new()),
        }
        let mut live: HashSet<String> = HashSet::new();
        for p in st.parts.values() {
            for e in &p.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        let mut dead: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for oid in dropped {
            if !live.contains(&oid) && seen.insert(oid.clone()) {
                dead.push(oid);
            }
        }
        Ok(dead)
    }
}
