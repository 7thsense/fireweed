//! The sequencing seam: offset assignment, the offset→location index, and
//! retention. The engine owns storage + buffering; a `Sequencer` owns offsets.

use crate::ObjectLogError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Opaque, engine-visible identifier for an independent offset stream (one dense,
/// monotonic offset space). A Kafka broker maps `(topic, partition)` onto one of
/// these; a WAL maps a shard. object-log treats it as an opaque key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionKey(pub String);

impl PartitionKey {
    /// Borrow the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Where a batch's bytes live inside its object. **Authored by the engine** (it
/// owns object layout); the sequencer stores it in the index and returns it from
/// [`Sequencer::lookup`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchLocation {
    /// Key of the object holding the batch.
    pub object_id: String,
    /// Byte offset of the batch within the object.
    pub byte_start: u32,
    /// Byte length of the batch.
    pub byte_len: u32,
}

/// One batch presented to [`Sequencer::commit`]. The engine fills `partition`,
/// `record_count`, and `location` (all engine-visible); `meta` is forwarded
/// **uninterpreted** — only the sequencer reads it.
pub struct CommitBatch<'a, M> {
    /// The offset stream this batch belongs to.
    pub partition: PartitionKey,
    /// Number of records in the batch (offsets advance by this).
    pub record_count: i32,
    /// Where the batch lives in its (already-durable) object.
    pub location: BatchLocation,
    /// Sequencer-private metadata (e.g. idempotent-producer identity). Opaque to
    /// the engine.
    pub meta: &'a M,
}

/// Per-batch result of [`Sequencer::commit`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOutcome {
    /// A fresh, contiguous offset range was assigned starting at `base_offset`.
    Assigned {
        /// First offset assigned to the batch.
        base_offset: i64,
        /// Number of records committed.
        record_count: i32,
    },
    /// A retried batch was recognized as already committed (idempotent no-op);
    /// the original `base_offset` is returned.
    Duplicate {
        /// The originally assigned first offset.
        base_offset: i64,
    },
}

/// An index entry resolving an offset range to its bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Where the batch's bytes live.
    pub location: BatchLocation,
    /// First offset in the batch.
    pub base_offset: i64,
    /// Number of records in the batch.
    pub record_count: i32,
}

/// The linearization point: assigns offsets to durably-stored batches and owns
/// the offset→location index.
///
/// **Synchronous on purpose** — a lin-point is a critical section, not async I/O.
/// The engine calls it from its flush worker (a dedicated thread / blocking
/// task), so a blocking implementation (a `Mutex`, a SQL transaction) is fine.
///
/// Contract: [`commit`](Sequencer::commit) is **atomic across the whole slice**
/// (all batches in one object commit together or not at all), and the engine
/// presents batches for any single [`PartitionKey`] in arrival order and never
/// splits one partition across concurrent `commit` calls.
pub trait Sequencer: Send + Sync {
    /// Sequencer-private per-batch metadata, forwarded uninterpreted by the
    /// engine. object-log's default sequencers use `()`; a Kafka coordinator uses
    /// its producer-identity fields.
    type Meta: Send + Sync;

    /// Assign offsets to a just-PUT object's batches and persist the index.
    /// Returns one [`CommitOutcome`] per input batch, in order. Atomic: on `Err`,
    /// nothing is committed.
    fn commit(
        &self,
        batches: &[CommitBatch<'_, Self::Meta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError>;

    /// Resolve `fetch_offset` to the ordered index entries covering it onward.
    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError>;

    /// The next offset to be assigned (index-only; no object reads).
    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;

    /// The first readable offset (advances on [`truncate_before`](Sequencer::truncate_before)).
    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError>;

    /// Retention MECHANISM (not policy): drop index entries below `offset` and
    /// return object ids that now have **no** live references from **any**
    /// partition (objects are multiplexed and shared), for the engine to delete.
    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError>;
}

/// Per-partition in-memory index state.
#[derive(Default)]
struct PartState {
    next_offset: i64,
    log_start: i64,
    entries: Vec<IndexEntry>,
}

/// A single-node, in-memory [`Sequencer`] (`Meta = ()`): assigns dense offsets and
/// keeps the offset→location index in memory.
///
/// The log *bytes* live durably in the `BlobStore`, but this index does not
/// survive a restart — use it for tests and single-process work, or persist the
/// index yourself for a crash-durable standalone log.
#[derive(Default)]
pub struct InMemorySequencer {
    state: Mutex<HashMap<PartitionKey, PartState>>,
}

impl InMemorySequencer {
    /// Create an empty sequencer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Object ids referenced by any live index entry (for orphan reaping).
    pub fn live_object_ids(&self) -> HashSet<String> {
        let state = self.state.lock().expect("poisoned");
        let mut live = HashSet::new();
        for ps in state.values() {
            for e in &ps.entries {
                live.insert(e.location.object_id.clone());
            }
        }
        live
    }
}

impl Sequencer for InMemorySequencer {
    type Meta = ();

    fn commit(
        &self,
        batches: &[CommitBatch<'_, Self::Meta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut state = self.state.lock().expect("poisoned");
        let mut outcomes = Vec::with_capacity(batches.len());
        for cb in batches {
            let ps = state.entry(cb.partition.clone()).or_default();
            let base = ps.next_offset;
            ps.entries.push(IndexEntry {
                location: cb.location.clone(),
                base_offset: base,
                record_count: cb.record_count,
            });
            ps.next_offset = base + cb.record_count as i64;
            outcomes.push(CommitOutcome::Assigned {
                base_offset: base,
                record_count: cb.record_count,
            });
        }
        Ok(outcomes)
    }

    fn lookup(
        &self,
        partition: &PartitionKey,
        fetch_offset: i64,
    ) -> Result<Vec<IndexEntry>, ObjectLogError> {
        let state = self.state.lock().expect("poisoned");
        let Some(ps) = state.get(partition) else {
            return Ok(Vec::new());
        };
        Ok(ps
            .entries
            .iter()
            .filter(|e| e.base_offset + e.record_count as i64 > fetch_offset)
            .cloned()
            .collect())
    }

    fn high_watermark(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .state
            .lock()
            .expect("poisoned")
            .get(partition)
            .map_or(0, |ps| ps.next_offset))
    }

    fn log_start_offset(&self, partition: &PartitionKey) -> Result<i64, ObjectLogError> {
        Ok(self
            .state
            .lock()
            .expect("poisoned")
            .get(partition)
            .map_or(0, |ps| ps.log_start))
    }

    fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<Vec<String>, ObjectLogError> {
        let mut state = self.state.lock().expect("poisoned");
        let mut dropped: Vec<String> = Vec::new();
        match state.get_mut(partition) {
            Some(ps) => {
                for e in ps.entries.iter() {
                    if e.base_offset + e.record_count as i64 <= offset {
                        dropped.push(e.location.object_id.clone());
                    }
                }
                ps.entries
                    .retain(|e| e.base_offset + e.record_count as i64 > offset);
                if offset > ps.log_start {
                    ps.log_start = offset.min(ps.next_offset);
                }
            }
            None => return Ok(Vec::new()),
        }
        // An object is reclaimable only when no entry in ANY partition references it.
        let mut live: HashSet<String> = HashSet::new();
        for ps in state.values() {
            for e in &ps.entries {
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
