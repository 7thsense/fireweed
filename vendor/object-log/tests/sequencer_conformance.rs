//! Shared Sequencer behavioral suite (CONTRACT-001 / TD-003).
//!
//! Run against in-crate sequencers so third-party implementors have a template
//! of cases their `Sequencer` must satisfy before binding to `LogEngine`.

use object_log::{
    BatchLocation, BlobStore, CommitBatch, CommitOutcome, InMemorySequencer, IndexEntry,
    ManifestSequencer, MemoryBlobStore, ObjectLogError, PartitionKey, Sequencer,
};
use std::sync::Arc;

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

fn loc(object_id: &str, start: u32, len: u32) -> BatchLocation {
    BatchLocation {
        object_id: object_id.into(),
        byte_start: start,
        byte_len: len,
    }
}

/// Core index/offset behaviors every `Sequencer<Meta = ()>` must satisfy.
fn suite_unit_meta(seq: &impl Sequencer<Meta = ()>) {
    let p0 = pk("part-0");
    let p1 = pk("part-1");
    let meta = ();

    // Empty partition bounds.
    assert_eq!(seq.high_watermark(&p0).unwrap(), 0);
    assert_eq!(seq.log_start_offset(&p0).unwrap(), 0);
    assert!(seq.lookup(&p0, 0).unwrap().is_empty());

    // Dense assignment, multi-partition atomic slice.
    let batches = [
        CommitBatch {
            partition: p0.clone(),
            record_count: 2,
            location: loc("obj-a", 0, 10),
            meta: &meta,
        },
        CommitBatch {
            partition: p1.clone(),
            record_count: 1,
            location: loc("obj-a", 10, 5),
            meta: &meta,
        },
        CommitBatch {
            partition: p0.clone(),
            record_count: 3,
            location: loc("obj-a", 15, 7),
            meta: &meta,
        },
    ];
    let outcomes = seq.commit(&batches).unwrap();
    assert_eq!(outcomes.len(), 3);
    assert_eq!(
        outcomes[0],
        CommitOutcome::Assigned {
            base_offset: 0,
            record_count: 2
        }
    );
    assert_eq!(
        outcomes[1],
        CommitOutcome::Assigned {
            base_offset: 0,
            record_count: 1
        }
    );
    assert_eq!(
        outcomes[2],
        CommitOutcome::Assigned {
            base_offset: 2,
            record_count: 3
        }
    );
    assert_eq!(seq.high_watermark(&p0).unwrap(), 5);
    assert_eq!(seq.high_watermark(&p1).unwrap(), 1);

    // Lookup from mid-range returns covering entries onward.
    let mid = seq.lookup(&p0, 2).unwrap();
    assert_eq!(mid.len(), 1);
    assert_eq!(mid[0].base_offset, 2);
    assert_eq!(mid[0].record_count, 3);

    let from_zero = seq.lookup(&p0, 0).unwrap();
    assert_eq!(from_zero.len(), 2);
    assert_eq!(from_zero[0].base_offset, 0);
    assert_eq!(from_zero[1].base_offset, 2);

    // Second object on p0; shared object still live after partial truncate.
    let more = [CommitBatch {
        partition: p0.clone(),
        record_count: 1,
        location: loc("obj-b", 0, 4),
        meta: &meta,
    }];
    let r = seq.commit(&more).unwrap();
    assert_eq!(
        r[0],
        CommitOutcome::Assigned {
            base_offset: 5,
            record_count: 1
        }
    );
    assert_eq!(seq.high_watermark(&p0).unwrap(), 6);

    // Truncate before 5: drops p0 entries ending at/before 5 (first two on obj-a).
    // obj-a still referenced by p1 → not reclaimable. obj-b still live.
    let dead = seq.truncate_before(&p0, 5).unwrap();
    assert!(
        !dead.iter().any(|id| id == "obj-a"),
        "obj-a still referenced by p1: {dead:?}"
    );
    assert!(!dead.iter().any(|id| id == "obj-b"));
    assert_eq!(seq.log_start_offset(&p0).unwrap(), 5);
    let remaining = seq.lookup(&p0, 0).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].base_offset, 5);

    // Drop p1 → obj-a becomes unreferenced.
    let dead2 = seq.truncate_before(&p1, 1).unwrap();
    assert!(
        dead2.iter().any(|id| id == "obj-a"),
        "obj-a reclaimable after last ref: {dead2:?}"
    );
}

#[test]
fn in_memory_sequencer_conforms() {
    suite_unit_meta(&InMemorySequencer::new());
}

#[tokio::test]
async fn manifest_sequencer_conforms() {
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_m/")
            .await
            .expect("open"),
    );
    // ManifestSequencer::commit uses a private Runtime::block_on; call it off
    // the tokio worker (same pattern as the engine flush thread).
    let seq2 = Arc::clone(&seq);
    tokio::task::spawn_blocking(move || suite_unit_meta(seq2.as_ref()))
        .await
        .expect("join");
}

/// Atomicity: when commit returns Err, a correct sequencer must not partially
/// advance state. InMemory cannot fail mid-slice; this documents the contract
/// with a poison sequencer that refuses after zero successful assigns.
#[test]
fn sequencer_err_commits_nothing_template() {
    struct AlwaysFail;
    impl Sequencer for AlwaysFail {
        type Meta = ();
        fn commit(
            &self,
            _batches: &[CommitBatch<'_, ()>],
        ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
            Err(ObjectLogError::Sequencer("nope".into()))
        }
        fn lookup(&self, _: &PartitionKey, _: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
            Ok(vec![])
        }
        fn high_watermark(&self, _: &PartitionKey) -> Result<i64, ObjectLogError> {
            Ok(0)
        }
        fn log_start_offset(&self, _: &PartitionKey) -> Result<i64, ObjectLogError> {
            Ok(0)
        }
        fn truncate_before(&self, _: &PartitionKey, _: i64) -> Result<Vec<String>, ObjectLogError> {
            Ok(vec![])
        }
    }
    let seq = AlwaysFail;
    let meta = ();
    let r = seq.commit(&[CommitBatch {
        partition: pk("x"),
        record_count: 1,
        location: loc("o", 0, 1),
        meta: &meta,
    }]);
    assert!(r.is_err());
    assert_eq!(seq.high_watermark(&pk("x")).unwrap(), 0);
}
