//! The persisted (manifest) sequencer makes a standalone log crash-durable: its
//! offset index is rebuilt from the BlobStore after a restart.

use bytes::Bytes;
use object_log::{
    BlobStore, Durability, FlushConfig, LogEngine, ManifestSequencer, MemoryBlobStore,
    PartitionKey, Sequencer,
};
use std::sync::Arc;
use std::time::Duration;

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

#[tokio::test]
async fn manifest_index_survives_restart() {
    // The BlobStore persists across the "restart"; only the engine + sequencer
    // are recreated (their in-memory state is gone).
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let p = pk("t-0");

    // First "process": produce two batches.
    {
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(
            Arc::clone(&blob),
            Arc::clone(&seq),
            FlushConfig::default(),
            "log/",
        );
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"a"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"bb"),
                2,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    } // engine + sequencer dropped — in-memory index gone.

    // "Restart": a fresh sequencer rebuilds the index from the manifest objects.
    let seq2 = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
            .await
            .unwrap(),
    );
    assert_eq!(
        seq2.high_watermark(&p).unwrap(),
        3,
        "index restored from manifests"
    );

    let engine2 = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq2),
        FlushConfig::default(),
        "log/",
    );
    let all = engine2.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].payload, "a");
    assert_eq!(all[1].base_offset, 1);
    assert_eq!(all[1].payload, "bb");

    // New writes continue from the recovered high-watermark.
    let out = engine2
        .produce(
            p.clone(),
            Bytes::from_static(b"c"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(3));
}

#[tokio::test]
async fn manifest_snapshot_lists_partitions_and_entries() {
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq),
        FlushConfig {
            linger: Duration::from_secs(3600),
            max_batches: 1,
            budget: object_log::BudgetConfig {
                enabled: false,
                ..Default::default()
            },
            ..FlushConfig::default()
        },
        "log/",
    );
    engine
        .produce(
            pk("alpha"),
            Bytes::from_static(b"a"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    engine
        .produce(
            pk("beta"),
            Bytes::from_static(b"bb"),
            2,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();

    let snap = seq.snapshot();
    assert_eq!(snap.manifest_prefix, "_manifest/");
    assert_eq!(snap.manifest_count, 2);
    assert_eq!(snap.partitions.len(), 2);
    assert_eq!(snap.partitions[0].partition, "alpha");
    assert_eq!(snap.partitions[0].high_watermark, 1);
    assert_eq!(snap.partitions[0].entry_count, 1);
    assert_eq!(snap.partitions[1].partition, "beta");
    assert_eq!(snap.partitions[1].high_watermark, 2);
    assert!(!seq.live_object_ids().is_empty());
}
