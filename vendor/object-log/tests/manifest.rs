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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_object_uploads_preserve_acknowledged_offsets_on_reopen() {
    use std::collections::{HashMap, HashSet};
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let sequencer = Arc::new(
        ManifestSequencer::open(blob.clone(), "manifest/")
            .await
            .unwrap(),
    );
    let mut config = FlushConfig::default();
    config.max_batches = 1;
    config.max_inflight_flushes = 4;
    config.linger = Duration::ZERO;
    config.budget.enabled = false;
    let engine = Arc::new(LogEngine::new(
        blob.clone(),
        sequencer.clone(),
        config,
        "data/",
    ));
    let mut tasks = Vec::new();
    for producer in 0..16 {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            let mut accepted = Vec::new();
            for ordinal in 0..4 {
                let payload = format!("{producer}:{ordinal}");
                let result = engine
                    .produce(
                        pk(&format!("partition-{}", producer % 2)),
                        Bytes::from(payload.clone()),
                        1,
                        (),
                        Durability::Sequenced,
                    )
                    .await
                    .unwrap();
                assert!(result.durable && result.sequenced);
                assert_eq!(result.base_offset, result.last_offset);
                accepted.push((payload, result.base_offset.unwrap()));
            }
            accepted
        }));
    }
    let mut accepted = HashMap::new();
    for task in tasks {
        for (payload, offset) in task.await.unwrap() {
            assert!(accepted.insert(payload, offset).is_none());
        }
    }
    engine.flush().await.unwrap();
    drop(engine);
    drop(sequencer);
    let sequencer = Arc::new(
        ManifestSequencer::open(blob.clone(), "manifest/")
            .await
            .unwrap(),
    );
    let reopened = LogEngine::new(blob.clone(), sequencer, config, "data/");
    let mut seen = HashSet::new();
    for partition in 0..2 {
        let rows = reopened
            .fetch(&pk(&format!("partition-{partition}")), 0, 65536)
            .await
            .unwrap();
        assert_eq!(rows.len(), 32);
        let mut next_by_producer = HashMap::new();
        for (offset, row) in rows.iter().enumerate() {
            assert_eq!(row.base_offset, offset as i64);
            let payload = std::str::from_utf8(&row.payload).unwrap();
            assert_eq!(accepted[payload], row.base_offset);
            assert!(seen.insert(payload.to_owned()));
            let (producer, ordinal) = payload.split_once(':').unwrap();
            let producer: usize = producer.parse().unwrap();
            let ordinal: usize = ordinal.parse().unwrap();
            assert_eq!(producer % 2, partition);
            let next = next_by_producer.entry(producer).or_insert(0);
            assert_eq!(ordinal, *next);
            *next += 1;
        }
    }
    assert_eq!(seen.len(), 64);
    assert_eq!(blob.list("data/").await.unwrap().len(), 64);
    let manifests = blob.list("manifest/").await.unwrap().len();
    assert!((1..=64).contains(&manifests));
}

#[tokio::test]
async fn grouped_manifest_reopen_uses_data_object_counter_not_manifest_count() {
    use object_log::{BatchLocation, CommitBatch};
    let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    blob.put("data/00000000000000000007", Bytes::from_static(b"first"))
        .await
        .unwrap();
    blob.put("data/00000000000000000011", Bytes::from_static(b"second"))
        .await
        .unwrap();
    let sequencer = Arc::new(
        ManifestSequencer::open(blob.clone(), "manifest/")
            .await
            .unwrap(),
    );
    let commit = sequencer.clone();
    std::thread::spawn(move || {
        commit.commit(&[
            CommitBatch {
                partition: pk("p"),
                record_count: 1,
                location: BatchLocation {
                    object_id: "data/00000000000000000007".into(),
                    byte_start: 0,
                    byte_len: 5,
                },
                meta: &(),
            },
            CommitBatch {
                partition: pk("p"),
                record_count: 2,
                location: BatchLocation {
                    object_id: "data/00000000000000000011".into(),
                    byte_start: 0,
                    byte_len: 6,
                },
                meta: &(),
            },
        ])
    })
    .join()
    .unwrap()
    .unwrap();
    assert_eq!(blob.list("manifest/").await.unwrap().len(), 1);
    drop(sequencer);
    let sequencer = Arc::new(
        ManifestSequencer::open(blob.clone(), "manifest/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(blob.clone(), sequencer, FlushConfig::default(), "data/");
    let accepted = engine
        .produce(
            pk("p"),
            Bytes::from_static(b"third"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(accepted.base_offset, Some(3));
    assert!(
        blob.get("data/00000000000000000012")
            .await
            .unwrap()
            .is_some()
    );
    let rows = engine.fetch(&pk("p"), 0, 1024).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.base_offset).collect::<Vec<_>>(),
        vec![0, 1, 3]
    );
    assert_eq!(
        rows.iter().map(|r| r.payload.as_ref()).collect::<Vec<_>>(),
        vec![
            b"first".as_slice(),
            b"second".as_slice(),
            b"third".as_slice()
        ]
    );
}
