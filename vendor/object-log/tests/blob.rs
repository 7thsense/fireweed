//! Conformance for the bundled `BlobStore` adapters.

use bytes::Bytes;
use object_log::{BlobStore, LocalBlobStore, MemoryBlobStore, ObjectLogError};

#[tokio::test]
async fn local_put_reports_two_media_ops() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(dir.path());
    let _ = store.take_media_op_stats();
    store.put("k", Bytes::from_static(b"abc")).await.unwrap();
    let stats = store.take_media_op_stats().expect("local stats");
    assert_eq!(stats.media_ops, 2, "file sync + dir sync");
    assert_eq!(stats.bytes, 3);
}

#[tokio::test]
async fn memory_put_reports_zero_media_ops() {
    let store = MemoryBlobStore::new();
    let _ = store.take_media_op_stats();
    store.put("k", Bytes::from_static(b"abc")).await.unwrap();
    let stats = store.take_media_op_stats().expect("memory stats");
    assert_eq!(stats.media_ops, 0);
    assert_eq!(stats.bytes, 3);
}

async fn port_suite(store: &dyn BlobStore) {
    // Absent key.
    assert!(store.get("topics/a/0").await.unwrap().is_none());
    assert!(store.get_range("topics/a/0", 0..1).await.unwrap().is_none());

    // put / get round-trip.
    store
        .put("topics/a/0", Bytes::from_static(b"hello world"))
        .await
        .unwrap();
    assert_eq!(
        store.get("topics/a/0").await.unwrap().unwrap(),
        Bytes::from_static(b"hello world")
    );

    // get_range: slice, empty, and bounds errors.
    assert_eq!(
        store.get_range("topics/a/0", 6..11).await.unwrap().unwrap(),
        Bytes::from_static(b"world")
    );
    // Empty range at the end of the object (endpoints derived at runtime so this
    // is not a literal empty range).
    let len = store.get("topics/a/0").await.unwrap().unwrap().len() as u64;
    let empty = store
        .get_range("topics/a/0", len..len)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(empty, Bytes::new());
    assert!(matches!(
        store.get_range("topics/a/0", 0..99).await,
        Err(ObjectLogError::RangeOutOfBounds(_))
    ));
    let (start, end) = (5u64, 2u64); // reversed range (runtime endpoints)
    assert!(matches!(
        store.get_range("topics/a/0", start..end).await,
        Err(ObjectLogError::RangeOutOfBounds(_))
    ));

    // list by prefix (nested keys, no temp files leaked).
    store
        .put("topics/a/1", Bytes::from_static(b"x"))
        .await
        .unwrap();
    store
        .put("topics/b/0", Bytes::from_static(b"y"))
        .await
        .unwrap();
    let mut a = store.list("topics/a/").await.unwrap();
    a.sort();
    assert_eq!(a, vec!["topics/a/0".to_string(), "topics/a/1".to_string()]);
    assert!(store.list("topics/z/").await.unwrap().is_empty());

    // delete, then deleting a missing key is a no-op.
    store.delete("topics/a/0").await.unwrap();
    assert!(store.get("topics/a/0").await.unwrap().is_none());
    store.delete("topics/a/0").await.unwrap();
}

#[tokio::test]
async fn memory_blob_store_conforms() {
    port_suite(&MemoryBlobStore::new()).await;
}

#[tokio::test]
async fn local_blob_store_conforms() {
    let dir = tempfile::tempdir().unwrap();
    port_suite(&LocalBlobStore::new(dir.path())).await;
}

#[tokio::test]
async fn local_blob_store_put_is_durable_and_readable_across_instances() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = LocalBlobStore::new(dir.path());
        store
            .put("seg/000", Bytes::from_static(b"durable"))
            .await
            .unwrap();
    }
    // A fresh instance over the same root sees the durably-written object.
    let reopened = LocalBlobStore::new(dir.path());
    assert_eq!(
        reopened.get("seg/000").await.unwrap().unwrap(),
        Bytes::from_static(b"durable")
    );
    assert_eq!(
        reopened.list("").await.unwrap(),
        vec!["seg/000".to_string()]
    );
}
