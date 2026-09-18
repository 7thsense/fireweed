//! Opt-in timings around the existing durable adapter; no alternate I/O path.
use bytes::Bytes;
use object_log::{BlobStore, MediaOpStats, ObjectLogError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{ops::Range, sync::Arc};

pub(crate) fn maybe_trace(inner: Arc<dyn BlobStore>, store: String) -> Arc<dyn BlobStore> {
    if std::env::var_os("FIREWEED_LOG_TRACE").is_some() {
        Arc::new(TracedBlobStore { inner, store })
    } else {
        inner
    }
}
struct TracedBlobStore {
    inner: Arc<dyn BlobStore>,
    store: String,
}

fn report(store: &str, key: &str, bytes: usize, started: Instant, started_unix_us: u128, ok: bool) {
    // Classify only; never emit arbitrary object keys or values.
    let class = if key.starts_with("fwlog/") {
        "segment"
    } else if key.starts_with("fwmeta/manifest/") {
        "manifest"
    } else {
        "metadata"
    };
    eprintln!(
        "log_blob class={class} bytes={bytes} put_us={} ok={ok} store={store} started_unix_us={started_unix_us}",
        started.elapsed().as_micros()
    );
}

fn trace_start() -> (Instant, u128) {
    (
        Instant::now(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
}

#[async_trait::async_trait]
impl BlobStore for TracedBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let bytes = value.len();
        let (started, started_unix_us) = trace_start();
        let result = self.inner.put(key, value).await;
        report(
            &self.store,
            key,
            bytes,
            started,
            started_unix_us,
            result.is_ok(),
        );
        result
    }
    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        let bytes = chunks.iter().map(Bytes::len).sum();
        let (started, started_unix_us) = trace_start();
        let result = self.inner.put_chunks(key, chunks).await;
        report(
            &self.store,
            key,
            bytes,
            started,
            started_unix_us,
            result.is_ok(),
        );
        result
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get_range(key, range).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        self.inner.take_media_op_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn tracing_preserves_chunk_range_error_and_accounting_contracts() {
        let store = TracedBlobStore {
            inner: Arc::new(object_log::MemoryBlobStore::new()),
            store: "memory".into(),
        };
        store
            .put_chunks(
                "fwlog/test",
                vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
            )
            .await
            .unwrap();
        assert_eq!(
            store.get("fwlog/test").await.unwrap(),
            Some(Bytes::from_static(b"abcd"))
        );
        assert_eq!(
            store.get_range("fwlog/test", 1..3).await.unwrap(),
            Some(Bytes::from_static(b"bc"))
        );
        assert!(store.get_range("fwlog/test", 0..5).await.is_err());
        assert_eq!(store.list("fwlog/").await.unwrap(), vec!["fwlog/test"]);
        assert_eq!(store.take_media_op_stats().unwrap().media_ops, 0);
        store.delete("fwlog/test").await.unwrap();
        assert_eq!(store.get("fwlog/test").await.unwrap(), None);
    }
}
