//! Test seam: hold one object-log head publish so another owner can take over first.
//!
//! The manifest sequencer publishes an append by compare-and-swapping the partition index under
//! `{meta_prefix}manifest/`, after the append's data object is written. Wrapping the log's blob
//! store with [`HeadPublishPause`] holds the first such swap after [`HeadPublishPause::arm`] until
//! [`HeadPublishPause::release`], which is where a stale owner's write meets a takeover.
use bytes::Bytes;
use object_log::{BlobStore, CasOutcome, MediaOpStats, ObjectLogError};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

pub struct HeadPublishPause {
    armed: AtomicBool,
    entered: watch::Sender<bool>,
    released: watch::Sender<bool>,
}

impl HeadPublishPause {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            entered: watch::channel(false).0,
            released: watch::channel(false).0,
        })
    }

    /// Wrap a log blob store whose manifest objects live under `manifest_prefix`.
    pub fn wrap(
        self: &Arc<Self>,
        inner: Arc<dyn BlobStore>,
        manifest_prefix: impl Into<String>,
    ) -> Arc<dyn BlobStore> {
        Arc::new(PausingBlobStore {
            inner,
            manifest_prefix: manifest_prefix.into(),
            pause: Arc::clone(self),
        })
    }

    /// Hold the next manifest compare-and-swap.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Wait until a head publish is being held.
    pub async fn entered(&self) {
        let mut entered = self.entered.subscribe();
        let _ = entered.wait_for(|held| *held).await;
    }

    /// Let the held head publish proceed.
    pub fn release(&self) {
        self.released.send_replace(true);
    }
}

struct PausingBlobStore {
    inner: Arc<dyn BlobStore>,
    manifest_prefix: String,
    pause: Arc<HeadPublishPause>,
}

#[async_trait::async_trait]
impl BlobStore for PausingBlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.inner.put(key, value).await
    }
    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        self.inner.put_chunks(key, chunks).await
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
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<CasOutcome, ObjectLogError> {
        if key.starts_with(&self.manifest_prefix) && self.pause.armed.swap(false, Ordering::SeqCst)
        {
            let mut released = self.pause.released.subscribe();
            self.pause.entered.send_replace(true);
            let _ = released.wait_for(|go| *go).await;
        }
        self.inner.compare_and_swap(key, expected, new_value).await
    }
    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        self.inner.take_media_op_stats()
    }
}
