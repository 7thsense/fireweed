//! A real native WAL write may block even though the Rust client API is async.
use crate::{TursoConfig, TursoRelational};
use fireweed_conformance::{envelope, item, qdef};
use fireweed_core::ItemState;
use fireweed_engine::{AsyncProjectionStore, CommandPosition, PushCommand, QueueCommand, QueueKey};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use turso_core::io::{FileId, FileSyncType};
use turso_core::{
    Buffer, Clock, Completion, File, IO, MemoryIO, MonotonicInstant, OpenFlags, WallClockInstant,
};

type Result<T> = turso_core::Result<T>;
struct WriteGate {
    armed: AtomicBool,
    timed_out: AtomicBool,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: Mutex<bool>,
    changed: Condvar,
}
impl WriteGate {
    fn pause(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.entered
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        // A native timeout prevents the red implementation from deadlocking
        // the only async worker (and therefore its own async test timeout).
        let (_guard, timeout) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_millis(500),
                |released| !*released,
            )
            .unwrap();
        self.timed_out.store(timeout.timed_out(), Ordering::SeqCst);
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct BlockingWalIo {
    inner: MemoryIO,
    gate: Arc<WriteGate>,
}
impl Clock for BlockingWalIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl IO for BlockingWalIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if path.ends_with("-wal") {
            Ok(Arc::new(BlockingWalFile {
                inner,
                gate: self.gate.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}
struct BlockingWalFile {
    inner: Arc<dyn File>,
    gate: Arc<WriteGate>,
}
impl File for BlockingWalFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.gate.pause();
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.gate.pause();
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, mode: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, mode)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}
async fn check_blocked_commit() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let gate = Arc::new(WriteGate {
        armed: AtomicBool::new(false),
        timed_out: AtomicBool::new(false),
        entered: Mutex::new(Some(entered_tx)),
        released: Mutex::new(false),
        changed: Condvar::new(),
    });
    let directory = tempfile::tempdir().unwrap();
    let io = Arc::new(BlockingWalIo {
        inner: MemoryIO::new(),
        gate: gate.clone(),
    });
    let store = Arc::new(
        TursoRelational::open_with_io(
            TursoConfig::local(directory.path().join("runtime.db")),
            Some(io),
        )
        .await
        .unwrap(),
    );
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();
    let row = item("701", "blocked-commit", 0);
    let id = row.item_id;
    let command = envelope(
        QueueCommand::Push(PushCommand { items: vec![row] }),
        vec![id],
    );
    gate.armed.store(true, Ordering::SeqCst);
    let apply_store = store.clone();
    let position = CommandPosition::new(shard.clone(), 0, 0);
    let apply = tokio::spawn(async move {
        AsyncProjectionStore::apply_live(apply_store.as_ref(), vec![position], vec![command]).await
    });
    let heartbeat = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        // This worker continuation must run while the VFS call is blocked.
        // Running it on the test's block_on thread would miss starvation of
        // the sole worker in a multi-thread runtime.
        let starved = gate.timed_out.load(Ordering::SeqCst);
        gate.release();
        starved
    });
    let starved = heartbeat.await.unwrap();
    apply.await.unwrap().unwrap();
    assert!(
        !starved,
        "native WAL commit blocked the application's async worker"
    );
    assert_eq!(
        AsyncProjectionStore::item_state(store.as_ref(), shard, id)
            .await
            .unwrap(),
        Some(ItemState::Pending)
    );
}
#[tokio::test(flavor = "current_thread")]
async fn blocked_wal_commit_preserves_current_thread_runtime_progress() {
    check_blocked_commit().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn blocked_wal_commit_preserves_multi_thread_runtime_progress() {
    check_blocked_commit().await;
}
