//! Filesystem I/O for a disposable, authoritative-log-backed projection.
//!
//! Preserve writes, ordering, errors and locks; omit only stable-storage sync.
//! A machine/power failure may require deleting and rebuilding this projection.
//! This adapter must never be used for the authoritative log itself.
use std::sync::Arc;
use turso_core::io::{FileId, FileSyncType, SharedWalLockKind, SharedWalMappedRegion};
use turso_core::{
    Buffer, Clock, Completion, File, IO, MonotonicInstant, OpenFlags, PlatformIO, WallClockInstant,
};

type Result<T> = turso_core::Result<T>;

pub(crate) struct RebuildableIo(Arc<dyn IO>);
impl RebuildableIo {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self(Arc::new(PlatformIO::new()?)))
    }
}
impl Clock for RebuildableIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.0.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.0.current_time_wall_clock()
    }
}
impl IO for RebuildableIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(RebuildableFile(
            self.0.open_file(path, flags, direct)?,
        )))
    }
    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        Ok(Arc::new(RebuildableFile(
            self.0.open_shared_wal_file(path)?,
        )))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.0.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.0.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        self.0.step()
    }
    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        self.0.cancel(completions)
    }
    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.0.drain_completions(completions)
    }
    fn wait_for_completion(&self, completion: Completion) -> Result<()> {
        self.0.wait_for_completion(completion)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.0.file_id(path)
    }
}

struct RebuildableFile(Arc<dyn File>);
impl File for RebuildableFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.0.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.0.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.0.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.0.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.0.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        c.complete(0);
        Ok(c)
    }
    fn size(&self) -> Result<u64> {
        self.0.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.0.truncate(len, c)
    }
    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        self.0.has_hole(pos, len)
    }
    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.0.punch_hole(pos, len)
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.0.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.0.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.0.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.0.shared_wal_set_len(len)
    }
    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.0.shared_wal_map(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailingFile {
        syncs: AtomicUsize,
    }
    impl File for FailingFile {
        fn lock_file(&self, _: bool) -> Result<()> {
            Err(turso_core::LimboError::Busy)
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn pread(&self, _: u64, _: Completion) -> Result<Completion> {
            Err(turso_core::LimboError::Busy)
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> Result<Completion> {
            Err(turso_core::LimboError::Busy)
        }
        fn sync(&self, c: Completion, _: FileSyncType) -> Result<Completion> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            c.complete(0);
            Ok(c)
        }
        fn size(&self) -> Result<u64> {
            Ok(1024)
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            Err(turso_core::LimboError::Busy)
        }
    }

    #[test]
    fn omits_only_sync_and_preserves_lock_and_storage_errors() {
        let inner = Arc::new(FailingFile {
            syncs: AtomicUsize::new(0),
        });
        let file = RebuildableFile(inner.clone());
        let completion = Completion::new_sync(|_| {});
        let result = file.sync(completion, FileSyncType::FullFsync).unwrap();
        assert!(result.finished());
        assert_eq!(inner.syncs.load(Ordering::SeqCst), 0);
        assert!(matches!(
            file.lock_file(true),
            Err(turso_core::LimboError::Busy)
        ));
        assert!(matches!(
            file.truncate(0, Completion::new_sync(|_| {})),
            Err(turso_core::LimboError::Busy)
        ));
        assert_eq!(file.size().unwrap(), 1024);
    }
}
