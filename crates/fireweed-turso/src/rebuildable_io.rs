//! Filesystem I/O for a disposable, authoritative-log-backed projection.
//!
//! Preserve writes, ordering, errors and locks; omit only stable-storage sync.
//! A machine/power failure may require deleting and rebuilding this projection.
//! This adapter must never be used for the authoritative log itself.
use std::sync::{Arc, Mutex};
use std::time::Instant;
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
        Ok(Arc::new(RebuildableFile::new(
            self.0.open_file(path, flags, direct)?,
            path,
        )))
    }
    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        Ok(Arc::new(RebuildableFile::new(
            self.0.open_shared_wal_file(path)?,
            path,
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

#[derive(Default)]
struct IoTotals {
    calls: u64,
    requested_bytes: u64,
    elapsed_us: u64,
    max_us: u64,
    over_1ms: u64,
    over_10ms: u64,
    over_100ms: u64,
    errors: u64,
}

struct IoTrace {
    class: &'static str,
    store: String,
    totals: Mutex<IoTotals>,
    read_totals: Mutex<IoTotals>,
}

fn diagnostic_store_tag(path: &str) -> String {
    let mut fallback = None;
    for component in std::path::Path::new(path).components() {
        let name = component.as_os_str().to_string_lossy();
        if let Some(rest) = name.strip_prefix("shard-")
            && !rest.is_empty()
            && rest.bytes().all(|b| b.is_ascii_digit())
        {
            return name.into_owned();
        }
        if !matches!(
            name.as_ref(),
            "log" | "fwlog" | "fwmeta" | "manifest" | "projection.db" | "/" | "."
        ) {
            fallback = Some(name.into_owned());
        }
    }
    fallback
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

struct RebuildableFile(Arc<dyn File>, Option<IoTrace>);
impl RebuildableFile {
    fn new(inner: Arc<dyn File>, path: &str) -> Self {
        let trace = std::env::var_os("FIREWEED_PROJECTION_IO_TRACE").map(|_| IoTrace {
            class: if path.ends_with("-wal") {
                "wal"
            } else if std::path::Path::new(path).file_name()
                == Some(std::ffi::OsStr::new("tursodb_temp_file"))
            {
                "temporary"
            } else {
                "main_or_other"
            },
            store: diagnostic_store_tag(path),
            totals: Mutex::default(),
            read_totals: Mutex::default(),
        });
        Self(inner, trace)
    }

    fn traced_write(
        &self,
        bytes: usize,
        write: impl FnOnce() -> Result<Completion>,
    ) -> Result<Completion> {
        let Some(trace) = &self.1 else {
            return write();
        };
        Self::traced_call(trace.class, "write", &trace.totals, bytes, write)
    }

    fn traced_call(
        class: &str,
        operation_name: &str,
        totals: &Mutex<IoTotals>,
        bytes: usize,
        operation: impl FnOnce() -> Result<Completion>,
    ) -> Result<Completion> {
        let started = Instant::now();
        let result = operation();
        if let Err(error) = &result {
            eprintln!(
                "projection_io_error class={class} operation={operation_name} requested_bytes={bytes} error={error:?}"
            );
        }
        // On Unix PlatformIO completes reads/writes synchronously, including
        // the completion callback. This is VFS-call elapsed time, not device
        // service time, physical bytes, or a count of page-cache misses.
        let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        let mut totals = totals.lock().expect("projection I/O trace poisoned");
        totals.calls += 1;
        totals.requested_bytes += bytes as u64;
        totals.elapsed_us += elapsed_us;
        totals.max_us = totals.max_us.max(elapsed_us);
        totals.over_1ms += u64::from(elapsed_us >= 1_000);
        totals.over_10ms += u64::from(elapsed_us >= 10_000);
        totals.over_100ms += u64::from(elapsed_us >= 100_000);
        totals.errors += u64::from(result.is_err());
        result
    }
}

impl Drop for RebuildableFile {
    fn drop(&mut self) {
        if let Some(trace) = &self.1 {
            let t = trace
                .totals
                .lock()
                .expect("projection write trace poisoned");
            eprintln!(
                "projection_io class={} store={} calls={} requested_bytes={} elapsed_us={} max_us={} over_1ms={} over_10ms={} over_100ms={} errors={}",
                trace.class,
                trace.store,
                t.calls,
                t.requested_bytes,
                t.elapsed_us,
                t.max_us,
                t.over_1ms,
                t.over_10ms,
                t.over_100ms,
                t.errors
            );
            let reads = trace
                .read_totals
                .lock()
                .expect("projection I/O trace poisoned");
            eprintln!(
                "projection_read class={} store={} calls={} requested_bytes={} elapsed_us={} max_us={} over_1ms={} over_10ms={} over_100ms={} errors={}",
                trace.class,
                trace.store,
                reads.calls,
                reads.requested_bytes,
                reads.elapsed_us,
                reads.max_us,
                reads.over_1ms,
                reads.over_10ms,
                reads.over_100ms,
                reads.errors
            );
        }
    }
}
impl File for RebuildableFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.0.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.0.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let Some(trace) = &self.1 else {
            return self.0.pread(pos, c);
        };
        let bytes = c.as_read().buf().as_slice().len();
        Self::traced_call(trace.class, "read", &trace.read_totals, bytes, || {
            self.0.pread(pos, c)
        })
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.traced_write(buffer.as_slice().len(), || self.0.pwrite(pos, buffer, c))
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        if self.1.is_none() {
            return self.0.pwritev(pos, buffers, c);
        }
        let bytes = buffers.iter().map(|buffer| buffer.as_slice().len()).sum();
        self.traced_write(bytes, || self.0.pwritev(pos, buffers, c))
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
    fn write_trace_preserves_results_and_accounts_for_blocking_and_errors() {
        let file = RebuildableFile(
            Arc::new(FailingFile {
                syncs: AtomicUsize::new(0),
            }),
            Some(IoTrace {
                class: "wal",
                store: "shard-0".into(),
                totals: Mutex::default(),
                read_totals: Mutex::default(),
            }),
        );
        assert!(matches!(
            file.traced_write(32, || Err(turso_core::LimboError::Busy)),
            Err(turso_core::LimboError::Busy)
        ));
        let completion = file
            .traced_write(64, || {
                std::thread::sleep(std::time::Duration::from_millis(12));
                let completion = Completion::new_sync(|_| {});
                completion.complete(0);
                Ok(completion)
            })
            .unwrap();
        assert!(completion.finished());
        let t = file.1.as_ref().unwrap().totals.lock().unwrap();
        assert_eq!(t.calls, 2);
        assert_eq!(t.requested_bytes, 96);
        assert_eq!(t.errors, 1);
        assert!(t.elapsed_us >= 10_000);
        assert!(t.max_us >= 10_000);
        assert!(t.over_10ms >= 1);
    }

    #[test]
    fn read_trace_preserves_short_reads_callbacks_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projection.db");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let io = PlatformIO::new().unwrap();
        let trace = || IoTrace {
            class: "main_or_other",
            store: "shard-0".into(),
            totals: Mutex::default(),
            read_totals: Mutex::default(),
        };
        let file = RebuildableFile(
            io.open_file(path.to_str().unwrap(), OpenFlags::None, false)
                .unwrap(),
            Some(trace()),
        );
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = callbacks.clone();
        let buffer = Arc::new(Buffer::new_temporary(16));
        let completion = Completion::new_read(buffer.clone(), move |result| {
            let (buffer, bytes) = result.unwrap();
            assert_eq!(bytes, 8);
            assert_eq!(&buffer.as_slice()[..8], b"abcdefgh");
            observed.fetch_add(1, Ordering::SeqCst);
            None
        });
        let completion = file.pread(0, completion).unwrap();
        io.wait_for_completion(completion.clone()).unwrap();
        assert!(completion.succeeded());
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        let totals = file.1.as_ref().unwrap().read_totals.lock().unwrap();
        assert_eq!(
            (totals.calls, totals.requested_bytes, totals.errors),
            (1, 16, 0)
        );
        assert_eq!(file.1.as_ref().unwrap().totals.lock().unwrap().calls, 0);
        drop(totals);

        let failing = RebuildableFile(
            Arc::new(FailingFile {
                syncs: AtomicUsize::new(0),
            }),
            Some(trace()),
        );
        assert!(matches!(
            failing.pread(0, Completion::new_read(buffer, |_| None)),
            Err(turso_core::LimboError::Busy)
        ));
        let totals = failing.1.as_ref().unwrap().read_totals.lock().unwrap();
        assert_eq!(
            (totals.calls, totals.requested_bytes, totals.errors),
            (1, 16, 1)
        );
    }

    #[test]
    fn diagnostic_store_tag_joins_workload_shard_directories() {
        assert_eq!(
            diagnostic_store_tag("/tmp/run/shard-12/projection.db"),
            "shard-12"
        );
        assert_eq!(
            diagnostic_store_tag("/tmp/run/shard-12/projection.db-wal"),
            "shard-12"
        );
    }

    #[test]
    fn omits_only_sync_and_preserves_lock_and_storage_errors() {
        let inner = Arc::new(FailingFile {
            syncs: AtomicUsize::new(0),
        });
        let file = RebuildableFile(inner.clone(), None);
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
