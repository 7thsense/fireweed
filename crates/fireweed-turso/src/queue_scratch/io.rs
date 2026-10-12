//! Private anonymous namespace for disposable native scratch, never authority.
use fireweed_queue_spool::{OwnedScratchDiskCharge, ScratchDiskQuota, ScratchRoot};
use std::{
    collections::BTreeMap,
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};
use turso_core::io::{FileId, FileSyncType};
use turso_core::{
    Buffer, Clock, Completion, File, IO, LimboError, MonotonicInstant, OpenFlags, UnixIO,
    WallClockInstant,
};
type Result<T> = turso_core::Result<T>;
fn error() -> LimboError {
    LimboError::InternalError("private scratch I/O failed".into())
}
fn absent() -> LimboError {
    turso_core::io_error(
        std::io::Error::from(std::io::ErrorKind::NotFound),
        "private scratch",
    )
}
struct Highwater {
    bytes: u64,
    charge: OwnedScratchDiskCharge,
}
struct FileOwner {
    // Both independently owned descriptors close before capacity is released.
    backend: Arc<dyn File>,
    _original: std::fs::File,
    highwater: Mutex<Highwater>,
}
impl FileOwner {
    fn reserve_end(&self, end: u64) -> Result<()> {
        if end > i64::MAX as u64 {
            return Err(error());
        }
        let mut state = self.highwater.lock().map_err(|_| error())?;
        if end > state.bytes {
            let growth = end - state.bytes;
            state
                .charge
                .grow(growth)
                .map_err(|_| LimboError::DatabaseFull)?;
            state.bytes = end;
        }
        Ok(())
    }
}
struct Handle {
    owner: Arc<FileOwner>,
    read_only: bool,
}
impl Handle {
    fn write_end(&self, pos: u64, bytes: u64) -> Result<()> {
        if self.read_only {
            return Err(LimboError::ReadOnly);
        }
        if bytes > i32::MAX as u64 {
            return Err(error());
        }
        let end = pos.checked_add(bytes).ok_or_else(error)?;
        if end > i64::MAX as u64 {
            return Err(error());
        }
        if bytes == 0 {
            return Ok(());
        }
        self.owner.reserve_end(end)
    }
}
impl File for Handle {
    fn lock_file(&self, _: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let bytes = c.as_read().buf().as_slice().len() as u64;
        if bytes > i32::MAX as u64
            || pos
                .checked_add(bytes)
                .is_none_or(|end| end > i64::MAX as u64)
        {
            return Err(error());
        }
        self.owner.backend.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.write_end(pos, buffer.as_slice().len() as u64)?;
        self.owner.backend.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        let bytes = buffers.iter().try_fold(0u64, |n, b| {
            n.checked_add(b.as_slice().len() as u64).ok_or_else(error)
        })?;
        self.write_end(pos, bytes)?;
        self.owner.backend.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, _: FileSyncType) -> Result<Completion> {
        // Disposable scratch is not an authoritative second log.
        c.complete(0);
        Ok(c)
    }
    fn size(&self) -> Result<u64> {
        self.owner.backend.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if self.read_only {
            return Err(LimboError::ReadOnly);
        }
        self.owner.reserve_end(len)?;
        self.owner.backend.truncate(len, c)
    }
}
pub(super) struct ScratchIo {
    // The map keeps backing bytes/charges alive through SDK reopens/removal.
    files: Mutex<BTreeMap<&'static str, Arc<FileOwner>>>,
    backend: UnixIO,
    root: Arc<ScratchRoot>,
    quota: Arc<ScratchDiskQuota>,
    namespace: String,
}
impl ScratchIo {
    pub(super) fn new(
        root: Arc<ScratchRoot>,
        quota: Arc<ScratchDiskQuota>,
        namespace: String,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            files: Mutex::new(BTreeMap::new()),
            backend: UnixIO::new()?,
            root,
            quota,
            namespace,
        }))
    }
    fn name(&self, path: &str) -> Result<&'static str> {
        let name = path.strip_prefix(&self.namespace).ok_or_else(error)?;
        match name {
            "/main.db" => Ok("main.db"),
            "/main.db-wal" => Ok("main.db-wal"),
            "/main.db-journal" => Ok("main.db-journal"),
            "/main.db-shm" => Ok("main.db-shm"),
            _ => Err(error()),
        }
    }
}
impl Clock for ScratchIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.backend.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.backend.current_time_wall_clock()
    }
}
impl IO for ScratchIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let name = self.name(path)?;
        let mut files = self.files.lock().map_err(|_| error())?;
        let read_only = flags.contains(OpenFlags::ReadOnly);
        let owner = if let Some(owner) = files.get(name) {
            Arc::clone(owner)
        } else {
            if read_only || !flags.contains(OpenFlags::Create) {
                return Err(absent());
            }
            let charge = self.quota.reserve(0).map_err(|_| error())?;
            let original = self.root.anonymous_file().map_err(|_| error())?;
            let backend = self.backend.open_file(
                &format!("/proc/self/fd/{}", original.as_raw_fd()),
                OpenFlags::NoLock,
                direct,
            )?;
            let owner = Arc::new(FileOwner {
                backend,
                _original: original,
                highwater: Mutex::new(Highwater { bytes: 0, charge }),
            });
            files.insert(name, Arc::clone(&owner));
            owner
        };
        Ok(Arc::new(Handle { owner, read_only }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        let name = self.name(path)?;
        self.files
            .lock()
            .map_err(|_| error())?
            .remove(name)
            .ok_or_else(absent)?;
        Ok(())
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.name(path)?;
        // Exact SDK Builder/open_async path skips the process-global registry.
        Err(LimboError::InternalError(
            "private scratch has no global file identity".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    use fireweed_queue_spool::ScratchWorkers;

    #[tokio::test]
    async fn shared_file_highwater_precedes_all_growth_and_survives_virtual_removal() {
        let directory = tempfile::tempdir().unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
            .await
            .unwrap();
        let quota = ScratchDiskQuota::new(100).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        workers
            .run({
                let quota = quota.clone();
                let job = job.clone();
                move || {
                    let _headers = job.owned_buffer(65536)?;
                    let io =
                        ScratchIo::new(root, quota.clone(), "/private-scratch".into()).unwrap();
                    assert!(
                        io.open_file("/private-scratch/main.db", OpenFlags::ReadOnly, false)
                            .is_err()
                    );
                    assert!(
                        io.open_file("/private-scratch/main.db", OpenFlags::NoLock, false)
                            .is_err()
                    );
                    assert!(
                        io.open_file("/outside/main.db", OpenFlags::Create, false)
                            .is_err()
                    );
                    assert!(
                        io.open_file("/private-scratch/../authority", OpenFlags::Create, false)
                            .is_err()
                    );
                    assert!(
                        io.open_file("/private-scratch/unexpected-temp", OpenFlags::Create, false)
                            .is_err()
                    );
                    let first = io
                        .open_file("/private-scratch/main.db", OpenFlags::Create, true)
                        .unwrap();
                    let completion = first
                        .pwrite(
                            0,
                            Arc::new(Buffer::new(vec![1; 40])),
                            Completion::new_write(|_| {}),
                        )
                        .unwrap();
                    assert!(completion.finished());
                    let second = io
                        .open_file("/private-scratch/main.db", OpenFlags::NoLock, false)
                        .unwrap();
                    let completion = second
                        .pwritev(
                            40,
                            vec![
                                Arc::new(Buffer::new(vec![2; 30])),
                                Arc::new(Buffer::new(vec![3; 30])),
                            ],
                            Completion::new_write(|_| {}),
                        )
                        .unwrap();
                    assert!(completion.finished());
                    assert_eq!(first.size().unwrap(), 100);
                    assert_eq!(quota.usage()?, 100);
                    assert!(matches!(
                        first.truncate(101, Completion::new_trunc(|_| {})),
                        Err(LimboError::DatabaseFull)
                    ));
                    assert_eq!(first.size().unwrap(), 100);
                    assert_eq!(quota.usage()?, 100);
                    let completion = first.truncate(20, Completion::new_trunc(|_| {})).unwrap();
                    assert!(completion.finished());
                    assert_eq!(first.size().unwrap(), 20);
                    assert_eq!(quota.usage()?, 100); // Conservative retained highwater.
                    assert!(
                        first
                            .pwrite(
                                i64::MAX as u64,
                                Arc::new(Buffer::new(vec![1])),
                                Completion::new_write(|_| {})
                            )
                            .is_err()
                    );
                    assert_eq!(quota.usage()?, 100);
                    let read_only = io
                        .open_file("/private-scratch/main.db", OpenFlags::ReadOnly, false)
                        .unwrap();
                    assert!(matches!(
                        read_only.pwrite(
                            0,
                            Arc::new(Buffer::new(vec![1])),
                            Completion::new_write(|_| {})
                        ),
                        Err(LimboError::ReadOnly)
                    ));
                    io.remove_file("/private-scratch/main.db").unwrap();
                    assert_eq!(quota.usage()?, 100); // Live handles still own old bytes.
                    let replacement = io
                        .open_file("/private-scratch/main.db", OpenFlags::Create, false)
                        .unwrap();
                    assert!(matches!(
                        replacement.pwrite(
                            0,
                            Arc::new(Buffer::new(vec![1])),
                            Completion::new_write(|_| {})
                        ),
                        Err(LimboError::DatabaseFull)
                    ));
                    assert_eq!(replacement.size().unwrap(), 0);
                    drop(first);
                    drop(second);
                    drop(read_only);
                    assert_eq!(quota.usage()?, 0);
                    let completion = replacement
                        .pwrite(
                            0,
                            Arc::new(Buffer::new(vec![1])),
                            Completion::new_write(|_| {}),
                        )
                        .unwrap();
                    assert!(completion.finished());
                    drop(io);
                    assert_eq!(quota.usage()?, 1);
                    drop(replacement);
                    assert_eq!(quota.usage()?, 0);
                    Ok(())
                }
            })
            .await
            .unwrap();
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    struct PartialBackend(Arc<dyn File>);
    impl File for PartialBackend {
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            self.0.pread(pos, c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            let bytes = &buffer.as_slice()[..buffer.as_slice().len().min(2)];
            let completed = self.0.pwrite(
                pos,
                Arc::new(Buffer::new(bytes.to_vec())),
                Completion::new_write(|_| {}),
            )?;
            assert!(completed.finished());
            c.abort();
            Err(error())
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
            self.0.sync(c, kind)
        }
        fn size(&self) -> Result<u64> {
            self.0.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.0.truncate(len, c)
        }
    }
    #[tokio::test]
    async fn partial_backend_error_retains_full_reserved_growth_until_file_close() {
        let directory = tempfile::tempdir().unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
            .await
            .unwrap();
        let quota = ScratchDiskQuota::new(10).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        workers
            .run({
                let job = job.clone();
                let quota = quota.clone();
                move || {
                    let _headers = job.owned_buffer(65536)?;
                    let original = root.anonymous_file()?;
                    let syscall = UnixIO::new()
                        .unwrap()
                        .open_file(
                            &format!("/proc/self/fd/{}", original.as_raw_fd()),
                            OpenFlags::NoLock,
                            false,
                        )
                        .unwrap();
                    let handle = Handle {
                        owner: Arc::new(FileOwner {
                            backend: Arc::new(PartialBackend(syscall)),
                            _original: original,
                            highwater: Mutex::new(Highwater {
                                bytes: 0,
                                charge: quota.reserve(0)?,
                            }),
                        }),
                        read_only: false,
                    };
                    assert!(
                        handle
                            .pwrite(
                                0,
                                Arc::new(Buffer::new(vec![1; 4])),
                                Completion::new_write(|_| {})
                            )
                            .is_err()
                    );
                    assert_eq!(handle.size().unwrap(), 2);
                    assert_eq!(quota.usage()?, 4);
                    assert!(
                        handle
                            .pwritev(
                                2,
                                vec![
                                    Arc::new(Buffer::new(vec![1; 3])),
                                    Arc::new(Buffer::new(vec![1; 3]))
                                ],
                                Completion::new_write(|_| {})
                            )
                            .is_err()
                    );
                    assert_eq!(handle.size().unwrap(), 4);
                    assert_eq!(quota.usage()?, 8);
                    drop(handle);
                    assert_eq!(quota.usage()?, 0);
                    Ok(())
                }
            })
            .await
            .unwrap();
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
