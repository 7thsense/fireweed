//! One projection database file per owned queue, in a directory the pipeline
//! owns alone. A file exists exactly as long as the tenure that opened it.
use super::*;
use fireweed_engine::queue_admission::QueueReservation;
use std::path::{Path, PathBuf};

/// The files a projection database may leave next to its main file.
const SUFFIXES: [&str; 4] = ["", "-wal", "-shm", "-journal"];

/// The lock target in the directory. It is never read, written, or deleted, and
/// its name is not a projection file's, so cleanup leaves it alone.
const LOCK_FILE: &str = ".lock";

/// The directory's exclusive OS lock, shared by the pipeline and by every tenure
/// projection. The OS lock is released when the last clone is dropped, so it
/// outlives every projection file's deletion.
#[derive(Clone, Debug)]
pub(super) struct DirectoryLock(#[allow(dead_code)] Arc<std::fs::File>);

/// Check the directory before any I/O, create it if missing, take the directory's
/// exclusive lock, and delete every projection file an earlier process left in
/// it. Nothing is read. The returned lock is held until its last clone is dropped, and
/// the OS releases it when the process exits, however it exits, so a lock cannot
/// outlive its holder. It relies on the OS's advisory file locks (`flock`), which
/// need a local file system.
///
/// The lock is taken before anything is deleted: a second handle on a directory
/// in use is refused without touching the first handle's files.
pub(super) fn prepare_directory(directory: &Path) -> QueueResult<DirectoryLock> {
    if !directory.is_absolute() {
        return Err(QueueError::Invalid(
            "projection directory must be an absolute path".into(),
        ));
    }
    match std::fs::metadata(directory) {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => {
            return Err(QueueError::Invalid(
                "projection directory is not a directory".into(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(directory).map_err(|e| unknown(&e.to_string()))?;
        }
        Err(error) => return Err(unknown(&error.to_string())),
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(LOCK_FILE))
        .map_err(|e| unknown(&e.to_string()))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(QueueError::Invalid(
                "projection directory is in use by another handle".into(),
            ));
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(unknown(&error.to_string())),
    }
    for entry in std::fs::read_dir(directory).map_err(|e| unknown(&e.to_string()))? {
        let entry = entry.map_err(|e| unknown(&e.to_string()))?;
        let is_file = entry.file_type().is_ok_and(|kind| kind.is_file());
        if is_file && entry.file_name().to_str().is_some_and(is_projection_file) {
            remove(&entry.path())?;
        }
    }
    Ok(DirectoryLock(Arc::new(lock)))
}

/// `q7-<64 lowercase hex>.db`, optionally followed by `-wal`, `-shm` or `-journal`.
fn is_projection_file(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("q7-") else {
        return false;
    };
    // Byte-wise so a multi-byte character straddling byte 64 cannot panic.
    let bytes = rest.as_bytes();
    let (Some(digest), Some(tail)) = (bytes.get(..64), bytes.get(64..)) else {
        return false;
    };
    digest
        .iter()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        && SUFFIXES
            .iter()
            .any(|suffix| tail == format!(".db{suffix}").as_bytes())
}

fn remove(path: &Path) -> QueueResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(unknown(&error.to_string())),
    }
}

/// The open projection database of one tenure. This is the only place a queue's
/// projection files are removed: dropping it closes the database, deletes the
/// files, then releases its clone of the directory lock, so no other handle can
/// take the directory while this tenure's files may still exist.
pub(super) struct TenureProjection {
    projection: Option<Arc<QueueProjection>>,
    path: PathBuf,
    lock: Option<DirectoryLock>,
}
impl TenureProjection {
    /// Open `<directory>/<name>.db` with whatever it held discarded.
    pub(super) async fn open(
        directory: &Path,
        lock: &DirectoryLock,
        name: &str,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Self> {
        let path = directory.join(format!("{name}.db"));
        // Constructed before the open so that a failed open still removes what it made.
        let mut owned = Self {
            projection: None,
            path,
            lock: Some(lock.clone()),
        };
        owned.projection = Some(QueueProjection::open(&owned.path, job, workers).await?);
        Ok(owned)
    }
    pub(super) fn get(&self) -> &Arc<QueueProjection> {
        self.projection.as_ref().expect("open until dropped")
    }
}
impl Drop for TenureProjection {
    fn drop(&mut self) {
        // Close the database before deleting its files.
        self.projection = None;
        for suffix in SUFFIXES {
            let mut file = self.path.clone().into_os_string();
            file.push(suffix);
            // Best effort: the next pipeline discards leftovers at construction.
            let _ = remove(Path::new(&file));
        }
        // Last: the directory is released only after the files are gone.
        drop(self.lock.take());
    }
}

#[cfg(test)]
mod tests {
    use super::{LOCK_FILE, QueueError, is_projection_file, prepare_directory};
    use std::io::{BufRead, BufReader};

    fn unique_directory(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "fireweed-lock-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn lock_file_is_not_a_projection_file() {
        assert!(!is_projection_file(LOCK_FILE));
    }

    #[test]
    fn second_holder_in_one_process_is_refused_until_the_first_is_dropped() {
        let directory = unique_directory("same-process");
        let first = prepare_directory(&directory).unwrap();
        let live = directory.join(format!("q7-{}.db", "a".repeat(64)));
        std::fs::write(&live, b"live").unwrap();
        assert_eq!(
            prepare_directory(&directory).unwrap_err(),
            QueueError::Invalid("projection directory is in use by another handle".into())
        );
        assert_eq!(
            std::fs::read(&live).unwrap(),
            b"live",
            "a refused handle must not delete the live handle's files"
        );
        drop(first);
        let second = prepare_directory(&directory).unwrap();
        assert!(
            !live.exists(),
            "leftovers are deleted once the lock is held"
        );
        assert!(directory.join(LOCK_FILE).exists(), "the lock file is kept");
        drop(second);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn leftover_lock_file_without_a_holder_does_not_block() {
        let directory = unique_directory("leftover");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(LOCK_FILE), b"left by a dead process").unwrap();
        let held = prepare_directory(&directory).unwrap();
        // Never read or written: the contents are untouched.
        assert_eq!(
            std::fs::read(directory.join(LOCK_FILE)).unwrap(),
            b"left by a dead process"
        );
        drop(held);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// Run by `lock_dies_with_a_killed_process` in a child process: take the lock,
    /// report it, and wait to be killed. Without the environment variable, as in a
    /// plain run of the suite, it does nothing.
    #[test]
    fn lock_child_holder() {
        let Some(directory) = std::env::var_os("FIREWEED_LOCK_CHILD_DIRECTORY") else {
            return;
        };
        let _held = prepare_directory(std::path::Path::new(&directory)).unwrap();
        println!("LOCK-HELD");
        std::thread::sleep(std::time::Duration::from_secs(120));
    }

    #[test]
    fn lock_dies_with_a_killed_process() {
        let directory = unique_directory("killed");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "queue_recovery_pipeline::projection_files::tests::lock_child_holder",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FIREWEED_LOCK_CHILD_DIRECTORY", &directory)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        assert!(
            lines.any(|line| line.is_ok_and(|line| line.contains("LOCK-HELD"))),
            "the child never took the lock"
        );
        assert_eq!(
            prepare_directory(&directory).unwrap_err(),
            QueueError::Invalid("projection directory is in use by another handle".into())
        );
        // SIGKILL on Unix: no destructor and no cleanup runs in the child.
        child.kill().unwrap();
        child.wait().unwrap();
        drop(prepare_directory(&directory).unwrap());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn projection_file_names_are_matched_exactly() {
        let digest = "0123456789abcdef".repeat(4);
        for suffix in ["", "-wal", "-shm", "-journal"] {
            assert!(is_projection_file(&format!("q7-{digest}.db{suffix}")));
        }
        assert!(!is_projection_file(&format!("q7-{digest}.db-other")));
        assert!(!is_projection_file(&format!("q7-{digest}")));
        assert!(!is_projection_file(&format!("q8-{digest}.db")));
        assert!(!is_projection_file(&format!("q7-{}.db", &digest[1..])));
        assert!(!is_projection_file(&format!("q7-{digest}0.db")));
        assert!(!is_projection_file(&format!(
            "q7-{}.db",
            digest.to_uppercase()
        )));
        assert!(!is_projection_file(&format!("q7-{}g.db", &digest[1..])));
        assert!(!is_projection_file("q7-"));
    }

    #[test]
    fn non_ascii_names_are_rejected_without_panicking() {
        // 63 ASCII bytes then a 3-byte character: byte 64 is mid-character.
        let straddling = format!("q7-{}\u{20ac}.db", "a".repeat(63));
        assert!(!is_projection_file(&straddling));
        assert!(!is_projection_file(&format!("q7-{}", "\u{e9}".repeat(40))));
        assert!(!is_projection_file("q7-\u{1f600}\u{1f600}"));
    }
}
