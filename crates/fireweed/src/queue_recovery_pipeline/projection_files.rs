//! One projection database file per owned queue, in a directory the pipeline
//! owns alone. A file exists exactly as long as the tenure that opened it.
use super::*;
use fireweed_engine::queue_admission::QueueReservation;
use std::path::{Path, PathBuf};

/// The files a projection database may leave next to its main file.
const SUFFIXES: [&str; 4] = ["", "-wal", "-shm", "-journal"];

/// Check the directory before any I/O, create it if missing, and delete every
/// projection file an earlier process left in it. Nothing is read.
pub(super) fn prepare_directory(directory: &Path) -> QueueResult<()> {
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
    for entry in std::fs::read_dir(directory).map_err(|e| unknown(&e.to_string()))? {
        let entry = entry.map_err(|e| unknown(&e.to_string()))?;
        let is_file = entry.file_type().is_ok_and(|kind| kind.is_file());
        if is_file && entry.file_name().to_str().is_some_and(is_projection_file) {
            remove(&entry.path())?;
        }
    }
    Ok(())
}

/// `q7-<64 lowercase hex>.db`, optionally followed by `-wal`, `-shm` or `-journal`.
fn is_projection_file(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("q7-") else {
        return false;
    };
    let (digest, tail) = rest.split_at(rest.len().min(64));
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && SUFFIXES.iter().any(|suffix| tail == format!(".db{suffix}"))
}

fn remove(path: &Path) -> QueueResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(unknown(&error.to_string())),
    }
}

/// The open projection database of one tenure. This is the only place a queue's
/// projection files are removed: dropping it closes the database, then deletes
/// the files.
pub(super) struct TenureProjection {
    projection: Option<Arc<QueueProjection>>,
    path: PathBuf,
}
impl TenureProjection {
    /// Open `<directory>/<name>.db` with whatever it held discarded.
    pub(super) async fn open(
        directory: &Path,
        name: &str,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Self> {
        let path = directory.join(format!("{name}.db"));
        // Constructed before the open so that a failed open still removes what it made.
        let mut owned = Self {
            projection: None,
            path,
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
    }
}
