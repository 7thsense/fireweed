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

#[cfg(test)]
mod tests {
    use super::is_projection_file;

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
