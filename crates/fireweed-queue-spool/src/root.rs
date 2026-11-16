//! Construction-owned scratch location. No environment reads or operation-local
//! configuration; the host's directory is never removed by this resource.
use crate::ScratchWorkers;
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
pub struct ScratchRoot {
    path: PathBuf,
}
impl ScratchRoot {
    pub fn validate_path(path: &Path) -> QueueResult<()> {
        if !path.is_absolute() {
            return Err(QueueError::Invalid("scratch root must be absolute".into()));
        }
        Ok(())
    }
    /// Run filesystem setup under the shared physical owner. Caller cancellation
    /// cannot release the closure/path/probe before it physically finishes.
    pub async fn open(path: PathBuf, workers: &ScratchWorkers) -> QueueResult<Arc<Self>> {
        Self::validate_path(&path)?;
        workers
            .run(move || {
                Ok((|| {
                    std::fs::create_dir_all(&path).map_err(|_| {
                        QueueError::Invalid("scratch root cannot be created".into())
                    })?;
                    let path = std::fs::canonicalize(path).map_err(|_| {
                        QueueError::Invalid("scratch root cannot be resolved".into())
                    })?;
                    if !path.is_dir() {
                        return Err(QueueError::Invalid(
                            "scratch root is not a directory".into(),
                        ));
                    }
                    let probe = tempfile::tempfile_in(&path)
                        .map_err(|_| QueueError::Invalid("scratch root is not writable".into()))?;
                    drop(probe);
                    Ok(Arc::new(Self { path }))
                })())
            })
            .await?
    }
    /// Create disposable bytes under this root. Call only inside an owned
    /// physical execution; retain the file until its disk-capacity owner can drop.
    pub fn anonymous_file(&self) -> QueueResult<std::fs::File> {
        tempfile::tempfile_in(&self.path)
            .map_err(|_| QueueError::OutcomeUnknown("scratch file creation failed".into()))
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configured_root_is_canonical_shared_and_preserves_host_contents() {
        let parent = tempfile::tempdir().unwrap();
        let requested = parent.path().join("scratch");
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let root = ScratchRoot::open(requested.clone(), &workers)
            .await
            .unwrap();
        assert_eq!(root.path(), std::fs::canonicalize(&requested).unwrap());
        let sentinel = requested.join("host-owned");
        std::fs::write(&sentinel, b"keep").unwrap();
        let clone = Arc::clone(&root);
        assert!(Arc::ptr_eq(&root, &clone));
        drop(root);
        drop(clone);
        assert_eq!(std::fs::read(sentinel).unwrap(), b"keep");
        assert_eq!(std::fs::read_dir(requested).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn invalid_roots_fail_with_sanitized_errors() {
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let relative = PathBuf::from("private-relative-root");
        assert!(matches!(ScratchRoot::open(relative, &workers).await,
            Err(QueueError::Invalid(message)) if message == "scratch root must be absolute"));
        let parent = tempfile::tempdir().unwrap();
        let file = parent.path().join("private-host-file");
        std::fs::write(&file, b"keep").unwrap();
        let result = ScratchRoot::open(file.clone(), &workers).await;
        assert!(
            matches!(&result, Err(QueueError::Invalid(message)) if message == "scratch root cannot be created"),
            "unexpected error: {:?}",
            result.err()
        );
        assert_eq!(std::fs::read(file).unwrap(), b"keep");
    }
}
