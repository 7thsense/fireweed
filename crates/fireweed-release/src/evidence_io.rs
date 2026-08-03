//! Typed ownership boundaries for release-evidence paths.
//!
//! These types are intentionally inert. Later migration work can require an
//! explicit [`Fixture`], [`RunOwned`], or [`Promoted`] value without changing
//! any current producer or reader in this module.

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// An operation a release-evidence path may authorize.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceOperation {
    Read,
    Write,
    Delete,
}

/// A validation failure at the evidence source-I/O boundary.
#[derive(Debug)]
pub enum EvidenceIoError {
    Io(std::io::Error),
    MissingInput(PathBuf),
    InvalidRunRoot(PathBuf),
    TrackedEvidence(PathBuf),
    RepositoryOwned(PathBuf),
    OutsideRunRoot {
        path: PathBuf,
        run_root: PathBuf,
    },
    SymlinkEscape {
        path: PathBuf,
        run_root: PathBuf,
    },
    OperationDenied {
        ownership: &'static str,
        operation: EvidenceOperation,
    },
}

impl fmt::Display for EvidenceIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "evidence path I/O failed: {error}"),
            Self::MissingInput(path) => {
                write!(
                    formatter,
                    "evidence input does not exist: {}",
                    path.display()
                )
            }
            Self::InvalidRunRoot(path) => write!(
                formatter,
                "run-owned root must be an existing external directory: {}",
                path.display()
            ),
            Self::TrackedEvidence(path) => write!(
                formatter,
                "run-owned output cannot target tracked evidence: {}",
                path.display()
            ),
            Self::RepositoryOwned(path) => write!(
                formatter,
                "run-owned output cannot target the repository: {}",
                path.display()
            ),
            Self::OutsideRunRoot { path, run_root } => write!(
                formatter,
                "run-owned output {} is outside {}",
                path.display(),
                run_root.display()
            ),
            Self::SymlinkEscape { path, run_root } => write!(
                formatter,
                "run-owned output {} escapes {} through a symlink",
                path.display(),
                run_root.display()
            ),
            Self::OperationDenied {
                ownership,
                operation,
            } => write!(
                formatter,
                "{ownership} evidence does not authorize {operation:?}"
            ),
        }
    }
}

impl Error for EvidenceIoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for EvidenceIoError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Immutable input supplied explicitly to an ordinary deterministic test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fixture {
    path: PathBuf,
}

mod sealed {
    pub trait Sealed {}
}

/// A typed evidence input whose ownership class authorizes reads.
pub trait ReadableEvidence: sealed::Sealed {
    fn readable_path(&self) -> Result<&Path, EvidenceIoError>;
}

impl Fixture {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, EvidenceIoError> {
        Ok(Self {
            path: canonical_input(path.as_ref())?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn authorize(&self, operation: EvidenceOperation) -> Result<&Path, EvidenceIoError> {
        authorize_read_only("Fixture", &self.path, operation)
    }
}

impl sealed::Sealed for Fixture {}

impl ReadableEvidence for Fixture {
    fn readable_path(&self) -> Result<&Path, EvidenceIoError> {
        self.authorize(EvidenceOperation::Read)
    }
}

/// Immutable evidence promoted from a separately verified run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Promoted {
    path: PathBuf,
}

impl Promoted {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, EvidenceIoError> {
        Ok(Self {
            path: canonical_input(path.as_ref())?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn authorize(&self, operation: EvidenceOperation) -> Result<&Path, EvidenceIoError> {
        authorize_read_only("Promoted", &self.path, operation)
    }
}

impl sealed::Sealed for Promoted {}

impl ReadableEvidence for Promoted {
    fn readable_path(&self) -> Result<&Path, EvidenceIoError> {
        self.authorize(EvidenceOperation::Read)
    }
}

/// An output proven to remain beneath one explicit external run root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunOwned {
    path: PathBuf,
    run_root: PathBuf,
}

impl RunOwned {
    /// Validate `path` without creating it.
    ///
    /// Relative paths are resolved below `run_root`. Existing ancestors are
    /// canonicalized so a symlink cannot redirect a not-yet-created child.
    pub fn new(
        repository_root: impl AsRef<Path>,
        run_root: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> Result<Self, EvidenceIoError> {
        let repository_root = fs::canonicalize(repository_root.as_ref())?;
        let requested_run_root = run_root.as_ref();
        let requested = if path.as_ref().is_absolute() {
            path.as_ref().to_path_buf()
        } else {
            requested_run_root.join(path)
        };
        let lexical = normalize_absolute(&requested)?;
        let tracked_root = repository_root.join("docs/perf/evidence");
        if lexical.starts_with(&tracked_root) {
            return Err(EvidenceIoError::TrackedEvidence(lexical));
        }
        if lexical.starts_with(&repository_root) {
            return Err(EvidenceIoError::RepositoryOwned(lexical));
        }

        let run_root = fs::canonicalize(requested_run_root)
            .map_err(|_| EvidenceIoError::InvalidRunRoot(requested_run_root.to_path_buf()))?;
        if !run_root.is_dir() || run_root.starts_with(&repository_root) {
            return Err(EvidenceIoError::InvalidRunRoot(run_root));
        }

        let relative =
            lexical
                .strip_prefix(&run_root)
                .map_err(|_| EvidenceIoError::OutsideRunRoot {
                    path: lexical.clone(),
                    run_root: run_root.clone(),
                })?;
        let mut cursor = run_root.clone();
        for component in relative.components() {
            cursor.push(component.as_os_str());
            if fs::symlink_metadata(&cursor).is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return Err(EvidenceIoError::SymlinkEscape {
                    path: cursor,
                    run_root,
                });
            }
        }

        let resolved = resolve_existing_ancestor(&lexical)?;
        if !resolved.starts_with(&run_root) {
            let error = if lexical.starts_with(&run_root) {
                EvidenceIoError::SymlinkEscape {
                    path: resolved,
                    run_root,
                }
            } else {
                EvidenceIoError::OutsideRunRoot {
                    path: resolved,
                    run_root,
                }
            };
            return Err(error);
        }

        Ok(Self {
            path: resolved,
            run_root,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn run_root(&self) -> &Path {
        &self.run_root
    }

    pub fn authorize(&self, _operation: EvidenceOperation) -> Result<&Path, EvidenceIoError> {
        let current_root = fs::canonicalize(&self.run_root)
            .map_err(|_| EvidenceIoError::InvalidRunRoot(self.run_root.clone()))?;
        if current_root != self.run_root || !current_root.is_dir() {
            return Err(EvidenceIoError::InvalidRunRoot(current_root));
        }
        let relative = self.path.strip_prefix(&self.run_root).map_err(|_| {
            EvidenceIoError::OutsideRunRoot {
                path: self.path.clone(),
                run_root: self.run_root.clone(),
            }
        })?;
        let mut cursor = self.run_root.clone();
        for component in relative.components() {
            cursor.push(component.as_os_str());
            if fs::symlink_metadata(&cursor).is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return Err(EvidenceIoError::SymlinkEscape {
                    path: cursor,
                    run_root: self.run_root.clone(),
                });
            }
        }
        let resolved = resolve_existing_ancestor(&self.path)?;
        if !resolved.starts_with(&self.run_root) {
            return Err(EvidenceIoError::SymlinkEscape {
                path: resolved,
                run_root: self.run_root.clone(),
            });
        }
        Ok(&self.path)
    }

    /// Write the complete artifact after re-validating write authority.
    pub fn write(&self, body: impl AsRef<[u8]>) -> Result<(), EvidenceIoError> {
        let path = self.authorize(EvidenceOperation::Write)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, body).map_err(EvidenceIoError::Io)
    }

    /// Delete this run's artifact if present after re-validating delete authority.
    pub fn delete(&self) -> Result<(), EvidenceIoError> {
        let path = self.authorize(EvidenceOperation::Delete)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(EvidenceIoError::Io(error)),
        }
    }
}

impl AsRef<Path> for RunOwned {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

impl sealed::Sealed for RunOwned {}

impl ReadableEvidence for RunOwned {
    fn readable_path(&self) -> Result<&Path, EvidenceIoError> {
        self.authorize(EvidenceOperation::Read)
    }
}

fn canonical_input(path: &Path) -> Result<PathBuf, EvidenceIoError> {
    fs::canonicalize(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            EvidenceIoError::MissingInput(path.to_path_buf())
        } else {
            EvidenceIoError::Io(error)
        }
    })
}

fn authorize_read_only<'a>(
    ownership: &'static str,
    path: &'a Path,
    operation: EvidenceOperation,
) -> Result<&'a Path, EvidenceIoError> {
    match operation {
        EvidenceOperation::Read => Ok(path),
        EvidenceOperation::Write | EvidenceOperation::Delete => {
            Err(EvidenceIoError::OperationDenied {
                ownership,
                operation,
            })
        }
    }
}

fn normalize_absolute(path: &Path) -> Result<PathBuf, EvidenceIoError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(EvidenceIoError::OutsideRunRoot {
                        path: absolute,
                        run_root: PathBuf::new(),
                    });
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

fn resolve_existing_ancestor(path: &Path) -> Result<PathBuf, EvidenceIoError> {
    let mut existing = path.to_path_buf();
    let mut suffix = Vec::new();
    while !existing.exists() {
        let Some(name) = existing.file_name().map(ToOwned::to_owned) else {
            return Err(EvidenceIoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no existing ancestor for {}", path.display()),
            )));
        };
        suffix.push(name);
        existing.pop();
    }
    let mut resolved = fs::canonicalize(existing)?;
    for part in suffix.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TempTree(PathBuf);

    impl TempTree {
        fn new(label: &str) -> Self {
            let suffix = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "fireweed-evidence-io-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn repository_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap()
    }

    #[test]
    fn accepts_not_yet_created_external_descendant() {
        let temp = TempTree::new("positive");
        let run_root = temp.path().join("run");
        fs::create_dir(&run_root).unwrap();
        let owned = RunOwned::new(repository_root(), &run_root, "nested/result.jsonl").unwrap();
        assert!(owned.path().starts_with(run_root.canonicalize().unwrap()));
        assert_eq!(
            owned.authorize(EvidenceOperation::Write).unwrap(),
            owned.path()
        );
        assert_eq!(
            owned.authorize(EvidenceOperation::Delete).unwrap(),
            owned.path()
        );
        assert!(!owned.path().exists());
    }

    #[test]
    fn rejects_repository_and_tracked_evidence_targets() {
        let temp = TempTree::new("repo-negative");
        let run_root = temp.path().join("run");
        fs::create_dir(&run_root).unwrap();
        let repo = repository_root();

        let tracked = repo.join("docs/perf/evidence/new.jsonl");
        assert!(matches!(
            RunOwned::new(&repo, &run_root, tracked),
            Err(EvidenceIoError::TrackedEvidence(_))
        ));

        let repository_output = repo.join("target/not-run-owned.jsonl");
        assert!(matches!(
            RunOwned::new(&repo, &run_root, repository_output),
            Err(EvidenceIoError::RepositoryOwned(_))
        ));
    }

    #[test]
    fn rejects_parent_traversal() {
        let temp = TempTree::new("traversal");
        let run_root = temp.path().join("run");
        fs::create_dir(&run_root).unwrap();
        assert!(matches!(
            RunOwned::new(repository_root(), &run_root, "../escape.jsonl"),
            Err(EvidenceIoError::OutsideRunRoot { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = TempTree::new("symlink");
        let run_root = temp.path().join("run");
        let outside = temp.path().join("outside");
        fs::create_dir(&run_root).unwrap();
        fs::create_dir(&outside).unwrap();
        symlink(&outside, run_root.join("redirect")).unwrap();

        assert!(matches!(
            RunOwned::new(
                repository_root(),
                &run_root,
                run_root.join("redirect/result.jsonl")
            ),
            Err(EvidenceIoError::SymlinkEscape { .. })
        ));

        let late = RunOwned::new(repository_root(), &run_root, "late/result.jsonl").unwrap();
        symlink(&outside, run_root.join("late")).unwrap();
        assert!(matches!(
            late.write(b"must not escape"),
            Err(EvidenceIoError::SymlinkEscape { .. })
        ));
        assert!(matches!(
            late.delete(),
            Err(EvidenceIoError::SymlinkEscape { .. })
        ));
    }

    #[test]
    fn fixture_and_promoted_are_read_only() {
        let temp = TempTree::new("ownership");
        let input = temp.path().join("input.jsonl");
        fs::write(&input, b"{}\n").unwrap();
        let fixture = Fixture::new(&input).unwrap();
        let promoted = Promoted::new(&input).unwrap();

        assert!(fixture.authorize(EvidenceOperation::Read).is_ok());
        assert!(promoted.authorize(EvidenceOperation::Read).is_ok());
        for operation in [EvidenceOperation::Write, EvidenceOperation::Delete] {
            assert!(matches!(
                fixture.authorize(operation),
                Err(EvidenceIoError::OperationDenied {
                    ownership: "Fixture",
                    operation: denied,
                }) if denied == operation
            ));
            assert!(matches!(
                promoted.authorize(operation),
                Err(EvidenceIoError::OperationDenied {
                    ownership: "Promoted",
                    operation: denied,
                }) if denied == operation
            ));
        }
    }
}
