//! Inert source/evidence boundary used by later release-gate migration work.
//!
//! The boundary is fail-closed: [`SourceBoundary::new`] starts with no promoted
//! evidence allowlist. Callers must name each promoted data input explicitly,
//! while source, executable, and build-context inputs must always resolve below
//! the source root and outside the evidence root.

use crate::Promoted;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// A source-side input class that an allowlist must never authorize.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceInputKind {
    Source,
    Executable,
    BuildContext,
}

/// A path consumed as source, an executable, or build context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceInput {
    path: PathBuf,
    kind: SourceInputKind,
}

impl SourceInput {
    pub fn new(kind: SourceInputKind, path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            kind,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn kind(&self) -> SourceInputKind {
        self.kind
    }
}

/// A Constraint 11 validation failure.
#[derive(Debug)]
pub enum SourceBoundaryError {
    Io(std::io::Error),
    InvalidRoot {
        root: &'static str,
        path: PathBuf,
    },
    OutsideSourceRoot {
        kind: SourceInputKind,
        path: PathBuf,
    },
    EvidenceOverlap {
        kind: SourceInputKind,
        path: PathBuf,
    },
    AllowlistEntryOutsideEvidenceRoot(PathBuf),
    PromotedDataNotAllowlisted(PathBuf),
}

impl fmt::Display for SourceBoundaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "source-boundary I/O failed: {error}"),
            Self::InvalidRoot { root, path } => {
                write!(
                    formatter,
                    "{root} root is not a directory: {}",
                    path.display()
                )
            }
            Self::OutsideSourceRoot { kind, path } => write!(
                formatter,
                "{kind:?} input originates outside the source root: {}",
                path.display()
            ),
            Self::EvidenceOverlap { kind, path } => write!(
                formatter,
                "{kind:?} input overlaps the evidence root: {}",
                path.display()
            ),
            Self::AllowlistEntryOutsideEvidenceRoot(path) => write!(
                formatter,
                "promoted-data allowlist entry is outside the evidence root: {}",
                path.display()
            ),
            Self::PromotedDataNotAllowlisted(path) => write!(
                formatter,
                "promoted data is not explicitly allowlisted: {}",
                path.display()
            ),
        }
    }
}

impl Error for SourceBoundaryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SourceBoundaryError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// A pure validation engine for the release source/evidence boundary.
#[derive(Clone, Debug)]
pub struct SourceBoundary {
    source_root: PathBuf,
    evidence_root: PathBuf,
    promoted_allowlist: BTreeSet<PathBuf>,
}

impl SourceBoundary {
    /// Construct a boundary whose promoted-data allowlist is empty.
    pub fn new(
        source_root: impl AsRef<Path>,
        evidence_root: impl AsRef<Path>,
    ) -> Result<Self, SourceBoundaryError> {
        let source_root = canonical_directory("source", source_root.as_ref())?;
        let evidence_root = canonical_directory("evidence", evidence_root.as_ref())?;
        Ok(Self {
            source_root,
            evidence_root,
            promoted_allowlist: BTreeSet::new(),
        })
    }

    /// Replace the promoted-data allowlist with exact, canonical evidence paths.
    ///
    /// Directory entries do not authorize descendants: validation compares the
    /// promoted input's canonical path for exact equality.
    pub fn with_promoted_allowlist<I, P>(mut self, paths: I) -> Result<Self, SourceBoundaryError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut allowlist = BTreeSet::new();
        for path in paths {
            let path = fs::canonicalize(path.as_ref())?;
            if !path.starts_with(&self.evidence_root) {
                return Err(SourceBoundaryError::AllowlistEntryOutsideEvidenceRoot(path));
            }
            allowlist.insert(path);
        }
        self.promoted_allowlist = allowlist;
        Ok(self)
    }

    /// Validate all declared source-side and promoted-data inputs.
    pub fn validate(
        &self,
        source_inputs: &[SourceInput],
        promoted_inputs: &[Promoted],
    ) -> Result<(), SourceBoundaryError> {
        for input in source_inputs {
            let path = fs::canonicalize(input.path())?;
            if !path.starts_with(&self.source_root) {
                return Err(SourceBoundaryError::OutsideSourceRoot {
                    kind: input.kind(),
                    path,
                });
            }
            if path.starts_with(&self.evidence_root) {
                return Err(SourceBoundaryError::EvidenceOverlap {
                    kind: input.kind(),
                    path,
                });
            }
        }

        for promoted in promoted_inputs {
            if !self.promoted_allowlist.contains(promoted.path()) {
                return Err(SourceBoundaryError::PromotedDataNotAllowlisted(
                    promoted.path().to_path_buf(),
                ));
            }
        }
        Ok(())
    }

    pub fn promoted_allowlist_is_empty(&self) -> bool {
        self.promoted_allowlist.is_empty()
    }
}

fn canonical_directory(root: &'static str, path: &Path) -> Result<PathBuf, SourceBoundaryError> {
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_dir() {
        return Err(SourceBoundaryError::InvalidRoot {
            root,
            path: canonical,
        });
    }
    Ok(canonical)
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
                "fireweed-source-boundary-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(path.join("source/evidence")).unwrap();
            fs::create_dir_all(path.join("outside")).unwrap();
            Self(path)
        }

        fn path(&self, relative: &str) -> PathBuf {
            self.0.join(relative)
        }

        fn write(&self, relative: &str) -> PathBuf {
            let path = self.path(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&path, b"fixture\n").unwrap();
            path
        }

        fn boundary(&self) -> SourceBoundary {
            SourceBoundary::new(self.path("source"), self.path("source/evidence")).unwrap()
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn accepts_source_only_inputs() {
        let temp = TempTree::new("source-only");
        let source = temp.write("source/src/lib.rs");
        let executable = temp.write("source/bin/verifier");
        let build_context = temp.write("source/Cargo.toml");
        let inputs = [
            SourceInput::new(SourceInputKind::Source, source),
            SourceInput::new(SourceInputKind::Executable, executable),
            SourceInput::new(SourceInputKind::BuildContext, build_context),
        ];

        temp.boundary().validate(&inputs, &[]).unwrap();
    }

    #[test]
    fn rejects_source_classes_in_the_evidence_root_even_when_named_in_allowlist() {
        let temp = TempTree::new("evidence-overlap");
        let evidence = temp.write("source/evidence/tracked.jsonl");

        for kind in [
            SourceInputKind::Source,
            SourceInputKind::Executable,
            SourceInputKind::BuildContext,
        ] {
            let boundary = temp
                .boundary()
                .with_promoted_allowlist([&evidence])
                .unwrap();
            let error = boundary
                .validate(&[SourceInput::new(kind, &evidence)], &[])
                .unwrap_err();
            assert!(matches!(
                error,
                SourceBoundaryError::EvidenceOverlap {
                    kind: rejected,
                    ..
                } if rejected == kind
            ));
        }
    }

    #[test]
    fn rejects_source_classes_outside_the_source_root() {
        let temp = TempTree::new("outside-source");
        let outside = temp.write("outside/input");

        for kind in [
            SourceInputKind::Source,
            SourceInputKind::Executable,
            SourceInputKind::BuildContext,
        ] {
            assert!(matches!(
                temp.boundary()
                    .validate(&[SourceInput::new(kind, &outside)], &[]),
                Err(SourceBoundaryError::OutsideSourceRoot {
                    kind: rejected,
                    ..
                }) if rejected == kind
            ));
        }
    }

    #[test]
    fn empty_default_rejects_promoted_data() {
        let temp = TempTree::new("empty-default");
        let evidence = temp.write("source/evidence/promoted.jsonl");
        let promoted = Promoted::new(evidence).unwrap();
        let boundary = temp.boundary();

        assert!(boundary.promoted_allowlist_is_empty());
        assert!(matches!(
            boundary.validate(&[], &[promoted]),
            Err(SourceBoundaryError::PromotedDataNotAllowlisted(_))
        ));
    }

    #[test]
    fn exact_allowlist_entry_accepts_only_the_named_promoted_data() {
        let temp = TempTree::new("exact-allowlist");
        let accepted = temp.write("source/evidence/accepted.jsonl");
        let rejected = temp.write("source/evidence/rejected.jsonl");
        let boundary = temp
            .boundary()
            .with_promoted_allowlist([&accepted])
            .unwrap();

        boundary
            .validate(&[], &[Promoted::new(&accepted).unwrap()])
            .unwrap();
        assert!(matches!(
            boundary.validate(&[], &[Promoted::new(rejected).unwrap()]),
            Err(SourceBoundaryError::PromotedDataNotAllowlisted(_))
        ));
    }

    #[test]
    fn allowlist_cannot_name_non_evidence_data() {
        let temp = TempTree::new("allowlist-scope");
        let source = temp.write("source/src/lib.rs");

        assert!(matches!(
            temp.boundary().with_promoted_allowlist([source]),
            Err(SourceBoundaryError::AllowlistEntryOutsideEvidenceRoot(_))
        ));
    }
}
