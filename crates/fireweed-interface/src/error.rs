//! Errors (API-006 §Errors).

use serde::{Deserialize, Serialize};

use crate::caps::{DurabilityLevel, QueueFeature, ShapeKind};
use crate::expr::EvalError;
use crate::ids::{FieldName, ObjectName, ReadName};

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataPlaneError {
    SpaceNotFound,
    ObjectNotFound(ObjectName),
    SchemaConflict {
        current_version: Option<u64>,
    },
    FeatureUnsupported(Unsupported),
    IndexNotReady(FieldName),
    SelectorNotIndexed,
    ReadNotDeclared {
        name: ReadName,
        version: u32,
    },
    GatesNotEnabled,
    InvalidRequest(String),
    BatchTooLarge(LimitKind),
    /// Reads only (Int `Sum` overflow).
    Evaluation(EvalError),
    /// Aggregate reads only.
    ScanLimitExceeded,
    RequestIdConflict,
    RequestExpired,
    CursorExpired {
        earliest: Option<u64>,
    },
    InvalidContinuation,
    BlobNotFound,
    DurabilityPending {
        reached: DurabilityLevel,
    },
    Contention,
    CommitTimeout,
    RateLimited {
        retry_after_ms: Option<u64>,
    },
    Unavailable(String),
    SpaceBusy,
    InstanceSuperseded,
    Internal(String),
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Unsupported {
    Shape(ShapeKind),
    QueueFeature(QueueFeature),
    Durability(DurabilityLevel),
    Limit(LimitKind),
    QueueHistory,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LimitKind {
    Ops,
    Entries,
    RequestBytes,
    InList,
    ClaimBatch,
    CohortSize,
    LeaseDuration,
    RequestIdRetention,
    Objects,
}

/// Whether an error leaves the outcome known (API-006 §Errors).
impl DataPlaneError {
    pub fn is_indeterminate(&self) -> bool {
        matches!(
            self,
            DataPlaneError::CommitTimeout | DataPlaneError::Unavailable(_)
        )
    }
}

impl std::fmt::Display for DataPlaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DataPlaneError {}

pub type DpResult<T> = Result<T, DataPlaneError>;
