//! Source selection planning and ownership; no projection IDs supply authority.
mod scalar;
pub use scalar::{QueueSourceScalarRange, QueueSourceScalarRanges, plan_source_scalar_ranges};

mod work;
pub use work::{QueueSourceSelectionBudget, QueueSourceSelectionWork};

mod scratch;
pub use scratch::QueueSourceCandidateScratch;

mod producer;
pub use producer::{
    QueueSourceCandidateProducer, QueueSourceScalarLeaf, QueueSourceSelectionRequest,
};
