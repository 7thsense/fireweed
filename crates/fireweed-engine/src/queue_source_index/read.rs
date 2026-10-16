//! Source-only reads against one immutable captured cut. This is a driven port,
//! not a transaction callback: implementations must never expose compiled edits
//! as preimages, refresh the cut, or infer absence/completion from cache state.
use super::{QueueSourceCut, QueueSourcePoint};
use fireweed_core::queue_contract::QueueResult;

pub trait QueueSourceReader: Sync {
    /// Owners retain backend source carriers, complete literal keys/bounds,
    /// originating cut identity and operation admission through every borrow.
    type Point: Send + Sync;
    type Range: Send + Sync;

    fn cut(&self) -> QueueResult<QueueSourceCut<'_>>;
    /// Every invocation, including failures, consumes a non-reusable grant from
    /// the SAME request-wide work ledger. SDK per-call bounds alone do not meet
    /// this contract; range continuation pages consume that ledger too.
    fn lookup(
        &self,
        full_key: &[u8],
    ) -> impl std::future::Future<Output = QueueResult<Self::Point>> + Send;
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>>;
    /// Returns a carrier only after the uninterrupted original-bound scan reaches
    /// Complete. Limits, cancellation, missing final pages and source errors fail.
    fn complete_range(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> impl std::future::Future<Output = QueueResult<Self::Range>> + Send;
    /// Validate the retained cut, bounds and complete-chain certificate BEFORE
    /// calling a fallible visitor. An error after a prefix never seals output.
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()>;
}

/// Explicit record-reading capability for addressed payload materialization.
/// Metadata-only compilation does not require or implicitly enable this port.
pub trait QueueSourceRecordReader: QueueSourceReader {
    /// Owns checked storage and the actual original cut/job/offset binding.
    type Record: Send + Sync;
    /// Validate the supplied original cut before dispatch; use explicit finite
    /// record limits and the SAME nonrefunding request ledger as semantic reads.
    fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> impl std::future::Future<Output = QueueResult<Self::Record>> + Send;
    /// Reject foreign carriers even when numeric heads and bytes are equal.
    /// The returned borrow remains protected by its actual retained carrier.
    fn checked_record<'a>(
        &'a self,
        owner: &'a Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<crate::queue_log_record::CheckedQueueLogRecord<'a>>;
}
