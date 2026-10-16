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

/// Incremental source authority for bounded backend selection. Observed pages
/// are provisional until `finish_stream` certifies the uninterrupted original
/// range. A completion proves that range only: the selection bridge must also
/// bind schema, selector, evaluation time and any private overlay generation,
/// and certify ALL contributing ranges before exposing stream EOF.
pub trait QueueSourceStreamingReader: QueueSourceReader {
    type Cursor: Send;
    type Page: Send + Sync;
    type Completion: Send + Sync;

    fn open_stream(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Cursor>;
    /// Poison/invalidate the cursor before dispatch. Failed or cancelled reads
    /// never restore credit or allow an alternate continuation. `None` is legal
    /// only after a successfully visited SDK Complete page, including empty EOF.
    fn next_stream_page(
        &self,
        cursor: &mut Self::Cursor,
    ) -> impl std::future::Future<Output = QueueResult<Option<Self::Page>>> + Send;
    /// Consume the exact outstanding page once. Validate the whole page before
    /// exposing any point, and poison before invoking the fallible visitor.
    /// Visitors must independently admit copies before retaining keys or values.
    /// Failure, unwind or dropping an unvisited page permanently prevents finish.
    fn visit_stream_page(
        &self,
        cursor: &mut Self::Cursor,
        page: Self::Page,
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()>;
    fn finish_stream(&self, cursor: Self::Cursor) -> QueueResult<Self::Completion>;
    /// Check the actual opaque source, operation and ORIGINAL full bounds.
    fn validate_stream_completion(
        &self,
        completion: &Self::Completion,
        lower: &[u8],
        upper: &[u8],
    ) -> QueueResult<()>;
}
