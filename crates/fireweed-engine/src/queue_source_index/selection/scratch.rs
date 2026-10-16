//! Driven disposable set storage. This port has no source-cut, record-version,
//! predicate or completion authority. Only the engine's private source driver
//! may combine these physical results with its actual uninterrupted range proofs.
use super::QueueSourceSelectionBudget;
use crate::queue_admission::QueueReservation;
use fireweed_core::queue_contract::QueueResult;
use std::{future::Future, sync::Arc};

/// Keep the actual owner in an admitted physical worker. Methods must take that
/// owner before awaiting and restore it only after complete success; cancellation
/// must retire it. A handle retains an identity token, never the physical owner.
/// Every traversal (including duplicate probes and terminal reads) spends the
/// SAME nonrefunding request ledger before dispatch/copy. Bulk SQL is not allowed
/// to substitute one statement charge for data-dependent row work.
///
/// `next` permits seeks, not coverage proofs. The source driver privately owns
/// the exact set and its uninterrupted cursor; arbitrary seek EOF is not enough.
pub trait QueueSourceCandidateScratch: Send {
    type Builder: Send;
    type Set: Send + Sync;
    type Row: Send + Sync;

    /// Check actual operation identity AND budget Arc identity before spending
    /// or dispatch. Equal counters or the same job alone do not authorize a
    /// replacement ledger during recapture.
    fn validate_operation(
        &self,
        job: &QueueReservation,
        budget: &Arc<QueueSourceSelectionBudget>,
    ) -> QueueResult<()>;
    fn begin_set(&mut self) -> QueueResult<Self::Builder>;
    fn insert(
        &mut self,
        builder: &mut Self::Builder,
        collection: Option<&str>,
        key: &str,
    ) -> impl Future<Output = QueueResult<()>> + Send;
    /// Consume the unique mutable builder. This seals physical membership only,
    /// never source coverage; failed/partial output cannot become a source proof.
    fn seal(&self, builder: Self::Builder) -> QueueResult<Self::Set>;
    fn union(
        &mut self,
        left: &Self::Set,
        right: &Self::Set,
    ) -> impl Future<Output = QueueResult<Self::Set>> + Send;
    fn intersection(
        &mut self,
        left: &Self::Set,
        right: &Self::Set,
    ) -> impl Future<Output = QueueResult<Self::Set>> + Send;
    fn difference(
        &mut self,
        left: &Self::Set,
        right: &Self::Set,
    ) -> impl Future<Output = QueueResult<Self::Set>> + Send;
    fn next(
        &mut self,
        set: &Self::Set,
        after: Option<&Self::Row>,
    ) -> impl Future<Output = QueueResult<Option<Self::Row>>> + Send;
    /// Validate the actual row/session before borrowing its retained full
    /// canonical QueueSourceKey::Record identity. SQL ordinals/hashes are not
    /// identities. This borrow proves neither source presence nor freshness.
    fn encoded_identity<'a>(&self, row: &'a Self::Row) -> QueueResult<&'a [u8]>;
    fn count(&self, set: &Self::Set) -> QueueResult<u64>;
}
