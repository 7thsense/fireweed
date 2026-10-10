//! The small API-007 storage boundary. Implementations own complete operations:
//! cancellation cannot split an atomic group or release its admission charge.
use fireweed_core::queue_contract::*;
use std::{future::Future, pin::Pin};

pub type QueueFuture<'a, T> = Pin<Box<dyn Future<Output = QueueResult<T>> + Send + 'a>>;

/// Mandatory operations, with no default unsupported methods or capability probes.
/// PostgreSQL and S3/Turso implement this same port. A consumer never sees the
/// store's transaction machinery, log positions, or projection coverage cursor.
pub trait QueueStore: Send + Sync {
    fn provision(&self, request: ProvisionRequest) -> QueueFuture<'_, ()>;
    fn insert(&self, request: InsertRequest) -> QueueFuture<'_, Vec<InsertOutcome>>;
    fn claim(&self, request: ClaimRequest) -> QueueFuture<'_, Vec<ClaimedWork>>;
    fn commit(&self, request: CommitBatch) -> QueueFuture<'_, Vec<CommitReceipt>>;
    fn stats(&self, request: StatsRequest) -> QueueFuture<'_, QueueStats>;
    fn read_state(&self, request: StateReadRequest) -> QueueFuture<'_, Vec<Option<StateRecord>>>;
    fn query(&self, request: QueryRequest) -> QueueFuture<'_, QueryPage>;
    fn read_commit(&self, request: ReceiptReadRequest) -> QueueFuture<'_, Vec<ReceiptLookup>>;
    /// Stop new admission and resolve/drain all admitted operations before teardown.
    fn shutdown(&self) -> QueueFuture<'_, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn store_boundary_is_object_safe_and_send_sync() {
        fn accepts_object(_: Option<std::sync::Arc<dyn QueueStore>>) {}
        accepts_object(None);
        fn send_sync<T: Send + Sync>() {}
        send_sync::<std::sync::Arc<dyn QueueStore>>();
    }
}
