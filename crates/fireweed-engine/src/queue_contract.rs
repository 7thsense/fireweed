//! The small API-007 storage boundary. Implementations own complete operations:
//! cancellation cannot split an atomic group or release its admission charge.
use fireweed_core::queue_contract::*;
use std::{future::Future, pin::Pin};

pub type QueueFuture<'a, T> = Pin<Box<dyn Future<Output = QueueResult<T>> + Send + 'a>>;

/// Own a response and its admission until the successful consumer handoff.
/// Field order ensures abandoned replies discard payloads before releasing
/// their physical owners. Keep this whole value intact in result channels.
pub struct OwnedQueueValue<T, Owner: Send> {
    value: T,
    _owner: Owner,
}
impl<T, Owner: Send> OwnedQueueValue<T, Owner> {
    pub fn new(value: T, owner: Owner) -> Self {
        Self {
            value,
            _owner: owner,
        }
    }
    /// Call only at the successful QueueStore future return to its caller.
    pub fn into_value(self) -> T {
        self.value
    }
}

/// Mandatory operations, with no default unsupported methods or capability probes.
/// PostgreSQL and S3/Turso implement this same port. A consumer never sees the
/// store's transaction machinery, log positions, or projection coverage cursor.
pub trait QueueStore: Send + Sync {
    fn provision(&self, request: ProvisionRequest) -> QueueFuture<'_, ()>;
    fn insert(&self, request: InsertRequest) -> QueueFuture<'_, Vec<InsertOutcome>>;
    fn claim(&self, request: ClaimRequest) -> QueueFuture<'_, Vec<ClaimedWork>>;
    fn commit(&self, request: CommitBatch) -> QueueFuture<'_, Vec<CommitReceipt>>;
    fn preview(&self, request: PreviewRequest) -> QueueFuture<'_, PreviewResult>;
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
    fn abandoned_owned_response_drops_payload_before_owner() {
        use std::sync::{Arc, Mutex};
        struct Dropped(&'static str, Arc<Mutex<Vec<&'static str>>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.1.lock().unwrap().push(self.0);
            }
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        drop(OwnedQueueValue::new(
            Dropped("payload", events.clone()),
            Dropped("owner", events.clone()),
        ));
        assert_eq!(*events.lock().unwrap(), vec!["payload", "owner"]);
        events.lock().unwrap().clear();
        let payload = OwnedQueueValue::new(
            Dropped("payload", events.clone()),
            Dropped("owner", events.clone()),
        )
        .into_value();
        assert_eq!(*events.lock().unwrap(), vec!["owner"]);
        drop(payload);
        assert_eq!(*events.lock().unwrap(), vec!["owner", "payload"]);
    }
    #[test]
    fn store_boundary_is_object_safe_and_send_sync() {
        fn accepts_object(_: Option<std::sync::Arc<dyn QueueStore>>) {}
        accepts_object(None);
        fn send_sync<T: Send + Sync>() {}
        send_sync::<std::sync::Arc<dyn QueueStore>>();
    }
}
