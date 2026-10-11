//! API-007's concrete consumer interface. Insert, filtered priority claim, atomic
//! commit with state/new work, and stats use identical types on every backend.
//! Supporting state/query/receipt operations carry no storage transaction handle.
//! The native PostgreSQL constructor is available with the `postgres` feature;
//! S3/Turso construction will use this same handle after its mandatory qualification.
pub use crate::{Bytes, Clock, ConfigSecret, SystemClock, UtcTimestamp};
pub use fireweed_core::queue_administration::*;
pub use fireweed_core::queue_contract::*;
use fireweed_engine::queue_contract::QueueStore;
use std::{fmt, sync::Arc};
/// Owned-operation future. Once admitted, dropping this caller future cannot
/// cancel the underlying atomic storage operation or release its reservation.
pub type QueueFuture<'a, T> = fireweed_engine::queue_contract::QueueFuture<'a, T>;

/// One backend-neutral queue handle. Clones share storage and admission.
/// `shutdown` closes admission and drains operations for **all** clones.
/// Backend selection happens only at construction; there is no capability probe,
/// transaction/session handle, backend getter or public backend injection.
#[derive(Clone)]
pub struct Queue {
    store: Arc<dyn QueueStore>,
}
impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Queue(API-007)")
    }
}

// Both public inherent calls and the mandatory internal port return the backend's
// original future directly. There is no extra task, admission or cancellation layer.
macro_rules! queue_methods {
    ($($name:ident($($arg:ident:$input:ty),*) -> $result:ty;)+) => {
        impl Queue {$(pub fn $name(&self,$($arg:$input),*) -> QueueFuture<'_,$result> {self.store.$name($($arg),*)})+}
        impl QueueStore for Queue {$(fn $name(&self,$($arg:$input),*) -> QueueFuture<'_,$result> {self.store.$name($($arg),*)})+}
    };
}
queue_methods! {
    provision(request:ProvisionRequest)->();
    insert(request:InsertRequest)->Vec<InsertOutcome>;
    claim(request:ClaimRequest)->Vec<ClaimedWork>;
    commit(request:CommitBatch)->Vec<CommitReceipt>;
    preview(request:PreviewRequest)->PreviewResult;
    stats(request:StatsRequest)->QueueStats;
    read_state(request:StateReadRequest)->Vec<Option<StateRecord>>;
    query(request:QueryRequest)->QueryPage;
    read_commit(request:ReceiptReadRequest)->Vec<ReceiptLookup>;
    shutdown()->();
}

/// Native baseline construction settings; the DSN stays redacted in Debug.
/// Limits are the same mandatory interface limits used by every adapter.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresOptions {
    pub url: ConfigSecret,
    pub limits: QueueLimits,
    pub pool_size: usize,
}
#[cfg(feature = "postgres")]
impl PostgresOptions {
    pub fn new(url: ConfigSecret) -> Self {
        Self {
            url,
            limits: QueueLimits::default(),
            pool_size: 2,
        }
    }
}
#[cfg(feature = "postgres")]
impl Queue {
    /// Construct the native asynchronous PostgreSQL baseline. The explicit clock
    /// controls logical eligibility/lease semantics, independently of operation
    /// admission and infrastructure deadlines. Use SystemClock in production.
    pub async fn postgres(options: PostgresOptions, clock: Arc<dyn Clock>) -> QueueResult<Self> {
        let store = fireweed_postgres::PostgresQueueStore::connect(
            &options.url.0.0,
            clock,
            options.limits,
            options.pool_size,
        )
        .await?;
        Ok(Self {
            store: Arc::new(store),
        })
    }
}
