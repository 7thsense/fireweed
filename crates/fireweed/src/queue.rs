//! API-007's concrete consumer interface. Insert, filtered priority claim, atomic
//! commit with state/new work, and stats use identical types on every backend.
//! Supporting state/query/receipt operations carry no storage transaction handle.
//! The native PostgreSQL constructor is available with the `postgres` feature;
//! S3/Turso construction will use this same handle after its mandatory qualification.
pub use crate::{Bytes, Clock, ConfigSecret, SystemClock, UtcTimestamp};
pub use fireweed_core::queue_administration::*;
pub use fireweed_core::queue_contract::*;
pub use fireweed_core::queue_json::{
    JsonEqualityEncoding, JsonNumberEquality, current_json_equality_encoding,
    encode_json_equality_for, json_attribute_name,
};
pub use fireweed_core::queue_runtime::QueueRuntimeOptions;
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
    pub runtime: QueueRuntimeOptions,
}
#[cfg(feature = "postgres")]
impl PostgresOptions {
    pub fn new(url: ConfigSecret) -> Self {
        Self {
            url,
            limits: QueueLimits::default(),
            pool_size: 2,
            runtime: QueueRuntimeOptions::default(),
        }
    }
}
#[cfg(feature = "postgres")]
impl Queue {
    /// Construct the native asynchronous PostgreSQL baseline. The explicit clock
    /// controls logical eligibility/lease semantics, independently of operation
    /// admission and infrastructure deadlines. Use SystemClock in production.
    pub async fn postgres(options: PostgresOptions, clock: Arc<dyn Clock>) -> QueueResult<Self> {
        let store = fireweed_postgres::PostgresQueueStore::connect_with_runtime(
            &options.url.0.0,
            clock,
            options.limits,
            options.pool_size,
            options.runtime,
        )
        .await?;
        Ok(Self {
            store: Arc::new(store),
        })
    }
}

#[cfg(test)]
mod equality_facade_tests {
    #[test]
    fn consumer_equality_codec_pins_namespace_and_rejects_drift() {
        use crate::queue::{
            JsonEqualityEncoding, JsonNumberEquality, QueueError, current_json_equality_encoding,
            encode_json_equality_for, json_attribute_name,
        };
        let pin: JsonEqualityEncoding = current_json_equality_encoding();
        assert!(matches!(
            pin.numbers,
            JsonNumberEquality::NumericVariants | JsonNumberEquality::LiteralRepresentations
        ));
        assert_eq!(
            json_attribute_name("snorri.state", "a/b~c"),
            "snorri.state/a~1b~0c"
        );
        let left = serde_json::json!({"a/b~c": [null, {"雪": true}], "n": 1});
        let right = serde_json::json!({"n": 1, "a/b~c": [null, {"雪": true}]});
        assert_eq!(
            encode_json_equality_for(&left, pin, 4096).unwrap(),
            encode_json_equality_for(&right, pin, 4096).unwrap()
        );
        let wrong = JsonEqualityEncoding {
            version: pin.version + 1,
            ..pin
        };
        assert!(matches!(
            encode_json_equality_for(&left, wrong, 4096),
            Err(QueueError::Invalid(_))
        ));
    }
}
