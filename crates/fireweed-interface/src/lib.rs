//! Fireweed data-plane interface (API-006).
//!
//! This crate is the normative definition of every type API-006 names. It
//! depends on no `fireweed-*` crate. `expr` is the reference semantics of
//! the expression language; every implementation must produce the same
//! results, and `fireweed-interface-conformance` checks that they do.

#![allow(clippy::large_enum_variant)]

pub mod caps;
pub mod error;
pub mod expr;
pub mod ids;
pub mod request;
pub mod schema;
pub mod value;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub use caps::*;
pub use error::*;
pub use expr::*;
pub use ids::*;
pub use request::*;
pub use schema::*;
pub use value::*;

pub type DpFuture<'a, T> = Pin<Box<dyn Future<Output = DpResult<T>> + Send + 'a>>;

/// The implementation boundary. Dyn-compatible; `Arc<dyn DataPlane>` is
/// `Send + Sync`.
pub trait DataPlane: Send + Sync {
    fn guarantees(&self) -> Guarantees;
    fn capabilities(&self) -> Capabilities;

    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse>;
    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>>;

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse>;
    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse>;
    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse>;
    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse>;
    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse>;

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse>;
    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>>;

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse>;
    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse>;
    fn read_log(&self, r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse>;
    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics>;
    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse>;

    fn put_blob(&self, r: PutBlobRequest) -> DpFuture<'_, PutBlobResponse>;
    fn get_blob(&self, r: GetBlobRequest) -> DpFuture<'_, Bytes>;
}

/// The time authority. It never moves backward (API-006 §Time rule 1).
pub trait Clock: Send + Sync {
    fn now(&self) -> UtcMillis;
}

/// A manual clock for tests: it only moves forward.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
    pub fn advance(&self, ms: i64) {
        assert!(ms >= 0, "ManualClock only moves forward");
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
    pub fn set(&self, ms: i64) {
        self.0.fetch_max(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> UtcMillis {
        UtcMillis(self.0.load(Ordering::SeqCst))
    }
}

fn strip_min_durability(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            map.remove("min_durability");
            for x in map.values_mut() {
                strip_min_durability(x);
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(strip_min_durability),
        _ => {}
    }
}

/// The request fingerprint (API-006 §Request records rule 2): hex SHA-256
/// of the canonical JSON serialization with `min_durability` removed.
pub fn fingerprint<T: Serialize>(request: &T) -> String {
    let mut v = serde_json::to_value(request).expect("requests serialize");
    strip_min_durability(&mut v);
    let canonical = serde_json::to_vec(&v).expect("serialize");
    to_hex(&Sha256::digest(&canonical))
}

const REDACTED: &str = "<redacted>";

/// Removes every lease token from a recorded response (API-006 §Request
/// records rule 5).
pub fn redact_tokens(r: &mut RecordedResponse) {
    fn lease(l: &mut Option<NewLease>) {
        if let Some(l) = l {
            l.lease_token = LeaseToken::new(REDACTED);
        }
    }
    fn entries(es: &mut [EntryResult]) {
        es.iter_mut().for_each(|e| lease(&mut e.lease));
    }
    fn claimed(c: &mut ClaimedItem) {
        if c.lease_token.is_some() {
            c.lease_token = Some(LeaseToken::new(REDACTED));
        }
    }
    match r {
        RecordedResponse::Enqueue(x) => entries(&mut x.entries),
        RecordedResponse::Claim(x) => {
            x.items.iter_mut().for_each(claimed);
            if let Some(c) = &mut x.cohort {
                c.cohort_lease_token = LeaseToken::new(REDACTED);
            }
        }
        RecordedResponse::ClaimByItemIds(x) => {
            for (_, res) in &mut x.results {
                if let Ok(c) = res {
                    claimed(c);
                }
            }
        }
        RecordedResponse::Renew(x) => entries(&mut x.entries),
        RecordedResponse::Finalize(x) => entries(&mut x.entries),
        RecordedResponse::Commit(x) => x.ops.iter_mut().for_each(|o| entries(&mut o.entries)),
    }
}

/// Content address of blob bytes.
pub fn blob_ref(bytes: &[u8]) -> BlobRef {
    BlobRef {
        sha256: Sha256::digest(bytes).into(),
        size: bytes.len() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_dyn(_: &dyn DataPlane) {}

    #[test]
    fn data_plane_is_dyn_compatible() {
        let _f: fn(&dyn DataPlane) = assert_dyn;
        fn send_sync<T: Send + Sync + ?Sized>() {}
        send_sync::<dyn DataPlane>();
    }

    #[test]
    fn fingerprint_ignores_min_durability() {
        let mut h = WriteHeader::new(SpaceKey::new("t", "s"), "r1");
        let a = fingerprint(&h);
        h.min_durability = Some(DurabilityLevel::Replicated);
        assert_eq!(a, fingerprint(&h));
        h.actor = Some("x".into());
        assert_ne!(a, fingerprint(&h));
    }
}
