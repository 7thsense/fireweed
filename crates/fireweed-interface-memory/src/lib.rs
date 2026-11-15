//! In-memory implementation of the Fireweed data-plane interface (API-006).
//!
//! `Process` durability, `Immediate` visibility, `SingleProcess` sharing.
//! Shapes: queues and tables. Event logs, blobs, queue history, cohorts and
//! group batching are reported unsupported (`capabilities()`, and
//! `apply_schema` fails with `FeatureUnsupported`).
//!
//! The request logic is `fireweed-interface-engine`, shared with the
//! PostgreSQL implementation. This crate keeps every space in memory and
//! runs each request on it under one lock.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_interface::*;
use fireweed_interface_engine::Space;
use fireweed_interface_engine::handlers::{self, Profile};

const PROFILE: Profile = Profile {
    durability: DurabilityLevel::Process,
    max_durability: DurabilityLevel::Process,
};

pub struct MemoryDataPlane {
    clock: Arc<dyn Clock>,
    limits: ImplementationLimits,
    spaces: Mutex<BTreeMap<SpaceKey, Space>>,
}

pub fn default_limits() -> ImplementationLimits {
    ImplementationLimits {
        max_ops_per_commit: 64,
        max_entries_per_commit: 10_000,
        max_request_bytes: 8 << 20,
        max_in_list: 1000,
        max_writer_scan_rows: 50_000,
        max_read_scan_rows: 5_000_000,
        max_cohort_size: 0,
        max_selector_affected: 10_000,
        max_body_bytes: 2 << 20,
        max_value_bytes: 256 << 10,
        max_set_values: 128,
        max_blob_bytes: 0,
        max_dry_run_count: 100_000,
        max_read_rows: 1000,
        max_claim_batch_size: 1000,
        max_lease_duration_ms: 3_600_000,
        max_request_id_retention_ms: 7 * 86_400_000,
        max_objects_per_space: 256,
    }
}

impl MemoryDataPlane {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self::with_limits(clock, default_limits())
    }

    pub fn with_limits(clock: Arc<dyn Clock>, limits: ImplementationLimits) -> Self {
        Self {
            clock,
            limits,
            spaces: Mutex::new(BTreeMap::new()),
        }
    }

    fn ready<T: Send + 'static>(r: DpResult<T>) -> DpFuture<'static, T> {
        Box::pin(std::future::ready(r))
    }

    /// Runs a write on the stored space.
    fn write<T>(
        &self,
        key: &SpaceKey,
        f: impl FnOnce(&mut Space, i64) -> DpResult<T>,
    ) -> DpResult<T> {
        let mut spaces = self.spaces.lock().unwrap_or_else(|p| p.into_inner());
        let space = spaces.get_mut(key).ok_or(DataPlaneError::SpaceNotFound)?;
        let t = handlers::begin(space, self.clock.now().0);
        f(space, t)
    }

    /// Runs a read on a copy with due transitions applied, so reads never
    /// change stored state. Request time is fixed under the same lock as the
    /// snapshot, and recorded, so no later write gets an earlier time.
    fn read_only<T>(
        &self,
        key: &SpaceKey,
        f: impl FnOnce(&Space, i64) -> DpResult<T>,
    ) -> DpResult<T> {
        let mut spaces = self.spaces.lock().unwrap_or_else(|p| p.into_inner());
        let stored = spaces.get_mut(key).ok_or(DataPlaneError::SpaceNotFound)?;
        let t = self.clock.now().0.max(stored.last_time);
        stored.last_time = t;
        let mut space = stored.clone();
        drop(spaces);
        handlers::begin(&mut space, t);
        f(&space, t)
    }
}

impl DataPlane for MemoryDataPlane {
    fn guarantees(&self) -> Guarantees {
        Guarantees {
            durability: DurabilityLevel::Process,
            new_work_visibility: Visibility::Immediate,
            sharing: Sharing::SingleProcess,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            max_durability: DurabilityLevel::Process,
            shapes: [ShapeKind::Queue, ShapeKind::Table].into_iter().collect(),
            queue_features: [
                QueueFeature::DynamicGates,
                QueueFeature::MetadataBlockers,
                QueueFeature::Recurrence,
            ]
            .into_iter()
            .collect(),
            queue_history: false,
            priority_kinds: [PriorityKind::Int64, PriorityKind::Timestamp]
                .into_iter()
                .collect(),
            ordering_modes: [OrderingMode::Strict].into_iter().collect(),
            limits: self.limits,
        }
    }

    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse> {
        let caps = self.capabilities();
        let mut spaces = self.spaces.lock().unwrap_or_else(|p| p.into_inner());
        let now = self.clock.now().0;
        let res = match spaces.get_mut(&r.space) {
            Some(space) => {
                handlers::apply_schema(Some(space), &r, &caps, &PROFILE, now).map(|(resp, _)| resp)
            }
            None => handlers::apply_schema(None, &r, &caps, &PROFILE, now).map(|(resp, new)| {
                if let Some(space) = new {
                    spaces.insert(r.space.clone(), space);
                }
                resp
            }),
        };
        Self::ready(res)
    }

    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>> {
        let spaces = self.spaces.lock().unwrap_or_else(|p| p.into_inner());
        Self::ready(Ok(spaces.get(&space).map(handlers::schema_state)))
    }

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::enqueue(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::claim(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::claim_by_item_ids(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::renew(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::finalize(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse> {
        Self::ready(self.write(&r.header.space, |sp, t| {
            handlers::commit(sp, t, &self.limits, &PROFILE, &r)
        }))
    }

    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>> {
        Self::ready(self.read_only(&r.space, |sp, _| Ok(handlers::get_request(sp, &r))))
    }

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse> {
        Self::ready(self.read_only(&r.space, |sp, t| handlers::get(sp, t, &self.limits, &r)))
    }

    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse> {
        Self::ready(self.read_only(&r.space, |sp, t| handlers::read(sp, t, &self.limits, &r)))
    }

    fn read_log(&self, _r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::EventLog,
        ))))
    }

    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics> {
        Self::ready(self.read_only(&r.space, |sp, t| handlers::metrics(sp, t, &r)))
    }

    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse> {
        Self::ready(self.read_only(&r.space, |sp, t| Ok(handlers::discover(sp, t, &r))))
    }

    fn put_blob(&self, _r: PutBlobRequest) -> DpFuture<'_, PutBlobResponse> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::Blob,
        ))))
    }

    fn get_blob(&self, _r: GetBlobRequest) -> DpFuture<'_, Bytes> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::Blob,
        ))))
    }
}
