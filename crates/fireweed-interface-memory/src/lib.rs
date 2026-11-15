//! In-memory implementation of the Fireweed data-plane interface (API-006).
//!
//! `Process` durability, `Immediate` visibility, `SingleProcess` sharing.
//! Shapes: queues and tables. Not yet implemented (reported through
//! `capabilities()` and rejected by `apply_schema` with
//! `FeatureUnsupported`): event logs, blobs, queue history, cohorts and
//! group batching.

mod exec;
mod reads;
mod state;
mod validate;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_interface::*;

use crate::exec::*;
use crate::state::*;

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

    /// Runs `f` on the space with the request time fixed and due
    /// transitions applied.
    fn with_space<T>(
        &self,
        key: &SpaceKey,
        f: impl FnOnce(&mut Space, i64, &ImplementationLimits) -> DpResult<T>,
    ) -> DpResult<T> {
        let mut spaces = self.spaces.lock().expect("lock");
        let space = spaces.get_mut(key).ok_or(DataPlaneError::SpaceNotFound)?;
        let t = self.clock.now().0.max(space.last_time);
        space.last_time = t;
        settle(space, t);
        f(space, t, &self.limits)
    }
}

fn check_durability(min: Option<DurabilityLevel>) -> DpResult<()> {
    match min {
        Some(l) if l > DurabilityLevel::Process => Err(DataPlaneError::InvalidRequest(
            "min_durability above this implementation's maximum (Process)".into(),
        )),
        _ => Ok(()),
    }
}

/// Looks up a request record. `Some(Ok(_))` replays it.
fn replay(space: &Space, rid: &RequestId, fp: &str) -> Option<DpResult<RecordedResponse>> {
    let rec = space.records.get(rid)?;
    if rec.fingerprint != fp {
        return Some(Err(DataPlaneError::RequestIdConflict));
    }
    Some(Ok(rec.response.clone()))
}

fn record(space: &mut Space, rid: &RequestId, fp: String, t: i64, response: RecordedResponse) {
    space.records.insert(
        rid.clone(),
        Record {
            fingerprint: fp,
            recorded_at: t,
            response,
        },
    );
}

fn queue_of<'a>(space: &'a Space, q: &ObjectName) -> DpResult<&'a Queue> {
    space
        .queues
        .get(q)
        .ok_or_else(|| DataPlaneError::ObjectNotFound(q.clone()))
}

macro_rules! recorded {
    ($resp:expr, $variant:ident) => {
        match $resp {
            RecordedResponse::$variant(x) => x,
            _ => return Err(DataPlaneError::RequestIdConflict),
        }
    };
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
        Self::ready((|| {
            check_durability(r.min_durability)?;
            validate::schema(&r.schema, &self.limits, &self.capabilities())?;
            let mut spaces = self.spaces.lock().expect("lock");
            let durability = DurabilityLevel::Process;
            match spaces.get_mut(&r.space) {
                None => {
                    if r.expected_version.is_some() {
                        return Err(DataPlaneError::SchemaConflict {
                            current_version: None,
                        });
                    }
                    let mut space = Space {
                        schema: r.schema.clone(),
                        queues: BTreeMap::new(),
                        tables: BTreeMap::new(),
                        records: BTreeMap::new(),
                        next_item_id: 0,
                        next_token: 0,
                        last_time: self.clock.now().0,
                    };
                    install(&mut space, &r.schema);
                    spaces.insert(r.space.clone(), space);
                    Ok(ApplySchemaResponse {
                        version: r.schema.version,
                        changed: true,
                        durability,
                    })
                }
                Some(space) => {
                    let current = space.schema.version;
                    if space.schema == r.schema {
                        return Ok(ApplySchemaResponse {
                            version: current,
                            changed: false,
                            durability,
                        });
                    }
                    if r.expected_version != Some(current) || r.schema.version <= current {
                        return Err(DataPlaneError::SchemaConflict {
                            current_version: Some(current),
                        });
                    }
                    validate::additive(&space.schema, &r.schema)?;
                    space.schema = r.schema.clone();
                    install(space, &r.schema);
                    Ok(ApplySchemaResponse {
                        version: r.schema.version,
                        changed: true,
                        durability,
                    })
                }
            }
        })())
    }

    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>> {
        let spaces = self.spaces.lock().expect("lock");
        let r = spaces.get(&space).map(|s| SchemaState {
            schema: s.schema.clone(),
            indexes: s
                .schema
                .queues
                .iter()
                .flat_map(|q| q.fields.iter().map(move |f| (q.name.clone(), f)))
                .chain(
                    s.schema
                        .tables
                        .iter()
                        .flat_map(|t| t.fields.iter().map(move |f| (t.name.clone(), f))),
                )
                .filter(|(_, f)| f.index != IndexHint::None)
                .map(|(o, f)| IndexState {
                    object: o,
                    field: f.name.clone(),
                    ready: true,
                })
                .collect(),
        });
        Self::ready(Ok(r))
    }

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                validate::enqueue_items(space, &r.queue, &r.items, limits)?;
                if let Some(rep) = replay(space, &r.header.request_id, &fp) {
                    return Ok(recorded!(rep?, Enqueue));
                }
                let mut cx = Cx {
                    t,
                    limits,
                    params: &Params::new(),
                    space_key: &r.header.space,
                    bumped: BTreeSet::new(),
                };
                let mut next = space.next_item_id;
                let q = space.queues.get_mut(&r.queue).expect("validated");
                let entries = r
                    .items
                    .iter()
                    .map(
                        |it| match enqueue_item(&mut next, q, it, r.on_key_conflict, &mut cx) {
                            Ok(o) => o.result,
                            Err(s) => EntryResult::status(s),
                        },
                    )
                    .collect();
                space.next_item_id = next;
                let resp = EnqueueResponse {
                    entries,
                    durability: DurabilityLevel::Process,
                };
                record(
                    space,
                    &r.header.request_id,
                    fp,
                    t,
                    RecordedResponse::Enqueue(resp.clone()),
                );
                Ok(resp)
            })
        })())
    }

    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                let q = queue_of(space, &r.queue)?;
                validate::claim(q, &r, limits)?;
                if let Some(rep) = replay(space, &r.header.request_id, &fp) {
                    let c = recorded!(rep?, Claim);
                    let active = c.items.iter().any(|ci| {
                        q.items.get(&ci.item_id.0).is_some_and(|it| {
                            it.state == St::Leased
                                && it.lease.as_ref().map(|l| l.token.as_str())
                                    == ci.lease_token.as_ref().map(|x| x.as_str())
                        })
                    });
                    if !c.items.is_empty() && !active {
                        return Err(DataPlaneError::RequestExpired);
                    }
                    return Ok(c);
                }
                let mut resp = ClaimResponse {
                    items: vec![],
                    cohort: None,
                    queue_paused: q.paused,
                    scan_limited: false,
                    as_of: UtcMillis(t),
                    durability: DurabilityLevel::Process,
                };
                if !q.paused {
                    let elig_t = r.eligibility_time.map(|e| e.0).unwrap_or(t);
                    let (ids, limited) = reads::claim_candidates(q, &r, t, elig_t, limits);
                    resp.scan_limited = limited;
                    let mut cx = Cx {
                        t,
                        limits,
                        params: &r.params,
                        space_key: &r.header.space,
                        bumped: BTreeSet::new(),
                    };
                    let mut tok = space.next_token;
                    let qm = space.queues.get_mut(&r.queue).expect("queue");
                    for id in ids {
                        resp.items.push(lease_item(
                            &mut tok,
                            qm,
                            id,
                            &r.worker_id,
                            r.lease_duration_ms,
                            &mut cx,
                        ));
                    }
                    space.next_token = tok;
                }
                record(
                    space,
                    &r.header.request_id,
                    fp,
                    t,
                    RecordedResponse::Claim(resp.clone()),
                );
                Ok(resp)
            })
        })())
    }

    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                queue_of(space, &r.queue)?;
                if r.lease_duration_ms == 0 || r.lease_duration_ms > limits.max_lease_duration_ms {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
                }
                if r.item_ids.len() > limits.max_claim_batch_size as usize {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::ClaimBatch));
                }
                if let Some(rep) = replay(space, &r.header.request_id, &fp) {
                    return Ok(recorded!(rep?, ClaimByItemIds));
                }
                let elig_t = r.eligibility_time.map(|e| e.0).unwrap_or(t);
                let mut cx = Cx {
                    t,
                    limits,
                    params: &Params::new(),
                    space_key: &r.header.space,
                    bumped: BTreeSet::new(),
                };
                let mut tok = space.next_token;
                let q = space.queues.get_mut(&r.queue).expect("queue");
                let mut seen = BTreeSet::new();
                let mut results = vec![];
                for id in &r.item_ids {
                    if !seen.insert(*id) {
                        continue;
                    }
                    let res = match q.items.get(&id.0) {
                        None => Err(EntryStatus::NotFound),
                        Some(it) => match it.state {
                            St::Leased => Err(EntryStatus::Leased),
                            St::Complete | St::Failed | St::Tombstone => Err(EntryStatus::Terminal),
                            St::Superseded => Err(EntryStatus::NotFound),
                            St::Pending if q.paused || !eligible(q, it, elig_t) => {
                                Err(EntryStatus::NotEligible)
                            }
                            St::Pending => Ok(()),
                        },
                    };
                    results.push((
                        *id,
                        res.map(|()| {
                            lease_item(
                                &mut tok,
                                q,
                                id.0,
                                &r.worker_id,
                                r.lease_duration_ms,
                                &mut cx,
                            )
                        }),
                    ));
                }
                space.next_token = tok;
                let resp = ClaimByItemIdsResponse {
                    results,
                    durability: DurabilityLevel::Process,
                };
                record(
                    space,
                    &r.header.request_id,
                    fp,
                    t,
                    RecordedResponse::ClaimByItemIds(resp.clone()),
                );
                Ok(resp)
            })
        })())
    }

    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                queue_of(space, &r.queue)?;
                if r.lease_duration_ms == 0 || r.lease_duration_ms > limits.max_lease_duration_ms {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
                }
                if r.leases.len() > limits.max_entries_per_commit as usize {
                    return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
                }
                if let Some(rep) = replay(space, &r.header.request_id, &fp) {
                    return Ok(recorded!(rep?, Renew));
                }
                let mut cx = Cx {
                    t,
                    limits,
                    params: &Params::new(),
                    space_key: &r.header.space,
                    bumped: BTreeSet::new(),
                };
                let q = space.queues.get_mut(&r.queue).expect("queue");
                let entries = r
                    .leases
                    .iter()
                    .map(|l| renew_one(q, l, r.lease_duration_ms, &mut cx))
                    .collect();
                let resp = RenewResponse {
                    entries,
                    durability: DurabilityLevel::Process,
                };
                record(
                    space,
                    &r.header.request_id,
                    fp,
                    t,
                    RecordedResponse::Renew(resp.clone()),
                );
                Ok(resp)
            })
        })())
    }

    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                let q = queue_of(space, &r.queue)?;
                validate::finalize_entries(q, &r.entries, &r.params, limits)?;
                if let Some(rep) = replay(space, &r.header.request_id, &fp) {
                    return Ok(recorded!(rep?, Finalize));
                }
                let mut cx = Cx {
                    t,
                    limits,
                    params: &r.params,
                    space_key: &r.header.space,
                    bumped: BTreeSet::new(),
                };
                let q = space.queues.get_mut(&r.queue).expect("queue");
                let entries = r
                    .entries
                    .iter()
                    .map(|e| finalize_one(q, e, &mut cx))
                    .collect();
                let resp = FinalizeResponse {
                    entries,
                    durability: DurabilityLevel::Process,
                };
                record(
                    space,
                    &r.header.request_id,
                    fp,
                    t,
                    RecordedResponse::Finalize(resp.clone()),
                );
                Ok(resp)
            })
        })())
    }

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse> {
        Self::ready((|| {
            check_durability(r.header.min_durability)?;
            let fp = fingerprint(&r);
            self.with_space(&r.header.space, |space, t, limits| {
                validate::commit(space, &r, limits)?;
                if !r.dry_run
                    && let Some(rep) = replay(space, &r.header.request_id, &fp)
                {
                    return Ok(recorded!(rep?, Commit));
                }
                let mut work = space.clone();
                let mut cx = Cx {
                    t,
                    limits,
                    params: &r.params,
                    space_key: &r.header.space,
                    bumped: BTreeSet::new(),
                };
                let mut ops: Vec<OpResult> = Vec::with_capacity(r.ops.len());
                let mut failure = None;
                let mut continuation = None;
                for (i, op) in r.ops.iter().enumerate() {
                    match run_op(&mut work, op, &mut cx) {
                        Ok((res, cont)) => {
                            ops.push(res);
                            if cont.is_some() {
                                continuation = cont;
                            }
                        }
                        Err((res, reason, entry)) => {
                            ops.push(res);
                            failure = Some(OpFailure {
                                op_index: i as u32,
                                entry_index: entry,
                                reason,
                            });
                            break;
                        }
                    }
                }
                while ops.len() < r.ops.len() {
                    ops.push(OpResult::default());
                }
                let status = if r.dry_run {
                    CommitStatus::DryRun
                } else if failure.is_some() {
                    CommitStatus::Rejected
                } else {
                    CommitStatus::Committed
                };
                if r.dry_run {
                    for o in &mut ops {
                        o.matched = o.matched.min(limits.max_dry_run_count);
                    }
                }
                let resp = CommitResponse {
                    status,
                    as_of: UtcMillis(t),
                    durability: if r.dry_run {
                        None
                    } else {
                        Some(DurabilityLevel::Process)
                    },
                    ops,
                    failure,
                    continuation: if status == CommitStatus::Committed {
                        continuation
                    } else {
                        None
                    },
                };
                if status == CommitStatus::Committed {
                    *space = work;
                }
                if !r.dry_run {
                    record(
                        space,
                        &r.header.request_id,
                        fp,
                        t,
                        RecordedResponse::Commit(resp.clone()),
                    );
                }
                Ok(resp)
            })
        })())
    }

    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>> {
        Self::ready(self.with_space(&r.space, |space, _, _| {
            Ok(space.records.get(&r.request_id).map(|rec| {
                let mut response = rec.response.clone();
                redact_tokens(&mut response);
                RequestRecord {
                    request_id: r.request_id.clone(),
                    fingerprint: rec.fingerprint.clone(),
                    recorded_at: UtcMillis(rec.recorded_at),
                    response,
                }
            }))
        }))
    }

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse> {
        Self::ready(self.with_space(&r.space, |space, t, _| reads::get(space, &r, t)))
    }

    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse> {
        Self::ready(self.with_space(&r.space, |space, t, limits| {
            reads::read(space, &r, t, limits)
        }))
    }

    fn read_log(&self, _r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::EventLog,
        ))))
    }

    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics> {
        Self::ready(self.with_space(&r.space, |space, t, _| {
            Ok(reads::metrics(queue_of(space, &r.queue)?, t))
        }))
    }

    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse> {
        Self::ready(self.with_space(&r.space, |space, t, _| Ok(reads::discover(space, &r, t))))
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

fn install(space: &mut Space, schema: &SpaceSchema) {
    for qd in &schema.queues {
        space
            .queues
            .entry(qd.name.clone())
            .and_modify(|q| q.decl = qd.clone())
            .or_insert_with(|| Queue {
                decl: qd.clone(),
                items: BTreeMap::new(),
                keys: BTreeMap::new(),
                paused: false,
                drain_intake: false,
                blocked: BTreeMap::new(),
                gate_epoch: 0,
            });
    }
    for td in &schema.tables {
        space
            .tables
            .entry(td.name.clone())
            .and_modify(|t| t.decl = td.clone())
            .or_insert_with(|| Table {
                decl: td.clone(),
                rows: BTreeMap::new(),
                hwm: BTreeMap::new(),
            });
    }
}

type OpErr = (OpResult, FailureReason, Option<u32>);

fn entry_rejection(entries: &[EntryResult]) -> Option<(FailureReason, u32)> {
    entries
        .iter()
        .enumerate()
        .find(|(_, e)| e.status.rejects_group())
        .map(|(i, e)| (FailureReason::Entry(e.status.clone()), i as u32))
}

fn run_op(
    space: &mut Space,
    op: &WriteOp,
    cx: &mut Cx<'_>,
) -> Result<(OpResult, Option<Continuation>), OpErr> {
    let mut res = OpResult {
        evaluated: true,
        ..Default::default()
    };
    let finish = |res: OpResult| -> Result<(OpResult, Option<Continuation>), OpErr> {
        match entry_rejection(&res.entries) {
            Some((reason, i)) => Err((res, reason, Some(i))),
            None => Ok((res, None)),
        }
    };
    match op {
        WriteOp::Enqueue {
            queue,
            items,
            on_key_conflict,
        } => {
            let mut next = space.next_item_id;
            let q = space.queues.get_mut(queue).expect("validated");
            for it in items {
                let e = match enqueue_item(&mut next, q, it, *on_key_conflict, cx) {
                    Ok(o) => o.result,
                    Err(s) => EntryResult::status(s),
                };
                let stop = e.status.rejects_group();
                res.entries.push(e);
                if stop {
                    break;
                }
            }
            space.next_item_id = next;
            finish(res)
        }
        WriteOp::Finalize { queue, entries } => {
            let q = space.queues.get_mut(queue).expect("validated");
            for e in entries {
                let r = finalize_one(q, e, cx);
                let stop = r.status.rejects_group();
                res.entries.push(r);
                if stop {
                    break;
                }
            }
            match entry_rejection(&res.entries) {
                Some((FailureReason::Entry(EntryStatus::EvaluationFault(x)), i)) => {
                    Err((res, FailureReason::Evaluation(x), Some(i)))
                }
                Some((reason, i)) => Err((res, reason, Some(i))),
                None => Ok((res, None)),
            }
        }
        WriteOp::Renew {
            queue,
            leases,
            lease_duration_ms,
        } => {
            let q = space.queues.get_mut(queue).expect("validated");
            for l in leases {
                let e = renew_one(q, l, *lease_duration_ms, cx);
                let stop = e.status.rejects_group();
                res.entries.push(e);
                if stop {
                    break;
                }
            }
            finish(res)
        }
        WriteOp::Reassign {
            queue,
            leases,
            to_worker,
            lease_duration_ms,
        } => {
            let mut tok = space.next_token;
            let q = space.queues.get_mut(queue).expect("validated");
            for l in leases {
                let e = reassign_one(&mut tok, q, l, to_worker, *lease_duration_ms, cx);
                let stop = e.status.rejects_group();
                res.entries.push(e);
                if stop {
                    break;
                }
            }
            space.next_token = tok;
            finish(res)
        }
        WriteOp::SetGates { queue, gates } => {
            let q = space.queues.get_mut(queue).expect("validated");
            for (g, state) in gates {
                match state {
                    GateState::Blocked => {
                        q.gate_epoch += 1;
                        q.blocked.insert(g.clone(), q.gate_epoch);
                    }
                    _ => {
                        q.blocked.remove(g);
                    }
                }
                res.affected += 1;
            }
            Ok((res, None))
        }
        WriteOp::SetQueueState {
            queue,
            paused,
            drain_intake,
        } => {
            let q = space.queues.get_mut(queue).expect("validated");
            if let Some(p) = paused {
                q.paused = *p;
            }
            if let Some(d) = drain_intake {
                q.drain_intake = *d;
            }
            res.affected = 1;
            Ok((res, None))
        }
        WriteOp::PutRows {
            table,
            rows,
            on_conflict,
        } => {
            let tb = space.tables.get_mut(table).expect("validated");
            for row in rows {
                let e = match put_row(tb, row, *on_conflict, cx) {
                    Ok(e) => e,
                    Err(s) => EntryResult::status(s),
                };
                let stop = e.status.rejects_group();
                res.entries.push(e);
                if stop {
                    break;
                }
            }
            res.affected = res
                .entries
                .iter()
                .filter(|e| e.status != EntryStatus::Skipped)
                .count() as u64;
            finish(res)
        }
        WriteOp::Mutate(m) => match exec_mutate(space, m, cx) {
            Ok((res, cont)) => Ok((res, cont)),
            Err(MutateFail::Reason(reason, entry)) => Err((res, reason, entry)),
        },
        _ => Err((
            res,
            FailureReason::Entry(EntryStatus::Invalid {
                reason: "operation".into(),
            }),
            None,
        )),
    }
}
