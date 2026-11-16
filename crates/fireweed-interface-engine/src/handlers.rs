//! Request handlers over a loaded `Space`. Each takes the request time
//! fixed by [`begin`] and returns the response; the caller persists the
//! changed space.

use std::collections::{BTreeMap, BTreeSet};

use fireweed_interface::*;

use crate::exec::*;
use crate::reads;
use crate::state::*;
use crate::validate;

/// The durability levels an implementation acknowledges at.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    /// Ascending, non-empty: the levels a commit can be acknowledged at.
    pub offered: &'static [DurabilityLevel],
    /// The level used when a request names none.
    pub default_durability: DurabilityLevel,
}

impl Profile {
    pub fn min(&self) -> DurabilityLevel {
        self.offered[0]
    }

    pub fn max(&self) -> DurabilityLevel {
        self.offered[self.offered.len() - 1]
    }

    /// The level a commit is acknowledged at: the lowest offered level at or
    /// above the request's (or the default). A level above the maximum is
    /// refused by `check_durability` before this is used.
    pub fn level(&self, requested: Option<DurabilityLevel>) -> DurabilityLevel {
        let want = requested.unwrap_or(self.default_durability);
        self.offered
            .iter()
            .copied()
            .find(|l| *l >= want)
            .unwrap_or(self.max())
    }
}

pub fn check_durability(p: &Profile, min: Option<DurabilityLevel>) -> DpResult<()> {
    match min {
        Some(l) if l > p.max() => Err(DataPlaneError::InvalidRequest(format!(
            "min_durability {l:?} above this implementation's maximum ({:?})",
            p.max()
        ))),
        _ => Ok(()),
    }
}

/// Fixes the request time (non-decreasing per space) and applies due
/// time-driven transitions.
pub fn begin(space: &mut Space, now: i64) -> i64 {
    let t = now.max(space.last_time);
    space.last_time = t;
    settle(space, t);
    t
}

/// `max_request_bytes` on the serialized request (API-006 §Work limits).
fn request_bytes_ok<T: serde::Serialize>(r: &T, limits: &ImplementationLimits) -> DpResult<()> {
    let n = serde_json::to_vec(r)
        .map(|v| v.len() as u64)
        .unwrap_or(u64::MAX);
    if n > limits.max_request_bytes {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::RequestBytes));
    }
    Ok(())
}

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

fn cx<'a>(
    t: i64,
    limits: &'a ImplementationLimits,
    params: &'a Params,
    space: &'a SpaceKey,
) -> Cx<'a> {
    Cx {
        t,
        limits,
        params,
        space_key: space,
        bumped: BTreeSet::new(),
        bumped_rows: BTreeSet::new(),
    }
}

pub fn new_space(schema: &SpaceSchema, now: i64) -> Space {
    let mut space = Space {
        schema: schema.clone(),
        queues: BTreeMap::new(),
        tables: BTreeMap::new(),
        records: BTreeMap::new(),
        next_item_id: 0,
        next_token: 0,
        last_time: now,
    };
    install(&mut space, schema);
    space
}

/// Creates or updates the schema. `existing` is the stored space, if any.
/// Returns the response and, for a new space, the space to store.
pub fn apply_schema(
    existing: Option<&mut Space>,
    r: &ApplySchemaRequest,
    caps: &Capabilities,
    p: &Profile,
    now: i64,
) -> DpResult<(ApplySchemaResponse, Option<Space>)> {
    check_durability(p, r.min_durability)?;
    validate::schema(&r.schema, &caps.limits, caps)?;
    let durability = p.level(r.min_durability);
    match existing {
        None => {
            if r.expected_version.is_some() {
                return Err(DataPlaneError::SchemaConflict {
                    current_version: None,
                });
            }
            let resp = ApplySchemaResponse {
                version: r.schema.version,
                changed: true,
                durability,
            };
            Ok((resp, Some(new_space(&r.schema, now))))
        }
        Some(space) => {
            // Expire what the old policy has already expired, so a raised
            // retention cannot bring it back.
            begin(space, now);
            let current = space.schema.version;
            if space.schema == r.schema {
                let resp = ApplySchemaResponse {
                    version: current,
                    changed: false,
                    durability,
                };
                return Ok((resp, None));
            }
            if r.expected_version != Some(current) || r.schema.version <= current {
                return Err(DataPlaneError::SchemaConflict {
                    current_version: Some(current),
                });
            }
            validate::additive(&space.schema, &r.schema)?;
            space.schema = r.schema.clone();
            install(space, &r.schema);
            let resp = ApplySchemaResponse {
                version: r.schema.version,
                changed: true,
                durability,
            };
            Ok((resp, None))
        }
    }
}

pub fn schema_state(s: &Space) -> SchemaState {
    SchemaState {
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
    }
}

/// Adds queues and tables declared in `schema` that the space lacks, and
/// refreshes declarations of those it has.
pub fn install(space: &mut Space, schema: &SpaceSchema) {
    for qd in &schema.queues {
        space
            .queues
            .entry(qd.name.clone())
            .and_modify(|q| q.decl = qd.clone())
            .or_insert_with(|| empty_queue(qd));
    }
    for td in &schema.tables {
        space
            .tables
            .entry(td.name.clone())
            .and_modify(|t| t.decl = td.clone())
            .or_insert_with(|| empty_table(td));
    }
}

pub fn empty_queue(decl: &QueueDecl) -> Queue {
    Queue {
        decl: decl.clone(),
        items: BTreeMap::new(),
        keys: BTreeMap::new(),
        paused: false,
        drain_intake: false,
        blocked: BTreeMap::new(),
        gate_epoch: 0,
    }
}

pub fn empty_table(decl: &TableDecl) -> Table {
    Table {
        decl: decl.clone(),
        rows: BTreeMap::new(),
        hwm: BTreeMap::new(),
        next_seq: 0,
    }
}

/// Rebuilds a queue's key index from its items.
pub fn rebuild_keys(q: &mut Queue) {
    q.keys = q
        .items
        .values()
        .filter(|it| it.keyed && it.state != St::Superseded)
        .map(|it| (it.key.clone(), it.id))
        .collect();
}

pub fn enqueue(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &EnqueueRequest,
) -> DpResult<EnqueueResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    if let Some(rep) = replay(space, &r.header.request_id, &fp) {
        return Ok(recorded!(rep?, Enqueue));
    }
    validate::enqueue_items(space, &r.queue, &r.items, limits)?;
    let empty = Params::new();
    let mut cx = cx(t, limits, &empty, &r.header.space);
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
        durability: p.level(r.header.min_durability),
    };
    record(
        space,
        &r.header.request_id,
        fp,
        t,
        RecordedResponse::Enqueue(resp.clone()),
    );
    Ok(resp)
}

pub fn claim(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &ClaimRequest,
) -> DpResult<ClaimResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    let q = queue_of(space, &r.queue)?;
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
    validate::claim(q, r, limits)?;
    let mut resp = ClaimResponse {
        items: vec![],
        cohort: None,
        queue_paused: q.paused,
        scan_limited: false,
        as_of: UtcMillis(t),
        durability: p.level(r.header.min_durability),
    };
    if !q.paused {
        let elig_t = r.eligibility_time.map(|e| e.0).unwrap_or(t);
        let (ids, limited) = reads::claim_candidates(q, r, t, elig_t, limits);
        resp.scan_limited = limited;
        let mut cx = cx(t, limits, &r.params, &r.header.space);
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
}

pub fn claim_by_item_ids(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &ClaimByItemIdsRequest,
) -> DpResult<ClaimByItemIdsResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    let q = queue_of(space, &r.queue)?;
    if let Some(rep) = replay(space, &r.header.request_id, &fp) {
        let c = recorded!(rep?, ClaimByItemIds);
        let claimed: Vec<&ClaimedItem> = c
            .results
            .iter()
            .filter_map(|(_, x)| x.as_ref().ok())
            .collect();
        let active = claimed.iter().any(|ci| {
            q.items.get(&ci.item_id.0).is_some_and(|it| {
                it.state == St::Leased
                    && it.lease.as_ref().map(|l| l.token.as_str())
                        == ci.lease_token.as_ref().map(|x| x.as_str())
            })
        });
        if !claimed.is_empty() && !active {
            return Err(DataPlaneError::RequestExpired);
        }
        return Ok(c);
    }
    if r.lease_duration_ms == 0 || r.lease_duration_ms > limits.max_lease_duration_ms {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
    }
    if r.item_ids.len() > limits.max_claim_batch_size as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::ClaimBatch));
    }
    validate::text_ok(r.worker_id.as_str(), "worker id")?;
    validate::eligibility_time(r.eligibility_time)?;
    let elig_t = r.eligibility_time.map(|e| e.0).unwrap_or(t);
    let empty = Params::new();
    let mut cx = cx(t, limits, &empty, &r.header.space);
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
        durability: p.level(r.header.min_durability),
    };
    record(
        space,
        &r.header.request_id,
        fp,
        t,
        RecordedResponse::ClaimByItemIds(resp.clone()),
    );
    Ok(resp)
}

pub fn renew(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &RenewRequest,
) -> DpResult<RenewResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    queue_of(space, &r.queue)?;
    if let Some(rep) = replay(space, &r.header.request_id, &fp) {
        return Ok(recorded!(rep?, Renew));
    }
    if r.lease_duration_ms == 0 || r.lease_duration_ms > limits.max_lease_duration_ms {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::LeaseDuration));
    }
    if r.leases.len() > limits.max_entries_per_commit as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
    }
    let empty = Params::new();
    let mut cx = cx(t, limits, &empty, &r.header.space);
    let q = space.queues.get_mut(&r.queue).expect("queue");
    let entries = r
        .leases
        .iter()
        .map(|l| renew_one(q, l, r.lease_duration_ms, &mut cx))
        .collect();
    let resp = RenewResponse {
        entries,
        durability: p.level(r.header.min_durability),
    };
    record(
        space,
        &r.header.request_id,
        fp,
        t,
        RecordedResponse::Renew(resp.clone()),
    );
    Ok(resp)
}

pub fn finalize(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &FinalizeRequest,
) -> DpResult<FinalizeResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    let q = queue_of(space, &r.queue)?;
    if let Some(rep) = replay(space, &r.header.request_id, &fp) {
        return Ok(recorded!(rep?, Finalize));
    }
    validate::finalize_entries(q, &r.entries, &r.params, limits)?;
    let mut cx = cx(t, limits, &r.params, &r.header.space);
    let q = space.queues.get_mut(&r.queue).expect("queue");
    let entries = r
        .entries
        .iter()
        .map(|e| finalize_one(q, e, &mut cx))
        .collect();
    let resp = FinalizeResponse {
        entries,
        durability: p.level(r.header.min_durability),
    };
    record(
        space,
        &r.header.request_id,
        fp,
        t,
        RecordedResponse::Finalize(resp.clone()),
    );
    Ok(resp)
}

pub fn commit(
    space: &mut Space,
    t: i64,
    limits: &ImplementationLimits,
    p: &Profile,
    r: &CommitRequest,
) -> DpResult<CommitResponse> {
    check_durability(p, r.header.min_durability)?;
    request_bytes_ok(r, limits)?;
    let fp = fingerprint(r);
    // A request id with a record replays before any state-dependent
    // validation (API-006 §Replay).
    if !r.dry_run
        && let Some(rep) = replay(space, &r.header.request_id, &fp)
    {
        return Ok(recorded!(rep?, Commit));
    }
    validate::commit(space, r, limits)?;
    // Ops change only the objects they name and the id counters, so the
    // working copy holds just those; it replaces them if the group commits.
    let objects = commit_objects(r);
    let mut work = Space {
        schema: space.schema.clone(),
        queues: space
            .queues
            .iter()
            .filter(|(n, _)| objects.contains(*n))
            .map(|(n, q)| (n.clone(), q.clone()))
            .collect(),
        tables: space
            .tables
            .iter()
            .filter(|(n, _)| objects.contains(*n))
            .map(|(n, tb)| (n.clone(), tb.clone()))
            .collect(),
        records: BTreeMap::new(),
        next_item_id: space.next_item_id,
        next_token: space.next_token,
        last_time: space.last_time,
    };
    let mut cx = cx(t, limits, &r.params, &r.header.space);
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
            Some(p.level(r.header.min_durability))
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
        space.queues.extend(work.queues);
        space.tables.extend(work.tables);
        space.next_item_id = work.next_item_id;
        space.next_token = work.next_token;
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
}

pub fn get_request(space: &Space, r: &GetRequestRequest) -> Option<RequestRecord> {
    space.records.get(&r.request_id).map(|rec| {
        let mut response = rec.response.clone();
        redact_tokens(&mut response);
        RequestRecord {
            request_id: r.request_id.clone(),
            fingerprint: rec.fingerprint.clone(),
            recorded_at: UtcMillis(rec.recorded_at),
            response,
        }
    })
}

/// Point reads; at most `max_read_rows` targets per request.
pub fn get(
    space: &Space,
    t: i64,
    limits: &ImplementationLimits,
    r: &GetRequest,
) -> DpResult<GetResponse> {
    if r.targets.len() > limits.max_read_rows as usize {
        return Err(DataPlaneError::BatchTooLarge(LimitKind::Entries));
    }
    reads::get(space, r, t)
}

pub fn read(
    space: &Space,
    t: i64,
    limits: &ImplementationLimits,
    r: &ReadRequest,
) -> DpResult<ReadResponse> {
    reads::read(space, r, t, limits)
}

pub fn metrics(space: &Space, t: i64, r: &MetricsRequest) -> DpResult<QueueMetrics> {
    Ok(reads::metrics(queue_of(space, &r.queue)?, t))
}

pub fn discover(space: &Space, t: i64, r: &DiscoverRequest) -> DiscoverResponse {
    reads::discover(space, r, t)
}

// ------------------------------------------------- objects a request uses

/// Objects a commit references, so a storage-backed implementation can load
/// them before running the request.
pub fn commit_objects(r: &CommitRequest) -> BTreeSet<ObjectName> {
    r.ops
        .iter()
        .filter_map(|op| match op {
            WriteOp::Enqueue { queue, .. }
            | WriteOp::Finalize { queue, .. }
            | WriteOp::Renew { queue, .. }
            | WriteOp::Reassign { queue, .. }
            | WriteOp::SetGates { queue, .. }
            | WriteOp::SetQueueState { queue, .. } => Some(queue.clone()),
            WriteOp::PutRows { table, .. } => Some(table.clone()),
            WriteOp::Mutate(m) => Some(m.target.clone()),
            WriteOp::Assert { target, .. } => Some(target.clone()),
            _ => None,
        })
        .collect()
}

/// Objects a get references.
pub fn get_objects(r: &GetRequest) -> BTreeSet<ObjectName> {
    r.targets
        .iter()
        .filter_map(|t| match t {
            GetTarget::Item { queue, .. } | GetTarget::QueueState { queue } => Some(queue.clone()),
            GetTarget::Row { table, .. } => Some(table.clone()),
            _ => None,
        })
        .collect()
}

/// Objects a read references (the declared source, or the `queue` parameter
/// of a built-in read).
pub fn read_objects(schema: &SpaceSchema, r: &ReadRequest) -> BTreeSet<ObjectName> {
    if r.name.as_str().starts_with("fw_") {
        return match r.params.get(&ParamName::new("queue")) {
            Some(Value::Text(q)) => [ObjectName::new(q.clone())].into_iter().collect(),
            _ => BTreeSet::new(),
        };
    }
    schema
        .reads
        .iter()
        .filter(|d| d.name == r.name && d.version == r.version)
        .map(|d| d.source.clone())
        .collect()
}

// ----------------------------------------------------------- group ops

type OpErr = (OpResult, FailureReason, Option<u32>);

fn entry_rejection(entries: &[EntryResult]) -> Option<(FailureReason, u32)> {
    entries
        .iter()
        .enumerate()
        .find(|(_, e)| e.status.rejects_group())
        .map(|(i, e)| (FailureReason::Entry(e.status.clone()), i as u32))
}

fn finish(res: OpResult) -> Result<(OpResult, Option<Continuation>), OpErr> {
    match entry_rejection(&res.entries) {
        Some((FailureReason::Entry(EntryStatus::EvaluationFault(x)), i)) => {
            Err((res, FailureReason::Evaluation(x), Some(i)))
        }
        Some((reason, i)) => Err((res, reason, Some(i))),
        None => Ok((res, None)),
    }
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
            finish(res)
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
            Err((reason, entry, partial)) => Err((partial, reason, entry)),
        },
        WriteOp::Assert {
            target,
            selection,
            expect,
        } => match exec_assert(space, target, selection, *expect, cx) {
            Ok(res) => Ok((res, None)),
            Err((reason, entry, partial)) => Err((partial, reason, entry)),
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
