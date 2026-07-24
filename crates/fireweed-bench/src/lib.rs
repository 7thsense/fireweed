//! Shared harness primitives for the pqueue performance / e2e suite (TP-002 + data-shape baseline).
//!
//! This library half holds the backend-agnostic pieces the binary (`src/main.rs`) and the e2e correctness
//! test (`tests/e2e_shapes_tests.rs`) both build on:
//!   * the [`Shape`] data-shape model (payload size, field cardinality, grouping, priority distribution)
//!     and its generator, so every workload can be driven over varied DATA SHAPES;
//!   * the generic throughput workloads (`ingest`, `claim_ack`) over the [`fireweed::Fireweed`] facade;
//!   * the fuller [`lifecycle`] workload — a correctness + perf pass that exercises
//!     push → claim → update_fields → ack/nack(retry) → reclaim_expired and asserts the state-machine
//!     invariants at each step (returning `Err` on any violation so a `cargo test` can fail loudly).
//!
//! Everything is driven by `futures::executor::block_on` (NOT tokio) so the sync `postgres` client works.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use fireweed::{
    Bytes, ClaimedItem, EngineError, Fireweed, GroupKey, ItemId, Nack, NewItem, PayloadUpdate,
    PriorityValue, QueueDefinition, UtcTimestamp,
};
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, EligibilityPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use fireweed_engine::{Clock, QueueKey};

/// Historical E0 product-capacity reference: 10,000,000 accepted items/hr. Diagnostic only.
pub const FLOOR_ITEMS_PER_HR: f64 = 10_000_000.0;
/// Historical capacity reference expressed per-second (2,777.78/s).
pub const FLOOR_ITEMS_PER_SEC: f64 = FLOOR_ITEMS_PER_HR / 3600.0;

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// Wall-clock used by every harness handle.
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

fn now_ts() -> UtcTimestamp {
    SystemClock.now()
}

/// `now + ms` as an absolute timestamp (queue-native retry backoff anchor).
fn plus_ms(base: UtcTimestamp, ms: i64) -> UtcTimestamp {
    let total = (base.seconds as i128) * 1_000_000_000
        + base.nanoseconds as i128
        + (ms as i128) * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

// ---------------------------------------------------------------------------
// Data-shape model
// ---------------------------------------------------------------------------

/// How items relate to one another for grouping / cohort selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grouping {
    /// No `group_key`.
    Ungrouped,
    /// `group_key` round-robins across `n` groups.
    Grouped(usize),
    /// Items are partitioned into cohorts of `size` consecutive items sharing a `group_key`; each item
    /// carries `cohort_size = size` and the queue runs cohort policy.
    Cohort(usize),
}

/// The priority distribution stamped on generated items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityDist {
    /// Pseudo-uniform across a small band.
    Uniform,
    /// Heavily skewed: ~90% share the floor priority, ~10% spread above it.
    Skewed,
    /// Strictly monotonic with the item index.
    Sequential,
}

/// A representative item DATA SHAPE: payload size, field cardinality/size, grouping, priority shape.
#[derive(Debug, Clone, Copy)]
pub struct Shape {
    pub name: &'static str,
    pub payload_bytes: usize,
    pub n_fields: usize,
    pub field_bytes: usize,
    pub grouping: Grouping,
    pub priority: PriorityDist,
}

impl Shape {
    /// True when this shape requires cohort policy on the queue.
    pub fn needs_cohort(&self) -> bool {
        matches!(self.grouping, Grouping::Cohort(_))
    }
}

/// The representative SET of shapes (deliberately NOT the full cross-product). See the module/baseline doc.
pub fn all_shapes() -> Vec<Shape> {
    vec![
        // The floor reference: nothing but an id + a priority.
        Shape {
            name: "minimal",
            payload_bytes: 0,
            n_fields: 0,
            field_bytes: 0,
            grouping: Grouping::Ungrouped,
            priority: PriorityDist::Sequential,
        },
        // cayce-style compound work record: a 1KB body plus a wide structured field map.
        Shape {
            name: "hot_record",
            payload_bytes: 1024,
            n_fields: 16,
            field_bytes: 64,
            grouping: Grouping::Ungrouped,
            priority: PriorityDist::Uniform,
        },
        // A fat opaque body, no structured fields.
        Shape {
            name: "large_payload",
            payload_bytes: 16 * 1024,
            n_fields: 0,
            field_bytes: 0,
            grouping: Grouping::Ungrouped,
            priority: PriorityDist::Uniform,
        },
        // Grouped work fanned across 64 keys.
        Shape {
            name: "grouped",
            payload_bytes: 256,
            n_fields: 4,
            field_bytes: 32,
            grouping: Grouping::Grouped(64),
            priority: PriorityDist::Uniform,
        },
        // Cohorted work (cohorts of 8) — only on a cohort-policy queue.
        Shape {
            name: "cohort",
            payload_bytes: 256,
            n_fields: 4,
            field_bytes: 32,
            grouping: Grouping::Cohort(8),
            priority: PriorityDist::Uniform,
        },
        // A skewed priority band (hot-head workload).
        Shape {
            name: "skewed_priority",
            payload_bytes: 256,
            n_fields: 0,
            field_bytes: 0,
            grouping: Grouping::Ungrouped,
            priority: PriorityDist::Skewed,
        },
    ]
}

/// Look a shape up by name.
pub fn shape_by_name(name: &str) -> Option<Shape> {
    all_shapes().into_iter().find(|s| s.name == name)
}

/// A deterministic bit-mixer (splitmix64) so the shapes are reproducible run-to-run.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn priority_for(shape: &Shape, idx: u64) -> i64 {
    match shape.priority {
        PriorityDist::Sequential => idx as i64,
        PriorityDist::Uniform => (mix(idx) % 1000) as i64,
        PriorityDist::Skewed => {
            if mix(idx).is_multiple_of(10) {
                (mix(idx ^ 0xABCD) % 100 + 1) as i64
            } else {
                0
            }
        }
    }
}

/// Build a single [`NewItem`] for `shape` at index `idx`.
pub fn make_item(shape: &Shape, idx: u64) -> NewItem {
    let payload = (shape.payload_bytes > 0).then(|| Bytes::from(vec![b'x'; shape.payload_bytes]));
    let mut fields = BTreeMap::new();
    for f in 0..shape.n_fields {
        fields.insert(format!("f{f}"), Bytes::from(vec![b'y'; shape.field_bytes]));
    }
    let group_key = match shape.grouping {
        Grouping::Ungrouped => None,
        Grouping::Grouped(g) => Some(GroupKey::new(format!("g{}", idx % g as u64)).expect("gk")),
        Grouping::Cohort(sz) => Some(GroupKey::new(format!("c{}", idx / sz as u64)).expect("gk")),
    };
    let cohort_size = match shape.grouping {
        Grouping::Cohort(sz) => Some(sz as u64),
        _ => None,
    };
    NewItem {
        client_item_key: None,
        priority: Some(PriorityValue::Int64(priority_for(shape, idx))),
        group_key,
        not_before: None,
        payload,
        fields,
        metadata: Default::default(),
        cohort_size,
        gate_keys: Vec::new(),
        entity: None,
    }
}

/// Build a batch of `n` items for `shape` starting at `start`.
pub fn make_batch(shape: &Shape, start: u64, n: usize) -> Vec<NewItem> {
    (0..n as u64).map(|k| make_item(shape, start + k)).collect()
}

// ---------------------------------------------------------------------------
// Queue definition
// ---------------------------------------------------------------------------

/// A bench queue definition. Enables cohort policy when `shape` is cohorted; otherwise leaves it off.
pub fn bench_qdef(tenant: &str, queue: &str, shape: &Shape) -> QueueDefinition {
    let cohort_policy = match shape.grouping {
        Grouping::Cohort(sz) => Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(sz as u64),
        }),
        _ => None,
    };
    QueueDefinition {
        tenant_id: TenantId::new(tenant).expect("tenant"),
        queue_id: QueueId::new(queue).expect("queue"),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

/// A bench `QueueKey` in the `bench` tenant.
pub fn qkey(queue: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("bench").expect("tenant"),
        QueueId::new(queue).expect("queue"),
    )
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Throughput + per-batch latency sample for one op.
pub struct OpStats {
    pub op: &'static str,
    pub items: u64,
    pub wall: Duration,
    pub lat: Vec<Duration>,
}

impl OpStats {
    pub fn items_per_sec(&self) -> f64 {
        if self.wall.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.items as f64 / self.wall.as_secs_f64()
        }
    }
    pub fn items_per_hr(&self) -> f64 {
        self.items_per_sec() * 3600.0
    }
    pub fn meets_capacity_reference(&self) -> bool {
        self.items_per_sec() >= FLOOR_ITEMS_PER_SEC
    }
    /// The `p`-quantile (0.0..=1.0) batch latency. Sorts `lat` in place.
    pub fn pct(&mut self, p: f64) -> Duration {
        if self.lat.is_empty() {
            return Duration::ZERO;
        }
        self.lat.sort_unstable();
        let idx = (((self.lat.len() as f64) * p).ceil() as usize).saturating_sub(1);
        self.lat[idx.min(self.lat.len() - 1)]
    }
}

// ---------------------------------------------------------------------------
// Generic throughput workloads
// ---------------------------------------------------------------------------

/// Push `items` items of `shape` into `q` in batches of `batch`, timing each `push_batch`.
pub async fn ingest(
    pq: &Fireweed,
    q: &QueueKey,
    shape: &Shape,
    items: u64,
    batch: usize,
) -> OpStats {
    let mut lat = Vec::new();
    let mut done = 0u64;
    let start = Instant::now();
    while done < items {
        let n = (items - done).min(batch as u64) as usize;
        let batch_items = make_batch(shape, done, n);
        let t = Instant::now();
        pq.push_batch(q, batch_items).await.expect("push_batch");
        lat.push(t.elapsed());
        done += n as u64;
    }
    OpStats {
        op: "ingest",
        items,
        wall: start.elapsed(),
        lat,
    }
}

/// Drain up to `items` already-pending records: `claim`+`ack` in batches. Returns (claim, ack) stats.
pub async fn claim_ack(
    pq: &Fireweed,
    q: &QueueKey,
    items: u64,
    batch: usize,
) -> (OpStats, OpStats) {
    let mut claim_lat = Vec::new();
    let mut ack_lat = Vec::new();
    let mut drained = 0u64;
    let start = Instant::now();
    while drained < items {
        let tc = Instant::now();
        let claimed = pq.claim(q, batch, 3_600_000).await.expect("claim");
        let cd = tc.elapsed();
        if claimed.is_empty() {
            break;
        }
        claim_lat.push(cd);
        let ids: Vec<ItemId> = claimed.iter().map(|c| c.item_id).collect();
        let n = ids.len() as u64;
        let ta = Instant::now();
        pq.ack(q, ids).await.expect("ack");
        ack_lat.push(ta.elapsed());
        drained += n;
    }
    let wall = start.elapsed();
    (
        OpStats {
            op: "claim",
            items: drained,
            wall,
            lat: claim_lat,
        },
        OpStats {
            op: "ack",
            items: drained,
            wall,
            lat: ack_lat,
        },
    )
}

/// Claim up to `target` eligible items from `q` with `lease_ms`, in batches of `batch`.
async fn claim_n(
    pq: &Fireweed,
    q: &QueueKey,
    target: u64,
    batch: usize,
    lease_ms: u64,
) -> Vec<ClaimedItem> {
    let mut out = Vec::new();
    while (out.len() as u64) < target {
        let want = ((target - out.len() as u64).min(batch as u64)) as usize;
        let got = pq.claim(q, want, lease_ms).await.expect("claim");
        if got.is_empty() {
            break;
        }
        out.extend(got);
    }
    out
}

// ---------------------------------------------------------------------------
// Lifecycle workload (correctness + perf)
// ---------------------------------------------------------------------------

/// Measured throughput of a [`lifecycle`] run; latency percentiles live on the embedded [`OpStats`].
pub struct LifecycleStats {
    pub push: OpStats,
    pub claim: OpStats,
    pub ack: OpStats,
    /// Whether the in-place `update_fields` step ran (false on the eventual-apply object-log class).
    pub update_ran: bool,
}

/// A correctness + perf lifecycle over one (shape, backend): create→push→claim→update_fields→
/// ack/nack(retry)→reclaim_expired→re-drain. Asserts the state-machine invariants at each step and returns
/// `Err(reason)` on any violation, so callers (the e2e test, the binary) fail loudly.
///
/// `supports_update` is the backend's atomic-class capability flag: the object-log backend refuses
/// `update_fields` with `Unavailable`, so the update step is skipped there (and `LifecycleStats.update_ran`
/// is false).
///
/// Timing model: a small "abandon" slice is claimed under a SHORT lease so it expires for the
/// `reclaim_expired` step; everything finalized in-band holds a long lease so an ack/nack never races
/// lease expiry. The harness sleeps once, past both the short lease and the nack backoff, before the
/// reclaim + re-drain.
pub async fn lifecycle(
    pq: &Fireweed,
    q: &QueueKey,
    shape: &Shape,
    items: u64,
    batch: usize,
    supports_update: bool,
) -> Result<LifecycleStats, String> {
    const SHORT_LEASE_MS: u64 = 120;
    const LONG_LEASE_MS: u64 = 3_600_000;
    const BACKOFF_MS: i64 = 40;
    const SAMPLE: usize = 16;

    let check = |cond: bool, msg: &str| -> Result<(), String> {
        if cond {
            Ok(())
        } else {
            Err(format!("[{}/{:?}] invariant: {}", shape.name, q, msg))
        }
    };
    macro_rules! metrics {
        () => {
            pq.metrics(q).await.map_err(|e| format!("{e:?}"))?
        };
    }

    // --- push -------------------------------------------------------------
    let push = ingest(pq, q, shape, items, batch).await;
    let m = metrics!();
    check(
        m.pending == items,
        &format!("pending=={items} after push (got {})", m.pending),
    )?;
    check(
        m.leased == 0 && m.complete == 0 && m.failed == 0,
        "clean state after push",
    )?;

    // Partition: ~10% abandoned (expire→reclaim), ~10% nack-retry, the rest acked in round 1.
    let abandon_n = (items / 10).max(1);
    let nack_n = (items / 10).max(1);
    let ack_n = items - abandon_n - nack_n;

    // --- claim (abandon slice under a SHORT lease, working set under a LONG lease) -------------
    let claim_start = Instant::now();
    let abandon = claim_n(pq, q, abandon_n, batch, SHORT_LEASE_MS).await;
    check(
        abandon.len() as u64 == abandon_n,
        &format!(
            "claimed abandon slice =={abandon_n} (got {})",
            abandon.len()
        ),
    )?;
    let working = claim_n(pq, q, ack_n + nack_n, batch, LONG_LEASE_MS).await;
    let claim_wall = claim_start.elapsed();
    check(
        working.len() as u64 == ack_n + nack_n,
        &format!(
            "claimed working set =={} (got {})",
            ack_n + nack_n,
            working.len()
        ),
    )?;
    let m = metrics!();
    check(
        m.leased == items,
        &format!("leased=={items} after claim-all (got {})", m.leased),
    )?;
    check(
        m.pending == 0,
        &format!("pending==0 after claim-all (got {})", m.pending),
    )?;

    // --- update_fields on a sample of the long-leased working set --------------------------
    let mut update_ran = false;
    if supports_update {
        let sample = working.iter().take(SAMPLE).cloned().collect::<Vec<_>>();
        for it in &sample {
            let mut ops = BTreeMap::new();
            ops.insert("bench_touch".to_string(), Some(Bytes::from_static(b"1")));
            let new_ver = pq
                .update_fields(q, it.item_id, ops, PayloadUpdate::Keep, None, None)
                .await
                .map_err(|e| format!("update_fields: {e:?}"))?;
            check(
                new_ver > it.item_version,
                &format!(
                    "item_version bumped (was {}, got {new_ver})",
                    it.item_version
                ),
            )?;
            let live = pq
                .live_item(q, it.client_item_key.clone())
                .await
                .map_err(|e| format!("live_item: {e:?}"))?
                .ok_or_else(|| format!("[{}] live_item returned None after update", shape.name))?;
            check(
                live.item_version == new_ver,
                "live_item reflects bumped version",
            )?;
            check(
                live.fields.get("bench_touch").map(|b| b.as_ref()) == Some(b"1".as_ref()),
                "live_item reflects the set field",
            )?;
        }
        update_ran = true;
    } else {
        // Eventual-apply class: the update must be refused, not silently dropped.
        if let Some(it) = working.first() {
            let mut ops = BTreeMap::new();
            ops.insert("bench_touch".to_string(), Some(Bytes::from_static(b"1")));
            match pq
                .update_fields(q, it.item_id, ops, PayloadUpdate::Keep, None, None)
                .await
            {
                Err(EngineError::Unavailable) => {}
                other => {
                    return Err(format!(
                        "[{}] expected update_fields Unavailable on eventual-apply class, got {other:?}",
                        shape.name
                    ));
                }
            }
        }
    }

    // --- finalize round 1: ack most, nack-retry some, abandon the rest ----------------------
    let ack_ids: Vec<ItemId> = working
        .iter()
        .take(ack_n as usize)
        .map(|c| c.item_id)
        .collect();
    let nack_ids: Vec<ItemId> = working
        .iter()
        .skip(ack_n as usize)
        .map(|c| c.item_id)
        .collect();
    let ack_start = Instant::now();
    pq.ack(q, ack_ids.clone())
        .await
        .map_err(|e| format!("ack: {e:?}"))?;
    let ack_wall = ack_start.elapsed();
    let not_before = Some(plus_ms(now_ts(), BACKOFF_MS));
    pq.nack(q, nack_ids.clone(), Nack::Retry { not_before })
        .await
        .map_err(|e| format!("nack-retry: {e:?}"))?;

    let m = metrics!();
    check(
        m.complete == ack_n,
        &format!("complete=={ack_n} after ack (got {})", m.complete),
    )?;
    check(m.failed == 0, "no failures")?;
    check(
        m.pending == nack_n,
        &format!("pending=={nack_n} (nacked) (got {})", m.pending),
    )?;
    check(
        m.leased == abandon_n,
        &format!("leased=={abandon_n} (abandoned) (got {})", m.leased),
    )?;

    // --- wait past the short lease + the nack backoff, then reclaim the abandoned leases ------
    std::thread::sleep(Duration::from_millis(
        (SHORT_LEASE_MS + 80).max(BACKOFF_MS as u64 + 80),
    ));
    let reclaimed = pq
        .reclaim_expired(q, None)
        .await
        .map_err(|e| format!("reclaim_expired: {e:?}"))?;
    check(
        reclaimed.len() as u64 == abandon_n,
        &format!(
            "reclaimed=={abandon_n} expired leases (got {})",
            reclaimed.len()
        ),
    )?;
    let m = metrics!();
    check(
        m.leased == 0,
        &format!("leased==0 after reclaim (got {})", m.leased),
    )?;
    check(
        m.pending == nack_n + abandon_n,
        &format!(
            "pending=={} after reclaim (got {})",
            nack_n + abandon_n,
            m.pending
        ),
    )?;

    // --- round 2: the nacked (backoff elapsed) + reclaimed items are now claimable -----------
    let round2 = claim_n(pq, q, nack_n + abandon_n, batch, LONG_LEASE_MS).await;
    check(
        round2.len() as u64 == nack_n + abandon_n,
        &format!(
            "round-2 claim drains the retried+reclaimed set (got {})",
            round2.len()
        ),
    )?;
    let r2_ids: Vec<ItemId> = round2.iter().map(|c| c.item_id).collect();
    pq.ack(q, r2_ids)
        .await
        .map_err(|e| format!("round-2 ack: {e:?}"))?;
    let m = metrics!();
    check(
        m.complete == items,
        &format!("complete=={items} at end (got {})", m.complete),
    )?;
    check(m.pending == 0 && m.leased == 0, "fully drained at end")?;

    Ok(LifecycleStats {
        push: OpStats {
            op: "lc-push",
            items,
            wall: push.wall,
            lat: push.lat,
        },
        claim: OpStats {
            op: "lc-claim",
            items,
            wall: claim_wall,
            lat: vec![claim_wall],
        },
        ack: OpStats {
            op: "lc-ack",
            items: ack_n,
            wall: ack_wall,
            lat: vec![ack_wall],
        },
        update_ran,
    })
}
