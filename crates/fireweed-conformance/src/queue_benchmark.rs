//! Same settled-work workload for every API-007 store. Adapter fixtures supply
//! construction/provenance; this module owns generation, timing and correctness.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_contract::QueueStore;
use std::{collections::BTreeMap, time::Instant};

/// Fixtures must hold the operational clock fixed at `available_at` for the
/// complete run; priority ageing and lease deadlines then stay deterministic.
#[derive(Clone, Debug)]
pub struct Workload {
    pub records: u32,
    pub batch: u32,
    pub body_bytes: usize,
    pub state_bytes: usize,
    pub seed: u64,
    pub available_at: i64,
}
struct Samples {
    buckets: [u64; 129],
    count: u64,
    total: u128,
}
impl Default for Samples {
    fn default() -> Self {
        Self {
            buckets: [0; 129],
            count: 0,
            total: 0,
        }
    }
}
impl Samples {
    fn add(&mut self, start: Instant) {
        self.observe(start.elapsed().as_nanos());
    }
    fn observe(&mut self, ns: u128) {
        let bucket = if ns == 0 {
            0
        } else {
            (128 - ns.leading_zeros()) as usize
        };
        self.buckets[bucket] = self.buckets[bucket]
            .checked_add(1)
            .expect("histogram count exhausted");
        self.count = self.count.checked_add(1).expect("sample count exhausted");
        self.total = self.total.checked_add(ns).expect("latency sum exhausted");
    }
    fn report(&self) -> serde_json::Value {
        let percentile = |p: u64| {
            let rank = (u128::from(self.count) * u128::from(p)).div_ceil(100);
            let mut count = 0u128;
            for (bucket, samples) in self.buckets.iter().enumerate() {
                count += u128::from(*samples);
                if count >= rank && rank > 0 {
                    let lower = if bucket == 0 {
                        0
                    } else {
                        1u128 << (bucket - 1)
                    };
                    let upper = if bucket == 128 {
                        u128::MAX
                    } else {
                        (1u128 << bucket) - 1
                    };
                    return serde_json::json!({"lower_ns": lower.to_string(), "upper_ns": upper.to_string()});
                }
            }
            serde_json::Value::Null
        };
        serde_json::json!({"samples":self.count,"p50_bounds":percentile(50),"p95_bounds":percentile(95),"p99_bounds":percentile(99),"total_ns":self.total.to_string(),"histogram_counts":self.buckets.as_slice(),"histogram":"zero then inclusive powers-of-two nanosecond intervals; final bucket ends at u128::MAX","raw_samples_retained":false})
    }
}
/// Fixed-clock priority order: bucket ascending, insertion ordinal ascending.
struct Ordinals {
    records: u32,
    bucket: u32,
    next: u64,
}
impl Ordinals {
    fn new(records: u32) -> Self {
        Self {
            records,
            bucket: 0,
            next: 0,
        }
    }
}
impl Iterator for Ordinals {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        while self.bucket < 17 {
            if self.next < u64::from(self.records) {
                let result = self.next as u32;
                self.next += 17;
                return Some(result);
            }
            self.bucket += 1;
            self.next = u64::from(self.bucket);
        }
        None
    }
}
fn payload(seed: u64, ordinal: u32, len: usize) -> bytes::Bytes {
    let mut value = seed ^ u64::from(ordinal).wrapping_mul(0x9e3779b97f4a7c15);
    (0..len)
        .map(|_| {
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            value as u8
        })
        .collect::<Vec<_>>()
        .into()
}
fn work(config: &Workload, n: u32, stage: &str) -> NewWork {
    let key = format!("{stage}-{n:010}");
    NewWork {
        key: key.clone(),
        priority: i64::from(n % 17) - 8,
        available_at: config.available_at,
        fields: BTreeMap::from([
            ("kind".into(), FieldValue::Text(stage.into())),
            ("score".into(), FieldValue::Integer(i64::from(n))),
            ("target".into(), FieldValue::Text(key)),
        ]),
        body: payload(config.seed, n, config.body_bytes),
    }
}
fn ordinal(work: &ClaimedWork) -> u32 {
    let FieldValue::Integer(n) = work.record.work.fields["score"] else {
        panic!("missing ordinal")
    };
    u32::try_from(n).unwrap()
}
/// Timing anchors stay live until the fixture completes owned shutdown.
/// Only a successful drain may promote the report to completed qualification.
pub struct RunEvidence {
    pub report: serde_json::Value,
    pub lifecycle_started: Instant,
    pub started: Instant,
}
pub struct RunOutcome {
    pub report: serde_json::Value,
    pub passed: bool,
}
/// Retain failure evidence and attempt owned shutdown on every workload outcome.
/// Warmup is optional and separately timed; measured anchors exclude warmup and
/// construction, whose boundaries belong to the adapter fixture.
pub async fn run_owned(
    store: &dyn QueueStore,
    queue: QueueAddress,
    config: Workload,
    warmup_queue: Option<QueueAddress>,
) -> RunOutcome {
    use futures::FutureExt;
    let measured_queue = queue.clone();
    let warmup_started = Instant::now();
    let mut warmup_ns = 0;
    let workload = std::panic::AssertUnwindSafe(async {
        if let Some(address) = warmup_queue {
            let mut warmup = config.clone();
            warmup.records = 64;
            run(store, address, warmup).await;
            warmup_ns = warmup_started.elapsed().as_nanos();
        }
        run(store, queue, config.clone()).await
    })
    .catch_unwind()
    .await;
    let drain = Instant::now();
    let shutdown = std::panic::AssertUnwindSafe(async { store.shutdown().await })
        .catch_unwind()
        .await;
    let drain_ns = drain.elapsed().as_nanos();
    let (mut report, workload_passed) = match workload {
        Ok(evidence) => {
            let settled_ns = evidence.lifecycle_started.elapsed().as_nanos();
            let mut report = evidence.report;
            report["settled_ns"] = serde_json::json!(settled_ns);
            report["measured_run_through_drain_ns"] =
                serde_json::json!(evidence.started.elapsed().as_nanos());
            report["measured_run_boundary"] = serde_json::json!(
                "provision start after construction and warmup through owned shutdown"
            );
            report["settled_per_second"] =
                serde_json::json!((f64::from(config.records) * 2.0) * 1e9 / (settled_ns as f64));
            report["rate_scope"] = serde_json::json!(
                "continuous lifecycle start through successful verification and owned shutdown"
            );
            (report, true)
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("non-string workload panic");
            (
                serde_json::json!({"qualification":"internal-port reference workload only","workload_failure":message}),
                false,
            )
        }
    };
    for (field, value) in [
        ("records", serde_json::json!(config.records)),
        ("batch", serde_json::json!(config.batch)),
        ("body_bytes", serde_json::json!(config.body_bytes)),
        ("state_bytes", serde_json::json!(config.state_bytes)),
        ("seed", serde_json::json!(config.seed)),
        ("available_at", serde_json::json!(config.available_at)),
    ] {
        report[field] = value;
    }
    report["queue"] = serde_json::json!(measured_queue);
    let passed = workload_passed && matches!(shutdown, Ok(Ok(())));
    report["correctness"] = serde_json::json!(if passed {
        "passed_after_owned_shutdown"
    } else {
        "failed"
    });
    report["warmup_ns"] = serde_json::json!(warmup_ns);
    report["drain_ns"] = serde_json::json!(drain_ns);
    report["shutdown"] = serde_json::json!(match shutdown {
        Ok(Ok(())) => "completed".into(),
        Ok(Err(error)) => format!("failed: {error:?}"),
        Err(_) => "failed: shutdown panicked".into(),
    });
    if !passed {
        report["settled_per_second"] = serde_json::Value::Null;
        report["rate_scope"] = serde_json::json!("failed run; no completed settlement rate");
    }
    RunOutcome { report, passed }
}
/// Single-client reference lane, deliberately measuring complete acknowledged
/// state+fanout settlement, with no adapter-private fast path or debt subtraction.
/// Setup, insert, lifecycle, verification and shutdown are separate phases.
pub async fn run(store: &dyn QueueStore, queue: QueueAddress, config: Workload) -> RunEvidence {
    let started = Instant::now();
    assert!(config.records > 0 && config.batch > 0 && config.batch <= 64);
    let mut insert = Samples::default();
    let mut claim = Samples::default();
    let mut commit = Samples::default();
    let mut stats = Samples::default();
    let mut query = Samples::default();
    let start = Instant::now();
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: crate::queue_contract::schema(),
        })
        .await
        .unwrap();
    let provision_ns = start.elapsed().as_nanos();
    let insertion = Instant::now();
    // Foreign records remain pending throughout the measured lifecycle, proving
    // filtered selection did not consume unrelated work.
    for offset in (0..config.records).step_by(config.batch as usize) {
        let end = config.records.min(offset.saturating_add(config.batch));
        let items = (offset..end)
            .flat_map(|n| [work(&config, n, "source"), work(&config, n, "foreign")])
            .collect();
        let start = Instant::now();
        let outcomes = store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: format!("seed-{offset}"),
                mode: InsertMode::InsertOnly,
                items,
            })
            .await
            .unwrap();
        insert.add(start);
        assert_eq!(outcomes.len(), ((end - offset) * 2) as usize);
        for (position, outcome) in outcomes.into_iter().enumerate() {
            let n = offset + (position / 2) as u32;
            let stage = if position % 2 == 0 {
                "source"
            } else {
                "foreign"
            };
            assert_eq!(
                outcome,
                InsertOutcome::Inserted {
                    key: format!("{stage}-{n:010}"),
                    version: 1,
                },
                "exact inserted positional identity/version"
            );
        }
    }
    let insertion_ns = insertion.elapsed().as_nanos();
    let lifecycle = Instant::now();
    let mut replay = None;
    for (phase, stage) in ["source", "next"].into_iter().enumerate() {
        let mut expected = Ordinals::new(config.records);
        let mut consumed = 0u64;
        let mut request = 0u64;
        loop {
            let start = Instant::now();
            let selected = store
                .claim(ClaimRequest {
                    queue: queue.clone(),
                    request_id: format!("claim-{stage}-{request}"),
                    filter: Filter {
                        fields: BTreeMap::from([(
                            "kind".into(),
                            Predicate::Equal(FieldValue::Text(stage.into())),
                        )]),
                        ..Filter::default()
                    },
                    order: ClaimOrder::PriorityAscending,
                    eligible_at: config.available_at,
                    lease_nanos: 60_000_000_000,
                    max_records: config.batch,
                    max_bytes: 4 << 20,
                    fields: None,
                    include_body: true,
                })
                .await
                .unwrap();
            claim.add(start);
            request = request
                .checked_add(1)
                .expect("claim request counter exhausted");
            if selected.is_empty() {
                break;
            }
            let mut groups = Vec::new();
            for work in selected {
                let n = ordinal(&work);
                assert_eq!(Some(n), expected.next(), "filtered priority/tie order");
                consumed += 1;
                assert_eq!(work.record.version, 2, "one insertion and one claim");
                assert_eq!(
                    work.record.insertion_sequence,
                    if phase == 0 {
                        u64::from(n) * 2 + 1
                    } else {
                        u64::from(config.records) * 2 + consumed
                    },
                    "exact insertion sequence"
                );
                assert_eq!(
                    work.record.work,
                    self::work(&config, n, stage),
                    "exact claimed work"
                );
                assert_eq!(work.record.work.key, format!("{stage}-{n:010}"));
                assert_eq!(
                    work.record.work.body,
                    payload(config.seed, n, config.body_bytes)
                );
                let state_updates = if phase == 0 {
                    vec![StateUpdate {
                        collection: "instances".into(),
                        key: format!("state-{n:010}"),
                        expect: StateExpect::Absent,
                        change: StateChange::Put {
                            fields: BTreeMap::from([(
                                "run".into(),
                                FieldValue::Text("benchmark".into()),
                            )]),
                            value: payload(config.seed ^ 17, n, config.state_bytes),
                        },
                    }]
                } else {
                    vec![]
                };
                groups.push(CommitGroup {
                    administration: None,
                    request_id: format!("settle-{stage}-{n}"),
                    claimed_updates: vec![ClaimedUpdate {
                        key: work.record.work.key,
                        lease: work.lease,
                        expected_version: work.record.version,
                        patch: WorkPatch::default(),
                        disposition: Disposition::Complete,
                    }],
                    state_updates,
                    work_inserts: if phase == 0 {
                        vec![self::work(&config, n, "next")]
                    } else {
                        vec![]
                    },
                    work_updates: vec![],
                });
            }
            let batch = CommitBatch {
                queue: queue.clone(),
                groups,
            };
            let start = Instant::now();
            let receipts = store.commit(batch.clone()).await.unwrap();
            commit.add(start);
            assert_eq!(receipts.len(), batch.groups.len());
            for (receipt, group) in receipts.iter().zip(&batch.groups) {
                assert_eq!(receipt.request_id, group.request_id);
                let claimed = &group.claimed_updates[0];
                let mut expected_versions = vec![RecordVersion {
                    collection: None,
                    key: claimed.key.clone(),
                    version: claimed.expected_version.checked_add(1).unwrap(),
                }];
                if phase == 0 {
                    expected_versions.push(RecordVersion {
                        collection: Some("instances".into()),
                        key: group.state_updates[0].key.clone(),
                        version: 1,
                    });
                    expected_versions.push(RecordVersion {
                        collection: None,
                        key: group.work_inserts[0].key.clone(),
                        version: 1,
                    });
                }
                assert_eq!(
                    receipt.outcome,
                    GroupOutcome::Committed {
                        versions: expected_versions
                    }
                );
            }
            replay.get_or_insert((batch, receipts));
        }
        assert_eq!(consumed, u64::from(config.records));
        assert_eq!(expected.next(), None, "priority iterator must be exhausted");
    }
    let lifecycle_ns = lifecycle.elapsed().as_nanos();
    let verify = Instant::now();
    let (batch, receipt) = replay.unwrap();
    assert_eq!(store.commit(batch).await.unwrap(), receipt);
    let start = Instant::now();
    let final_stats = store
        .stats(StatsRequest {
            buckets: None,
            queue: queue.clone(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: config.available_at,
        })
        .await
        .unwrap();
    stats.add(start);
    assert_eq!(final_stats.totals.completed, u64::from(config.records) * 2);
    assert_eq!(final_stats.totals.pending, u64::from(config.records));
    assert_eq!(final_stats.totals.leased, 0);
    assert_eq!(final_stats.totals.failed, 0);
    let mut cursor = None;
    let mut state_count = 0u32;
    loop {
        let before_page = state_count;
        let start = Instant::now();
        let page = store
            .query(QueryRequest {
                queue: queue.clone(),
                source: RecordSource::State("instances".into()),
                index: "$key".into(),
                filter: Filter::default(),
                descending: false,
                cursor,
                max_records: config.batch,
                max_bytes: 4 << 20,
            })
            .await
            .unwrap();
        query.add(start);
        for record in page.records {
            let QueryRecord::State(state) = record else {
                panic!("work leaked into state query")
            };
            let n = state
                .key
                .strip_prefix("state-")
                .unwrap()
                .parse::<u32>()
                .unwrap();
            assert!(state_count < config.records, "extra state record");
            assert_eq!(n, state_count, "exact consecutive state ordinal");
            assert_eq!(state.key, format!("state-{state_count:010}"));
            state_count += 1;
            assert_eq!(state.version, 1);
            assert_eq!(
                state.fields,
                BTreeMap::from([("run".into(), FieldValue::Text("benchmark".into()))])
            );
            assert_eq!(
                state.value,
                payload(config.seed ^ 17, n, config.state_bytes)
            );
        }
        assert!(
            page.cursor.is_none() || state_count > before_page,
            "non-progressing page"
        );
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(state_count, config.records);
    let verify_ns = verify.elapsed().as_nanos();
    let report = serde_json::json!({"qualification": "single-client reference workload only", "rate_scope": "acknowledged lifecycle rate; physical drain/debt unmeasured", "latency_scope": "claim and commit aggregate source+next phases; claim includes terminal empty probes; source commits include state+fanout, next commits settlement only", "correctness": "workload_verified_shutdown_pending", "records": config.records,"source_inserts":config.records,"foreign_inserts":config.records,"generated_next_inserts":config.records,"latency_evidence":"bounded histogram only; no raw latency campaign qualification","batch":config.batch,"body_bytes":config.body_bytes,"state_bytes":config.state_bytes,"seed":config.seed,"available_at":config.available_at,"provision_ns":provision_ns,"insertion_ns":insertion_ns,"lifecycle_ns":lifecycle_ns,"verification_ns":verify_ns,"useful_settled_work":u64::from(config.records)*2,"acknowledged_settled_per_second":(f64::from(config.records)*2.0)*1e9/(lifecycle_ns as f64),"insert":insert.report(),"claim":claim.report(),"commit":commit.report(),"stats":stats.report(),"query":query.report()});
    RunEvidence {
        report,
        lifecycle_started: lifecycle,
        started,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_includes_zero_and_maximum_without_overflow() {
        let mut samples = Samples::default();
        for ns in [0, 1, 2, 3, 4, 8] {
            samples.observe(ns);
        }
        let report = samples.report();
        assert_eq!(report["samples"], 6);
        assert_eq!(
            report["p50_bounds"],
            serde_json::json!({"lower_ns":"2","upper_ns":"3"})
        );
        assert_eq!(
            report["p99_bounds"],
            serde_json::json!({"lower_ns":"8","upper_ns":"15"})
        );
        let mut maximum = Samples::default();
        maximum.observe(u128::MAX);
        assert_eq!(maximum.buckets[128], 1);
        assert_eq!(
            maximum.report()["p99_bounds"]["upper_ns"],
            serde_json::json!(u128::MAX.to_string())
        );
        assert!(Samples::default().report()["p50_bounds"].is_null());
    }
    #[test]
    fn bounded_priority_oracle_matches_sort_and_exhausts() {
        for records in [0, 1, 16, 17, 18, 64, 1001] {
            let mut expected: Vec<_> = (0..records).collect();
            expected.sort_unstable_by_key(|n| (n % 17, *n));
            let mut actual = Ordinals::new(records);
            for n in expected {
                assert_eq!(actual.next(), Some(n));
            }
            assert_eq!(actual.next(), None);
            assert_eq!(actual.next(), None);
        }
        let mut largest = Ordinals::new(u32::MAX);
        assert_eq!(largest.next(), Some(0));
        largest.bucket = 16;
        largest.next = u64::from(u32::MAX) - 1;
        assert_eq!(largest.next(), Some(u32::MAX - 1));
        assert_eq!(largest.next(), None);
    }
}
