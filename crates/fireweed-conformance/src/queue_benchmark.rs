//! Same settled-work workload for every API-007 store. Adapter fixtures supply
//! construction/provenance; this module owns generation, timing and correctness.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_contract::QueueStore;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct Workload {
    pub records: u32,
    pub batch: u32,
    pub body_bytes: usize,
    pub state_bytes: usize,
    pub seed: u64,
    pub available_at: i64,
}
#[derive(Default)]
struct Samples(Vec<u128>);
impl Samples {
    fn add(&mut self, start: Instant) {
        self.0.push(start.elapsed().as_nanos());
    }
    fn report(&mut self) -> serde_json::Value {
        self.0.sort_unstable();
        let percentile = |p: usize| {
            self.0
                .get((self.0.len() * p).div_ceil(100).saturating_sub(1))
                .copied()
                .unwrap_or(0)
        };
        serde_json::json!({"samples": self.0.len(), "p50_ns": percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99), "total_ns": self.0.iter().sum::<u128>()})
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
/// Single-client reference lane, deliberately measuring complete acknowledged
/// state+fanout settlement, with no adapter-private fast path or debt subtraction.
/// Setup, insert, lifecycle, verification and shutdown are separate phases.
pub async fn run(
    store: &dyn QueueStore,
    queue: QueueAddress,
    config: Workload,
) -> serde_json::Value {
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
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, InsertOutcome::Inserted { .. }))
        );
    }
    let lifecycle = Instant::now();
    let mut seen = [BTreeSet::new(), BTreeSet::new()];
    let mut expected: Vec<_> = (0..config.records).collect();
    expected.sort_unstable_by_key(|n| (i64::from(n % 17) - 8, *n));
    let mut replay = None;
    for (phase, stage) in ["source", "next"].into_iter().enumerate() {
        let mut request = 0;
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
            request += 1;
            if selected.is_empty() {
                break;
            }
            let actual: Vec<_> = selected.iter().map(ordinal).collect();
            let consumed = seen[phase].len();
            assert_eq!(
                actual,
                expected[consumed..consumed + actual.len()],
                "filtered priority/tie order"
            );
            let mut groups = Vec::new();
            for work in selected {
                let n = ordinal(&work);
                assert!(
                    n < config.records && seen[phase].insert(n),
                    "duplicate or foreign lease"
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
            assert!(
                receipts
                    .iter()
                    .all(|r| matches!(r.outcome, GroupOutcome::Committed { .. }))
            );
            replay.get_or_insert((batch, receipts));
        }
        assert_eq!(seen[phase].len(), config.records as usize);
    }
    let lifecycle_ns = lifecycle.elapsed().as_nanos();
    let verify = Instant::now();
    let (batch, receipt) = replay.unwrap();
    assert_eq!(store.commit(batch).await.unwrap(), receipt);
    let start = Instant::now();
    let final_stats = store
        .stats(StatsRequest {
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
    let mut state_keys = BTreeSet::new();
    loop {
        let before_page = state_keys.len();
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
            assert!(n < config.records && state_keys.insert(n));
            assert_eq!(
                state.value,
                payload(config.seed ^ 17, n, config.state_bytes)
            );
        }
        assert!(
            page.cursor.is_none() || state_keys.len() > before_page,
            "non-progressing page"
        );
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(state_keys.len(), config.records as usize);
    let verify_ns = verify.elapsed().as_nanos();
    serde_json::json!({"qualification": "single-client reference workload only", "rate_scope": "acknowledged lifecycle rate; physical drain/debt unmeasured", "latency_scope": "claim and commit aggregate source+next phases; claim includes terminal empty probes; source commits include state+fanout, next commits settlement only", "correctness": "passed", "records": config.records,"batch":config.batch,"body_bytes":config.body_bytes,"state_bytes":config.state_bytes,"seed":config.seed,"available_at":config.available_at,"provision_ns":provision_ns,"lifecycle_ns":lifecycle_ns,"verification_ns":verify_ns,"useful_settled_work":u64::from(config.records)*2,"acknowledged_settled_per_second":(f64::from(config.records)*2.0)*1e9/(lifecycle_ns as f64),"insert":insert.report(),"claim":claim.report(),"commit":commit.report(),"stats":stats.report(),"query":query.report()})
}
