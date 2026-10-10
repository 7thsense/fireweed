//! Mandatory API-007 scenarios shared by every adapter, without capability skips.
//! This is the first operational slice; crash/cutover and qualification workloads
//! extend this same suite before production activation.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_contract::QueueStore;
use std::collections::{BTreeMap, BTreeSet};

pub fn schema() -> QueueSchema {
    let work = CollectionDefinition {
        additional_field_prefixes: BTreeSet::new(),
        fields: BTreeMap::from([
            ("kind".into(), FieldType::Text),
            ("score".into(), FieldType::Integer),
            ("target".into(), FieldType::Text),
        ]),
        indexes: BTreeMap::from([
            ("by_score".into(), vec!["score".into()]),
            ("by_target".into(), vec!["target".into()]),
        ]),
        unique_indexes: BTreeSet::from(["by_target".into()]),
    };
    QueueSchema {
        work,
        state: BTreeMap::from([(
            "instances".into(),
            CollectionDefinition {
                additional_field_prefixes: BTreeSet::new(),
                fields: BTreeMap::from([("run".into(), FieldType::Text)]),
                indexes: BTreeMap::from([("by_run".into(), vec!["run".into()])]),
                unique_indexes: BTreeSet::new(),
            },
        )]),
        progress_bound_nanos: 1_000_000_000,
        max_attempts: 3,
    }
}
fn item(key: &str, kind: &str, priority: i64, score: i64) -> NewWork {
    NewWork {
        key: key.into(),
        priority,
        available_at: 0,
        fields: BTreeMap::from([
            ("kind".into(), FieldValue::Text(kind.into())),
            ("score".into(), FieldValue::Integer(score)),
            ("target".into(), FieldValue::Text(key.into())),
        ]),
        body: vec![0, 1, 255].into(),
    }
}
fn group(id: &str) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
    }
}
fn filter(kind: &str) -> Filter {
    Filter {
        fields: BTreeMap::from([(
            "kind".into(),
            Predicate::Equal(FieldValue::Text(kind.into())),
        )]),
        ..Filter::default()
    }
}
fn claim(queue: &QueueAddress, id: &str, kind: &str) -> ClaimRequest {
    ClaimRequest {
        queue: queue.clone(),
        request_id: id.into(),
        filter: filter(kind),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000_000,
        max_records: 10,
        max_bytes: 1 << 20,
        fields: None,
        include_body: true,
    }
}
fn update(work: &ClaimedWork) -> ClaimedUpdate {
    ClaimedUpdate {
        key: work.record.work.key.clone(),
        lease: work.lease.clone(),
        expected_version: work.record.version,
        patch: WorkPatch::default(),
        disposition: Disposition::Complete,
    }
}
fn committed(receipt: &CommitReceipt) {
    assert!(
        matches!(receipt.outcome, GroupOutcome::Committed { .. }),
        "{receipt:?}"
    );
}

pub async fn run(store: &dyn QueueStore, queue: QueueAddress) {
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let mut changed = schema();
    changed.max_attempts += 1;
    assert_eq!(
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: changed
            })
            .await,
        Err(QueueError::DefinitionConflict)
    );
    let insert = InsertRequest {
        queue: queue.clone(),
        request_id: "seed".into(),
        mode: InsertMode::InsertOnly,
        items: vec![
            item("low", "matching", -9, -12),
            item("high", "matching", 8, 3),
            item("foreign", "foreign", -100, 1),
        ],
    };
    let first = store.insert(insert.clone()).await.unwrap();
    assert_eq!(first.len(), 3);
    assert_eq!(store.insert(insert.clone()).await.unwrap(), first);
    let mut altered = insert;
    altered.items[0].priority += 1;
    assert_eq!(
        store.insert(altered).await,
        Err(QueueError::RequestConflict)
    );
    let mut tiny = claim(&queue, "too-small", "matching");
    tiny.max_bytes = 1;
    assert_eq!(store.claim(tiny).await, Err(QueueError::TooLarge));
    let request = claim(&queue, "first-claim", "matching");
    let selected = store.claim(request.clone()).await.unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|w| w.record.work.key.as_str())
            .collect::<Vec<_>>(),
        vec!["low", "high"]
    );
    assert_eq!(store.claim(request.clone()).await.unwrap(), selected);
    let mut transition = group("transition");
    transition.claimed_updates.push(update(&selected[0]));
    transition.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "instance".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([("run".into(), FieldValue::Text("run-1".into()))]),
            value: vec![7, 8].into(),
        },
    });
    transition.work_inserts.push(item("next", "next", 0, 2));
    let receipt = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![transition.clone()],
        })
        .await
        .unwrap();
    committed(&receipt[0]);
    let reads = store
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["instance".into(), "missing".into(), "instance".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(reads.len(), 3);
    assert_eq!(reads[0], reads[2]);
    assert_eq!(reads[1], None);
    assert_eq!(reads[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    assert_eq!(store.claim(request).await, Err(QueueError::ClaimExpired));
    let mut independent = group("independent");
    independent
        .work_inserts
        .push(item("independent", "next", 1, 4));
    let regrouped = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![independent, transition.clone()],
        })
        .await
        .unwrap();
    committed(&regrouped[0]);
    assert_eq!(regrouped[1], receipt[0]);
    let lookup = store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["transition".into(), "absent".into(), "transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(
        lookup,
        vec![
            ReceiptLookup::Found(receipt[0].clone()),
            ReceiptLookup::Absent,
            ReceiptLookup::Found(receipt[0].clone())
        ]
    );
    transition.work_inserts[0].priority += 1;
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![transition]
            })
            .await
            .unwrap()[0]
            .outcome,
        GroupOutcome::RequestConflict
    );
    let mut rejected = group("rejected");
    rejected.claimed_updates.push(update(&selected[1]));
    rejected.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "instance".into(),
        expect: StateExpect::Version(999),
        change: StateChange::Delete,
    });
    rejected
        .work_inserts
        .push(item("must-not-exist", "next", 0, 0));
    let rejection = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![rejected.clone()],
        })
        .await
        .unwrap();
    assert_eq!(
        rejection[0].outcome,
        GroupOutcome::Rejected(Rejection::VersionConflict)
    );
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![rejected]
            })
            .await
            .unwrap(),
        rejection
    );
    let stats = store
        .stats(StatsRequest {
            queue: queue.clone(),
            filter: filter("matching"),
            group_by: vec!["kind".into()],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(stats.totals.completed, 1);
    assert_eq!(stats.totals.leased, 1);
    assert_eq!(stats.totals.pending, 0);
    assert_eq!(stats.groups.len(), 1);
    assert_eq!(stats.groups[0].counts, stats.totals);
    let next = store
        .claim(claim(&queue, "claim-next", "next"))
        .await
        .unwrap();
    assert_eq!(next.len(), 2);
    assert!(!next.iter().any(|w| w.record.work.key == "must-not-exist"));
    let query = QueryRequest {
        queue: queue.clone(),
        source: RecordSource::Work,
        index: "by_score".into(),
        filter: filter("matching"),
        descending: false,
        cursor: None,
        max_records: 1,
        max_bytes: 1 << 20,
    };
    let page = store.query(query.clone()).await.unwrap();
    assert_eq!(page.records.len(), 1);
    let QueryRecord::Work(low) = &page.records[0] else {
        panic!("work query")
    };
    assert_eq!(low.work.key, "low");
    let mut second = query.clone();
    second.cursor = page.cursor;
    let page = store.query(second.clone()).await.unwrap();
    let QueryRecord::Work(high) = &page.records[0] else {
        panic!("work query")
    };
    assert_eq!(high.work.key, "high");
    if let Some(cursor) = page.cursor {
        second.cursor = Some(cursor);
        assert!(store.query(second).await.unwrap().records.is_empty());
    }
    let mut descending = query;
    descending.descending = true;
    let page = store.query(descending).await.unwrap();
    let QueryRecord::Work(high) = &page.records[0] else {
        panic!("work query")
    };
    assert_eq!(high.work.key, "high");
    let mut bad_cursor = claim(&queue, "unchanged-claim", "foreign");
    bad_cursor.fields = Some(BTreeSet::new());
    bad_cursor.include_body = false;
    let foreign = store.claim(bad_cursor).await.unwrap();
    assert_eq!(foreign.len(), 1);
    assert!(foreign[0].record.work.body.is_empty());
    assert!(foreign[0].record.work.fields.is_empty());
    let mut finish = group("finish-foreign");
    finish.claimed_updates.push(update(&foreign[0]));
    store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![finish],
        })
        .await
        .unwrap();
    let page = store
        .query(QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "$key".into(),
            filter: filter("foreign"),
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    let QueryRecord::Work(work) = &page.records[0] else {
        panic!("work query")
    };
    assert_eq!(work.work.body.as_ref(), &[0, 1, 255]);
    // Pending-only upsert: cannot re-enroll terminal work with a new identity.
    let terminal = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "terminal-upsert".into(),
            mode: InsertMode::Upsert,
            items: vec![item("foreign", "foreign", 0, 1)],
        })
        .await
        .unwrap();
    assert!(matches!(
        &terminal[0],
        InsertOutcome::Rejected {
            reason: Rejection::Terminal,
            ..
        }
    ));
    let mut unique = item("duplicate-target", "unique", 0, 0);
    unique
        .fields
        .insert("target".into(), FieldValue::Text("low".into()));
    let result = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "unique".into(),
            mode: InsertMode::InsertOnly,
            items: vec![unique],
        })
        .await
        .unwrap();
    assert!(matches!(
        &result[0],
        InsertOutcome::Rejected {
            reason: Rejection::UniqueConflict,
            ..
        }
    ));
}

/// Identical injected ownership clock fixture for every new binding adapter.
#[derive(Default)]
pub struct TestClock(std::sync::atomic::AtomicI64);
impl TestClock {
    pub fn set(&self, nanos: i64) {
        self.0.store(nanos, std::sync::atomic::Ordering::SeqCst);
    }
}
impl fireweed_engine::Clock for TestClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}

pub async fn competing_claims(a: &dyn QueueStore, b: &dyn QueueStore, queue: QueueAddress) {
    a.provision(ProvisionRequest {
        queue: queue.clone(),
        schema: schema(),
    })
    .await
    .unwrap();
    let items = (0..100)
        .map(|n| item(&format!("race-{n}"), "race", n, n))
        .collect();
    a.insert(InsertRequest {
        queue: queue.clone(),
        request_id: "seed-race".into(),
        mode: InsertMode::InsertOnly,
        items,
    })
    .await
    .unwrap();
    let mut left = claim(&queue, "left-race", "race");
    left.max_records = 50;
    let mut right = left.clone();
    right.request_id = "right-race".into();
    let (left, right) = tokio::join!(a.claim(left), b.claim(right));
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.len(), 50);
    assert_eq!(right.len(), 50);
    let keys: BTreeSet<_> = left
        .iter()
        .chain(&right)
        .map(|w| w.record.work.key.clone())
        .collect();
    assert_eq!(keys.len(), 100);
}

pub async fn expiry_and_administration(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    clock.set(0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "seed-expiry".into(),
            mode: InsertMode::InsertOnly,
            items: vec![item("expiring", "expiry", 0, 0)],
        })
        .await
        .unwrap();
    let mut request = claim(&queue, "expiring-claim", "expiry");
    request.lease_nanos = 1_000_000;
    let first = store.claim(request.clone()).await.unwrap();
    assert_eq!(first.len(), 1);
    clock.set(1_000_000);
    assert_eq!(store.claim(request).await, Err(QueueError::ClaimExpired));
    let mut stale = group("expired-settle");
    stale.claimed_updates.push(update(&first[0]));
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![stale]
            })
            .await
            .unwrap()[0]
            .outcome,
        GroupOutcome::Rejected(Rejection::StaleLease)
    );
    let second = store
        .claim(claim(&queue, "new-claim", "expiry"))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_ne!(second[0].lease, first[0].lease);
    assert!(second[0].record.version > first[0].record.version);
    let mut rearm = group("force-due");
    rearm.work_updates.push(UnclaimedUpdate {
        key: "expiring".into(),
        expect: StateExpect::Version(second[0].record.version),
        change: UnclaimedChange::Administrative {
            patch: WorkPatch::default(),
            lifecycle: AdministrativeLifecycle::Pending,
            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
        },
    });
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![rearm],
            })
            .await
            .unwrap()[0],
    );
    let third = store
        .claim(claim(&queue, "third-claim", "expiry"))
        .await
        .unwrap();
    assert_eq!(third[0].record.attempts, 3);
    assert_eq!(
        third[0].record.insertion_sequence,
        first[0].record.insertion_sequence
    );
    let mut operator = group("operator-complete");
    operator.work_updates.push(UnclaimedUpdate {
        key: "expiring".into(),
        expect: StateExpect::Version(third[0].record.version),
        change: UnclaimedChange::Administrative {
            patch: WorkPatch {
                body: Some(third[0].record.work.body.clone()),
                ..WorkPatch::default()
            },
            lifecycle: AdministrativeLifecycle::Completed,
            lease_guard: AdministrativeLeaseGuard::RequireActive,
        },
    });
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![operator],
            })
            .await
            .unwrap()[0],
    );
    let stats = store
        .stats(StatsRequest {
            queue,
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(stats.totals.completed, 1);
    assert_eq!(stats.totals.leased, 0);
    clock.set(0);
}

pub async fn ordered_long_values_and_cursor_scope(store: &dyn QueueStore, queue: QueueAddress) {
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let long = "a".repeat(4096);
    let values = [
        format!("{long}z"),
        format!("{long}aa"),
        format!("{long}é"),
        format!("{long}🦊"),
    ];
    let items = values
        .iter()
        .enumerate()
        .map(|(n, value)| {
            let mut work = item(&format!("{long}/{n}"), "long", 0, n as i64);
            work.fields
                .insert("target".into(), FieldValue::Text(value.clone()));
            work
        })
        .collect();
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "long-seed".into(),
            mode: InsertMode::InsertOnly,
            items,
        })
        .await
        .unwrap();
    let query = QueryRequest {
        queue: queue.clone(),
        source: RecordSource::Work,
        index: "by_target".into(),
        filter: filter("long"),
        descending: false,
        cursor: None,
        max_records: 1,
        max_bytes: 1 << 20,
    };
    let page = store.query(query.clone()).await.unwrap();
    let cursor = page.cursor.clone().unwrap();
    let QueryRecord::Work(first) = &page.records[0] else {
        panic!("work query")
    };
    assert_eq!(
        first.work.fields["target"],
        FieldValue::Text(values[1].clone())
    );
    let mut wrong = query.clone();
    wrong.cursor = Some(cursor.clone());
    wrong.descending = true;
    assert!(matches!(
        store.query(wrong).await,
        Err(QueueError::Invalid(_))
    ));
    let mut corrupt = cursor;
    corrupt[0] ^= 1;
    let mut wrong = query.clone();
    wrong.cursor = Some(corrupt);
    assert!(matches!(
        store.query(wrong).await,
        Err(QueueError::Invalid(_))
    ));
    let mut seen = vec![values[1].clone()];
    let mut query = query;
    query.cursor = page.cursor;
    loop {
        let page = store.query(query.clone()).await.unwrap();
        for record in page.records {
            let QueryRecord::Work(work) = record else {
                panic!("work query")
            };
            let FieldValue::Text(value) = &work.work.fields["target"] else {
                panic!("text")
            };
            seen.push(value.clone());
        }
        if page.cursor.is_none() {
            break;
        }
        query.cursor = page.cursor;
    }
    let mut expected = values.to_vec();
    expected.sort();
    assert_eq!(seen, expected);
}

pub async fn predicate_and_order_oracles(store: &dyn QueueStore, queue: QueueAddress) {
    let mut definition = schema();
    for (name, ty) in [
        ("blob", FieldType::Bytes),
        ("flag", FieldType::Boolean),
        ("float", FieldType::Float),
        ("text", FieldType::Text),
    ] {
        definition.work.fields.insert(name.into(), ty);
        definition
            .work
            .indexes
            .insert(format!("by_{name}"), vec![name.into()]);
    }
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let domains = vec![
        (
            "score",
            vec![
                FieldValue::Integer(i64::MIN),
                FieldValue::Integer(-1),
                FieldValue::Integer(0),
                FieldValue::Integer(1),
                FieldValue::Integer(i64::MAX),
            ],
        ),
        (
            "float",
            vec![
                FieldValue::Float(-f64::MAX),
                FieldValue::Float(-1.0),
                FieldValue::Float(-0.0),
                FieldValue::Float(0.0),
                FieldValue::Float(1.0),
                FieldValue::Float(f64::MAX),
            ],
        ),
        (
            "flag",
            vec![FieldValue::Boolean(false), FieldValue::Boolean(true)],
        ),
        (
            "blob",
            vec![
                FieldValue::Bytes(vec![]),
                FieldValue::Bytes(vec![0]),
                FieldValue::Bytes(vec![0, 0]),
                FieldValue::Bytes(vec![0, 255]),
                FieldValue::Bytes(vec![255]),
            ],
        ),
        (
            "text",
            ["", "a\0", "aa", "z", "é", "🦊"]
                .into_iter()
                .map(|s| FieldValue::Text(s.into()))
                .collect(),
        ),
    ];
    for (field, values) in domains {
        let mut items: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(n, value)| {
                let mut w = item(&format!("{field}-{n}"), field, 0, n as i64);
                w.fields.insert(field.into(), value.clone());
                w
            })
            .collect();
        let mut missing = item(&format!("{field}-missing"), field, 0, 0);
        missing.fields.remove(field);
        items.push(missing);
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: format!("seed-{field}"),
                mode: InsertMode::InsertOnly,
                items: items.clone(),
            })
            .await
            .unwrap();
        let index = if field == "score" {
            "by_score".to_string()
        } else {
            format!("by_{field}")
        };
        for descending in [false, true] {
            let mut expected = items.clone();
            expected.sort_by(|a, b| {
                let order = compare_optional_fields(a.fields.get(field), b.fields.get(field))
                    .unwrap()
                    .then_with(|| a.key.as_bytes().cmp(b.key.as_bytes()));
                if descending { order.reverse() } else { order }
            });
            let request = QueryRequest {
                queue: queue.clone(),
                source: RecordSource::Work,
                index: index.clone(),
                filter: filter(field),
                descending,
                cursor: None,
                max_records: 2,
                max_bytes: 1 << 20,
            };
            assert_eq!(
                query_keys(store, request).await,
                expected.iter().map(|w| w.key.clone()).collect::<Vec<_>>()
            );
        }
        let mut predicates: Vec<_> = values.iter().cloned().map(Predicate::Equal).collect();
        predicates.push(Predicate::In(vec![
            values[0].clone(),
            values.last().unwrap().clone(),
        ]));
        for lower_inclusive in [false, true] {
            for upper_inclusive in [false, true] {
                let lower = values[0].clone();
                let upper = values.last().unwrap().clone();
                predicates.push(Predicate::Range {
                    lower: Some(if lower_inclusive {
                        Bound::Included(lower)
                    } else {
                        Bound::Excluded(lower)
                    }),
                    upper: Some(if upper_inclusive {
                        Bound::Included(upper)
                    } else {
                        Bound::Excluded(upper)
                    }),
                });
            }
        }
        for (n, predicate) in predicates.into_iter().enumerate() {
            let mut selected = filter(field);
            selected.fields.insert(field.into(), predicate.clone());
            let expected: Vec<_> = items
                .iter()
                .filter(|w| predicate.matches(w.fields.get(field)))
                .map(|w| w.key.clone())
                .collect();
            let query = QueryRequest {
                queue: queue.clone(),
                source: RecordSource::Work,
                index: "$key".into(),
                filter: selected.clone(),
                descending: false,
                cursor: None,
                max_records: 2,
                max_bytes: 1 << 20,
            };
            let mut keys = query_keys(store, query).await;
            keys.sort();
            let mut sorted_expected = expected.clone();
            sorted_expected.sort();
            assert_eq!(keys, sorted_expected, "query {field} predicate {n}");
            let mut request = claim(&queue, &format!("predicate-{field}-{n}"), field);
            request.filter = selected;
            let leased = store.claim(request).await.unwrap();
            assert_eq!(
                leased
                    .iter()
                    .map(|w| w.record.work.key.clone())
                    .collect::<Vec<_>>(),
                expected,
                "claim {field} predicate {n}"
            );
            if !leased.is_empty() {
                let mut release = group(&format!("release-{field}-{n}"));
                release.claimed_updates = leased
                    .iter()
                    .map(|w| {
                        let mut update = update(w);
                        update.disposition = Disposition::Release;
                        update
                    })
                    .collect();
                committed(
                    &store
                        .commit(CommitBatch {
                            queue: queue.clone(),
                            groups: vec![release],
                        })
                        .await
                        .unwrap()[0],
                );
            }
        }
    }
    let mut ordered = vec![
        item("order-a", "order", 2, 0),
        item("order-b", "order", 2, 1),
        item("order-c", "order", 9, 2),
        item("order-future", "order", 20, 3),
    ];
    ordered[0].available_at = 100;
    ordered[3].available_at = 300;
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "order-seed".into(),
            mode: InsertMode::InsertOnly,
            items: ordered,
        })
        .await
        .unwrap();
    for (n, order, eligible, expected) in [
        (
            0,
            ClaimOrder::PriorityDescending,
            299,
            vec!["order-c", "order-a", "order-b"],
        ),
        (
            1,
            ClaimOrder::ScheduledAscending,
            300,
            vec!["order-b", "order-c", "order-a", "order-future"],
        ),
    ] {
        let mut request = claim(&queue, &format!("ordered-{n}"), "order");
        request.order = order;
        request.eligible_at = eligible;
        let leased = store.claim(request).await.unwrap();
        assert_eq!(
            leased
                .iter()
                .map(|w| w.record.work.key.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        let mut release = group(&format!("order-release-{n}"));
        release.claimed_updates = leased
            .iter()
            .map(|w| {
                let mut u = update(w);
                u.disposition = Disposition::Release;
                u
            })
            .collect();
        committed(
            &store
                .commit(CommitBatch {
                    queue: queue.clone(),
                    groups: vec![release],
                })
                .await
                .unwrap()[0],
        );
    }
    let mut states = group("state-page-seed");
    states.state_updates = (0..5)
        .map(|n| StateUpdate {
            collection: "instances".into(),
            key: format!("state-{n}"),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: BTreeMap::from([(
                    "run".into(),
                    FieldValue::Text(format!("run-{}", 4 - n)),
                )]),
                value: vec![n].into(),
            },
        })
        .collect();
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![states],
            })
            .await
            .unwrap()[0],
    );
    let keys = query_keys(
        store,
        QueryRequest {
            queue,
            source: RecordSource::State("instances".into()),
            index: "by_run".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 2,
            max_bytes: 1 << 20,
        },
    )
    .await;
    assert_eq!(
        keys,
        vec!["state-4", "state-3", "state-2", "state-1", "state-0"]
    );
}
async fn query_keys(store: &dyn QueueStore, mut request: QueryRequest) -> Vec<String> {
    let mut keys = vec![];
    loop {
        let page = store.query(request.clone()).await.unwrap();
        for record in page.records {
            keys.push(match record {
                QueryRecord::Work(w) => w.work.key,
                QueryRecord::State(s) => s.key,
            });
        }
        if page.cursor.is_none() {
            break;
        }
        request.cursor = page.cursor;
        assert!(keys.len() < 100_000, "cursor must advance");
    }
    keys
}

pub async fn ordered_groups_sparse_stats_and_replay_precedence(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    clock.set(0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let mut sparse = item("sparse", "groups", 0, 0);
    sparse.fields.remove("score");
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "groups-seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![
                item("shared", "groups", 0, 2),
                sparse,
                item("early", "groups", 0, 1),
            ],
        })
        .await
        .unwrap();
    let mut valid = group("group-first");
    valid.work_updates.push(UnclaimedUpdate {
        key: "shared".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(1),
            ..WorkPatch::default()
        }),
    });
    let mut stale = valid.clone();
    stale.request_id = "group-stale".into();
    let mut final_group = valid.clone();
    final_group.request_id = "group-final".into();
    final_group.work_updates[0].expect = StateExpect::Version(2);
    final_group.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
        priority: Some(2),
        ..WorkPatch::default()
    });
    let receipts = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![valid.clone(), stale.clone(), final_group.clone()],
        })
        .await
        .unwrap();
    committed(&receipts[0]);
    assert_eq!(
        receipts[1].outcome,
        GroupOutcome::Rejected(Rejection::VersionConflict)
    );
    committed(&receipts[2]);
    let replay = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![final_group, stale, valid],
        })
        .await
        .unwrap();
    assert_eq!(
        replay,
        vec![
            receipts[2].clone(),
            receipts[1].clone(),
            receipts[0].clone()
        ]
    );
    let stats = store
        .stats(StatsRequest {
            queue: queue.clone(),
            filter: filter("groups"),
            group_by: vec!["score".into(), "kind".into()],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        stats
            .groups
            .iter()
            .map(|g| g.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            None,
            Some(FieldValue::Integer(1)),
            Some(FieldValue::Integer(2))
        ]
    );
    for partial in [true, false] {
        clock.set(0);
        let kind = if partial {
            "partial-growth"
        } else {
            "expired-growth"
        };
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: format!("seed-{kind}"),
                mode: InsertMode::InsertOnly,
                items: vec![
                    item(&format!("{kind}-a"), kind, 0, 0),
                    item(&format!("{kind}-b"), kind, 0, 1),
                ],
            })
            .await
            .unwrap();
        let mut request = claim(&queue, &format!("claim-{kind}"), kind);
        request.max_bytes = 4096;
        let selected = store.claim(request.clone()).await.unwrap();
        assert_eq!(selected.len(), 2);
        let mut grow = group(&format!("grow-{kind}"));
        grow.claimed_updates = selected
            .iter()
            .enumerate()
            .map(|(n, w)| {
                let mut u = update(w);
                u.disposition = if partial && n == 0 {
                    Disposition::Complete
                } else {
                    Disposition::Retain { renew_nanos: None }
                };
                if n == 1 {
                    u.patch.body = Some(vec![5; 8192].into());
                }
                u
            })
            .collect();
        committed(
            &store
                .commit(CommitBatch {
                    queue: queue.clone(),
                    groups: vec![grow],
                })
                .await
                .unwrap()[0],
        );
        if partial {
            assert_eq!(store.claim(request).await, Err(QueueError::ClaimExpired));
        } else {
            assert_eq!(
                store.claim(request.clone()).await,
                Err(QueueError::TooLarge)
            );
            clock.set(1_000_000_000);
            assert_eq!(store.claim(request).await, Err(QueueError::ClaimExpired));
        }
    }
    clock.set(0);
}

pub async fn unrestricted_names_obey_response_bounds(store: &dyn QueueStore, queue: QueueAddress) {
    let name = "n".repeat(32768);
    let collection = "c".repeat(32768);
    let mut def = schema();
    def.work.fields.insert(name.clone(), FieldType::Integer);
    let long_index = "i".repeat(32768);
    def.work
        .indexes
        .insert(long_index.clone(), vec!["target".into()]);
    def.work.unique_indexes.insert(long_index);
    def.state.insert(
        collection.clone(),
        CollectionDefinition {
            additional_field_prefixes: BTreeSet::new(),
            fields: BTreeMap::new(),
            indexes: BTreeMap::new(),
            unique_indexes: BTreeSet::new(),
        },
    );
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: def,
        })
        .await
        .unwrap();
    let items = (0..64)
        .map(|n| {
            let mut work = item(&format!("named-{n}"), "names", 0, n);
            work.fields.insert(name.clone(), FieldValue::Integer(n));
            work
        })
        .collect();
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "named-seed".into(),
            mode: InsertMode::InsertOnly,
            items,
        })
        .await
        .unwrap();
    let mut request = claim(&queue, "named-claim", "names");
    request.max_bytes = 1024;
    assert_eq!(
        store.claim(request.clone()).await,
        Err(QueueError::TooLarge)
    );
    request.request_id = "named-selective".into();
    request.fields = Some(BTreeSet::new());
    request.include_body = false;
    let selected = store.claim(request).await.unwrap();
    assert!(!selected.is_empty());
    assert!(selected.iter().all(|w| w.record.work.fields.is_empty()));
    let grouped = store
        .stats(StatsRequest {
            queue: queue.clone(),
            filter: filter("names"),
            group_by: vec![name.clone()],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(grouped.groups.len(), 64);
    assert_eq!(grouped.groups[0].values, vec![Some(FieldValue::Integer(0))]);
    let mut state = group("long-collection");
    state.state_updates.push(StateUpdate {
        collection: collection.clone(),
        key: "state".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Fields::new(),
            value: vec![1].into(),
        },
    });
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![state],
            })
            .await
            .unwrap()[0],
    );
    assert_eq!(
        store
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: collection.clone(),
                keys: vec!["state".into(); 64],
                max_bytes: 1024
            })
            .await,
        Err(QueueError::TooLarge)
    );
    let mut more = group("long-collection-pages");
    more.state_updates = (0..4)
        .map(|n| StateUpdate {
            collection: collection.clone(),
            key: format!("page-{n}"),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Fields::new(),
                value: vec![n].into(),
            },
        })
        .collect();
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![more],
            })
            .await
            .unwrap()[0],
    );
    let keys = query_keys(
        store,
        QueryRequest {
            queue,
            source: RecordSource::State(collection),
            index: "$key".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 10,
            max_bytes: 65536,
        },
    )
    .await;
    assert_eq!(keys, vec!["page-0", "page-1", "page-2", "page-3", "state"]);
}

/// Fixture-independent acknowledged-write payload for adapter process-loss tests.
/// The fixture must kill the writer without calling shutdown, then open a fresh
/// process and invoke `verify_process_loss`; neither function weakens semantics.
pub async fn prepare_process_loss(
    store: &dyn QueueStore,
    queue: QueueAddress,
) -> (CommitBatch, Vec<CommitReceipt>) {
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let seed = InsertRequest {
        queue: queue.clone(),
        request_id: "crash-seed".into(),
        mode: InsertMode::InsertOnly,
        items: vec![item("crash-source", "source", 0, 0)],
    };
    assert!(matches!(
        store.insert(seed).await.unwrap()[0],
        InsertOutcome::Inserted { .. }
    ));
    let selected = store
        .claim(claim(&queue, "crash-claim", "source"))
        .await
        .unwrap();
    assert_eq!(selected.len(), 1);
    let mut transition = group("crash-transition");
    transition.claimed_updates.push(update(&selected[0]));
    transition.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "crash-state".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([("run".into(), FieldValue::Text("crash-run".into()))]),
            value: vec![0, 255, 17].into(),
        },
    });
    transition
        .work_inserts
        .push(item("crash-next", "next", 7, 9));
    let batch = CommitBatch {
        queue,
        groups: vec![transition],
    };
    let receipts = store.commit(batch.clone()).await.unwrap();
    committed(&receipts[0]);
    (batch, receipts)
}

pub async fn verify_process_loss(
    store: &dyn QueueStore,
    acknowledged: (CommitBatch, Vec<CommitReceipt>),
) {
    let (batch, acknowledged_receipts) = acknowledged;
    let queue = batch.queue.clone();
    let state = store
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["crash-state".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[0, 255, 17]);
    let receipts = store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["crash-transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(
        receipts,
        acknowledged_receipts
            .iter()
            .cloned()
            .map(ReceiptLookup::Found)
            .collect::<Vec<_>>()
    );
    let ReceiptLookup::Found(original) = &receipts[0] else {
        panic!("acknowledged receipt missing: {receipts:?}")
    };
    committed(original);
    assert_eq!(store.commit(batch).await.unwrap(), vec![original.clone()]);
    assert_eq!(
        store.claim(claim(&queue, "crash-claim", "source")).await,
        Err(QueueError::ClaimExpired)
    );
    assert!(
        store
            .claim(claim(&queue, "crash-source-new", "source"))
            .await
            .unwrap()
            .is_empty()
    );
    let next = store
        .claim(claim(&queue, "crash-next-new", "next"))
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].record.work.key, "crash-next");
    assert_eq!(next[0].record.work.body.as_ref(), &[0, 1, 255]);
    let stats = store
        .stats(StatsRequest {
            queue: queue.clone(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(stats.totals.completed, 1);
    assert_eq!(stats.totals.leased, 1);
    let mut changed = group("crash-transition");
    changed.work_inserts.push(item("wrong-next", "next", 0, 0));
    let conflict = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![changed],
        })
        .await
        .unwrap();
    assert!(matches!(conflict[0].outcome, GroupOutcome::RequestConflict));
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue,
                request_ids: vec!["crash-transition".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        receipts
    );
}

/// Insert-vector authority must account for accepted earlier rekeys and rejected
/// siblings without conflating independent per-item outcomes.
pub async fn ordered_insert_uniqueness(store: &dyn QueueStore, queue: QueueAddress) {
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    let target = |key: &str, value: &str| {
        let mut work = item(key, "unique-vector", 0, 0);
        work.fields
            .insert("target".into(), FieldValue::Text(value.into()));
        work
    };
    let seed = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "vector-seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![target("a", "x"), target("external", "z")],
        })
        .await
        .unwrap();
    assert!(
        seed.iter()
            .all(|o| matches!(o, InsertOutcome::Inserted { .. }))
    );
    let request = InsertRequest {
        queue: queue.clone(),
        request_id: "vector-upsert".into(),
        mode: InsertMode::Upsert,
        items: vec![
            target("a", "y"),
            target("b", "x"),
            target("c", "y"),
            target("d", "z"),
            target("e", "free"),
        ],
    };
    let result = store.insert(request.clone()).await.unwrap();
    assert!(matches!(result[0], InsertOutcome::Replaced { .. }));
    assert!(matches!(result[1], InsertOutcome::Inserted { .. }));
    for outcome in &result[2..4] {
        assert!(matches!(
            outcome,
            InsertOutcome::Rejected {
                reason: Rejection::UniqueConflict,
                ..
            }
        ));
    }
    assert!(matches!(result[4], InsertOutcome::Inserted { .. }));
    assert_eq!(store.insert(request).await.unwrap(), result);
    let selected = store
        .claim(claim(&queue, "vector-claim", "unique-vector"))
        .await
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|w| w.record.work.key.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["a", "b", "e", "external"])
    );
    assert_eq!(
        selected
            .iter()
            .find(|w| w.record.work.key == "a")
            .unwrap()
            .record
            .work
            .fields["target"],
        FieldValue::Text("y".into())
    );
}

pub async fn shrinking_upsert_index_amplification(store: &dyn QueueStore, queue: QueueAddress) {
    let mut definition = schema();
    for n in 0..32 {
        let name = format!("unique-target-{n}");
        definition
            .work
            .indexes
            .insert(name.clone(), vec!["target".into()]);
        definition.work.unique_indexes.insert(name);
    }
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let mut old = item("shrinking", "shrink", 0, 0);
    old.fields
        .insert("target".into(), FieldValue::Text("\0".repeat(2048)));
    let inserted = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "large-index-seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![old.clone()],
        })
        .await
        .unwrap();
    assert!(matches!(inserted[0], InsertOutcome::Inserted { .. }));
    let replacement = item("shrinking", "shrink", 0, 0);
    let replaced = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "small-index-upsert".into(),
            mode: InsertMode::Upsert,
            items: vec![replacement],
        })
        .await
        .unwrap();
    assert!(matches!(replaced[0], InsertOutcome::Replaced { .. }));
    old.key = "released-owner".into();
    let release = store
        .insert(InsertRequest {
            queue,
            request_id: "released-index-insert".into(),
            mode: InsertMode::InsertOnly,
            items: vec![old],
        })
        .await
        .unwrap();
    assert!(matches!(release[0], InsertOutcome::Inserted { .. }));
}

/// Literal full-name property-bag indexes, with exact type/equality semantics.
pub async fn indexed_additional_attributes(store: &dyn QueueStore, queue: QueueAddress) {
    use fireweed_core::queue_json::{
        current_json_equality_encoding, encode_json_equality_for, json_attribute_name,
    };
    let mut definition = schema();
    definition.work.additional_field_prefixes =
        BTreeSet::from(["/payload/".into(), "/gates/".into()]);
    definition
        .state
        .get_mut("instances")
        .unwrap()
        .additional_field_prefixes
        .insert("/meta/".into());
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let pin = current_json_equality_encoding();
    let literal = json_attribute_name("/payload", "route/~tier");
    let encoded =
        encode_json_equality_for(&serde_json::json!({"tier":[1,null,"λ"]}), pin, 1 << 20).unwrap();
    let long = format!("/payload/{}", "n\0λ/".repeat(8192));
    let mut a = item("a", "attributes", 0, 0);
    a.fields
        .insert(literal.clone(), FieldValue::Bytes(encoded.clone()));
    a.fields
        .insert("/payload/mixed".into(), FieldValue::Integer(7));
    a.fields
        .insert("/gates/rule/~1".into(), FieldValue::Boolean(true));
    a.fields
        .insert(long.clone(), FieldValue::Text("long value".into()));
    let mut b = item("b", "attributes", 1, 1);
    b.fields.insert(
        literal.clone(),
        FieldValue::Bytes(encode_json_equality_for(&serde_json::json!(null), pin, 1024).unwrap()),
    );
    b.fields
        .insert("/payload/mixed".into(), FieldValue::Float(7.0));
    let mut c = item("c", "attributes", 2, 2);
    c.fields
        .insert("/payload/mixed".into(), FieldValue::Boolean(true));
    let d = item("d", "attributes", 3, 3);
    let outcomes = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "attribute-seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![a, b, c, d],
        })
        .await
        .unwrap();
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, InsertOutcome::Inserted { .. }))
    );
    let select = |name: String, predicate: Predicate| QueryRequest {
        queue: queue.clone(),
        source: RecordSource::Work,
        index: "$key".into(),
        filter: Filter {
            fields: BTreeMap::from([(name, predicate)]),
            ..Filter::default()
        },
        descending: false,
        cursor: None,
        max_records: 10,
        max_bytes: 1 << 20,
    };
    assert_eq!(
        query_keys(
            store,
            select(
                literal.clone(),
                Predicate::Equal(FieldValue::Bytes(encoded))
            )
        )
        .await,
        vec!["a"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                literal.clone(),
                Predicate::Equal(FieldValue::Bytes(
                    encode_json_equality_for(&serde_json::json!(null), pin, 1024).unwrap()
                ))
            )
        )
        .await,
        vec!["b"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/gates/rule/~1".into(),
                Predicate::Equal(FieldValue::Boolean(true))
            )
        )
        .await,
        vec!["a"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/mixed".into(),
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Integer(0))),
                    upper: None
                }
            )
        )
        .await,
        vec!["a"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/mixed".into(),
                Predicate::Equal(FieldValue::Float(7.0))
            )
        )
        .await,
        vec!["b"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                long.clone(),
                Predicate::Equal(FieldValue::Text("long value".into()))
            )
        )
        .await,
        vec!["a"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/mixed".into(),
                Predicate::In(vec![FieldValue::Float(7.0), FieldValue::Boolean(true)])
            )
        )
        .await,
        vec!["b", "c"]
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/mixed".into(),
                Predicate::Range {
                    lower: None,
                    upper: Some(Bound::Excluded(FieldValue::Integer(8)))
                }
            )
        )
        .await,
        vec!["a"]
    );
    let groups = store
        .stats(StatsRequest {
            queue: queue.clone(),
            filter: Filter::default(),
            group_by: vec!["/payload/mixed".into()],
            eligible_at: 0,
        })
        .await
        .unwrap();
    assert_eq!(groups.totals.pending, 4);
    assert_eq!(groups.groups.len(), 4);
    assert_eq!(
        groups
            .groups
            .iter()
            .map(|g| g.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![None],
            vec![Some(FieldValue::Integer(7))],
            vec![Some(FieldValue::Float(7.0))],
            vec![Some(FieldValue::Boolean(true))]
        ]
    );
    assert!(groups.groups.iter().all(|g| g.counts.pending == 1));
    let mut tiny = claim(&queue, "attribute-tiny", "attributes");
    tiny.filter = Filter {
        keys: Some(BTreeSet::from(["a".into()])),
        ..Filter::default()
    };
    tiny.max_bytes = 4096;
    assert_eq!(store.claim(tiny.clone()).await, Err(QueueError::TooLarge));
    tiny.request_id = "attribute-selective".into();
    tiny.fields = Some(BTreeSet::from(["/gates/rule/~1".into()]));
    tiny.include_body = false;
    let leased = store.claim(tiny).await.unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(
        leased[0].record.work.fields,
        BTreeMap::from([("/gates/rule/~1".into(), FieldValue::Boolean(true))])
    );
    let mut patch = group("attribute-patch");
    patch.claimed_updates.push(ClaimedUpdate {
        key: "a".into(),
        lease: leased[0].lease.clone(),
        expected_version: leased[0].record.version,
        patch: WorkPatch {
            fields: BTreeMap::from([
                (literal.clone(), None),
                (
                    "/payload/new".into(),
                    Some(FieldValue::Text("updated".into())),
                ),
            ]),
            ..WorkPatch::default()
        },
        disposition: Disposition::Release,
    });
    patch.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "attributes".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([("/meta/literal".into(), FieldValue::Bytes(vec![0, 255]))]),
            value: vec![9].into(),
        },
    });
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![patch],
            })
            .await
            .unwrap()[0],
    );
    assert_eq!(
        query_keys(
            store,
            select(
                literal,
                Predicate::Equal(FieldValue::Bytes(
                    encode_json_equality_for(
                        &serde_json::json!({"tier":[1,null,"λ"]}),
                        pin,
                        1 << 20
                    )
                    .unwrap()
                ))
            )
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/new".into(),
                Predicate::Equal(FieldValue::Text("updated".into()))
            )
        )
        .await,
        vec!["a"]
    );
    let state = store
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["attributes".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(
        state[0].as_ref().unwrap().fields["/meta/literal"],
        FieldValue::Bytes(vec![0, 255])
    );
    let mut state_query = select(
        "/meta/literal".into(),
        Predicate::Equal(FieldValue::Bytes(vec![0, 255])),
    );
    state_query.source = RecordSource::State("instances".into());
    assert_eq!(query_keys(store, state_query).await, vec!["attributes"]);
    let mut replacement = item("a", "attributes", 0, 0);
    replacement
        .fields
        .insert("/payload/new".into(), FieldValue::Integer(11));
    let replacement_outcomes = store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "attribute-replace".into(),
            mode: InsertMode::Upsert,
            items: vec![replacement],
        })
        .await
        .unwrap();
    let InsertOutcome::Replaced { version, .. } = replacement_outcomes[0] else {
        panic!("expected replacement")
    };
    assert_eq!(
        query_keys(
            store,
            select(
                long,
                Predicate::Equal(FieldValue::Text("long value".into()))
            )
        )
        .await,
        Vec::<String>::new()
    );
    let mut deletion = group("attribute-delete");
    deletion.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(version),
        change: UnclaimedChange::Delete,
    });
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![deletion],
            })
            .await
            .unwrap()[0],
    );
    assert_eq!(
        query_keys(
            store,
            select(
                "/payload/new".into(),
                Predicate::Equal(FieldValue::Integer(11))
            )
        )
        .await,
        Vec::<String>::new()
    );
    let mut invalid = item("invalid", "attributes", 0, 0);
    invalid
        .fields
        .insert("/undeclared/value".into(), FieldValue::Text("no".into()));
    assert!(matches!(
        store
            .insert(InsertRequest {
                queue,
                request_id: "attribute-invalid".into(),
                mode: InsertMode::InsertOnly,
                items: vec![invalid]
            })
            .await,
        Err(QueueError::Invalid(_))
    ));
}
