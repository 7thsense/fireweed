//! Mandatory API-007 scenarios shared by every adapter, without capability skips.
//! This is the first operational slice; crash/cutover and qualification workloads
//! extend this same suite before production activation.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_contract::QueueStore;
use std::collections::{BTreeMap, BTreeSet};

/// Receipt lookups for ids issued at time zero, which is what the shared
/// request builders stamp.
pub fn read_ids<S: Into<String>>(ids: impl IntoIterator<Item = S>) -> Vec<ReceiptReadId> {
    ids.into_iter()
        .map(|request_id| ReceiptReadId {
            request_id: request_id.into(),
            issued_at: 0,
        })
        .collect()
}
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
        retention: fireweed_core::queue_contract::QueueRetention {
            request_id_nanos: 86_400_000_000_000,
            deleted_keys: fireweed_core::queue_contract::DeletedKeyPolicy::Reusable,
        },
    }
}
/// The shared schema with other retention windows.
pub fn schema_with(retention: QueueRetention) -> QueueSchema {
    QueueSchema {
        retention,
        ..schema()
    }
}
pub fn item(key: &str, kind: &str, priority: i64, score: i64) -> NewWork {
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
pub fn group(id: &str) -> CommitGroup {
    CommitGroup {
        administration: None,
        request_id: id.into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
        issued_at: 0,
    }
}
pub fn filter(kind: &str) -> Filter {
    Filter {
        fields: BTreeMap::from([(
            "kind".into(),
            Predicate::Equal(FieldValue::Text(kind.into())),
        )]),
        ..Filter::default()
    }
}
pub fn claim(queue: &QueueAddress, id: &str, kind: &str) -> ClaimRequest {
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
        issued_at: 0,
    }
}
pub fn update(work: &ClaimedWork) -> ClaimedUpdate {
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

/// Returns the original checks-only request for later expiry/rebuild replay stimuli.
pub async fn run(store: &dyn QueueStore, queue: QueueAddress) -> CommitBatch {
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
        issued_at: 0,
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
    let mut checks = group("claim-and-absence-checks");
    checks.claimed_updates = selected
        .iter()
        .map(|work| {
            let mut check = update(work);
            check.disposition = Disposition::Check;
            check
        })
        .collect();
    checks.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "instance".into(),
        expect: StateExpect::Absent,
        change: StateChange::Check,
    });
    assert_eq!(
        store
            .preview(PreviewRequest {
                queue: queue.clone(),
                group: checks.clone(),
            })
            .await
            .unwrap()
            .outcome,
        PreviewOutcome::Committed { versions: vec![] }
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![ReceiptReadId {
                    request_id: checks.request_id.clone(),
                    issued_at: checks.issued_at
                }],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Absent]
    );
    let checked = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![checks.clone()],
        })
        .await
        .unwrap();
    assert_eq!(
        checked[0].outcome,
        GroupOutcome::Committed { versions: vec![] }
    );
    assert_eq!(store.claim(request.clone()).await.unwrap(), selected);
    let mut renewal = group("renew-first-claim");
    renewal.claimed_updates = selected
        .iter()
        .map(|work| {
            let mut change = update(work);
            change.disposition = Disposition::Retain {
                renew_nanos: Some(2_000_000_000),
            };
            change
        })
        .collect();
    committed(
        &store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![renewal],
            })
            .await
            .unwrap()[0],
    );
    let renewed = store.claim(request.clone()).await.unwrap();
    assert_eq!(renewed.len(), selected.len());
    for (current, original) in renewed.iter().zip(&selected) {
        assert_eq!(current.lease.token, original.lease.token);
        assert!(current.record.version > original.record.version);
        assert!(current.record.lease_expires_at > original.record.lease_expires_at);
    }
    let selected = renewed;
    let mut transition = group("transition");
    transition.claimed_updates.push(update(&selected[0]));
    let mut guard = update(&selected[1]);
    guard.disposition = Disposition::Check;
    transition.claimed_updates.push(guard);
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
    let GroupOutcome::Committed { versions } = &receipt[0].outcome else {
        unreachable!()
    };
    assert_eq!(
        versions.iter().map(|v| v.key.as_str()).collect::<Vec<_>>(),
        vec!["low", "instance", "next"]
    );
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
    // Retained success is authoritative after both the checked claim version
    // and the checked absent state have changed.
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![checks.clone()],
            })
            .await
            .unwrap(),
        checked
    );
    let mut state_checks = group("present-and-absent-state-checks");
    state_checks.state_updates = vec![
        StateUpdate {
            collection: "instances".into(),
            key: "instance".into(),
            expect: StateExpect::Version(reads[0].as_ref().unwrap().version),
            change: StateChange::Check,
        },
        StateUpdate {
            collection: "instances".into(),
            key: "missing".into(),
            expect: StateExpect::Absent,
            change: StateChange::Check,
        },
    ];
    let state_checked = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![state_checks],
        })
        .await
        .unwrap();
    assert_eq!(
        state_checked[0].outcome,
        GroupOutcome::Committed { versions: vec![] }
    );
    assert_eq!(
        store
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: "instances".into(),
                keys: vec!["instance".into(), "missing".into(), "instance".into()],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap(),
        reads
    );
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
            request_ids: vec![
                ReceiptReadId {
                    request_id: "transition".into(),
                    issued_at: 0,
                },
                ReceiptReadId {
                    request_id: "absent".into(),
                    issued_at: 0,
                },
                ReceiptReadId {
                    request_id: "transition".into(),
                    issued_at: 0,
                },
            ],
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
        change: StateChange::Check,
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
            buckets: None,
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
    assert_eq!(
        next.iter()
            .map(|w| (w.record.work.key.as_str(), w.record.insertion_sequence))
            .collect::<Vec<_>>(),
        vec![("next", 4), ("independent", 5)]
    );
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
    assert_eq!(high.version, selected[1].record.version);
    assert_eq!(high.lease_expires_at, selected[1].record.lease_expires_at);
    assert_eq!(
        high.insertion_sequence,
        selected[1].record.insertion_sequence
    );
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
            issued_at: 0,
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
            issued_at: 0,
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
    CommitBatch {
        queue,
        groups: vec![checks],
    }
}

/// Identical injected ownership clock fixture for every new binding adapter.
#[derive(Default)]
pub struct TestClock(std::sync::atomic::AtomicI64);
impl TestClock {
    pub fn set(&self, nanos: i64) {
        self.0.store(nanos, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn get(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl fireweed_engine::Clock for TestClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}

/// Ownership moves between the two stores. A backend with one owner per queue
/// returns `Busy` from the store that does not own it; this step makes the other
/// store the owner. A backend without an owner limit never needs it.
pub type Handoff<'a> =
    &'a (dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Sync);

pub async fn competing_claims(a: &dyn QueueStore, b: &dyn QueueStore, queue: QueueAddress) {
    competing_claims_with_handoff(a, b, queue, &|| Box::pin(async {})).await
}

/// Concurrent claims through one store are disjoint. A second store either claims
/// disjoint work too or returns `Busy` with no effects, and after `handoff` it
/// continues with disjoint work. `handoff` is called once more at the end so the
/// first store can resume.
pub async fn competing_claims_with_handoff(
    a: &dyn QueueStore,
    b: &dyn QueueStore,
    queue: QueueAddress,
    handoff: Handoff<'_>,
) {
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
        issued_at: 0,
    })
    .await
    .unwrap();
    let mut claimed = Vec::new();
    // Through one owner the loop plans concurrent claims in order: disjoint.
    let mut first = claim(&queue, "first-race", "race");
    first.max_records = 25;
    let mut second = first.clone();
    second.request_id = "second-race".into();
    let (first, second) = tokio::join!(a.claim(first), a.claim(second));
    for result in [first, second] {
        let result = result.unwrap();
        assert_eq!(result.len(), 25);
        claimed.extend(result);
    }
    // A second store: disjoint work or Busy without effects.
    let mut left = claim(&queue, "left-race", "race");
    left.max_records = 25;
    let mut right = left.clone();
    right.request_id = "right-race".into();
    let (left, right_result) = tokio::join!(a.claim(left), b.claim(right.clone()));
    let left = left.unwrap();
    assert_eq!(left.len(), 25);
    claimed.extend(left);
    let right = match right_result {
        Ok(right) => right,
        Err(QueueError::Busy) => {
            handoff().await;
            b.claim(right).await.unwrap()
        }
        Err(error) => panic!("second store: {error:?}"),
    };
    assert_eq!(right.len(), 25);
    claimed.extend(right);
    let keys: BTreeSet<_> = claimed.iter().map(|w| w.record.work.key.clone()).collect();
    assert_eq!(keys.len(), 100);
    handoff().await;
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
            issued_at: 0,
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
    // Expired delivery can be reclaimed even beyond max_attempts: that bound
    // classifies RetryAt commits; it is not a claim cap.
    clock.set(third[0].record.lease_expires_at.unwrap());
    assert_eq!(
        store.claim(claim(&queue, "third-claim", "expiry")).await,
        Err(QueueError::ClaimExpired)
    );
    let mut selective = claim(&queue, "fourth-claim", "expiry");
    selective.fields = Some(BTreeSet::new());
    selective.include_body = false;
    let fourth = store.claim(selective).await.unwrap();
    assert_eq!(fourth.len(), 1);
    assert_eq!(fourth[0].record.attempts, 4);
    assert_eq!(
        fourth[0].record.insertion_sequence,
        third[0].record.insertion_sequence
    );
    assert!(fourth[0].record.work.fields.is_empty());
    assert!(fourth[0].record.work.body.is_empty());
    assert_ne!(fourth[0].lease, third[0].lease);
    let mut operator = group("operator-complete");
    operator.work_updates.push(UnclaimedUpdate {
        key: "expiring".into(),
        expect: StateExpect::Version(fourth[0].record.version),
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
            buckets: None,
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
            issued_at: 0,
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
                issued_at: 0,
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
        // Every domain's values are ascending, so the closed range over its
        // ends is the union of all typed ranges and must equal presence.
        let union = Predicate::Range {
            lower: Some(Bound::Included(values[0].clone())),
            upper: Some(Bound::Included(values.last().unwrap().clone())),
        };
        let keys_matching = |predicate: &Predicate| -> Vec<String> {
            items
                .iter()
                .filter(|w| predicate.matches(w.fields.get(field)))
                .map(|w| w.key.clone())
                .collect()
        };
        assert_eq!(keys_matching(&Predicate::Present), keys_matching(&union));
        assert_eq!(
            keys_matching(&Predicate::Absent),
            vec![format!("{field}-missing")]
        );
        predicates.push(Predicate::Present);
        predicates.push(Predicate::Absent);
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
            issued_at: 0,
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
/// Presence and absence of declared fields across query, claim and stats, kept
/// in step with a model through patches, replacements, deletion and reinsertion.
pub async fn presence_and_absence_predicates(store: &dyn QueueStore, queue: QueueAddress) {
    let mut definition = schema();
    for (name, ty) in [
        ("note", FieldType::Text),
        ("flag", FieldType::Boolean),
        ("count", FieldType::Integer),
        ("blob", FieldType::Bytes),
    ] {
        definition.work.fields.insert(name.into(), ty);
    }
    definition
        .state
        .get_mut("instances")
        .unwrap()
        .fields
        .insert("tag".into(), FieldType::Text);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let work = |key: &str,
                priority: i64,
                available_at: i64,
                score: i64,
                extra: Vec<(&str, FieldValue)>| {
        let mut w = item(key, "presence", priority, score);
        w.available_at = available_at;
        w.fields
            .extend(extra.into_iter().map(|(name, v)| (name.to_string(), v)));
        w
    };
    let seed = vec![
        // Zero-like values are present.
        work(
            "a",
            3,
            0,
            1,
            vec![
                ("note", FieldValue::Text(String::new())),
                ("flag", FieldValue::Boolean(false)),
                ("count", FieldValue::Integer(0)),
                ("blob", FieldValue::Bytes(vec![])),
            ],
        ),
        work("b", 1, 20, 2, vec![]),
        work(
            "c",
            2,
            10,
            3,
            vec![
                ("note", FieldValue::Text("x".into())),
                ("flag", FieldValue::Boolean(true)),
            ],
        ),
        work(
            "d",
            1,
            0,
            4,
            vec![
                ("note", FieldValue::Text("y".into())),
                ("count", FieldValue::Integer(5)),
            ],
        ),
        work("e", 5, 0, 5, vec![]),
    ];
    let mut ctx = PresenceContext {
        store,
        queue: queue.clone(),
        model: seed.iter().map(|w| (w.key.clone(), w.clone())).collect(),
        serial: 0,
    };
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "presence-seed".into(),
            mode: InsertMode::InsertOnly,
            items: seed,
            issued_at: 0,
        })
        .await
        .unwrap();
    let only = |entries: Vec<(&str, Predicate)>| -> BTreeMap<String, Predicate> {
        entries
            .into_iter()
            .map(|(name, predicate)| (name.to_string(), predicate))
            .collect()
    };
    let one_field_sets = || {
        let mut sets = vec![];
        for name in ["note", "flag", "count", "blob"] {
            sets.push(only(vec![(name, Predicate::Present)]));
            sets.push(only(vec![(name, Predicate::Absent)]));
        }
        sets
    };
    let conjunctions = || {
        vec![
            only(vec![
                ("note", Predicate::Present),
                ("count", Predicate::Absent),
            ]),
            only(vec![
                ("note", Predicate::Absent),
                ("flag", Predicate::Present),
            ]),
            only(vec![
                ("flag", Predicate::Absent),
                (
                    "score",
                    Predicate::Range {
                        lower: Some(Bound::Included(FieldValue::Integer(2))),
                        upper: None,
                    },
                ),
            ]),
            only(vec![
                ("note", Predicate::Present),
                ("flag", Predicate::Equal(FieldValue::Boolean(true))),
            ]),
            only(vec![
                ("count", Predicate::Present),
                (
                    "score",
                    Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(4)]),
                ),
            ]),
            only(vec![
                ("note", Predicate::Absent),
                ("count", Predicate::Absent),
                ("flag", Predicate::Absent),
                ("blob", Predicate::Absent),
            ]),
        ]
    };
    let all_sets = || {
        let mut sets = one_field_sets();
        sets.extend(conjunctions());
        sets
    };
    let ordered_sets = || {
        vec![
            only(vec![("count", Predicate::Absent)]),
            only(vec![("note", Predicate::Present)]),
            only(vec![("note", Predicate::Absent)]),
        ]
    };
    let orders = [
        ClaimOrder::PriorityAscending,
        ClaimOrder::PriorityDescending,
        ClaimOrder::ScheduledAscending,
    ];
    // Stage 0: never set, plus the zero-like present values.
    assert_eq!(
        ctx.expected(&only(vec![("count", Predicate::Absent)])),
        vec!["b", "c", "e"]
    );
    assert_eq!(
        ctx.expected(&only(vec![("note", Predicate::Present)])),
        vec!["a", "c", "d"]
    );
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    for set in ordered_sets() {
        for order in orders {
            ctx.check_claim_order(&set, order).await;
        }
    }
    ctx.check_stats_shapes().await;
    // Stage 1: set a missing field, remove a present one, by patch.
    let mut patch = group("presence-patch");
    for (key, field, value) in [
        ("b", "note", Some(FieldValue::Text("later".into()))),
        ("a", "note", None),
        ("d", "count", None),
        ("c", "count", Some(FieldValue::Integer(9))),
    ] {
        patch.work_updates.push(UnclaimedUpdate {
            key: key.into(),
            expect: StateExpect::Version(ctx.version(key).await),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([(field.to_string(), value.clone())]),
                ..WorkPatch::default()
            }),
        });
        let w = ctx.model.get_mut(key).unwrap();
        match value {
            Some(value) => w.fields.insert(field.into(), value),
            None => w.fields.remove(field),
        };
    }
    ctx.commit(patch).await;
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    ctx.check_claim_order(
        &only(vec![("count", Predicate::Absent)]),
        ClaimOrder::PriorityDescending,
    )
    .await;
    ctx.check_stats_shapes().await;
    // Stage 2: replacement by upsert drops and adds fields.
    let replaced = vec![
        work("c", 2, 10, 3, vec![("blob", FieldValue::Bytes(vec![1]))]),
        work(
            "e",
            5,
            0,
            5,
            vec![
                ("note", FieldValue::Text("e".into())),
                ("flag", FieldValue::Boolean(false)),
                ("count", FieldValue::Integer(1)),
                ("blob", FieldValue::Bytes(vec![])),
            ],
        ),
        work("f", 4, 0, 6, vec![]),
    ];
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "presence-upsert".into(),
            mode: InsertMode::Upsert,
            items: replaced.clone(),
            issued_at: 0,
        })
        .await
        .unwrap();
    for w in replaced {
        ctx.model.insert(w.key.clone(), w);
    }
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    ctx.check_stats_shapes().await;
    // Stage 3: replacement by a version-fenced commit.
    let replacement = work("a", 3, 0, 1, vec![("count", FieldValue::Integer(3))]);
    let mut replace = group("presence-replace");
    replace.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(ctx.version("a").await),
        change: UnclaimedChange::Replace(replacement.clone()),
    });
    ctx.commit(replace).await;
    ctx.model.insert("a".into(), replacement);
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    ctx.check_claim_order(
        &only(vec![("note", Predicate::Absent)]),
        ClaimOrder::ScheduledAscending,
    )
    .await;
    // Stage 4: delete, then reinsert the same key with different fields.
    let mut selection = claim(&queue, "presence-delete-claim", "presence");
    selection.filter.keys = Some(BTreeSet::from(["d".into()]));
    selection.eligible_at = 100;
    let claimed = store.claim(selection).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let mut delete = group("presence-delete");
    delete.claimed_updates = claimed
        .iter()
        .map(|w| {
            let mut u = update(w);
            u.disposition = Disposition::Delete;
            u
        })
        .collect();
    ctx.commit(delete).await;
    ctx.model.remove("d");
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    let reinserted = work("d", 1, 0, 4, vec![("count", FieldValue::Integer(1))]);
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "presence-reinsert".into(),
            mode: InsertMode::InsertOnly,
            items: vec![reinserted.clone()],
            issued_at: 0,
        })
        .await
        .unwrap();
    ctx.model.insert("d".into(), reinserted);
    for set in all_sets() {
        ctx.check_reads(&set).await;
    }
    ctx.check_stats_shapes().await;
    ctx.check_state_collection().await;
}
struct PresenceContext<'a> {
    store: &'a dyn QueueStore,
    queue: QueueAddress,
    model: BTreeMap<String, NewWork>,
    serial: u32,
}
impl PresenceContext<'_> {
    fn id(&mut self, what: &str) -> String {
        self.serial += 1;
        format!("presence-{what}-{}", self.serial)
    }
    fn selected(&self, fields: &BTreeMap<String, Predicate>) -> Filter {
        let mut selected = filter("presence");
        selected.fields.extend(fields.clone());
        selected
    }
    /// Keys in ascending order, decided by the shared in-memory predicate oracle.
    fn expected(&self, fields: &BTreeMap<String, Predicate>) -> Vec<String> {
        self.model
            .values()
            .filter(|w| {
                fields
                    .iter()
                    .all(|(name, predicate)| predicate.matches(w.fields.get(name)))
            })
            .map(|w| w.key.clone())
            .collect()
    }
    async fn version(&self, key: &str) -> u64 {
        let page = self
            .store
            .query(QueryRequest {
                queue: self.queue.clone(),
                source: RecordSource::Work,
                index: "$key".into(),
                filter: Filter {
                    keys: Some(BTreeSet::from([key.to_string()])),
                    ..Filter::default()
                },
                descending: false,
                cursor: None,
                max_records: 1,
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        match page.records.into_iter().next() {
            Some(QueryRecord::Work(w)) => w.version,
            other => panic!("expected {key}: {other:?}"),
        }
    }
    async fn commit(&self, group: CommitGroup) {
        committed(
            &self
                .store
                .commit(CommitBatch {
                    queue: self.queue.clone(),
                    groups: vec![group],
                })
                .await
                .unwrap()[0],
        );
    }
    /// Paged queries in both directions (one record per page) and stats totals.
    async fn check_reads(&mut self, fields: &BTreeMap<String, Predicate>) {
        let expected = self.expected(fields);
        let selected = self.selected(fields);
        for descending in [false, true] {
            let keys = query_keys(
                self.store,
                QueryRequest {
                    queue: self.queue.clone(),
                    source: RecordSource::Work,
                    index: "$key".into(),
                    filter: selected.clone(),
                    descending,
                    cursor: None,
                    max_records: 1,
                    max_bytes: 1 << 20,
                },
            )
            .await;
            let mut want = expected.clone();
            if descending {
                want.reverse();
            }
            assert_eq!(keys, want, "query {fields:?} descending {descending}");
        }
        let stats = self
            .store
            .stats(StatsRequest {
                buckets: None,
                queue: self.queue.clone(),
                filter: selected,
                group_by: vec![],
                eligible_at: 0,
            })
            .await
            .unwrap();
        assert_eq!(
            stats.totals.pending,
            expected.len() as u64,
            "stats {fields:?}"
        );
    }
    async fn check_claim_order(&mut self, fields: &BTreeMap<String, Predicate>, order: ClaimOrder) {
        let mut want: Vec<&NewWork> = self
            .model
            .values()
            .filter(|w| {
                fields
                    .iter()
                    .all(|(name, predicate)| predicate.matches(w.fields.get(name)))
            })
            .collect();
        want.sort_by(|a, b| match order {
            ClaimOrder::PriorityAscending => a.priority.cmp(&b.priority),
            ClaimOrder::PriorityDescending => b.priority.cmp(&a.priority),
            ClaimOrder::ScheduledAscending => a
                .available_at
                .cmp(&b.available_at)
                .then(a.priority.cmp(&b.priority)),
        });
        let want: Vec<String> = want.into_iter().map(|w| w.key.clone()).collect();
        let id = self.id("claim");
        let mut request = claim(&self.queue, &id, "presence");
        request.filter = self.selected(fields);
        request.order = order;
        request.eligible_at = 100;
        let leased = self.store.claim(request).await.unwrap();
        assert_eq!(
            leased
                .iter()
                .map(|w| w.record.work.key.clone())
                .collect::<Vec<_>>(),
            want,
            "claim {fields:?} {order:?}"
        );
        if leased.is_empty() {
            return;
        }
        let mut release = group(&self.id("release"));
        release.claimed_updates = leased
            .iter()
            .map(|w| {
                let mut u = update(w);
                u.disposition = Disposition::Release;
                u
            })
            .collect();
        self.commit(release).await;
    }
    /// Grouping and numeric buckets agree with absence: the missing bucket and
    /// the null group count exactly the records the absence predicate selects.
    async fn check_stats_shapes(&self) {
        let missing = BTreeMap::from([("count".to_string(), Predicate::Absent)]);
        let present = BTreeMap::from([("count".to_string(), Predicate::Present)]);
        let absent_count = self.expected(&missing).len() as u64;
        let present_count = self.expected(&present).len() as u64;
        let dimension = NumericBucketDimension {
            field: "count".into(),
            buckets: vec![
                NumericBucket {
                    label: "low".into(),
                    lower_inclusive: FieldValue::Integer(i64::MIN),
                    upper_exclusive: FieldValue::Integer(5),
                },
                NumericBucket {
                    label: "high".into(),
                    lower_inclusive: FieldValue::Integer(5),
                    upper_exclusive: FieldValue::Integer(i64::MAX),
                },
            ],
            missing_label: "none".into(),
        };
        let request =
            |fields: Option<&BTreeMap<String, Predicate>>, group_by: Vec<String>| StatsRequest {
                buckets: Some(dimension.clone()),
                queue: self.queue.clone(),
                filter: fields.map_or_else(|| filter("presence"), |f| self.selected(f)),
                group_by,
                eligible_at: 0,
            };
        let everything = self.store.stats(request(None, vec![])).await.unwrap();
        assert_eq!(everything.buckets.last().unwrap().label, "none");
        assert_eq!(
            everything.buckets.last().unwrap().counts.pending,
            absent_count
        );
        assert_eq!(
            everything.buckets[..2]
                .iter()
                .map(|b| b.counts.pending)
                .sum::<u64>(),
            present_count
        );
        let absent = self
            .store
            .stats(request(Some(&missing), vec!["count".into()]))
            .await
            .unwrap();
        assert_eq!(absent.totals.pending, absent_count);
        assert_eq!(absent.buckets.last().unwrap().counts.pending, absent_count);
        assert!(absent.buckets[..2].iter().all(|b| b.counts.pending == 0));
        if absent_count > 0 {
            assert_eq!(absent.groups.len(), 1);
            assert_eq!(absent.groups[0].values, vec![None]);
            assert_eq!(absent.groups[0].counts.pending, absent_count);
        } else {
            assert!(absent.groups.is_empty());
        }
        let present = self
            .store
            .stats(request(Some(&present), vec!["count".into()]))
            .await
            .unwrap();
        assert_eq!(present.totals.pending, present_count);
        assert_eq!(present.buckets.last().unwrap().counts.pending, 0);
        assert!(present.groups.iter().all(|g| g.values[0].is_some()));
    }
    async fn check_state_collection(&mut self) {
        let mut seed = group(&self.id("state-seed"));
        for (n, (run, tag)) in [
            ("r2", Some("t")),
            ("r1", None),
            ("r0", None),
            ("r3", Some("u")),
        ]
        .into_iter()
        .enumerate()
        {
            let mut fields = BTreeMap::from([("run".to_string(), FieldValue::Text(run.into()))]);
            if let Some(tag) = tag {
                fields.insert("tag".into(), FieldValue::Text(tag.into()));
            }
            seed.state_updates.push(StateUpdate {
                collection: "instances".into(),
                key: format!("s{n}"),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields,
                    value: vec![n as u8].into(),
                },
            });
        }
        self.commit(seed).await;
        let address = self.queue.clone();
        let query = |predicate| QueryRequest {
            queue: address.clone(),
            source: RecordSource::State("instances".into()),
            index: "by_run".into(),
            filter: Filter {
                fields: BTreeMap::from([("tag".to_string(), predicate)]),
                ..Filter::default()
            },
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 1 << 20,
        };
        assert_eq!(
            query_keys(self.store, query(Predicate::Absent)).await,
            vec!["s2", "s1"]
        );
        assert_eq!(
            query_keys(self.store, query(Predicate::Present)).await,
            vec!["s0", "s3"]
        );
        // Drop one tag and add another by replacing the whole state row.
        let page = self
            .store
            .query(QueryRequest {
                filter: Filter {
                    keys: Some(BTreeSet::from(["s0".into(), "s1".into()])),
                    ..Filter::default()
                },
                max_records: 10,
                ..query(Predicate::Present)
            })
            .await
            .unwrap();
        let versions: BTreeMap<String, u64> = page
            .records
            .into_iter()
            .map(|record| match record {
                QueryRecord::State(s) => (s.key, s.version),
                other => panic!("{other:?}"),
            })
            .collect();
        let mut replace = group(&self.id("state-replace"));
        for (key, run, tag) in [("s0", "r2", None), ("s1", "r1", Some("v"))] {
            let mut fields = BTreeMap::from([("run".to_string(), FieldValue::Text(run.into()))]);
            if let Some(tag) = tag {
                fields.insert("tag".into(), FieldValue::Text(tag.into()));
            }
            replace.state_updates.push(StateUpdate {
                collection: "instances".into(),
                key: key.into(),
                expect: StateExpect::Version(versions[key]),
                change: StateChange::Put {
                    fields,
                    value: vec![9].into(),
                },
            });
        }
        self.commit(replace).await;
        assert_eq!(
            query_keys(self.store, query(Predicate::Absent)).await,
            vec!["s2", "s0"]
        );
        assert_eq!(
            query_keys(self.store, query(Predicate::Present)).await,
            vec!["s1", "s3"]
        );
    }
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
            issued_at: 0,
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
            buckets: None,
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
                issued_at: 0,
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
            issued_at: 0,
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
            buckets: None,
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

/// Fixture-independent acknowledged-write payload for adapter loss tests.
/// Writer-loss bindings kill without shutdown and use a fresh verifier process;
/// server-loss bindings crash/recover storage and open a fresh store. Both invoke
/// `verify_process_loss` with the exact acknowledgement; neither weakens semantics.
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
        issued_at: 0,
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
            request_ids: vec![ReceiptReadId {
                request_id: "crash-transition".into(),
                issued_at: 0,
            }],
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
    let GroupOutcome::Committed { versions } = &original.outcome else {
        unreachable!()
    };
    let acknowledged_state_version = versions
        .iter()
        .find(|v| v.collection.as_deref() == Some("instances") && v.key == "crash-state")
        .expect("state version in acknowledged atomic receipt")
        .version;
    assert_eq!(
        state[0].as_ref().unwrap().version,
        acknowledged_state_version
    );
    assert_eq!(
        state[0].as_ref().unwrap().fields.get("run"),
        Some(&FieldValue::Text("crash-run".into()))
    );
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
            buckets: None,
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
                request_ids: vec![ReceiptReadId {
                    request_id: "crash-transition".into(),
                    issued_at: 0
                }],
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
            issued_at: 0,
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
        issued_at: 0,
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
            issued_at: 0,
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
            issued_at: 0,
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
            issued_at: 0,
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
            issued_at: 0,
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
    // JSON null is a present value, so presence sees it; absence is a declared
    // field concept and an admitted-by-prefix name cannot be tested for it.
    for (name, expected) in [
        (literal.clone(), vec!["a", "b"]),
        ("/payload/mixed".to_string(), vec!["a", "b", "c"]),
        (long.clone(), vec!["a"]),
        ("/gates/rule/~1".to_string(), vec!["a"]),
        ("/payload/never-written".to_string(), vec![]),
    ] {
        assert_eq!(
            query_keys(store, select(name.clone(), Predicate::Present)).await,
            expected,
            "present {name}"
        );
        assert!(
            matches!(
                store.query(select(name.clone(), Predicate::Absent)).await,
                Err(QueueError::Invalid(_))
            ),
            "absent {name}"
        );
    }
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
            buckets: None,
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
            issued_at: 0,
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
                items: vec![invalid],
                issued_at: 0,
            })
            .await,
        Err(QueueError::Invalid(_))
    ));
}

/// Mandatory numeric visibility oracle: positional bins and one lifecycle snapshot.
pub async fn numeric_bucket_stats(store: &dyn QueueStore, clock: &TestClock, queue: QueueAddress) {
    clock.set(0);
    let mut definition = schema();
    definition
        .work
        .additional_field_prefixes
        .insert("/numeric/".into());
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let field = format!("/numeric/{}", "p\0λ/".repeat(16384));
    let mut items = Vec::new();
    for (i, v) in [-1.0, 0.0, 0.25, 0.5, 0.75, 1.0].into_iter().enumerate() {
        let mut work = item(&format!("n{i}"), "buckets", i as i64, i as i64);
        work.fields.insert(field.clone(), FieldValue::Float(v));
        items.push(work);
    }
    items.push(item("absent", "buckets", 8, 8));
    let mut integer = item("integer", "buckets", 9, 9);
    integer.fields.insert(field.clone(), FieldValue::Integer(0));
    items.push(integer);
    items.push(item("foreign", "foreign", 10, 10));
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "bucket-seed".into(),
            mode: InsertMode::InsertOnly,
            items,
            issued_at: 0,
        })
        .await
        .unwrap();
    let bin = |label: &str, lo: f64, hi: f64| NumericBucket {
        label: label.into(),
        lower_inclusive: FieldValue::Float(lo),
        upper_exclusive: FieldValue::Float(hi),
    };
    let dimension = NumericBucketDimension {
        field: field.clone(),
        buckets: vec![
            bin("same", 0.5, 1.0),
            bin("same", 0.0, 0.5),
            bin("missing", 2.0, 3.0),
        ],
        missing_label: "missing".into(),
    };
    let request = StatsRequest {
        buckets: Some(dimension.clone()),
        queue: queue.clone(),
        filter: filter("buckets"),
        group_by: vec!["kind".into()],
        eligible_at: 0,
    };
    let result = store.stats(request.clone()).await.unwrap();
    assert_eq!(result.as_of, 0);
    clock.set(-1);
    let negative = store.stats(request.clone()).await.unwrap();
    assert_eq!(negative.as_of, -1);
    assert_eq!(negative.totals, result.totals);
    assert_eq!(negative.groups, result.groups);
    assert_eq!(negative.buckets, result.buckets);
    clock.set(0);
    assert_eq!(result.totals.pending, 8);
    assert_eq!(result.groups.len(), 1);
    assert_eq!(result.groups[0].counts, result.totals);
    assert_eq!(
        result
            .buckets
            .iter()
            .map(|b| (b.label.as_str(), b.counts.pending))
            .collect::<Vec<_>>(),
        vec![("same", 2), ("same", 2), ("missing", 0), ("missing", 1)]
    );
    for value in [
        FieldValue::Float(-1.0),
        FieldValue::Float(1.0),
        FieldValue::Integer(0),
    ] {
        assert_eq!(dimension.classify(Some(&value)), None);
    }
    assert_eq!(dimension.classify(None), Some(3));
    let mut empty = request.clone();
    empty.buckets.as_mut().unwrap().buckets.clear();
    let result = store.stats(empty).await.unwrap();
    assert_eq!(result.buckets.len(), 1);
    assert_eq!(result.buckets[0].counts.pending, 1);
    for intervals in [
        vec![bin("a", 0.0, 1.0), bin("b", 0.5, 2.0)],
        vec![bin("a", f64::NAN, 1.0)],
        vec![bin("a", 1.0, 1.0)],
        (0..51)
            .map(|i| bin("a", i as f64, i as f64 + 1.0))
            .collect(),
    ] {
        let mut invalid = request.clone();
        invalid.buckets.as_mut().unwrap().buckets = intervals;
        assert!(matches!(
            store.stats(invalid).await,
            Err(QueueError::Invalid(_))
        ));
    }
    let mut fifty = request.clone();
    fifty.buckets.as_mut().unwrap().buckets = (0..50)
        .map(|i| bin("bin", i as f64, i as f64 + 1.0))
        .collect();
    assert_eq!(store.stats(fifty).await.unwrap().buckets.len(), 51);
    let mut integers = request.clone();
    integers.buckets = Some(NumericBucketDimension {
        field: "score".into(),
        missing_label: "missing".into(),
        buckets: vec![
            NumericBucket {
                label: "high".into(),
                lower_inclusive: FieldValue::Integer(3),
                upper_exclusive: FieldValue::Integer(i64::MAX),
            },
            NumericBucket {
                label: "low".into(),
                lower_inclusive: FieldValue::Integer(i64::MIN),
                upper_exclusive: FieldValue::Integer(3),
            },
        ],
    });
    assert_eq!(
        store
            .stats(integers)
            .await
            .unwrap()
            .buckets
            .iter()
            .map(|b| b.counts.pending)
            .collect::<Vec<_>>(),
        vec![5, 3, 0]
    );
    let mut selection = claim(&queue, "bucket-claim", "buckets");
    selection.filter.keys = Some(BTreeSet::from(["n1".into()]));
    let lease = store.claim(selection).await.unwrap();
    assert_eq!(lease.len(), 1);
    let result = store.stats(request.clone()).await.unwrap();
    assert_eq!(
        (
            result.buckets[1].counts.pending,
            result.buckets[1].counts.leased
        ),
        (1, 1)
    );
    clock.set(1_000_000_000);
    let mut pending = request.clone();
    pending.filter.lifecycle = Some(Lifecycle::Pending);
    let result = store.stats(pending).await.unwrap();
    assert_eq!(result.totals.pending, 8);
    assert_eq!(
        (
            result.buckets[1].counts.pending,
            result.buckets[1].counts.leased
        ),
        (2, 0)
    );
    let visible = query_keys(
        store,
        QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "$key".into(),
            filter: Filter {
                lifecycle: Some(Lifecycle::Pending),
                ..filter("buckets")
            },
            descending: false,
            cursor: None,
            max_records: 20,
            max_bytes: 1 << 24,
        },
    )
    .await;
    assert_eq!(visible.len(), 8);
    assert!(visible.contains(&"n1".into()));
    clock.set(0);
}

/// Mandatory administration/preview/receipt scenario; every backend runs it.
pub async fn atomic_administration_preview_and_replay(store: &dyn QueueStore, queue: QueueAddress) {
    use fireweed_core::queue_administration::*;
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
            request_id: "seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![
                item("control", "control", 0, 1),
                item("worker", "worker", 0, 2),
            ],
            issued_at: 0,
        })
        .await
        .unwrap();
    let claimed = store.claim(claim(&queue, "lease", "worker")).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let mut transition = group("admin-transition");
    transition.claimed_updates.push(update(&claimed[0]));
    transition.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "read-only-guard".into(),
        expect: StateExpect::Absent,
        change: StateChange::Check,
    });
    transition.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "workflow".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([("run".into(), FieldValue::Text("atomic".into()))]),
            value: vec![7, 8].into(),
        },
    });
    transition.work_inserts.push(item("spawn", "spawn", 0, 3));
    transition.administration = Some(AdministrativeCommit {
        max_result_bytes: 4096,
        intent: AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(BTreeSet::from(["kind".into()])),
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "control".into(),
                expected_version: Some(1),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(17),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: BTreeSet::new(),
                },
            }]),
        },
    });
    let preview = store
        .preview(PreviewRequest {
            queue: queue.clone(),
            group: transition.clone(),
        })
        .await
        .unwrap();
    let PreviewOutcome::AdministrativeCommitted {
        versions,
        administration,
    } = &preview.outcome
    else {
        panic!("{preview:?}")
    };
    assert_eq!(
        versions.iter().map(|v| v.key.as_str()).collect::<Vec<_>>(),
        vec!["worker", "workflow", "spawn"]
    );
    assert_eq!(
        administration.items[0].outcome,
        AdministrativeOutcome::Changed { version: 2 }
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![ReceiptReadId {
                    request_id: transition.request_id.clone(),
                    issued_at: transition.issued_at
                }],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Absent]
    );
    assert!(
        store
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: "instances".into(),
                keys: vec!["workflow".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap()[0]
            .is_none()
    );
    let batch = CommitBatch {
        queue: queue.clone(),
        groups: vec![transition.clone()],
    };
    let receipt = store.commit(batch.clone()).await.unwrap();
    let GroupOutcome::AdministrativeCommitted {
        versions: actual_versions,
        administration: actual_administration,
    } = &receipt[0].outcome
    else {
        panic!("{receipt:?}")
    };
    assert_eq!(actual_versions, versions);
    assert_eq!(actual_administration, administration);
    assert_eq!(
        store
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: "instances".into(),
                keys: vec!["workflow".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .value
            .as_ref(),
        &[7, 8]
    );
    assert_eq!(
        store
            .claim(claim(&queue, "claim-spawn", "spawn"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.commit(batch).await.unwrap(), receipt); // Replay ignores now-stale credentials/CAS.
    let preview = store
        .preview(PreviewRequest {
            queue: queue.clone(),
            group: transition.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        preview.outcome,
        PreviewOutcome::Rejected(Rejection::StaleLease)
    ); // Fresh preview ignores retained receipt.
    transition.administration.as_mut().unwrap().max_result_bytes += 1;
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
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue,
                request_ids: vec![ReceiptReadId {
                    request_id: "admin-transition".into(),
                    issued_at: 0
                }],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Found(receipt[0].clone())]
    );
}

/// A logical oversized middle group has zero effects and an exact retained rejection.
pub async fn administrative_size_siblings_and_preflight(
    store: &dyn QueueStore,
    queue: QueueAddress,
) {
    use fireweed_core::queue_administration::*;
    let mut definition = schema();
    definition
        .work
        .fields
        .insert("wide".into(), FieldType::Bytes);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: definition,
        })
        .await
        .unwrap();
    let mut wide = item("wide", "wide", 0, 0);
    wide.fields
        .insert("wide".into(), FieldValue::Bytes(vec![9; 32768]));
    store
        .insert(InsertRequest {
            queue: queue.clone(),
            request_id: "seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![item("first", "first", 0, 0), wide],
            issued_at: 0,
        })
        .await
        .unwrap();
    let proposal =
        |id: &str, key: &str, version: u64, priority: i64, before: Option<&str>, budget: u64| {
            let mut group = group(id);
            group.administration = Some(AdministrativeCommit {
                max_result_bytes: budget,
                intent: AdministrativeIntent {
                    evaluated_at: 0,
                    before_fields: before.map(|name| BTreeSet::from([name.into()])),
                    operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                        key: key.into(),
                        expected_version: Some(version),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(priority),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: BTreeSet::new(),
                        },
                    }]),
                },
            });
            group
        };
    let first = proposal("first", "first", 1, 10, None, 4096);
    let mut middle = proposal("middle", "wide", 1, 20, Some("wide"), 1024);
    middle.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "discarded".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Default::default(),
            value: vec![1].into(),
        },
    });
    middle
        .work_inserts
        .push(item("discarded-work", "discarded", 0, 0));
    let mut last = proposal("last", "first", 2, 30, Some("kind"), 4096);
    last.work_inserts
        .push(item("visible-next", "visible", 0, 1));
    let receipts = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![first.clone(), middle.clone(), last.clone()],
        })
        .await
        .unwrap();
    assert!(matches!(
        receipts[0].outcome,
        GroupOutcome::AdministrativeCommitted { .. }
    ));
    assert_eq!(
        receipts[1].outcome,
        GroupOutcome::Rejected(Rejection::TooLarge)
    );
    let GroupOutcome::AdministrativeCommitted { administration, .. } = &receipts[2].outcome else {
        panic!("{receipts:?}")
    };
    assert_eq!(administration.items[0].before.as_ref().unwrap().version, 2);
    assert_eq!(
        administration.items[0].outcome,
        AdministrativeOutcome::Changed { version: 3 }
    );
    assert!(
        store
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: "instances".into(),
                keys: vec!["discarded".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap()[0]
            .is_none()
    );
    assert!(
        store
            .claim(claim(&queue, "discarded-claim", "discarded"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .claim(claim(&queue, "visible-claim", "visible"))
            .await
            .unwrap()
            .len(),
        1
    );
    let replay = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![last, middle, first],
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
    let wide = proposal("wide-accepted", "wide", 1, 50, Some("wide"), 65536);
    let wide = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![wide],
        })
        .await
        .unwrap();
    assert!(matches!(
        wide[0].outcome,
        GroupOutcome::AdministrativeCommitted { .. }
    ));
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![ReceiptReadId {
                    request_id: "wide-accepted".into(),
                    issued_at: 0
                }],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Found(wide[0].clone())]
    );
    let valid = proposal("preflight-valid", "first", 3, 40, None, 4096);
    let invalid = proposal("preflight-invalid", "wide", 1, 40, None, 1);
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![valid.clone(), invalid]
            })
            .await,
        Err(QueueError::TooLarge)
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![ReceiptReadId {
                    request_id: valid.request_id.clone(),
                    issued_at: valid.issued_at
                }],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Absent]
    );
    let preview = store
        .preview(PreviewRequest {
            queue,
            group: valid,
        })
        .await
        .unwrap();
    assert!(matches!(
        preview.outcome,
        PreviewOutcome::AdministrativeCommitted { .. }
    ));
}

/// Selection is complete beyond an input chunk and returns compact clause stats.
pub async fn broad_administration_exceeds_input_chunk(store: &dyn QueueStore, queue: QueueAddress) {
    use fireweed_core::queue_administration::*;
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    for start in (0..5001).step_by(1000) {
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: format!("seed-{start}"),
                mode: InsertMode::InsertOnly,
                items: (start..(start + 1000).min(5001))
                    .map(|n| item(&format!("bulk-{n:05}"), "bulk", 0, n))
                    .collect(),
                issued_at: 0,
            })
            .await
            .unwrap();
    }
    let mut change = group("bulk-change");
    change.administration = Some(AdministrativeCommit {
        max_result_bytes: 4096,
        intent: AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(123),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: BTreeSet::new(),
                },
            }]),
        },
    });
    let receipt = store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![change.clone()],
        })
        .await
        .unwrap();
    let GroupOutcome::AdministrativeCommitted {
        versions,
        administration,
    } = &receipt[0].outcome
    else {
        panic!("{receipt:?}")
    };
    assert!(versions.is_empty());
    assert!(administration.items.is_empty());
    assert_eq!(administration.histogram.matched, 5001);
    assert_eq!(
        administration.clauses[0].histogram.outcomes[&AdministrativeOutcomeKind::Changed],
        5001
    );
    assert_eq!(
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![change]
            })
            .await
            .unwrap(),
        receipt
    );
    let mut claim = claim(&queue, "verify-bulk", "bulk");
    claim.max_records = 3;
    assert!(
        store
            .claim(claim)
            .await
            .unwrap()
            .iter()
            .all(|work| work.record.version == 3 && work.record.work.priority == 123)
    );
}

const MINUTE: i64 = 60_000_000_000;
const HOUR: i64 = 60 * MINUTE;
const SKEW: i64 = 5 * MINUTE;

fn retention(request_id_nanos: i64, deleted_keys: DeletedKeyPolicy) -> QueueRetention {
    QueueRetention {
        request_id_nanos: u64::try_from(request_id_nanos).unwrap(),
        deleted_keys,
    }
}
fn insert_request(
    queue: &QueueAddress,
    id: &str,
    mode: InsertMode,
    key: &str,
    issued_at: i64,
) -> InsertRequest {
    InsertRequest {
        queue: queue.clone(),
        request_id: id.into(),
        mode,
        items: vec![item(key, "retention", 0, 0)],
        issued_at,
    }
}
async fn commit_one(
    store: &dyn QueueStore,
    queue: &QueueAddress,
    group: CommitGroup,
) -> QueueResult<CommitReceipt> {
    Ok(store
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![group],
        })
        .await?
        .remove(0))
}
/// Claims pending work and deletes `key` with a group issued at `issued_at`.
/// Returns the version the deletion committed.
async fn claim_and_delete(
    store: &dyn QueueStore,
    queue: &QueueAddress,
    key: &str,
    id: &str,
    issued_at: i64,
) -> u64 {
    let claimed = store
        .claim(ClaimRequest {
            issued_at,
            ..claim(queue, &format!("{id}-claim"), "retention")
        })
        .await
        .unwrap();
    let target = claimed
        .iter()
        .find(|work| work.record.work.key == key)
        .expect("claimed the key");
    let mut update = update(target);
    update.disposition = Disposition::Delete;
    let mut deletion = group(id);
    deletion.issued_at = issued_at;
    deletion.claimed_updates.push(update);
    match commit_one(store, queue, deletion).await.unwrap().outcome {
        GroupOutcome::Committed { versions } => versions[0].version,
        other => panic!("deletion not committed: {other:?}"),
    }
}
fn inserted_version(outcomes: &[InsertOutcome]) -> u64 {
    match outcomes {
        [InsertOutcome::Inserted { version, .. }] => *version,
        other => panic!("not inserted: {other:?}"),
    }
}
fn already_exists(outcomes: &[InsertOutcome]) -> bool {
    matches!(
        outcomes,
        [InsertOutcome::Rejected {
            reason: Rejection::AlreadyExists,
            ..
        }]
    )
}

/// A `Retired` window must cover the request id window plus five minutes. The
/// floor is refused at provisioning and nothing is created.
pub async fn retention_floor_refused_at_provisioning(store: &dyn QueueStore, queue: QueueAddress) {
    let provision = |retention| {
        store.provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention),
        })
    };
    let floor = HOUR + SKEW;
    for refused in [
        retention(0, DeletedKeyPolicy::Reusable),
        retention(HOUR, DeletedKeyPolicy::Retired { window_nanos: 0 }),
        retention(
            HOUR,
            DeletedKeyPolicy::Retired {
                window_nanos: u64::try_from(floor - 1).unwrap(),
            },
        ),
    ] {
        assert!(
            matches!(provision(refused).await, Err(QueueError::Invalid(_))),
            "floor must be refused"
        );
    }
    // Nothing was created by the refusals.
    assert_eq!(
        store
            .insert(insert_request(
                &queue,
                "no-queue",
                InsertMode::InsertOnly,
                "a",
                0
            ))
            .await,
        Err(QueueError::QueueNotFound)
    );
    provision(retention(
        HOUR,
        DeletedKeyPolicy::Retired {
            window_nanos: u64::try_from(floor).unwrap(),
        },
    ))
    .await
    .unwrap();
}

/// `issued_at` beyond the skew is refused with no effect and the same id may be
/// retried later. The same id with another `issued_at` conflicts.
pub async fn issued_at_skew_and_conflict(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let now = 100 * HOUR;
    clock.set(now);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema(),
        })
        .await
        .unwrap();
    fn invalid<T>(result: &QueueResult<T>) -> bool {
        matches!(result, Err(QueueError::Invalid(_)))
    }
    let ahead = now + SKEW + 1;

    // Insert: the exact skew passes; one nanosecond more is refused, then passes later.
    let at_limit = insert_request(
        &queue,
        "skew-edge",
        InsertMode::InsertOnly,
        "edge",
        now + SKEW,
    );
    assert_eq!(inserted_version(&store.insert(at_limit).await.unwrap()), 1);
    let early = insert_request(
        &queue,
        "early-insert",
        InsertMode::InsertOnly,
        "early",
        ahead,
    );
    assert!(invalid(&store.insert(early.clone()).await));
    clock.set(now + 2);
    let first = store.insert(early.clone()).await.unwrap();
    assert_eq!(inserted_version(&first), 1);
    assert_eq!(store.insert(early.clone()).await.unwrap(), first);
    let mut shifted = early.clone();
    shifted.issued_at += 1;
    assert_eq!(
        store.insert(shifted).await,
        Err(QueueError::RequestConflict)
    );

    // Claim.
    let now = now + 2;
    let early_claim = ClaimRequest {
        issued_at: now + SKEW + 10,
        ..claim(&queue, "early-claim", "retention")
    };
    assert!(invalid(&store.claim(early_claim.clone()).await));
    clock.set(now + 10);
    let claimed = store.claim(early_claim.clone()).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(store.claim(early_claim.clone()).await.unwrap(), claimed);
    let mut shifted = early_claim;
    shifted.issued_at -= 1;
    assert_eq!(store.claim(shifted).await, Err(QueueError::RequestConflict));

    // Commit and preview.
    let now = now + 10;
    let mut early_group = group("early-group");
    early_group
        .work_inserts
        .push(item("grouped", "retention", 0, 0));
    early_group.issued_at = now + SKEW + 10;
    assert!(invalid(
        &store
            .preview(PreviewRequest {
                queue: queue.clone(),
                group: early_group.clone()
            })
            .await
    ));
    assert!(invalid(
        &commit_one(store, &queue, early_group.clone()).await
    ));
    clock.set(now + 10);
    assert!(
        store
            .preview(PreviewRequest {
                queue: queue.clone(),
                group: early_group.clone()
            })
            .await
            .is_ok()
    );
    let committed = commit_one(store, &queue, early_group.clone())
        .await
        .unwrap();
    assert!(matches!(committed.outcome, GroupOutcome::Committed { .. }));
    assert_eq!(
        commit_one(store, &queue, early_group.clone())
            .await
            .unwrap(),
        committed
    );
    let mut shifted = early_group;
    shifted.issued_at -= 1;
    assert_eq!(
        commit_one(store, &queue, shifted).await.unwrap().outcome,
        GroupOutcome::RequestConflict
    );
}

/// `Retired` refuses a reinsert for `window_nanos` after the deletion's
/// `issued_at`, through both insert modes and `work_inserts`. Afterwards the key
/// returns with a higher version.
pub async fn retired_keys_refuse_reinsert_within_window(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let window = 2 * HOUR;
    let now = 10 * HOUR;
    clock.set(now);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(
                HOUR,
                DeletedKeyPolicy::Retired {
                    window_nanos: u64::try_from(window).unwrap(),
                },
            )),
        })
        .await
        .unwrap();
    store
        .insert(insert_request(
            &queue,
            "seed",
            InsertMode::InsertOnly,
            "a",
            now,
        ))
        .await
        .unwrap();
    // The deletion's time is its group's issued_at, not the clock reading.
    let deleted_at = now - 1_000;
    let deleted_version = claim_and_delete(store, &queue, "a", "delete-a", deleted_at).await;
    let expires = deleted_at + window;
    let mut serial = 0;
    let mut id = |label: &str| {
        serial += 1;
        format!("{label}-{serial}")
    };

    for tick in [now, expires - 1] {
        clock.set(tick);
        for mode in [InsertMode::InsertOnly, InsertMode::Upsert] {
            let outcomes = store
                .insert(insert_request(&queue, &id("insert"), mode, "a", tick))
                .await
                .unwrap();
            assert!(
                already_exists(&outcomes),
                "{mode:?} at {tick}: {outcomes:?}"
            );
        }
        let mut grouped = group(&id("group"));
        grouped.issued_at = tick;
        grouped.work_inserts.push(item("a", "retention", 0, 0));
        assert_eq!(
            commit_one(store, &queue, grouped).await.unwrap().outcome,
            GroupOutcome::Rejected(Rejection::AlreadyExists)
        );
    }
    // A key that was never deleted is unaffected.
    let fresh = store
        .insert(insert_request(
            &queue,
            &id("insert"),
            InsertMode::InsertOnly,
            "never-deleted",
            expires - 1,
        ))
        .await
        .unwrap();
    assert_eq!(inserted_version(&fresh), 1);

    clock.set(expires);
    let again = inserted_version(
        &store
            .insert(insert_request(
                &queue,
                &id("insert"),
                InsertMode::InsertOnly,
                "a",
                expires,
            ))
            .await
            .unwrap(),
    );
    assert!(again > deleted_version, "{again} after {deleted_version}");

    // Deleted again, and reinserted through a group once the window has passed.
    let second_delete = claim_and_delete(store, &queue, "a", "delete-a-again", expires).await;
    assert!(second_delete > again);
    clock.set(expires + window - 1);
    let mut grouped = group(&id("group"));
    grouped.issued_at = expires + window - 1;
    grouped.work_inserts.push(item("a", "retention", 0, 0));
    assert_eq!(
        commit_one(store, &queue, grouped).await.unwrap().outcome,
        GroupOutcome::Rejected(Rejection::AlreadyExists)
    );
    clock.set(expires + window);
    let mut grouped = group(&id("group"));
    grouped.issued_at = expires + window;
    grouped.work_inserts.push(item("a", "retention", 0, 0));
    match commit_one(store, &queue, grouped).await.unwrap().outcome {
        GroupOutcome::Committed { versions } => assert!(versions[0].version > second_delete),
        other => panic!("not committed: {other:?}"),
    }
}

/// `Reusable` keeps today's behaviour: a deleted key returns at once, with a
/// higher version.
pub async fn reusable_keys_reinsert_at_once(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let now = 10 * HOUR;
    clock.set(now);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(HOUR, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    store
        .insert(insert_request(
            &queue,
            "seed",
            InsertMode::InsertOnly,
            "a",
            now,
        ))
        .await
        .unwrap();
    let deleted_version = claim_and_delete(store, &queue, "a", "delete-a", now).await;
    let again = inserted_version(
        &store
            .insert(insert_request(
                &queue,
                "again",
                InsertMode::Upsert,
                "a",
                now,
            ))
            .await
            .unwrap(),
    );
    assert!(again > deleted_version);
    let second_delete = claim_and_delete(store, &queue, "a", "delete-a-again", now).await;
    let mut grouped = group("grouped");
    grouped.issued_at = now;
    grouped.work_inserts.push(item("a", "retention", 0, 0));
    match commit_one(store, &queue, grouped).await.unwrap().outcome {
        GroupOutcome::Committed { versions } => assert!(versions[0].version > second_delete),
        other => panic!("not committed: {other:?}"),
    }
}

/// The ways a work key can be deleted. Each stamps the deleting group's
/// `issued_at` as the key's deletion time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeletionRoute {
    /// A claimed update with `Disposition::Delete`.
    Claimed,
    /// An `UnclaimedChange::Delete` work update.
    Unclaimed,
    /// An administrative delete addressed by key.
    AdministrativeAddressed,
    /// An administrative delete selected by a filter, through the broad route.
    AdministrativeSelected,
}
const DELETION_ROUTES: [DeletionRoute; 4] = [
    DeletionRoute::Claimed,
    DeletionRoute::Unclaimed,
    DeletionRoute::AdministrativeAddressed,
    DeletionRoute::AdministrativeSelected,
];
impl DeletionRoute {
    fn key(self) -> &'static str {
        match self {
            Self::Claimed => "route-claimed",
            Self::Unclaimed => "route-unclaimed",
            Self::AdministrativeAddressed => "route-addressed",
            Self::AdministrativeSelected => "route-selected",
        }
    }
}
/// Seeds `route`'s key at version 1 and deletes it with a group issued at
/// `deleted_at`, returning the version the deletion committed. The clock and the
/// administrative `evaluated_at` deliberately differ from `deleted_at`.
async fn delete_by_route(
    store: &dyn QueueStore,
    queue: &QueueAddress,
    route: DeletionRoute,
    deleted_at: i64,
) -> u64 {
    use fireweed_core::queue_administration::*;
    let key = route.key();
    let id = format!("route-delete-{key}");
    if route == DeletionRoute::Claimed {
        store
            .insert(insert_request(
                queue,
                &format!("{id}-seed"),
                InsertMode::InsertOnly,
                key,
                deleted_at,
            ))
            .await
            .unwrap();
        return claim_and_delete(store, queue, key, &id, deleted_at).await;
    }
    // Not of the claimed kind, so no claim leases it.
    let mut seed = insert_request(
        queue,
        &format!("{id}-seed"),
        InsertMode::InsertOnly,
        key,
        deleted_at,
    );
    seed.items = vec![item(key, "administered", 0, 0)];
    assert_eq!(inserted_version(&store.insert(seed).await.unwrap()), 1);
    let mut deletion = group(&id);
    deletion.issued_at = deleted_at;
    let administrative = |operation| {
        Some(AdministrativeCommit {
            max_result_bytes: 4096,
            intent: AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation,
            },
        })
    };
    match route {
        DeletionRoute::Claimed => unreachable!("handled above"),
        DeletionRoute::Unclaimed => deletion.work_updates.push(UnclaimedUpdate {
            key: key.into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        }),
        DeletionRoute::AdministrativeAddressed => {
            deletion.administration = administrative(AdministrativeOperation::Addressed(vec![
                AdministrativeEntry {
                    key: key.into(),
                    expected_version: Some(1),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                },
            ]))
        }
        DeletionRoute::AdministrativeSelected => {
            deletion.administration = administrative(AdministrativeOperation::SelectFirst(vec![
                AdministrativeClause {
                    id: "delete".into(),
                    selector: WorkSelector::leaf(SelectorNode::Keys(BTreeSet::from([key.into()]))),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                },
            ]))
        }
    }
    match commit_one(store, queue, deletion).await.unwrap().outcome {
        GroupOutcome::Committed { versions } => versions[0].version,
        GroupOutcome::AdministrativeCommitted {
            versions,
            administration,
        } => {
            assert!(versions.is_empty(), "{route:?}: {versions:?}");
            match route {
                DeletionRoute::AdministrativeAddressed => match administration.items[0].outcome {
                    AdministrativeOutcome::Deleted { version } => version,
                    ref other => panic!("{route:?} not deleted: {other:?}"),
                },
                _ => {
                    assert_eq!(administration.histogram.matched, 1);
                    assert_eq!(
                        administration.clauses[0].histogram.outcomes
                            [&AdministrativeOutcomeKind::Deleted],
                        1
                    );
                    // A broad delete reports counts only; the version after the
                    // seed is the next one.
                    2
                }
            }
        }
        other => panic!("{route:?} not committed: {other:?}"),
    }
}

/// A `Retired` deletion that a reopened store or a rebuilt projection must
/// reproduce: same refusals inside the window, same acceptance after it, for a
/// key removed by each deletion route.
#[derive(Debug, Clone)]
pub struct RetiredDeletion {
    queue: QueueAddress,
    /// Ascending by `expires_at`.
    keys: Vec<RetiredKey>,
}
#[derive(Debug, Clone)]
struct RetiredKey {
    key: &'static str,
    deleted_version: u64,
    expires_at: i64,
}
const RETIRED_WINDOW: i64 = 2 * HOUR;
/// Deletes one key through each route, each with its own `issued_at`, later
/// routes later, and checks the refusals while the owner that deleted them is
/// still live.
pub async fn prepare_retired_deletion(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) -> RetiredDeletion {
    prepare_retired_deletion_through(store, clock, queue, cfg!(target_os = "linux")).await
}
/// `broad_administration` adds the filter-selected administrative delete. The
/// native store runs that route only on Linux; PostgreSQL runs it everywhere.
pub async fn prepare_retired_deletion_through(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
    broad_administration: bool,
) -> RetiredDeletion {
    let now = 10 * HOUR;
    clock.set(now);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(
                HOUR,
                DeletedKeyPolicy::Retired {
                    window_nanos: u64::try_from(RETIRED_WINDOW).unwrap(),
                },
            )),
        })
        .await
        .unwrap();
    let mut keys = Vec::new();
    let routes = DELETION_ROUTES
        .into_iter()
        .filter(|route| broad_administration || *route != DeletionRoute::AdministrativeSelected);
    for (index, route) in routes.enumerate() {
        // Distinct, ascending times behind the clock: a deletion time taken from
        // the clock, from `evaluated_at` (0) or from another route's group fails.
        let deleted_at = now - 10_000 + 1_000 * i64::try_from(index).unwrap();
        let deleted_version = delete_by_route(store, &queue, route, deleted_at).await;
        keys.push(RetiredKey {
            key: route.key(),
            deleted_version,
            expires_at: deleted_at + RETIRED_WINDOW,
        });
    }
    let prepared = RetiredDeletion { queue, keys };
    refuse_reinserts(store, clock, &prepared, "live", 0).await;
    prepared
}
/// At one tick before the first expiry among `keys[from..]`, every one of them
/// refuses a reinsert in each insert mode and through a group.
async fn refuse_reinserts(
    store: &dyn QueueStore,
    clock: &TestClock,
    deletion: &RetiredDeletion,
    label: &str,
    from: usize,
) {
    let tick = deletion.keys[from].expires_at - 1;
    clock.set(tick);
    for retired in &deletion.keys[from..] {
        for mode in [InsertMode::InsertOnly, InsertMode::Upsert] {
            let outcomes = store
                .insert(insert_request(
                    &deletion.queue,
                    &format!("{label}-{}-{mode:?}-{tick}", retired.key),
                    mode,
                    retired.key,
                    tick,
                ))
                .await
                .unwrap();
            assert!(
                already_exists(&outcomes),
                "{label} {} {mode:?}: {outcomes:?}",
                retired.key
            );
        }
        let mut grouped = group(&format!("{label}-{}-group-{tick}", retired.key));
        grouped.issued_at = tick;
        grouped
            .work_inserts
            .push(item(retired.key, "retention", 0, 0));
        assert_eq!(
            commit_one(store, &deletion.queue, grouped)
                .await
                .unwrap()
                .outcome,
            GroupOutcome::Rejected(Rejection::AlreadyExists),
            "{label} {}",
            retired.key
        );
    }
}
/// Walks the expiries in order: just before each, the keys not yet expired
/// refuse; at it, that key returns at a higher version.
pub async fn verify_retired_deletion(
    store: &dyn QueueStore,
    clock: &TestClock,
    deletion: RetiredDeletion,
) {
    for (index, retired) in deletion.keys.iter().enumerate() {
        refuse_reinserts(store, clock, &deletion, "rebuilt", index).await;
        clock.set(retired.expires_at);
        let again = inserted_version(
            &store
                .insert(insert_request(
                    &deletion.queue,
                    &format!("rebuilt-after-window-{}", retired.key),
                    InsertMode::InsertOnly,
                    retired.key,
                    retired.expires_at,
                ))
                .await
                .unwrap(),
        );
        assert!(
            again > retired.deleted_version,
            "{}: {again} after {}",
            retired.key,
            retired.deleted_version
        );
    }
}
/// Every deletion route stamps the deleting group's `issued_at` and honours the
/// window: refused inside it, accepted after it at a higher version.
pub async fn retired_keys_cover_every_deletion_route(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
    broad_administration: bool,
) {
    let deletion =
        prepare_retired_deletion_through(store, clock, queue, broad_administration).await;
    verify_retired_deletion(store, clock, deletion).await;
}

/// Advances the store's cleanup schedule: any operation counts, and a read of no
/// ids stores nothing. A pass that this operation starts runs after it, and the
/// next operation on the queue follows it.
async fn tick(store: &dyn QueueStore, queue: &QueueAddress) {
    store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec![],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
}
fn read_id_at(id: &str, issued_at: i64) -> ReceiptReadId {
    ReceiptReadId {
        request_id: id.into(),
        issued_at,
    }
}

/// Cleanup advances a horizon `request_id_nanos` behind the store clock, offered
/// once a minute of store time, and a request issued before it is
/// `RequestExpired` with no lookup and no effect. An id at the horizon still
/// replays its recorded outcome. `read_commit`
/// answers `Expired` for an id older than the horizon, `Absent` for a newer one
/// nobody sent, and `Invalid` for one beyond the skew.
pub async fn request_horizon_expires_old_requests(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let window = 30 * MINUTE;
    let t0 = 100 * HOUR;
    clock.set(t0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(window, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    tick(store, &queue).await;
    let insert = insert_request(&queue, "horizon-insert", InsertMode::InsertOnly, "h1", t0);
    let inserted = store.insert(insert.clone()).await.unwrap();
    let mut grouped = group("horizon-group");
    grouped.issued_at = t0;
    grouped.work_inserts.push(item("h2", "retention", 0, 0));
    let committed = commit_one(store, &queue, grouped.clone()).await.unwrap();
    assert!(matches!(committed.outcome, GroupOutcome::Committed { .. }));
    let first_claim = ClaimRequest {
        issued_at: t0,
        ..claim(&queue, "horizon-claim", "retention")
    };
    assert_eq!(store.claim(first_claim.clone()).await.unwrap().len(), 2);

    // The horizon reaches `t0` exactly: an id issued then is not older than it.
    clock.set(t0 + window);
    tick(store, &queue).await;
    assert_eq!(store.insert(insert.clone()).await.unwrap(), inserted);
    assert_eq!(
        commit_one(store, &queue, grouped.clone()).await.unwrap(),
        committed
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![read_id_at("horizon-group", t0)],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Found(committed.clone())]
    );

    // A schedule interval later (one minute) it has passed them all.
    let horizon = t0 + MINUTE;
    let now = t0 + window + MINUTE;
    clock.set(now);
    tick(store, &queue).await;
    assert_eq!(
        store.insert(insert.clone()).await,
        Err(QueueError::RequestExpired)
    );
    assert_eq!(
        commit_one(store, &queue, grouped.clone())
            .await
            .unwrap()
            .outcome,
        GroupOutcome::RequestExpired
    );
    assert_eq!(
        store.claim(first_claim.clone()).await,
        Err(QueueError::RequestExpired)
    );
    // Preview applies the same horizon as commit.
    assert_eq!(
        store
            .preview(PreviewRequest {
                queue: queue.clone(),
                group: grouped.clone(),
            })
            .await,
        Err(QueueError::RequestExpired)
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![
                    read_id_at("horizon-group", t0),
                    read_id_at("never-sent", t0),
                    read_id_at("never-sent", now),
                ],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap(),
        vec![
            ReceiptLookup::Expired,
            ReceiptLookup::Expired,
            ReceiptLookup::Absent
        ]
    );
    assert!(matches!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![read_id_at("too-early", now + SKEW + 1)],
                max_bytes: 1 << 20,
            })
            .await,
        Err(QueueError::Invalid(_))
    ));

    // Refused with no lookup and no effect: the key was never created.
    assert_eq!(
        store
            .insert(insert_request(
                &queue,
                "ghost-old",
                InsertMode::InsertOnly,
                "ghost",
                t0
            ))
            .await,
        Err(QueueError::RequestExpired)
    );
    assert_eq!(
        inserted_version(
            &store
                .insert(insert_request(
                    &queue,
                    "ghost-new",
                    InsertMode::InsertOnly,
                    "ghost",
                    now
                ))
                .await
                .unwrap()
        ),
        1
    );
    // An id at the horizon is new, and replays once recorded.
    let at_horizon = insert_request(
        &queue,
        "at-horizon",
        InsertMode::InsertOnly,
        "at-horizon-key",
        horizon,
    );
    let recorded = store.insert(at_horizon.clone()).await.unwrap();
    assert_eq!(store.insert(at_horizon).await.unwrap(), recorded);
    // The expired claim yielded nothing; a claim that is not expired gets the work.
    let fresh = store
        .claim(ClaimRequest {
            issued_at: now,
            ..claim(&queue, "horizon-claim-fresh", "retention")
        })
        .await
        .unwrap();
    assert_eq!(fresh.len(), 4);
}

/// The horizon never passes a claim whose lease is live: its id still replays
/// the same work, and once the lease and the skew are over it expires.
/// Claims, then renews past the first lease end. Returns the claim and the
/// replay it must keep producing, for [`verify_renewed_claim_after_rebuild`].
pub async fn prepare_renewed_claim(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) -> (ClaimRequest, Vec<ClaimedWork>) {
    let before = clock.get();
    let t = 300 * HOUR;
    clock.set(t);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(30 * MINUTE, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    tick(store, &queue).await;
    store
        .insert(insert_request(
            &queue,
            "renewed-seed",
            InsertMode::InsertOnly,
            "r1",
            t,
        ))
        .await
        .unwrap();
    let request = ClaimRequest {
        issued_at: t,
        lease_nanos: u64::try_from(HOUR).unwrap(),
        ..claim(&queue, "renewed-rebuild-claim", "retention")
    };
    let granted = store.claim(request.clone()).await.unwrap();
    assert_eq!(granted.len(), 1);
    clock.set(t + 30 * MINUTE);
    let mut renewal = group("renew-rebuild-claim");
    renewal.issued_at = t + 30 * MINUTE;
    renewal.claimed_updates = granted
        .iter()
        .map(|work| {
            let mut change = update(work);
            change.disposition = Disposition::Retain {
                renew_nanos: Some(u64::try_from(HOUR).unwrap()),
            };
            change
        })
        .collect();
    committed(&commit_one(store, &queue, renewal).await.unwrap());
    let renewed = store.claim(request.clone()).await.unwrap();
    assert!(renewed[0].record.lease_expires_at > granted[0].record.lease_expires_at);
    // Later steps of the caller's run keep the clock they had.
    clock.set(before);
    (request, renewed)
}
/// After a restart or a cold rebuild from the log: the renewed lease replays
/// unchanged and still holds the horizon at the claim's `issued_at`.
pub async fn verify_renewed_claim_after_rebuild(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
    request: ClaimRequest,
    expected: Vec<ClaimedWork>,
) {
    clock.set(request.issued_at + 30 * MINUTE);
    assert_eq!(store.claim(request.clone()).await.unwrap(), expected);
    // Past the first lease, its skew and the request window; only the renewal is live.
    clock.set(request.issued_at + HOUR + SKEW + MINUTE);
    tick(store, &queue).await;
    assert_eq!(store.claim(request.clone()).await.unwrap(), expected);
    assert!(
        store
            .insert(insert_request(
                &queue,
                "after-rebuild",
                InsertMode::InsertOnly,
                "r2",
                request.issued_at
            ))
            .await
            .is_ok()
    );
    clock.set(request.issued_at + 30 * MINUTE + HOUR + SKEW + 1);
    tick(store, &queue).await;
    assert_eq!(store.claim(request).await, Err(QueueError::RequestExpired));
}
pub async fn horizon_never_passes_a_live_claim(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let window = 30 * MINUTE;
    let t0 = 100 * HOUR;
    clock.set(t0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(window, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    tick(store, &queue).await;
    let seed = insert_request(&queue, "live-seed", InsertMode::InsertOnly, "l1", t0);
    let seeded = store.insert(seed.clone()).await.unwrap();
    let live = ClaimRequest {
        issued_at: t0,
        lease_nanos: u64::try_from(HOUR).unwrap(),
        ..claim(&queue, "live-claim", "retention")
    };
    let claimed = store.claim(live.clone()).await.unwrap();
    assert_eq!(claimed.len(), 1);

    // The request horizon would be `t0 + 20 minutes`; the live claim holds it at `t0`.
    clock.set(t0 + 50 * MINUTE);
    tick(store, &queue).await;
    assert_eq!(store.claim(live.clone()).await.unwrap(), claimed);
    assert_eq!(store.insert(seed.clone()).await.unwrap(), seeded);
    assert_eq!(
        store
            .insert(insert_request(
                &queue,
                "before-claim",
                InsertMode::InsertOnly,
                "l0",
                t0 - 1
            ))
            .await,
        Err(QueueError::RequestExpired)
    );
    let between = insert_request(
        &queue,
        "between",
        InsertMode::InsertOnly,
        "l2",
        t0 + 10 * MINUTE,
    );
    let between_outcome = store.insert(between.clone()).await.unwrap();
    assert_eq!(inserted_version(&between_outcome), 1);

    // Past the lease and the skew the claim is no longer live, and the horizon moves on.
    clock.set(t0 + 2 * HOUR);
    tick(store, &queue).await;
    assert_eq!(
        store.claim(live.clone()).await,
        Err(QueueError::RequestExpired)
    );
    assert_eq!(store.insert(between).await, Err(QueueError::RequestExpired));
    // The expired claim gave no work and the key is still claimable by a new one.
    let again = store
        .claim(ClaimRequest {
            issued_at: t0 + 2 * HOUR,
            ..claim(&queue, "live-claim-again", "retention")
        })
        .await
        .unwrap();
    assert_eq!(again.len(), 2);

    // A renewal moves the lease on the work row only. The renewed claim must
    // keep holding the horizon after its own first lease and the request window
    // are both behind the clock.
    let t1 = 200 * HOUR;
    clock.set(t1);
    tick(store, &queue).await;
    let renewed_claim = ClaimRequest {
        issued_at: t1,
        lease_nanos: u64::try_from(HOUR).unwrap(),
        ..claim(&queue, "renewed-claim", "retention")
    };
    let granted = store.claim(renewed_claim.clone()).await.unwrap();
    assert!(!granted.is_empty());
    clock.set(t1 + 30 * MINUTE);
    let mut renewal = group("renew-held-claim");
    renewal.issued_at = t1 + 30 * MINUTE;
    renewal.claimed_updates = granted
        .iter()
        .map(|work| {
            let mut change = update(work);
            change.disposition = Disposition::Retain {
                renew_nanos: Some(u64::try_from(HOUR).unwrap()),
            };
            change
        })
        .collect();
    committed(&commit_one(store, &queue, renewal).await.unwrap());
    let renewed = store.claim(renewed_claim.clone()).await.unwrap();
    assert_eq!(renewed.len(), granted.len());
    // Past the first lease and its skew, and past `issued_at + window + skew`;
    // only the renewed lease is still live.
    clock.set(t1 + HOUR + SKEW + MINUTE);
    tick(store, &queue).await;
    assert_eq!(store.claim(renewed_claim.clone()).await.unwrap(), renewed);
    assert!(
        store
            .insert(insert_request(
                &queue,
                "at-renewed-claim",
                InsertMode::InsertOnly,
                "l3",
                t1
            ))
            .await
            .is_ok()
    );
    // Once the renewed lease and the skew are behind the clock the claim goes.
    clock.set(t1 + 30 * MINUTE + HOUR + SKEW + 1);
    tick(store, &queue).await;
    assert_eq!(
        store.claim(renewed_claim).await,
        Err(QueueError::RequestExpired)
    );
}

/// After cleanup removes a retired deleted row, the key may be inserted again
/// and its version still rises above every version it had.
pub async fn removed_deleted_keys_keep_versions_rising(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) {
    let window = 2 * HOUR;
    let start = 10 * HOUR;
    clock.set(start);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(
                HOUR,
                DeletedKeyPolicy::Retired {
                    window_nanos: u64::try_from(window).unwrap(),
                },
            )),
        })
        .await
        .unwrap();
    tick(store, &queue).await;
    let mut last = 0;
    for round in 0..3 {
        let base = start + round * 5 * HOUR;
        clock.set(base);
        tick(store, &queue).await;
        let inserted = inserted_version(
            &store
                .insert(insert_request(
                    &queue,
                    &format!("seed-{round}"),
                    InsertMode::InsertOnly,
                    "a",
                    base,
                ))
                .await
                .unwrap(),
        );
        assert!(inserted > last, "round {round}: {inserted} after {last}");
        let deleted = claim_and_delete(store, &queue, "a", &format!("delete-{round}"), base).await;
        assert!(deleted > inserted);
        // Inside the window the key is refused, whatever cleanup has done.
        clock.set(base + window - 2 * MINUTE);
        tick(store, &queue).await;
        let early = insert_request(
            &queue,
            &format!("early-{round}"),
            InsertMode::InsertOnly,
            "a",
            base + window - 2 * MINUTE,
        );
        assert!(already_exists(&store.insert(early).await.unwrap()));
        // After it cleanup removes the row, and the key comes back above its versions.
        clock.set(base + window + 1);
        tick(store, &queue).await;
        last = deleted;
    }
    clock.set(start + 3 * 5 * HOUR);
    tick(store, &queue).await;
    let again = inserted_version(
        &store
            .insert(insert_request(
                &queue,
                "after-last",
                InsertMode::InsertOnly,
                "a",
                start + 3 * 5 * HOUR,
            ))
            .await
            .unwrap(),
    );
    assert!(again > last);
}

/// What `prepare_request_horizon` leaves for a store rebuilt from the same log.
pub struct RequestHorizon {
    queue: QueueAddress,
    old_insert: InsertRequest,
    old_group: CommitGroup,
    at_horizon: InsertRequest,
    recorded: Vec<InsertOutcome>,
    now: i64,
}
/// Records requests before and at a horizon, then lets cleanup advance it past
/// the first and not the second. The clock is left at the time of the last pass.
pub async fn prepare_request_horizon(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
) -> RequestHorizon {
    let window = 30 * MINUTE;
    let t0 = 100 * HOUR;
    clock.set(t0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(window, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    let old_insert = insert_request(&queue, "old-insert", InsertMode::InsertOnly, "o1", t0);
    store.insert(old_insert.clone()).await.unwrap();
    let mut old_group = group("old-group");
    old_group.issued_at = t0;
    old_group.work_inserts.push(item("o2", "retention", 0, 0));
    committed(&commit_one(store, &queue, old_group.clone()).await.unwrap());
    let horizon = t0 + MINUTE;
    let at_horizon = insert_request(&queue, "at-horizon", InsertMode::InsertOnly, "o3", horizon);
    // Recorded before the horizon reaches it, so the clock has to be there.
    clock.set(horizon);
    let recorded = store.insert(at_horizon.clone()).await.unwrap();
    let now = t0 + window + MINUTE;
    clock.set(now);
    // The pass this starts moves the horizon to the at-horizon request, no further.
    tick(store, &queue).await;
    RequestHorizon {
        queue,
        old_insert,
        old_group,
        at_horizon,
        recorded,
        now,
    }
}
/// A store rebuilt from the log answers exactly as the one that ran cleanup: the
/// horizon, and what it expires, come from the log and not from a clock.
pub async fn verify_request_horizon(
    store: &dyn QueueStore,
    clock: &TestClock,
    prepared: RequestHorizon,
) {
    // Time stands where it did, so no pass of the rebuilt store changes anything.
    clock.set(prepared.now);
    let queue = &prepared.queue;
    assert_eq!(
        store.insert(prepared.old_insert.clone()).await,
        Err(QueueError::RequestExpired)
    );
    assert_eq!(
        commit_one(store, queue, prepared.old_group.clone())
            .await
            .unwrap()
            .outcome,
        GroupOutcome::RequestExpired
    );
    assert_eq!(
        store.insert(prepared.at_horizon.clone()).await.unwrap(),
        prepared.recorded
    );
    assert_eq!(
        store
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec![
                    read_id_at("old-group", prepared.old_group.issued_at),
                    read_id_at("fresh", prepared.now),
                ],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap(),
        vec![ReceiptLookup::Expired, ReceiptLookup::Absent]
    );
}

/// Counts what a backend retains: request, receipt and claim rows plus work rows.
pub type RetainedRows<'a> =
    &'a (dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = i64> + Send>> + Sync);

/// One cleanup pass removes at most its budget, and the rest follow in later
/// passes. A backend that can count what it retains drives this.
pub async fn one_pass_removes_at_most_its_budget(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
    retained: RetainedRows<'_>,
    budget: i64,
) {
    let window = 30 * MINUTE;
    let t0 = 100 * HOUR;
    clock.set(t0);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(window, DeletedKeyPolicy::Reusable)),
        })
        .await
        .unwrap();
    tick(store, &queue).await;
    let total = budget + budget / 10;
    let mut next = 0;
    while next < total {
        let groups = (total - next).min(64);
        let batch = (0..groups)
            .map(|n| {
                let mut grouped = group(&format!("budget-{}", next + n));
                grouped.issued_at = t0;
                grouped.work_inserts.push(item(
                    &format!("budget-key-{}", next + n),
                    "retention",
                    0,
                    0,
                ));
                grouped
            })
            .collect();
        for receipt in store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: batch,
            })
            .await
            .unwrap()
        {
            committed(&receipt);
        }
        next += groups;
    }
    assert_eq!(retained().await, 2 * total);
    // The horizon passes every request; one pass removes only its budget.
    clock.set(t0 + window + MINUTE);
    tick(store, &queue).await;
    tick(store, &queue).await;
    assert_eq!(retained().await, 2 * total - budget);
    clock.set(t0 + window + 2 * MINUTE);
    tick(store, &queue).await;
    tick(store, &queue).await;
    // Only the work rows are left, which cleanup never removes.
    assert_eq!(retained().await, total);
}

/// Requests and retired deleted keys churn through a queue for far longer than
/// the windows last, and the rows it retains stay where the windows hold them.
pub async fn retained_rows_stay_flat_under_churn(
    store: &dyn QueueStore,
    clock: &TestClock,
    queue: QueueAddress,
    retained: RetainedRows<'_>,
    rounds: i64,
) {
    let step = 6 * MINUTE;
    let start = 100 * HOUR;
    clock.set(start);
    store
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema: schema_with(retention(
                30 * MINUTE,
                DeletedKeyPolicy::Retired {
                    window_nanos: u64::try_from(HOUR).unwrap(),
                },
            )),
        })
        .await
        .unwrap();
    let mut peak = 0;
    for round in 0..rounds {
        let at = start + round * step;
        clock.set(at);
        let key = format!("churn-{round}");
        store
            .insert(insert_request(
                &queue,
                &format!("churn-insert-{round}"),
                InsertMode::InsertOnly,
                &key,
                at,
            ))
            .await
            .unwrap();
        claim_and_delete(store, &queue, &key, &format!("churn-delete-{round}"), at).await;
        // The pass this tick starts finishes before the count.
        tick(store, &queue).await;
        tick(store, &queue).await;
        peak = peak.max(retained().await);
    }
    // Per round: an insert, a claim and a delete request, and one deleted row. The
    // request window holds five rounds and the deleted key window ten.
    assert!(peak < 80, "peak {peak} after {rounds} rounds");
    assert!(retained().await < 80);
}
