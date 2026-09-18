use super::*;
use bytes::Bytes;

use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, CompoundIndexDef, CompoundIndexField, EligibilityPolicy,
    GateKeyPolicy, GroupByField, GroupKey, IndexDef, LeaseToken, Metadata, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, FinalizeCommand, FinalizeKind, FinalizeOutcome, PauseQueueCommand,
    PayloadUpdate, PushCommand, PushItem, QueueCommand, RequestOutcome, ScheduleUpdate,
    SetGatesCommand, UpdateFieldsCommand,
};

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn single(name: &str, field: &str, kind: IndexType, unique: bool) -> QueueIndex {
    QueueIndex {
        name: name.into(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: field.into(),
            index_type: kind,
            unique,
        }),
    }
}

fn definition() -> QueueDefinition {
    let compound = |name: &str, second: &str, kind| QueueIndex {
        name: name.into(),
        declaration: IndexDeclaration::Compound(CompoundIndexDef {
            fields: vec![
                CompoundIndexField {
                    field: "kind".into(),
                    index_type: IndexType::String,
                },
                CompoundIndexField {
                    field: second.into(),
                    index_type: kind,
                },
            ],
            unique: false,
        }),
    };
    QueueDefinition {
        tenant_id: TenantId::new("native-query").unwrap(),
        queue_id: QueueId::new("recipients").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(64),
            max_gates_per_request: Some(64),
            ..EligibilityPolicy::default()
        },
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        }),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: Some(100),
        secondary_indexes: Vec::new(),
        entity_schema: None,
        typed_indexes: vec![
            compound("by_kind_score", "score", IndexType::Integer),
            compound("by_kind_due", "due", IndexType::Datetime),
            single("by_email", "email", IndexType::String, true),
            single("by_label", "label", IndexType::String, false),
        ],
        emit_change_records: false,
    }
}

fn recipient(id: u64, kind: &str, label: &str, score: Option<i64>, due: i64) -> PushItem {
    let mut fields = BTreeMap::from([
        ("kind".into(), TypedValue::String(kind.into())),
        ("label".into(), TypedValue::String(label.into())),
        (
            "email".into(),
            TypedValue::String(format!("recipient-{id}@example.test")),
        ),
        ("due".into(), TypedValue::DateTime(ts(due))),
    ]);
    if let Some(score) = score {
        fields.insert("score".into(), TypedValue::Integer(score));
    }
    PushItem {
        client_item_key: ClientItemKey::new(format!("recipient-{id}")).unwrap(),
        item_id: ItemId::from_u64(id),
        priority: None,
        not_before: None,
        group_key: None,
        max_attempts: 3,
        payload: Some(bytes::Bytes::from_static(
            b"private payload excluded from index query",
        )),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        index_fields: fields,
        entity_document: None,
    }
}

async fn apply(
    store: &TursoRelational,
    shard: &QueueKey,
    sequence: &mut u64,
    command: QueueCommand,
    ids: Vec<ItemId>,
    now: i64,
) {
    let envelope = CommandEnvelope {
        command_id: CommandId::new(format!("query-test-{sequence}")),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids: ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(now),
    };
    AsyncProjectionStore::apply_live(
        store,
        vec![CommandPosition::new(shard.clone(), 1, *sequence)],
        vec![envelope],
    )
    .await
    .unwrap();
    *sequence += 1;
}

async fn fixture(items: Vec<PushItem>) -> (TursoRelational, QueueKey, u64) {
    let store = TursoRelational::in_memory().await.unwrap();
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let mut sequence = 0;
    let ids = items.iter().map(|item| item.item_id).collect();
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Push(PushCommand { items }),
        ids,
        1,
    )
    .await;
    (store, shard, sequence)
}

fn order(field: &str, direction: SortDirection) -> OrderField {
    OrderField {
        field: field.into(),
        direction,
    }
}

fn eq_kind(kind: &str) -> QueryFilter {
    QueryFilter {
        field: "kind".into(),
        op: FilterOp::Eq,
        value: TypedValue::String(kind.into()),
    }
}

fn scan(index: &str, field: &str) -> RangeScanRequest {
    RangeScanRequest {
        index: Some(index.into()),
        filters: Vec::new(),
        order_by: vec![order(field, SortDirection::Ascending)],
        page_size: 2,
        cursor: None,
    }
}

fn ids(page: &RangeScanResponse) -> Vec<u64> {
    page.rows.iter().map(|row| row.item_id.as_u64()).collect()
}

#[tokio::test]
async fn native_range_uses_typed_order_numeric_id_ties_and_validated_keyset_cursors() {
    let anchor = recipient(9, "send", "a", Some(10), 3_600);
    let (store, shard, mut sequence) = fixture(vec![
        recipient(10, "send", "a", Some(10), 3_600),
        recipient(2, "send", "aa", Some(20), 7_200),
        recipient(u64::MAX, "send", "b", Some(30), 7_200),
        recipient(1, "other", "z", Some(40), 3_600),
        anchor.clone(),
    ])
    .await;
    let request = scan("by_label", "label");
    let first = store
        .server_range_scan(&shard, request.clone())
        .await
        .unwrap();
    assert_eq!(ids(&first), vec![9, 10]);
    assert_eq!(
        first.rows[0]
            .fields
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["label"]
    );
    let second = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                cursor: first.next_cursor.clone(),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&second), vec![2, u64::MAX]);
    let third = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                cursor: second.next_cursor,
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&third), vec![1]);
    assert!(third.next_cursor.is_none());
    let descending = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                order_by: vec![order("label", SortDirection::Descending)],
                page_size: 100,
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&descending), vec![1, u64::MAX, 2, 9, 10]);

    let filters = vec![
        eq_kind("send"),
        QueryFilter {
            field: "score".into(),
            op: FilterOp::Gte,
            value: TypedValue::Integer(10),
        },
        QueryFilter {
            field: "score".into(),
            op: FilterOp::Lt,
            value: TypedValue::Integer(30),
        },
    ];
    let compound = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                filters: filters.clone(),
                page_size: 100,
                ..scan("by_kind_score", "score")
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&compound), vec![9, 10, 2]);
    let later_only = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                filters: vec![filters[2].clone()],
                page_size: 100,
                ..scan("by_kind_score", "score")
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&later_only), vec![9, 10, 2]);
    let malformed = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                cursor: Some(QueryCursor("not a cursor".into())),
                ..request.clone()
            },
        )
        .await;
    assert!(matches!(
        malformed,
        Err(EngineError::Invalid("cursor-invalidated"))
    ));
    let changed = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                cursor: first.next_cursor.clone(),
                order_by: vec![order("label", SortDirection::Descending)],
                ..request.clone()
            },
        )
        .await;
    assert!(matches!(
        changed,
        Err(EngineError::Invalid("cursor-invalidated"))
    ));

    let mut entity =
        fireweed_engine::index_fields::index_fields_as_entity(&anchor.index_fields).unwrap();
    entity["label"] = "changed".into();
    // Cursor anchor is id 10; change it through ordinary durable projection apply.
    entity["email"] = "recipient-10@example.test".into();
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::UpdateFields(UpdateFieldsCommand {
            item_id: ItemId::from_u64(10),
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Keep,
            set_priority: ScheduleUpdate::Keep,
            set_not_before: ScheduleUpdate::Keep,
            set_entity_document: Some(entity),
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: false,
            client_item_key: None,
            expected_item_version: None,
        }),
        vec![ItemId::from_u64(10)],
        2,
    )
    .await;
    let invalidated = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                cursor: first.next_cursor,
                ..request
            },
        )
        .await;
    assert!(matches!(
        invalidated,
        Err(EngineError::Invalid("cursor-invalidated"))
    ));
}

#[tokio::test]
async fn native_aggregates_cover_sparse_nulls_time_buckets_and_filtered_dispositions() {
    let (store, shard, mut sequence) = fixture(vec![
        recipient(1, "send", "one", Some(10), 3_610),
        recipient(2, "send", "two", Some(20), 3_620),
        recipient(3, "send", "three", None, 7_210),
        recipient(4, "other", "four", Some(30), 7_220),
    ])
    .await;
    let request = GroupedAggregateRequest {
        index: Some("by_kind_due".into()),
        filters: vec![eq_kind("send")],
        group_by: vec![GroupByField {
            field: "due".into(),
            time_bucket: Some(TimeBucket::Hour),
        }],
        max_groups: 2,
    };
    let groups = store
        .server_grouped_aggregate(&shard, request.clone())
        .await
        .unwrap();
    assert_eq!(
        groups
            .groups
            .iter()
            .map(|group| group.count)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(groups.groups[0].key["due"], TypedValue::DateTime(ts(3_600)));
    assert!(matches!(
        store
            .server_grouped_aggregate(
                &shard,
                GroupedAggregateRequest {
                    max_groups: 1,
                    ..request
                }
            )
            .await,
        Err(EngineError::Invalid("aggregate-too-large"))
    ));
    let buckets = store
        .server_declared_bucket_segment(
            &shard,
            DeclaredBucketSegmentRequest {
                index: Some("by_kind_score".into()),
                filters: vec![eq_kind("send")],
                field: "score".into(),
                buckets: vec![
                    fireweed_core::BucketRule {
                        label: "low".into(),
                        exact: None,
                        gt: None,
                        gte: Some(0.0),
                        lt: Some(15.0),
                        lte: None,
                    },
                    fireweed_core::BucketRule {
                        label: "high".into(),
                        exact: None,
                        gt: None,
                        gte: Some(15.0),
                        lt: None,
                        lte: None,
                    },
                ],
                null_bucket_label: "unenriched".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        buckets.buckets,
        vec![
            BucketCount {
                label: "low".into(),
                count: 1
            },
            BucketCount {
                label: "high".into(),
                count: 1
            },
            BucketCount {
                label: "unenriched".into(),
                count: 1
            }
        ]
    );
    let partial_index_metrics = store
        .server_metrics_by_query(
            &shard,
            MetricsByQueryRequest {
                index: Some("by_kind_score".into()),
                filters: vec![eq_kind("send")],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        partial_index_metrics.pending, 3,
        "unenriched rows still count towards campaign progress"
    );
    let token = LeaseToken::new("query-metrics-lease").unwrap();
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![ItemId::from_u64(1)],
            lease_token: token,
            lease_expires_at: ts(30),
            worker_id: None,
            authority_first: true,
        }),
        vec![ItemId::from_u64(1)],
        2,
    )
    .await;
    let metrics_request = MetricsByQueryRequest {
        index: Some("by_kind_score".into()),
        filters: vec![
            QueryFilter {
                field: "score".into(),
                op: FilterOp::Gte,
                value: TypedValue::Integer(10),
            },
            QueryFilter {
                field: "score".into(),
                op: FilterOp::Lte,
                value: TypedValue::Integer(20),
            },
        ],
    };
    let metrics = store
        .server_metrics_by_query(&shard, metrics_request.clone())
        .await
        .unwrap();
    assert_eq!((metrics.pending, metrics.leased), (1, 1));
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: ItemId::from_u64(1),
                kind: FinalizeKind::Complete,
                applied_state: Some(fireweed_core::ItemState::Complete),
                not_before: None,
            }],
        }),
        vec![ItemId::from_u64(1)],
        3,
    )
    .await;
    let metrics = store
        .server_metrics_by_query(&shard, metrics_request)
        .await
        .unwrap();
    assert_eq!(
        (
            metrics.pending,
            metrics.complete,
            metrics.resident_terminal_count
        ),
        (1, 1, 1)
    );
    let queue = store
        .server_metrics_by_query(
            &shard,
            MetricsByQueryRequest {
                index: None,
                filters: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!((queue.pending, queue.complete), (3, 1));
    let exact = store
        .server_index_get_unique(&shard, "by_email", &[b"recipient-1@example.test".to_vec()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.item_id, ItemId::from_u64(1));
    assert_eq!(exact.item_version, 3);
    assert!(matches!(
        store
            .server_index_get_unique(&shard, "by_label", &[b"one".to_vec()])
            .await,
        Err(EngineError::Invalid("secondary index is not unique"))
    ));
    assert!(matches!(
        store
            .server_index_lookup(&shard, "by_kind_score", &[b"send".to_vec()])
            .await,
        Err(EngineError::Invalid("secondary index key arity mismatch"))
    ));
}

#[tokio::test]
async fn native_query_claims_and_discovery_respect_time_gates_pause_and_cohort_boundaries() {
    let mut future = recipient(2, "send", "future", Some(1), 0);
    future.not_before = Some(ts(20));
    let mut blocked = recipient(3, "send", "blocked", Some(1), 0);
    blocked.gate_keys = vec!["blocked".into()];
    let mut cohort = recipient(4, "send", "cohort", Some(1), 0);
    cohort.cohort_size = Some(2);
    cohort.group_key = Some(GroupKey::new("cohort").unwrap());
    let (store, shard, mut sequence) = fixture(vec![
        recipient(1, "send", "ready", Some(1), 10),
        future,
        blocked,
        cohort,
        recipient(5, "send", "leased", Some(1), 0),
        recipient(6, "send", "exhausted", Some(1), 0),
        recipient(7, "send", "fenced", Some(1), 0),
    ])
    .await;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["blocked".into()],
            blocked: true,
        }),
        Vec::new(),
        2,
    )
    .await;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![ItemId::from_u64(5)],
            lease_token: LeaseToken::new("leased").unwrap(),
            lease_expires_at: ts(3),
            worker_id: None,
            authority_first: true,
        }),
        vec![ItemId::from_u64(5)],
        2,
    )
    .await;
    // Inject legal persisted eligibility states directly to exercise the selection guards separately
    // from the transition planner (which normally prevents exhausted Pending records).
    {
        let writer = store.writer.lock().await;
        writer
            .execute(
                "UPDATE fireweed_items SET retry_count=max_attempts WHERE item_id='6'",
                (),
            )
            .await
            .unwrap();
        writer
            .execute("UPDATE fireweed_items SET fenced=1 WHERE item_id='7'", ())
            .await
            .unwrap();
    }
    let order_by = order("due", SortDirection::Ascending);
    // Check the actual declared-index query shape, including its item-table correlation.
    // Seeing an index name alone is insufficient: both gate probes must bind the full key.
    {
        let definition = definition();
        let spec = index_spec(&definition, Some("by_kind_due")).unwrap();
        let filters = [eq_kind("send")];
        let mut query = IndexedSql::new(&shard, spec, &filters).unwrap();
        let direction = query
            .native_order(spec, &filters, std::slice::from_ref(&order_by))
            .unwrap()
            .unwrap();
        let now_bind = query.bind(ts_nanos(ts(10)));
        query.predicates.extend([
            "i.lifecycle_state='Pending'".into(),
            "i.fenced=0".into(),
            "i.cohort_size IS NULL".into(),
            "i.retry_count<i.max_attempts".into(),
            format!("(i.not_before IS NULL OR i.not_before<={now_bind})"),
            UNBLOCKED.into(),
        ]);
        let limit = query.bind(100_i64);
        let sql = query.select(
            "i.item_id",
            &format!("ORDER BY {} LIMIT {limit}", native_order_sql(direction)),
        );
        let connection = store.reader.lock().await;
        let plan = rows_on(
            &connection,
            &format!("EXPLAIN QUERY PLAN {sql}"),
            query.params,
        )
        .await
        .unwrap();
        let details = plan
            .iter()
            .map(|row| text(&row[3]).unwrap())
            .collect::<Vec<_>>();
        for (table, key) in [("ig", "item_id"), ("gs", "gate_key")] {
            assert!(
                details.iter().any(|line| {
                    line.starts_with(&format!("SEARCH {table} USING INDEX"))
                        && line.contains(&format!("tenant_id=? AND queue_id=? AND {key}=?"))
                }),
                "query eligibility must seek each full gate key: {details:?}"
            );
        }
    }
    let selected = store
        .server_select_claim_by_query(
            &shard,
            Some("by_kind_due"),
            &[eq_kind("send")],
            &order_by,
            100,
            ts(10),
        )
        .await
        .unwrap();
    assert_eq!(selected, vec![ItemId::from_u64(1)]);
    let ids = (1..=8).map(ItemId::from_u64).collect::<Vec<_>>();
    let classes = store
        .server_classify_claim_by_item_ids(&shard, &ids, ts(10))
        .await
        .unwrap();
    assert_eq!(
        classes
            .into_iter()
            .map(|(_, class)| class)
            .collect::<Vec<_>>(),
        vec![
            ClaimByItemIdClass::Claimable,
            ClaimByItemIdClass::NotEligible,
            ClaimByItemIdClass::NotEligible,
            ClaimByItemIdClass::NotEligible,
            ClaimByItemIdClass::Leased,
            ClaimByItemIdClass::NotEligible,
            ClaimByItemIdClass::NotEligible,
            ClaimByItemIdClass::NotFound
        ]
    );
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::PauseQueue(PauseQueueCommand {
            drain_intake: false,
        }),
        Vec::new(),
        3,
    )
    .await;
    assert!(
        store
            .server_select_claim_by_query(&shard, Some("by_kind_due"), &[], &order_by, 100, ts(30))
            .await
            .unwrap()
            .is_empty()
    );
    // Discovery reports intrinsic pending backlog even during pause. Future-only due crossings are
    // visible without any intervening mutation; blocked rows never inflate the reported count.
    let before = store
        .server_discover_active_scopes(&shard, DiscoveryGranularity::Queue, ts(10))
        .await
        .unwrap();
    let later = store
        .server_discover_active_scopes(&shard, DiscoveryGranularity::Queue, ts(30))
        .await
        .unwrap();
    assert_eq!(
        later[0].eligible_count.unwrap(),
        before[0].eligible_count.unwrap() + 1
    );
    assert_eq!(later[0].progress_bound_risk_count, None);
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::ResumeQueue,
        Vec::new(),
        4,
    )
    .await;
    let selected = store
        .server_select_claim_by_query(
            &shard,
            Some("by_kind_due"),
            &[eq_kind("send")],
            &order_by,
            100,
            ts(30),
        )
        .await
        .unwrap();
    assert_eq!(selected, vec![ItemId::from_u64(2), ItemId::from_u64(1)]);
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["blocked".into()],
            blocked: false,
        }),
        Vec::new(),
        5,
    )
    .await;
    let selected = store
        .server_select_claim_by_query(
            &shard,
            Some("by_kind_due"),
            &[eq_kind("send")],
            &order_by,
            100,
            ts(30),
        )
        .await
        .unwrap();
    assert_eq!(
        selected,
        vec![
            ItemId::from_u64(2),
            ItemId::from_u64(3),
            ItemId::from_u64(1)
        ]
    );
    assert_eq!(
        store
            .server_classify_claim_by_item_ids(&shard, &[ItemId::from_u64(3)], ts(30))
            .await
            .unwrap(),
        vec![(ItemId::from_u64(3), ClaimByItemIdClass::Claimable)]
    );
    let reopened = store
        .server_discover_active_scopes(&shard, DiscoveryGranularity::Queue, ts(30))
        .await
        .unwrap();
    assert_eq!(
        reopened[0].eligible_count.unwrap(),
        later[0].eligible_count.unwrap() + 1
    );
}

#[tokio::test]
async fn native_bounded_mutation_pages_preserve_versions_and_reserve_unique_keys_globally() {
    let (store, shard, _) = fixture(vec![
        recipient(1, "send", "one", Some(1), 0),
        recipient(2, "send", "two", Some(2), 0),
        recipient(3, "send", "three", Some(3), 0),
        recipient(4, "other", "outside", Some(4), 0),
    ])
    .await;
    let request = BoundedMutationRequest {
        index: Some("by_kind_score".into()),
        filters: vec![eq_kind("send")],
        set_fields: BTreeMap::from([(
            "email".into(),
            TypedValue::String("shared@example.test".into()),
        )]),
        max_scan_rows: 1,
    };
    let plan = store
        .server_plan_bounded_mutation(&shard, request.clone())
        .await
        .unwrap();
    assert_eq!(
        plan.response
            .results
            .iter()
            .map(|result| result.outcome)
            .collect::<Vec<_>>(),
        vec![
            MutationOutcome::Updated,
            MutationOutcome::Conflict,
            MutationOutcome::Conflict
        ]
    );
    assert_eq!(plan.updates.len(), 1);
    assert_eq!(plan.updates[0].expected_item_version, 1);
    assert_eq!(
        plan.updates[0]
            .command
            .set_entity_document
            .as_ref()
            .unwrap()["kind"],
        "send"
    );
    // Planning writes nothing and retains original exact-index occupancy.
    assert!(
        store
            .server_index_get_unique(&shard, "by_email", &[b"shared@example.test".to_vec()])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .server_index_get_unique(&shard, "by_email", &[b"recipient-1@example.test".to_vec()])
            .await
            .unwrap()
            .is_some()
    );
    let outside = store
        .server_plan_bounded_mutation(
            &shard,
            BoundedMutationRequest {
                set_fields: BTreeMap::from([(
                    "email".into(),
                    TypedValue::String("recipient-4@example.test".into()),
                )]),
                ..request.clone()
            },
        )
        .await
        .unwrap();
    assert!(outside.updates.is_empty());
    assert!(
        outside
            .response
            .results
            .iter()
            .all(|result| result.outcome == MutationOutcome::Conflict)
    );
    let batched = store
        .server_plan_bounded_mutation(
            &shard,
            BoundedMutationRequest {
                max_scan_rows: 2,
                ..request
            },
        )
        .await
        .unwrap();
    assert_eq!(batched.response, plan.response);
}

#[tokio::test]
async fn native_query_receipt_checks_conflict_before_expiry_and_keeps_the_original_token() {
    let (store, shard, mut sequence) = fixture(vec![recipient(1, "send", "one", Some(1), 0)]).await;
    let id = ItemId::from_u64(1);
    let token = LeaseToken::new("original-query-token").unwrap();
    let request = RequestId::new("query-receipt").unwrap();
    let envelope = CommandEnvelope {
        command_id: CommandId::new("query-receipt-command"),
        request_id: Some(request.clone()),
        request_fingerprint: Some(17),
        request_outcome: Some(RequestOutcome::ClaimByQuery {
            item_ids: vec![id],
            lease_token: token.clone(),
            worker_id: None,
        }),
        item_ids: vec![id],
        command: QueueCommand::Claim(ClaimCommand {
            item_ids: vec![id],
            lease_token: token.clone(),
            lease_expires_at: ts(30),
            worker_id: None,
            authority_first: true,
        }),
        checksum: CommandChecksum(0),
        created_at: ts(2),
    };
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 1, sequence)],
        vec![envelope],
    )
    .await
    .unwrap();
    sequence += 1;
    let receipt = store
        .server_query_claim_replay(&shard, "claim_by_query", &request, 17, ts(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt["lease_token"], token.as_str());
    assert_eq!(receipt["item_ids"], serde_json::json!([id]));
    assert!(matches!(
        store
            .server_query_claim_replay(&shard, "claim_by_query", &request, 18, ts(100))
            .await,
        Err(EngineError::RequestIdConflict)
    ));
    assert!(matches!(
        store
            .server_query_claim_replay(&shard, "claim_by_query", &request, 17, ts(100))
            .await,
        Err(EngineError::RequestExpired)
    ));
    assert!(
        store
            .server_query_claim_replay(
                &shard,
                "claim_by_query",
                &RequestId::new("absent").unwrap(),
                17,
                ts(3)
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(sequence, 2);
}

#[test]
fn native_query_sql_bounds_declared_keys_and_rejects_unknown_fields_before_reading_rows() {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let spec = index_spec(&definition, Some("by_kind_score")).unwrap();
    let query = IndexedSql::new(
        &shard,
        spec,
        &[
            eq_kind("send"),
            QueryFilter {
                field: "score".into(),
                op: FilterOp::Gte,
                value: TypedValue::Integer(10),
            },
        ],
    )
    .unwrap();
    assert!(
        query
            .ctes
            .contains("INDEXED BY fireweed_item_index_key_numeric_asc_idx")
    );
    assert_eq!(
        query.ctes.matches("index_key>=").count(),
        2,
        "equality prefix and numeric range both seek the covering index"
    );
    assert!(!query.ctes.contains("entity_document"));
    assert!(!query.ctes.contains("payload"));
    assert!(matches!(
        IndexedSql::new(
            &shard,
            spec,
            &[QueryFilter {
                field: "undeclared".into(),
                op: FilterOp::Eq,
                value: TypedValue::Integer(1)
            }]
        ),
        Err(EngineError::Invalid("unindexed-field"))
    ));
    assert!(matches!(
        IndexedSql::new(
            &shard,
            spec,
            &[QueryFilter {
                field: "score".into(),
                op: FilterOp::Eq,
                value: TypedValue::String("wrong".into())
            }]
        ),
        Err(EngineError::Invalid(_))
    ));
}

#[tokio::test]
async fn native_legacy_exact_indexes_preserve_opaque_bytes_and_update_occupancy() {
    let mut definition = definition();
    definition.secondary_indexes = vec![
        fireweed_core::IndexSpec {
            name: "opaque_identity".into(),
            fields: vec!["account".into(), "tag".into()],
            unique: true,
        },
        fireweed_core::IndexSpec {
            name: "opaque_tag".into(),
            fields: vec!["tag".into()],
            unique: false,
        },
    ];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .unwrap();
    let opaque = vec![0xff, 0, 1];
    let mut first = recipient(1, "send", "one", Some(1), 0);
    first.fields = BTreeMap::from([
        ("account".into(), bytes::Bytes::from(opaque.clone())),
        ("tag".into(), bytes::Bytes::from_static(b"shared")),
    ]);
    let mut second = recipient(2, "send", "two", Some(2), 0);
    second.fields = BTreeMap::from([
        ("account".into(), bytes::Bytes::from_static(&[0xff, 0, 2])),
        ("tag".into(), bytes::Bytes::from_static(b"shared")),
    ]);
    let mut sequence = 0;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Push(PushCommand {
            items: vec![first.clone(), second.clone()],
        }),
        vec![first.item_id, second.item_id],
        1,
    )
    .await;
    let hit = store
        .server_index_get_unique(
            &shard,
            "opaque_identity",
            &[opaque.clone(), b"shared".to_vec()],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hit.item_id, first.item_id);
    assert_eq!(
        store
            .server_index_lookup(&shard, "opaque_tag", &[b"shared".to_vec()])
            .await
            .unwrap()
            .iter()
            .map(|hit| hit.item_id)
            .collect::<Vec<_>>(),
        vec![first.item_id, second.item_id]
    );
    let mut duplicate = recipient(3, "send", "duplicate", Some(3), 0);
    duplicate.fields = first.fields.clone();
    assert!(matches!(
        AsyncProjectionStore::validate_push(&store, shard.clone(), vec![duplicate], ts(2)).await,
        Err(EngineError::Conflict)
    ));
    let command = UpdateFieldsCommand {
        item_id: second.item_id,
        field_ops: BTreeMap::from([("tag".into(), Some(bytes::Bytes::from_static(b"changed")))]),
        payload: PayloadUpdate::Keep,
        set_priority: ScheduleUpdate::Keep,
        set_not_before: ScheduleUpdate::Keep,
        set_entity_document: None,
        set_fields: None,
        set_metadata: None,
        set_gate_keys: None,
        api001_batch: false,
        client_item_key: None,
        expected_item_version: None,
    };
    assert_eq!(
        store
            .server_validate_update_fields(&shard, &definition, &command, Some(1))
            .await
            .unwrap(),
        1
    );
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::UpdateFields(command),
        vec![second.item_id],
        2,
    )
    .await;
    assert_eq!(
        store
            .server_index_lookup(&shard, "opaque_tag", &[b"shared".to_vec()])
            .await
            .unwrap()
            .iter()
            .map(|hit| hit.item_id)
            .collect::<Vec<_>>(),
        vec![first.item_id]
    );
    let updated = store
        .server_index_get_unique(
            &shard,
            "opaque_identity",
            &[vec![0xff, 0, 2], b"changed".to_vec()],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!((updated.item_id, updated.item_version), (second.item_id, 2));
}

#[tokio::test]
async fn native_terminal_emission_metrics_use_terminal_positions_and_clamp_clock_skew() {
    let (store, shard, mut sequence) = fixture(vec![
        recipient(1, "send", "one", Some(1), 0),
        recipient(2, "send", "two", Some(2), 0),
        recipient(3, "send", "three", Some(3), 0),
    ])
    .await;
    let ids = (1..=3).map(ItemId::from_u64).collect::<Vec<_>>();
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Claim(ClaimCommand {
            item_ids: ids.clone(),
            lease_token: LeaseToken::new("emission-lease").unwrap(),
            lease_expires_at: ts(30),
            worker_id: None,
            authority_first: true,
        }),
        ids.clone(),
        2,
    )
    .await;
    let outcome = |id, kind| FinalizeOutcome {
        item_id: ItemId::from_u64(id),
        kind,
        applied_state: Some(if kind == FinalizeKind::Complete {
            fireweed_core::ItemState::Complete
        } else {
            fireweed_core::ItemState::Failed
        }),
        not_before: None,
    };
    let first_terminal = CommandPosition::new(shard.clone(), 1, sequence);
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![outcome(1, FinalizeKind::Complete)],
        }),
        vec![ids[0]],
        3,
    )
    .await;
    let last_terminal = CommandPosition::new(shard.clone(), 1, sequence);
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![
                outcome(2, FinalizeKind::Complete),
                outcome(3, FinalizeKind::Fail),
            ],
        }),
        ids[1..].to_vec(),
        5,
    )
    .await;

    let all = store
        .server_terminal_emission_metrics_at(&shard, ts(10), true, None)
        .await
        .unwrap();
    assert_eq!(
        (
            all.resident_terminal_count,
            all.emission_lag_commands,
            all.emission_oldest_unemitted_age_ms
        ),
        (3, 3, 7_000)
    );
    let after_first = store
        .server_terminal_emission_metrics_at(&shard, ts(10), true, Some(&first_terminal))
        .await
        .unwrap();
    assert_eq!(
        (
            after_first.resident_terminal_count,
            after_first.emission_lag_commands,
            after_first.emission_oldest_unemitted_age_ms
        ),
        (3, 2, 5_000),
        "a single command containing two terminal records contributes two lagging records"
    );
    let caught_up = store
        .server_terminal_emission_metrics_at(&shard, ts(10), true, Some(&last_terminal))
        .await
        .unwrap();
    assert_eq!(
        (
            caught_up.resident_terminal_count,
            caught_up.emission_lag_commands,
            caught_up.emission_oldest_unemitted_age_ms
        ),
        (3, 0, 0)
    );
    let older_epoch = CommandPosition::new(shard.clone(), 0, 999);
    assert_eq!(
        store
            .server_terminal_emission_metrics_at(&shard, ts(10), true, Some(&older_epoch))
            .await
            .unwrap()
            .emission_lag_commands,
        3
    );
    let newer_epoch = CommandPosition::new(shard.clone(), 2, 0);
    assert_eq!(
        store
            .server_terminal_emission_metrics_at(&shard, ts(10), true, Some(&newer_epoch))
            .await
            .unwrap()
            .emission_lag_commands,
        0
    );
    let disabled = store
        .server_terminal_emission_metrics_at(&shard, ts(10), false, None)
        .await
        .unwrap();
    assert_eq!(
        (
            disabled.resident_terminal_count,
            disabled.emission_lag_commands,
            disabled.emission_oldest_unemitted_age_ms
        ),
        (3, 0, 0)
    );
    assert_eq!(
        store
            .server_terminal_emission_metrics_at(&shard, ts(2), true, None)
            .await
            .unwrap()
            .emission_oldest_unemitted_age_ms,
        0
    );
    // Older checkpoints can contain terminal rows without a terminal position or timestamp. Such
    // rows remain resident, while missing positions do not manufacture emission lag.
    {
        let writer = store.writer.lock().await;
        writer
            .execute(
                "UPDATE fireweed_items SET terminal_command_epoch=NULL WHERE item_id='3'",
                (),
            )
            .await
            .unwrap();
        writer
            .execute(
                "UPDATE fireweed_items SET terminal_at=NULL WHERE item_id='2'",
                (),
            )
            .await
            .unwrap();
    }
    let legacy = store
        .server_terminal_emission_metrics_at(&shard, ts(10), true, None)
        .await
        .unwrap();
    assert_eq!(
        (
            legacy.resident_terminal_count,
            legacy.emission_lag_commands,
            legacy.emission_oldest_unemitted_age_ms
        ),
        (3, 2, 7_000)
    );
}

#[tokio::test]
async fn native_upsert_prevalidation_excludes_only_the_predecessor_and_never_changes_rows() {
    let mut definition = definition();
    definition.cohort_policy = None;
    definition.max_eligible_group_size = None;
    definition.secondary_indexes = vec![fireweed_core::IndexSpec {
        name: "opaque_unique".into(),
        fields: vec!["opaque".into()],
        unique: true,
    }];
    definition.entity_schema = Some(
        serde_json::from_value(serde_json::json!({
            "entity_schema": { "type": "object", "properties": { "name": { "type": "string" } } }
        }))
        .unwrap(),
    );
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .unwrap();
    let mut first = recipient(1, "send", "one", Some(1), 0);
    first
        .fields
        .insert("opaque".into(), bytes::Bytes::from_static(&[0xff, 1]));
    let mut second = recipient(2, "send", "two", Some(2), 0);
    second
        .fields
        .insert("opaque".into(), bytes::Bytes::from_static(&[0xff, 2]));
    let mut sequence = 0;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Push(PushCommand {
            items: vec![first.clone(), second.clone()],
        }),
        vec![first.item_id, second.item_id],
        1,
    )
    .await;
    let mut replacement = first.clone();
    replacement.item_id = ItemId::from_u64(3);
    // Reusing the predecessor's typed AND opaque unique values is allowed.
    store
        .server_validate_upsert(
            &shard,
            &definition,
            &replacement,
            Some(first.item_id),
            ts(2),
        )
        .await
        .unwrap();
    let mut typed_collision = replacement.clone();
    typed_collision
        .index_fields
        .insert("email".into(), second.index_fields["email"].clone());
    assert!(matches!(
        store
            .server_validate_upsert(
                &shard,
                &definition,
                &typed_collision,
                Some(first.item_id),
                ts(2)
            )
            .await,
        Err(EngineError::Conflict)
    ));
    let mut opaque_collision = replacement.clone();
    opaque_collision.fields = second.fields.clone();
    assert!(matches!(
        store
            .server_validate_upsert(
                &shard,
                &definition,
                &opaque_collision,
                Some(first.item_id),
                ts(2)
            )
            .await,
        Err(EngineError::Conflict)
    ));
    let mut bad_priority = replacement.clone();
    bad_priority.priority = Some(fireweed_core::PriorityValue::Text("wrong-model".into()));
    assert!(matches!(
        store
            .server_validate_upsert(
                &shard,
                &definition,
                &bad_priority,
                Some(first.item_id),
                ts(2)
            )
            .await,
        Err(EngineError::Invalid("priority does not match queue model"))
    ));
    let mut bad_gates = replacement.clone();
    bad_gates.gate_keys = vec!["invalid gate key".into()];
    assert!(matches!(
        store
            .server_validate_upsert(&shard, &definition, &bad_gates, Some(first.item_id), ts(2))
            .await,
        Err(EngineError::Invalid("invalid gate key"))
    ));
    let mut bad_entity = replacement.clone();
    bad_entity.entity_document = Some(serde_json::json!({ "name": 123 }));
    assert!(matches!(
        store
            .server_validate_upsert(&shard, &definition, &bad_entity, Some(first.item_id), ts(2))
            .await,
        Err(EngineError::EntitySchemaViolation(_))
    ));
    assert_eq!(
        AsyncProjectionStore::item_state(&store, shard.clone(), first.item_id)
            .await
            .unwrap(),
        Some(fireweed_core::ItemState::Pending)
    );
    assert_eq!(
        AsyncProjectionStore::item_state(&store, shard.clone(), replacement.item_id)
            .await
            .unwrap(),
        None
    );
    let original = store
        .server_index_get_unique(&shard, "by_email", &[b"recipient-1@example.test".to_vec()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (original.item_id, original.item_version),
        (first.item_id, 1)
    );
    assert_eq!(store.server_metrics(&shard).await.unwrap().pending, 2);

    // The inserted-new-key branch still consults retained client keys after terminal purge.
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![first.item_id],
            lease_token: LeaseToken::new("upsert-retention").unwrap(),
            lease_expires_at: ts(30),
            worker_id: None,
            authority_first: true,
        }),
        vec![first.item_id],
        3,
    )
    .await;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: first.item_id,
                kind: FinalizeKind::Complete,
                applied_state: Some(fireweed_core::ItemState::Complete),
                not_before: None,
            }],
        }),
        vec![first.item_id],
        4,
    )
    .await;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::PurgeItems(fireweed_engine::PurgeItemsCommand {
            item_ids: vec![first.item_id],
            force: true,
        }),
        vec![first.item_id],
        5,
    )
    .await;
    assert!(matches!(
        store
            .server_validate_upsert(&shard, &definition, &replacement, None, ts(6))
            .await,
        Err(EngineError::Conflict)
    ));
    store
        .server_validate_upsert(&shard, &definition, &replacement, None, ts(100))
        .await
        .unwrap();
    assert_eq!(
        store.server_metrics(&shard).await.unwrap().pending,
        1,
        "validation of a now-admissible insertion still appends nothing"
    );
}

#[tokio::test]
async fn native_ordered_numeric_and_timestamp_plans_seek_without_a_temporary_sort() {
    let item_ids = [u64::MAX, 10, 2, 100, 9, 1, u64::MAX - 1];
    let (store, shard, _) = fixture(
        item_ids
            .iter()
            .map(|id| recipient(*id, "send", &format!("item-{id}"), Some(10), 3_600))
            .collect(),
    )
    .await;
    let definition = definition();
    for (index, field, bound) in [
        ("by_kind_score", "score", TypedValue::Integer(10)),
        ("by_kind_due", "due", TypedValue::DateTime(ts(3_600))),
    ] {
        let filters = vec![
            eq_kind("send"),
            QueryFilter {
                field: field.into(),
                op: FilterOp::Gte,
                value: bound.clone(),
            },
        ];
        for direction in [SortDirection::Ascending, SortDirection::Descending] {
            let spec = index_spec(&definition, Some(index)).unwrap();
            let mut query = IndexedSql::new(&shard, spec, &filters).unwrap();
            assert_eq!(
                query
                    .native_order(spec, &filters, &[order(field, direction)])
                    .unwrap(),
                Some(direction)
            );
            assert!(
                query.ctes.is_empty(),
                "native ordered reads bypass component-decoding CTEs"
            );
            let mut first_query = query.clone();
            let limit = first_query.bind(3_i64);
            let first_sql = first_query.select(
                "i.item_id,k.index_key",
                &format!("ORDER BY {} LIMIT {limit}", native_order_sql(direction)),
            );
            let key = fireweed_engine::index_fields::typed_index_key(
                &spec.declaration,
                &BTreeMap::from([
                    ("kind".into(), TypedValue::String("send".into())),
                    (field.into(), bound.clone()),
                ]),
            )
            .unwrap()
            .unwrap();
            let mut statements = vec![(first_sql, first_query.params)];
            for part in 0..3 {
                statements.push(native_cursor_statement(
                    &query,
                    direction,
                    &key,
                    ItemId::from_u64(9),
                    part,
                    3,
                ));
            }
            {
                let connection = store.reader.lock().await;
                for (sql, params) in statements {
                    let plan = rows_on(&connection, &format!("EXPLAIN QUERY PLAN {sql}"), params)
                        .await
                        .unwrap();
                    let details = plan
                        .iter()
                        .map(|row| text(&row[3]).unwrap())
                        .collect::<Vec<_>>()
                        .join("\n");
                    let index_name = if direction == SortDirection::Ascending {
                        "fireweed_item_index_key_numeric_asc_idx"
                    } else {
                        "fireweed_item_index_key_numeric_desc_idx"
                    };
                    assert!(
                        details.contains(index_name),
                        "expected covering index seek: {details}"
                    );
                    assert!(
                        details.to_uppercase().contains("SEARCH"),
                        "expected seek constraints: {details}"
                    );
                    assert!(
                        !details.to_uppercase().contains("TEMP B-TREE")
                            && !details.to_uppercase().contains("SORT"),
                        "LIMIT must stop index traversal rather than a sorted matching corpus: {details}"
                    );
                }
            }
            let mut cursor = None;
            let mut observed = Vec::new();
            loop {
                let page = store
                    .server_range_scan(
                        &shard,
                        RangeScanRequest {
                            index: Some(index.into()),
                            filters: filters.clone(),
                            order_by: vec![order(field, direction)],
                            page_size: 2,
                            cursor,
                        },
                    )
                    .await
                    .unwrap();
                observed.extend(ids(&page));
                let Some(next) = page.next_cursor else {
                    break;
                };
                cursor = Some(next);
            }
            assert_eq!(
                observed,
                vec![1, 2, 9, 10, 100, u64::MAX - 1, u64::MAX],
                "numeric ID ties and keyset boundaries hold in either index direction"
            );
        }
    }
}

#[tokio::test]
async fn native_bounded_mutation_existing_unique_collision_has_page_independent_outcomes() {
    let (store, shard, _) = fixture(vec![
        recipient(1, "send", "one", Some(1), 0),
        recipient(2, "send", "two", Some(2), 0),
    ])
    .await;
    for max_scan_rows in [1, 2, 100] {
        let request = BoundedMutationRequest {
            index: Some("by_kind_score".into()),
            filters: vec![eq_kind("send")],
            set_fields: BTreeMap::from([(
                "email".into(),
                TypedValue::String("recipient-1@example.test".into()),
            )]),
            max_scan_rows,
        };
        let plan = store
            .server_plan_bounded_mutation(&shard, request)
            .await
            .unwrap();
        assert_eq!(
            plan.response
                .results
                .iter()
                .map(|result| (result.item_id.as_u64(), result.outcome))
                .collect::<Vec<_>>(),
            vec![
                (1, MutationOutcome::Updated),
                (2, MutationOutcome::Conflict)
            ]
        );
        assert_eq!(plan.updates.len(), 1);
        assert_eq!(plan.updates[0].command.item_id.as_u64(), 1);
    }
    assert_eq!(
        store
            .server_index_get_unique(&shard, "by_email", &[b"recipient-2@example.test".to_vec()])
            .await
            .unwrap()
            .unwrap()
            .item_version,
        1,
        "all planning remains read-only"
    );
}

#[tokio::test]
async fn native_numeric_tie_indexes_migrate_once_and_preserve_existing_keys() {
    let directory = tempfile::tempdir().unwrap();
    let config = crate::TursoConfig::local(directory.path().join("numeric-index-migration.db"));
    let store = TursoRelational::open(config.clone()).await.unwrap();
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let items = vec![
        recipient(10, "send", "same", Some(1), 0),
        recipient(2, "send", "same", Some(1), 0),
    ];
    apply(
        &store,
        &shard,
        &mut 0,
        QueueCommand::Push(PushCommand { items }),
        vec![ItemId::from_u64(10), ItemId::from_u64(2)],
        1,
    )
    .await;
    {
        let writer = store.writer.lock().await;
        writer.execute("CREATE INDEX fireweed_item_index_key_item_asc_idx ON fireweed_item_index(tenant_id,queue_id,index_name,index_key ASC,item_id ASC)", ()).await.unwrap();
        writer.execute("CREATE INDEX fireweed_item_index_key_item_desc_idx ON fireweed_item_index(tenant_id,queue_id,index_name,index_key DESC,item_id ASC)", ()).await.unwrap();
        writer
            .execute("DROP INDEX fireweed_item_index_key_numeric_asc_idx", ())
            .await
            .unwrap();
        writer
            .execute("DROP INDEX fireweed_item_index_key_numeric_desc_idx", ())
            .await
            .unwrap();
    }
    drop(store);
    let store = TursoRelational::open(config).await.unwrap();
    let indexes = store.schema_report().await.unwrap().indexes;
    assert!(
        indexes
            .iter()
            .any(|name| name == "fireweed_item_index_key_numeric_asc_idx")
    );
    assert!(
        indexes
            .iter()
            .any(|name| name == "fireweed_item_index_key_numeric_desc_idx")
    );
    assert!(
        !indexes
            .iter()
            .any(|name| name == "fireweed_item_index_key_item_asc_idx"
                || name == "fireweed_item_index_key_item_desc_idx")
    );
    let before = {
        let connection = store.reader.lock().await;
        rows_on(&connection, "PRAGMA schema_version", Vec::new())
            .await
            .unwrap()
    };
    store.migrate().await.unwrap();
    let after = {
        let connection = store.reader.lock().await;
        rows_on(&connection, "PRAGMA schema_version", Vec::new())
            .await
            .unwrap()
    };
    assert_eq!(
        before, after,
        "repeat migration must not rebuild the physical indexes"
    );
    let found = store
        .server_index_lookup(&shard, "by_label", &[b"same".to_vec()])
        .await
        .unwrap();
    assert_eq!(
        found
            .iter()
            .map(|hit| hit.item_id.as_u64())
            .collect::<Vec<_>>(),
        vec![2, 10]
    );
}

#[tokio::test]
async fn native_ordinary_claim_filters_gates_before_limit_and_unblocks_after_hint_reset() {
    let mut first = recipient(1, "send", "blocked-first", Some(1), 0);
    first.gate_keys = vec!["blocked".into(), "open".into()];
    let mut second = recipient(2, "send", "open-second", Some(2), 0);
    second.gate_keys = vec!["open".into()];
    let mut third = recipient(3, "send", "blocked-third", Some(3), 0);
    third.gate_keys = vec!["blocked".into()];
    let (store, shard, mut sequence) = fixture(vec![first, second, third]).await;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["blocked".into()],
            blocked: true,
        }),
        Vec::new(),
        2,
    )
    .await;
    assert_eq!(
        store
            .server_peek(&shard, 1)
            .await
            .unwrap()
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        vec![ItemId::from_u64(2)],
        "peek filters blocked gates before LIMIT and exposes the next open row"
    );
    let token = LeaseToken::new("ordinary-gates").unwrap();
    {
        let connection = store.reader.lock().await;
        for floor in [None, Some(1)] {
            let (ids, items, _) = crate::projection::select_and_materialize_item_claims_on(
                &connection,
                &shard,
                ts(10),
                1,
                &[],
                &token,
                ts(30),
                crate::projection::ItemClaimScan {
                    rowid_floor: floor,
                    gate_policy: Some(GateKeyPolicy::Dynamic),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                ids,
                vec![ItemId::from_u64(2)],
                "blocked rows before and after an open row cannot consume the bounded result"
            );
            assert_eq!(
                items[0].gate_keys,
                vec!["open".to_string()],
                "ordinary claims must preserve membership metadata"
            );
            let (ids, _, _) = crate::projection::select_and_materialize_item_claims_on(
                &connection,
                &shard,
                ts(10),
                1,
                &[ItemId::from_u64(2)],
                &token,
                ts(30),
                crate::projection::ItemClaimScan {
                    rowid_floor: floor,
                    gate_policy: Some(GateKeyPolicy::Dynamic),
                },
            )
            .await
            .unwrap();
            assert!(
                ids.is_empty(),
                "excluding the only open row cannot expose a blocked row"
            );
        }
    }
    store
        .claim_scan_default_fifo
        .lock()
        .unwrap()
        .insert(shard.clone(), true);
    store.replace_claim_scan_hint(&shard, Some(100));
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["blocked".into()],
            blocked: false,
        }),
        Vec::new(),
        3,
    )
    .await;
    assert!(!store.claim_scan_is_fifo(&shard));
    assert_eq!(store.claim_scan_hint(&shard), None);
    assert_eq!(
        store
            .server_peek(&shard, 1)
            .await
            .unwrap()
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        vec![ItemId::from_u64(1)],
        "unblocking restores the original peek head without rewriting the item"
    );
    let batches = store
        .item_claim_microbatch_on_serving_reader(&shard, &[(ts(10), 1, token, ts(30))], &[])
        .await
        .unwrap();
    assert_eq!(
        batches[0].0,
        vec![ItemId::from_u64(1)],
        "unblocking restores the original head even after a later row was selected"
    );
    assert_eq!(
        batches[0].1[0].gate_keys,
        vec!["blocked".to_string(), "open".to_string()]
    );
}

#[tokio::test]
async fn native_retained_pages_follow_numeric_ids_with_bounded_index_seeks() {
    let input = [u64::MAX, 10, 2, 100, 9, 1, u64::MAX - 1];
    let (store, shard, _) = fixture(
        input
            .iter()
            .map(|id| {
                let mut item = recipient(*id, "send", &format!("retained-{id}"), Some(1), 0);
                item.payload = Some(Bytes::from(format!("payload-{id}")));
                item
            })
            .collect(),
    )
    .await;
    {
        let writer = store.writer.lock().await;
        writer
            .execute(
                "UPDATE fireweed_items SET superseded=1 WHERE item_id='9'",
                (),
            )
            .await
            .unwrap();
        writer
            .execute(
                "UPDATE fireweed_items SET lifecycle_state='Complete' WHERE item_id='10'",
                (),
            )
            .await
            .unwrap();
        writer
            .execute(
                "UPDATE fireweed_items SET lifecycle_state='Failed' WHERE item_id='100'",
                (),
            )
            .await
            .unwrap();
    }
    let connection = store.reader.lock().await;
    for page_size in [1, 2, 1000] {
        let mut after = None;
        let mut observed = Vec::new();
        loop {
            let page =
                crate::projection::server_retained_items_on(&connection, &shard, after, page_size)
                    .await
                    .unwrap();
            if page.is_empty() {
                break;
            }
            after = Some(page.last().unwrap().item_id);
            for item in page {
                assert_eq!(
                    item.payload,
                    Some(Bytes::from(format!("payload-{}", item.item_id)))
                );
                observed.push(item.item_id.as_u64());
            }
        }
        assert_eq!(
            observed,
            vec![1, 2, 10, 100, u64::MAX - 1, u64::MAX],
            "terminal rows remain retained; superseded rows do not"
        );
    }
    let after_missing = crate::projection::server_retained_items_on(
        &connection,
        &shard,
        Some(ItemId::from_u64(3)),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        after_missing
            .iter()
            .map(|item| item.item_id.as_u64())
            .collect::<Vec<_>>(),
        vec![10, 100]
    );
    for limit in [0, 1001] {
        assert!(matches!(
            crate::projection::server_retained_items_on(&connection, &shard, None, limit).await,
            Err(EngineError::Invalid(_))
        ));
    }
    for same_width in [true, false] {
        let mut params = vec![
            Value::Text(shard.tenant_id.as_str().into()),
            Value::Text(shard.queue_id.as_str().into()),
            Value::Integer(1),
            Value::Integer(2),
        ];
        if same_width {
            params.push(Value::Text("2".into()));
        }
        let sql = crate::projection::retained_items_sql(same_width);
        let plan = rows_on(&connection, &format!("EXPLAIN QUERY PLAN {sql}"), params)
            .await
            .unwrap();
        let details = plan
            .iter()
            .map(|row| text(&row[3]).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            details.contains("SEARCH i USING INDEX fireweed_items_retained_numeric_idx"),
            "retained pages require a bounded numeric-order seek: {details}"
        );
        assert!(
            !details.to_uppercase().contains("TEMP B-TREE")
                && !details.to_uppercase().contains("SORT"),
            "numeric pagination must not sort the full retained queue: {details}"
        );
    }
}

#[tokio::test]
async fn native_compound_strings_stream_variable_length_headers_without_deep_expressions() {
    let mut definition = definition();
    definition.typed_indexes = vec![QueueIndex {
        name: "by_recipient".into(),
        declaration: IndexDeclaration::Compound(CompoundIndexDef {
            fields: ["kind", "label", "email"]
                .into_iter()
                .map(|field| CompoundIndexField {
                    field: field.into(),
                    index_type: IndexType::String,
                })
                .collect(),
            unique: false,
        }),
    }];
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    // 256 UTF-8 bytes crosses a frame-length byte boundary. Empty strings and
    // multiple unconstrained components require exact dynamic offsets too.
    let long_kind = "é".repeat(128);
    let items = vec![
        recipient(1, "z", "", None, 0),
        recipient(2, &long_kind, "b", None, 0),
        recipient(3, &long_kind, "a", None, 0),
    ];
    let ids = items.iter().map(|item| item.item_id).collect();
    let mut sequence = 0;
    apply(
        &store,
        &shard,
        &mut sequence,
        QueueCommand::Push(PushCommand { items }),
        ids,
        1,
    )
    .await;
    let page = store
        .server_range_scan(
            &shard,
            RangeScanRequest {
                index: Some("by_recipient".into()),
                filters: Vec::new(),
                order_by: ["kind", "label", "email"]
                    .into_iter()
                    .map(|field| order(field, SortDirection::Ascending))
                    .collect(),
                page_size: 10,
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.item_id.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 3, 2]
    );
    let metrics = store
        .server_metrics_by_query(
            &shard,
            MetricsByQueryRequest {
                index: Some("by_recipient".into()),
                filters: vec![QueryFilter {
                    field: "label".into(),
                    op: FilterOp::Gte,
                    value: TypedValue::String("a".into()),
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(metrics.pending, 2);
}
