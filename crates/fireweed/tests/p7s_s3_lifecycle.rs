//! P7S3 — S3 append, claim, finalize, and lifecycle parity.
//!
//! Executable boundary (fireweed-3f5a1de3): run P7N's applicable product
//! assertions on all three S3 cells with native-CAS failover and P1s provenance;
//! consume the provider-neutral verifier without editing its shared logic.
//!
//! - `s3×memory` / `s3×sqlite`: full `public_interface::run`
//! - `s3×postgres`: P7 method family only (append/claim/finalize). Shared
//!   verifier also hits P6/P8 stubs (`Unavailable` on upsert/update_fields/
//!   current_position) outside P7 ownership — follow-on scope.

#[path = "support/public_interface.rs"]
mod public_interface;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    ClaimAt, ClaimByItemIdsDisposition, ClaimByItemIdsRequest, ClaimByQueryAt, ClaimByQueryRequest,
    ClaimCompatibility, ClientItemKey, CompoundIndexDef, CompoundIndexField, ConfigSecret,
    EligibilityPolicy, EngineError, Fireweed, IndexDeclaration, IndexType, LogConfig,
    MultiQueueClaimLimits, MultiQueueClaimTarget, Nack, NewItem, ObjectLogAuthority, OrderField,
    OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    PriorityValue, ProjectionStoreConfig, QueryFilter, QueueDefinition, QueueId, QueueIndex,
    QueueKey, RecoveryAction, RecoveryPolicy, RecurrenceMode, RecurrencePolicy, RequestId,
    ResponseBarrier, RetryPolicy, SegmentConfig, SortDirection, StorageConfig, SystemClock,
    TenantId, TypedValue, UtcTimestamp, WorkerId,
};
use serde_json::json;

static ORDINAL: AtomicU64 = AtomicU64::new(0);

fn require_s3_env() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P7S3 (P1s provenance)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed".into());
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access =
        std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret =
        std::env::var("FIREWEED_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    (endpoint, bucket, region, access, secret)
}

fn require_pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required for P7S3 s3×postgres cell")
}

fn unique_ns(label: &str) -> String {
    let n = ORDINAL.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("p7s-{label}-{}-{n}-{nanos}", std::process::id())
}

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(unique_ns(label));
        std::fs::create_dir_all(&path).expect("fixture root");
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn s3_log_config(namespace: String, projection: ProjectionStoreConfig) -> StorageConfig {
    let (endpoint, bucket, region, access, secret) = require_s3_env();
    StorageConfig {
        log: LogConfig::S3 {
            endpoint,
            bucket,
            region,
            access_key_id: ConfigSecret::new(access),
            secret_access_key: ConfigSecret::new(secret),
            allow_insecure_http: true,
        },
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

async fn open_cell(cell_id: &str, config: StorageConfig) -> Fireweed {
    config
        .validate()
        .unwrap_or_else(|e| panic!("{cell_id} validate: {e:?}"));
    fireweed::open_async(config, Arc::new(SystemClock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} open: {e:?}"))
}

async fn run_full_verifier(cell_id: &str, config: StorageConfig, expect_projection_control: bool) {
    let fireweed = open_cell(cell_id, config).await;
    public_interface::run(cell_id, &fireweed, expect_projection_control).await;
    eprintln!("P7S3 PASS {cell_id} public_interface (full verifier)");
}

fn qdef(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("p7s").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: Default::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![QueueIndex {
            name: "by_kind_due".into(),
            declaration: IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    CompoundIndexField {
                        field: "kind".into(),
                        index_type: IndexType::String,
                    },
                    CompoundIndexField {
                        field: "due_at".into(),
                        index_type: IndexType::Datetime,
                    },
                ],
                unique: false,
            }),
        }],
        emit_change_records: true,
    }
}

fn item(label: &str, priority: i64) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(label).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("payload-{label}").into()),
        entity: Some(json!({
            "kind": "work",
            "due_at": "2026-07-25T12:00:00Z",
            "score": priority as f64,
            "external_id": label,
            "mutated": false
        })),
        fields: BTreeMap::from([("label".into(), label.as_bytes().to_vec().into())]),
        ..Default::default()
    }
}

fn query_req(request_id: &str) -> ClaimByQueryRequest {
    ClaimByQueryRequest {
        index: Some("by_kind_due".into()),
        filters: vec![QueryFilter {
            field: "kind".into(),
            op: fireweed::FilterOp::Eq,
            value: TypedValue::String("work".into()),
        }],
        order_by: OrderField {
            field: "due_at".into(),
            direction: SortDirection::Ascending,
        },
        max_items: 1,
        lease_duration_ms: 60_000,
        worker_id: WorkerId::new("p7s-query-worker").unwrap(),
        request_id: Some(RequestId::new(request_id).unwrap()),
    }
}

/// P7 method family (append/claim/finalize) — mirrors public_interface P7 assertions.
async fn run_p7_methods(cell_id: &str, fw: &Fireweed) {
    let mut failures: Vec<String> = Vec::new();
    let record = |failures: &mut Vec<String>, method: &str, detail: String| {
        failures.push(format!("{cell_id}.{method}: {detail}"));
    };

    let q = QueueKey::new(
        TenantId::new("p7s").unwrap(),
        QueueId::new("p7-core").unwrap(),
    );
    match fw.create_queue(qdef("p7-core")).await {
        Ok(o) if o.created => {}
        Ok(_) => record(
            &mut failures,
            "create_queue",
            "expected created=true".into(),
        ),
        Err(e) => record(&mut failures, "create_queue", e.to_string()),
    }

    if let Err(e) = fw.push(&q, item("push-one", 1)).await {
        record(&mut failures, "push", e.to_string());
    }
    if let Err(e) = fw
        .push_with_request_id(
            &q,
            RequestId::new("p7s-rid-1").unwrap(),
            item("push-rid", 2),
        )
        .await
    {
        record(&mut failures, "push_with_request_id", e.to_string());
    }
    match fw
        .push_batch(
            &q,
            (0..6)
                .map(|i| item(&format!("batch-{i}"), 10 + i))
                .collect(),
        )
        .await
    {
        Ok(ids) if ids.len() == 6 => {}
        Ok(ids) => record(
            &mut failures,
            "push_batch",
            format!("expected 6, got {}", ids.len()),
        ),
        Err(e) => record(&mut failures, "push_batch", e.to_string()),
    }
    if let Err(e) = fw
        .push_batch_with_request_id(
            &q,
            RequestId::new("p7s-batch-rid").unwrap(),
            vec![item("batch-rid-a", 20), item("batch-rid-b", 21)],
        )
        .await
    {
        record(&mut failures, "push_batch_with_request_id", e.to_string());
    }

    match fw
        .claim_with(&q, 1, 60_000, ClaimCompatibility::default())
        .await
    {
        Ok(items) if items.len() == 1 => {
            if let Err(e) = fw.complete(&q, [items[0].item_id]).await {
                record(&mut failures, "complete", e.to_string());
            }
        }
        Ok(items) => record(
            &mut failures,
            "claim_with",
            format!("expected 1, got {}", items.len()),
        ),
        Err(e) => record(&mut failures, "claim_with", e.to_string()),
    }

    match fw.claim(&q, 1, 60_000).await {
        Ok(items) if items.len() == 1 => {
            if let Err(e) = fw.ack(&q, [items[0].item_id]).await {
                record(&mut failures, "ack", e.to_string());
            }
        }
        Ok(items) => record(
            &mut failures,
            "claim",
            format!("expected 1, got {}", items.len()),
        ),
        Err(e) => record(&mut failures, "claim", e.to_string()),
    }

    match fw
        .claim_response_with(&q, 1, 60_000, ClaimCompatibility::default())
        .await
    {
        Ok(resp) if resp.items.len() == 1 => {
            if let Err(e) = fw.nack(&q, [resp.items[0].item_id], Nack::Release).await {
                record(&mut failures, "nack", e.to_string());
            }
        }
        Ok(resp) => record(
            &mut failures,
            "claim_response_with",
            format!("expected 1, got {}", resp.items.len()),
        ),
        Err(e) => record(&mut failures, "claim_response_with", e.to_string()),
    }

    match fw.claim_at(&q, ClaimAt::new(1, 60_000)).await {
        Ok(items) if items.len() == 1 => {
            if let Err(e) = fw.retry(&q, [items[0].item_id], None).await {
                record(&mut failures, "retry", e.to_string());
            }
        }
        Ok(items) => record(
            &mut failures,
            "claim_at",
            format!("expected 1, got {}", items.len()),
        ),
        Err(e) => record(&mut failures, "claim_at", e.to_string()),
    }

    match fw.claim_response_at(&q, ClaimAt::new(1, 60_000)).await {
        Ok(resp) if resp.items.len() == 1 => {
            if let Err(e) = fw.release(&q, [resp.items[0].item_id]).await {
                record(&mut failures, "release", e.to_string());
            }
        }
        Ok(resp) => record(
            &mut failures,
            "claim_response_at",
            format!("expected 1, got {}", resp.items.len()),
        ),
        Err(e) => record(&mut failures, "claim_response_at", e.to_string()),
    }

    match fw.claim_by_query(&q, query_req("p7s-query-claim")).await {
        Ok(resp) if !resp.items.is_empty() => {
            if let Err(e) = fw.fail(&q, [resp.items[0].item_id]).await {
                record(&mut failures, "fail", e.to_string());
            }
        }
        Ok(_) => {
            let _ = fw.push(&q, item("query-seed", 50)).await;
            match fw.claim_by_query(&q, query_req("p7s-query-claim-2")).await {
                Ok(resp) if !resp.items.is_empty() => {
                    let _ = fw.fail(&q, [resp.items[0].item_id]).await;
                }
                Ok(_) => record(
                    &mut failures,
                    "claim_by_query",
                    "no leased item after seed".into(),
                ),
                Err(e) => record(&mut failures, "claim_by_query", e.to_string()),
            }
        }
        Err(e) => record(&mut failures, "claim_by_query", e.to_string()),
    }

    match fw
        .claim_by_query_at(
            &q,
            query_req("p7s-query-claim-at"),
            ClaimByQueryAt::new().eligibility_time(UtcTimestamp::new(1_800_000_000, 0).unwrap()),
        )
        .await
    {
        Ok(resp) if !resp.items.is_empty() => {
            let _ = fw.ack(&q, [resp.items[0].item_id]).await;
        }
        Ok(_) => {
            let _ = fw.push(&q, item("query-at-seed", 51)).await;
            if let Err(e) = fw
                .claim_by_query_at(
                    &q,
                    query_req("p7s-query-claim-at-2"),
                    ClaimByQueryAt::new()
                        .eligibility_time(UtcTimestamp::new(1_800_000_000, 0).unwrap()),
                )
                .await
            {
                record(&mut failures, "claim_by_query_at", e.to_string());
            }
        }
        Err(e) => record(&mut failures, "claim_by_query_at", e.to_string()),
    }

    let qa = QueueKey::new(
        TenantId::new("p7s").unwrap(),
        QueueId::new("multi-a").unwrap(),
    );
    let qb = QueueKey::new(
        TenantId::new("p7s").unwrap(),
        QueueId::new("multi-b").unwrap(),
    );
    let _ = fw.create_queue(qdef("multi-a")).await;
    let _ = fw.create_queue(qdef("multi-b")).await;
    let _ = fw.push(&qa, item("multi-a", 1)).await;
    let _ = fw.push(&qb, item("multi-b", 1)).await;
    match fw
        .claim_across_queues(
            vec![
                MultiQueueClaimTarget {
                    queue: qa,
                    claim: ClaimAt::new(1, 60_000),
                },
                MultiQueueClaimTarget {
                    queue: qb,
                    claim: ClaimAt::new(1, 60_000),
                },
            ],
            MultiQueueClaimLimits::default(),
        )
        .await
    {
        Ok(results) if results.len() == 2 => {}
        Ok(results) => record(
            &mut failures,
            "claim_across_queues",
            format!("expected 2 results, got {}", results.len()),
        ),
        Err(e) => record(&mut failures, "claim_across_queues", e.to_string()),
    }

    match fw
        .push_batch(&q, vec![item("cbi-target", 1), item("cbi-other", 2)])
        .await
    {
        Ok(ids) if ids.len() == 2 => {
            let advertises = fw.hot_projection_capabilities(&q).claim_by_item_ids;
            let request = ClaimByItemIdsRequest {
                item_ids: vec![ids[0]],
                lease_duration_ms: 60_000,
                worker_id: WorkerId::new("p7s-cbi").unwrap(),
                request_id: RequestId::new("p7s-cbi").unwrap(),
                lease_token: None,
            };
            if advertises {
                match fw.claim_by_item_ids(&q, request).await {
                    Ok(resp)
                        if resp.items.len() == 1
                            && resp.outcomes.first().is_some_and(|o| {
                                o.disposition == ClaimByItemIdsDisposition::Claimed
                            }) =>
                    {
                        let _ = fw.ack(&q, [ids[0]]).await;
                    }
                    Ok(_) => record(
                        &mut failures,
                        "claim_by_item_ids",
                        "unexpected response shape".into(),
                    ),
                    Err(e) => record(&mut failures, "claim_by_item_ids", e.to_string()),
                }
            } else {
                match fw.claim_by_item_ids(&q, request).await {
                    Err(EngineError::Unavailable) => {}
                    Err(e) => record(
                        &mut failures,
                        "claim_by_item_ids",
                        format!("expected Unavailable without capability, got {e}"),
                    ),
                    Ok(_) => record(
                        &mut failures,
                        "claim_by_item_ids",
                        "Ok without advertising claim_by_item_ids capability".into(),
                    ),
                }
            }
        }
        Ok(ids) => record(
            &mut failures,
            "push_batch[cbi]",
            format!("expected 2, got {}", ids.len()),
        ),
        Err(e) => record(&mut failures, "push_batch[cbi]", e.to_string()),
    }

    // nack_retry_after / retry_after on oneshot leased items
    for (label, op) in [("nack_retry_after", 0u8), ("retry_after", 1u8)] {
        if let Err(e) = fw.push(&q, item(label, 30 + i64::from(op))).await {
            record(&mut failures, label, format!("seed push: {e}"));
            continue;
        }
        match fw.claim(&q, 1, 60_000).await {
            Ok(items) if !items.is_empty() => {
                let id = items[0].item_id;
                let res = if op == 0 {
                    fw.nack_retry_after(&q, [id], 1).await
                } else {
                    fw.retry_after(&q, [id], 1).await
                };
                if let Err(e) = res {
                    record(&mut failures, label, e.to_string());
                }
            }
            Ok(_) => record(&mut failures, label, "claim returned empty".into()),
            Err(e) => record(&mut failures, label, e.to_string()),
        }
    }

    // rearm family requires a recurring queue (same as public_interface seed_claim)
    let mut recurring_def = qdef("p7-rearm");
    recurring_def.recurrence = RecurrencePolicy {
        mode: RecurrenceMode::Recurring,
        until: None,
    };
    let rq = QueueKey::new(
        TenantId::new("p7s").unwrap(),
        QueueId::new("p7-rearm").unwrap(),
    );
    if let Err(e) = fw.create_queue(recurring_def).await {
        record(&mut failures, "create_queue[rearm]", e.to_string());
    }
    for (label, op) in [("rearm", 0u8), ("rearm_at", 1u8), ("rearm_after", 2u8)] {
        if let Err(e) = fw.push(&rq, item(label, 40 + i64::from(op))).await {
            record(&mut failures, label, format!("seed push: {e}"));
            continue;
        }
        match fw.claim(&rq, 1, 60_000).await {
            Ok(items) if !items.is_empty() => {
                let id = items[0].item_id;
                let res = match op {
                    0 => fw.rearm(&rq, [id]).await,
                    1 => {
                        fw.rearm_at(&rq, [id], UtcTimestamp::new(1, 0).unwrap())
                            .await
                    }
                    _ => fw.rearm_after(&rq, [id], 1).await,
                };
                if let Err(e) = res {
                    record(&mut failures, label, e.to_string());
                }
            }
            Ok(_) => record(&mut failures, label, "claim returned empty".into()),
            Err(e) => record(&mut failures, label, e.to_string()),
        }
    }

    if !failures.is_empty() {
        panic!(
            "P7S3 P7 method parity failed for {cell_id}:\n{}",
            failures.join("\n")
        );
    }
    eprintln!("P7S3 PASS {cell_id} P7 method family (append/claim/finalize)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_memory_strict_public_interface_lifecycle() {
    let _s3 = require_s3_env();
    let ns = unique_ns("s3-memory");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_full_verifier("s3--memory--strict", config, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sqlite_strict_public_interface_lifecycle() {
    let _s3 = require_s3_env();
    let fixture = FixtureRoot::new("s3-sqlite");
    let ns = unique_ns("s3-sqlite");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_full_verifier("s3--sqlite--strict", config, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_postgres_strict_p7_method_parity() {
    let _s3 = require_s3_env();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    let fireweed = open_cell("s3--postgres--strict", config).await;
    run_p7_methods("s3--postgres--strict", &fireweed).await;
}
