//! ad-hoc probe: facade push_batch_with_request_id cost, sqlite-relational vs sqlite log+projection.
use std::sync::Arc;
use std::time::Instant;

use fireweed::{
    CohortPolicy, EligibilityPolicy, NewItem, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueKey, RecurrencePolicy,
    RequestId, RetryPolicy, SystemClock,
};

fn bo<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

fn qdef(q: &QueueKey) -> QueueDefinition {
    let _: Option<CohortPolicy> = None;
    QueueDefinition {
        tenant_id: q.tenant_id.clone(),
        queue_id: q.queue_id.clone(),
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
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100_000,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn items(n: usize, offset: usize) -> Vec<NewItem> {
    (0..n)
        .map(|i| NewItem {
            client_item_key: Some(
                fireweed::ClientItemKey::new(format!("cost-{offset}-{i}")).unwrap(),
            ),
            ..Default::default()
        })
        .collect()
}

async fn run(fw: fireweed::Fireweed, label: &str) {
    let q = QueueKey::new(
        fireweed::TenantId::new("t").unwrap(),
        fireweed::QueueId::new("q").unwrap(),
    );
    fw.create_queue(qdef(&q)).await.expect("create_queue");

    for wave in 0..5 {
        let rid = RequestId::new(format!("seed-{wave}")).unwrap();
        fw.push_batch_with_request_id(&q, rid, items(2_000, wave * 2_000))
            .await
            .expect("seed push");
    }
    let m = fw.metrics(&q).await.expect("metrics");
    assert_eq!(m.pending, 10_000, "expected 10k pending, got {}", m.pending);

    let t10 = Instant::now();
    let rid = RequestId::new("probe-10k").unwrap();
    fw.push_batch_with_request_id(&q, rid, items(2_000, 10_000))
        .await
        .expect("probe push 10k");
    let ms10 = t10.elapsed().as_secs_f64() * 1000.0;

    for wave in 0..9 {
        let rid = RequestId::new(format!("grow-{wave}")).unwrap();
        fw.push_batch_with_request_id(&q, rid, items(2_000, 12_000 + wave * 2_000))
            .await
            .expect("grow push");
    }
    let m = fw.metrics(&q).await.expect("metrics");
    assert_eq!(m.pending, 30_000, "expected 30k pending, got {}", m.pending);

    let t30 = Instant::now();
    let rid = RequestId::new("probe-30k").unwrap();
    fw.push_batch_with_request_id(&q, rid, items(2_000, 30_000))
        .await
        .expect("probe push 30k");
    let ms30 = t30.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "{label}: @10k {ms10:.1} ms ({:.4} ms/item); @30k {ms30:.1} ms ({:.4} ms/item)",
        ms10 / 2000.0,
        ms30 / 2000.0,
    );
}

fn main() {
    let dir = std::env::temp_dir().join(format!("push-floor-probe2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Cell: unified sqlite relational (single file, both axes).
    {
        let path = dir.join("relational.sqlite");
        let fw = fireweed::open_sqlite_relational(path.to_str().unwrap(), Arc::new(SystemClock))
            .expect("open sqlite relational");
        bo(run(fw, "facade sqlite-relational (unified, file)"));
    }

    // Cell: orthogonal sqlite log (durable, synchronous=FULL) x sqlite projection (NORMAL).
    {
        let log_path = dir.join("log.sqlite");
        let proj_path = dir.join("proj.sqlite");
        let fw = fireweed::open_sqlite_sqlite_projection(
            log_path.to_str().unwrap(),
            proj_path.to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite log x sqlite projection");
        bo(run(fw, "facade sqlite-log x sqlite-projection (file)"));
    }

    // Cell: sqlite log x memory projection.
    {
        let log_path = dir.join("logmem.sqlite");
        let fw = fireweed::open_sqlite(log_path.to_str().unwrap(), Arc::new(SystemClock))
            .expect("open sqlite x memory");
        bo(run(fw, "facade sqlite-log x memory-projection (file)"));
    }

    let _ = std::fs::remove_dir_all(&dir);
}
