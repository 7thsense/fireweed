//! Queue lifecycle on a throwaway on-disk queue: provision, insert, claim, commit, retry and
//! scheduled delivery. `cargo run -p fireweed --example queue_lifecycle`.
use fireweed::queue::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn schema() -> QueueSchema {
    QueueSchema {
        work: CollectionDefinition {
            fields: BTreeMap::from([("kind".into(), FieldType::Text)]),
            indexes: BTreeMap::new(),
            unique_indexes: BTreeSet::new(),
            additional_field_prefixes: BTreeSet::new(),
        },
        state: BTreeMap::new(),
        progress_bound_nanos: 1_000_000_000,
        max_attempts: 3,
        retention: QueueRetention {
            request_id_nanos: 86_400_000_000_000,
            deleted_keys: DeletedKeyPolicy::Reusable,
        },
    }
}

fn job(key: &str, priority: i64, available_at: i64) -> NewWork {
    NewWork {
        key: key.into(),
        priority,
        available_at,
        fields: BTreeMap::from([("kind".into(), FieldValue::Text("email".into()))]),
        body: Bytes::from_static(b"payload"),
    }
}

fn now() -> i64 {
    timestamp_nanos(SystemClock.now()).expect("clock in range")
}

fn claim_request(queue: &QueueAddress, id: &str, eligible_at: i64) -> ClaimRequest {
    ClaimRequest {
        queue: queue.clone(),
        request_id: id.into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at,
        lease_nanos: 30_000_000_000,
        max_records: 10,
        max_bytes: 1 << 20,
        fields: None,
        include_body: true,
        issued_at: now(),
    }
}

fn settle(
    queue: &QueueAddress,
    id: &str,
    work: &ClaimedWork,
    disposition: Disposition,
) -> CommitBatch {
    CommitBatch {
        queue: queue.clone(),
        groups: vec![CommitGroup {
            request_id: id.into(),
            claimed_updates: vec![ClaimedUpdate {
                key: work.record.work.key.clone(),
                lease: work.lease.clone(),
                expected_version: work.record.version,
                patch: WorkPatch::default(),
                disposition,
            }],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
            administration: None,
            issued_at: now(),
        }],
    }
}

async fn push_claim_complete(queue: &Queue, address: &QueueAddress) -> QueueResult<()> {
    queue
        .provision(ProvisionRequest {
            queue: address.clone(),
            schema: schema(),
        })
        .await?;
    queue
        .insert(InsertRequest {
            queue: address.clone(),
            request_id: "load-1".into(),
            mode: InsertMode::InsertOnly,
            items: vec![job("welcome", 5, 0), job("digest", 1, 0)],
            issued_at: now(),
        })
        .await?;

    // Lower priority values are claimed first under `PriorityAscending`.
    let claimed = queue
        .claim(claim_request(address, "claim-1", now()))
        .await?;
    assert_eq!(claimed[0].record.work.key, "digest");

    for (index, work) in claimed.iter().enumerate() {
        let receipts = queue
            .commit(settle(
                address,
                &format!("done-{index}"),
                work,
                Disposition::Complete,
            ))
            .await?;
        assert!(matches!(
            receipts[0].outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    let stats = queue
        .stats(StatsRequest {
            buckets: None,
            queue: address.clone(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: now(),
        })
        .await?;
    assert_eq!(stats.totals.completed, 2);
    Ok(())
}

async fn retry_and_schedule(queue: &Queue, address: &QueueAddress) -> QueueResult<()> {
    let later = now() + 3_600_000_000_000;
    queue
        .insert(InsertRequest {
            queue: address.clone(),
            request_id: "load-2".into(),
            mode: InsertMode::InsertOnly,
            items: vec![job("flaky", 1, 0), job("nightly", 1, later)],
            issued_at: now(),
        })
        .await?;

    // `nightly` is not eligible for an hour, so only `flaky` is delivered.
    let claimed = queue
        .claim(claim_request(address, "claim-2", now()))
        .await?;
    assert_eq!(claimed.len(), 1);
    let retry_at = now() + 60_000_000_000;
    queue
        .commit(settle(
            address,
            "retry-1",
            &claimed[0],
            Disposition::RetryAt(retry_at),
        ))
        .await?;

    // Neither item is eligible yet; both become claimable once the clock passes their times.
    assert!(
        queue
            .claim(claim_request(address, "claim-3", now()))
            .await?
            .is_empty()
    );
    let due = queue
        .claim(claim_request(address, "claim-4", later + 1))
        .await?;
    assert_eq!(due.len(), 2);
    Ok(())
}

/// Production construction: S3 object log, local Turso cache, PostgreSQL control plane.
#[allow(dead_code)]
async fn open_production() -> QueueResult<Queue> {
    let secret = |name: &str| ConfigSecret::new(std::env::var(name).unwrap_or_default());
    let options = S3TursoOptions::new(
        S3LogOptions {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "work-queues".into(),
            namespace: "prod/mail".into(),
            access_key_id: secret("S3_ACCESS_KEY_ID"),
            secret_access_key: secret("S3_SECRET_ACCESS_KEY"),
            allow_insecure_http: false,
        },
        TursoCacheOptions {
            directory: "/var/lib/fireweed/cache".into(),
        },
        PostgresControlPlaneOptions {
            url: secret("DATABASE_URL"),
            schema: "fireweed".into(),
            instance_id: OwnerId::new("worker-1")
                .map_err(|e| QueueError::Invalid(e.to_string()))?,
            heartbeat_ttl_ms: 10_000,
            lease_ttl_ms: 30_000,
            ownership_clock: None,
        },
    );
    Queue::s3_turso(options, Arc::new(SystemClock)).await
}

#[tokio::main]
async fn main() -> QueueResult<()> {
    let queue = Queue::ephemeral_disk(Arc::new(SystemClock)).await?;
    let address = QueueAddress {
        tenant: "example".into(),
        queue: "mail".into(),
    };
    push_claim_complete(&queue, &address).await?;
    retry_and_schedule(&queue, &address).await?;
    queue.shutdown().await
}
