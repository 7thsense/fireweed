//! End-to-end: an OFF-THE-SHELF Redis client (`redis` crate) drives the pqueue RESP front over real
//! TCP: produce via XADD, drain via XREADGROUP `>`, ack via XACK, and reconcile that every
//! produced item is delivered exactly once, in priority order, with ties broken by insertion order
//! (plan section 3 drain-and-reconcile, validating Invariant 1 through the stock command surface).

use std::sync::Arc;

use pqueue_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use pqueue_engine::Clock;
use pqueue_engine::{
    Backend, CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore, FenceLeaseCommand,
    LogWriter, ProjectionWriter, QueueCommand, QueueKey,
};
use pqueue_memory::{ComposedMemoryBackend, ManualClock, composed_memory_backend};
use pqueue_resp::{RespBackend, SystemClock, serve};
use redis::Value;
use redis::streams::StreamReadReply;

/// Boot the RESP front over a real ephemeral TCP port with a fresh memory backend + created queue, and
/// return an off-the-shelf async Redis connection plus the backend handle (for operator-side setup).
async fn setup() -> (
    redis::aio::MultiplexedConnection,
    Arc<ComposedMemoryBackend>,
) {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (con, _) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;
    (con, backend)
}

/// Boot the RESP front over an arbitrary backend + clock; returns the off-the-shelf client connection.
async fn serve_backend<B: RespBackend>(
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
) -> (redis::aio::MultiplexedConnection, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, backend, clock));
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let con = client.get_multiplexed_async_connection().await.unwrap();
    (con, addr)
}

fn shard() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

/// Operator-fence a leased item directly on the backend (no RESP operator surface yet), so the e2e can
/// prove a fenced holder's XACK is refused with `-ERR pqueue stale_lease`.
async fn fence(backend: &ComposedMemoryBackend, id: &str) {
    let item = ItemId::new(id).unwrap();
    let env = CommandEnvelope {
        command_id: CommandId::new("fence"),
        request_id: None,
        item_ids: vec![item],
        command: QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![item],
        }),
        checksum: CommandChecksum(0),
        created_at: UtcTimestamp::new(0, 0).unwrap(),
    };
    let sk = shard();
    let epoch = backend.current_epoch(&sk).await.unwrap();
    backend
        .write(
            move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
                let pos = lw.append(&sk, std::slice::from_ref(&env), epoch)?;
                pw.apply(&pos, std::slice::from_ref(&env))?;
                Ok(())
            },
        )
        .await
        .unwrap();
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_and_reconcile_with_offtheshelf_client() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, _) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;

    // Produce mixed priorities incl. a DUPLICATE (two 30s) to exercise the CreatedSequence
    // tie-breaker. Record the server-assigned id per insertion so we can check tie order.
    let priorities: Vec<i64> = vec![50, 10, 90, 30, 70, 20, 80, 40, 60, 5, 30];
    let mut produced_ids: Vec<String> = Vec::new();
    for &p in &priorities {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
        produced_ids.push(id);
    }

    // Drain COUNT 3 at a time, acking each batch, until empty.
    let mut delivered: Vec<(i64, String)> = Vec::new();
    let mut round_bounds: Vec<(i64, i64)> = Vec::new(); // (min, max) priority per round
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(
            rounds < 100,
            "drain did not terminate (possible orphan/hang)"
        );
        let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c")
            .arg("COUNT")
            .arg(3)
            .arg("STREAMS")
            .arg("t1:q1")
            .arg(">")
            .query_async(&mut con)
            .await
            .unwrap();
        let Some(reply) = reply else { break };
        if reply.keys.iter().all(|k| k.ids.is_empty()) {
            break;
        }
        let mut round: Vec<i64> = Vec::new();
        let mut ack = redis::cmd("XACK");
        ack.arg("t1:q1").arg("g");
        for key in &reply.keys {
            for id in &key.ids {
                let p: i64 = id.get("priority").expect("priority field present");
                delivered.push((p, id.id.clone()));
                round.push(p);
                ack.arg(&id.id);
            }
        }
        round_bounds.push((*round.iter().min().unwrap(), *round.iter().max().unwrap()));
        let _acked: i64 = ack.query_async(&mut con).await.unwrap();
    }

    // (a) exactly-once + global priority order: delivered priorities == sorted(produced).
    let delivered_pri: Vec<i64> = delivered.iter().map(|(p, _)| *p).collect();
    let mut expected = priorities.clone();
    expected.sort();
    assert_eq!(
        delivered_pri, expected,
        "delivered set must equal produced set, in priority order (Invariant 1, exactly once)"
    );

    // (b) cross-batch ordering: each round's max <= the next round's min (so a backend that sorted
    // only WITHIN a batch would fail here).
    for w in round_bounds.windows(2) {
        assert!(
            w[0].1 <= w[1].0,
            "round priority bands must not overlap: {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    // (c) tie-break: the first-inserted priority-30 item is delivered before the second.
    let first_30 = &produced_ids[3];
    let second_30 = &produced_ids[10];
    let pos_first = delivered.iter().position(|(_, id)| id == first_30).unwrap();
    let pos_second = delivered
        .iter()
        .position(|(_, id)| id == second_30)
        .unwrap();
    assert!(
        pos_first < pos_second,
        "equal-priority items must break ties by insertion order (CreatedSequence)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xadd_on_client_item_key_upserts_not_appends() {
    // TD-006 §2 / Invariant 2: a second XADD with the same client_item_key REPLACES the pending item
    // (XADD-on-key upsert), so the queue holds one item, not two.
    let (mut con, _backend) = setup().await;
    for p in [50, 20] {
        let _: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("client_item_key")
            .arg("dup")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
    }
    // Drain: exactly ONE entry, carrying the LATEST priority (20, the replacement).
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let ids: Vec<_> = reply.keys.iter().flat_map(|k| &k.ids).collect();
    assert_eq!(
        ids.len(),
        1,
        "same client_item_key upserts to a single pending item"
    );
    let p: i64 = ids[0].get("priority").unwrap();
    assert_eq!(p, 20, "the upsert kept the replacement's priority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xreadgroup_returns_api001_claimed_item_shape() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, _) = serve_backend(backend, Arc::new(ManualClock::at(1_000))).await;
    let id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("shape-key")
        .arg("priority")
        .arg(7)
        .arg("group_key")
        .arg("group-a")
        .arg("not_before")
        .arg(1_000_000_i64)
        .arg("payload")
        .arg("opaque")
        .arg("metadata")
        .arg(r#"{"tenant_segment":{"String":"vip"}}"#)
        .arg("recipient_ref")
        .arg("r-1")
        .query_async(&mut con)
        .await
        .unwrap();

    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let claimed = &reply.keys[0].ids[0];
    assert_eq!(claimed.id, id);
    assert_eq!(
        claimed.get::<String>("item_id").as_deref(),
        Some(id.as_str())
    );
    assert_eq!(
        claimed.get::<String>("client_item_key").as_deref(),
        Some("shape-key")
    );
    assert_eq!(claimed.get::<u64>("item_version"), Some(2));
    assert_eq!(claimed.get::<i64>("priority"), Some(7));
    assert_eq!(
        claimed.get::<String>("group_key").as_deref(),
        Some("group-a")
    );
    assert_eq!(claimed.get::<i64>("not_before"), Some(1_000_000));
    assert_eq!(claimed.get::<String>("lease_token").as_deref(), Some("L1"));
    assert_eq!(claimed.get::<i64>("lease_expires_at"), Some(1_060_000));
    assert_eq!(claimed.get::<u32>("attempt_count"), Some(1));
    assert_eq!(
        claimed.get::<Vec<u8>>("payload").as_deref(),
        Some(&b"opaque"[..])
    );
    assert_eq!(
        claimed.get::<Vec<u8>>("recipient_ref").as_deref(),
        Some(&b"r-1"[..])
    );
    assert_eq!(
        claimed.get::<String>("metadata").as_deref(),
        Some(r#"{"tenant_segment":{"String":"vip"}}"#)
    );
    assert!(
        claimed.get::<String>("gate_keys").is_none(),
        "gate_keys are omitted for gate_keys=none queues"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xadd_rejects_claimed_shape_reserved_lease_token_field() {
    let (mut con, _) = setup().await;
    let result: redis::RedisResult<String> = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("reserved-lease")
        .arg("lease_token")
        .arg("user-value")
        .query_async(&mut con)
        .await;

    let err = result.expect_err("lease_token is reserved by XREADGROUP output");
    assert!(
        err.to_string().contains("field 'lease_token' is reserved"),
        "unexpected error: {err}"
    );
}

fn value_array(value: Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items,
        other => panic!("expected RESP array, got {other:?}"),
    }
}

fn bulk_string(value: &Value) -> Vec<u8> {
    match value {
        Value::BulkString(bytes) => bytes.clone(),
        other => panic!("expected bulk string, got {other:?}"),
    }
}

fn field_value(fields: &[Value], name: &str) -> Option<Vec<u8>> {
    for pair in fields.chunks_exact(2) {
        if bulk_string(&pair[0]) == name.as_bytes() {
            return Some(bulk_string(&pair[1]));
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pq_live_hash_reads_return_structured_fields_until_ack() {
    let (mut con, _backend) = setup().await;
    let id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("work-1")
        .arg("priority")
        .arg(7)
        .arg("payload")
        .arg("opaque")
        .arg("recipient_ref")
        .arg("r-1")
        .arg("payload_ref")
        .arg("p-1")
        .query_async(&mut con)
        .await
        .unwrap();

    let hgetall = value_array(
        redis::cmd("PQ.HGETALL")
            .arg("t1:q1")
            .arg("work-1")
            .query_async::<Value>(&mut con)
            .await
            .unwrap(),
    );
    assert_eq!(
        field_value(&hgetall, "recipient_ref").as_deref(),
        Some(&b"r-1"[..])
    );
    assert_eq!(
        field_value(&hgetall, "payload").as_deref(),
        Some(&b"opaque"[..])
    );
    assert_eq!(
        field_value(&hgetall, "lifecycle_state").as_deref(),
        Some(&b"Pending"[..])
    );

    let hmget = value_array(
        redis::cmd("PQ.HMGET")
            .arg("t1:q1")
            .arg("work-1")
            .arg("recipient_ref")
            .arg("payload_ref")
            .arg("missing")
            .query_async::<Value>(&mut con)
            .await
            .unwrap(),
    );
    assert_eq!(bulk_string(&hmget[0]), b"r-1");
    assert_eq!(bulk_string(&hmget[1]), b"p-1");
    assert!(matches!(hmget[2], Value::Nil));

    let mget = value_array(
        redis::cmd("PQ.MGET")
            .arg("t1:q1")
            .arg("missing")
            .arg("work-1")
            .query_async::<Value>(&mut con)
            .await
            .unwrap(),
    );
    assert!(matches!(mget[0], Value::Nil));
    let Value::Array(entry) = &mget[1] else {
        panic!("live key should render as stream-entry-shaped array");
    };
    assert_eq!(bulk_string(&entry[0]), id.as_bytes());
    let Value::Array(entry_fields) = &entry[1] else {
        panic!("entry fields should be an array");
    };
    assert_eq!(
        field_value(entry_fields, "recipient_ref").as_deref(),
        Some(&b"r-1"[..])
    );

    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let claimed = &reply.keys[0].ids[0];
    let recipient: String = claimed.get("recipient_ref").unwrap();
    assert_eq!(recipient, "r-1");

    let _acked: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&claimed.id)
        .query_async(&mut con)
        .await
        .unwrap();
    let after_ack = value_array(
        redis::cmd("PQ.HMGET")
            .arg("t1:q1")
            .arg("work-1")
            .arg("recipient_ref")
            .query_async::<Value>(&mut con)
            .await
            .unwrap(),
    );
    assert!(matches!(after_ack[0], Value::Nil));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xpending_lists_leased_then_shrinks_on_ack() {
    let (mut con, _backend) = setup().await;
    for p in [10, 20] {
        let _: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
    }
    // Claim both → both pending (leased, not acked).
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let claimed: Vec<String> = reply
        .keys
        .iter()
        .flat_map(|k| k.ids.iter().map(|e| e.id.clone()))
        .collect();
    assert_eq!(claimed.len(), 2);

    // XPENDING extended → 2 entries.
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pend.len(), 2, "both leased items are pending");

    // Ack one → XPENDING shrinks to 1.
    let _: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&claimed[0])
        .query_async(&mut con)
        .await
        .unwrap();
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pend.len(), 1, "acked item leaves the pending set");
    assert_eq!(pend[0].0, claimed[1], "the un-acked item remains pending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_lease_xack_is_stale_over_the_wire() {
    // Operator fences a leased item; the holder's XACK must surface `-ERR pqueue stale_lease` to the
    // stock client (TD-006 §3/§7) — not a silent success.
    let (mut con, backend) = setup().await;
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let id = reply.keys[0].ids[0].id.clone();

    fence(&backend, &id).await;

    let res: redis::RedisResult<i64> = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&id)
        .query_async(&mut con)
        .await;
    let err = res.expect_err("fenced XACK must be an error reply");
    assert!(
        err.to_string().contains("stale_lease"),
        "expected -ERR pqueue stale_lease, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xack_of_superseded_id_is_superseded_over_the_wire() {
    // After an XADD-on-key upsert, the OLD id is superseded; acking it must surface
    // `-ERR pqueue superseded` (TD-006 §3/§6.5), NOT the generic `-ERR pqueue invalid`.
    let (mut con, _backend) = setup().await;
    let old_id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("dup")
        .arg("priority")
        .arg(50)
        .query_async(&mut con)
        .await
        .unwrap();
    // Second XADD with the same key supersedes the first.
    let _new_id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("dup")
        .arg("priority")
        .arg(20)
        .query_async(&mut con)
        .await
        .unwrap();

    let res: redis::RedisResult<i64> = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&old_id)
        .query_async(&mut con)
        .await;
    let err = res.expect_err("acking a superseded id must be an error reply");
    assert!(
        err.to_string().contains("superseded"),
        "expected -ERR pqueue superseded, got: {err}"
    );
}

type AutoClaim = (String, Vec<(String, Vec<(String, String)>)>, Vec<String>);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xautoclaim_redelivers_expired_leases() {
    // Real lease TTL + reclaim: claim an item, advance the (manual) clock past its lease, then
    // XAUTOCLAIM reclaims the expired lease and re-delivers it with a bumped attempt_count.
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap(); // max_lease_duration_ms = 60_000 (60s)
    let clock = Arc::new(ManualClock::at(1_000)); // t = 1000s
    let (mut con, _) = serve_backend(backend.clone(), clock.clone()).await;

    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    // Claim → leased, lease_expires_at = 1000s + 60s = 1060s, attempt_count = 1.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 1);

    // At exactly expiry (t == 1060): half-open lease still held → nothing reclaimed, and XPENDING still
    // shows the item leased with attempt_count == 1 (the boundary is load-bearing at the RESP layer).
    clock.set(1_060);
    // A large min-idle-time (1h) is deliberately ignored: pqueue reclaims by lease expiry, not idle.
    let (_c, entries, _d): AutoClaim = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c")
        .arg(3_600_000)
        .arg("0-0")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        entries.is_empty(),
        "lease valid through the expiry instant; nothing reclaimed"
    );
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        pend.len(),
        1,
        "still leased at the expiry instant (half-open)"
    );
    assert_eq!(pend[0].3, 1, "attempt_count still 1 before any reclaim");

    // One unit past expiry: XAUTOCLAIM reclaims + redelivers. attempt_count is EXACTLY 2 — the reclaim
    // (LeaseExpired) does NOT charge (it's not a delivery); only the original claim(1) + the redelivery
    // (Claim, 1) charge. INVARIANT: attempt_count = number of deliveries (TD-006:129).
    clock.set(1_061);
    let (_c, entries, _d): AutoClaim = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c")
        .arg(3_600_000)
        .arg("0-0")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "expired lease redelivered despite the large min-idle-time (ignored)"
    );
    let fields = &entries[0].1;
    let attempt: i64 = fields
        .iter()
        .find(|(k, _)| k == "attempt_count")
        .unwrap()
        .1
        .parse()
        .unwrap();
    assert_eq!(
        attempt, 2,
        "claim(1) + redeliver(1) = 2; the reclaim does not charge"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_recovery_rebuilds_durable_state_over_the_wire() {
    use pqueue_sqlite::composed_sqlite_backend;
    let path = std::env::temp_dir().join(format!("pqueue-resp-crash-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let p = path.to_str().unwrap().to_string();

    // Session 1: produce 3, claim 1 (leave it leased + un-acked), then "crash".
    let leased_id = {
        let b = Arc::new(composed_sqlite_backend(&p).unwrap());
        b.create_queue(qdef()).await.unwrap();
        let handle = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let h = tokio::spawn(serve(listener, b.clone(), Arc::new(SystemClock)));
            let client = redis::Client::open(format!("redis://{addr}")).unwrap();
            let mut con = client.get_multiplexed_async_connection().await.unwrap();
            for pr in [10, 20, 30] {
                let _: String = redis::cmd("XADD")
                    .arg("t1:q1")
                    .arg("*")
                    .arg("priority")
                    .arg(pr)
                    .query_async(&mut con)
                    .await
                    .unwrap();
            }
            let reply: StreamReadReply = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg("g")
                .arg("c")
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS")
                .arg("t1:q1")
                .arg(">")
                .query_async(&mut con)
                .await
                .unwrap();
            let id = reply.keys[0].ids[0].id.clone();
            (h, id)
        };
        handle.0.abort(); // crash the server
        handle.1
    }; // backend dropped → sqlite file closed

    // Session 2: reopen the SAME database — projection rebuilt from the durable log.
    let b = Arc::new(composed_sqlite_backend(&p).unwrap());
    let (mut con, _) = serve_backend(b.clone(), Arc::new(SystemClock)).await;
    // The leased item survived as pending-in-flight.
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pend.len(), 1, "the leased item is reconstructed as pending");
    assert_eq!(pend[0].0, leased_id);
    // The other two un-claimed items are still drainable.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        reply.keys[0].ids.len(),
        2,
        "the two unclaimed items survived the crash"
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xadd_collision_with_leased_then_terminal_is_an_error() {
    // I4: XADD-on-key against an IN-FLIGHT item → `-ERR pqueue invalid`; against a TERMINAL item →
    // `-ERR pqueue terminal` (TD-006 §3 collision contract), never a silent success.
    let (mut con, _b) = setup().await;
    let a: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("k")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    // Claim it → leased.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids[0].id, a);
    // Same-key XADD while leased → invalid collision.
    let res: redis::RedisResult<String> = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("k")
        .arg("priority")
        .arg(6)
        .query_async(&mut con)
        .await;
    assert!(
        res.unwrap_err().to_string().contains("invalid"),
        "leased collision → -ERR pqueue invalid"
    );
    // Ack → terminal. Same-key XADD → terminal collision.
    let _: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&a)
        .query_async(&mut con)
        .await
        .unwrap();
    let res: redis::RedisResult<String> = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("k")
        .arg("priority")
        .arg(7)
        .query_async(&mut con)
        .await;
    assert!(
        res.unwrap_err().to_string().contains("terminal"),
        "terminal collision → -ERR pqueue terminal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_on_one_backend_assign_distinct_xadd_ids() {
    // C: XADD ids are BACKEND-assigned, so two RESP servers sharing one backend mint DISTINCT ids and
    // both items coexist (a per-server counter would collide).
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con_a, _) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;
    let (mut con_b, _) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;

    let id_a: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con_a)
        .await
        .unwrap();
    let id_b: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(6)
        .query_async(&mut con_b)
        .await
        .unwrap();
    assert_ne!(
        id_a, id_b,
        "backend-assigned ids differ across servers on one backend"
    );

    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con_a)
        .await
        .unwrap();
    assert_eq!(
        reply.keys[0].ids.len(),
        2,
        "both items coexist (no silent overwrite)"
    );
}

/// XCLAIM both semantics over the stock client (owed-item E / Chunk 6a): the RESP "consumer" IS the
/// lease token (what XPENDING reports). Self-XCLAIM (consumer == current owner) RENEWS without charging
/// a delivery; cross-consumer XCLAIM REASSIGNS — transfers ownership and charges exactly one delivery
/// (TD-006:129). Verified through XPENDING's `[id, consumer, idle, delivery-count]` rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xclaim_self_renews_no_charge_cross_consumer_reclaims_with_attempt_bump() {
    let (mut con, _backend) = setup().await;

    // Produce one item and claim it (consumer c1). The front mints the lease token.
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let id = reply.keys[0].ids[0].id.clone();

    // Current owner (lease token) + delivery-count via XPENDING extended.
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pend.len(), 1);
    let owner = pend[0].1.clone();
    assert_eq!(
        pend[0].3, 1,
        "exactly one delivery so far (the initial claim)"
    );

    // SELF-XCLAIM: consumer == the current owner token → renew, NO attempt charge.
    let claimed: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg(&owner)
        .arg(0)
        .arg(&id)
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        claimed,
        vec![id.clone()],
        "self-XCLAIM returns the claimed id"
    );
    let pend2: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pend2[0].1, owner, "self-XCLAIM keeps the same owner");
    assert_eq!(
        pend2[0].3, 1,
        "self-XCLAIM (renew) does NOT charge an attempt"
    );

    // CROSS-CONSUMER XCLAIM to c2 → reassign: ownership transfers, delivery-count +1.
    let claimed2: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c2")
        .arg(0)
        .arg(&id)
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(claimed2, vec![id.clone()]);
    let pend3: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        pend3[0].1, "c2",
        "cross-consumer XCLAIM transfers ownership to c2"
    );
    assert_eq!(
        pend3[0].3, 2,
        "cross-consumer XCLAIM charges one delivery (now 2)"
    );

    // Without JUSTID, XCLAIM returns the rich entry [id, [field value …]] for the (now c2-owned) item.
    let entries: Vec<(String, Vec<String>)> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c2")
        .arg(0)
        .arg(&id)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, id, "entry carries the item id");

    // A REPEATED id in one XCLAIM is idempotent: it must charge the delivery count once, not per copy.
    let before: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    let attempts_before = before[0].3;
    let _: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c3")
        .arg(0)
        .arg(&id)
        .arg(&id)
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    let after: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        after[0].1, "c3",
        "duplicate-id reclaim still transfers ownership"
    );
    assert_eq!(
        after[0].3,
        attempts_before + 1,
        "a duplicated id charges exactly ONE delivery, not one per copy"
    );
}

/// XLEN / XDEL / XINFO over the stock client (owed-item E.2 / Chunk 6b). XLEN counts LIVE entries
/// (pending + in-flight), terminal/acked entries drop out; XDEL hard-removes and returns the count;
/// XINFO STREAM/GROUPS summarize. These are pqueue-flavored reads — divergences documented in TD-006 §3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xlen_xdel_xinfo_over_offtheshelf_client() {
    let (mut con, _backend) = setup().await;

    // Two items → XLEN 2 (both pending). Capture the server-assigned ids.
    let mut produced: Vec<String> = Vec::new();
    for p in [5, 9] {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
        produced.push(id);
    }
    let len: i64 = redis::cmd("XLEN")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 2, "two live (pending) entries");

    // Claim one → still 2 live (1 pending + 1 in-flight).
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let claimed_id = reply.keys[0].ids[0].id.clone();
    let len: i64 = redis::cmd("XLEN")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 2, "leased entry still counts as live");

    // Ack (complete) the claimed one → drops out of XLEN (terminal, like an acked+trimmed entry).
    let _: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&claimed_id)
        .query_async(&mut con)
        .await
        .unwrap();
    let len: i64 = redis::cmd("XLEN")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 1, "completed entry is not counted");

    // XINFO STREAM: length reflects the one remaining live (pending) entry.
    let info: std::collections::HashMap<String, redis::Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    let stream_len = match &info["length"] {
        redis::Value::Int(n) => *n,
        other => panic!("XINFO STREAM length should be an int, got {other:?}"),
    };
    assert_eq!(stream_len, 1, "XINFO STREAM length == live entries");

    // XINFO GROUPS: a single implicit group is reported (non-empty array).
    let groups: redis::Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    match groups {
        redis::Value::Array(ref g) => assert_eq!(g.len(), 1, "one group"),
        other => panic!("expected an array of groups, got {other:?}"),
    }

    // XDEL the remaining (still-pending) entry directly — the produced id that was not the claimed one.
    // XDEL force-removes regardless of state, so no need to claim it first.
    let survivor = produced
        .iter()
        .find(|id| **id != claimed_id)
        .expect("the other produced id")
        .clone();
    let deleted: i64 = redis::cmd("XDEL")
        .arg("t1:q1")
        .arg(&survivor)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "one entry hard-deleted");
    let len: i64 = redis::cmd("XLEN")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 0, "no live entries after XDEL");
    let deleted_again: i64 = redis::cmd("XDEL")
        .arg("t1:q1")
        .arg(&survivor)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(deleted_again, 0, "deleting an absent id is a no-op");
}

/// Paginated XAUTOCLAIM (owed-item E.3 / Chunk 6c): the PEL is scanned in a stable id order; COUNT bounds
/// each page; the returned cursor advances and lands on `0-0` once the whole PEL is covered. A client
/// loops `0-0`→…→`0-0` and reclaims every in-flight entry EXACTLY once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xautoclaim_paginates_the_pel_cursor() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let clock = Arc::new(ManualClock::at(1_000));
    let (mut con, _) = serve_backend(backend.clone(), clock.clone()).await;

    // Produce 12 items (crossing the 10-entry boundary where a lexical id sort would mis-order
    // "…-10-0" before "…-2-0") and claim them ALL → the PEL has 12 in-flight entries (expiry = 1060).
    let mut produced = Vec::new();
    for p in 0..12 {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
        produced.push(id);
    }
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("COUNT")
        .arg(12)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 12, "all twelve leased");

    // COUNT 0 is rejected (it would never advance the cursor → client livelock).
    let zero: redis::RedisResult<redis::Value> = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c2")
        .arg(0)
        .arg("0-0")
        .arg("COUNT")
        .arg(0)
        .query_async(&mut con)
        .await;
    assert!(zero.is_err(), "COUNT 0 must be rejected");

    // Past expiry: page the PEL with COUNT 5 until the cursor returns to 0-0.
    clock.set(1_061);
    let mut reclaimed: Vec<String> = Vec::new();
    let mut cursor = "0-0".to_string();
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 30, "pagination did not terminate");
        let (next, entries, _del): AutoClaim = redis::cmd("XAUTOCLAIM")
            .arg("t1:q1")
            .arg("g")
            .arg("c2")
            .arg(0)
            .arg(&cursor)
            .arg("COUNT")
            .arg(5)
            .query_async(&mut con)
            .await
            .unwrap();
        for (id, _fields) in entries {
            reclaimed.push(id);
        }
        if next == "0-0" {
            break;
        }
        cursor = next;
    }
    reclaimed.sort();
    let mut expected = produced.clone();
    expected.sort();
    assert_eq!(
        reclaimed, expected,
        "every PEL entry reclaimed EXACTLY once across the pages"
    );
    assert!(
        guard >= 3,
        "COUNT 5 over 12 entries must take multiple pages (not single-shot)"
    );

    // A fresh full scan reclaims nothing — the reclaimed entries hold fresh (unexpired) leases.
    let (next, entries, _): AutoClaim = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("c2")
        .arg(0)
        .arg("0-0")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        entries.is_empty(),
        "freshly-reclaimed leases are not expired"
    );
    assert_eq!(next, "0-0");
}

/// Intra-group exclusion (owed-item E.4 / Chunk 6c): two consumers concurrently draining the SAME group
/// with `XREADGROUP >` never receive the same item — the single-writer engine serializes claims, so each
/// produced item is delivered to exactly one consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consumers_in_a_group_never_get_the_same_item() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con1, addr) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con2 = client.get_multiplexed_async_connection().await.unwrap();

    let mut produced = std::collections::HashSet::new();
    for p in 0..20 {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con1)
            .await
            .unwrap();
        produced.insert(id);
    }

    async fn drain(con: &mut redis::aio::MultiplexedConnection, consumer: &str) -> Vec<String> {
        let mut got = Vec::new();
        loop {
            let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg("g")
                .arg(consumer)
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS")
                .arg("t1:q1")
                .arg(">")
                .query_async(con)
                .await
                .unwrap();
            let Some(reply) = reply else { break };
            let ids: Vec<String> = reply
                .keys
                .iter()
                .flat_map(|k| k.ids.iter().map(|e| e.id.clone()))
                .collect();
            if ids.is_empty() {
                break;
            }
            got.extend(ids);
        }
        got
    }
    let (a, b) = tokio::join!(drain(&mut con1, "c1"), drain(&mut con2, "c2"));

    let set_a: std::collections::HashSet<_> = a.iter().cloned().collect();
    let set_b: std::collections::HashSet<_> = b.iter().cloned().collect();
    assert!(
        set_a.is_disjoint(&set_b),
        "no item delivered to BOTH consumers (intra-group exclusion)"
    );
    assert_eq!(a.len(), set_a.len(), "no item delivered twice to c1");
    assert_eq!(b.len(), set_b.len(), "no item delivered twice to c2");
    let union: std::collections::HashSet<_> = set_a.union(&set_b).cloned().collect();
    assert_eq!(
        union, produced,
        "every produced item delivered exactly once across the group"
    );
}

/// upsert ↔ claim race (owed-item E.4 / Chunk 6c, best-effort): an `XADD`-on-key (replace-pending) racing
/// a concurrent `XREADGROUP` claim of the same item leaves a CONSISTENT state — exactly one live entry,
/// no double-insert, no loss — whichever order the single-writer engine serializes them in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_and_claim_race_stays_consistent() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con1, addr) = serve_backend(backend.clone(), Arc::new(SystemClock)).await;
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con2 = client.get_multiplexed_async_connection().await.unwrap();

    // Seed one pending item under a client_item_key (so the next XADD-on-key is an upsert/replace).
    let _: redis::Value = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("k1")
        .arg("priority")
        .arg(5)
        .query_async(&mut con1)
        .await
        .unwrap();

    // Concurrently: (a) upsert the same key, (b) claim via XREADGROUP. One serializes before the other.
    let mut upsert_cmd = redis::cmd("XADD");
    upsert_cmd
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("k1")
        .arg("priority")
        .arg(9);
    let mut claim_cmd = redis::cmd("XREADGROUP");
    claim_cmd
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">");
    let (_u, _c) = tokio::join!(
        upsert_cmd.query_async::<redis::Value>(&mut con1),
        claim_cmd.query_async::<Option<StreamReadReply>>(&mut con2),
    );

    // Whichever won, exactly ONE live entry survives (claim→leased + upsert-collision-error, OR
    // upsert→supersede-old+new-pending then claim→leased). No double-insert, no loss.
    let len: i64 = redis::cmd("XLEN")
        .arg("t1:q1")
        .query_async(&mut con1)
        .await
        .unwrap();
    assert_eq!(
        len, 1,
        "exactly one live entry survives the upsert↔claim race"
    );
}

/// BQ-30 — a stock redis client bootstraps the cluster surface over the real RESP wire: `CLUSTER KEYSLOT`
/// returns the SAME slot the client itself would compute (the canonical Redis values), and `CLUSTER SLOTS`
/// advertises this single node owning the whole 0..=16383 slot space so a cluster-aware client can route.
#[tokio::test]
async fn cluster_bootstrap_over_the_wire() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, addr) = serve_backend(backend, Arc::new(SystemClock)).await;

    // CLUSTER KEYSLOT matches Redis's own slot computation (so a stock cluster client agrees with us).
    let foo: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("foo")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(foo, 12182, "CLUSTER KEYSLOT foo must match the Redis value");
    // The hash-tag rule is honored on the wire: {user1000}.x co-locates with user1000.
    let tagged: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("{user1000}.following")
        .query_async(&mut con)
        .await
        .unwrap();
    let plain: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("user1000")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(tagged, plain, "hash-tag {{user1000}} routes by its content");

    // CLUSTER MYID is a 40-hex node id.
    let myid: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(myid.len(), 40);
    assert!(myid.chars().all(|c| c.is_ascii_hexdigit()));

    // CLUSTER SLOTS: one range [0, 16383, [host, port, id]] — the full space owned by this node.
    let slots: Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut con)
        .await
        .unwrap();
    let Value::Array(ranges) = slots else {
        panic!("CLUSTER SLOTS must be an array, got {slots:?}");
    };
    assert_eq!(ranges.len(), 1, "single-node: exactly one slot range");
    let Value::Array(r) = &ranges[0] else {
        panic!("slot range must be an array");
    };
    assert_eq!(r[0], Value::Int(0));
    assert_eq!(r[1], Value::Int(16383), "covers the full slot space");
    let Value::Array(ep) = &r[2] else {
        panic!("endpoint must be an array");
    };
    assert_eq!(
        ep[1],
        Value::Int(addr.port() as i64),
        "advertises this node's bound port"
    );
    // The endpoint's node id matches MYID.
    let Value::BulkString(id_bytes) = &ep[2] else {
        panic!("node id must be a bulk string");
    };
    assert_eq!(String::from_utf8_lossy(id_bytes), myid);
}

/// BQ-30 — a REAL stock redis CLUSTER client bootstraps against the single pqueue node: `ClusterClient`
/// reads `CLUSTER SLOTS`, builds its routing table, and routes a command by computed slot to this node.
/// This is the definitive "a stock redis-cluster client bootstraps" evidence (the plain-client test above
/// proves the reply shapes + slot constants).
#[tokio::test]
async fn real_cluster_client_bootstraps_and_routes() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, backend, Arc::new(SystemClock)));

    // Build a cluster-aware client seeded with the single node; getting a connection triggers the bootstrap
    // handshake (CLUSTER SLOTS → routing table).
    let client = redis::cluster::ClusterClient::new(vec![format!("redis://{addr}")]).unwrap();
    let mut con = client
        .get_async_connection()
        .await
        .expect("cluster bootstrap");

    // Route a real command by slot: CLUSTER KEYSLOT goes to the node owning that slot (the only node).
    let slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("foo")
        .query_async(&mut con)
        .await
        .expect("routed command");
    assert_eq!(
        slot, 12182,
        "the bootstrapped cluster client routed to us and got the right slot"
    );
}
