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
use pqueue_engine::{
    Backend, CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore, FenceLeaseCommand,
    LogWriter, ProjectionWriter, QueueCommand, ShardId, ShardKey,
};
use pqueue_engine::Clock;
use pqueue_memory::{ManualClock, MemoryBackend};
use pqueue_resp::{serve, RespBackend, SystemClock};
use redis::streams::StreamReadReply;

/// Boot the RESP front over a real ephemeral TCP port with a fresh memory backend + created queue, and
/// return an off-the-shelf async Redis connection plus the backend handle (for operator-side setup).
async fn setup() -> (redis::aio::MultiplexedConnection, Arc<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
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

fn shard() -> ShardKey {
    ShardKey::new(
        TenantId::new("t1").unwrap(),
        QueueId::new("q1").unwrap(),
        ShardId::ZERO,
    )
}

/// Operator-fence a leased item directly on the backend (no RESP operator surface yet), so the e2e can
/// prove a fenced holder's XACK is refused with `-ERR pqueue stale_lease`.
async fn fence(backend: &MemoryBackend, id: &str) {
    let item = ItemId::new(id).unwrap();
    let env = CommandEnvelope {
        command_id: CommandId::new("fence"),
        request_id: None,
        shard_id: ShardId::ZERO,
        item_ids: vec![item.clone()],
        command: QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![item],
        }),
        checksum: CommandChecksum(0),
        created_at: UtcTimestamp::new(0, 0).unwrap(),
    };
    let sk = shard();
    backend
        .write(move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
            let pos = lw.append(&sk, std::slice::from_ref(&env))?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
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
        group_co_residency: false,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        shard_count: 1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_and_reconcile_with_offtheshelf_client() {
    let backend = Arc::new(MemoryBackend::new());
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
        .arg("GROUP").arg("g").arg("c")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let ids: Vec<_> = reply.keys.iter().flat_map(|k| &k.ids).collect();
    assert_eq!(ids.len(), 1, "same client_item_key upserts to a single pending item");
    let p: i64 = ids[0].get("priority").unwrap();
    assert_eq!(p, 20, "the upsert kept the replacement's priority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xpending_lists_leased_then_shrinks_on_ack() {
    let (mut con, _backend) = setup().await;
    for p in [10, 20] {
        let _: String = redis::cmd("XADD").arg("t1:q1").arg("*").arg("priority").arg(p)
            .query_async(&mut con).await.unwrap();
    }
    // Claim both → both pending (leased, not acked).
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("g").arg("c").arg("COUNT").arg(10)
        .arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con).await.unwrap();
    let claimed: Vec<String> = reply.keys.iter().flat_map(|k| k.ids.iter().map(|e| e.id.clone())).collect();
    assert_eq!(claimed.len(), 2);

    // XPENDING extended → 2 entries.
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1").arg("g").arg("-").arg("+").arg(10)
        .query_async(&mut con).await.unwrap();
    assert_eq!(pend.len(), 2, "both leased items are pending");

    // Ack one → XPENDING shrinks to 1.
    let _: i64 = redis::cmd("XACK").arg("t1:q1").arg("g").arg(&claimed[0])
        .query_async(&mut con).await.unwrap();
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1").arg("g").arg("-").arg("+").arg(10)
        .query_async(&mut con).await.unwrap();
    assert_eq!(pend.len(), 1, "acked item leaves the pending set");
    assert_eq!(pend[0].0, claimed[1], "the un-acked item remains pending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_lease_xack_is_stale_over_the_wire() {
    // Operator fences a leased item; the holder's XACK must surface `-ERR pqueue stale_lease` to the
    // stock client (TD-006 §3/§7) — not a silent success.
    let (mut con, backend) = setup().await;
    let _: String = redis::cmd("XADD").arg("t1:q1").arg("*").arg("priority").arg(5)
        .query_async(&mut con).await.unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("g").arg("c").arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con).await.unwrap();
    let id = reply.keys[0].ids[0].id.clone();

    fence(&backend, &id).await;

    let res: redis::RedisResult<i64> = redis::cmd("XACK").arg("t1:q1").arg("g").arg(&id)
        .query_async(&mut con).await;
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
        .arg("t1:q1").arg("*").arg("client_item_key").arg("dup").arg("priority").arg(50)
        .query_async(&mut con).await.unwrap();
    // Second XADD with the same key supersedes the first.
    let _new_id: String = redis::cmd("XADD")
        .arg("t1:q1").arg("*").arg("client_item_key").arg("dup").arg("priority").arg(20)
        .query_async(&mut con).await.unwrap();

    let res: redis::RedisResult<i64> = redis::cmd("XACK").arg("t1:q1").arg("g").arg(&old_id)
        .query_async(&mut con).await;
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
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap(); // max_lease_duration_ms = 60_000 (60s)
    let clock = Arc::new(ManualClock::at(1_000)); // t = 1000s
    let (mut con, _) = serve_backend(backend.clone(), clock.clone()).await;

    let _: String = redis::cmd("XADD").arg("t1:q1").arg("*").arg("priority").arg(5)
        .query_async(&mut con).await.unwrap();
    // Claim → leased, lease_expires_at = 1000s + 60s = 1060s, attempt_count = 1.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("g").arg("c").arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con).await.unwrap();
    assert_eq!(reply.keys[0].ids.len(), 1);

    // At exactly expiry (t == 1060): half-open lease still held → nothing reclaimed, and XPENDING still
    // shows the item leased with attempt_count == 1 (the boundary is load-bearing at the RESP layer).
    clock.set(1_060);
    // A large min-idle-time (1h) is deliberately ignored: pqueue reclaims by lease expiry, not idle.
    let (_c, entries, _d): AutoClaim = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1").arg("g").arg("c").arg(3_600_000).arg("0-0")
        .query_async(&mut con).await.unwrap();
    assert!(entries.is_empty(), "lease valid through the expiry instant; nothing reclaimed");
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1").arg("g").arg("-").arg("+").arg(10)
        .query_async(&mut con).await.unwrap();
    assert_eq!(pend.len(), 1, "still leased at the expiry instant (half-open)");
    assert_eq!(pend[0].3, 1, "attempt_count still 1 before any reclaim");

    // One unit past expiry: XAUTOCLAIM reclaims + redelivers. attempt_count is EXACTLY 2 — the reclaim
    // (LeaseExpired) does NOT charge (it's not a delivery); only the original claim(1) + the redelivery
    // (Claim, 1) charge. INVARIANT: attempt_count = number of deliveries (TD-006:129).
    clock.set(1_061);
    let (_c, entries, _d): AutoClaim = redis::cmd("XAUTOCLAIM")
        .arg("t1:q1").arg("g").arg("c").arg(3_600_000).arg("0-0")
        .query_async(&mut con).await.unwrap();
    assert_eq!(entries.len(), 1, "expired lease redelivered despite the large min-idle-time (ignored)");
    let fields = &entries[0].1;
    let attempt: i64 = fields.iter().find(|(k, _)| k == "attempt_count").unwrap().1.parse().unwrap();
    assert_eq!(attempt, 2, "claim(1) + redeliver(1) = 2; the reclaim does not charge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_recovery_rebuilds_durable_state_over_the_wire() {
    use pqueue_sqlite::SqliteBackend;
    let path = std::env::temp_dir().join(format!("pqueue-resp-crash-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let p = path.to_str().unwrap().to_string();

    // Session 1: produce 3, claim 1 (leave it leased + un-acked), then "crash".
    let leased_id = {
        let b = Arc::new(SqliteBackend::open(&p).unwrap());
        b.create_queue(qdef()).await.unwrap();
        let handle = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let h = tokio::spawn(serve(listener, b.clone(), Arc::new(SystemClock)));
            let client = redis::Client::open(format!("redis://{addr}")).unwrap();
            let mut con = client.get_multiplexed_async_connection().await.unwrap();
            for pr in [10, 20, 30] {
                let _: String = redis::cmd("XADD").arg("t1:q1").arg("*").arg("priority").arg(pr)
                    .query_async(&mut con).await.unwrap();
            }
            let reply: StreamReadReply = redis::cmd("XREADGROUP")
                .arg("GROUP").arg("g").arg("c").arg("COUNT").arg(1)
                .arg("STREAMS").arg("t1:q1").arg(">")
                .query_async(&mut con).await.unwrap();
            let id = reply.keys[0].ids[0].id.clone();
            (h, id)
        };
        handle.0.abort(); // crash the server
        handle.1
    }; // backend dropped → sqlite file closed

    // Session 2: reopen the SAME database — projection rebuilt from the durable log.
    let b = Arc::new(SqliteBackend::open(&p).unwrap());
    let (mut con, _) = serve_backend(b.clone(), Arc::new(SystemClock)).await;
    // The leased item survived as pending-in-flight.
    let pend: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1").arg("g").arg("-").arg("+").arg(10)
        .query_async(&mut con).await.unwrap();
    assert_eq!(pend.len(), 1, "the leased item is reconstructed as pending");
    assert_eq!(pend[0].0, leased_id);
    // The other two un-claimed items are still drainable.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("g").arg("c").arg("COUNT").arg(10)
        .arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con).await.unwrap();
    assert_eq!(reply.keys[0].ids.len(), 2, "the two unclaimed items survived the crash");

    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xadd_collision_with_leased_then_terminal_is_an_error() {
    // I4: XADD-on-key against an IN-FLIGHT item → `-ERR pqueue invalid`; against a TERMINAL item →
    // `-ERR pqueue terminal` (TD-006 §3 collision contract), never a silent success.
    let (mut con, _b) = setup().await;
    let a: String = redis::cmd("XADD").arg("t1:q1").arg("*")
        .arg("client_item_key").arg("k").arg("priority").arg(5)
        .query_async(&mut con).await.unwrap();
    // Claim it → leased.
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("g").arg("c").arg("STREAMS").arg("t1:q1").arg(">")
        .query_async(&mut con).await.unwrap();
    assert_eq!(reply.keys[0].ids[0].id, a);
    // Same-key XADD while leased → invalid collision.
    let res: redis::RedisResult<String> = redis::cmd("XADD").arg("t1:q1").arg("*")
        .arg("client_item_key").arg("k").arg("priority").arg(6)
        .query_async(&mut con).await;
    assert!(res.unwrap_err().to_string().contains("invalid"), "leased collision → -ERR pqueue invalid");
    // Ack → terminal. Same-key XADD → terminal collision.
    let _: i64 = redis::cmd("XACK").arg("t1:q1").arg("g").arg(&a).query_async(&mut con).await.unwrap();
    let res: redis::RedisResult<String> = redis::cmd("XADD").arg("t1:q1").arg("*")
        .arg("client_item_key").arg("k").arg("priority").arg(7)
        .query_async(&mut con).await;
    assert!(res.unwrap_err().to_string().contains("terminal"), "terminal collision → -ERR pqueue terminal");
}
