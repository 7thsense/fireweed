//! End-to-end: an OFF-THE-SHELF Redis client (`redis` crate) drives the fireweed RESP front over real
//! TCP: produce via XADD, drain via XREADGROUP `>`, ack via XACK, and reconcile that every
//! produced item is delivered exactly once, in priority order, with ties broken by insertion order
//! (plan section 3 drain-and-reconcile, validating Invariant 1 through the stock command surface).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use fireweed_engine::AsyncLogReplayBackend;
use fireweed_engine::Clock;
use fireweed_engine::{
    Backend, ClaimedItem, CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore,
    EngineError, FenceLeaseCommand, PayloadUpdate, ProjectionRead, QueueCommand, QueueKey,
    RawCommitRequest, UpdateFieldsPort,
};
use fireweed_memory::{InMemoryProjection, ManualClock, MemoryLog, composed_memory_backend};
use fireweed_resp::{RespBackend, SystemClock, serve};
use redis::Value;
use redis::streams::StreamReadReply;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Boot the RESP front over a real ephemeral TCP port with a fresh memory backend + created queue, and
/// return an off-the-shelf async Redis connection plus the backend handle (for operator-side setup).
async fn setup() -> (
    redis::aio::MultiplexedConnection,
    Arc<AsyncLogReplayBackend<MemoryLog, InMemoryProjection>>,
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
/// prove a fenced holder's XACK is refused with `-ERR fireweed stale_lease`.
async fn fence(backend: &AsyncLogReplayBackend<MemoryLog, InMemoryProjection>, id: &str) {
    let item = ItemId::new(id).unwrap();
    let env = CommandEnvelope {
        command_id: CommandId::new("fence"),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
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
        .commit_raw(RawCommitRequest::new(sk, vec![env], epoch))
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
        emit_change_records: true,
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
async fn resp_wire_carries_updated_field_a_and_field_c() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, _) = serve_backend(backend.clone(), Arc::new(ManualClock::at(1_000))).await;
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
        .arg("field-a")
        .arg("value-a")
        .arg("field-b")
        .arg("value-b")
        .query_async(&mut con)
        .await
        .unwrap();

    let item_id = ItemId::new(&id).unwrap();
    let updated_version = backend
        .update_fields(
            &shard(),
            item_id,
            BTreeMap::from([
                (
                    "field-a".to_string(),
                    Some(Bytes::from_static(b"value-a-2")),
                ),
                ("field-b".to_string(), None),
                ("field-c".to_string(), Some(Bytes::from_static(b"value-c"))),
            ]),
            PayloadUpdate::Keep,
            None,
            Some(1),
            UtcTimestamp::new(1_000, 0).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(updated_version, 2, "update_fields bumps item_version");

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
        claimed.get::<Vec<u8>>("field-a").as_deref(),
        Some(b"value-a-2".as_slice()),
        "RESP wire must carry the updated field-a value"
    );
    assert_eq!(
        claimed.get::<Vec<u8>>("field-c").as_deref(),
        Some(b"value-c".as_slice()),
        "RESP wire must carry the newly added field-c value"
    );

    let claimed_view: Vec<ClaimedItem> = backend.claimed_view(&shard(), &[item_id]).await.unwrap();
    assert_eq!(claimed_view.len(), 1);
    assert_eq!(
        claimed.get::<u64>("item_version"),
        Some(claimed_view[0].item_version)
    );
    assert_claimed_entry_parity(claimed, &claimed_view[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp_oracle_does_not_use_backend_echo() {
    let backend = Arc::new(LyingClaimedViewBackend::new(composed_memory_backend()));
    backend.create_queue(qdef()).await.unwrap();
    let (mut con, _) = serve_backend(backend.clone(), Arc::new(ManualClock::at(1_000))).await;

    let id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("client_item_key")
        .arg("echo-key")
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
        .arg("field-a")
        .arg("value-a")
        .arg("field-b")
        .arg("value-b")
        .query_async(&mut con)
        .await
        .unwrap();

    let item_id = ItemId::new(&id).unwrap();
    let updated_version = backend
        .update_fields(
            &shard(),
            item_id,
            BTreeMap::from([
                (
                    "field-a".to_string(),
                    Some(Bytes::from_static(b"value-a-2")),
                ),
                ("field-b".to_string(), None),
                ("field-c".to_string(), Some(Bytes::from_static(b"value-c"))),
            ]),
            PayloadUpdate::Keep,
            None,
            Some(1),
            UtcTimestamp::new(1_000, 0).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(updated_version, 2, "update_fields bumps item_version");

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
    assert_eq!(
        claimed.get::<Vec<u8>>("field-a").as_deref(),
        Some(b"value-a-2".as_slice()),
        "the wire assertion must use the response payload, not backend echo"
    );
    assert_eq!(
        claimed.get::<Vec<u8>>("field-c").as_deref(),
        Some(b"value-c".as_slice()),
        "the wire assertion must use the response payload, not backend echo"
    );

    let claimed_view: Vec<ClaimedItem> = backend.claimed_view(&shard(), &[item_id]).await.unwrap();
    assert_eq!(claimed_view.len(), 1);
    assert_eq!(
        claimed_view[0]
            .fields
            .get("field-a")
            .map(|bytes| bytes.as_ref()),
        Some(b"value-a".as_slice()),
        "the backend echo is intentionally stale in this test"
    );
    assert!(
        !claimed_view[0].fields.contains_key("field-c"),
        "the backend echo is intentionally stale in this test"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_names_rejected_on_write_paths() {
    let (mut con, backend) = setup().await;
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

    let id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(7)
        .query_async(&mut con)
        .await
        .unwrap();
    let item_id = ItemId::new(&id).unwrap();
    let err = backend
        .update_fields(
            &shard(),
            item_id,
            BTreeMap::from([
                (
                    "lease_token".to_string(),
                    Some(Bytes::from_static(b"user-value")),
                ),
                (
                    "payload".to_string(),
                    Some(Bytes::from_static(b"user-payload")),
                ),
            ]),
            PayloadUpdate::Keep,
            None,
            None,
            UtcTimestamp::new(1, 0).unwrap(),
            None,
        )
        .await
        .expect_err("reserved fields are rejected before commit");
    assert!(matches!(err, EngineError::Invalid(_)));
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

fn bulk_string_text(value: &Value) -> String {
    String::from_utf8(bulk_string(value)).expect("RESP bulk string must be valid UTF-8")
}

fn field_value(fields: &[Value], name: &str) -> Option<Vec<u8>> {
    for pair in fields.chunks_exact(2) {
        if bulk_string(&pair[0]) == name.as_bytes() {
            return Some(bulk_string(&pair[1]));
        }
    }
    None
}

fn ts_millis(ts: UtcTimestamp) -> i64 {
    ts.seconds * 1_000 + i64::from(ts.nanoseconds / 1_000_000)
}

#[tokio::test(flavor = "multi_thread")]
async fn thousand_prebuffered_xadds_preserve_ordered_independent_scalar_semantics() {
    let backend = Arc::new(LyingClaimedViewBackend::new(composed_memory_backend()));
    backend.inner.create_queue(qdef()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Connect and fill the kernel receive queue before accept. This exercises the real socket reader while
    // making the entire bounded pipeline available to its single 1 MiB incremental read window.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut pipeline = Vec::new();
    for priority in 0..1_000 {
        let priority = priority.to_string();
        let args = [
            b"XADD".as_slice(),
            b"t1:q1".as_slice(),
            b"*".as_slice(),
            b"priority".as_slice(),
            priority.as_bytes(),
            b"payload".as_slice(),
            b"body".as_slice(),
        ];
        pipeline.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            pipeline.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            pipeline.extend_from_slice(arg);
            pipeline.extend_from_slice(b"\r\n");
        }
    }
    socket.write_all(&pipeline).await.unwrap();
    let server = tokio::spawn(serve(listener, backend.clone(), Arc::new(SystemClock)));

    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    for _ in 0..1_000 {
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with('$'));
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.ends_with("\r\n"));
    }
    let batch_sizes = backend.push_batch_sizes.lock().unwrap();
    assert_eq!(batch_sizes.len(), 1_000);
    assert!(batch_sizes.iter().all(|size| *size == 1));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_bulk_terminator_closes_after_valid_prefix_without_running_suffix() {
    fn raw(args: &[&[u8]]) -> Vec<u8> {
        let mut command = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            command.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            command.extend_from_slice(arg);
            command.extend_from_slice(b"\r\n");
        }
        command
    }

    let backend = Arc::new(LyingClaimedViewBackend::new(composed_memory_backend()));
    backend.inner.create_queue(qdef()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let valid = raw(&[b"XADD", b"t1:q1", b"*", b"priority", b"1"]);
    let mut pipeline = valid.clone();
    pipeline.extend_from_slice(b"*1\r\n$4\r\nPINGxx");
    pipeline.extend_from_slice(&valid);
    socket.write_all(&pipeline).await.unwrap();
    let server = tokio::spawn(serve(listener, backend.clone(), Arc::new(SystemClock)));

    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert!(line.starts_with('$'));
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert!(line.ends_with("\r\n"));
    line.clear();
    assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
    assert_eq!(backend.push_batch_sizes.lock().unwrap().as_slice(), &[1]);
    server.abort();
}

struct LyingClaimedViewBackend {
    inner:
        Arc<AsyncLogReplayBackend<fireweed_memory::MemoryLog, fireweed_memory::InMemoryProjection>>,
    push_batch_sizes: Mutex<Vec<usize>>,
}

impl LyingClaimedViewBackend {
    fn new(
        inner: AsyncLogReplayBackend<
            fireweed_memory::MemoryLog,
            fireweed_memory::InMemoryProjection,
        >,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            push_batch_sizes: Mutex::new(Vec::new()),
        }
    }
}

impl fireweed_engine::PushPort for LyingClaimedViewBackend {
    fn push(
        &self,
        shard: &fireweed_engine::QueueKey,
        items: Vec<fireweed_engine::PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<Vec<ItemId>>> + Send {
        self.push_batch_sizes.lock().unwrap().push(items.len());
        self.inner.push(shard, items, now, expected_epoch)
    }

    fn push_with_request_id(
        &self,
        shard: &fireweed_engine::QueueKey,
        request_id: fireweed_core::RequestId,
        items: Vec<fireweed_engine::PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::PushBatchOutcome>,
    > + Send {
        self.inner
            .push_with_request_id(shard, request_id, items, now, expected_epoch)
    }
}

impl fireweed_engine::ClaimPort for LyingClaimedViewBackend {
    fn claim(
        &self,
        req: fireweed_engine::ClaimRequest,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<fireweed_engine::Claimed>> + Send
    {
        self.inner.claim(req)
    }
}

impl fireweed_engine::UpsertPort for LyingClaimedViewBackend {
    fn replace_if_pending(
        &self,
        shard: &fireweed_engine::QueueKey,
        client_item_key: &fireweed_core::ClientItemKey,
        priority: Option<fireweed_core::PriorityValue>,
        group_key: Option<fireweed_core::GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: fireweed_core::Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::UpsertOutcome>,
    > + Send {
        self.inner.replace_if_pending(
            shard,
            client_item_key,
            priority,
            group_key,
            not_before,
            payload,
            fields,
            metadata,
            entity,
            now,
            expected_epoch,
        )
    }
}

impl UpdateFieldsPort for LyingClaimedViewBackend {
    fn update_fields(
        &self,
        shard: &fireweed_engine::QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<u64>> + Send {
        self.inner.update_fields(
            shard,
            item_id,
            field_ops,
            payload,
            entity,
            expected_item_version,
            now,
            expected_epoch,
        )
    }
}

impl fireweed_engine::FinalizePort for LyingClaimedViewBackend {
    fn finalize(
        &self,
        shard: &fireweed_engine::QueueKey,
        outcomes: Vec<fireweed_engine::FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<()>> + Send {
        self.inner.finalize(shard, outcomes, now, expected_epoch)
    }
}

impl fireweed_engine::RenewLeasePort for LyingClaimedViewBackend {
    fn renew(
        &self,
        shard: &fireweed_engine::QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<()>> + Send {
        self.inner
            .renew(shard, item_ids, new_lease_expires_at, now, expected_epoch)
    }
}

impl fireweed_engine::ReassignLeasePort for LyingClaimedViewBackend {
    fn reassign(
        &self,
        shard: &fireweed_engine::QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: fireweed_core::LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<()>> + Send {
        self.inner.reassign(
            shard,
            item_ids,
            new_lease_token,
            new_lease_expires_at,
            now,
            expected_epoch,
        )
    }
}

impl fireweed_engine::PurgePort for LyingClaimedViewBackend {
    fn purge(
        &self,
        shard: &fireweed_engine::QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<u64>> + Send {
        self.inner
            .purge(shard, item_ids, force, now, expected_epoch)
    }
}

impl fireweed_engine::ReclaimDriver for LyingClaimedViewBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::TickReport>,
    > + Send {
        self.inner.tick(now)
    }
}

impl fireweed_engine::ControlPlaneStore for LyingClaimedViewBackend {
    fn create_queue(
        &self,
        definition: fireweed_core::QueueDefinition,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::CreateQueueOutcome>,
    > + Send {
        self.inner.create_queue(definition)
    }

    fn queue_definition(
        &self,
        key: &fireweed_engine::QueueKey,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_core::QueueDefinition>,
    > + Send {
        self.inner.queue_definition(key)
    }

    fn list_queues(
        &self,
        tenant: &fireweed_core::TenantId,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<Vec<fireweed_core::QueueId>>,
    > + Send {
        self.inner.list_queues(tenant)
    }

    fn current_epoch(
        &self,
        shard: &fireweed_engine::QueueKey,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<u64>> + Send {
        self.inner.current_epoch(shard)
    }

    fn acquire_epoch(
        &self,
        shard: &fireweed_engine::QueueKey,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<u64>> + Send {
        self.inner.acquire_epoch(shard)
    }
}

impl fireweed_engine::ProjectionRead for LyingClaimedViewBackend {
    fn select_eligible(
        &self,
        shard: &fireweed_engine::QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<Vec<ItemId>>> + Send {
        self.inner.select_eligible(shard, now, limit)
    }

    fn peek(
        &self,
        shard: &fireweed_engine::QueueKey,
        limit: usize,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<Vec<fireweed_engine::ItemView>>,
    > + Send {
        self.inner.peek(shard, limit)
    }

    fn pending(
        &self,
        shard: &fireweed_engine::QueueKey,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<Vec<fireweed_engine::LeaseView>>,
    > + Send {
        self.inner.pending(shard)
    }

    fn claimed_view(
        &self,
        shard: &fireweed_engine::QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = fireweed_engine::EngineResult<Vec<ClaimedItem>>> + Send
    {
        let inner = self.inner.clone();
        let shard = shard.clone();
        let ids = ids.to_vec();
        async move {
            let mut items = inner.claimed_view(&shard, &ids).await?;
            for item in &mut items {
                item.fields
                    .insert("field-a".to_string(), Bytes::from_static(b"value-a"));
                item.fields.remove("field-c");
            }
            Ok(items)
        }
    }

    fn live_items(
        &self,
        shard: &fireweed_engine::QueueKey,
        keys: &[fireweed_core::ClientItemKey],
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<Vec<Option<fireweed_engine::LiveItemView>>>,
    > + Send {
        self.inner.live_items(shard, keys)
    }

    fn metrics(
        &self,
        queue: &fireweed_engine::QueueKey,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::QueueMetrics>,
    > + Send {
        self.inner.metrics(queue)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &fireweed_engine::QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&fireweed_engine::CommandPosition>,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::TerminalEmissionMetrics>,
    > + Send {
        self.inner
            .terminal_emission_metrics(shard, now, emit_change_records, emission_cursor)
    }
}

impl fireweed_engine::HotProjectionQueryPort for LyingClaimedViewBackend {
    fn hot_projection_capabilities(
        &self,
        shard: &fireweed_engine::QueueKey,
    ) -> fireweed_core::QueryCapabilityFlags {
        self.inner.hot_projection_capabilities(shard)
    }

    fn claim_by_item_ids(
        &self,
        shard: &fireweed_engine::QueueKey,
        request: fireweed_core::ClaimByItemIdsRequest,
        context: fireweed_engine::ClaimByQueryContext,
    ) -> impl std::future::Future<
        Output = fireweed_engine::EngineResult<fireweed_engine::ClaimByItemIdsResponse>,
    > + Send {
        self.inner.claim_by_item_ids(shard, request, context)
    }
}

fn assert_claimed_entry_parity(entry: &redis::streams::StreamId, claimed: &ClaimedItem) {
    let item_id = claimed.item_id.to_string();
    assert_eq!(
        entry.get::<String>("item_id").as_deref(),
        Some(item_id.as_str())
    );
    assert_eq!(entry.id, item_id);
    assert_eq!(
        entry.get::<String>("client_item_key").as_deref(),
        Some(claimed.client_item_key.as_str())
    );
    assert_eq!(entry.get::<u64>("item_version"), Some(claimed.item_version));
    assert_eq!(
        entry.get::<i64>("priority"),
        match claimed.priority.as_ref() {
            Some(fireweed_core::PriorityValue::Int64(n)) => Some(*n),
            _ => None,
        }
    );
    assert_eq!(
        entry.get::<String>("lease_token").as_deref(),
        claimed.lease_token.as_ref().map(|token| token.as_str())
    );
    assert_eq!(
        entry.get::<i64>("lease_expires_at"),
        Some(ts_millis(claimed.lease_expires_at))
    );
    assert_eq!(
        entry.get::<u32>("attempt_count"),
        Some(claimed.attempt_count)
    );
    assert_eq!(
        entry.get::<Vec<u8>>("payload").as_deref(),
        claimed.payload.as_deref()
    );
    assert_eq!(
        entry.get::<String>("group_key").as_deref(),
        claimed.group_key.as_ref().map(|group| group.as_str())
    );
    assert_eq!(
        entry.get::<i64>("not_before"),
        claimed.not_before.map(ts_millis)
    );
    assert_eq!(
        entry.get::<String>("metadata").as_deref(),
        Some(r#"{"tenant_segment":{"String":"vip"}}"#)
    );
    assert!(
        entry.get::<String>("gate_keys").is_none(),
        "gate_keys are omitted for gate_keys=none queues"
    );

    for (name, value) in &claimed.fields {
        assert_eq!(
            entry.get::<Vec<u8>>(name.as_str()).as_deref(),
            Some(value.as_ref()),
            "RESP entry must carry the current structured field map"
        );
    }
    assert_eq!(
        entry.get::<Vec<u8>>("field-b").as_deref(),
        None,
        "removed fields stay absent from the claimed shape"
    );
}

// TD-006: "consumer group" on this surface is stock Redis Streams wire vocabulary only.
// fireweed accepts XGROUP/XREADGROUP group names for stock-client compatibility but never persists
// them — delivery is priority-ordered per-item tracking under one implicit group. This has nothing
// to do with Kafka consumer groups, which are entirely fjord's concern on the change-log surface
// (ADR-014): fireweed produces to fjord; fjord does Kafka things.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp_named_consumer_groups_are_wire_fiction_not_persisted() {
    let (mut con, _backend) = setup().await;

    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(11)
        .query_async(&mut con)
        .await
        .unwrap();

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("t1:q1")
        .arg("worker-group")
        .arg("0")
        .query_async(&mut con)
        .await
        .unwrap();

    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("worker-group")
        .arg("worker-1")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 1);
    let claimed_id = reply.keys[0].ids[0].id.clone();

    let _: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("worker-group")
        .arg(&claimed_id)
        .query_async(&mut con)
        .await
        .unwrap();

    let groups: Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    let Value::Array(groups) = groups else {
        panic!("XINFO GROUPS must be an array, got {groups:?}");
    };
    assert_eq!(groups.len(), 1, "fireweed keeps only its implicit group");
    let Value::Array(group) = &groups[0] else {
        panic!("group entry must be an array");
    };
    let mut group_fields = std::collections::HashMap::new();
    for pair in group.chunks_exact(2) {
        group_fields.insert(bulk_string_text(&pair[0]), pair[1].clone());
    }
    assert_eq!(
        group_fields.get("name").map(bulk_string_text),
        Some("default".to_string()),
        "named consumer groups are not stored in fireweed"
    );
    assert_eq!(
        group_fields.get("pending").and_then(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        }),
        Some(0),
        "acked leases are not retained as group-owned offset state"
    );

    let stream_info: std::collections::HashMap<String, Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        stream_info
            .get("last-delivered-id")
            .map(bulk_string_text)
            .as_deref(),
        Some("0-0"),
        "fireweed does not persist committed-stream offsets"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fireweed_live_hash_reads_return_structured_fields_until_ack() {
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
        redis::cmd("FW.HGETALL")
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
        redis::cmd("FW.HMGET")
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
        redis::cmd("FW.MGET")
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
        redis::cmd("FW.HMGET")
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
    // Operator fences a leased item; the holder's XACK must surface `-ERR fireweed stale_lease` to the
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
        "expected -ERR fireweed stale_lease, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xack_of_superseded_id_is_superseded_over_the_wire() {
    // After an XADD-on-key upsert, the OLD id is superseded; acking it must surface
    // `-ERR fireweed superseded` (TD-006 §3/§6.5), NOT the generic `-ERR fireweed invalid`.
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
        "expected -ERR fireweed superseded, got: {err}"
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
    // A large min-idle-time (1h) is deliberately ignored: fireweed reclaims by lease expiry, not idle.
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
    use fireweed_sqlite::composed_sqlite_backend;
    let path = std::env::temp_dir().join(format!("fireweed-resp-crash-{}.db", std::process::id()));
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
    // I4: XADD-on-key against an IN-FLIGHT item → `-ERR fireweed invalid`; against a TERMINAL item →
    // `-ERR fireweed terminal` (TD-006 §3 collision contract), never a silent success.
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
        "leased collision → -ERR fireweed invalid"
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
        "terminal collision → -ERR fireweed terminal"
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

/// TD-006 §3 first-delivery disposition: XCLAIM of pending eligible entry ids reserves them via
/// BatchClaimByItemIds without a prior XREADGROUP `>`. Non-claimable ids in the same command are
/// omitted (Redis shape), not fatal to successful peers. XACK then succeeds for the claimed entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xclaim_first_delivery_pending_ids_without_xreadgroup() {
    let (mut con, _backend) = setup().await;

    let id_a: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    let id_b: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();

    // First delivery: XCLAIM pending ids with consumer as lease token.
    let claimed: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("external-trigger")
        .arg(0)
        .arg(&id_a)
        .arg(&id_b)
        .arg("999999999") // valid ItemId shape, not present in the queue
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        claimed,
        vec![id_a.clone(), id_b.clone()],
        "pending eligible ids first-deliver; missing id is omitted"
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
    assert_eq!(pend.len(), 2);
    for row in &pend {
        assert_eq!(
            row.1, "external-trigger",
            "consumer name is the lease token"
        );
        assert_eq!(row.3, 1, "first delivery charges attempt_count = 1");
    }

    // XACK succeeds without prior XREADGROUP.
    let acked: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&id_a)
        .arg(&id_b)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(acked, 2);

    // Self-renew still works on a freshly first-delivered lease (new item).
    let id_c: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    let _: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("holder")
        .arg(0)
        .arg(&id_c)
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    let renewed: Vec<String> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg("holder")
        .arg(0)
        .arg(&id_c)
        .arg("JUSTID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(renewed, vec![id_c.clone()]);
    let pend_c: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("t1:q1")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    let row = pend_c.iter().find(|r| r.0 == id_c).expect("id_c pending");
    assert_eq!(row.1, "holder");
    assert_eq!(
        row.3, 1,
        "self-renew after first-delivery does not re-charge"
    );
}

/// XLEN / XDEL / XINFO over the stock client (owed-item E.2 / Chunk 6b). XLEN counts LIVE entries
/// (pending + in-flight), terminal/acked entries drop out; XDEL hard-removes and returns the count;
/// XINFO STREAM/GROUPS summarize. These are fireweed-flavored reads — divergences documented in TD-006 §3.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_terminal_count_published_in_metrics() {
    let (mut con, _backend) = setup().await;

    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
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
    let claimed_id = reply.keys[0].ids[0].id.clone();
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
    assert_eq!(len, 0, "terminal entries stay out of the live count");

    let info: std::collections::HashMap<String, redis::Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        match &info["resident-terminal-count"] {
            redis::Value::Int(n) => *n,
            other => panic!("XINFO STREAM resident-terminal-count should be an int, got {other:?}"),
        },
        1,
        "terminal residency is surfaced in production metrics",
    );
    assert_eq!(
        match &info["length"] {
            redis::Value::Int(n) => *n,
            other => panic!("XINFO STREAM length should be an int, got {other:?}"),
        },
        0,
        "the live-count semantic stays unchanged",
    );
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

/// BQ-30 — a REAL stock redis CLUSTER client bootstraps against the single fireweed node: `ClusterClient`
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
