#![forbid(unsafe_code)]
//! # pqueue-resp
//!
//! Minimal RESP/Redis-Streams driving adapter over the engine ports (Phase 1d smoke front). It maps
//! the worker hot path - `XADD` -> insert/upsert, `XREADGROUP >` -> priority claim,
//! `XACK` -> finalize-complete - so an off-the-shelf Redis client can drive it.
//! The full command surface, auth, idempotency, and the proper engine orchestration
//! layer land in later phases (TD-006; plan section 3/4). Unsupported
//! commands return `-ERR`, never a silent stub.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, MetadataValue, PriorityValue, QueueId,
    TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimPort, ClaimRequest, ClaimedItem, Clock, ControlPlaneStore, EngineError,
    EngineResult, FinalizeKind, FinalizeOutcome, FinalizePort, LeaseView, LiveItemView,
    ProjectionRead, PurgePort, PushPort, PushSpec, QueueKey, ReassignLeasePort, ReclaimDriver,
    RenewLeasePort, UpsertOutcome, UpsertPort,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

mod cluster;
mod drain;
mod routing;
pub use cluster::{ClusterNode, hash_slot, queue_routing_key, queue_slot};
pub use drain::{DrainClass, drain_class, is_new_claim_on_drain};
pub use routing::{RouteDecision, route};

/// The backend capabilities the RESP front needs. A concrete backend (e.g. `MemoryBackend`) is
/// injected by the composition root / tests; the adapter never names one (hexagonal).
pub trait RespBackend:
    Backend
    + PushPort
    + ClaimPort
    + UpsertPort
    + FinalizePort
    + RenewLeasePort
    + ReassignLeasePort
    + PurgePort
    + ReclaimDriver
    + ControlPlaneStore
    + ProjectionRead
    + Send
    + Sync
    + 'static
{
}
impl<T> RespBackend for T where
    T: Backend
        + PushPort
        + ClaimPort
        + UpsertPort
        + FinalizePort
        + RenewLeasePort
        + ReassignLeasePort
        + PurgePort
        + ReclaimDriver
        + ControlPlaneStore
        + ProjectionRead
        + Send
        + Sync
        + 'static
{
}

pub trait RespHooks: Send + Sync + 'static {
    /// Optional live-routing hook. Default single-node/backward-compatible behavior serves locally.
    fn route_command(
        &self,
        _name: &str,
        _args: &[Vec<u8>],
        _routing_key: &[u8],
        _now: UtcTimestamp,
        _is_new_claim: bool,
    ) -> impl std::future::Future<Output = EngineResult<RouteDecision>> + Send {
        std::future::ready(Ok(RouteDecision::Serve))
    }

    /// Optional ownership fence hook for queue writes. Default backends run the degenerate sole-owner path.
    fn expected_epoch_for_write(
        &self,
        _shard: &QueueKey,
        _now: UtcTimestamp,
        _is_new_claim: bool,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        std::future::ready(Ok(None))
    }
}

#[derive(Debug, Default)]
pub struct NoopRespHooks;

impl RespHooks for NoopRespHooks {}

struct ServerState {
    ids: AtomicU64,
    clock: Arc<dyn Clock>,
    /// This node's advertised cluster identity (for the CLUSTER bootstrap replies). Single-node today: it
    /// advertises owning all slots (BQ-30); the multi-node slot→owner view + `-MOVED` is BQ-31.
    node: cluster::ClusterNode,
}

impl ServerState {
    /// Monotonic unique id: one shared pool for item ids, lease tokens, and command ids (all just
    /// need uniqueness over this front).
    fn next(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }
    fn now(&self) -> UtcTimestamp {
        self.clock.now()
    }
}

/// Wall-clock `Clock` for production use; tests inject a controllable clock (e.g. `ManualClock`).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

/// `ts + millis`, normalizing nanoseconds (used to derive a lease expiry from `now`).
fn add_millis(ts: UtcTimestamp, millis: u64) -> UtcTimestamp {
    let total_nanos =
        ts.seconds as i128 * 1_000_000_000 + ts.nanoseconds as i128 + millis as i128 * 1_000_000;
    let seconds = (total_nanos.div_euclid(1_000_000_000)) as i64;
    let nanos = (total_nanos.rem_euclid(1_000_000_000)) as u32;
    UtcTimestamp::new(seconds, nanos).expect("valid ts")
}

fn ts_ms(ts: UtcTimestamp) -> i64 {
    ts.seconds * 1000 + (ts.nanoseconds / 1_000_000) as i64
}

fn ms_ts(ms: i64) -> Result<UtcTimestamp, EngineError> {
    let seconds = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as u32) * 1_000_000;
    UtcTimestamp::new(seconds, nanos).map_err(|_| EngineError::Invalid("bad timestamp"))
}

/// Parse a stream key `tenant:queue` (or bare `queue` with a default tenant) into a launch shard key.
fn parse_shard(key: &[u8]) -> Result<QueueKey, EngineError> {
    let s = std::str::from_utf8(key).map_err(|_| EngineError::Invalid("non-utf8 key"))?;
    let (tenant, queue) = match s.split_once(':') {
        Some((t, q)) => (t, q),
        None => ("default", s),
    };
    let tenant = TenantId::new(tenant).map_err(|_| EngineError::Invalid("bad tenant"))?;
    let queue = QueueId::new(queue).map_err(|_| EngineError::Invalid("bad queue"))?;
    Ok(QueueKey::new(tenant, queue))
}

// ---------------------------------------------------------------------------
// RESP encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    NullBulk,
    Array(Vec<Resp>),
    NullArray,
}

fn encode(r: &Resp, out: &mut Vec<u8>) {
    match r {
        Resp::Simple(s) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Error(s) => {
            out.extend_from_slice(b"-");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Int(n) => {
            out.extend_from_slice(format!(":{n}\r\n").as_bytes());
        }
        Resp::Bulk(b) => {
            out.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Resp::NullBulk => out.extend_from_slice(b"$-1\r\n"),
        Resp::NullArray => out.extend_from_slice(b"*-1\r\n"),
        Resp::Array(items) => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            for it in items {
                encode(it, out);
            }
        }
    }
}

/// Read one client command (RESP array of bulk strings). `Ok(None)` on clean EOF.
async fn read_command<R: AsyncBufRead + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let mut header = String::new();
    if r.read_line(&mut header).await? == 0 {
        return Ok(None);
    }
    let header = header.trim_end();
    if !header.starts_with('*') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected RESP array",
        ));
    }
    let count: usize = header[1..]
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad array len"))?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut bulk_header = String::new();
        if r.read_line(&mut bulk_header).await? == 0 {
            return Ok(None);
        }
        let bulk_header = bulk_header.trim_end();
        if !bulk_header.starts_with('$') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        }
        let len: usize = bulk_header[1..]
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad bulk len"))?;
        let mut buf = vec![0u8; len + 2];
        r.read_exact(&mut buf).await?;
        buf.truncate(len);
        args.push(buf);
    }
    Ok(Some(args))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Serve RESP connections over `listener`, dispatching to `backend`, with no external shutdown signal.
/// `clock` supplies wall time for lease expiry + reclaim (inject a controllable clock in tests). The
/// accept loop runs until `listener.accept()` errors (the listener is dropped/closed); it then **drains**
/// the in-flight connection handlers before returning — so an idle keep-alive connection will keep this
/// future pending until that connection closes. Production code wants [`serve_with_shutdown`], which adds
/// a cancellation signal and a caller-bounded drain. (Every caller of `serve` today spawns it detached
/// and aborts the handle, so the post-accept drain is never reached in practice.)
pub async fn serve<B: RespBackend>(listener: TcpListener, backend: Arc<B>, clock: Arc<dyn Clock>) {
    // A token that is never cancelled: the accept loop ends only when the listener errors.
    serve_with_shutdown(listener, backend, clock, CancellationToken::new()).await;
}

/// Serve RESP connections until either the listener errors OR `cancel` fires. On cancel the accept loop
/// stops taking new connections and the in-flight per-connection handlers are **drained**: each observes
/// `cancel` between commands (finishing any command already in flight) and exits, and this future awaits
/// them all before returning — no detached stragglers on the happy path.
///
/// The handlers are owned by a [`JoinSet`], so the drain is genuinely **bounded**: if the caller wraps
/// this future in a timeout (as `pqueue-server`'s `shutdown_and_drain` does) and the bound elapses, the
/// caller aborts this future; dropping it drops the `JoinSet`, which ABORTS every handler still
/// running — including one wedged inside a single command that never reaches the between-commands cancel
/// check. That is the hard bound a `TaskTracker` could not provide (it does not abort on drop).
pub async fn serve_with_shutdown<B: RespBackend>(
    listener: TcpListener,
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    cancel: CancellationToken,
) {
    serve_with_shutdown_and_hooks(listener, backend, Arc::new(NoopRespHooks), clock, cancel).await;
}

pub async fn serve_with_shutdown_and_hooks<B: RespBackend, H: RespHooks>(
    listener: TcpListener,
    backend: Arc<B>,
    hooks: Arc<H>,
    clock: Arc<dyn Clock>,
    cancel: CancellationToken,
) {
    // Derive this node's advertised cluster identity from the bound address. An unspecified bind host
    // (0.0.0.0/::) is not connectable, so advertise loopback for a stock client connecting locally.
    let node = match listener.local_addr() {
        Ok(addr) => {
            let ip = addr.ip();
            let host = if ip.is_unspecified() {
                "127.0.0.1".to_string()
            } else {
                ip.to_string()
            };
            cluster::ClusterNode::new(host, addr.port())
        }
        Err(_) => cluster::ClusterNode::new("127.0.0.1", 0),
    };
    let state = Arc::new(ServerState {
        ids: AtomicU64::new(1),
        clock,
        node,
    });
    // The JoinSet OWNS the handler tasks (unlike a TaskTracker, which only observes them): dropping it
    // aborts every task, which is what makes the caller-bounded drain a hard bound.
    let mut conns: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            // Stop accepting the moment shutdown is signalled (biased so a pending cancel wins a ready
            // accept, making the drain deterministic).
            biased;
            _ = cancel.cancelled() => break,
            // Reap finished handlers so the set tracks only LIVE connections (bounded memory on a
            // long-running server). Disabled while empty so `join_next` does not return `None` and spin.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    break;
                };
                // RESP is a small-message request/reply protocol: leaving Nagle on coalesces each tiny
                // reply and, paired with the peer's delayed-ACK, stalls a pipelined connection ~40ms per
                // command over a real (non-loopback) link. Disable it so replies flush immediately.
                let _ = stream.set_nodelay(true);
                let backend = backend.clone();
                let hooks = hooks.clone();
                let state = state.clone();
                let conn_cancel = cancel.clone();
                conns.spawn(async move {
                    let _ = handle_conn(stream, backend, hooks, state, conn_cancel).await;
                });
            }
        }
    }
    // Drain the in-flight handlers (each exits on `cancel` between commands). If the caller's bound
    // elapses and aborts this future, `conns` drops here and aborts any handler still running.
    while conns.join_next().await.is_some() {}
}

async fn handle_conn<B: RespBackend, H: RespHooks>(
    stream: TcpStream,
    backend: Arc<B>,
    hooks: Arc<H>,
    state: Arc<ServerState>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    loop {
        // Graceful drain: on shutdown, stop waiting for the NEXT command and close the connection. A
        // command already being read/dispatched below is allowed to finish (we only branch here while
        // idle between commands), so no in-flight request is cut off mid-reply.
        let args = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            r = read_command(&mut reader) => match r? {
                Some(args) => args,
                None => break,
            },
        };
        if args.is_empty() {
            continue;
        }
        let reply = dispatch(&backend, &hooks, &state, &args).await;
        let mut buf = Vec::new();
        encode(&reply, &mut buf);
        wr.write_all(&buf).await?;
        wr.flush().await?;
    }
    Ok(())
}

fn arg_eq(a: &[u8], s: &str) -> bool {
    a.eq_ignore_ascii_case(s.as_bytes())
}

async fn dispatch<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    if let Some(key) = routing_key_for(&name, args) {
        let class = drain_class(&name, args);
        let is_new_claim = is_new_claim_on_drain(class, false);
        match hooks
            .route_command(&name, args, key, state.now(), is_new_claim)
            .await
        {
            Ok(RouteDecision::Serve) => {}
            Ok(RouteDecision::Moved { slot, endpoint }) => {
                return Resp::Error(format!("MOVED {slot} {endpoint}"));
            }
            Ok(RouteDecision::NoPerm) => return Resp::Error("NOPERM unauthorized".into()),
            Ok(RouteDecision::Unavailable) => {
                return Resp::Error("ERR pqueue unavailable".into());
            }
            Err(e) => return err_reply(&e),
        }
    }
    match name.as_str() {
        "PING" => Resp::Simple("PONG".into()),
        // DEFERRED (Phase 4): handshake + group commands are benign no-ops. No group state is
        // created and HELLO returns +OK (not a RESP3 map). Enough for a stock client to connect.
        "CLIENT" | "HELLO" => Resp::Simple("OK".into()),
        "COMMAND" => Resp::Array(vec![]),
        "CLUSTER" => cluster_cmd(state, args),
        "XGROUP" => Resp::Simple("OK".into()),
        "XADD" => xadd(backend, hooks, state, args).await,
        "XREADGROUP" => xreadgroup(backend, hooks, state, args).await,
        "XACK" => xack(backend, hooks, state, args).await,
        "XPENDING" => xpending(backend, state, args).await,
        "XAUTOCLAIM" => xautoclaim(backend, hooks, state, args).await,
        "XCLAIM" => xclaim(backend, hooks, state, args).await,
        "XLEN" => xlen(backend, args).await,
        "XDEL" => xdel(backend, hooks, state, args).await,
        "XINFO" => xinfo(backend, args).await,
        "PQ.MGET" => pq_mget(backend, args).await,
        "PQ.HGETALL" => pq_hgetall(backend, args).await,
        "PQ.HMGET" => pq_hmget(backend, args).await,
        other => Resp::Error(format!("ERR unknown command '{other}'")),
    }
}

fn routing_key_for<'a>(name: &str, args: &'a [Vec<u8>]) -> Option<&'a [u8]> {
    match name {
        "XREADGROUP" => {
            let streams_at = args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))?;
            args.get(streams_at + 1).map(Vec::as_slice)
        }
        "XINFO" => args.get(2).map(Vec::as_slice),
        "XADD" | "XACK" | "XPENDING" | "XAUTOCLAIM" | "XCLAIM" | "XLEN" | "XDEL" | "PQ.MGET"
        | "PQ.HGETALL" | "PQ.HMGET" => args.get(1).map(Vec::as_slice),
        _ => None,
    }
}

/// `CLUSTER <subcommand>` — the bootstrap surface (TD-006 §1A) so a stock cluster-aware client discovers
/// the topology and computes slots identically. SINGLE-NODE today: this node advertises owning all 16384
/// slots; `KEYSLOT` computes the Redis slot of a key. The multi-node slot→owner view + per-queue `-MOVED`
/// redirect to the recorded `active_owner` are BQ-31 / the server-runtime follow-up.
fn cluster_cmd(state: &Arc<ServerState>, args: &[Vec<u8>]) -> Resp {
    let Some(sub) = args.get(1) else {
        return Resp::Error("ERR wrong number of arguments for 'cluster'".into());
    };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "SLOTS" => cluster::cluster_slots_single_node(&state.node),
        "SHARDS" => cluster::cluster_shards_single_node(&state.node),
        "NODES" => cluster::cluster_nodes_single_node(&state.node),
        "INFO" => cluster::cluster_info_single_node(),
        "MYID" => Resp::Bulk(state.node.id.clone().into_bytes()),
        "KEYSLOT" => match args.get(2) {
            Some(key) => Resp::Int(cluster::hash_slot(key) as i64),
            None => Resp::Error("ERR wrong number of arguments for 'cluster|keyslot'".into()),
        },
        other => Resp::Error(format!("ERR unknown CLUSTER subcommand '{other}'")),
    }
}

/// `XADD key <*|id> field value [field value ...]` - insert one item (container-object fields).
async fn xadd<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 5 {
        return Resp::Error("ERR wrong number of arguments for 'xadd'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let now = state.now();
    let expected_epoch = match hooks.expected_epoch_for_write(&shard, now, false).await {
        Ok(epoch) => epoch,
        Err(e) => return err_reply(&e),
    };
    if !(args.len() - 3).is_multiple_of(2) {
        return Resp::Error("ERR wrong number of field/value arguments for 'xadd'".into());
    }
    // Reserved container fields (TD-006 section 2). Field/value pairs start at index 3.
    let mut priority: Option<PriorityValue> = None;
    let mut client_item_key: Option<String> = None;
    let mut group_key: Option<GroupKey> = None;
    let mut not_before: Option<UtcTimestamp> = None;
    let mut payload: Option<bytes::Bytes> = None;
    let mut metadata = Metadata::default();
    let mut fields: BTreeMap<String, bytes::Bytes> = BTreeMap::new();
    for pair in args[3..].chunks_exact(2) {
        if arg_eq(&pair[0], "priority")
            && let Ok(s) = std::str::from_utf8(&pair[1])
            && let Ok(n) = s.parse::<i64>()
        {
            priority = Some(PriorityValue::Int64(n));
        } else if arg_eq(&pair[0], "client_item_key")
            && let Ok(s) = std::str::from_utf8(&pair[1])
        {
            client_item_key = Some(s.to_string());
        } else if arg_eq(&pair[0], "group_key")
            && let Ok(s) = std::str::from_utf8(&pair[1])
        {
            let Ok(group) = GroupKey::new(s) else {
                return Resp::Error("ERR invalid group_key".into());
            };
            group_key = Some(group);
        } else if arg_eq(&pair[0], "not_before")
            && let Ok(s) = std::str::from_utf8(&pair[1])
        {
            let Ok(ms) = s.parse::<i64>() else {
                return Resp::Error("ERR invalid not_before".into());
            };
            let Ok(ts) = ms_ts(ms) else {
                return Resp::Error("ERR invalid not_before".into());
            };
            not_before = Some(ts);
        } else if arg_eq(&pair[0], "payload") {
            payload = Some(bytes::Bytes::copy_from_slice(&pair[1]));
        } else if arg_eq(&pair[0], "metadata") {
            let Ok(raw) = std::str::from_utf8(&pair[1]) else {
                return Resp::Error("ERR metadata must be utf-8 JSON".into());
            };
            let entries = match serde_json::from_str::<BTreeMap<String, MetadataValue>>(raw) {
                Ok(entries) => entries,
                Err(_) => return Resp::Error("ERR invalid metadata".into()),
            };
            metadata = Metadata::from_entries(entries);
        } else {
            let Ok(field) = std::str::from_utf8(&pair[0]) else {
                return Resp::Error("ERR field names must be utf-8".into());
            };
            if pqueue_engine::is_api001_reserved_write_field(field) {
                return Resp::Error(format!("ERR field '{field}' is reserved"));
            }
            fields.insert(field.to_string(), bytes::Bytes::copy_from_slice(&pair[1]));
        }
    }
    // The BACKEND assigns the item id in both paths (restart-safe, collision-free across servers — the
    // RESP front never mints ids itself). `client_item_key` is the upsert key (TD-006 §2, Invariant 2):
    // with a key, a second XADD REPLACES the pending item (via UpsertPort); absent a key, each XADD is a
    // unique append (via PushPort). Remaining reserved fields (group_key/not_before/payload) DEFERRED.
    match client_item_key {
        Some(k) => {
            let key = match ClientItemKey::new(k) {
                Ok(k) => k,
                Err(_) => return Resp::Error("ERR invalid client_item_key".into()),
            };
            match backend
                .replace_if_pending(
                    &shard,
                    &key,
                    priority,
                    group_key,
                    not_before,
                    payload,
                    fields,
                    metadata,
                    None,
                    now,
                    expected_epoch,
                )
                .await
            {
                Ok(UpsertOutcome::Inserted { item_id })
                | Ok(UpsertOutcome::Replaced {
                    new_item_id: item_id,
                    ..
                }) => Resp::Bulk(item_id.to_string().into_bytes()),
                Err(e) => err_reply(&e),
            }
        }
        None => {
            let spec = PushSpec {
                client_item_key: None,
                priority,
                not_before,
                group_key,
                payload,
                fields,
                metadata,
                cohort_size: None, // RESP XADD has no cohort declaration (library-only, plan §3)
                gate_keys: Vec::new(), // RESP XADD carries no gate keys (library-only)
                entity: None, // RESP XADD is schema-less (typed entities are library-only, ADR-011)
            };
            match backend.push(&shard, vec![spec], now, expected_epoch).await {
                Ok(ids) => Resp::Bulk(ids[0].to_string().into_bytes()),
                Err(e) => err_reply(&e),
            }
        }
    }
}

/// `XREADGROUP GROUP g consumer [COUNT n] [BLOCK ms] STREAMS key id` - priority claim for `id == >`.
async fn xreadgroup<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    // Locate COUNT, STREAMS, and the trailing key + id.
    let mut count = 100usize;
    let mut streams_at = None;
    let mut i = 1;
    while i < args.len() {
        if arg_eq(&args[i], "COUNT") && i + 1 < args.len() {
            if let Ok(s) = std::str::from_utf8(&args[i + 1]) {
                count = s.parse().unwrap_or(100);
            }
            i += 2;
        } else if arg_eq(&args[i], "STREAMS") {
            streams_at = Some(i + 1);
            break;
        } else {
            i += 1;
        }
    }
    let Some(s) = streams_at else {
        return Resp::Error("ERR 'xreadgroup' missing STREAMS".into());
    };
    // STREAMS key id  (single stream)
    if s + 1 >= args.len() {
        return Resp::Error("ERR 'xreadgroup' missing key/id".into());
    }
    let key = &args[s];
    let id = &args[s + 1];
    if id != b">" {
        // History/pending reads are library-only in this minimal front (TD-006 section 3).
        return Resp::Error("ERR only '>' is supported".into());
    }
    let shard = match parse_shard(key) {
        Ok(sh) => sh,
        Err(e) => return err_reply(&e),
    };
    // Lease TTL = the queue's `max_lease_duration_ms` from `now` — leases actually expire, so a crashed
    // worker's items are reclaimed by the ReclaimDriver / XAUTOCLAIM (TD-006 §3).
    let now = state.now();
    let lease_ms = match backend.queue_definition(&shard.clone()).await {
        Ok(def) => def.max_lease_duration_ms,
        Err(e) => return err_reply(&e),
    };
    let lease = state.next();
    let expected_epoch = match hooks.expected_epoch_for_write(&shard, now, true).await {
        Ok(epoch) => epoch,
        Err(e) => return err_reply(&e),
    };
    let req = ClaimRequest {
        eligibility_time: None,
        shard,
        worker_id: WorkerId::new("resp").expect("w"),
        max_items: count,
        lease_token: LeaseToken::new(format!("L{lease}")).expect("lease"),
        lease_expires_at: add_millis(now, lease_ms),
        now,
        // RESP XREADGROUP is an item-level claim; group/cohort compatibility is library-only (plan §3).
        compatibility: pqueue_engine::ClaimCompatibility::default(),
        expected_epoch,
    };
    match backend.claim(req).await {
        Ok(claimed) if claimed.items.is_empty() => Resp::NullArray, // Redis returns nil when none
        Ok(claimed) => {
            let entries: Vec<Resp> = claimed.items.iter().map(claimed_to_entry).collect();
            // [[ key, [ entries... ] ]]
            Resp::Array(vec![Resp::Array(vec![
                Resp::Bulk(key.to_vec()),
                Resp::Array(entries),
            ])])
        }
        Err(e) => err_reply(&e),
    }
}

/// Render a claimed item as a Streams entry `[id, [field, value, ...]]`.
fn claimed_to_entry(item: &ClaimedItem) -> Resp {
    let mut fields = base_fields(
        &item.item_id.to_string(),
        item.client_item_key.as_str(),
        item.item_version,
        None,
        &item.priority,
        item.attempt_count,
        item.payload.as_ref(),
    );
    if let Some(lease_token) = &item.lease_token {
        fields.push(Resp::Bulk(b"lease_token".to_vec()));
        fields.push(Resp::Bulk(lease_token.to_string().into_bytes()));
    }
    fields.push(Resp::Bulk(b"lease_expires_at".to_vec()));
    fields.push(Resp::Bulk(
        ts_ms(item.lease_expires_at).to_string().into_bytes(),
    ));
    if let Some(not_before) = item.not_before {
        fields.push(Resp::Bulk(b"not_before".to_vec()));
        fields.push(Resp::Bulk(ts_ms(not_before).to_string().into_bytes()));
    }
    if let Some(group_key) = &item.group_key {
        fields.push(Resp::Bulk(b"group_key".to_vec()));
        fields.push(Resp::Bulk(group_key.as_str().as_bytes().to_vec()));
    }
    if !item.metadata.is_empty() {
        fields.push(Resp::Bulk(b"metadata".to_vec()));
        fields.push(Resp::Bulk(
            serde_json::to_vec(&item.metadata.clone().into_inner())
                .expect("metadata value serializes"),
        ));
    }
    if !item.gate_keys.is_empty() {
        fields.push(Resp::Bulk(b"gate_keys".to_vec()));
        fields.push(Resp::Bulk(
            serde_json::to_vec(&item.gate_keys).expect("gate keys serialize"),
        ));
    }
    append_user_fields(&mut fields, &item.fields);
    Resp::Array(vec![
        Resp::Bulk(item.item_id.to_string().into_bytes()),
        Resp::Array(fields),
    ])
}

fn base_fields(
    item_id: &str,
    client_item_key: &str,
    item_version: u64,
    lifecycle_state: Option<&str>,
    priority: &Option<PriorityValue>,
    attempt_count: u32,
    payload: Option<&bytes::Bytes>,
) -> Vec<Resp> {
    let mut fields = vec![
        Resp::Bulk(b"item_id".to_vec()),
        Resp::Bulk(item_id.as_bytes().to_vec()),
        Resp::Bulk(b"client_item_key".to_vec()),
        Resp::Bulk(client_item_key.as_bytes().to_vec()),
        Resp::Bulk(b"item_version".to_vec()),
        Resp::Bulk(item_version.to_string().into_bytes()),
    ];
    if let Some(state) = lifecycle_state {
        fields.push(Resp::Bulk(b"lifecycle_state".to_vec()));
        fields.push(Resp::Bulk(state.as_bytes().to_vec()));
    }
    if let Some(PriorityValue::Int64(n)) = priority {
        fields.push(Resp::Bulk(b"priority".to_vec()));
        fields.push(Resp::Bulk(n.to_string().into_bytes()));
    }
    fields.push(Resp::Bulk(b"attempt_count".to_vec()));
    fields.push(Resp::Bulk(attempt_count.to_string().into_bytes()));
    if let Some(payload) = payload {
        fields.push(Resp::Bulk(b"payload".to_vec()));
        fields.push(Resp::Bulk(payload.to_vec()));
    }
    fields
}

fn append_user_fields(fields: &mut Vec<Resp>, user_fields: &BTreeMap<String, bytes::Bytes>) {
    for (name, value) in user_fields {
        fields.push(Resp::Bulk(name.as_bytes().to_vec()));
        fields.push(Resp::Bulk(value.to_vec()));
    }
}

fn lifecycle_name(state: pqueue_core::ItemState) -> &'static str {
    match state {
        pqueue_core::ItemState::Pending => "Pending",
        pqueue_core::ItemState::Leased => "Leased",
        pqueue_core::ItemState::Complete => "Complete",
        pqueue_core::ItemState::Failed => "Failed",
    }
}

fn live_to_entry(item: &LiveItemView) -> Resp {
    let mut fields = base_fields(
        &item.item_id.to_string(),
        item.client_item_key.as_str(),
        item.item_version,
        Some(lifecycle_name(item.lifecycle_state)),
        &item.priority,
        item.attempt_count,
        item.payload.as_ref(),
    );
    if let Some(group) = &item.group_key {
        fields.push(Resp::Bulk(b"group_key".to_vec()));
        fields.push(Resp::Bulk(group.to_string().into_bytes()));
    }
    if let Some(not_before) = item.not_before {
        fields.push(Resp::Bulk(b"not_before".to_vec()));
        fields.push(Resp::Bulk(ts_ms(not_before).to_string().into_bytes()));
    }
    append_user_fields(&mut fields, &item.fields);
    Resp::Array(vec![
        Resp::Bulk(item.item_id.to_string().into_bytes()),
        Resp::Array(fields),
    ])
}

fn live_field_value(item: &LiveItemView, field: &[u8]) -> Option<Vec<u8>> {
    if arg_eq(field, "item_id") {
        Some(item.item_id.to_string().into_bytes())
    } else if arg_eq(field, "client_item_key") {
        Some(item.client_item_key.to_string().into_bytes())
    } else if arg_eq(field, "item_version") {
        Some(item.item_version.to_string().into_bytes())
    } else if arg_eq(field, "lifecycle_state") {
        Some(lifecycle_name(item.lifecycle_state).as_bytes().to_vec())
    } else if arg_eq(field, "priority") {
        match &item.priority {
            Some(PriorityValue::Int64(n)) => Some(n.to_string().into_bytes()),
            _ => None,
        }
    } else if arg_eq(field, "attempt_count") {
        Some(item.attempt_count.to_string().into_bytes())
    } else if arg_eq(field, "payload") {
        item.payload.as_ref().map(|p| p.to_vec())
    } else if arg_eq(field, "group_key") {
        item.group_key.as_ref().map(|g| g.to_string().into_bytes())
    } else if arg_eq(field, "not_before") {
        item.not_before.map(|ts| ts_ms(ts).to_string().into_bytes())
    } else {
        let name = std::str::from_utf8(field).ok()?;
        item.fields.get(name).map(|v| v.to_vec())
    }
}

async fn pq_mget<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'pq.mget'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let mut keys = Vec::with_capacity(args.len() - 2);
    for raw in &args[2..] {
        let Ok(s) = std::str::from_utf8(raw) else {
            return Resp::Error("ERR client_item_key must be utf-8".into());
        };
        let Ok(key) = ClientItemKey::new(s) else {
            return Resp::Error("ERR invalid client_item_key".into());
        };
        keys.push(key);
    }
    match backend.live_items(&shard, &keys).await {
        Ok(items) => Resp::Array(
            items
                .iter()
                .map(|item| item.as_ref().map(live_to_entry).unwrap_or(Resp::NullArray))
                .collect(),
        ),
        Err(e) => err_reply(&e),
    }
}

async fn pq_hgetall<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'pq.hgetall'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let Ok(s) = std::str::from_utf8(&args[2]) else {
        return Resp::Error("ERR client_item_key must be utf-8".into());
    };
    let key = match ClientItemKey::new(s) {
        Ok(k) => k,
        Err(_) => return Resp::Error("ERR invalid client_item_key".into()),
    };
    match backend.live_items(&shard, &[key]).await {
        Ok(mut items) => match items.pop().flatten() {
            Some(item) => match live_to_entry(&item) {
                Resp::Array(mut entry) => match entry.pop() {
                    Some(Resp::Array(fields)) => Resp::Array(fields),
                    _ => Resp::Array(vec![]),
                },
                _ => Resp::Array(vec![]),
            },
            None => Resp::Array(vec![]),
        },
        Err(e) => err_reply(&e),
    }
}

async fn pq_hmget<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'pq.hmget'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let Ok(s) = std::str::from_utf8(&args[2]) else {
        return Resp::Error("ERR client_item_key must be utf-8".into());
    };
    let key = match ClientItemKey::new(s) {
        Ok(k) => k,
        Err(_) => return Resp::Error("ERR invalid client_item_key".into()),
    };
    match backend.live_items(&shard, &[key]).await {
        Ok(mut items) => {
            let item = items.pop().flatten();
            Resp::Array(
                args[3..]
                    .iter()
                    .map(|field| {
                        item.as_ref()
                            .and_then(|item| live_field_value(item, field))
                            .map(Resp::Bulk)
                            .unwrap_or(Resp::NullBulk)
                    })
                    .collect(),
            )
        }
        Err(e) => err_reply(&e),
    }
}

/// `XACK key group id [id ...]` - finalize-complete the acked entries.
///
/// The batch is all-or-nothing (FinalizePort pre-validates): a fenced lease → `-ERR pqueue stale_lease`,
/// a superseded id → `-ERR pqueue superseded`, a non-leased id → `-ERR pqueue invalid`, NOTHING is
/// committed, and the reply is the acked count only on full success. (Per-id partial results +
/// lease-token/PEL ownership are a later refinement, TD-006 §3.)
async fn xack<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'xack'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let ids: Vec<ItemId> = args[3..]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .filter_map(|s| ItemId::new(s).ok())
        .collect();
    let outcomes: Vec<FinalizeOutcome> = ids
        .iter()
        .map(|id| FinalizeOutcome::new(*id, FinalizeKind::Complete))
        .collect();
    let now = state.now();
    let expected_epoch = match hooks.expected_epoch_for_write(&shard, now, false).await {
        Ok(epoch) => epoch,
        Err(e) => return err_reply(&e),
    };
    match backend
        .finalize(&shard, outcomes, now, expected_epoch)
        .await
    {
        Ok(()) => Resp::Int(ids.len() as i64),
        Err(e) => err_reply(&e),
    }
}

/// Numeric order key for a server-assigned item id. An [`ItemId`](pqueue_core::ItemId) is a single packed
/// `u64` rendered as decimal, and its numeric value IS stream/insertion order by construction (epoch high,
/// counter low — ADR-009). So the order key is just the parsed value. The `"0-0"` cursor sentinel (and any
/// non-numeric cursor) keys as `0`, sorting at/before the first real id so a `start = "0-0"` scan includes
/// the whole PEL.
fn id_order(id: &str) -> u64 {
    id.parse::<u64>().unwrap_or(0)
}

/// `XPENDING key group [start end count [consumer]]` — the in-flight (leased, not-yet-acked) items.
///
/// Summary form (`XPENDING key group`): `[count, min-id, max-id, [[consumer, count]]]`, where the
/// `consumer` axis is the **lease token** (pqueue's closest analog of a Redis consumer — who holds the
/// lease). Extended form (`XPENDING key group start end count`): one `[id, consumer(=lease token),
/// idle-ms, delivery-count]` per leased item, capped at the requested `count`.
///
/// NOTE (TD-006 §6.1 divergence): delivery is priority-ordered, so the id `min`/`max` bounds are NOT a
/// meaningful claimable range — they reflect insertion order only. `idle-ms` is the wall-clock time since
/// the item was last delivered (= `now - (lease_expires_at - max_lease_duration_ms)`).
async fn xpending<B: RespBackend>(
    backend: &Arc<B>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'xpending'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let leases = match backend.pending(&shard).await {
        Ok(l) => l,
        Err(e) => return err_reply(&e),
    };
    let extended = args.len() > 3; // start/end/count present → per-entry form
    if !extended {
        // Summary form. Empty → `[0, nil, nil, nil]` (Redis convention).
        if leases.is_empty() {
            return Resp::Array(vec![
                Resp::Int(0),
                Resp::NullArray,
                Resp::NullArray,
                Resp::NullArray,
            ]);
        }
        let mut ids: Vec<ItemId> = leases.iter().map(|l| l.item_id).collect();
        ids.sort_by_key(|id| id.as_u64());
        // Aggregate the per-consumer (lease-token) counts.
        let mut by_consumer: std::collections::BTreeMap<&str, usize> =
            std::collections::BTreeMap::new();
        for l in &leases {
            *by_consumer.entry(l.lease_token.as_str()).or_default() += 1;
        }
        let consumers: Vec<Resp> = by_consumer
            .into_iter()
            .map(|(token, n)| {
                Resp::Array(vec![
                    Resp::Bulk(token.as_bytes().to_vec()),
                    Resp::Bulk(n.to_string().into_bytes()),
                ])
            })
            .collect();
        return Resp::Array(vec![
            Resp::Int(leases.len() as i64),
            Resp::Bulk(ids.first().unwrap().to_string().into_bytes()),
            Resp::Bulk(ids.last().unwrap().to_string().into_bytes()),
            Resp::Array(consumers),
        ]);
    }
    // Extended form: `XPENDING key group start end count [consumer]` — `count` is args[5].
    let limit = args
        .get(5)
        .and_then(|a| std::str::from_utf8(a).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let now_ms = ts_ms(state.now());
    let lease_ms = backend
        .queue_definition(&shard.clone())
        .await
        .map(|d| d.max_lease_duration_ms as i64)
        .unwrap_or(0);
    let mut entries: Vec<&LeaseView> = leases.iter().collect();
    entries.sort_by_key(|lv| lv.item_id.as_u64());
    let out: Vec<Resp> = entries
        .into_iter()
        .take(limit)
        .map(|lv| {
            // idle = now - claimed_at, claimed_at = lease_expires_at - lease_ms.
            let idle = ((now_ms - ts_ms(lv.lease_expires_at)) + lease_ms).max(0);
            Resp::Array(vec![
                Resp::Bulk(lv.item_id.to_string().into_bytes()),
                Resp::Bulk(lv.lease_token.to_string().into_bytes()),
                Resp::Int(idle),
                Resp::Int(lv.attempt_count as i64),
            ])
        })
        .collect();
    Resp::Array(out)
}

/// `XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]` — page through the PEL (the
/// in-flight/leased entries), reclaiming the **idle** (lease-expired) ones to `consumer`. Reply:
/// `[cursor, [entries...], [deleted-ids]]`.
///
/// **Paginated cursor (TD-006 §3):** the PEL is scanned in a stable id order from `start` (`0-0` = the
/// beginning); a `COUNT`-sized window is examined, and the cursor returned is the id of the next
/// unscanned entry, or `0-0` once the window reaches the end of the PEL — so a client loops `0-0`→…→`0-0`
/// to cover the whole PEL. Reclaim is per-page (no global sweep): an expired entry in the window is
/// transferred to `consumer` via [`ReassignLeasePort`] (a re-delivery), keeping its id so the cursor is
/// stable across the reclaim.
///
/// pqueue-flavored divergences:
/// - **`min-idle-time` is ignored** — pqueue reclaims strictly by **lease expiry** (the engine's timed
///   transition: a lease is held THROUGH `lease_expires_at`, idle once `now > lease_expires_at`), not a
///   caller-supplied idle floor.
/// - **attempt accounting** (TD-006:129): `attempt_count` = number of deliveries. Reclaiming an idle
///   entry to `consumer` is one re-delivery, so it charges exactly one attempt (the reassign), never more.
/// - **direct transfer, not re-queue** — unlike the background `ReclaimDriver` (`tick` → `LeaseExpired`,
///   which returns an expired lease to *pending* for priority re-dispatch, no charge), XAUTOCLAIM hands
///   the idle entry straight to the calling `consumer` (the Redis PEL-ownership-transfer semantic). So the
///   two reclaim paths leave different post-states for the same expired lease; this is intentional.
/// - **all-or-nothing page** — the window's idle entries are reassigned in ONE batch; if a racing
///   ack/fence/purge invalidates any of them between the snapshot and the reassign, the whole page errors
///   and reclaims nothing (a safe failure — nothing wrong is committed; the client simply retries). Redis
///   is per-entry best-effort here; the third reply element (deleted ids) is therefore always empty.
async fn xautoclaim<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 6 {
        return Resp::Error("ERR wrong number of arguments for 'xautoclaim'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let consumer = String::from_utf8_lossy(&args[3]).to_string();
    let consumer_token = match LeaseToken::new(consumer) {
        Ok(t) => t,
        Err(_) => return Resp::Error("ERR pqueue invalid".into()),
    };
    let start = String::from_utf8_lossy(&args[5]).to_string();
    let mut count = 100usize;
    let mut justid = false;
    let mut i = 6;
    while i < args.len() {
        if arg_eq(&args[i], "COUNT") && i + 1 < args.len() {
            if let Ok(s) = std::str::from_utf8(&args[i + 1]) {
                count = s.parse().unwrap_or(100);
            }
            i += 2;
        } else {
            if arg_eq(&args[i], "JUSTID") {
                justid = true;
            }
            i += 1;
        }
    }
    if count == 0 {
        // Redis rejects COUNT <= 0; we must too — a 0-window never advances the cursor off the first
        // entry, so a compliant `0-0`→cursor→`0-0` client loop would spin forever.
        return Resp::Error("ERR COUNT must be > 0".into());
    }
    let now = state.now();
    let lease_ms = match backend.queue_definition(&shard.clone()).await {
        Ok(def) => def.max_lease_duration_ms,
        Err(e) => return err_reply(&e),
    };

    // PEL snapshot in a stable id order; page from the `start` cursor.
    let mut pel = match backend.pending(&shard).await {
        Ok(p) => p,
        Err(e) => return err_reply(&e),
    };
    pel.sort_by_key(|a| a.item_id.as_u64());
    let start_key = id_order(&start);
    let from: Vec<&LeaseView> = pel
        .iter()
        .filter(|lv| lv.item_id.as_u64() >= start_key)
        .collect();

    // Examine a COUNT-sized window; the idle (lease-expired) entries in it are reclaimed to `consumer`.
    let expired_ids: Vec<ItemId> = from
        .iter()
        .take(count)
        .filter(|lv| lv.lease_expires_at < now)
        .map(|lv| lv.item_id)
        .collect();
    let reassign_epoch = if expired_ids.is_empty() {
        None
    } else {
        match hooks.expected_epoch_for_write(&shard, now, true).await {
            Ok(epoch) => epoch,
            Err(e) => return err_reply(&e),
        }
    };
    if !expired_ids.is_empty()
        && let Err(e) = backend
            .reassign(
                &shard,
                expired_ids.clone(),
                consumer_token,
                add_millis(now, lease_ms),
                now,
                reassign_epoch,
            )
            .await
    {
        return err_reply(&e);
    }

    // Cursor: the entry after the scanned window, or `0-0` once the window covers the PEL tail.
    let next_cursor = if from.len() > count {
        from[count].item_id.to_string().into_bytes()
    } else {
        b"0-0".to_vec()
    };

    let entries: Vec<Resp> = if justid {
        expired_ids
            .iter()
            .map(|id| Resp::Bulk(id.to_string().into_bytes()))
            .collect()
    } else {
        match backend.claimed_view(&shard, &expired_ids).await {
            Ok(items) => items.iter().map(claimed_to_entry).collect(),
            Err(e) => return err_reply(&e),
        }
    };
    Resp::Array(vec![
        Resp::Bulk(next_cursor),
        Resp::Array(entries),
        Resp::Array(vec![]),
    ])
}

/// `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE]
/// [JUSTID] [LASTID id]` — transfer ownership of specific in-flight (leased) entries to `consumer`.
///
/// pqueue semantics (TD-006 §3): the **consumer name IS the lease token** (the identity `XPENDING`
/// reports). So per id:
/// - `consumer` == the id's CURRENT lease token → **renew** (extend the lease, [`RenewLeasePort`], NO
///   attempt charge — §3 flavor #7, a worker re-affirming its own claim).
/// - `consumer` != the current token (or a different worker) → **reassign** ([`ReassignLeasePort`]):
///   swap the token to `consumer` and charge exactly one delivery (TD-006:129).
///
/// Divergences: **`min-idle-time` is ignored** (pqueue gates by lease expiry, like `XAUTOCLAIM`); the
/// `IDLE`/`TIME`/`RETRYCOUNT`/`FORCE`/`LASTID` options are accepted and ignored (the lease deadline is
/// reset to the queue's `max_lease_duration`). Repeated ids are de-duplicated (Redis treats them
/// idempotently; without this a duplicated id would charge the delivery count twice). A mixed batch
/// (some ids self-owned, some not) issues a renew AND a reassign — these are two separate commits and
/// NOT atomic with each other: each is individually all-or-nothing + pre-validated, but if the first
/// disposition commits and the second then rejects (e.g. an id was fenced/reclaimed between the snapshot
/// and the commit), the client gets the error yet the first disposition's effects are already durable
/// (PARTIAL EFFECTS POSSIBLE on a mixed-batch error). Reply: the claimed entries
/// (`[id, [field value …]]`), or just the ids with `JUSTID`.
async fn xclaim<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 6 {
        return Resp::Error("ERR wrong number of arguments for 'xclaim'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let is_opt = |a: &[u8]| {
        ["IDLE", "TIME", "RETRYCOUNT", "FORCE", "JUSTID", "LASTID"]
            .iter()
            .any(|k| arg_eq(a, k))
    };
    // Ids are contiguous from arg 5 until the first trailing option keyword (XCLAIM grammar).
    let mut ids: Vec<ItemId> = Vec::new();
    let mut i = 5;
    while i < args.len() && !is_opt(&args[i]) {
        match ItemId::new(String::from_utf8_lossy(&args[i])) {
            Ok(id) => ids.push(id),
            Err(_) => return Resp::Error("ERR pqueue invalid".into()),
        }
        i += 1;
    }
    if ids.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'xclaim'".into());
    }
    // De-duplicate (preserving order): a repeated id must transfer/renew once, not charge the delivery
    // count once per occurrence in the command's `item_ids` (the apply arm bumps per element).
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(*id));
    let justid = args[i..].iter().any(|a| arg_eq(a, "JUSTID"));

    let consumer = String::from_utf8_lossy(&args[3]).to_string();
    let consumer_token = match LeaseToken::new(consumer.clone()) {
        Ok(t) => t,
        Err(_) => return Resp::Error("ERR pqueue invalid".into()),
    };

    // Partition by CURRENT owner: ids already owned by `consumer` → renew; the rest → reassign. An id that
    // is not currently leased has no owner, so it falls to reassign and is rejected by reassign_validate.
    let leases = match backend.pending(&shard).await {
        Ok(l) => l,
        Err(e) => return err_reply(&e),
    };
    let mut renew_ids: Vec<ItemId> = Vec::new();
    let mut reassign_ids: Vec<ItemId> = Vec::new();
    for id in &ids {
        let current = leases
            .iter()
            .find(|lv| lv.item_id == *id)
            .map(|lv| lv.lease_token.as_str());
        if current == Some(consumer.as_str()) {
            renew_ids.push(*id);
        } else {
            reassign_ids.push(*id);
        }
    }

    let now = state.now();
    let lease_ms = match backend.queue_definition(&shard.clone()).await {
        Ok(def) => def.max_lease_duration_ms,
        Err(e) => return err_reply(&e),
    };
    let new_expiry = add_millis(now, lease_ms);

    let renew_epoch = if renew_ids.is_empty() {
        None
    } else {
        match hooks.expected_epoch_for_write(&shard, now, false).await {
            Ok(epoch) => epoch,
            Err(e) => return err_reply(&e),
        }
    };
    if !renew_ids.is_empty()
        && let Err(e) = backend
            .renew(&shard, renew_ids, new_expiry, now, renew_epoch)
            .await
    {
        return err_reply(&e);
    }
    let reassign_epoch = if reassign_ids.is_empty() {
        None
    } else {
        match hooks.expected_epoch_for_write(&shard, now, true).await {
            Ok(epoch) => epoch,
            Err(e) => return err_reply(&e),
        }
    };
    if !reassign_ids.is_empty()
        && let Err(e) = backend
            .reassign(
                &shard,
                reassign_ids,
                consumer_token,
                new_expiry,
                now,
                reassign_epoch,
            )
            .await
    {
        return err_reply(&e);
    }

    if justid {
        return Resp::Array(
            ids.iter()
                .map(|id| Resp::Bulk(id.to_string().into_bytes()))
                .collect(),
        );
    }
    match backend.claimed_view(&shard, &ids).await {
        Ok(items) => Resp::Array(items.iter().map(claimed_to_entry).collect()),
        Err(e) => err_reply(&e),
    }
}

/// `XLEN key` — the number of LIVE entries in the stream: pending + in-flight (leased). Like Redis,
/// terminal entries (complete/failed, the pqueue analog of acked) are NOT counted, and purged (`XDEL`)
/// entries are gone. A pqueue-flavored read over `metrics` (TD-006 §3).
async fn xlen<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'xlen'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    match backend.metrics(&shard.clone()).await {
        Ok(m) => Resp::Int((m.pending + m.leased) as i64),
        Err(e) => err_reply(&e),
    }
}

/// `XDEL key id [id ...]` — hard-delete the named entries via [`PurgePort`] (`force = true`, like Redis
/// which deletes regardless of PEL/lease state). Reply: the count actually removed (absent ids are
/// no-ops). Distinct from `XACK` (which completes a lease); `XDEL` removes the item outright.
async fn xdel<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'xdel'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let mut ids = Vec::with_capacity(args.len() - 2);
    for a in &args[2..] {
        match ItemId::new(String::from_utf8_lossy(a)) {
            Ok(id) => ids.push(id),
            Err(_) => return Resp::Error("ERR pqueue invalid".into()),
        }
    }
    let now = state.now();
    let expected_epoch = match hooks.expected_epoch_for_write(&shard, now, false).await {
        Ok(epoch) => epoch,
        Err(e) => return err_reply(&e),
    };
    match backend.purge(&shard, ids, true, now, expected_epoch).await {
        Ok(n) => Resp::Int(n as i64),
        Err(e) => err_reply(&e),
    }
}

/// `XINFO STREAM key` / `XINFO GROUPS key` — summary reads over `metrics`/`pending`. Only the `STREAM`
/// and `GROUPS` subcommands are offered (a documented divergence; `CONSUMERS`/`FULL` are owed). pqueue
/// flavor (TD-006 §3): there is no meaningful `last-delivered-id` / stream-id high-water (delivery is
/// priority-ordered + cursorless, not id-monotonic), so that field is reported as `0-0`.
async fn xinfo<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'xinfo'".into());
    }
    let shard = match parse_shard(&args[2]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let m = match backend.metrics(&shard.clone()).await {
        Ok(m) => m,
        Err(e) => return err_reply(&e),
    };
    let terminal_metrics = match backend.queue_definition(&shard).await {
        Ok(definition) => match backend
            .terminal_emission_metrics(
                &shard,
                UtcTimestamp::new(0, 0).expect("valid timestamp"),
                definition.emit_change_records,
                None,
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(e) => return err_reply(&e),
        },
        Err(e) => return err_reply(&e),
    };
    let live = (m.pending + m.leased) as i64;
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "STREAM" => Resp::Array(vec![
            Resp::Bulk(b"length".to_vec()),
            Resp::Int(live),
            Resp::Bulk(b"resident-terminal-count".to_vec()),
            Resp::Int(terminal_metrics.resident_terminal_count as i64),
            Resp::Bulk(b"groups".to_vec()),
            Resp::Int(1), // single implicit consumer group (pqueue has no named-group state)
            Resp::Bulk(b"last-generated-id".to_vec()),
            Resp::Bulk(b"0-0".to_vec()),
            Resp::Bulk(b"last-delivered-id".to_vec()),
            Resp::Bulk(b"0-0".to_vec()),
        ]),
        "GROUPS" => {
            // One implicit group; `pending` = in-flight (leased) count = the group's PEL size.
            let pending = match backend.pending(&shard).await {
                Ok(p) => p.len() as i64,
                Err(e) => return err_reply(&e),
            };
            Resp::Array(vec![Resp::Array(vec![
                Resp::Bulk(b"name".to_vec()),
                Resp::Bulk(b"default".to_vec()),
                Resp::Bulk(b"consumers".to_vec()),
                Resp::Int(0),
                Resp::Bulk(b"pending".to_vec()),
                Resp::Int(pending),
                Resp::Bulk(b"last-delivered-id".to_vec()),
                Resp::Bulk(b"0-0".to_vec()),
            ])])
        }
        other => Resp::Error(format!(
            "ERR unsupported XINFO subcommand '{other}' (only STREAM and GROUPS)"
        )),
    }
}

fn err_reply(e: &EngineError) -> Resp {
    // `-ERR pqueue …` tokens map straight through; the non-`-ERR` errors get their idiomatic Redis
    // reply (TD-006 §2/§7): Forbidden → `-NOPERM`, not-found → `-ERR no such queue`.
    if let Some(tok) = e.resp_token() {
        return Resp::Error(tok.trim_start_matches('-').to_string());
    }
    match e {
        EngineError::Forbidden(why) => Resp::Error(format!("NOPERM {why}")),
        EngineError::NotFound => Resp::Error("ERR no such queue".into()),
        // A client-caused incompatible re-create, not an internal fault (queue-create is library-only
        // over RESP today, so this is latent — but the token must be honest).
        EngineError::QueueDefinitionConflict => Resp::Error("ERR pqueue queue_conflict".into()),
        EngineError::Storage(_) | EngineError::DurableDataCorrupt { .. } => {
            Resp::Error("ERR pqueue internal".into())
        }
        // Every other variant carries a `-ERR pqueue …` token via `resp_token()` above; this arm is
        // unreachable, but stays total.
        _ => Resp::Error("ERR pqueue internal".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_text(e: &EngineError) -> String {
        match err_reply(e) {
            Resp::Error(s) => s,
            _ => panic!("expected error reply"),
        }
    }

    #[test]
    fn forbidden_maps_to_noperm_not_generic_err() {
        // TD-006 §2: cross-tenant / operator denial → -NOPERM (NOT a fake -ERR pqueue token).
        assert!(err_text(&EngineError::Forbidden("nope")).starts_with("NOPERM"));
        // `-ERR pqueue …` tokened errors pass through verbatim.
        assert_eq!(err_text(&EngineError::StaleLease), "ERR pqueue stale_lease");
        assert_eq!(err_text(&EngineError::Superseded), "ERR pqueue superseded");
        assert_eq!(err_text(&EngineError::NotFound), "ERR no such queue");
    }
}
