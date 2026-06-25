#![forbid(unsafe_code)]
//! # pqueue-resp
//!
//! Minimal RESP/Redis-Streams driving adapter over the engine ports (Phase 1d smoke front). It maps
//! the worker hot path - `XADD` -> insert/upsert, `XREADGROUP >` -> priority claim,
//! `XACK` -> finalize-complete - so an off-the-shelf Redis client can drive it.
//! The full command surface, auth, idempotency, and the proper engine orchestration
//! layer land in later phases (TD-006; plan section 3/4). Unsupported
//! commands return `-ERR`, never a silent stub.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{
    ClientItemKey, ItemId, LeaseToken, PriorityValue, QueueId, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimPort, ClaimRequest, ClaimedItem, Clock, ControlPlaneStore, EngineError,
    FinalizeKind, FinalizeOutcome, FinalizePort, LeaseView, ProjectionRead, PurgePort, PushPort,
    PushSpec, ReassignLeasePort, ReclaimDriver, RenewLeasePort, QueueKey, UpsertOutcome,
    UpsertPort,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

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

struct ServerState {
    ids: AtomicU64,
    clock: Arc<dyn Clock>,
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

enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
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
    let state = Arc::new(ServerState {
        ids: AtomicU64::new(1),
        clock,
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
                let backend = backend.clone();
                let state = state.clone();
                let conn_cancel = cancel.clone();
                conns.spawn(async move {
                    let _ = handle_conn(stream, backend, state, conn_cancel).await;
                });
            }
        }
    }
    // Drain the in-flight handlers (each exits on `cancel` between commands). If the caller's bound
    // elapses and aborts this future, `conns` drops here and aborts any handler still running.
    while conns.join_next().await.is_some() {}
}

async fn handle_conn<B: RespBackend>(
    stream: TcpStream,
    backend: Arc<B>,
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
        let reply = dispatch(&backend, &state, &args).await;
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

async fn dispatch<B: RespBackend>(
    backend: &Arc<B>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match name.as_str() {
        "PING" => Resp::Simple("PONG".into()),
        // DEFERRED (Phase 4): handshake + group commands are benign no-ops. No group state is
        // created and HELLO returns +OK (not a RESP3 map). Enough for a stock client to connect.
        "CLIENT" | "HELLO" => Resp::Simple("OK".into()),
        "COMMAND" => Resp::Array(vec![]),
        "XGROUP" => Resp::Simple("OK".into()),
        "XADD" => xadd(backend, state, args).await,
        "XREADGROUP" => xreadgroup(backend, state, args).await,
        "XACK" => xack(backend, state, args).await,
        "XPENDING" => xpending(backend, state, args).await,
        "XAUTOCLAIM" => xautoclaim(backend, state, args).await,
        "XCLAIM" => xclaim(backend, state, args).await,
        "XLEN" => xlen(backend, args).await,
        "XDEL" => xdel(backend, state, args).await,
        "XINFO" => xinfo(backend, args).await,
        other => Resp::Error(format!("ERR unknown command '{other}'")),
    }
}

/// `XADD key <*|id> field value [field value ...]` - insert one item (container-object fields).
async fn xadd<B: RespBackend>(
    backend: &Arc<B>,
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
    // Reserved container fields (TD-006 section 2). Field/value pairs start at index 3.
    let mut priority: Option<PriorityValue> = None;
    let mut client_item_key: Option<String> = None;
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
                .replace_if_pending(&shard, &key, priority, None, None, None, state.now())
                .await
            {
                Ok(UpsertOutcome::Inserted { item_id })
                | Ok(UpsertOutcome::Replaced {
                    new_item_id: item_id,
                    ..
                }) => Resp::Bulk(item_id.as_str().as_bytes().to_vec()),
                Err(e) => err_reply(&e),
            }
        }
        None => {
            let spec = PushSpec {
                client_item_key: None,
                priority,
                not_before: None,
                group_key: None,
                payload: None,
            };
            match backend.push(&shard, vec![spec], state.now()).await {
                Ok(ids) => Resp::Bulk(ids[0].as_str().as_bytes().to_vec()),
                Err(e) => err_reply(&e),
            }
        }
    }
}

/// `XREADGROUP GROUP g consumer [COUNT n] [BLOCK ms] STREAMS key id` - priority claim for `id == >`.
async fn xreadgroup<B: RespBackend>(
    backend: &Arc<B>,
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
    let req = ClaimRequest {
        shard,
        worker_id: WorkerId::new("resp").expect("w"),
        max_items: count,
        lease_token: LeaseToken::new(format!("L{lease}")).expect("lease"),
        lease_expires_at: add_millis(now, lease_ms),
        now,
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
    let mut fields: Vec<Resp> = Vec::new();
    fields.push(Resp::Bulk(b"client_item_key".to_vec()));
    fields.push(Resp::Bulk(
        item.client_item_key.as_str().as_bytes().to_vec(),
    ));
    if let Some(PriorityValue::Int64(n)) = &item.priority {
        fields.push(Resp::Bulk(b"priority".to_vec()));
        fields.push(Resp::Bulk(n.to_string().into_bytes()));
    }
    fields.push(Resp::Bulk(b"item_version".to_vec()));
    fields.push(Resp::Bulk(item.item_version.to_string().into_bytes()));
    fields.push(Resp::Bulk(b"attempt_count".to_vec()));
    fields.push(Resp::Bulk(item.attempt_count.to_string().into_bytes()));
    Resp::Array(vec![
        Resp::Bulk(item.item_id.as_str().as_bytes().to_vec()),
        Resp::Array(fields),
    ])
}

/// `XACK key group id [id ...]` - finalize-complete the acked entries.
///
/// The batch is all-or-nothing (FinalizePort pre-validates): a fenced lease → `-ERR pqueue stale_lease`,
/// a superseded id → `-ERR pqueue superseded`, a non-leased id → `-ERR pqueue invalid`, NOTHING is
/// committed, and the reply is the acked count only on full success. (Per-id partial results +
/// lease-token/PEL ownership are a later refinement, TD-006 §3.)
async fn xack<B: RespBackend>(
    backend: &Arc<B>,
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
        .map(|id| FinalizeOutcome {
            item_id: id.clone(),
            kind: FinalizeKind::Complete,
        })
        .collect();
    match backend.finalize(&shard, outcomes, state.now()).await {
        Ok(()) => Resp::Int(ids.len() as i64),
        Err(e) => err_reply(&e),
    }
}

/// Numeric order key for a backend-assigned item id `"{prefix}-{n}-{i}"` (e.g. `"mem-5-0"`): order by the
/// two numeric components `(n, i)` — `n` the command sequence (insertion order), `i` the index within a
/// batch — NOT a lexical compare (which would mis-order `"mem-10-0" < "mem-2-0"` past 10 items). The full
/// id is the final tie-break. The `"0-0"` cursor sentinel keys as `(0, 0, "0-0")`, sorting at/before the
/// first real id so a `start = "0-0"` scan includes the whole PEL.
fn id_order(id: &str) -> (u64, u64, &str) {
    let mut nums = id.split('-').filter_map(|p| p.parse::<u64>().ok());
    let n = nums.next().unwrap_or(u64::MAX);
    let i = nums.next().unwrap_or(0);
    (n, i, id)
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
        let mut ids: Vec<&str> = leases.iter().map(|l| l.item_id.as_str()).collect();
        ids.sort_by_key(|id| id_order(id));
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
            Resp::Bulk(ids.first().unwrap().as_bytes().to_vec()),
            Resp::Bulk(ids.last().unwrap().as_bytes().to_vec()),
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
    entries.sort_by_key(|lv| id_order(lv.item_id.as_str()));
    let out: Vec<Resp> = entries
        .into_iter()
        .take(limit)
        .map(|lv| {
            // idle = now - claimed_at, claimed_at = lease_expires_at - lease_ms.
            let idle = ((now_ms - ts_ms(lv.lease_expires_at)) + lease_ms).max(0);
            Resp::Array(vec![
                Resp::Bulk(lv.item_id.as_str().as_bytes().to_vec()),
                Resp::Bulk(lv.lease_token.as_str().as_bytes().to_vec()),
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
async fn xautoclaim<B: RespBackend>(
    backend: &Arc<B>,
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
    pel.sort_by(|a, b| id_order(a.item_id.as_str()).cmp(&id_order(b.item_id.as_str())));
    let start_key = id_order(&start);
    let from: Vec<&LeaseView> = pel
        .iter()
        .filter(|lv| id_order(lv.item_id.as_str()) >= start_key)
        .collect();

    // Examine a COUNT-sized window; the idle (lease-expired) entries in it are reclaimed to `consumer`.
    let expired_ids: Vec<ItemId> = from
        .iter()
        .take(count)
        .filter(|lv| lv.lease_expires_at < now)
        .map(|lv| lv.item_id.clone())
        .collect();
    if !expired_ids.is_empty()
        && let Err(e) = backend
            .reassign(&shard, expired_ids.clone(), consumer_token, add_millis(now, lease_ms), now)
            .await
    {
        return err_reply(&e);
    }

    // Cursor: the entry after the scanned window, or `0-0` once the window covers the PEL tail.
    let next_cursor = if from.len() > count {
        from[count].item_id.as_str().as_bytes().to_vec()
    } else {
        b"0-0".to_vec()
    };

    let entries: Vec<Resp> = if justid {
        expired_ids
            .iter()
            .map(|id| Resp::Bulk(id.as_str().as_bytes().to_vec()))
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
async fn xclaim<B: RespBackend>(
    backend: &Arc<B>,
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
        match ItemId::new(String::from_utf8_lossy(&args[i]).to_string()) {
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
    ids.retain(|id| seen.insert(id.clone()));
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
            renew_ids.push(id.clone());
        } else {
            reassign_ids.push(id.clone());
        }
    }

    let now = state.now();
    let lease_ms = match backend.queue_definition(&shard.clone()).await {
        Ok(def) => def.max_lease_duration_ms,
        Err(e) => return err_reply(&e),
    };
    let new_expiry = add_millis(now, lease_ms);

    if !renew_ids.is_empty()
        && let Err(e) = backend.renew(&shard, renew_ids, new_expiry, now).await
    {
        return err_reply(&e);
    }
    if !reassign_ids.is_empty()
        && let Err(e) = backend
            .reassign(&shard, reassign_ids, consumer_token, new_expiry, now)
            .await
    {
        return err_reply(&e);
    }

    if justid {
        return Resp::Array(
            ids.iter()
                .map(|id| Resp::Bulk(id.as_str().as_bytes().to_vec()))
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
async fn xdel<B: RespBackend>(backend: &Arc<B>, state: &Arc<ServerState>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'xdel'".into());
    }
    let shard = match parse_shard(&args[1]) {
        Ok(s) => s,
        Err(e) => return err_reply(&e),
    };
    let mut ids = Vec::with_capacity(args.len() - 2);
    for a in &args[2..] {
        match ItemId::new(String::from_utf8_lossy(a).to_string()) {
            Ok(id) => ids.push(id),
            Err(_) => return Resp::Error("ERR pqueue invalid".into()),
        }
    }
    match backend.purge(&shard, ids, true, state.now()).await {
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
    let live = (m.pending + m.leased) as i64;
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "STREAM" => Resp::Array(vec![
            Resp::Bulk(b"length".to_vec()),
            Resp::Int(live),
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
        EngineError::Storage(_) => Resp::Error("ERR pqueue internal".into()),
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
