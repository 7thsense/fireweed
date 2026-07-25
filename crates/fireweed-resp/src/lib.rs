#![forbid(unsafe_code)]
//! # fireweed-resp
//!
//! Minimal RESP/Redis-Streams driving adapter over the engine ports (Phase 1d smoke front). It maps
//! the worker hot path - `XADD` -> insert/upsert, `XREADGROUP >` -> priority claim,
//! `XACK` -> finalize-complete - so an off-the-shelf Redis client can drive it.
//! The full command surface, auth, idempotency, and the proper engine orchestration
//! layer land in later phases (TD-006; plan section 3/4). Unsupported
//! commands return `-ERR`, never a silent stub.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, MetadataValue, PriorityValue, QueueId,
    TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ClaimPort, ClaimRequest, ClaimedItem, Clock, ControlPlaneStore, EngineError, EngineResult,
    FinalizeKind, FinalizeOutcome, FinalizePort, LiveItemView, ProjectionRead, PurgePort, PushPort,
    PushSpec, QueueKey, ReassignLeasePort, ReclaimDriver, RenewLeasePort, UpsertOutcome,
    UpsertPort,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

static MAX_LIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(usize::MAX);
static MAX_RUNTIME_TASKS: AtomicUsize = AtomicUsize::new(usize::MAX);
static LIVE_RUNTIME_TASKS: AtomicUsize = AtomicUsize::new(0);
static MAX_OBSERVED_TASKS: AtomicUsize = AtomicUsize::new(0);
static TASK_SPAWN_LOCK: Mutex<()> = Mutex::new(());
static LIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static MAX_OBSERVED_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// Install a process-wide allocation bound for RESP handlers. Production leaves this unlimited; the
/// density harness enables a governed bound before the listener starts so no sampling interval can miss
/// an over-limit connection spike.
pub fn set_max_live_connections(limit: usize) {
    assert!(limit > 0, "RESP connection limit must be positive");
    MAX_LIVE_CONNECTIONS.store(limit, Ordering::SeqCst);
}

pub fn set_max_runtime_tasks(limit: usize) {
    assert!(limit > 0, "runtime task limit must be positive");
    MAX_RUNTIME_TASKS.store(limit, Ordering::SeqCst);
}

pub fn connection_resource_counts() -> (usize, usize, usize) {
    (
        LIVE_CONNECTIONS.load(Ordering::SeqCst),
        MAX_OBSERVED_CONNECTIONS.load(Ordering::SeqCst),
        MAX_LIVE_CONNECTIONS.load(Ordering::SeqCst),
    )
}

pub fn max_observed_runtime_tasks() -> usize {
    MAX_OBSERVED_TASKS.load(Ordering::SeqCst)
}

pub fn runtime_task_resource_counts() -> (usize, usize, usize) {
    (
        LIVE_RUNTIME_TASKS.load(Ordering::SeqCst),
        MAX_OBSERVED_TASKS.load(Ordering::SeqCst),
        MAX_RUNTIME_TASKS.load(Ordering::SeqCst),
    )
}

struct RuntimeTaskGuard;

impl Drop for RuntimeTaskGuard {
    fn drop(&mut self) {
        LIVE_RUNTIME_TASKS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Allocate one application-owned async task while `TASK_SPAWN_LOCK` is held. This counter follows
/// task ownership directly and therefore does not depend on when Tokio reclaims completed task allocations.
fn try_acquire_runtime_task_locked() -> Option<RuntimeTaskGuard> {
    let live = LIVE_RUNTIME_TASKS.load(Ordering::SeqCst);
    let limit = MAX_RUNTIME_TASKS.load(Ordering::SeqCst);
    if live >= limit {
        return None;
    }
    let live = LIVE_RUNTIME_TASKS.fetch_add(1, Ordering::SeqCst) + 1;
    MAX_OBSERVED_TASKS.fetch_max(live, Ordering::SeqCst);
    Some(RuntimeTaskGuard)
}

/// Keep a finite connection allowance usable when both density resource governors are active.
/// Mandatory server tasks still use the full node-wide task limit; opportunistic maintenance and
/// control-plane work may use only the portion not reserved for RESP connection handlers.
fn opportunistic_runtime_task_limit() -> usize {
    let task_limit = MAX_RUNTIME_TASKS.load(Ordering::SeqCst);
    let connection_reserve = MAX_LIVE_CONNECTIONS.load(Ordering::SeqCst);
    if task_limit == usize::MAX || connection_reserve == usize::MAX {
        task_limit
    } else {
        task_limit.saturating_sub(connection_reserve)
    }
}

pub fn spawn_governed<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let limit = MAX_RUNTIME_TASKS.load(Ordering::SeqCst);
    let guard = {
        let _spawn = TASK_SPAWN_LOCK.lock().expect("task spawn lock poisoned");
        try_acquire_runtime_task_locked()
    }
    .unwrap_or_else(|| panic!("runtime task allocation would exceed governed limit {limit}"));
    tokio::spawn(async move {
        let _guard = guard;
        future.await
    })
}

/// Try to spawn opportunistic application work without panicking when the node-wide task budget is full.
/// Capacity is reserved under the same lock as mandatory spawns and released by the task-owned RAII guard.
pub fn try_spawn_governed<F>(future: F) -> Option<tokio::task::JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let guard = {
        let _spawn = TASK_SPAWN_LOCK.lock().expect("task spawn lock poisoned");
        if LIVE_RUNTIME_TASKS.load(Ordering::SeqCst) >= opportunistic_runtime_task_limit() {
            return None;
        }
        try_acquire_runtime_task_locked()
    }?;
    Some(tokio::spawn(async move {
        let _guard = guard;
        future.await
    }))
}
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
    PushPort
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
    T: PushPort
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
    /// Optional live-routing hook. Routing is queue-scoped: one decision authorizes one atomic batch of
    /// contiguous commands for the same queue. Default single-node/backward-compatible behavior serves locally.
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

    /// Optional cached ownership epoch for queue writes. One lookup fences one atomic same-queue batch;
    /// the backend validates the epoch again at commit. Default backends run the degenerate sole-owner path.
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
    if count > PIPELINE_XADD_ARG_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "RESP array exceeds argument limit",
        ));
    }
    let mut command_bytes = header.len() + 2;
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
        let frame_bytes = bulk_header
            .len()
            .checked_add(2)
            .and_then(|bytes| bytes.checked_add(len))
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bulk frame length overflow",
                )
            })?;
        command_bytes = command_bytes.checked_add(frame_bytes).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "command length overflow")
        })?;
        if len > PIPELINE_XADD_BYTE_LIMIT || command_bytes > PIPELINE_XADD_BYTE_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RESP command exceeds pipeline byte limit",
            ));
        }
        let allocation = len.checked_add(2).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "bulk allocation overflow")
        })?;
        let mut buf = vec![0u8; allocation];
        r.read_exact(&mut buf).await?;
        if &buf[len..] != b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bulk string missing terminator",
            ));
        }
        buf.truncate(len);
        args.push(buf);
    }
    Ok(Some(args))
}

const PIPELINE_XADD_COMMAND_LIMIT: usize = fireweed_engine::MAX_ORDERED_INDEPENDENT_PUSH_ITEMS;
const PIPELINE_XADD_BYTE_LIMIT: usize = 1024 * 1024;
const PIPELINE_XADD_ARG_LIMIT: usize = 65_536;

/// Parse one complete command already buffered by Tokio without awaiting or consuming a partial frame.
/// The connection loop uses this only for pipeline lookahead, so shutdown never cancels a partially-consuming
/// `read_command` future and a fragmented next command remains intact for the normal reader path.
fn parse_buffered_command(buf: &[u8]) -> std::io::Result<Option<(Vec<Vec<u8>>, usize)>> {
    fn line(buf: &[u8], start: usize) -> Option<(&[u8], usize)> {
        let end = buf[start..].windows(2).position(|pair| pair == b"\r\n")? + start;
        Some((&buf[start..end], end + 2))
    }

    let Some((header, mut cursor)) = line(buf, 0) else {
        return Ok(None);
    };
    let Some(count) = header
        .strip_prefix(b"*")
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|raw| raw.parse::<usize>().ok())
    else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected RESP array",
        ));
    };
    if count > PIPELINE_XADD_ARG_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "RESP array exceeds argument limit",
        ));
    }
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((bulk, next)) = line(buf, cursor) else {
            return Ok(None);
        };
        let Some(len) = bulk
            .strip_prefix(b"$")
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<usize>().ok())
        else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        };
        if len > PIPELINE_XADD_BYTE_LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RESP bulk string exceeds pipeline byte limit",
            ));
        }
        let Some(end) = next.checked_add(len) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bulk length overflow",
            ));
        };
        let Some(frame_end) = end.checked_add(2) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bulk frame length overflow",
            ));
        };
        if frame_end > buf.len() {
            return Ok(None);
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bulk string missing terminator",
            ));
        }
        args.push(buf[next..end].to_vec());
        cursor = end + 2;
    }
    Ok(Some((args, cursor)))
}

fn encoded_command_len(args: &[Vec<u8>]) -> usize {
    1 + args.len().to_string().len()
        + 2
        + args
            .iter()
            .map(|arg| 1 + arg.len().to_string().len() + 2 + arg.len() + 2)
            .sum::<usize>()
}

/// Select complete, compatible XADD frames from the bytes Tokio has already buffered. A semantic,
/// malformed, or partial boundary is left untouched for the ordinary command path, which preserves
/// its normal ordered reply and makes cancellation safe.
fn buffered_xadd_window(
    buf: &[u8],
    shard: &QueueKey,
    max_commands: usize,
    max_bytes: usize,
) -> (Vec<Vec<Vec<u8>>>, usize) {
    let mut commands = Vec::new();
    let mut consumed = 0;
    while commands.len() < max_commands && consumed < max_bytes {
        let Ok(Some((args, frame_bytes))) = parse_buffered_command(&buf[consumed..]) else {
            break;
        };
        if frame_bytes > max_bytes - consumed {
            break;
        }
        let Ok(parsed) = parse_xadd(&args) else {
            break;
        };
        if !arg_eq(&args[0], "XADD") || parsed.shard != *shard || parsed.client_item_key.is_some() {
            break;
        }
        consumed += frame_bytes;
        commands.push(args);
    }
    (commands, consumed)
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
/// this future in a timeout (as `fireweed-server`'s `shutdown_and_drain` does) and the bound elapses, the
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
                let live = LIVE_CONNECTIONS.fetch_add(1, Ordering::SeqCst) + 1;
                let limit = MAX_LIVE_CONNECTIONS.load(Ordering::SeqCst);
                let _spawn = TASK_SPAWN_LOCK.lock().expect("task spawn lock poisoned");
                if live > limit {
                    LIVE_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
                    drop(stream);
                    continue;
                }
                let Some(task_guard) = try_acquire_runtime_task_locked() else {
                    LIVE_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
                    drop(stream);
                    continue;
                };
                MAX_OBSERVED_CONNECTIONS.fetch_max(live, Ordering::SeqCst);
                // RESP is a small-message request/reply protocol: leaving Nagle on coalesces each tiny
                // reply and, paired with the peer's delayed-ACK, stalls a pipelined connection ~40ms per
                // command over a real (non-loopback) link. Disable it so replies flush immediately.
                let _ = stream.set_nodelay(true);
                let backend = backend.clone();
                let hooks = hooks.clone();
                let state = state.clone();
                let conn_cancel = cancel.clone();
                conns.spawn(async move {
                    let _task_guard = task_guard;
                    let _ = handle_conn(stream, backend, hooks, state, conn_cancel).await;
                    LIVE_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
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
    let mut reader = BufReader::with_capacity(PIPELINE_XADD_BYTE_LIMIT, rd);
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
        let mut commands = vec![args];
        let first = parse_xadd(&commands[0])
            .ok()
            .filter(|parsed| arg_eq(&commands[0][0], "XADD") && parsed.client_item_key.is_none());
        if let Some(parsed) = &first {
            let first_bytes = encoded_command_len(&commands[0]);
            let (extra, consumed) = buffered_xadd_window(
                reader.buffer(),
                &parsed.shard,
                PIPELINE_XADD_COMMAND_LIMIT - 1,
                PIPELINE_XADD_BYTE_LIMIT.saturating_sub(first_bytes),
            );
            reader.consume(consumed);
            commands.extend(extra);
        }
        let replies = if first.is_some() {
            dispatch_simple_xadd_batch(&backend, &hooks, &state, &commands).await
        } else {
            vec![dispatch(&backend, &hooks, &state, &commands[0]).await]
        };
        let mut buf = Vec::new();
        for reply in replies {
            encode(&reply, &mut buf);
        }
        wr.write_all(&buf).await?;
        wr.flush().await?;
    }
    Ok(())
}

fn arg_eq(a: &[u8], s: &str) -> bool {
    a.eq_ignore_ascii_case(s.as_bytes())
}

async fn push_xadds_ordered_independent<B: PushPort>(
    backend: &B,
    shard: &QueueKey,
    specs: Vec<PushSpec>,
    now: UtcTimestamp,
    expected_epoch: Option<u64>,
) -> Vec<Resp> {
    backend
        .push_ordered_independent(shard, specs, now, expected_epoch)
        .await
        .into_iter()
        .map(|outcome| match outcome {
            Ok(id) => Resp::Bulk(id.to_string().into_bytes()),
            Err(error) => err_reply(&error),
        })
        .collect()
}

async fn xadd_admission<H: RespHooks>(
    hooks: &H,
    args: &[Vec<u8>],
    shard: &QueueKey,
    now: UtcTimestamp,
) -> Result<Option<u64>, Resp> {
    let routing_key = routing_key_for("XADD", args).expect("parsed XADD has routing key");
    match hooks
        .route_command("XADD", args, routing_key, now, false)
        .await
    {
        Ok(RouteDecision::Serve) => {}
        Ok(RouteDecision::Moved { slot, endpoint }) => {
            return Err(Resp::Error(format!("MOVED {slot} {endpoint}")));
        }
        Ok(RouteDecision::NoPerm) => return Err(Resp::Error("NOPERM unauthorized".into())),
        Ok(RouteDecision::Unavailable) => {
            return Err(Resp::Error("ERR fireweed unavailable".into()));
        }
        Err(error) => return Err(err_reply(&error)),
    }
    hooks
        .expected_epoch_for_write(shard, now, false)
        .await
        .map_err(|error| err_reply(&error))
}

async fn dispatch_simple_xadd_batch<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    commands: &[Vec<Vec<u8>>],
) -> Vec<Resp> {
    dispatch_simple_xadd_batch_at(backend.as_ref(), hooks.as_ref(), state.now(), commands).await
}

async fn dispatch_simple_xadd_batch_at<B: PushPort, H: RespHooks>(
    backend: &B,
    hooks: &H,
    now: UtcTimestamp,
    commands: &[Vec<Vec<u8>>],
) -> Vec<Resp> {
    let parsed: Vec<_> = commands
        .iter()
        .map(|args| parse_xadd(args).expect("buffered XADD window contains parsed commands"))
        .collect();
    let shard = parsed[0].shard.clone();
    let expected_epoch = match xadd_admission(hooks, &commands[0], &shard, now).await {
        Ok(epoch) => epoch,
        Err(reply) => return vec![reply; commands.len()],
    };
    // One queue admission is shared by the contiguous pipeline, but each RESP command retains its own
    // scalar transaction and reply. Every backend commit checks the cached epoch, so a handoff fences
    // the not-yet-committed commands without rolling back successful independent siblings.
    push_xadds_ordered_independent(
        backend,
        &shard,
        parsed.into_iter().map(|parsed| parsed.spec).collect(),
        now,
        expected_epoch,
    )
    .await
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
                return Resp::Error("ERR fireweed unavailable".into());
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
        "FW.MGET" => fireweed_mget(backend, args).await,
        "FW.HGETALL" => fireweed_hgetall(backend, args).await,
        "FW.HMGET" => fireweed_hmget(backend, args).await,
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
        "XADD" | "XACK" | "XPENDING" | "XAUTOCLAIM" | "XCLAIM" | "XLEN" | "XDEL" | "FW.MGET"
        | "FW.HGETALL" | "FW.HMGET" => args.get(1).map(Vec::as_slice),
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
struct ParsedXadd {
    shard: QueueKey,
    client_item_key: Option<ClientItemKey>,
    spec: PushSpec,
}

fn parse_xadd(args: &[Vec<u8>]) -> Result<ParsedXadd, Resp> {
    if args.len() < 5 {
        return Err(Resp::Error(
            "ERR wrong number of arguments for 'xadd'".into(),
        ));
    }
    let shard = parse_shard(&args[1]).map_err(|e| err_reply(&e))?;
    if !(args.len() - 3).is_multiple_of(2) {
        return Err(Resp::Error(
            "ERR wrong number of field/value arguments for 'xadd'".into(),
        ));
    }
    // Reserved container fields (TD-006 section 2). Field/value pairs start at index 3.
    let mut priority: Option<PriorityValue> = None;
    let mut client_item_key = None;
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
            client_item_key = Some(
                ClientItemKey::new(s)
                    .map_err(|_| Resp::Error("ERR invalid client_item_key".into()))?,
            );
        } else if arg_eq(&pair[0], "group_key")
            && let Ok(s) = std::str::from_utf8(&pair[1])
        {
            group_key =
                Some(GroupKey::new(s).map_err(|_| Resp::Error("ERR invalid group_key".into()))?);
        } else if arg_eq(&pair[0], "not_before")
            && let Ok(s) = std::str::from_utf8(&pair[1])
        {
            let ms = s
                .parse::<i64>()
                .map_err(|_| Resp::Error("ERR invalid not_before".into()))?;
            not_before = Some(ms_ts(ms).map_err(|_| Resp::Error("ERR invalid not_before".into()))?);
        } else if arg_eq(&pair[0], "payload") {
            payload = Some(bytes::Bytes::copy_from_slice(&pair[1]));
        } else if arg_eq(&pair[0], "metadata") {
            let raw = std::str::from_utf8(&pair[1])
                .map_err(|_| Resp::Error("ERR metadata must be utf-8 JSON".into()))?;
            let entries = serde_json::from_str::<BTreeMap<String, MetadataValue>>(raw)
                .map_err(|_| Resp::Error("ERR invalid metadata".into()))?;
            metadata = Metadata::from_entries(entries);
        } else {
            let field = std::str::from_utf8(&pair[0])
                .map_err(|_| Resp::Error("ERR field names must be utf-8".into()))?;
            if fireweed_engine::is_api001_reserved_write_field(field) {
                return Err(Resp::Error(format!("ERR field '{field}' is reserved")));
            }
            fields.insert(field.to_string(), bytes::Bytes::copy_from_slice(&pair[1]));
        }
    }
    Ok(ParsedXadd {
        shard,
        client_item_key,
        spec: PushSpec {
            client_item_key: None,
            priority,
            not_before,
            group_key,
            payload,
            fields,
            metadata,
            cohort_size: None,
            gate_keys: Vec::new(),
            entity: None,
        },
    })
}

async fn xadd<B: RespBackend, H: RespHooks>(
    backend: &Arc<B>,
    hooks: &Arc<H>,
    state: &Arc<ServerState>,
    args: &[Vec<u8>],
) -> Resp {
    let ParsedXadd {
        shard,
        client_item_key,
        spec,
    } = match parse_xadd(args) {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    let now = state.now();
    let expected_epoch = match hooks.expected_epoch_for_write(&shard, now, false).await {
        Ok(epoch) => epoch,
        Err(e) => return err_reply(&e),
    };
    // The BACKEND assigns the item id in both paths (restart-safe, collision-free across servers — the
    // RESP front never mints ids itself). `client_item_key` is the upsert key (TD-006 §2, Invariant 2):
    // with a key, a second XADD REPLACES the pending item (via UpsertPort); absent a key, each XADD is a
    // unique append (via PushPort). Remaining reserved fields (group_key/not_before/payload) DEFERRED.
    match client_item_key {
        Some(key) => {
            match backend
                .replace_if_pending(
                    &shard,
                    &key,
                    spec.priority,
                    spec.group_key,
                    spec.not_before,
                    spec.payload,
                    spec.fields,
                    spec.metadata,
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
        None => match backend.push(&shard, vec![spec], now, expected_epoch).await {
            Ok(ids) => Resp::Bulk(ids[0].to_string().into_bytes()),
            Err(e) => err_reply(&e),
        },
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
        compatibility: fireweed_engine::ClaimCompatibility::default(),
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

fn lifecycle_name(state: fireweed_core::ItemState) -> &'static str {
    match state {
        fireweed_core::ItemState::Pending => "Pending",
        fireweed_core::ItemState::Leased => "Leased",
        fireweed_core::ItemState::Complete => "Complete",
        fireweed_core::ItemState::Failed => "Failed",
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

async fn fireweed_mget<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'fireweed.mget'".into());
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

async fn fireweed_hgetall<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'fireweed.hgetall'".into());
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

async fn fireweed_hmget<B: RespBackend>(backend: &Arc<B>, args: &[Vec<u8>]) -> Resp {
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'fireweed.hmget'".into());
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
/// The batch is all-or-nothing (FinalizePort pre-validates): a fenced lease → `-ERR fireweed stale_lease`,
/// a superseded id → `-ERR fireweed superseded`, a non-leased id → `-ERR fireweed invalid`, NOTHING is
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

/// Numeric order key for a server-assigned item id. An [`ItemId`](fireweed_core::ItemId) is a single packed
/// `u64` rendered as decimal, and its numeric value IS stream/insertion order by construction (epoch high,
/// counter low — ADR-009). So the order key is just the parsed value. The `"0-0"` cursor sentinel (and any
/// non-numeric cursor) keys as `0`, sorting at/before the first real id so a `start = "0-0"` scan includes
/// the whole PEL.
/// `XPENDING key group [start end count [consumer]]` — the in-flight (leased, not-yet-acked) items.
///
/// Summary form (`XPENDING key group`): `[count, min-id, max-id, [[consumer, count]]]`, where the
/// `consumer` axis is the **lease token** (fireweed's closest analog of a Redis consumer — who holds the
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
    let extended = args.len() > 3; // start/end/count present → per-entry form
    if !extended {
        let summary = match backend.pending_summary(&shard).await {
            Ok(summary) => summary,
            Err(e) => return err_reply(&e),
        };
        // Summary form. Empty → `[0, nil, nil, nil]` (Redis convention).
        if summary.count == 0 {
            return Resp::Array(vec![
                Resp::Int(0),
                Resp::NullArray,
                Resp::NullArray,
                Resp::NullArray,
            ]);
        }
        let consumers: Vec<Resp> = summary
            .consumers
            .iter()
            .map(|(token, n)| {
                Resp::Array(vec![
                    Resp::Bulk(token.as_str().as_bytes().to_vec()),
                    Resp::Bulk(n.to_string().into_bytes()),
                ])
            })
            .collect();
        return Resp::Array(vec![
            Resp::Int(summary.count as i64),
            Resp::Bulk(summary.min_id.unwrap().to_string().into_bytes()),
            Resp::Bulk(summary.max_id.unwrap().to_string().into_bytes()),
            Resp::Array(consumers),
        ]);
    }
    if args.len() != 6 && args.len() != 7 {
        return Resp::Error("ERR wrong number of arguments for 'xpending'".into());
    }
    // Extended form: `XPENDING key group start end count [consumer]` — `count` is args[5].
    let limit = match std::str::from_utf8(&args[5])
        .ok()
        .and_then(|count| count.parse::<usize>().ok())
    {
        Some(limit) if limit > 0 => limit,
        _ => return Resp::Error("ERR count must be > 0".into()),
    };
    let start = match &args[3][..] {
        b"-" => None,
        raw => match ItemId::new(String::from_utf8_lossy(raw)) {
            Ok(id) => Some(id),
            Err(_) => return Resp::Error("ERR fireweed invalid".into()),
        },
    };
    let end = match &args[4][..] {
        b"+" => None,
        raw => match ItemId::new(String::from_utf8_lossy(raw)) {
            Ok(id) => Some(id),
            Err(_) => return Resp::Error("ERR fireweed invalid".into()),
        },
    };
    let consumer = args
        .get(6)
        .map(|raw| LeaseToken::new(String::from_utf8_lossy(raw)));
    let consumer = match consumer.transpose() {
        Ok(token) => token,
        Err(_) => return Resp::Error("ERR fireweed invalid".into()),
    };
    let leases = match backend
        .pending_range(&shard, start, end, consumer.as_ref(), limit)
        .await
    {
        Ok(leases) => leases,
        Err(e) => return err_reply(&e),
    };
    let now_ms = ts_ms(state.now());
    let lease_ms = backend
        .queue_definition(&shard.clone())
        .await
        .map(|d| d.max_lease_duration_ms as i64)
        .unwrap_or(0);
    let out: Vec<Resp> = leases
        .iter()
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
/// fireweed-flavored divergences:
/// - **`min-idle-time` is ignored** — fireweed reclaims strictly by **lease expiry** (the engine's timed
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
        Err(_) => return Resp::Error("ERR fireweed invalid".into()),
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

    let start_id = if start == "0-0" {
        None
    } else {
        match ItemId::new(start.clone()) {
            Ok(id) => Some(id),
            Err(_) => return Resp::Error("ERR fireweed invalid".into()),
        }
    };
    // Read only the COUNT-sized PEL window and one cursor row.
    let page = match backend.pending_page(&shard, start_id, count).await {
        Ok(page) => page,
        Err(e) => return err_reply(&e),
    };

    // Examine a COUNT-sized window; the idle (lease-expired) entries in it are reclaimed to `consumer`.
    let expired_ids: Vec<ItemId> = page
        .entries
        .iter()
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
    let next_cursor = page
        .next
        .map_or_else(|| b"0-0".to_vec(), |id| id.to_string().into_bytes());

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
/// fireweed semantics (TD-006 §3): the **consumer name IS the lease token** (the identity `XPENDING`
/// reports). So per id:
/// - `consumer` == the id's CURRENT lease token → **renew** (extend the lease, [`RenewLeasePort`], NO
///   attempt charge — §3 flavor #7, a worker re-affirming its own claim).
/// - `consumer` != the current token (or a different worker) → **reassign** ([`ReassignLeasePort`]):
///   swap the token to `consumer` and charge exactly one delivery (TD-006:129).
///
/// Divergences: **`min-idle-time` is ignored** (fireweed gates by lease expiry, like `XAUTOCLAIM`); the
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
            Err(_) => return Resp::Error("ERR fireweed invalid".into()),
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
        Err(_) => return Resp::Error("ERR fireweed invalid".into()),
    };

    // Partition by CURRENT owner: ids already owned by `consumer` → renew; the rest → reassign. An id that
    // is not currently leased has no owner, so it falls to reassign and is rejected by reassign_validate.
    let leases = match backend.pending_by_ids(&shard, &ids).await {
        Ok(l) => l,
        Err(e) => return err_reply(&e),
    };
    let leases_by_id: std::collections::HashMap<_, _> = leases
        .iter()
        .map(|lease| (lease.item_id, lease.lease_token.as_str()))
        .collect();
    let mut renew_ids: Vec<ItemId> = Vec::new();
    let mut reassign_ids: Vec<ItemId> = Vec::new();
    for id in &ids {
        let current = leases_by_id.get(id).copied();
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
/// terminal entries (complete/failed, the fireweed analog of acked) are NOT counted, and purged (`XDEL`)
/// entries are gone. A fireweed-flavored read over `metrics` (TD-006 §3).
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
            Err(_) => return Resp::Error("ERR fireweed invalid".into()),
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
/// and `GROUPS` subcommands are offered (a documented divergence; `CONSUMERS`/`FULL` are owed). fireweed
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
            Resp::Int(1), // single implicit consumer group (fireweed has no named-group state)
            Resp::Bulk(b"last-generated-id".to_vec()),
            Resp::Bulk(b"0-0".to_vec()),
            Resp::Bulk(b"last-delivered-id".to_vec()),
            Resp::Bulk(b"0-0".to_vec()),
        ]),
        "GROUPS" => {
            // One implicit group; `pending` = in-flight (leased) count = the group's PEL size.
            let pending = m.leased as i64;
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
    // `-ERR fireweed …` tokens map straight through; the non-`-ERR` errors get their idiomatic Redis
    // reply (TD-006 §2/§7): Forbidden → `-NOPERM`, not-found → `-ERR no such queue`.
    if let Some(tok) = e.resp_token() {
        return Resp::Error(tok.trim_start_matches('-').to_string());
    }
    match e {
        EngineError::Forbidden(why) => Resp::Error(format!("NOPERM {why}")),
        EngineError::NotFound => Resp::Error("ERR no such queue".into()),
        // A client-caused incompatible re-create, not an internal fault (queue-create is library-only
        // over RESP today, so this is latent — but the token must be honest).
        EngineError::QueueDefinitionConflict => Resp::Error("ERR fireweed queue_conflict".into()),
        EngineError::Storage(_) | EngineError::DurableDataCorrupt { .. } => {
            // Keep adapter/storage details out of the client-visible RESP token, but do not erase them
            // from operator diagnostics. Live release evidence relies on the server log to distinguish a
            // durable fault from an intentionally retryable admission response.
            eprintln!("[fireweed-resp] internal engine error: {e}");
            Resp::Error("ERR fireweed internal".into())
        }
        // Every other variant carries a `-ERR fireweed …` token via `resp_token()` above; this arm is
        // unreachable, but stays total.
        _ => Resp::Error("ERR fireweed internal".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(args: &[&[u8]]) -> Vec<Vec<u8>> {
        args.iter().map(|arg| arg.to_vec()).collect()
    }

    fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
        let mut encoded = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            encoded.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            encoded.extend_from_slice(arg);
            encoded.extend_from_slice(b"\r\n");
        }
        encoded
    }

    fn plain_xadd(queue: &'static [u8], n: usize) -> Vec<Vec<u8>> {
        command(&[
            b"XADD",
            queue,
            b"*",
            b"priority",
            n.to_string().leak().as_bytes(),
            b"payload",
            b"body",
            b"custom",
            b"value",
        ])
    }

    #[derive(Default)]
    struct SpyPush {
        calls: Mutex<Vec<usize>>,
        active: AtomicUsize,
        max_active: AtomicUsize,
        next_id: AtomicUsize,
        fail_priority: Option<i64>,
    }

    #[derive(Default)]
    struct CountingAdmission {
        routes: AtomicUsize,
        epochs: AtomicUsize,
        unavailable: bool,
    }

    impl RespHooks for CountingAdmission {
        async fn route_command(
            &self,
            _name: &str,
            _args: &[Vec<u8>],
            _routing_key: &[u8],
            _now: UtcTimestamp,
            _is_new_claim: bool,
        ) -> EngineResult<RouteDecision> {
            self.routes.fetch_add(1, Ordering::SeqCst);
            Ok(if self.unavailable {
                RouteDecision::Unavailable
            } else {
                RouteDecision::Serve
            })
        }

        async fn expected_epoch_for_write(
            &self,
            _shard: &QueueKey,
            _now: UtcTimestamp,
            _is_new_claim: bool,
        ) -> EngineResult<Option<u64>> {
            self.epochs.fetch_add(1, Ordering::SeqCst);
            Ok(Some(7))
        }
    }

    impl PushPort for SpyPush {
        async fn push(
            &self,
            _shard: &QueueKey,
            items: Vec<PushSpec>,
            _now: UtcTimestamp,
            _expected_epoch: Option<u64>,
        ) -> EngineResult<Vec<ItemId>> {
            self.calls.lock().unwrap().push(items.len());
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            if items[0].priority == self.fail_priority.map(PriorityValue::Int64) {
                return Err(EngineError::Invalid("spy item failure"));
            }
            let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(vec![ItemId::from_u64(id as u64)])
        }

        async fn push_with_request_id(
            &self,
            _shard: &QueueKey,
            _request_id: fireweed_core::RequestId,
            _items: Vec<PushSpec>,
            _now: UtcTimestamp,
            _expected_epoch: Option<u64>,
        ) -> EngineResult<Vec<ItemId>> {
            unreachable!("RESP XADD does not carry request_id")
        }
    }

    fn err_text(e: &EngineError) -> String {
        match err_reply(e) {
            Resp::Error(s) => s,
            _ => panic!("expected error reply"),
        }
    }

    #[test]
    fn forbidden_maps_to_noperm_not_generic_err() {
        // TD-006 §2: cross-tenant / operator denial → -NOPERM (NOT a fake -ERR fireweed token).
        assert!(err_text(&EngineError::Forbidden("nope")).starts_with("NOPERM"));
        // `-ERR fireweed …` tokened errors pass through verbatim.
        assert_eq!(
            err_text(&EngineError::StaleLease),
            "ERR fireweed stale_lease"
        );
        assert_eq!(
            err_text(&EngineError::Superseded),
            "ERR fireweed superseded"
        );
        assert_eq!(err_text(&EngineError::NotFound), "ERR no such queue");
    }

    #[test]
    fn buffered_xadd_window_is_bounded_and_preserves_boundaries() {
        let shard = parse_shard(b"tenant:q").unwrap();
        let compatible = plain_xadd(b"tenant:q", 1);
        let other_queue = plain_xadd(b"tenant:other", 2);
        let upsert = command(&[b"XADD", b"tenant:q", b"*", b"client_item_key", b"key"]);
        let non_xadd = command(&[b"PING"]);

        for boundary in [&other_queue, &upsert, &non_xadd] {
            let first = encode_command(&compatible);
            let mut bytes = first.clone();
            bytes.extend_from_slice(&encode_command(boundary));
            let (commands, consumed) = buffered_xadd_window(&bytes, &shard, 100, usize::MAX);
            assert_eq!(commands, vec![compatible.clone()]);
            assert_eq!(consumed, first.len(), "boundary must remain unconsumed");
        }

        let full = encode_command(&compatible);
        let partial = &full[..full.len() - 1];
        let (commands, consumed) = buffered_xadd_window(partial, &shard, 100, usize::MAX);
        assert!(commands.is_empty());
        assert_eq!(consumed, 0, "partial frame must remain unconsumed");

        let mut two = full.clone();
        two.extend_from_slice(&full);
        let (commands, consumed) = buffered_xadd_window(&two, &shard, 100, full.len());
        assert_eq!(commands.len(), 1);
        assert_eq!(
            consumed,
            full.len(),
            "raw RESP framing counts toward the byte cap"
        );
        let (commands, _) = buffered_xadd_window(&two, &shard, 1, usize::MAX);
        assert_eq!(commands.len(), 1, "command count bounds the window");
    }

    #[tokio::test]
    async fn both_command_parsers_reject_unbounded_array_and_bulk_allocations() {
        let excessive_count = format!("*{}\r\n", PIPELINE_XADD_ARG_LIMIT + 1).into_bytes();
        assert!(parse_buffered_command(&excessive_count).is_err());
        let mut reader = BufReader::new(excessive_count.as_slice());
        assert!(read_command(&mut reader).await.is_err());

        let excessive_bulk = format!("*1\r\n${}\r\n", PIPELINE_XADD_BYTE_LIMIT + 1).into_bytes();
        assert!(parse_buffered_command(&excessive_bulk).is_err());
        let mut reader = BufReader::new(excessive_bulk.as_slice());
        assert!(read_command(&mut reader).await.is_err());

        let malformed_terminator = b"*1\r\n$4\r\nPINGxx";
        assert!(parse_buffered_command(malformed_terminator).is_err());
        let mut reader = BufReader::new(malformed_terminator.as_slice());
        assert!(read_command(&mut reader).await.is_err());
    }

    #[tokio::test]
    async fn xadd_windows_use_one_admission_bounded_scalar_pushes_and_isolated_errors() {
        let now = UtcTimestamp::new(1, 0).unwrap();
        for count in [1, 100, 1_000] {
            let commands: Vec<_> = (0..count).map(|n| plain_xadd(b"tenant:q", n)).collect();
            let hooks = CountingAdmission::default();
            let spy = SpyPush::default();
            let replies = dispatch_simple_xadd_batch_at(&spy, &hooks, now, &commands).await;
            assert_eq!(hooks.routes.load(Ordering::SeqCst), 1);
            assert_eq!(hooks.epochs.load(Ordering::SeqCst), 1);
            assert_eq!(spy.calls.lock().unwrap().len(), count);
            assert!(spy.calls.lock().unwrap().iter().all(|size| *size == 1));
            assert_eq!(spy.max_active.load(Ordering::SeqCst), 1);
            assert_eq!(replies.len(), count);
            for (index, reply) in replies.iter().enumerate() {
                assert_eq!(
                    reply,
                    &Resp::Bulk((index + 1).to_string().into_bytes()),
                    "default port method preserves request/ID order"
                );
            }
        }

        let commands: Vec<_> = (0..100).map(|n| plain_xadd(b"tenant:q", n)).collect();
        let hooks = CountingAdmission {
            unavailable: true,
            ..CountingAdmission::default()
        };
        let spy = SpyPush::default();
        let replies = dispatch_simple_xadd_batch_at(&spy, &hooks, now, &commands).await;
        assert_eq!(hooks.routes.load(Ordering::SeqCst), 1);
        assert_eq!(hooks.epochs.load(Ordering::SeqCst), 0);
        assert!(spy.calls.lock().unwrap().is_empty());
        assert_eq!(
            replies,
            vec![Resp::Error("ERR fireweed unavailable".into()); 100]
        );

        let hooks = CountingAdmission::default();
        let spy = SpyPush {
            fail_priority: Some(42),
            ..SpyPush::default()
        };
        let replies = dispatch_simple_xadd_batch_at(&spy, &hooks, now, &commands).await;
        assert_eq!(replies.len(), 100);
        assert!(matches!(&replies[42], Resp::Error(error) if error.contains("invalid")));
        assert!(
            replies[..42]
                .iter()
                .all(|reply| matches!(reply, Resp::Bulk(_)))
        );
        assert!(
            replies[43..]
                .iter()
                .all(|reply| matches!(reply, Resp::Bulk(_)))
        );
        assert_eq!(spy.calls.lock().unwrap().len(), 100);
    }

    #[tokio::test]
    async fn compatible_xadd_windows_use_ordered_independent_pushes_for_1_100_and_1000() {
        let shard = parse_shard(b"tenant:q").unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        for count in [1, 100, 1_000] {
            let commands: Vec<_> = (0..count).map(|n| plain_xadd(b"tenant:q", n)).collect();
            let bytes: Vec<_> = commands
                .iter()
                .flat_map(|args| encode_command(args))
                .collect();
            let (parsed, consumed) =
                buffered_xadd_window(&bytes, &shard, count, PIPELINE_XADD_BYTE_LIMIT);
            assert_eq!(parsed.len(), count);
            assert_eq!(consumed, bytes.len());
            let specs = parsed
                .iter()
                .map(|args| parse_xadd(args).unwrap().spec)
                .collect();
            let spy = SpyPush::default();
            let replies = push_xadds_ordered_independent(&spy, &shard, specs, now, Some(7)).await;
            assert_eq!(spy.calls.lock().unwrap().len(), count);
            assert!(spy.calls.lock().unwrap().iter().all(|size| *size == 1));
            assert_eq!(replies.len(), count);
            for (index, reply) in replies.iter().enumerate() {
                assert_eq!(reply, &Resp::Bulk((index + 1).to_string().into_bytes()));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn governed_task_cap_is_non_poisoning_and_retries_after_capacity_returns() {
        assert_eq!(LIVE_RUNTIME_TASKS.load(Ordering::SeqCst), 0);
        let prior_limit = MAX_RUNTIME_TASKS.swap(1, Ordering::SeqCst);
        let prior_connection_limit = MAX_LIVE_CONNECTIONS.swap(usize::MAX, Ordering::SeqCst);
        let prior_max = MAX_OBSERVED_TASKS.swap(0, Ordering::SeqCst);

        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let first = spawn_governed(async move {
            let _ = wait.await;
        });
        assert_eq!(runtime_task_resource_counts(), (1, 1, 1));
        assert!(try_spawn_governed(async {}).is_none());

        let mandatory = std::panic::catch_unwind(|| spawn_governed(async {}));
        assert!(mandatory.is_err(), "mandatory admission still fails closed");
        assert!(
            TASK_SPAWN_LOCK.lock().is_ok(),
            "capacity panic must happen after releasing the admission lock"
        );

        release.send(()).unwrap();
        first.await.unwrap();
        let retry = try_spawn_governed(async { 7 }).expect("capacity is reusable after completion");
        assert_eq!(retry.await.unwrap(), 7);
        assert_eq!(LIVE_RUNTIME_TASKS.load(Ordering::SeqCst), 0);

        MAX_RUNTIME_TASKS.store(3, Ordering::SeqCst);
        MAX_LIVE_CONNECTIONS.store(1, Ordering::SeqCst);
        let (release_mandatory, wait_mandatory) = tokio::sync::oneshot::channel::<()>();
        let mandatory = spawn_governed(async move {
            let _ = wait_mandatory.await;
        });
        let (release_opportunistic, wait_opportunistic) = tokio::sync::oneshot::channel::<()>();
        let opportunistic = try_spawn_governed(async move {
            let _ = wait_opportunistic.await;
        })
        .expect("one opportunistic task fits below connection headroom");
        assert!(
            try_spawn_governed(async {}).is_none(),
            "opportunistic work must preserve the configured connection headroom"
        );
        let connection_guard = {
            let _spawn = TASK_SPAWN_LOCK.lock().expect("task spawn lock poisoned");
            try_acquire_runtime_task_locked()
        }
        .expect("the reserved connection task still fits under the node-wide limit");
        assert_eq!(runtime_task_resource_counts(), (3, 3, 3));
        drop(connection_guard);
        release_opportunistic.send(()).unwrap();
        release_mandatory.send(()).unwrap();
        opportunistic.await.unwrap();
        mandatory.await.unwrap();
        assert_eq!(LIVE_RUNTIME_TASKS.load(Ordering::SeqCst), 0);

        MAX_RUNTIME_TASKS.store(prior_limit, Ordering::SeqCst);
        MAX_LIVE_CONNECTIONS.store(prior_connection_limit, Ordering::SeqCst);
        MAX_OBSERVED_TASKS.store(prior_max, Ordering::SeqCst);
    }
}
