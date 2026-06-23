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
    Backend, ClaimPort, ClaimRequest, ClaimedItem, CommandChecksum, CommandEnvelope, CommandId,
    ControlPlaneStore, EngineError, FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionRead,
    QueueCommand, ShardId, ShardKey, UpsertOutcome, UpsertPort,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// The backend capabilities the RESP front needs. A concrete backend (e.g. `MemoryBackend`) is
/// injected by the composition root / tests; the adapter never names one (hexagonal).
pub trait RespBackend:
    Backend + ClaimPort + UpsertPort + ControlPlaneStore + ProjectionRead + Send + Sync + 'static
{
}
impl<T> RespBackend for T where
    T: Backend
        + ClaimPort
        + UpsertPort
        + ControlPlaneStore
        + ProjectionRead
        + Send
        + Sync
        + 'static
{
}

struct ServerState {
    ids: AtomicU64,
}

impl ServerState {
    /// Monotonic unique id: one shared pool for item ids, lease tokens, and command ids (all just
    /// need uniqueness in the smoke front).
    fn next(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }
}

// DEFERRED (composition root, Phase 4/5): a fixed stub clock. Leases never expire and `not_before`
// is always due, so reclaim/delay CANNOT be exercised over this front. The engine's `Clock` port is
// threaded by the composition root later; do not use this front for any time-dependent behavior.
fn now_ts() -> UtcTimestamp {
    UtcTimestamp::new(0, 0).expect("ts")
}
fn far_future() -> UtcTimestamp {
    UtcTimestamp::new(1_000_000, 0).expect("ts")
}

/// Parse a stream key `tenant:queue` (or bare `queue` with a default tenant) into a launch shard key.
fn parse_shard(key: &[u8]) -> Result<ShardKey, EngineError> {
    let s = std::str::from_utf8(key).map_err(|_| EngineError::Invalid("non-utf8 key"))?;
    let (tenant, queue) = match s.split_once(':') {
        Some((t, q)) => (t, q),
        None => ("default", s),
    };
    let tenant = TenantId::new(tenant).map_err(|_| EngineError::Invalid("bad tenant"))?;
    let queue = QueueId::new(queue).map_err(|_| EngineError::Invalid("bad queue"))?;
    Ok(ShardKey::new(tenant, queue, ShardId::ZERO))
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

/// Serve RESP connections over `listener`, dispatching to `backend`, until the listener closes.
pub async fn serve<B: RespBackend>(listener: TcpListener, backend: Arc<B>) {
    let state = Arc::new(ServerState {
        ids: AtomicU64::new(1),
    });
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let backend = backend.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let _ = handle_conn(stream, backend, state).await;
        });
    }
}

async fn handle_conn<B: RespBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    state: Arc<ServerState>,
) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    while let Some(args) = read_command(&mut reader).await? {
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
    let mut fields = args[3..].chunks_exact(2);
    for pair in fields.by_ref() {
        if arg_eq(&pair[0], "priority")
            && let Ok(s) = std::str::from_utf8(&pair[1])
            && let Ok(n) = s.parse::<i64>()
        {
            priority = Some(PriorityValue::Int64(n));
        }
    }
    // Only `priority` is read in this smoke front; other reserved container fields (group_key,
    // not_before, payload, client_item_key - TD-006 section 2) are DEFERRED to Phase 4. No
    // client_item_key means each XADD is a unique insert (append semantics).
    let n = state.next();
    let item_id = ItemId::new(format!("{n}-0")).expect("id");
    let key = ClientItemKey::new(format!("k-{n}")).expect("key");
    match backend
        .replace_if_pending(
            &shard,
            &key,
            item_id.clone(),
            priority,
            None,
            None,
            None,
            now_ts(),
        )
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
    let lease = state.next();
    let req = ClaimRequest {
        shard,
        worker_id: WorkerId::new("resp").expect("w"),
        max_items: count,
        lease_token: LeaseToken::new(format!("L{lease}")).expect("lease"),
        lease_expires_at: far_future(),
        now: now_ts(),
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
/// DEFERRED (Phase 4): no lease/PEL ownership or stale-fence validation, and the reply is the
/// *requested* id count, not the actually-finalized count. Real XACK must surface
/// `-ERR pqueue stale_lease`/`superseded` and count only PEL removals (TD-006 section 3).
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
    let n = state.next();
    let env = CommandEnvelope {
        command_id: CommandId::new(format!("ack-{n}")),
        request_id: None,
        shard_id: ShardId::ZERO,
        item_ids: ids.clone(),
        command: QueueCommand::Finalize(FinalizeCommand { outcomes }),
        checksum: CommandChecksum(0),
        created_at: now_ts(),
    };
    let result = backend
        .write(move |lw, pw| {
            let pos = lw.append(&shard, std::slice::from_ref(&env))?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await;
    match result {
        Ok(()) => Resp::Int(ids.len() as i64),
        Err(e) => err_reply(&e),
    }
}

fn err_reply(e: &EngineError) -> Resp {
    match e.resp_token() {
        Some(tok) => Resp::Error(tok.trim_start_matches('-').to_string()),
        None => Resp::Error("ERR pqueue error".into()),
    }
}
