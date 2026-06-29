//! TP-002 **E2 in-cluster load generator** (ADR-008: the queue is the unit of sharding).
//!
//! Two subcommands:
//!
//! * `run` — connect to a set of independent owner Services over the cluster network (pod->pod), drive the
//!   segmented `object_log_sqlite_projection` RESP workload (pipelined `XADD` ingest, `XREADGROUP >` claim,
//!   `XACK` finalize) at ONE owner count, live-prove one-owner-per-queue (`XLEN` answers an integer on the
//!   owner and `-ERR no such queue` on every other node), and print a single measured `RESULT {json}` line:
//!   per-queue + aggregate ingest and claim+finalize throughput. Designed to run as an in-cluster `Job` with
//!   a BOUNDED CPU limit so it never starves the co-located CPU-limited server pods (the fix that lets each
//!   owner clear the per-queue floor — see `docs/perf/tp002-e2-multinode-kind-release.md`).
//!
//! * `emit-row` — fold three per-owner-count `run` results (owners 2 / 4 / 8) into ONE TP-002 E2
//!   verification-ledger row. It judges the four release bars (ingest aggregate strictly non-decreasing
//!   2->4->8; 8-owner ingest aggregate at least 3.5x the 2-owner; worst per-queue ingest AND claim+finalize
//!   each at least the E0 floor 2,777.78/s; one-owner-per-queue live-proven), emits `evidence_tier=release`
//!   ONLY when all bars hold (else `smoke`, never a faked release row), strict-validates the row via
//!   `pqueue_release`, prints the verdict, and exits non-zero unless all bars pass (so a sweep that misses
//!   the bars fails the orchestrator and keeps the bead open).
//!
//! The RESP client is a raw `std::net::TcpStream` client (no new dependency) — the same wire the
//! off-the-shelf-client e2e exercises. Every number is MEASURED, never hard-coded.

use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;
/// The E2 headline cross-node multiple: 8-owner aggregate must be at least this times the 2-owner aggregate.
const SCALE_MULTIPLE_BAR: f64 = 3.5;

// ----------------------------------------------------------------------------------------------------
// Spec + result wire types (JSON between the orchestrator and this generator).
// ----------------------------------------------------------------------------------------------------

/// One owner node: the Service address (`host:port`) reachable on the cluster network and the FULL queue
/// keys it owns (disjoint from every other node).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSpec {
    addr: String,
    queues: Vec<String>,
}

/// The owner cluster for one scale point.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSpec {
    owners: usize,
    nodes: Vec<NodeSpec>,
}

/// One measured scale point (printed by `run`, consumed by `emit-row`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunResult {
    owners: usize,
    ingest_aggregate: f64,
    ingest_min_per_queue: f64,
    drain_aggregate: f64,
    drain_min_per_queue: f64,
    one_owner_confirmations: usize,
    queues_per_owner: usize,
    items_per_queue: u64,
    conns_per_queue: usize,
}

/// Per-node tuning recorded into the evidence row (passed by the orchestrator to `emit-row`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TuningMeta {
    segment_max_latency_ms: u64,
    segment_target_bytes: usize,
    worker_threads_per_node: usize,
    server_cpu_limit: String,
    server_cpu_request: String,
    loadgen_cpu_limit: String,
    cores: usize,
    kind_node_image: String,
    sweep: u64,
}

// ----------------------------------------------------------------------------------------------------
// Raw RESP2 client over std::net::TcpStream (no new dependency).
// ----------------------------------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
enum Val {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Arr(Option<Vec<Val>>),
}

fn read_line<R: BufRead>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let n = r.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "eof reading RESP line",
        ));
    }
    while matches!(buf.last(), Some(b'\n') | Some(b'\r')) {
        buf.pop();
    }
    Ok(buf)
}

fn read_val<R: BufRead>(r: &mut R) -> io::Result<Val> {
    let line = read_line(r)?;
    if line.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty RESP line",
        ));
    }
    let tag = line[0];
    let rest = String::from_utf8_lossy(&line[1..]).into_owned();
    match tag {
        b'+' => Ok(Val::Simple(rest)),
        b'-' => Ok(Val::Err(rest)),
        b':' => Ok(Val::Int(rest.parse().unwrap_or(0))),
        b'$' => {
            let n: i64 = rest.parse().unwrap_or(-1);
            if n < 0 {
                return Ok(Val::Bulk(None));
            }
            let mut data = vec![0u8; n as usize];
            r.read_exact(&mut data)?;
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf)?;
            Ok(Val::Bulk(Some(data)))
        }
        b'*' => {
            let n: i64 = rest.parse().unwrap_or(-1);
            if n < 0 {
                return Ok(Val::Arr(None));
            }
            let mut v = Vec::with_capacity(n as usize);
            for _ in 0..n {
                v.push(read_val(r)?);
            }
            Ok(Val::Arr(Some(v)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected RESP tag {:?}", other as char),
        )),
    }
}

fn encode(buf: &mut Vec<u8>, args: &[&[u8]]) {
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
}

/// One RESP connection: a write half plus a buffered read half over a cloned socket.
struct Conn {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

impl Conn {
    fn connect(addr: &str) -> io::Result<Conn> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_secs(120)))?;
        let r = BufReader::new(s.try_clone()?);
        Ok(Conn { w: s, r })
    }

    fn send(&mut self, args: &[&[u8]]) -> io::Result<()> {
        let mut b = Vec::new();
        encode(&mut b, args);
        self.w.write_all(&b)
    }

    fn recv(&mut self) -> io::Result<Val> {
        read_val(&mut self.r)
    }
}

/// `XLEN key` → `Ok(n)` on the owner, `Err(reply)` when the queue is unknown (`-ERR no such queue`).
fn xlen(conn: &mut Conn, key: &str) -> io::Result<Result<i64, String>> {
    conn.send(&[b"XLEN", key.as_bytes()])?;
    Ok(match conn.recv()? {
        Val::Int(n) => Ok(n),
        Val::Err(e) => Err(e),
        other => Err(format!("unexpected XLEN reply {other:?}")),
    })
}

fn extract_ids(v: &Val) -> Vec<String> {
    let mut ids = Vec::new();
    if let Val::Arr(Some(streams)) = v {
        for s in streams {
            if let Val::Arr(Some(pair)) = s
                && let Some(Val::Arr(Some(entries))) = pair.get(1)
            {
                for e in entries {
                    if let Val::Arr(Some(entry)) = e
                        && let Some(Val::Bulk(Some(idb))) = entry.first()
                    {
                        ids.push(String::from_utf8_lossy(idb).into_owned());
                    }
                }
            }
        }
    }
    ids
}

/// A transient owner-epoch fence: under heavy CPU contention an owner's lease can momentarily flap, the next
/// write re-acquires at a bumped epoch, and in-flight commands cached at the old epoch are fenced
/// (`-ERR pqueue epoch_stale`) or briefly see no owner (`-ERR ... unavailable`). A real client re-resolves
/// and retries; we do the same (the retry cost stays INSIDE the timed window, so throughput stays honest).
fn is_transient_fence(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    e.contains("epoch_stale") || e.contains("unavailable")
}

/// Push `total` items into `key`, PIPELINED `pipe` at a time. Each item carries a rotating priority. Only
/// SUCCESSFUL `XADD`s count toward `total`; an item fenced by a transient epoch flap is simply re-sent on the
/// next wave (the item payloads are interchangeable, so retry preserves the exact pushed count). A long fence
/// storm trips the guard and fails the run loudly rather than spinning forever.
fn push_items(conn: &mut Conn, key: &str, total: u64, pipe: usize) {
    let mut done = 0u64;
    let mut pr = 0u64;
    let mut fence_waves = 0u64;
    while done < total {
        let n = ((total - done) as usize).min(pipe.max(1));
        let mut buf = Vec::new();
        for k in 0..n {
            let p = ((pr + k as u64) % 1000).to_string();
            encode(
                &mut buf,
                &[b"XADD", key.as_bytes(), b"*", b"priority", p.as_bytes()],
            );
        }
        conn.w.write_all(&buf).expect("write XADD batch");
        let mut ok = 0u64;
        for _ in 0..n {
            match conn.recv().expect("read XADD reply") {
                Val::Bulk(Some(_)) => ok += 1,
                Val::Err(e) if is_transient_fence(&e) => {}
                other => panic!("XADD on {key} failed: {other:?}"),
            }
        }
        done += ok;
        pr += n as u64;
        if ok < n as u64 {
            fence_waves += 1;
            assert!(
                fence_waves < 100_000,
                "XADD on {key} kept hitting a transient fence; giving up"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Cooperatively drain `key` (`XREADGROUP >` + `XACK`) until an empty batch. Returns the count drained.
fn drain(conn: &mut Conn, key: &str, consumer: &str, batch: usize) -> u64 {
    let mut total = 0u64;
    let count = batch.max(1).to_string();
    let mut guard = 0u64;
    loop {
        guard += 1;
        assert!(guard < 100_000_000, "drain did not terminate on {key}");
        conn.send(&[
            b"XREADGROUP",
            b"GROUP",
            b"g",
            consumer.as_bytes(),
            b"COUNT",
            count.as_bytes(),
            b"STREAMS",
            key.as_bytes(),
            b">",
        ])
        .expect("send XREADGROUP");
        let reply = conn.recv().expect("read XREADGROUP reply");
        // A transient epoch flap fences the read; re-resolve and retry (no items were claimed).
        if let Val::Err(e) = &reply
            && is_transient_fence(e)
        {
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        let ids = extract_ids(&reply);
        if ids.is_empty() {
            break;
        }
        let id_bytes: Vec<Vec<u8>> = ids.iter().map(|s| s.clone().into_bytes()).collect();
        let mut args: Vec<&[u8]> = vec![b"XACK", key.as_bytes(), b"g"];
        for ib in &id_bytes {
            args.push(ib);
        }
        // Re-send the XACK until it lands; acking the same ids twice is harmless (idempotent finalize).
        loop {
            conn.send(&args).expect("send XACK");
            match conn.recv().expect("read XACK reply") {
                Val::Int(_) => break,
                Val::Err(e) if is_transient_fence(&e) => {
                    thread::sleep(Duration::from_millis(10));
                }
                other => panic!("XACK on {key} failed: {other:?}"),
            }
        }
        total += ids.len() as u64;
    }
    total
}

// ----------------------------------------------------------------------------------------------------
// Driving the cluster.
// ----------------------------------------------------------------------------------------------------

/// Block until every node answers an `XLEN` of its first owned queue with an integer (bootstrap complete).
fn await_ready(spec: &RunSpec) {
    let deadline = Instant::now() + Duration::from_secs(120);
    for n in &spec.nodes {
        let probe = n.queues.first().expect("every node owns >=1 queue");
        loop {
            if let Ok(mut c) = Conn::connect(&n.addr)
                && let Ok(Ok(_)) = xlen(&mut c, probe)
            {
                break;
            }
            assert!(
                Instant::now() <= deadline,
                "node {} not ready within deadline",
                n.addr
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

/// PROVE one-owner-per-queue: for every queue, the owner answers `XLEN` with an integer and EVERY other node
/// rejects it as `-ERR no such queue`. Returns the number of cross-node "unknown" confirmations.
fn assert_one_owner_per_queue(spec: &RunSpec) -> usize {
    let mut cross_node_unknown = 0usize;
    let all: Vec<(usize, &String)> = spec
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(i, n)| n.queues.iter().map(move |q| (i, q)))
        .collect();
    for (owner_i, q) in &all {
        for (j, node) in spec.nodes.iter().enumerate() {
            let mut c = Conn::connect(&node.addr).expect("probe connect");
            let res = xlen(&mut c, q).expect("probe xlen");
            if j == *owner_i {
                assert!(
                    res.is_ok(),
                    "queue {q} must be served by its owner node {j} ({}), got {res:?}",
                    node.addr
                );
            } else {
                let err = res.expect_err("non-owner must reject the queue");
                assert!(
                    err.contains("no such queue"),
                    "queue {q} must be UNKNOWN on non-owner node {j} ({}); got {err:?}",
                    node.addr
                );
                cross_node_unknown += 1;
            }
        }
    }
    cross_node_unknown
}

/// Run ONE phase across every queue's connections and return `(per_queue_max_ns, wall_seconds)`.
fn run_phase<F>(queue_keys: &[(String, String)], conns_per_queue: usize, work: F) -> (Vec<u64>, f64)
where
    F: Fn(&mut Conn, &str, usize) + Send + Sync + 'static,
{
    let num_queues = queue_keys.len();
    let total_threads = num_queues * conns_per_queue;
    let barrier = Arc::new(Barrier::new(total_threads + 1));
    let per_queue_ns: Arc<Vec<AtomicU64>> =
        Arc::new((0..num_queues).map(|_| AtomicU64::new(0)).collect());
    let work = Arc::new(work);
    let mut handles = Vec::with_capacity(total_threads);
    for (qidx, (addr, key)) in queue_keys.iter().enumerate() {
        for ci in 0..conns_per_queue {
            let addr = addr.clone();
            let key = key.clone();
            let barrier = Arc::clone(&barrier);
            let per_queue_ns = Arc::clone(&per_queue_ns);
            let work = Arc::clone(&work);
            handles.push(thread::spawn(move || {
                let mut c = Conn::connect(&addr).expect("driver connect");
                barrier.wait();
                let t = Instant::now();
                work(&mut c, &key, ci);
                per_queue_ns[qidx].fetch_max(t.elapsed().as_nanos() as u64, Ordering::SeqCst);
            }));
        }
    }
    barrier.wait();
    let wall_start = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    let wall = wall_start.elapsed().as_secs_f64();
    let per_queue = Arc::try_unwrap(per_queue_ns)
        .expect("phase done")
        .into_iter()
        .map(|a| a.into_inner())
        .collect();
    (per_queue, wall)
}

/// Establish durable ownership of every queue WITHOUT concurrency, BEFORE the timed phases. The first WRITE
/// to an unowned queue triggers a non-idempotent cold-start epoch acquire; when many connections race that
/// first write, a connection can observe the freshly-bumped control-plane epoch before the backend's durable
/// epoch catches up and fence itself (`-ERR pqueue epoch_stale`). A single serial connection per queue
/// (push one item, then drain+ack it) forces the acquire to complete durably with no race and leaves the
/// queue empty + owned, so the concurrent timed phases measure steady-state throughput on an owned queue.
/// Warm-up runs one thread PER QUEUE (queues are independent owners), but only ONE connection per queue.
fn warm_up(queue_keys: &[(String, String)], batch: usize) {
    let handles: Vec<_> = queue_keys
        .iter()
        .map(|(addr, key)| {
            let addr = addr.clone();
            let key = key.clone();
            thread::spawn(move || {
                let mut c = Conn::connect(&addr).expect("warm-up connect");
                push_items(&mut c, &key, 1, 1);
                let drained = drain(&mut c, &key, "warmup", batch);
                assert_eq!(
                    drained, 1,
                    "warm-up of {key} must drain its single seed item"
                );
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

/// Drive the cluster in two sequential, independently-timed phases (INGEST, then CLAIM+FINALIZE).
fn measure(
    spec: &RunSpec,
    items_per_queue: u64,
    conns_per_queue: usize,
    pipe: usize,
    batch: usize,
) -> (f64, f64, f64, f64) {
    let conns_per_queue = conns_per_queue.max(1);
    let queue_keys: Vec<(String, String)> = spec
        .nodes
        .iter()
        .flat_map(|n| n.queues.iter().map(move |q| (n.addr.clone(), q.clone())))
        .collect();
    let num_queues = queue_keys.len();
    // Establish durable ownership serially first (no concurrent cold-start epoch race), leaving queues empty.
    warm_up(&queue_keys, batch);
    let per_conn = items_per_queue / conns_per_queue as u64;
    let remainder = items_per_queue - per_conn * conns_per_queue as u64;

    // ---- Phase 1: INGEST ----
    let (push_ns, ingest_wall) = run_phase(&queue_keys, conns_per_queue, move |c, key, ci| {
        let my_push = per_conn + if ci == 0 { remainder } else { 0 };
        push_items(c, key, my_push, pipe);
    });

    // ---- Phase 2: CLAIM+FINALIZE ----
    let drained_total = Arc::new(AtomicU64::new(0));
    let dt = Arc::clone(&drained_total);
    let (drain_ns, drain_wall) = run_phase(&queue_keys, conns_per_queue, move |c, key, ci| {
        let consumer = format!("c{ci}");
        let got = drain(c, key, &consumer, batch);
        dt.fetch_add(got, Ordering::SeqCst);
    });
    assert_eq!(
        drained_total.load(Ordering::SeqCst),
        num_queues as u64 * items_per_queue,
        "every pushed item across every queue must be claimed+finalized"
    );

    let min_per_queue = |slot: &[u64]| {
        slot.iter()
            .map(|&ns| items_per_queue as f64 / (ns as f64 / 1e9))
            .fold(f64::INFINITY, f64::min)
    };
    let total_items = (num_queues as u64 * items_per_queue) as f64;
    (
        total_items / ingest_wall,
        min_per_queue(&push_ns),
        total_items / drain_wall,
        min_per_queue(&drain_ns),
    )
}

// ----------------------------------------------------------------------------------------------------
// Subcommands.
// ----------------------------------------------------------------------------------------------------

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn arg_values(args: &[String], flag: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == flag
            && let Some(v) = args.get(i + 1)
        {
            out.push(v.clone());
            i += 1;
        }
        i += 1;
    }
    out
}

fn parse_usize_arg(args: &[String], flag: &str, default: usize) -> usize {
    arg_value(args, flag)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_arg(args: &[String], flag: &str, default: u64) -> u64 {
    arg_value(args, flag)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn cmd_run(args: &[String]) -> ! {
    let spec_json = match (arg_value(args, "--spec"), arg_value(args, "--spec-file")) {
        (Some(s), _) => s,
        (None, Some(path)) => {
            let mut f =
                std::fs::File::open(&path).unwrap_or_else(|e| panic!("open spec file {path}: {e}"));
            let mut s = String::new();
            f.read_to_string(&mut s).expect("read spec file");
            s
        }
        (None, None) => {
            eprintln!("run requires --spec <json> or --spec-file <path>");
            exit(2);
        }
    };
    let spec: RunSpec = serde_json::from_str(&spec_json).expect("parse --spec JSON");
    assert!(!spec.nodes.is_empty(), "spec has no nodes");
    let queues_per_owner = spec.nodes.first().map(|n| n.queues.len()).unwrap_or(0);

    let items_per_queue = parse_u64_arg(args, "--items-per-queue", 12_000);
    let conns_per_queue = parse_usize_arg(args, "--conns-per-queue", 8);
    let pipe = parse_usize_arg(args, "--pipe", 1_000);
    let batch = parse_usize_arg(args, "--batch", 1_000);

    await_ready(&spec);
    let one_owner_confirmations = assert_one_owner_per_queue(&spec);
    let (ingest_aggregate, ingest_min_per_queue, drain_aggregate, drain_min_per_queue) =
        measure(&spec, items_per_queue, conns_per_queue, pipe, batch);

    let result = RunResult {
        owners: spec.owners,
        ingest_aggregate,
        ingest_min_per_queue,
        drain_aggregate,
        drain_min_per_queue,
        one_owner_confirmations,
        queues_per_owner,
        items_per_queue,
        conns_per_queue,
    };
    eprintln!(
        "owners={} ingest_agg={:.0}/s worst_ingest/q={:.0}/s claim+final_agg={:.0}/s worst_claim+final/q={:.0}/s one_owner_confirmations={}",
        result.owners,
        result.ingest_aggregate,
        result.ingest_min_per_queue,
        result.drain_aggregate,
        result.drain_min_per_queue,
        result.one_owner_confirmations,
    );
    // The single machine-readable line the orchestrator greps for.
    println!(
        "RESULT {}",
        serde_json::to_string(&result).expect("serialize result")
    );
    exit(0);
}

fn cmd_emit_row(args: &[String]) -> ! {
    let result_paths = arg_values(args, "--result");
    assert_eq!(
        result_paths.len(),
        3,
        "emit-row needs exactly three --result files (owners 2, 4, 8)"
    );
    let mut results: Vec<RunResult> = result_paths
        .iter()
        .map(|p| {
            let s = std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read result {p}: {e}"));
            // Accept either a bare result JSON or the `RESULT {json}` log line.
            let json = s
                .lines()
                .find_map(|l| l.strip_prefix("RESULT "))
                .map(|l| l.to_string())
                .unwrap_or_else(|| s.trim().to_string());
            serde_json::from_str::<RunResult>(&json).unwrap_or_else(|e| panic!("parse {p}: {e}"))
        })
        .collect();
    results.sort_by_key(|r| r.owners);
    let owners: Vec<usize> = results.iter().map(|r| r.owners).collect();
    assert_eq!(owners, vec![2, 4, 8], "results must cover owners 2, 4, 8");
    let at = |n: usize| results.iter().find(|r| r.owners == n).unwrap();

    let tuning_json = arg_value(args, "--tuning").expect("emit-row needs --tuning <json>");
    let tuning: TuningMeta = serde_json::from_str(&tuning_json).expect("parse --tuning JSON");
    let out = arg_value(args, "--out").expect("emit-row needs --out <path>");

    // ---- Evaluate the four E2 bars (every value measured). ----
    let nondecreasing = at(4).ingest_aggregate >= at(2).ingest_aggregate
        && at(8).ingest_aggregate >= at(4).ingest_aggregate;
    let ratio_8_2 = at(8).ingest_aggregate / at(2).ingest_aggregate;
    let scale_pass = ratio_8_2 >= SCALE_MULTIPLE_BAR;
    let worst_ingest_per_queue = results
        .iter()
        .map(|r| r.ingest_min_per_queue)
        .fold(f64::INFINITY, f64::min);
    let worst_drain_per_queue = results
        .iter()
        .map(|r| r.drain_min_per_queue)
        .fold(f64::INFINITY, f64::min);
    let worst_per_queue = worst_ingest_per_queue.min(worst_drain_per_queue);
    let floor_pass = worst_per_queue >= FLOOR_ITEMS_PER_SEC;
    let confirmations_at_8 = at(8).one_owner_confirmations;
    let disjoint_pass = confirmations_at_8 > 0 && at(8).queues_per_owner > 0;
    let all_pass = nondecreasing && scale_pass && floor_pass && disjoint_pass;

    eprintln!("\n--- TP-002 E2 sweep {} verdict (kind) ---", tuning.sweep);
    eprintln!(
        "  (1) non-decreasing ingest 2->4->8 : {} ({:.0} -> {:.0} -> {:.0})",
        yn(nondecreasing),
        at(2).ingest_aggregate,
        at(4).ingest_aggregate,
        at(8).ingest_aggregate
    );
    eprintln!(
        "  (2) 8/2 ingest aggregate multiple : {ratio_8_2:.2}x (bar >= {SCALE_MULTIPLE_BAR}x) -> {}",
        yn(scale_pass)
    );
    eprintln!(
        "  (3) worst per-queue (floor {FLOOR_ITEMS_PER_SEC:.0}/s) : ingest {worst_ingest_per_queue:.0}/s, claim+finalize {worst_drain_per_queue:.0}/s -> {}",
        yn(floor_pass)
    );
    eprintln!(
        "  (4) one-owner-per-queue : {confirmations_at_8} cross-node 'no such queue' confirmations at 8 owners -> {}",
        yn(disjoint_pass)
    );
    eprintln!(
        "  ==> headline bars {}",
        if all_pass { "PASS" } else { "NOT MET" }
    );

    let tier = if all_pass { "release" } else { "smoke" };
    let scale = if all_pass { "release" } else { "smoke" };
    let values = BTreeMap::from([
        (
            "owners_2_ingest_aggregate_per_s".to_string(),
            serde_json::json!(at(2).ingest_aggregate.round()),
        ),
        (
            "owners_4_ingest_aggregate_per_s".to_string(),
            serde_json::json!(at(4).ingest_aggregate.round()),
        ),
        (
            "owners_8_ingest_aggregate_per_s".to_string(),
            serde_json::json!(at(8).ingest_aggregate.round()),
        ),
        (
            "owners_2_claim_finalize_aggregate_per_s".to_string(),
            serde_json::json!(at(2).drain_aggregate.round()),
        ),
        (
            "owners_4_claim_finalize_aggregate_per_s".to_string(),
            serde_json::json!(at(4).drain_aggregate.round()),
        ),
        (
            "owners_8_claim_finalize_aggregate_per_s".to_string(),
            serde_json::json!(at(8).drain_aggregate.round()),
        ),
        (
            "scale_out_8_vs_2_ingest_multiple".to_string(),
            serde_json::json!((ratio_8_2 * 100.0).round() / 100.0),
        ),
        (
            "scale_multiple_bar".to_string(),
            serde_json::json!(SCALE_MULTIPLE_BAR),
        ),
        (
            "ingest_aggregate_non_decreasing".to_string(),
            serde_json::json!(nondecreasing),
        ),
        (
            "worst_ingest_per_queue_per_s".to_string(),
            serde_json::json!(worst_ingest_per_queue.round()),
        ),
        (
            "worst_claim_finalize_per_queue_per_s".to_string(),
            serde_json::json!(worst_drain_per_queue.round()),
        ),
        (
            "e0_floor_per_s".to_string(),
            serde_json::json!(FLOOR_ITEMS_PER_SEC.round()),
        ),
        (
            "one_owner_per_queue_confirmations".to_string(),
            serde_json::json!(confirmations_at_8),
        ),
        (
            "queues_per_owner".to_string(),
            serde_json::json!(at(8).queues_per_owner),
        ),
        (
            "items_per_queue".to_string(),
            serde_json::json!(at(8).items_per_queue),
        ),
        (
            "conns_per_queue".to_string(),
            serde_json::json!(at(8).conns_per_queue),
        ),
        (
            "segment_max_latency_ms".to_string(),
            serde_json::json!(tuning.segment_max_latency_ms),
        ),
        (
            "segment_target_bytes".to_string(),
            serde_json::json!(tuning.segment_target_bytes),
        ),
        (
            "worker_threads_per_node".to_string(),
            serde_json::json!(tuning.worker_threads_per_node),
        ),
        (
            "server_cpu_limit".to_string(),
            serde_json::json!(tuning.server_cpu_limit),
        ),
        (
            "server_cpu_request".to_string(),
            serde_json::json!(tuning.server_cpu_request),
        ),
        (
            "loadgen_cpu_limit".to_string(),
            serde_json::json!(tuning.loadgen_cpu_limit),
        ),
        (
            "kind_node_image".to_string(),
            serde_json::json!(tuning.kind_node_image),
        ),
        ("sweep".to_string(), serde_json::json!(tuning.sweep)),
        ("cores".to_string(), serde_json::json!(tuning.cores)),
        ("bars_met".to_string(), serde_json::json!(all_pass)),
    ]);
    let row = pqueue_release::LedgerRow {
        suite: "performance_multi_node_object_log_e2_kind".into(),
        command: "scripts/perf/tp002-e2-kind.sh (pqueue-loadgen run -> emit-row; kind: CPU-limited server pods + lean in-cluster load Job)".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: scale.into(),
        seed: 0,
        environment: format!(
            "live multi-node ADR-008 owner cluster on a kind (Kubernetes-in-docker) cluster; \
             {cores} cores; node image {node_image}; owner counts 2/4/8; each owner an independent \
             pqueue-service Deployment(replicas=1)+Service on object_log_sqlite_projection in SEGMENTED \
             group-commit mode (TD-004) with its own object-log root + sqlite projection on an emptyDir \
             medium=Memory tmpfs, distinct PQUEUE_NODE_ID, disjoint PQUEUE_BOOTSTRAP_QUEUES, CPU \
             request={req}/limit={lim}, {worker} worker threads; load driven by a LEAN, SEPARATED \
             in-cluster Job (CPU limit {load}) speaking raw RESP pod->pod over Service ClusterIP to each \
             owner; each queue driven by {conns} concurrent connections",
            cores = tuning.cores,
            node_image = tuning.kind_node_image,
            req = tuning.server_cpu_request,
            lim = tuning.server_cpu_limit,
            worker = tuning.worker_threads_per_node,
            load = tuning.loadgen_cpu_limit,
            conns = at(8).conns_per_queue,
        ),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "E2: ingest aggregate strictly non-decreasing 2->4->8; 8-owner ingest aggregate >= 3.5x 2-owner; worst per-queue ingest AND claim+finalize each >= E0 floor (2777.78/s); no queue served by more than one owner".into(),
        evidence_tier: tier.into(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["E2".into()],
            values,
        },
    };

    let path = std::path::PathBuf::from(&out);
    pqueue_release::append_row(&path, &row).expect("emit ledger row");
    let summary =
        pqueue_release::verify_ledger(&path, true).expect("emitted ledger validates strict");
    let bucket_ok = if all_pass {
        summary.evidence_ids.contains("E2")
    } else {
        summary.smoke_evidence_ids.contains("E2")
    };
    assert!(
        bucket_ok,
        "emitted row must carry the E2 evidence id in the {tier} bucket"
    );
    eprintln!("  emitted {tier}-tier E2 row -> {out}");

    exit(if all_pass { 0 } else { 1 });
}

fn yn(b: bool) -> &'static str {
    if b { "PASS" } else { "FAIL" }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => cmd_run(&args[1..]),
        Some("emit-row") => cmd_emit_row(&args[1..]),
        other => {
            eprintln!(
                "usage:\n  pqueue-loadgen run --spec <json>|--spec-file <path> [--items-per-queue N] \
                 [--conns-per-queue C] [--pipe P] [--batch B]\n  pqueue-loadgen emit-row --result <f> \
                 --result <f> --result <f> --tuning <json> --out <path>\ngot: {other:?}"
            );
            exit(2);
        }
    }
}
