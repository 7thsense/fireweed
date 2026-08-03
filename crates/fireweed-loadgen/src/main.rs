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
//!   verification-ledger row. It records aggregate monotonicity, the 8/2 scale multiple, and absolute rates
//!   as declared-topology capacity observations. Portable release judgment requires canonical topology,
//!   positive measured progress at every point, and exact one-owner-per-queue isolation; it emits
//!   `evidence_tier=release`
//!   ONLY when all bars hold (else `smoke`, never a faked release row), strict-validates the row via
//!   `fireweed_release`, prints the verdict, and exits non-zero unless all bars pass (so a sweep that misses
//!   the bars fails the orchestrator and keeps the bead open).
//!
//! The RESP client is a raw `std::net::TcpStream` client (no new dependency) — the same wire the
//! off-the-shelf-client e2e exercises. Every number is MEASURED, never hard-coded.

use std::collections::HashSet;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Product capacity target, reported for context only. It is not a host-independent pass/fail bar.
const FLOOR_ITEMS_PER_SEC: f64 = fireweed_release::e2::FLOOR_ITEMS_PER_SEC;
/// The E2 headline cross-node multiple: 8-owner aggregate must be at least this times the 2-owner aggregate.
const SCALE_MULTIPLE_BAR: f64 = fireweed_release::e2::SCALE_MULTIPLE_BAR;

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

/// One measured scale point (printed by `run`, consumed by `emit-row`). This IS the shared
/// `fireweed_release::e2::E2ScalePoint` (identical field names + serde shape) so the four-bar judgment is the
/// SAME pure, unit-tested function the gate uses — never a fork.
type RunResult = fireweed_release::e2::E2ScalePoint;

/// Per-node tuning recorded into the evidence row (passed by the orchestrator to `emit-row`). This IS the
/// shared `fireweed_release::e2::E2Tuning`.
type TuningMeta = fireweed_release::e2::E2Tuning;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DensityRunResult {
    hot_items: u64,
    control_items: u64,
    hot_sustain_windows: u64,
    hot_sustain_items: u64,
    hot_connections: usize,
    cold_worker_count: usize,
    seed: u64,
    hot_phase_started_unix_ms: u64,
    hot_phase_ended_unix_ms: u64,
    total_queues: usize,
    cold_queues_active: usize,
    cold_queues_progress_eligible: usize,
    cold_empty_claim_responses: usize,
    hot_accepted_items: u64,
    hot_claimed_items: u64,
    hot_finalized_items: u64,
    cold_accepted_items: u64,
    cold_claimed_items: u64,
    cold_finalized_items: u64,
    cold_pending_items: u64,
    lost_items: u64,
    duplicate_transitions: u64,
    queue_global_progress_violations: u64,
    baseline_before_ingest_per_s: f64,
    baseline_before_claim_finalize_per_s: f64,
    baseline_after_ingest_per_s: f64,
    baseline_after_claim_finalize_per_s: f64,
    baseline_control_ingest_per_s: f64,
    baseline_control_claim_finalize_per_s: f64,
    hot_ingest_per_s: f64,
    hot_claim_finalize_per_s: f64,
    max_progress_latency_ms: u64,
    progress_bound_ms: u64,
    noisy_neighbor_ingest_retention_pct: f64,
    noisy_neighbor_claim_retention_pct: f64,
    duration_seconds: u64,
}

#[derive(Default)]
struct LifecycleIdentityLedger {
    accepted: Mutex<HashSet<String>>,
    claimed: Mutex<HashSet<String>>,
    finalized: Mutex<HashSet<String>>,
    duplicate_transitions: AtomicU64,
}

impl LifecycleIdentityLedger {
    fn record(&self, target: &Mutex<HashSet<String>>, queue: &str, ids: &[String]) {
        let mut seen = target.lock().expect("lifecycle identity ledger poisoned");
        for id in ids {
            if !seen.insert(format!("{queue}\0{id}")) {
                self.duplicate_transitions.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn counts(&self) -> (u64, u64, u64, u64) {
        (
            self.accepted.lock().unwrap().len() as u64,
            self.claimed.lock().unwrap().len() as u64,
            self.finalized.lock().unwrap().len() as u64,
            self.duplicate_transitions.load(Ordering::SeqCst),
        )
    }

    fn snapshots(&self) -> (HashSet<String>, HashSet<String>, HashSet<String>) {
        (
            self.accepted.lock().unwrap().clone(),
            self.claimed.lock().unwrap().clone(),
            self.finalized.lock().unwrap().clone(),
        )
    }

    fn identity_violations(&self, retained_queues: &[String]) -> usize {
        let (accepted, claimed, finalized) = self.snapshots();
        let mut violations = if retained_queues.is_empty() {
            accepted.symmetric_difference(&claimed).count()
                + claimed.symmetric_difference(&finalized).count()
        } else {
            claimed.symmetric_difference(&finalized).count()
                + claimed.difference(&accepted).count()
                + finalized.difference(&accepted).count()
        };
        if !retained_queues.is_empty() {
            let retained: Vec<&String> = accepted.difference(&finalized).collect();
            for queue in retained_queues {
                let prefix = format!("{queue}\0");
                violations += retained
                    .iter()
                    .filter(|identity| identity.starts_with(&prefix))
                    .count()
                    .abs_diff(1);
            }
            violations += retained
                .iter()
                .filter(|identity| {
                    !retained_queues
                        .iter()
                        .any(|queue| identity.starts_with(&format!("{queue}\0")))
                })
                .count();
        }
        violations
    }
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
/// (`-ERR fireweed epoch_stale`) or briefly see no owner (`-ERR ... unavailable`). A real client re-resolves
/// and retries; we do the same (the retry cost stays INSIDE the timed window, so throughput stays honest).
fn is_transient_fence(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    e.contains("epoch_stale") || e.contains("unavailable")
}

/// Push `total` items into `key`, PIPELINED `pipe` at a time. Each item carries a rotating priority. Only
/// SUCCESSFUL `XADD`s count toward `total`; an item fenced by a transient epoch flap is simply re-sent on the
/// next wave (the item payloads are interchangeable, so retry preserves the exact pushed count). A long fence
/// storm trips the guard and fails the run loudly rather than spinning forever.
fn push_items_counted(
    conn: &mut Conn,
    key: &str,
    total: u64,
    pipe: usize,
    completed: Option<&AtomicU64>,
    identities: Option<&LifecycleIdentityLedger>,
) {
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
        let mut accepted_ids = Vec::with_capacity(n);
        for _ in 0..n {
            match conn.recv().expect("read XADD reply") {
                Val::Bulk(Some(id)) => {
                    ok += 1;
                    accepted_ids.push(String::from_utf8(id).expect("XADD id must be UTF-8"));
                }
                Val::Err(e) if is_transient_fence(&e) => {}
                other => panic!("XADD on {key} failed: {other:?}"),
            }
        }
        if let Some(identities) = identities {
            identities.record(&identities.accepted, key, &accepted_ids);
        }
        done += ok;
        if let Some(completed) = completed {
            completed.fetch_add(ok, Ordering::Relaxed);
        }
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

fn push_items(conn: &mut Conn, key: &str, total: u64, pipe: usize) {
    push_items_counted(conn, key, total, pipe, None, None);
}

/// Cooperatively drain `key` (`XREADGROUP >` + `XACK`) until an empty batch. Returns the count drained.
fn drain_counted(
    conn: &mut Conn,
    key: &str,
    consumer: &str,
    batch: usize,
    completed: Option<&AtomicU64>,
    identities: Option<&LifecycleIdentityLedger>,
) -> u64 {
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
        if let Some(identities) = identities {
            identities.record(&identities.claimed, key, &ids);
        }
        let mut args: Vec<&[u8]> = vec![b"XACK", key.as_bytes(), b"g"];
        for ib in &id_bytes {
            args.push(ib);
        }
        // Re-send the XACK until it lands; acking the same ids twice is harmless (idempotent finalize).
        loop {
            conn.send(&args).expect("send XACK");
            match conn.recv().expect("read XACK reply") {
                Val::Int(acked) => {
                    assert_eq!(
                        acked,
                        ids.len() as i64,
                        "XACK must finalize every claimed id on {key}"
                    );
                    break;
                }
                Val::Err(e) if is_transient_fence(&e) => {
                    thread::sleep(Duration::from_millis(10));
                }
                other => panic!("XACK on {key} failed: {other:?}"),
            }
        }
        if let Some(identities) = identities {
            identities.record(&identities.finalized, key, &ids);
        }
        total += ids.len() as u64;
        if let Some(completed) = completed {
            completed.fetch_add(ids.len() as u64, Ordering::Relaxed);
        }
    }
    total
}

fn drain(conn: &mut Conn, key: &str, consumer: &str, batch: usize) -> u64 {
    drain_counted(conn, key, consumer, batch, None, None)
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
fn run_phase<F>(
    queue_keys: &[(String, String)],
    conns_per_queue: usize,
    progress: Option<(&str, u64, Arc<AtomicU64>)>,
    work: F,
) -> (Vec<u64>, f64)
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
    let progress_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let progress_reporter = progress.as_ref().map(|(stage, total, completed)| {
        println!(
            "DENSITY_PROGRESS stage={stage} completed={} total={total} elapsed_ms=0",
            completed.load(Ordering::Relaxed)
        );
        io::stdout().flush().expect("flush density progress");
        let stage = (*stage).to_string();
        let total = *total;
        let completed = Arc::clone(completed);
        let stop = Arc::clone(&progress_stop);
        thread::spawn(move || {
            let mut next_report = Duration::from_secs(5);
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(250));
                if wall_start.elapsed() < next_report {
                    continue;
                }
                println!(
                    "DENSITY_PROGRESS stage={stage} completed={} total={total} elapsed_ms={}",
                    completed.load(Ordering::Relaxed),
                    wall_start.elapsed().as_millis()
                );
                io::stdout().flush().expect("flush density progress");
                next_report += Duration::from_secs(5);
            }
        })
    });
    for h in handles {
        h.join().unwrap();
    }
    let wall = wall_start.elapsed().as_secs_f64();
    progress_stop.store(true, Ordering::Relaxed);
    if let Some(reporter) = progress_reporter {
        reporter.join().expect("density progress reporter");
    }
    if let Some((stage, total, completed)) = &progress {
        println!(
            "DENSITY_PROGRESS stage={stage} completed={} total={total} elapsed_ms={}",
            completed.load(Ordering::Relaxed),
            (wall * 1_000.0).round() as u128
        );
        io::stdout().flush().expect("flush density progress");
    }
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
/// epoch catches up and fence itself (`-ERR fireweed epoch_stale`). A single serial connection per queue
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
    measure_with_progress(
        spec,
        items_per_queue,
        conns_per_queue,
        pipe,
        batch,
        None,
        None,
    )
}

fn measure_with_progress(
    spec: &RunSpec,
    items_per_queue: u64,
    conns_per_queue: usize,
    pipe: usize,
    batch: usize,
    progress_prefix: Option<&str>,
    identities: Option<Arc<LifecycleIdentityLedger>>,
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
    let total_items = num_queues as u64 * items_per_queue;
    let ingest_completed = Arc::new(AtomicU64::new(0));
    let ingest_worker_completed = Arc::clone(&ingest_completed);
    let ingest_identities = identities.clone();
    let ingest_stage = progress_prefix.map(|prefix| format!("{prefix}_INGEST"));
    let ingest_progress = ingest_stage
        .as_deref()
        .map(|stage| (stage, total_items, Arc::clone(&ingest_completed)));
    let (push_ns, ingest_wall) = run_phase(
        &queue_keys,
        conns_per_queue,
        ingest_progress,
        move |c, key, ci| {
            let my_push = per_conn + if ci == 0 { remainder } else { 0 };
            push_items_counted(
                c,
                key,
                my_push,
                pipe,
                Some(&ingest_worker_completed),
                ingest_identities.as_deref(),
            );
        },
    );

    // ---- Phase 2: CLAIM+FINALIZE ----
    let drained_total = Arc::new(AtomicU64::new(0));
    let dt = Arc::clone(&drained_total);
    let drain_completed = Arc::new(AtomicU64::new(0));
    let drain_worker_completed = Arc::clone(&drain_completed);
    let drain_identities = identities;
    let drain_stage = progress_prefix.map(|prefix| format!("{prefix}_CLAIM_FINALIZE"));
    let drain_progress = drain_stage
        .as_deref()
        .map(|stage| (stage, total_items, Arc::clone(&drain_completed)));
    let (drain_ns, drain_wall) = run_phase(
        &queue_keys,
        conns_per_queue,
        drain_progress,
        move |c, key, ci| {
            let consumer = format!("c{ci}");
            let got = drain_counted(
                c,
                key,
                &consumer,
                batch,
                Some(&drain_worker_completed),
                drain_identities.as_deref(),
            );
            dt.fetch_add(got, Ordering::SeqCst);
        },
    );
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
    let total_items = total_items as f64;
    (
        total_items / ingest_wall,
        min_per_queue(&push_ns),
        total_items / drain_wall,
        min_per_queue(&drain_ns),
    )
}

/// Keep every cold queue progress-eligible with a bounded worker pool while the hot queue is measured.
/// Each worker first completes one claim/finalize cycle on every queue assigned to it, then continues
/// cycling those queues until `stop` is set. The returned count is the number of distinct cold queues that
/// proved progress before the hot phase began.
#[derive(Clone)]
struct DensityWorkerSignals {
    stop: Arc<std::sync::atomic::AtomicBool>,
    ready: Arc<AtomicU64>,
    hot_phase: Arc<std::sync::atomic::AtomicBool>,
    hot_seen: Arc<Vec<std::sync::atomic::AtomicBool>>,
    hot_progressed: Arc<AtomicU64>,
    max_progress_latency_ms: Arc<AtomicU64>,
    cold_empty_claim_responses: Arc<AtomicU64>,
    cold_accepted_items: Arc<AtomicU64>,
    cold_claimed_items: Arc<AtomicU64>,
    cold_finalized_items: Arc<AtomicU64>,
    identities: Arc<LifecycleIdentityLedger>,
}

fn start_density_workers(
    addr: &str,
    cold_keys: &[String],
    workers: usize,
    signals: DensityWorkerSignals,
) -> Vec<thread::JoinHandle<()>> {
    let workers = workers.max(1).min(cold_keys.len().max(1));
    (0..workers)
        .map(|worker| {
            let addr = addr.to_string();
            let keys: Vec<(usize, String)> = cold_keys
                .iter()
                .enumerate()
                .skip(worker)
                .step_by(workers)
                .map(|(index, key)| (index, key.clone()))
                .collect();
            let signals = signals.clone();
            thread::spawn(move || {
                let mut conn = Conn::connect(&addr).expect("density cold-worker connect");
                let mut states = Vec::with_capacity(keys.len());
                for (index, key) in keys {
                    push_items_counted(&mut conn, &key, 1, 1, None, Some(&signals.identities));
                    signals.cold_accepted_items.fetch_add(1, Ordering::SeqCst);
                    let got = drain_counted(
                        &mut conn,
                        &key,
                        &format!("density-w{worker}"),
                        1,
                        None,
                        Some(&signals.identities),
                    );
                    if got == 0 {
                        signals
                            .cold_empty_claim_responses
                            .fetch_add(1, Ordering::SeqCst);
                    }
                    assert_eq!(got, 1, "cold queue {key} must claim/finalize its seed");
                    signals.cold_claimed_items.fetch_add(got, Ordering::SeqCst);
                    signals
                        .cold_finalized_items
                        .fetch_add(got, Ordering::SeqCst);
                    let eligible_since = Instant::now();
                    push_items_counted(&mut conn, &key, 1, 1, None, Some(&signals.identities));
                    signals.cold_accepted_items.fetch_add(1, Ordering::SeqCst);
                    states.push((index, key, eligible_since));
                    signals.ready.fetch_add(1, Ordering::SeqCst);
                }
                while !signals.stop.load(Ordering::Relaxed) {
                    for (index, key, eligible_since) in &mut states {
                        if signals.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        // Capture phase at operation START. A claim begun before HOT_START must not be
                        // promoted merely because it finishes after the flag changes.
                        let started_in_hot_phase = signals.hot_phase.load(Ordering::SeqCst);
                        let got = drain_counted(
                            &mut conn,
                            key,
                            &format!("density-w{worker}"),
                            1,
                            None,
                            Some(&signals.identities),
                        );
                        if got == 0 {
                            signals
                                .cold_empty_claim_responses
                                .fetch_add(1, Ordering::SeqCst);
                        }
                        assert_eq!(got, 1, "cold queue {key} must remain progress eligible");
                        signals.cold_claimed_items.fetch_add(got, Ordering::SeqCst);
                        signals
                            .cold_finalized_items
                            .fetch_add(got, Ordering::SeqCst);
                        let latency_ms = eligible_since.elapsed().as_millis() as u64;
                        if started_in_hot_phase && signals.hot_phase.load(Ordering::SeqCst) {
                            signals
                                .max_progress_latency_ms
                                .fetch_max(latency_ms, Ordering::SeqCst);
                            if signals.hot_seen[*index]
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                            {
                                signals.hot_progressed.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        *eligible_since = Instant::now();
                        push_items_counted(&mut conn, key, 1, 1, None, Some(&signals.identities));
                        signals.cold_accepted_items.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect()
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

    // ---- Judge the four E2 bars + build the row via the SHARED, pure, unit-tested judgment ----
    // (`fireweed_release::e2`). This is the SAME function the `fireweed-bench` acceptance test exercises, so the
    // gate and this binary can never disagree on what "release" means.
    let verdict = fireweed_release::e2::evaluate_e2_bars(&results);
    let all_pass = verdict.bars_met;
    let tier = if all_pass { "release" } else { "smoke" };

    eprintln!("\n--- TP-002 E2 sweep {} verdict (kind) ---", tuning.sweep);
    eprintln!(
        "  capacity: non-decreasing ingest 2->4->8 : {} ({:.0} -> {:.0} -> {:.0})",
        yn(verdict.nondecreasing),
        at(2).ingest_aggregate,
        at(4).ingest_aggregate,
        at(8).ingest_aggregate
    );
    eprintln!(
        "  capacity: 8/2 ingest aggregate multiple : {:.2}x (product target {SCALE_MULTIPLE_BAR}x; not a release gate) -> measurement {}",
        verdict.ratio_8_2,
        yn(verdict.scale_pass)
    );
    eprintln!(
        "  capacity: worst per-queue (product target {FLOOR_ITEMS_PER_SEC:.0}/s; not a release gate) : ingest {:.0}/s, claim+finalize {:.0}/s -> progress {}",
        verdict.worst_ingest_per_queue,
        verdict.worst_drain_per_queue,
        yn(verdict.floor_pass)
    );
    eprintln!(
        "  portable gate: one-owner-per-queue : {} of {} expected cross-node 'no such queue' confirmations at 8 owners -> {}",
        verdict.one_owner_confirmations,
        verdict.expected_confirmations,
        yn(verdict.disjoint_pass)
    );
    eprintln!(
        "  ==> headline bars {}",
        if all_pass { "PASS" } else { "NOT MET" }
    );

    let row = fireweed_release::e2::build_e2_row(&results, &tuning, &verdict);

    let path = run_owned_output(&out);
    fireweed_release::append_row(&path, &row).expect("emit ledger row");
    let summary = fireweed_release::verify_ledger(path.path(), true)
        .expect("emitted ledger validates strict");
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

fn cmd_density_run(args: &[String]) -> ! {
    let addr = arg_value(args, "--addr").unwrap_or_else(|| "fireweed:8080".into());
    let queue_prefix = arg_value(args, "--queue-prefix").unwrap_or_else(|| "density:q".into());
    let total_queues = parse_usize_arg(args, "--queue-count", 1001);
    let items = parse_u64_arg(args, "--items", 300_000);
    let control_items = parse_u64_arg(args, "--control-items", 10_000);
    let conns = parse_usize_arg(args, "--hot-connections", 8);
    let pipe = parse_usize_arg(args, "--pipe", 1_000);
    let batch = parse_usize_arg(args, "--batch", 1_000);
    let noisy_workers = parse_usize_arg(args, "--noisy-workers", 8);
    let seed = parse_u64_arg(args, "--seed", 42);
    let progress_bound_ms = parse_u64_arg(args, "--progress-bound-ms", 60_000);
    assert_eq!(
        total_queues, 1001,
        "density workload requires exactly 1001 queues"
    );

    let keys: Vec<String> = (0..total_queues)
        .map(|i| format!("{queue_prefix}{i}"))
        .collect();
    let hot = keys.last().unwrap().clone();
    let mut cold = keys[..keys.len() - 1].to_vec();
    let cold_len = cold.len();
    cold.rotate_left((seed as usize) % cold_len);
    let all_spec = RunSpec {
        owners: 1,
        nodes: vec![NodeSpec {
            addr: addr.clone(),
            queues: keys.clone(),
        }],
    };
    println!("DENSITY_STAGE stage=READINESS status=START");
    io::stdout().flush().expect("flush density readiness stage");
    await_ready(&all_spec);
    println!("DENSITY_STAGE stage=READINESS status=DONE");
    println!(
        "DENSITY_STAGE stage=INVENTORY status=START completed=0 total={}",
        keys.len()
    );
    io::stdout().flush().expect("flush density inventory stage");
    let mut inventory_conn = Conn::connect(&addr).expect("density inventory probe connect");
    for key in &keys {
        assert!(
            xlen(&mut inventory_conn, key)
                .expect("density inventory XLEN")
                .is_ok(),
            "generated queue {key} is absent"
        );
    }
    println!(
        "DENSITY_STAGE stage=INVENTORY status=DONE completed={} total={}",
        keys.len(),
        keys.len()
    );
    io::stdout().flush().expect("flush density inventory stage");

    let hot_spec = RunSpec {
        owners: 1,
        nodes: vec![NodeSpec {
            addr: addr.clone(),
            queues: vec![hot],
        }],
    };
    let started = Instant::now();
    let hot_identities = Arc::new(LifecycleIdentityLedger::default());
    println!("DENSITY_STAGE stage=BASELINE status=START");
    io::stdout().flush().expect("flush density baseline stage");
    let (_, baseline_before_ingest, _, baseline_before_claim) = measure_with_progress(
        &hot_spec,
        control_items,
        conns,
        pipe,
        batch,
        Some("BASELINE"),
        Some(Arc::clone(&hot_identities)),
    );
    println!("DENSITY_STAGE stage=BASELINE status=DONE");
    println!(
        "DENSITY_STAGE stage=COLD_PRIME status=START completed=0 total={}",
        cold.len()
    );
    io::stdout()
        .flush()
        .expect("flush density cold-prime stage");

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = Arc::new(AtomicU64::new(0));
    let hot_phase = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hot_seen = Arc::new(
        (0..cold.len())
            .map(|_| std::sync::atomic::AtomicBool::new(false))
            .collect(),
    );
    let hot_progressed = Arc::new(AtomicU64::new(0));
    let max_progress_latency_ms = Arc::new(AtomicU64::new(0));
    let cold_empty_claim_responses = Arc::new(AtomicU64::new(0));
    let cold_accepted_items = Arc::new(AtomicU64::new(0));
    let cold_claimed_items = Arc::new(AtomicU64::new(0));
    let cold_finalized_items = Arc::new(AtomicU64::new(0));
    let cold_identities = Arc::new(LifecycleIdentityLedger::default());
    let handles = start_density_workers(
        &addr,
        &cold,
        noisy_workers,
        DensityWorkerSignals {
            stop: Arc::clone(&stop),
            ready: Arc::clone(&ready),
            hot_phase: Arc::clone(&hot_phase),
            hot_seen: Arc::clone(&hot_seen),
            hot_progressed: Arc::clone(&hot_progressed),
            max_progress_latency_ms: Arc::clone(&max_progress_latency_ms),
            cold_empty_claim_responses: Arc::clone(&cold_empty_claim_responses),
            cold_accepted_items: Arc::clone(&cold_accepted_items),
            cold_claimed_items: Arc::clone(&cold_claimed_items),
            cold_finalized_items: Arc::clone(&cold_finalized_items),
            identities: Arc::clone(&cold_identities),
        },
    );
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut next_cold_report = Instant::now();
    while ready.load(Ordering::SeqCst) < cold.len() as u64 {
        assert!(
            Instant::now() < deadline,
            "cold queues did not all prove progress within 300s"
        );
        if Instant::now() >= next_cold_report {
            let completed = ready.load(Ordering::SeqCst);
            println!(
                "DENSITY_PROGRESS stage=COLD_PRIME completed={completed} total={}",
                cold.len()
            );
            io::stdout()
                .flush()
                .expect("flush density cold-prime progress");
            next_cold_report += Duration::from_secs(5);
        }
        thread::sleep(Duration::from_millis(50));
    }
    println!(
        "DENSITY_STAGE stage=COLD_PRIME status=DONE completed={} total={}",
        cold.len(),
        cold.len()
    );
    let hot_phase_started_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64;
    hot_phase.store(true, Ordering::SeqCst);
    println!("DENSITY_PHASE HOT_START {hot_phase_started_unix_ms}");
    io::stdout().flush().expect("flush HOT_START marker");
    let (_, loaded_ingest, _, loaded_claim) = measure_with_progress(
        &hot_spec,
        items,
        conns,
        pipe,
        batch,
        Some("LOADED"),
        Some(Arc::clone(&hot_identities)),
    );
    hot_phase.store(false, Ordering::SeqCst);
    let mut hot_sustain_windows = 1_u64;
    while hot_progressed.load(Ordering::SeqCst) < cold.len() as u64 {
        hot_sustain_windows += 1;
        println!(
            "DENSITY_STAGE stage=LOADED_SUSTAIN window={hot_sustain_windows} cold_progress={}/{}",
            hot_progressed.load(Ordering::SeqCst),
            cold.len()
        );
        io::stdout().flush().expect("flush loaded sustain stage");
        hot_phase.store(true, Ordering::SeqCst);
        let _ = measure_with_progress(
            &hot_spec,
            items,
            conns,
            pipe,
            batch,
            Some("LOADED_SUSTAIN"),
            Some(Arc::clone(&hot_identities)),
        );
        hot_phase.store(false, Ordering::SeqCst);
    }
    hot_phase.store(false, Ordering::SeqCst);
    let hot_phase_ended_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64;
    println!("DENSITY_PHASE HOT_END {hot_phase_ended_unix_ms}");
    io::stdout().flush().expect("flush HOT_END marker");
    stop.store(true, Ordering::SeqCst);
    for handle in handles {
        handle.join().expect("density cold worker");
    }

    // Bracket the loaded measurement with a second control window from the same process, deployment,
    // generated inventory, and revision. This A/B/A shape absorbs monotonic host drift without requiring
    // an idle or specially selected machine. Rates remain capacity observations; only exact completion,
    // progress, resource bounds, and a well-formed same-run comparison are release gates.
    println!("DENSITY_STAGE stage=BASELINE_AFTER status=START");
    io::stdout().flush().expect("flush trailing baseline stage");
    let (_, baseline_after_ingest, _, baseline_after_claim) = measure_with_progress(
        &hot_spec,
        control_items,
        conns,
        pipe,
        batch,
        Some("BASELINE_AFTER"),
        Some(Arc::clone(&hot_identities)),
    );
    println!("DENSITY_STAGE stage=BASELINE_AFTER status=DONE");

    let paired_control = |before: f64, after: f64| {
        if before.is_finite() && before > 0.0 && after.is_finite() && after > 0.0 {
            2.0 / (before.recip() + after.recip())
        } else {
            f64::NAN
        }
    };
    let baseline_ingest = paired_control(baseline_before_ingest, baseline_after_ingest);
    let baseline_claim = paired_control(baseline_before_claim, baseline_after_claim);

    let mut cold_active = 0usize;
    let mut cold_pending_items = 0_u64;
    for key in &cold {
        let mut conn = Conn::connect(&addr).expect("density final inventory connect");
        let length = xlen(&mut conn, key)
            .expect("density final XLEN")
            .expect("density final queue must exist");
        cold_pending_items = cold_pending_items
            .checked_add(u64::try_from(length).expect("density XLEN must be non-negative"))
            .expect("density cold pending count overflow");
        if length > 0 {
            cold_active += 1;
        }
    }
    let (hot_accepted, hot_claimed, hot_finalized, hot_duplicates) = hot_identities.counts();
    let (cold_accepted, cold_claimed, cold_finalized, cold_duplicates) = cold_identities.counts();
    assert_eq!(
        cold_accepted_items.load(Ordering::SeqCst),
        cold_accepted,
        "cold accepted identity/count reconciliation"
    );
    assert_eq!(cold_claimed_items.load(Ordering::SeqCst), cold_claimed);
    assert_eq!(cold_finalized_items.load(Ordering::SeqCst), cold_finalized);
    let (hot_accepted_ids, hot_claimed_ids, _) = hot_identities.snapshots();
    let (cold_accepted_ids, cold_claimed_ids, _) = cold_identities.snapshots();
    let hot_identity_violations = hot_identities.identity_violations(&[]);
    let cold_identity_violations = cold_identities.identity_violations(&cold);
    let lost_items = (hot_identity_violations + cold_identity_violations) as u64;
    let progress_eligible = hot_progressed.load(Ordering::SeqCst) as usize;
    let duplicate_transitions = hot_duplicates
        .checked_add(cold_duplicates)
        .and_then(|duplicates| {
            duplicates.checked_add(
                (hot_claimed_ids.difference(&hot_accepted_ids).count()
                    + cold_claimed_ids.difference(&cold_accepted_ids).count())
                    as u64,
            )
        })
        .expect("density duplicate transition count overflow");
    let duration_seconds = started.elapsed().as_secs().max(1);
    let result = DensityRunResult {
        hot_items: items,
        control_items,
        hot_sustain_windows,
        hot_sustain_items: items
            .checked_mul(hot_sustain_windows)
            .expect("density hot sustain item count overflow"),
        hot_connections: conns,
        cold_worker_count: noisy_workers,
        seed,
        hot_phase_started_unix_ms,
        hot_phase_ended_unix_ms,
        total_queues,
        cold_queues_active: cold_active,
        cold_queues_progress_eligible: progress_eligible,
        cold_empty_claim_responses: cold_empty_claim_responses.load(Ordering::SeqCst) as usize,
        hot_accepted_items: hot_accepted,
        hot_claimed_items: hot_claimed,
        hot_finalized_items: hot_finalized,
        cold_accepted_items: cold_accepted,
        cold_claimed_items: cold_claimed,
        cold_finalized_items: cold_finalized,
        cold_pending_items,
        lost_items,
        duplicate_transitions,
        // This is an operation-completion invariant, not an elapsed-time gate: a queue is a
        // violation unless it completed a non-empty claim/finalize wholly inside the hot phase.
        queue_global_progress_violations: cold.len().saturating_sub(progress_eligible) as u64,
        baseline_before_ingest_per_s: baseline_before_ingest,
        baseline_before_claim_finalize_per_s: baseline_before_claim,
        baseline_after_ingest_per_s: baseline_after_ingest,
        baseline_after_claim_finalize_per_s: baseline_after_claim,
        baseline_control_ingest_per_s: baseline_ingest,
        baseline_control_claim_finalize_per_s: baseline_claim,
        hot_ingest_per_s: loaded_ingest,
        hot_claim_finalize_per_s: loaded_claim,
        max_progress_latency_ms: max_progress_latency_ms.load(Ordering::SeqCst),
        progress_bound_ms,
        noisy_neighbor_ingest_retention_pct: loaded_ingest / baseline_ingest * 100.0,
        noisy_neighbor_claim_retention_pct: loaded_claim / baseline_claim * 100.0,
        duration_seconds,
    };
    eprintln!(
        "density baseline ingest={baseline_ingest:.0}/s claim={baseline_claim:.0}/s; loaded ingest={loaded_ingest:.0}/s claim={loaded_claim:.0}/s; cold progress={progress_eligible}/{}; max progress latency={}ms",
        cold.len(),
        result.max_progress_latency_ms
    );
    println!("DENSITY_RESULT {}", serde_json::to_string(&result).unwrap());
    exit(0);
}

fn cmd_density_emit_row(args: &[String]) -> ! {
    let result_path = arg_value(args, "--result").expect("density-emit-row needs --result");
    let raw = std::fs::read_to_string(&result_path).expect("read density result");
    let json = raw
        .lines()
        .find_map(|line| line.strip_prefix("DENSITY_RESULT "))
        .unwrap_or(raw.trim());
    let result: DensityRunResult = serde_json::from_str(json).expect("parse density result");
    let observed_threads = parse_usize_arg(args, "--observed-threads", usize::MAX);
    let thread_limit = parse_usize_arg(args, "--thread-limit", 4);
    let observed_connections = parse_usize_arg(args, "--observed-connections", usize::MAX);
    let connection_limit = parse_usize_arg(args, "--connection-limit", 32);
    let observed_tasks = parse_usize_arg(args, "--observed-tasks", usize::MAX);
    let task_limit = parse_usize_arg(args, "--task-limit", 64);
    let memory_current_bytes = parse_u64_arg(args, "--memory-current-bytes", 0);
    let memory_peak_bytes = parse_u64_arg(args, "--memory-peak-bytes", 0);
    let memory_limit_bytes = parse_u64_arg(args, "--memory-limit-bytes", 0);
    let memory_accounting_source =
        arg_value(args, "--memory-accounting-source").unwrap_or_default();
    let hot_phase_resource_samples = parse_usize_arg(args, "--hot-phase-resource-samples", 0);
    let first_hot_resource_sample_unix_ms =
        parse_u64_arg(args, "--first-hot-resource-sample-ms", 0);
    let last_hot_resource_sample_unix_ms = parse_u64_arg(args, "--last-hot-resource-sample-ms", 0);
    let revision = arg_value(args, "--revision").expect("density-emit-row needs --revision");
    let image_digest =
        arg_value(args, "--image-digest").expect("density-emit-row needs --image-digest");
    let hardware = arg_value(args, "--hardware").expect("density-emit-row needs --hardware");
    let topology = arg_value(args, "--topology").expect("density-emit-row needs --topology");
    let out = arg_value(args, "--out").expect("density-emit-row needs --out");
    let measurement = fireweed_release::density::DensityMeasurement {
        hot_items: result.hot_items,
        control_items: result.control_items,
        hot_sustain_windows: result.hot_sustain_windows,
        hot_sustain_items: result.hot_sustain_items,
        hot_connections: result.hot_connections,
        cold_worker_count: result.cold_worker_count,
        configured_server_workers: observed_threads,
        total_queues: result.total_queues,
        cold_queues_active: result.cold_queues_active,
        cold_queues_progress_eligible: result.cold_queues_progress_eligible,
        hot_ingest_per_s: result.hot_ingest_per_s,
        hot_claim_finalize_per_s: result.hot_claim_finalize_per_s,
        cold_empty_claim_responses: result.cold_empty_claim_responses,
        hot_accepted_items: result.hot_accepted_items,
        hot_claimed_items: result.hot_claimed_items,
        hot_finalized_items: result.hot_finalized_items,
        cold_accepted_items: result.cold_accepted_items,
        cold_claimed_items: result.cold_claimed_items,
        cold_finalized_items: result.cold_finalized_items,
        cold_pending_items: result.cold_pending_items,
        lost_items: result.lost_items,
        duplicate_transitions: result.duplicate_transitions,
        queue_global_progress_violations: result.queue_global_progress_violations,
        baseline_before_ingest_per_s: result.baseline_before_ingest_per_s,
        baseline_before_claim_finalize_per_s: result.baseline_before_claim_finalize_per_s,
        baseline_after_ingest_per_s: result.baseline_after_ingest_per_s,
        baseline_after_claim_finalize_per_s: result.baseline_after_claim_finalize_per_s,
        baseline_control_ingest_per_s: result.baseline_control_ingest_per_s,
        baseline_control_claim_finalize_per_s: result.baseline_control_claim_finalize_per_s,
        max_progress_latency_ms: result.max_progress_latency_ms,
        progress_bound_ms: result.progress_bound_ms,
        noisy_neighbor_ingest_retention_pct: result.noisy_neighbor_ingest_retention_pct,
        noisy_neighbor_claim_retention_pct: result.noisy_neighbor_claim_retention_pct,
        shared_worker_count: observed_threads,
        shared_worker_limit: thread_limit,
        connection_count: observed_connections,
        connection_limit,
        task_count: observed_tasks,
        task_limit,
        memory_current_bytes,
        memory_peak_bytes,
        memory_limit_bytes,
        memory_accounting_source,
        resource_enforcement_active: true,
        hot_phase_resource_samples,
        first_hot_resource_sample_unix_ms,
        last_hot_resource_sample_unix_ms,
        hot_phase_started_unix_ms: result.hot_phase_started_unix_ms,
        hot_phase_ended_unix_ms: result.hot_phase_ended_unix_ms,
    };
    let metadata = fireweed_release::density::DensityMetadata {
        command: "scripts/perf/tp002-e2-density-kind.sh".into(),
        revision,
        topology,
        hardware,
        seed: result.seed,
        duration_seconds: result.duration_seconds,
        queue_activity_definition: fireweed_release::density::QUEUE_ACTIVITY_DEFINITION.into(),
        image_digest,
        clean_revision: true,
    };
    let row = fireweed_release::density::build_release_row(&measurement, &metadata);
    let passed = fireweed_release::density::validate_release_row(&row).is_ok();
    let path = run_owned_output(&out);
    path.delete().expect("clear run-owned density ledger");
    fireweed_release::append_row(&path, &row).expect("append density row");
    fireweed_release::verify_ledger(path.path(), true).expect("density ledger strict-validates");
    println!("DENSITY_ROW {}", row.to_jsonl());
    exit(if passed { 0 } else { 1 });
}

fn yn(b: bool) -> &'static str {
    if b { "PASS" } else { "FAIL" }
}

fn run_owned_output(output: &str) -> fireweed_release::RunOwned {
    let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("resolve repository root for evidence output");
    let path = std::path::PathBuf::from(output);
    let run_root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .expect("evidence output must have an existing external parent directory");
    fireweed_release::RunOwned::new(repository_root, run_root, &path)
        .expect("evidence output must be run-owned and outside the repository")
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => cmd_run(&args[1..]),
        Some("emit-row") => cmd_emit_row(&args[1..]),
        Some("density-run") => cmd_density_run(&args[1..]),
        Some("density-emit-row") => cmd_density_emit_row(&args[1..]),
        other => {
            eprintln!(
                "usage:\n  fireweed-loadgen run --spec <json>|--spec-file <path> [--items-per-queue N] \
                 [--conns-per-queue C] [--pipe P] [--batch B]\n  fireweed-loadgen emit-row --result <f> \
                 --result <f> --result <f> --tuning <json> --out <path>\n  fireweed-loadgen density-run --addr <host:port> --queue-count 1001 [--items N] [--seed N]\n  fireweed-loadgen density-emit-row --result <f> --observed-threads N --observed-connections N --observed-tasks N --memory-current-bytes N --memory-peak-bytes N --memory-limit-bytes N --memory-accounting-source cgroup_v2 --hot-phase-resource-samples N --first-hot-resource-sample-ms N --last-hot-resource-sample-ms N --revision <sha> --image-digest <sha256> --topology <description> --hardware <description> --out <path>\ngot: {other:?}"
            );
            exit(2);
        }
    }
}

#[cfg(test)]
mod density_lifecycle_tests {
    use super::*;

    #[test]
    fn identity_reconciliation_detects_offsetting_loss_and_duplication() {
        let ledger = LifecycleIdentityLedger::default();
        ledger.record(&ledger.accepted, "q", &["1".into(), "2".into()]);
        ledger.record(&ledger.claimed, "q", &["1".into(), "1".into()]);
        ledger.record(&ledger.finalized, "q", &["1".into(), "1".into()]);

        let (accepted, claimed, finalized, duplicates) = ledger.counts();
        assert_eq!((accepted, claimed, finalized), (2, 1, 1));
        assert_eq!(duplicates, 2);
        assert_eq!(
            accepted - finalized,
            1,
            "the missing identity remains visible"
        );
    }

    #[test]
    fn identity_reconciliation_detects_equal_count_phantom_substitution() {
        let ledger = LifecycleIdentityLedger::default();
        ledger.record(&ledger.accepted, "q", &["1".into(), "2".into()]);
        ledger.record(&ledger.claimed, "q", &["1".into(), "3".into()]);
        ledger.record(&ledger.finalized, "q", &["1".into(), "3".into()]);

        assert_eq!(ledger.counts(), (2, 2, 2, 0), "cardinalities alone cancel");
        assert!(ledger.identity_violations(&[] as &[String]) > 0);
    }
}
