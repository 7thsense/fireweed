//! TP-002 **E2 — cross-queue scale-out, the LIVE multi-node HEADLINE** (ADR-008: the queue is the unit of
//! sharding; horizontal scale comes from distributing queues across INDEPENDENT, shared-nothing owner nodes,
//! NOT from intra-queue sharding). This is the REAL multi-node run that the in-process memory smoke suite
//! (`performance_cross_queue_scale_out_tests`) deliberately DEFERS to — read that suite's module doc: it
//! substantiates only the ARCHITECTURAL owner-independence property on one in-memory node and must NOT green
//! the E2 headline. THIS suite is the headline evidence.
//!
//! TOPOLOGY (a real ADR-008 multi-node cluster of independent owners, as DOCKER CONTAINERS on the bridge).
//! Each owner node is an independent `pqueue-service` process running in its own `ubuntu:25.04` container on
//! the docker bridge network, on the FAST `object_log_sqlite_projection` backend in **segmented group-commit
//! mode** (`PQUEUE_LOG_BACKEND=objectlog` + `PQUEUE_PROJECTION_BACKEND=sqlite` +
//! `PQUEUE_OBJECT_LOG_MODE=segmented` → the local-object-log authority with a group-committing segmented
//! substrate plus a SQLite materialized projection, TD-004). Shared-nothing: a distinct `PQUEUE_NODE_ID`, its
//! OWN object-log root + sqlite projection on a per-container `tmpfs /data`, its own `0.0.0.0:8080` listener,
//! and a DISJOINT `PQUEUE_BOOTSTRAP_QUEUES` set (each node owns its own M queues; no queue lives on two
//! nodes). The driver (this cargo-test host process) reaches each node directly at its **container IP:8080**
//! over real bridge TCP — host-process → container, the ADR-008 ownership model with no shared store, no
//! shared lock, no shared projection.
//!
//! WHY CONTAINERS, NOT HOST PROCESSES. An earlier revision spawned nodes as host processes on `127.0.0.1`;
//! in this sandbox/orbstack environment a SUSTAINED loopback RESP connection is killed with signal 16 once
//! load ramps, so a benchmark over loopback cannot complete. Container bridge-IP traffic survives indefinitely
//! (proven by the 36-min postgres run). The nodes therefore run as bridge containers and the driver speaks
//! RESP to each container IP.
//!
//! LOAD DRIVER: a raw RESP client over `std::net::TcpStream` (no new dependency — the same wire the
//! off-the-shelf-client e2e exercises: `XADD`=push, `XREADGROUP >`=priority claim, `XACK`=finalize-complete).
//! Each queue is driven by MANY concurrent RESP connections (a real owner has many workers): the concurrent
//! pushers co-buffer into the same segment, which is exactly how the group-commit substrate reaches its
//! per-queue throughput (one closed-loop connection only ever has one command in flight per seal). All
//! connections across every node run CONCURRENTLY on real OS threads, barrier-released together so the
//! wall-clock reflects genuine parallel execution.
//!
//! MEASURE + HARD-FAIL. We run at owner counts 2 / 4 / 8 with fixed per-owner queues and a fixed per-owner
//! item budget (so total work scales with owners). We measure AGGREGATE throughput (total items / wall-clock)
//! and the WORST single-queue throughput, then HARD-FAIL unless ALL of:
//!   1. aggregate is STRICTLY NON-DECREASING across 2 → 4 → 8,
//!   2. 8-owner aggregate >= 3.5x the 2-owner aggregate,
//!   3. worst per-queue throughput >= 2777.78 items/s (the E0 per-queue floor = 10,000,000/hr),
//!   4. NO queue is served by more than one owner (probed live: each queue answers `XLEN` with an integer on
//!      its owner and `-ERR no such queue` on every other node — pairwise-disjoint ownership).
//!
//! Every number is MEASURED, never hard-coded.
//!
//! EVIDENCE. On a passing run this emits `evidence_tier=release` ledger rows
//! (`backend_profile="object_log_sqlite_projection"`, `measurements.tp002_evidence_ids=["E2"]`,
//! `scale="release"`) and self-validates them strict (mirroring the postgres single-deployment baseline). If
//! the bars are NOT met on this box (a core / I-O ceiling), the row is emitted SMOKE-tier (honest, never a
//! faked release row) AND the test still hard-fails so the bead stays open with the measured ceiling visible.
//!
//! ENV-GATED. Self-skips (LOUD) without `PQUEUE_E2_MULTINODE=1`, so a routine `cargo test` is short and never
//! spins up an 8-node container cluster. To run the headline:
//!   cargo build -p pqueue-server --release --bin pqueue-service
//!   PQUEUE_E2_MULTINODE=1 cargo test --manifest-path crates/pqueue-bench/Cargo.toml \
//!     --test performance_multi_node_object_log_e2_tests -- --nocapture
//! Tunables (env, all optional): PQUEUE_E2_QUEUES_PER_OWNER (default 1), PQUEUE_E2_ITEMS_PER_QUEUE
//! (default 12000), PQUEUE_E2_CONNS_PER_QUEUE (default 8), PQUEUE_E2_PIPE (default 1000),
//! PQUEUE_E2_BATCH (default 1000), PQUEUE_E2_SEGMENT_MAX_LATENCY_MS (default 1),
//! PQUEUE_E2_SEGMENT_TARGET_BYTES (default 262144), PQUEUE_E2_WORKER_THREADS (default 4 per container),
//! PQUEUE_E2_IMAGE (default ubuntu:25.04), PQUEUE_SERVICE_BIN (default <repo>/target/release/pqueue-service).

use std::collections::BTreeMap;
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;
/// The E2 headline cross-node multiple: 8-owner aggregate must be at least this times the 2-owner aggregate.
const SCALE_MULTIPLE_BAR: f64 = 3.5;
/// The RESP port every node container listens on (bridge IP : this port).
const NODE_PORT: u16 = 8080;

// ----------------------------------------------------------------------------------------------------
// Raw RESP client over std::net::TcpStream (no new dependency).
// ----------------------------------------------------------------------------------------------------

/// A parsed RESP2 value (only the shapes this driver consumes). Some variants' payloads are carried for
/// completeness of the protocol parse but not inspected by this driver (e.g. `Simple` for `+OK`/`+PONG`).
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

/// `XLEN key` → `Ok(n)` on the owner (queue known), `Err(reply)` when the queue is unknown to this node
/// (`-ERR no such queue`) — the live probe behind the one-owner-per-queue assertion.
fn xlen(conn: &mut Conn, key: &str) -> io::Result<Result<i64, String>> {
    conn.send(&[b"XLEN", key.as_bytes()])?;
    Ok(match conn.recv()? {
        Val::Int(n) => Ok(n),
        Val::Err(e) => Err(e),
        other => Err(format!("unexpected XLEN reply {other:?}")),
    })
}

/// Pull every item id out of an `XREADGROUP` reply (`[ [stream, [ [id, [field value …]] … ]] … ]`).
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

/// Push `total` items into `key`, PIPELINED `pipe` at a time (write a batch of `XADD`s, then read their
/// replies) so round-trips are amortized. Each item carries a rotating priority.
fn push_items(conn: &mut Conn, key: &str, total: u64, pipe: usize) {
    let mut done = 0u64;
    while done < total {
        let n = ((total - done) as usize).min(pipe.max(1));
        let mut buf = Vec::new();
        for k in 0..n {
            let p = ((done + k as u64) % 1000).to_string();
            encode(
                &mut buf,
                &[b"XADD", key.as_bytes(), b"*", b"priority", p.as_bytes()],
            );
        }
        conn.w.write_all(&buf).expect("write XADD batch");
        for _ in 0..n {
            match conn.recv().expect("read XADD reply") {
                Val::Bulk(Some(_)) => {}
                other => panic!("XADD on {key} failed: {other:?}"),
            }
        }
        done += n as u64;
    }
}

/// Cooperatively drain `key` via repeated `XREADGROUP > COUNT batch` + `XACK` until this consumer reads an
/// empty batch (every item is then acked or delivered-pending-ack to some sibling consumer). `consumer`
/// distinguishes this connection in the group. Returns the count THIS connection drained+acked. The caller
/// sums across the queue's connections and asserts the total equals the queue's pushed budget.
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
        let ids = extract_ids(&reply);
        if ids.is_empty() {
            break;
        }
        let id_bytes: Vec<Vec<u8>> = ids.iter().map(|s| s.clone().into_bytes()).collect();
        let mut args: Vec<&[u8]> = vec![b"XACK", key.as_bytes(), b"g"];
        for ib in &id_bytes {
            args.push(ib);
        }
        conn.send(&args).expect("send XACK");
        match conn.recv().expect("read XACK reply") {
            Val::Int(_) => {}
            other => panic!("XACK on {key} failed: {other:?}"),
        }
        total += ids.len() as u64;
    }
    total
}

// ----------------------------------------------------------------------------------------------------
// The bridge-container owner cluster.
// ----------------------------------------------------------------------------------------------------

/// Per-container segment + runtime tuning (passed into every node's env, recorded into the evidence).
#[derive(Clone)]
struct NodeTuning {
    segment_target_bytes: usize,
    segment_max_latency_ms: u64,
    worker_threads: usize,
    image: String,
    bin: PathBuf,
}

/// One owner node: its container name, bridge address (`IP:8080`), and the FULL queue keys it owns (disjoint
/// from every other node).
struct Node {
    name: String,
    addr: String,
    owned: Vec<String>,
}

/// A spawned cluster of independent owner CONTAINERS. Drop force-removes every container so a panicking test
/// never leaks a running node.
struct Cluster {
    nodes: Vec<Node>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &self.nodes {
            let _ = Command::new("docker").args(["rm", "-f", &n.name]).output();
        }
    }
}

/// Read a container's bridge IP (`docker inspect`), retrying briefly while networking attaches.
fn container_ip(name: &str) -> Option<String> {
    for _ in 0..50 {
        let out = Command::new("docker")
            .args([
                "inspect",
                name,
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            ])
            .output()
            .expect("docker inspect");
        let ip = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !ip.is_empty() {
            return Some(ip);
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
}

/// Launch `owner_count` independent owner containers, each owning `queues_per_owner` globally-unique queues
/// (so ownership is disjoint by construction; the live XLEN probe then PROVES each node serves only its set).
fn spawn_cluster(
    tag: &str,
    owner_count: usize,
    queues_per_owner: usize,
    tuning: &NodeTuning,
) -> Cluster {
    let svc = tuning.bin.to_string_lossy().into_owned();
    let mount = format!("{svc}:/svc:ro");
    let mut nodes = Vec::with_capacity(owner_count);
    for idx in 0..owner_count {
        let name = format!("pqe2-{tag}-o{owner_count}-n{idx}");
        // Best-effort remove a leftover with the same name from an aborted prior run.
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();
        let owned: Vec<String> = (0..queues_per_owner)
            .map(|j| format!("t1:o{owner_count}n{idx}q{j}"))
            .collect();
        let bootstrap = owned.join(",");
        let status = Command::new("docker")
            .args([
                "run", "-d", "--name", &name, "--tmpfs", "/data", "-v", &mount,
            ])
            .args(["-e", "PQUEUE_LOG_BACKEND=objectlog"])
            .args(["-e", "PQUEUE_PROJECTION_BACKEND=sqlite"])
            .args(["-e", "PQUEUE_OBJECT_LOG_MODE=segmented"])
            .args([
                "-e",
                &format!(
                    "PQUEUE_SEGMENT_TARGET_BYTES={}",
                    tuning.segment_target_bytes
                ),
            ])
            .args([
                "-e",
                &format!(
                    "PQUEUE_SEGMENT_MAX_LATENCY_MS={}",
                    tuning.segment_max_latency_ms
                ),
            ])
            .args([
                "-e",
                &format!("PQUEUE_WORKER_THREADS={}", tuning.worker_threads),
            ])
            .args(["-e", &format!("PQUEUE_NODE_ID={}", idx + 1)])
            .args(["-e", "PQUEUE_OBJECT_LOG_ROOT=/data/olog"])
            .args(["-e", "PQUEUE_SQLITE_PROJECTION_PATH=/data/proj.db"])
            .args(["-e", &format!("PQUEUE_LISTEN_ADDR=0.0.0.0:{NODE_PORT}")])
            .args(["-e", &format!("PQUEUE_BOOTSTRAP_QUEUES={bootstrap}")])
            .args(["-e", "PQUEUE_RECLAIM_INTERVAL_MS=60000"])
            .args([&tuning.image, "/svc"])
            .output()
            .expect("docker run");
        assert!(
            status.status.success(),
            "docker run for {name} failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        nodes.push(Node {
            name: name.clone(),
            addr: String::new(),
            owned,
        });
        // Resolve the bridge IP now (the driver connects to IP:8080).
        let ip = container_ip(&name)
            .unwrap_or_else(|| panic!("could not read bridge IP for container {name}"));
        nodes.last_mut().unwrap().addr = format!("{ip}:{NODE_PORT}");
    }
    Cluster { nodes }
}

/// Block until every node answers an `XLEN` of its first owned queue with an integer (bootstrap complete), or
/// panic after the deadline (dumping the container's logs so a boot failure is visible, not a silent hang).
fn await_ready(cluster: &Cluster) {
    let deadline = Instant::now() + Duration::from_secs(60);
    for n in &cluster.nodes {
        loop {
            if let Ok(mut c) = Conn::connect(&n.addr)
                && let Ok(Ok(_)) = xlen(&mut c, &n.owned[0])
            {
                break;
            }
            if Instant::now() > deadline {
                let logs = Command::new("docker")
                    .args(["logs", &n.name])
                    .output()
                    .map(|o| {
                        format!(
                            "{}{}",
                            String::from_utf8_lossy(&o.stdout),
                            String::from_utf8_lossy(&o.stderr)
                        )
                    })
                    .unwrap_or_default();
                panic!(
                    "node {} ({}) not ready within deadline; container logs:\n{logs}",
                    n.name, n.addr
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

/// PROVE one-owner-per-queue (TP-002 E2 bar 4): for every queue, exactly the owner node answers `XLEN` with an
/// integer and EVERY other node rejects it as unknown (`-ERR no such queue`). Returns the number of
/// cross-node "unknown" confirmations (each is a distinct piece of disjoint-ownership evidence).
fn assert_one_owner_per_queue(cluster: &Cluster) -> usize {
    let mut cross_node_unknown = 0usize;
    let all: Vec<(usize, &String)> = cluster
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(i, n)| n.owned.iter().map(move |q| (i, q)))
        .collect();
    for (owner_i, q) in &all {
        for (j, node) in cluster.nodes.iter().enumerate() {
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
                    "queue {q} must be UNKNOWN on non-owner node {j} ({}) — one queue cannot exceed one owner; got {err:?}",
                    node.addr
                );
                cross_node_unknown += 1;
            }
        }
    }
    cross_node_unknown
}

/// One scale point. The INGEST (push/accept) phase and the CLAIM+FINALIZE (drain) phase are measured
/// SEPARATELY — mirroring the postgres E0 release baseline, which reports ingest and claim+finalize each
/// against the per-queue floor rather than a combined lifecycle rate. `*_aggregate` = total items / that
/// phase's wall-clock across all owners; `*_min_per_queue` = the slowest single queue in that phase.
struct ScalePoint {
    owners: usize,
    ingest_aggregate: f64,
    ingest_min_per_queue: f64,
    drain_aggregate: f64,
    drain_min_per_queue: f64,
}

/// Run ONE phase across every queue's connections and return `(per_queue_max_ns, wall_seconds)`. The phase
/// runs as an independent batch of threads: each connection opens its own socket, all rendezvous at a single
/// START barrier (so the timed window excludes connect setup and reflects genuine parallel execution), do
/// `work`, and exit; the MAIN thread (also at the barrier) times from release to the last join. Splitting the
/// run into two SEQUENTIAL spawn→barrier→work→join phases — rather than one batch fenced by a post-work
/// barrier — is deliberate: the only barrier is BEFORE the work, so a worker that fails mid-`work` propagates
/// through `join().unwrap()` as a clean test failure instead of deadlocking a post-work rendezvous.
fn run_phase<F>(queue_keys: &[(&str, &str)], conns_per_queue: usize, work: F) -> (Vec<u64>, f64)
where
    F: Fn(&mut Conn, &str, usize, &AtomicU64) + Send + Sync + 'static,
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
            let addr = addr.to_string();
            let key = key.to_string();
            let barrier = Arc::clone(&barrier);
            let per_queue_ns = Arc::clone(&per_queue_ns);
            let work = Arc::clone(&work);
            handles.push(thread::spawn(move || {
                // Connect (pay TCP setup) BEFORE the barrier so the timed window is pure work.
                let mut c = Conn::connect(&addr).expect("driver connect");
                barrier.wait();
                let t = Instant::now();
                work(&mut c, &key, ci, &per_queue_ns[qidx]);
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

/// Drive the whole cluster in two SEQUENTIAL, independently-timed phases, exactly the two quantities the
/// postgres E0 release baseline holds to the per-queue floor: (1) INGEST — every connection pushes its even
/// share of its queue's `items_per_queue` budget; phase 1 fully joins (so every item is durably accepted)
/// before phase 2 starts. (2) CLAIM+FINALIZE — every connection cooperatively drains (`XREADGROUP >` +
/// `XACK`). Per-phase aggregate = total items / that phase's wall-clock; per-phase per-queue rate = its budget
/// / its slowest connection's phase time; worst = the slowest single queue in that phase. Every value is
/// measured.
fn measure(
    cluster: &Cluster,
    items_per_queue: u64,
    conns_per_queue: usize,
    pipe: usize,
    batch: usize,
) -> ScalePoint {
    let conns_per_queue = conns_per_queue.max(1);
    let queue_keys: Vec<(&str, &str)> = cluster
        .nodes
        .iter()
        .flat_map(|n| n.owned.iter().map(move |q| (n.addr.as_str(), q.as_str())))
        .collect();
    let num_queues = queue_keys.len();
    let per_conn = items_per_queue / conns_per_queue as u64;
    let remainder = items_per_queue - per_conn * conns_per_queue as u64;

    // ---- Phase 1: INGEST (push / accept) ----
    let (push_ns, ingest_wall) = run_phase(&queue_keys, conns_per_queue, move |c, key, ci, _q| {
        // The first connection of each queue carries any remainder so the queue's budget is exact.
        let my_push = per_conn + if ci == 0 { remainder } else { 0 };
        push_items(c, key, my_push, pipe);
    });

    // ---- Phase 2: CLAIM+FINALIZE (cooperative drain) ----
    let drained_total = Arc::new(AtomicU64::new(0));
    let dt = Arc::clone(&drained_total);
    let (drain_ns, drain_wall) = run_phase(&queue_keys, conns_per_queue, move |c, key, ci, _q| {
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
    ScalePoint {
        owners: cluster.nodes.len(),
        ingest_aggregate: total_items / ingest_wall,
        ingest_min_per_queue: min_per_queue(&push_ns),
        drain_aggregate: total_items / drain_wall,
        drain_min_per_queue: min_per_queue(&drain_ns),
    }
}

fn locate_binary() -> PathBuf {
    if let Ok(p) = env::var("PQUEUE_SERVICE_BIN") {
        return PathBuf::from(p);
    }
    // crates/pqueue-bench/../../target/release/pqueue-service == repo-root target.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/release/pqueue-service")
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn performance_multi_node_object_log_e2_tests() {
    if env::var("PQUEUE_E2_MULTINODE").is_err() {
        eprintln!(
            "TP-002 E2 LIVE MULTI-NODE object_log_sqlite_projection SKIPPED — set PQUEUE_E2_MULTINODE=1 (and \
             build the service: `cargo build -p pqueue-server --release --bin pqueue-service`) to run the \
             headline cross-queue scale-out at owner counts 2/4/8 as bridge containers. The >=3.5x-at-8 \
             multiple + worst-per-queue floor evidence is DEFERRED (not measured), never a hidden pass."
        );
        return;
    }

    let bin = locate_binary();
    assert!(
        bin.exists(),
        "pqueue-service binary not found at {} — build it first: \
         `cargo build -p pqueue-server --release --bin pqueue-service` (or set PQUEUE_SERVICE_BIN)",
        bin.display()
    );
    // The driver shells out to docker; fail loudly (not as a benchmark miss) if it is unavailable.
    let docker_ok = Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(
        docker_ok,
        "docker is required for the containerized E2 cluster but `docker version` failed"
    );

    // Defaults are the operating point tuned for THIS 12-core box (see the evidence doc): one queue per
    // owner; each queue driven by a few large-pipeline connections (keeps the driver thread count low so the
    // 8 co-located node containers are not starved); 4 worker threads per node (enough that the force-sealed
    // claim+finalize path keeps its per-queue floor at 8 owners, few enough that the box scales near-linearly
    // 2->8). Override any via env to re-tune on different hardware.
    let queues_per_owner = env_usize("PQUEUE_E2_QUEUES_PER_OWNER", 1);
    let items_per_queue = env_u64("PQUEUE_E2_ITEMS_PER_QUEUE", 12_000);
    let conns_per_queue = env_usize("PQUEUE_E2_CONNS_PER_QUEUE", 8);
    let pipe = env_usize("PQUEUE_E2_PIPE", 1_000);
    let batch = env_usize("PQUEUE_E2_BATCH", 1_000);
    let tuning = NodeTuning {
        segment_target_bytes: env_usize("PQUEUE_E2_SEGMENT_TARGET_BYTES", 262_144),
        segment_max_latency_ms: env_u64("PQUEUE_E2_SEGMENT_MAX_LATENCY_MS", 1),
        worker_threads: env_usize("PQUEUE_E2_WORKER_THREADS", 4),
        image: env::var("PQUEUE_E2_IMAGE").unwrap_or_else(|_| "ubuntu:25.04".to_string()),
        bin: bin.clone(),
    };
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let tag = std::process::id().to_string();
    let counts = [2usize, 4, 8];

    println!(
        "\nTP-002 E2 LIVE multi-node object_log_sqlite_projection (segmented) scale-out — bridge containers; \
         {cores} cores; queues/owner={queues_per_owner}, items/queue={items_per_queue}, \
         conns/queue={conns_per_queue}, seg_target_bytes={}, seg_max_latency_ms={}, worker_threads/node={}",
        tuning.segment_target_bytes, tuning.segment_max_latency_ms, tuning.worker_threads
    );
    println!(
        "  owners | ingest agg items/s | worst ingest/q | claim+final agg | worst claim+final/q"
    );

    let mut points: Vec<ScalePoint> = Vec::new();
    let mut cross_node_unknown_at_8 = 0usize;
    for &n in &counts {
        let cluster = spawn_cluster(&tag, n, queues_per_owner, &tuning);
        await_ready(&cluster);
        // Live one-owner-per-queue proof at every scale point (bar 4); keep the 8-owner count for the row.
        let unknown = assert_one_owner_per_queue(&cluster);
        if n == 8 {
            cross_node_unknown_at_8 = unknown;
        }
        let p = measure(&cluster, items_per_queue, conns_per_queue, pipe, batch);
        println!(
            "  {:>6} | {:>18.0} | {:>14.0} | {:>15.0} | {:>19.0}",
            p.owners,
            p.ingest_aggregate,
            p.ingest_min_per_queue,
            p.drain_aggregate,
            p.drain_min_per_queue
        );
        points.push(p);
        // Cluster dropped here → every container force-removed before the next scale point spins up.
    }

    let at = |n: usize| points.iter().find(|p| p.owners == n).unwrap();

    // ---- Evaluate the four E2 bars (every value measured). The headline cross-queue scale-out is the
    // INGEST (accept) throughput, exactly the E0 "accepted items/hr" quantity the floor is defined in. ----
    // (1) STRICTLY NON-DECREASING ingest aggregate across 2 → 4 → 8.
    let nondecreasing = at(4).ingest_aggregate >= at(2).ingest_aggregate
        && at(8).ingest_aggregate >= at(4).ingest_aggregate;
    // (2) 8-owner ingest aggregate >= 3.5x the 2-owner ingest aggregate.
    let ratio_8_2 = at(8).ingest_aggregate / at(2).ingest_aggregate;
    let scale_pass = ratio_8_2 >= SCALE_MULTIPLE_BAR;
    // (3) worst single-queue throughput across ALL scale points >= the E0 floor, for BOTH ingest AND
    // claim+finalize (the two metrics the postgres E0 baseline holds to the same floor).
    let worst_ingest_per_queue = points
        .iter()
        .map(|p| p.ingest_min_per_queue)
        .fold(f64::INFINITY, f64::min);
    let worst_drain_per_queue = points
        .iter()
        .map(|p| p.drain_min_per_queue)
        .fold(f64::INFINITY, f64::min);
    let worst_per_queue = worst_ingest_per_queue.min(worst_drain_per_queue);
    let floor_pass = worst_per_queue >= FLOOR_ITEMS_PER_SEC;
    // (4) one-owner-per-queue: assert_one_owner_per_queue already HARD-asserts it (panics on violation); a
    // non-zero confirmation count means the disjoint-ownership probe actually ran with teeth.
    let disjoint_pass = cross_node_unknown_at_8 > 0 && queues_per_owner > 0;

    let all_pass = nondecreasing && scale_pass && floor_pass && disjoint_pass;

    println!(
        "\n  (1) non-decreasing ingest 2->4->8 : {} ({:.0} -> {:.0} -> {:.0})",
        yn(nondecreasing),
        at(2).ingest_aggregate,
        at(4).ingest_aggregate,
        at(8).ingest_aggregate
    );
    println!(
        "  (2) 8/2 ingest aggregate multiple : {ratio_8_2:.2}x (bar >= {SCALE_MULTIPLE_BAR}x) -> {}",
        yn(scale_pass)
    );
    println!(
        "  (3) worst per-queue (floor {FLOOR_ITEMS_PER_SEC:.0}/s) : ingest {worst_ingest_per_queue:.0}/s, claim+finalize {worst_drain_per_queue:.0}/s -> {}",
        yn(floor_pass)
    );
    println!(
        "  (4) one-owner-per-queue    : {cross_node_unknown_at_8} cross-node 'no such queue' confirmations at 8 owners -> {}",
        yn(disjoint_pass)
    );
    println!(
        "  ==> headline bars {} on this box",
        if all_pass { "PASS" } else { "NOT MET" }
    );
    if !all_pass {
        eprintln!(
            "NOTE: the TP-002 E2 release bars were NOT met on this box — emitted as SMOKE evidence (never a \
             faked release row). The measured ceiling is recorded; the bead stays open. This is honest \
             evidence, not a hidden pass."
        );
    }

    // ---- Emit the E2 ledger row from the REAL measured values (release-tier ONLY when all bars pass) ----
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
            serde_json::json!(cross_node_unknown_at_8),
        ),
        (
            "queues_per_owner".to_string(),
            serde_json::json!(queues_per_owner),
        ),
        (
            "items_per_queue".to_string(),
            serde_json::json!(items_per_queue),
        ),
        (
            "conns_per_queue".to_string(),
            serde_json::json!(conns_per_queue),
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
            serde_json::json!(tuning.worker_threads),
        ),
        ("cores".to_string(), serde_json::json!(cores)),
        ("bars_met".to_string(), serde_json::json!(all_pass)),
    ]);
    let row = pqueue_release::LedgerRow {
        suite: "performance_multi_node_object_log_e2_tests".into(),
        command: "PQUEUE_E2_MULTINODE=1 cargo test --manifest-path crates/pqueue-bench/Cargo.toml --test performance_multi_node_object_log_e2_tests".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: scale.into(),
        seed: 0,
        environment: format!(
            "live multi-node cluster of independent ADR-008 owners as docker bridge containers on \
             object_log_sqlite_projection in SEGMENTED group-commit mode (TD-004); {cores} cores; owner counts \
             2/4/8; each owner an independent pqueue-service container with its own object-log root + sqlite \
             projection on tmpfs + disjoint bootstrap queues + {worker} worker threads; each queue driven by \
             {conns} concurrent RESP connections; driver speaks raw RESP over bridge TCP to each container IP",
            worker = tuning.worker_threads,
            conns = conns_per_queue,
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
    emit_and_verify(
        "performance_multi_node_object_log_e2_tests",
        &row,
        "E2",
        all_pass,
    );

    // ---- Hard-fail unless ALL bars hold (a non-pass keeps the bead open, with the smoke row already on disk) ----
    assert!(
        nondecreasing,
        "E2 bar (1): ingest aggregate must be strictly non-decreasing across 2->4->8: {:.0} -> {:.0} -> {:.0}",
        at(2).ingest_aggregate,
        at(4).ingest_aggregate,
        at(8).ingest_aggregate
    );
    assert!(
        scale_pass,
        "E2 bar (2): 8-owner ingest aggregate must be >= {SCALE_MULTIPLE_BAR}x the 2-owner ingest aggregate, measured {ratio_8_2:.2}x"
    );
    assert!(
        floor_pass,
        "E2 bar (3): worst per-queue throughput must be >= the E0 floor ({FLOOR_ITEMS_PER_SEC:.0}/s) for BOTH ingest (measured {worst_ingest_per_queue:.0}/s) and claim+finalize (measured {worst_drain_per_queue:.0}/s)"
    );
    assert!(
        disjoint_pass,
        "E2 bar (4): one-owner-per-queue must be live-proven (cross-node 'no such queue' confirmations > 0)"
    );
}

fn yn(b: bool) -> &'static str {
    if b { "PASS" } else { "FAIL" }
}

/// Write `row` to its `<suite>.jsonl` ledger and assert it round-trips strict validation, carrying the E2 id
/// under the RIGHT tier bucket (release rows count toward the headline; smoke rows are recorded but never
/// satisfy a release gate). Mirrors the postgres single-deployment baseline's emit+self-validate.
fn emit_and_verify(suite: &str, row: &pqueue_release::LedgerRow, evidence_id: &str, release: bool) {
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), suite);
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, row).expect("emit ledger row");
    let summary = pqueue_release::verify_ledger(&path, true).expect("emitted row validates strict");
    let seen = if release {
        summary.evidence_ids.contains(evidence_id)
    } else {
        summary.smoke_evidence_ids.contains(evidence_id)
    };
    assert!(
        seen,
        "emitted {} row must carry the {evidence_id} evidence id",
        if release { "release" } else { "smoke" }
    );
}
