//! TP-002 **E2 — cross-queue scale-out, the LIVE multi-node HEADLINE** (ADR-008: the queue is the unit of
//! sharding; horizontal scale comes from distributing queues across INDEPENDENT, shared-nothing owner nodes,
//! NOT from intra-queue sharding). This is the REAL multi-node run that the in-process memory smoke suite
//! (`performance_cross_queue_scale_out_tests`) deliberately DEFERS to — read that suite's module doc: it
//! substantiates only the ARCHITECTURAL owner-independence property on one in-memory node and must NOT green
//! the E2 headline. THIS suite is the headline evidence.
//!
//! TOPOLOGY (a real ADR-008 multi-node cluster of independent owners, as HOST processes). Each owner node is
//! an independent `pqueue-service` OS process on the `object_log_sqlite_projection` backend
//! (`PQUEUE_LOG_BACKEND=objectlog` + `PQUEUE_PROJECTION_BACKEND=sqlite` → the local-object-log authority plus
//! a SQLite materialized projection, TD-004), shared-nothing: a distinct `PQUEUE_NODE_ID`, its OWN
//! `PQUEUE_OBJECT_LOG_ROOT` + `PQUEUE_SQLITE_PROJECTION_PATH` (under a fresh per-run temp dir), its own
//! `PQUEUE_LISTEN_ADDR=127.0.0.1:<port>`, and a DISJOINT `PQUEUE_BOOTSTRAP_QUEUES` set (each node owns its own
//! M queues; no queue lives on two nodes). The driver reaches each node directly at `127.0.0.1:<port>` over
//! real TCP — host-process → host-process loopback (this is exactly the ADR-008 ownership model: independent
//! owners with no shared store, no shared lock, no shared projection). NOTE: the nodes are run as CHILD HOST
//! PROCESSES, NOT containers — the docker *published-port* path does not work in this orbstack namespace, so
//! the cluster is built from host processes the driver speaks RESP to directly.
//!
//! LOAD DRIVER: a raw RESP client over `std::net::TcpStream` (no new dependency — the same wire the
//! off-the-shelf-client e2e exercises: `XADD`=push, `XREADGROUP >`=priority claim, `XACK`=finalize-complete).
//! A fixed-per-owner push+claim+ack workload runs CONCURRENTLY across every node on real OS threads (one
//! driver thread per queue), barrier-released together so the wall-clock reflects genuine parallel execution.
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
//! spins up an 8-node cluster. To run the headline:
//!   cargo build -p pqueue-server --release --bin pqueue-service
//!   PQUEUE_E2_MULTINODE=1 cargo test --manifest-path crates/pqueue-bench/Cargo.toml \
//!     --test performance_multi_node_object_log_e2_tests -- --nocapture
//! Tunables (env, all optional): PQUEUE_E2_QUEUES_PER_OWNER (default 2), PQUEUE_E2_ITEMS_PER_QUEUE
//! (default 20000), PQUEUE_E2_PIPE (default 1000), PQUEUE_E2_BATCH (default 500),
//! PQUEUE_SERVICE_BIN (default <repo>/target/release/pqueue-service).

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;
/// The E2 headline cross-node multiple: 8-owner aggregate must be at least this times the 2-owner aggregate.
const SCALE_MULTIPLE_BAR: f64 = 3.5;

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
        s.set_read_timeout(Some(Duration::from_secs(60)))?;
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
/// replies) so loopback round-trips are amortized. Each item carries a rotating priority.
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

/// Drain `key` via repeated `XREADGROUP > COUNT batch` + `XACK` until empty; returns the count drained.
fn drain(conn: &mut Conn, key: &str, batch: usize) -> u64 {
    let mut total = 0u64;
    let count = batch.max(1).to_string();
    let mut guard = 0u64;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "drain did not terminate on {key}");
        conn.send(&[
            b"XREADGROUP",
            b"GROUP",
            b"g",
            b"c",
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
// The host-process owner cluster.
// ----------------------------------------------------------------------------------------------------

/// One owner node: its listen address, the FULL queue keys it owns (disjoint from every other node), and the
/// child process + data dir for teardown.
struct Node {
    addr: String,
    owned: Vec<String>,
    child: Child,
    dir: PathBuf,
}

/// A spawned cluster of independent owners. Drop kills every child and removes the data tree.
struct Cluster {
    nodes: Vec<Node>,
    base: PathBuf,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.kill();
            let _ = n.child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// Grab a currently-free loopback port (bind :0, read it, drop the listener). A tiny TOCTOU window remains,
/// but on loopback in a test it is acceptable.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

fn spawn_cluster(bin: &Path, base: &Path, owner_count: usize, queues_per_owner: usize) -> Cluster {
    std::fs::create_dir_all(base).expect("create base dir");
    let mut nodes = Vec::with_capacity(owner_count);
    for idx in 0..owner_count {
        let dir = base.join(format!("node{idx}"));
        std::fs::create_dir_all(&dir).expect("node dir");
        // Globally-unique queue names per node → ownership is disjoint by construction; the live XLEN probe
        // then PROVES each server only serves its own set.
        let owned: Vec<String> = (0..queues_per_owner)
            .map(|j| format!("t1:n{idx}q{j}"))
            .collect();
        let bootstrap = owned.join(",");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let err_log = File::create(dir.join("service.err.log")).expect("err log");
        let child = Command::new(bin)
            .env("PQUEUE_LOG_BACKEND", "objectlog")
            .env("PQUEUE_PROJECTION_BACKEND", "sqlite")
            .env("PQUEUE_NODE_ID", (idx as u32 + 1).to_string())
            .env("PQUEUE_OBJECT_LOG_ROOT", dir.join("obj"))
            .env("PQUEUE_SQLITE_PROJECTION_PATH", dir.join("proj.db"))
            .env("PQUEUE_LISTEN_ADDR", &addr)
            .env("PQUEUE_BOOTSTRAP_QUEUES", &bootstrap)
            .env("PQUEUE_RECLAIM_INTERVAL_MS", "60000")
            .stdout(Stdio::null())
            .stderr(Stdio::from(err_log))
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
        nodes.push(Node {
            addr,
            owned,
            child,
            dir,
        });
    }
    Cluster {
        nodes,
        base: base.to_path_buf(),
    }
}

/// Block until every node answers an `XLEN` of its first owned queue with an integer (bootstrap complete), or
/// panic after the deadline (dumping the node's stderr so a boot failure is visible, not a silent hang).
fn await_ready(cluster: &mut Cluster) {
    let deadline = Instant::now() + Duration::from_secs(30);
    for n in &mut cluster.nodes {
        loop {
            if let Some(status) = n.child.try_wait().expect("try_wait") {
                let err =
                    std::fs::read_to_string(n.dir.join("service.err.log")).unwrap_or_default();
                panic!(
                    "node {} exited early ({status}) before becoming ready; stderr:\n{err}",
                    n.addr
                );
            }
            if let Ok(mut c) = Conn::connect(&n.addr)
                && let Ok(Ok(_)) = xlen(&mut c, &n.owned[0])
            {
                break;
            }
            if Instant::now() > deadline {
                let err =
                    std::fs::read_to_string(n.dir.join("service.err.log")).unwrap_or_default();
                panic!("node {} not ready within deadline; stderr:\n{err}", n.addr);
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

/// One scale point: the aggregate throughput of all owners running concurrently and the MINIMUM single-queue
/// throughput across every queue of every owner (the worst queue the per-queue floor must clear).
struct ScalePoint {
    owners: usize,
    aggregate: f64,
    min_per_queue: f64,
}

/// Drive the whole cluster: one driver thread per queue, barrier-released together, each pushing then draining
/// `items_per_queue`. Aggregate = total items / overall wall-clock; worst = the slowest single queue.
fn measure(cluster: &Cluster, items_per_queue: u64, pipe: usize, batch: usize) -> ScalePoint {
    let total_threads: usize = cluster.nodes.iter().map(|n| n.owned.len()).sum();
    let barrier = Arc::new(Barrier::new(total_threads + 1));
    let mut handles = Vec::with_capacity(total_threads);
    for node in &cluster.nodes {
        for q in &node.owned {
            let addr = node.addr.clone();
            let key = q.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                // Connect (and so pay TCP setup) BEFORE the barrier, so the timed window is pure work.
                let mut c = Conn::connect(&addr).expect("driver connect");
                barrier.wait();
                let start = Instant::now();
                push_items(&mut c, &key, items_per_queue, pipe);
                let drained = drain(&mut c, &key, batch);
                assert_eq!(
                    drained, items_per_queue,
                    "every pushed item must drain on {key}"
                );
                items_per_queue as f64 / start.elapsed().as_secs_f64()
            }));
        }
    }
    barrier.wait();
    let wall_start = Instant::now();
    let rates: Vec<f64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wall = wall_start.elapsed().as_secs_f64();
    let total_items = (total_threads as u64 * items_per_queue) as f64;
    let min_per_queue = rates.iter().copied().fold(f64::INFINITY, f64::min);
    ScalePoint {
        owners: cluster.nodes.len(),
        aggregate: total_items / wall,
        min_per_queue,
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
             headline cross-queue scale-out at owner counts 2/4/8. The >=3.5x-at-8 multiple + worst-per-queue \
             floor evidence is DEFERRED (not measured), never a hidden pass."
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

    let queues_per_owner = env_usize("PQUEUE_E2_QUEUES_PER_OWNER", 2);
    let items_per_queue = env_u64("PQUEUE_E2_ITEMS_PER_QUEUE", 20_000);
    let pipe = env_usize("PQUEUE_E2_PIPE", 1_000);
    let batch = env_usize("PQUEUE_E2_BATCH", 500);
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let base_root = env::temp_dir().join(format!("pqueue-e2-{}", std::process::id()));
    let counts = [2usize, 4, 8];

    println!(
        "\nTP-002 E2 LIVE multi-node object_log_sqlite_projection scale-out \
         (host-process owners; {cores} cores; queues/owner={queues_per_owner}, items/queue={items_per_queue})"
    );
    println!("  owners | aggregate items/s | worst per-queue items/s");

    let mut points: Vec<ScalePoint> = Vec::new();
    let mut cross_node_unknown_at_8 = 0usize;
    for &n in &counts {
        let base = base_root.join(format!("owners{n}"));
        let mut cluster = spawn_cluster(&bin, &base, n, queues_per_owner);
        await_ready(&mut cluster);
        // Live one-owner-per-queue proof at every scale point (bar 4); keep the 8-owner count for the row.
        let unknown = assert_one_owner_per_queue(&cluster);
        if n == 8 {
            cross_node_unknown_at_8 = unknown;
        }
        let p = measure(&cluster, items_per_queue, pipe, batch);
        println!(
            "  {:>6} | {:>17.0} | {:>23.0}",
            p.owners, p.aggregate, p.min_per_queue
        );
        points.push(p);
        // Cluster dropped here → children killed, data dir removed, before the next scale point spins up.
    }

    let at = |n: usize| points.iter().find(|p| p.owners == n).unwrap();

    // ---- Evaluate the four E2 bars (every value measured) ----
    // (1) STRICTLY NON-DECREASING aggregate across 2 → 4 → 8.
    let nondecreasing = at(4).aggregate >= at(2).aggregate && at(8).aggregate >= at(4).aggregate;
    // (2) 8-owner aggregate >= 3.5x the 2-owner aggregate.
    let ratio_8_2 = at(8).aggregate / at(2).aggregate;
    let scale_pass = ratio_8_2 >= SCALE_MULTIPLE_BAR;
    // (3) worst single-queue throughput across ALL scale points >= the E0 floor.
    let worst_per_queue = points
        .iter()
        .map(|p| p.min_per_queue)
        .fold(f64::INFINITY, f64::min);
    let floor_pass = worst_per_queue >= FLOOR_ITEMS_PER_SEC;
    // (4) one-owner-per-queue: assert_one_owner_per_queue already HARD-asserts it (panics on violation); a
    // non-zero confirmation count means the disjoint-ownership probe actually ran with teeth.
    let disjoint_pass = cross_node_unknown_at_8 > 0 && queues_per_owner > 0;

    let all_pass = nondecreasing && scale_pass && floor_pass && disjoint_pass;

    println!(
        "\n  (1) non-decreasing 2->4->8 : {} ({:.0} -> {:.0} -> {:.0})",
        yn(nondecreasing),
        at(2).aggregate,
        at(4).aggregate,
        at(8).aggregate
    );
    println!(
        "  (2) 8/2 aggregate multiple : {ratio_8_2:.2}x (bar >= {SCALE_MULTIPLE_BAR}x) -> {}",
        yn(scale_pass)
    );
    println!(
        "  (3) worst per-queue        : {worst_per_queue:.0}/s (floor {FLOOR_ITEMS_PER_SEC:.0}/s) -> {}",
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
            "owners_2_aggregate_per_s".to_string(),
            serde_json::json!(at(2).aggregate.round()),
        ),
        (
            "owners_4_aggregate_per_s".to_string(),
            serde_json::json!(at(4).aggregate.round()),
        ),
        (
            "owners_8_aggregate_per_s".to_string(),
            serde_json::json!(at(8).aggregate.round()),
        ),
        (
            "scale_out_8_vs_2_multiple".to_string(),
            serde_json::json!((ratio_8_2 * 100.0).round() / 100.0),
        ),
        (
            "scale_multiple_bar".to_string(),
            serde_json::json!(SCALE_MULTIPLE_BAR),
        ),
        (
            "aggregate_non_decreasing".to_string(),
            serde_json::json!(nondecreasing),
        ),
        (
            "worst_per_queue_per_s".to_string(),
            serde_json::json!(worst_per_queue.round()),
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
            "live multi-node host-process cluster of independent ADR-008 owners on object_log_sqlite_projection \
             (TD-004); {cores} cores; owner counts 2/4/8; each owner an independent pqueue-service process with \
             its own object-log root + sqlite projection + disjoint bootstrap queues; driver speaks raw RESP \
             over loopback TCP"
        ),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "E2: aggregate strictly non-decreasing 2->4->8; 8-owner aggregate >= 3.5x 2-owner; worst per-queue >= E0 floor (2777.78/s); no queue served by more than one owner".into(),
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
        "E2 bar (1): aggregate must be strictly non-decreasing across 2->4->8: {:.0} -> {:.0} -> {:.0}",
        at(2).aggregate,
        at(4).aggregate,
        at(8).aggregate
    );
    assert!(
        scale_pass,
        "E2 bar (2): 8-owner aggregate must be >= {SCALE_MULTIPLE_BAR}x the 2-owner aggregate, measured {ratio_8_2:.2}x"
    );
    assert!(
        floor_pass,
        "E2 bar (3): worst per-queue throughput must be >= the E0 floor ({FLOOR_ITEMS_PER_SEC:.0}/s), measured {worst_per_queue:.0}/s"
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
