//! pqueue at-scale performance harness (TP-002 E0/E1/E3).
//!
//! Drives the four durable-log backends (the realized "log store" axis — the projection is the single
//! shared `pqueue-projection` materialization on all of them) through four workloads and reports against
//! the **E0 floor** (>=10,000,000 accepted items/hr per queue == 2,777.78 items/s):
//!   * `ingest`    — `push_batch` throughput + per-batch latency percentiles.
//!   * `claim`     — `claim`+`ack` throughput + per-batch latency percentiles.
//!   * `recovery`  — rebuild-from-log time on reopen (durable backends only; the E3 recovery bar).
//!   * `density`   — many concurrently-resident queues on one node; the hot queue still hits the floor.
//!
//! Driven by `futures::executor::block_on` (NOT tokio) so the sync `postgres` client works uniformly.
//!
//! Usage:
//!   cargo run --release -p pqueue-bench -- [--items N] [--batch B] [--backends a,b,c]
//!       [--workloads ingest,claim,recovery,density] [--queues Q] [--pg-url URL]
//!   # full TP-002 single-queue substantiation:
//!   cargo run --release -p pqueue-bench -- --items 10000000 --batch 10000
//!   # postgres needs a live DB (else it loud-skips):
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@HOST:5432/postgres cargo run --release -p pqueue-bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use pqueue::{NewItem, Pqueue};
use pqueue_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp,
};
use pqueue_engine::{Clock, QueueKey};
use pqueue_memory::MemoryBackend;
use pqueue_objectlog::ObjectLogBackend;
use pqueue_postgres::PostgresBackend;
use pqueue_sqlite::SqliteBackend;

/// The E0 per-queue throughput floor: 10,000,000 accepted items/hr.
const FLOOR_ITEMS_PER_HR: f64 = 10_000_000.0;
const FLOOR_ITEMS_PER_SEC: f64 = FLOOR_ITEMS_PER_HR / 3600.0; // 2,777.78/s

fn main() {
    let cfg = Config::from_args();
    cfg.print_header();
    futures::executor::block_on(run(&cfg));
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Config {
    items: u64,
    batch: usize,
    backends: Vec<String>,
    workloads: Vec<String>,
    queues: usize,
    pg_url: Option<String>,
}

impl Config {
    fn from_args() -> Self {
        let mut items = 100_000u64;
        let mut batch = 10_000usize;
        let mut backends = vec![
            "memory".into(),
            "sqlite".into(),
            "objectlog".into(),
            "postgres".into(),
        ];
        let mut workloads = vec![
            "ingest".into(),
            "claim".into(),
            "recovery".into(),
            "density".into(),
        ];
        let mut queues = 1000usize;
        let mut pg_url = std::env::var("PQUEUE_PG_TEST_URL").ok();

        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            let val = args.get(i + 1).cloned().unwrap_or_default();
            match args[i].as_str() {
                "--items" => items = val.parse().expect("--items N"),
                "--batch" => batch = val.parse().expect("--batch B"),
                "--backends" => backends = val.split(',').map(|s| s.trim().to_string()).collect(),
                "--workloads" => workloads = val.split(',').map(|s| s.trim().to_string()).collect(),
                "--queues" => queues = val.parse().expect("--queues Q"),
                "--pg-url" => pg_url = Some(val),
                other => panic!("unknown arg {other}"),
            }
            i += 2;
        }
        Config {
            items,
            batch,
            backends,
            workloads,
            queues,
            pg_url,
        }
    }

    fn has(&self, w: &str) -> bool {
        self.workloads.iter().any(|x| x == w)
    }

    fn print_header(&self) {
        println!("pqueue at-scale harness — TP-002 E0/E1/E3");
        println!(
            "  items/queue = {}   batch = {}   queues(density) = {}",
            fmt_count(self.items),
            self.batch,
            self.queues
        );
        println!(
            "  E0 floor    = {:.0} items/hr ({:.0} items/s)\n",
            FLOOR_ITEMS_PER_HR, FLOOR_ITEMS_PER_SEC
        );
        println!(
            "{:<10} {:<8} {:>10} {:>13} {:>8} {:>10} {:>10} {:>10}",
            "backend", "op", "items", "items/hr", "floor", "p50", "p95", "p99"
        );
        println!("{}", "-".repeat(84));
    }
}

// ---------------------------------------------------------------------------
// Clock + queue definition
// ---------------------------------------------------------------------------

struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

fn bench_qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).expect("tenant"),
        queue_id: QueueId::new(queue).expect("queue"),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

struct OpStats {
    op: &'static str,
    items: u64,
    wall: Duration,
    lat: Vec<Duration>,
}

impl OpStats {
    fn items_per_sec(&self) -> f64 {
        if self.wall.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.items as f64 / self.wall.as_secs_f64()
    }
    fn items_per_hr(&self) -> f64 {
        self.items_per_sec() * 3600.0
    }
    fn pct(&mut self, p: f64) -> Duration {
        if self.lat.is_empty() {
            return Duration::ZERO;
        }
        self.lat.sort_unstable();
        let idx = (((self.lat.len() as f64) * p).ceil() as usize).saturating_sub(1);
        self.lat[idx.min(self.lat.len() - 1)]
    }
    fn report(&mut self, backend: &str) {
        let ips = self.items_per_sec();
        let pass = if ips >= FLOOR_ITEMS_PER_SEC {
            "PASS"
        } else {
            "FAIL"
        };
        // Compute the percentiles up front (each `pct` call mutably sorts `self.lat`) so the `println!`
        // below holds only a shared borrow of `self`.
        let (p50, p95, p99) = (self.pct(0.50), self.pct(0.95), self.pct(0.99));
        println!(
            "{:<10} {:<8} {:>10} {:>13} {:>8} {:>10} {:>10} {:>10}",
            backend,
            self.op,
            fmt_count(self.items),
            fmt_rate(self.items_per_hr()),
            pass,
            fmt_dur(p50),
            fmt_dur(p95),
            fmt_dur(p99),
        );
    }
}

// ---------------------------------------------------------------------------
// Generic workloads (over the library facade)
// ---------------------------------------------------------------------------

async fn ingest<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    q: &QueueKey,
    items: u64,
    batch: usize,
) -> OpStats {
    let mut lat = Vec::new();
    let mut done = 0u64;
    let start = Instant::now();
    while done < items {
        let n = (items - done).min(batch as u64) as usize;
        let batch_items: Vec<NewItem> = (0..n)
            .map(|k| NewItem {
                priority: Some(PriorityValue::Int64(((done + k as u64) % 1000) as i64)),
                ..Default::default()
            })
            .collect();
        let t = Instant::now();
        pq.push_batch(q, batch_items).await.expect("push_batch");
        lat.push(t.elapsed());
        done += n as u64;
    }
    OpStats {
        op: "ingest",
        items,
        wall: start.elapsed(),
        lat,
    }
}

/// Returns (claim stats, ack stats). Drains up to `items` already-pending records.
async fn claim_ack<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    q: &QueueKey,
    items: u64,
    batch: usize,
) -> (OpStats, OpStats) {
    let mut claim_lat = Vec::new();
    let mut ack_lat = Vec::new();
    let mut drained = 0u64;
    let start = Instant::now();
    while drained < items {
        let tc = Instant::now();
        let claimed = pq.claim(q, batch, 3_600_000).await.expect("claim");
        let cd = tc.elapsed();
        if claimed.is_empty() {
            break;
        }
        claim_lat.push(cd);
        let ids: Vec<ItemId> = claimed.iter().map(|c| c.item_id.clone()).collect();
        let n = ids.len() as u64;
        let ta = Instant::now();
        pq.ack(q, ids).await.expect("ack");
        ack_lat.push(ta.elapsed());
        drained += n;
    }
    let wall = start.elapsed();
    (
        OpStats {
            op: "claim",
            items: drained,
            wall,
            lat: claim_lat,
        },
        OpStats {
            op: "ack",
            items: drained,
            wall,
            lat: ack_lat,
        },
    )
}

// ---------------------------------------------------------------------------
// Per-backend runners
// ---------------------------------------------------------------------------

async fn run(cfg: &Config) {
    for backend in &cfg.backends {
        match backend.as_str() {
            "memory" => run_memory(cfg).await,
            "sqlite" => run_sqlite(cfg).await,
            "objectlog" => run_objectlog(cfg).await,
            "postgres" => run_postgres(cfg).await,
            other => println!("(skipping unknown backend '{other}')"),
        }
    }
    if cfg.has("density") {
        density(cfg).await;
    }
}

async fn run_memory(cfg: &Config) {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(SystemClock));
    let q = qkey("hot");
    pq.create_queue(bench_qdef("bench", "hot")).await.unwrap();
    run_throughput(cfg, "memory", &pq, &q).await;
    if cfg.has("recovery") {
        println!(
            "{:<10} {:<8} {:>10} {:>13} {:>8}   (non-durable — no replay)",
            "memory", "recovery", "-", "-", "-"
        );
    }
}

async fn run_sqlite(cfg: &Config) {
    let path = tmp("sqlite", "db");
    let _ = std::fs::remove_file(&path);
    {
        let pq = Pqueue::new(
            Arc::new(SqliteBackend::open(path.to_str().expect("utf8 path")).expect("open sqlite")),
            Arc::new(SystemClock),
        );
        let q = qkey("hot");
        pq.create_queue(bench_qdef("bench", "hot")).await.unwrap();
        run_throughput(cfg, "sqlite", &pq, &q).await;
    } // drop -> only the durable file remains
    if cfg.has("recovery") {
        let t = Instant::now();
        let pq = Pqueue::new(
            Arc::new(
                SqliteBackend::open(path.to_str().expect("utf8 path")).expect("reopen sqlite"),
            ),
            Arc::new(SystemClock),
        );
        report_recovery("sqlite", t.elapsed(), &pq, cfg).await;
    }
    let _ = std::fs::remove_file(&path);
}

async fn run_objectlog(cfg: &Config) {
    let dir = tmp("objectlog", "dir");
    let _ = std::fs::remove_dir_all(&dir);
    {
        let pq = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("open objectlog")),
            Arc::new(SystemClock),
        );
        let q = qkey("hot");
        pq.create_queue(bench_qdef("bench", "hot")).await.unwrap();
        run_throughput(cfg, "objectlog", &pq, &q).await;
    }
    if cfg.has("recovery") {
        let t = Instant::now();
        let pq = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("reopen objectlog")),
            Arc::new(SystemClock),
        );
        report_recovery("objectlog", t.elapsed(), &pq, cfg).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

async fn run_postgres(cfg: &Config) {
    let Some(url) = cfg.pg_url.clone() else {
        println!(
            "{:<10} (SKIPPED — set --pg-url or PQUEUE_PG_TEST_URL to a live DB)",
            "postgres"
        );
        return;
    };
    let schema = format!("pq_bench_{}", std::process::id());
    {
        let pq = Pqueue::new(
            Arc::new(PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres")),
            Arc::new(SystemClock),
        );
        let q = qkey("hot");
        pq.create_queue(bench_qdef("bench", "hot")).await.unwrap();
        run_throughput(cfg, "postgres", &pq, &q).await;
    }
    if cfg.has("recovery") {
        let t = Instant::now();
        let pq = Pqueue::new(
            Arc::new(
                PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect postgres"),
            ),
            Arc::new(SystemClock),
        );
        report_recovery("postgres", t.elapsed(), &pq, cfg).await;
    }
}

/// Run the ingest + claim/ack throughput workloads on a prepared handle.
async fn run_throughput<B: pqueue::LibBackend>(
    cfg: &Config,
    name: &str,
    pq: &Pqueue<B>,
    q: &QueueKey,
) {
    if cfg.has("ingest") || cfg.has("recovery") || cfg.has("claim") {
        // ingest is the precondition for claim + recovery, so always run it when any of them is requested.
        let mut s = ingest(pq, q, cfg.items, cfg.batch).await;
        if cfg.has("ingest") {
            s.report(name);
        }
    }
    if cfg.has("claim") {
        let (mut c, mut a) = claim_ack(pq, q, cfg.items, cfg.batch).await;
        c.report(name);
        a.report(name);
    }
}

async fn report_recovery<B: pqueue::LibBackend>(
    name: &str,
    elapsed: Duration,
    pq: &Pqueue<B>,
    cfg: &Config,
) {
    // Sanity: the replayed projection must hold the resident set. (claim drained it on the same run only
    // if `claim` was requested; recovery reopens the durable log which still has every committed command,
    // so pending == ingested-minus-acked.)
    let resident = pq
        .metrics(&qkey("hot"))
        .await
        .map(|m| m.pending + m.leased)
        .unwrap_or(0);
    let ips = if elapsed.as_secs_f64() > 0.0 {
        cfg.items as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "{:<10} {:<8} {:>10} {:>13} {:>8}   rebuilt {} resident in {} ({}/s replay)",
        name,
        "recovery",
        fmt_count(cfg.items),
        "-",
        "-",
        fmt_count(resident),
        fmt_dur(elapsed),
        fmt_count(ips as u64),
    );
}

/// Queue density: create `queues` queues on ONE node, seed each with a small resident set, then drive the
/// designated hot queue at full rate and confirm it still hits the E0 floor while the others stay active.
async fn density(cfg: &Config) {
    println!("\nqueue density (single node, memory backend):");
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(SystemClock));
    let cold_each = 100u64; // keep the other queues "active"/resident without dominating the run
    let create_start = Instant::now();
    for i in 0..cfg.queues {
        let name = format!("q{i}");
        pq.create_queue(bench_qdef("bench", &name)).await.unwrap();
        ingest(&pq, &qkey(&name), cold_each, cfg.batch).await;
    }
    let setup = create_start.elapsed();
    let resident = (cfg.queues as u64) * cold_each;
    println!(
        "  created {} active queues, {} resident items, in {}",
        cfg.queues,
        fmt_count(resident),
        fmt_dur(setup)
    );

    // Hot queue: full ingest + drain while the other queues stay resident.
    let hot = format!("q{}", cfg.queues); // a fresh queue id beyond the cold set
    pq.create_queue(bench_qdef("bench", &hot)).await.unwrap();
    let hk = qkey(&hot);
    let mut ing = ingest(&pq, &hk, cfg.items, cfg.batch).await;
    ing.op = "hot-ingest";
    ing.report("density");
    let (mut c, _a) = claim_ack(&pq, &hk, cfg.items, cfg.batch).await;
    c.op = "hot-claim";
    c.report("density");
    println!(
        "  -> hot queue floor check with {} other active queues resident.\n",
        cfg.queues
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn qkey(queue: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("bench").unwrap(),
        QueueId::new(queue).unwrap(),
    )
}

fn tmp(tag: &str, ext: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("pqueue-bench-{tag}-{}.{ext}", std::process::id()))
}

fn fmt_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1e6)
    } else if n >= 1_000 {
        format!("{:.0}K", n as f64 / 1e3)
    } else {
        n.to_string()
    }
}

fn fmt_rate(items_per_hr: f64) -> String {
    if items_per_hr >= 1e9 {
        format!("{:.2}B/hr", items_per_hr / 1e9)
    } else {
        format!("{:.1}M/hr", items_per_hr / 1e6)
    }
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us >= 1_000_000 {
        format!("{:.2}s", d.as_secs_f64())
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}us")
    }
}
