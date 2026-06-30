//! pqueue performance / e2e harness (TP-002 + data-shape baseline).
//!
//! Drives SIX backends across BOTH projection families through ingest / claim+ack / lifecycle / recovery /
//! density workloads over a representative SET of data SHAPES, and reports throughput vs the E0 floor
//! (>=10,000,000 accepted items/hr per queue == 2,777.78 items/s) plus per-batch latency percentiles.
//!
//! Projection families:
//!   * `log-replay` (in-memory projection rebuilt from a durable log): `memory`, `sqlite`, `objectlog`,
//!     `postgres`.
//!   * `relational` (DB-resident / DB-authoritative projection): `sqlite_relational`, `postgres_relational`.
//!
//! Driven by `futures::executor::block_on` (NOT tokio) so the sync `postgres` client works uniformly.
//!
//! Usage:
//!   cargo run --release -p pqueue-bench -- [--items N] [--batch B] [--backends a,b,c]
//!       [--workloads ingest,claim,lifecycle,recovery,density] [--shapes minimal,hot_record,...]
//!       [--queues Q] [--pg-url URL]
//!   # postgres / postgres_relational need a live DB (else they loud-skip):
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@HOST:5432/postgres cargo run --release -p pqueue-bench

use std::sync::Arc;
use std::time::{Duration, Instant};

use pqueue::Pqueue;
use pqueue_bench::{
    FLOOR_ITEMS_PER_HR, FLOOR_ITEMS_PER_SEC, OpStats, Shape, SystemClock, all_shapes, bench_qdef,
    claim_ack, ingest, lifecycle, qkey, shape_by_name,
};
use pqueue_memory::composed_memory_backend;
use pqueue_objectlog::ObjectLogBackend;
use pqueue_postgres::{PostgresBackend, PostgresRelationalBackend};
use pqueue_sqlite::{SqliteRelationalBackend, composed_sqlite_backend};

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
    shapes: Vec<Shape>,
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
            "sqlite_relational".into(),
            "objectlog".into(),
            "postgres".into(),
            "postgres_relational".into(),
        ];
        let mut workloads = vec![
            "ingest".into(),
            "claim".into(),
            "lifecycle".into(),
            "recovery".into(),
            "density".into(),
        ];
        let mut shape_names: Option<Vec<String>> = None;
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
                "--shapes" => {
                    shape_names = Some(val.split(',').map(|s| s.trim().to_string()).collect())
                }
                "--queues" => queues = val.parse().expect("--queues Q"),
                "--pg-url" => pg_url = Some(val),
                other => panic!("unknown arg {other}"),
            }
            i += 2;
        }

        let shapes = match shape_names {
            None => all_shapes(),
            Some(names) => names
                .iter()
                .map(|n| shape_by_name(n).unwrap_or_else(|| panic!("unknown shape '{n}'")))
                .collect(),
        };

        Config {
            items,
            batch,
            backends,
            workloads,
            shapes,
            queues,
            pg_url,
        }
    }

    fn has(&self, w: &str) -> bool {
        self.workloads.iter().any(|x| x == w)
    }

    fn print_header(&self) {
        println!("pqueue performance / e2e harness — TP-002 + data-shape baseline");
        println!(
            "  items/queue = {}   batch = {}   queues(density) = {}",
            fmt_count(self.items),
            self.batch,
            self.queues
        );
        println!(
            "  shapes      = {}",
            self.shapes
                .iter()
                .map(|s| s.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!(
            "  E0 floor    = {:.0} items/hr ({:.0} items/s)\n",
            FLOOR_ITEMS_PER_HR, FLOOR_ITEMS_PER_SEC
        );
        print_table_header();
    }
}

fn print_table_header() {
    println!(
        "{:<20} {:<11} {:<16} {:<9} {:>9} {:>11} {:>6} {:>9} {:>9} {:>9}",
        "backend", "family", "shape", "op", "items", "items/hr", "floor", "p50", "p95", "p99"
    );
    println!("{}", "-".repeat(116));
}

// ---------------------------------------------------------------------------
// Row printing
// ---------------------------------------------------------------------------

fn print_row(backend: &str, family: &str, shape: &str, stats: &mut OpStats) {
    let pass = if stats.passes_floor() { "PASS" } else { "FAIL" };
    let (p50, p95, p99) = (stats.pct(0.50), stats.pct(0.95), stats.pct(0.99));
    println!(
        "{:<20} {:<11} {:<16} {:<9} {:>9} {:>11} {:>6} {:>9} {:>9} {:>9}",
        backend,
        family,
        shape,
        stats.op,
        fmt_count(stats.items),
        fmt_rate(stats.items_per_hr()),
        pass,
        fmt_dur(p50),
        fmt_dur(p95),
        fmt_dur(p99),
    );
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

const LOG_FAMILY: &str = "log-replay";
const REL_FAMILY: &str = "relational";

async fn run(cfg: &Config) {
    for backend in &cfg.backends {
        match backend.as_str() {
            "memory" => run_memory(cfg).await,
            "sqlite" => run_sqlite(cfg).await,
            "sqlite_relational" => run_sqlite_relational(cfg).await,
            "objectlog" => run_objectlog(cfg).await,
            "postgres" => run_postgres(cfg).await,
            "postgres_relational" => run_postgres_relational(cfg).await,
            other => println!("(skipping unknown backend '{other}')"),
        }
    }
    if cfg.has("density") {
        density(cfg).await;
    }
}

/// Run the per-shape throughput + lifecycle workloads for one prepared backend. `supports_update` is the
/// atomic-class flag for `update_fields` (false for the eventual-apply object-log backend).
async fn run_shapes<B, F>(
    cfg: &Config,
    name: &str,
    family: &str,
    supports_update: bool,
    mut make: F,
) where
    B: pqueue::LibBackend,
    F: FnMut() -> Pqueue<B>,
{
    for shape in &cfg.shapes {
        // ingest / claim share one prepared queue per shape (claim drains what ingest pushed).
        if cfg.has("ingest") || cfg.has("claim") {
            let pq = make();
            let qn = format!("{name}-{}-tput", shape.name);
            let q = qkey(&qn);
            pq.create_queue(bench_qdef("bench", &qn, shape))
                .await
                .expect("create queue");
            let mut s = ingest(&pq, &q, shape, cfg.items, cfg.batch).await;
            if cfg.has("ingest") {
                print_row(name, family, shape.name, &mut s);
            }
            if cfg.has("claim") {
                let (mut c, mut a) = claim_ack(&pq, &q, cfg.items, cfg.batch).await;
                print_row(name, family, shape.name, &mut c);
                print_row(name, family, shape.name, &mut a);
            }
        }
        if cfg.has("lifecycle") {
            let pq = make();
            let qn = format!("{name}-{}-life", shape.name);
            let q = qkey(&qn);
            pq.create_queue(bench_qdef("bench", &qn, shape))
                .await
                .expect("create queue");
            match lifecycle(&pq, &q, shape, cfg.items, cfg.batch, supports_update).await {
                Ok(mut ls) => {
                    print_row(name, family, shape.name, &mut ls.push);
                    print_row(name, family, shape.name, &mut ls.claim);
                    print_row(name, family, shape.name, &mut ls.ack);
                    if !ls.update_ran {
                        println!(
                            "{:<20} {:<11} {:<16} (update_fields skipped — eventual-apply class)",
                            name, family, shape.name
                        );
                    }
                }
                Err(e) => println!(
                    "{:<20} {:<11} {:<16} LIFECYCLE FAILED: {e}",
                    name, family, shape.name
                ),
            }
        }
    }
}

async fn run_memory(cfg: &Config) {
    run_shapes(cfg, "memory", LOG_FAMILY, true, || {
        Pqueue::new(Arc::new(composed_memory_backend()), Arc::new(SystemClock))
    })
    .await;
    if cfg.has("recovery") {
        println!(
            "{:<20} {:<11} {:<16} {:<9} (non-durable — no replay)",
            "memory", LOG_FAMILY, "-", "recovery"
        );
    }
}

async fn run_sqlite(cfg: &Config) {
    // throughput / lifecycle each build a fresh in-process file via a unique path.
    let mut counter = 0usize;
    run_shapes(cfg, "sqlite", LOG_FAMILY, true, || {
        counter += 1;
        let path = tmp("sqlite", &format!("{counter}"))
            .to_string_lossy()
            .into_owned();
        let _ = std::fs::remove_file(&path);
        Pqueue::new(
            Arc::new(composed_sqlite_backend(&path).expect("open sqlite")),
            Arc::new(SystemClock),
        )
    })
    .await;
    if cfg.has("recovery") {
        recovery_durable(cfg, "sqlite", LOG_FAMILY, |path| {
            Arc::new(composed_sqlite_backend(path).expect("sqlite"))
        })
        .await;
    }
}

async fn run_sqlite_relational(cfg: &Config) {
    run_shapes(cfg, "sqlite_relational", REL_FAMILY, true, || {
        Pqueue::new(
            Arc::new(SqliteRelationalBackend::in_memory().expect("sqlite relational")),
            Arc::new(SystemClock),
        )
    })
    .await;
    if cfg.has("recovery") {
        println!(
            "{:<20} {:<11} {:<16} {:<9} (in-memory DB-resident — replay N/A)",
            "sqlite_relational", REL_FAMILY, "-", "recovery"
        );
    }
}

async fn run_objectlog(cfg: &Config) {
    let mut counter = 0usize;
    run_shapes(cfg, "objectlog", LOG_FAMILY, false, || {
        counter += 1;
        let dir = tmp("objectlog", &format!("{counter}"));
        let _ = std::fs::remove_dir_all(&dir);
        Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("open objectlog")),
            Arc::new(SystemClock),
        )
    })
    .await;
    if cfg.has("recovery") {
        let dir = tmp("objectlog", "recov");
        let _ = std::fs::remove_dir_all(&dir);
        let shape = &cfg.shapes[0];
        {
            let pq = Pqueue::new(
                Arc::new(ObjectLogBackend::open(&dir).expect("open objectlog")),
                Arc::new(SystemClock),
            );
            let q = qkey("recov");
            pq.create_queue(bench_qdef("bench", "recov", shape))
                .await
                .unwrap();
            ingest(&pq, &q, shape, cfg.items, cfg.batch).await;
        }
        let t = Instant::now();
        let pq = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("reopen objectlog")),
            Arc::new(SystemClock),
        );
        report_recovery("objectlog", LOG_FAMILY, t.elapsed(), &pq, cfg).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

async fn run_postgres(cfg: &Config) {
    let Some(url) = cfg.pg_url.clone() else {
        println!(
            "{:<20} (SKIPPED — set --pg-url or PQUEUE_PG_TEST_URL to a live DB)",
            "postgres"
        );
        return;
    };
    let mut counter = 0usize;
    run_shapes(cfg, "postgres", LOG_FAMILY, true, || {
        counter += 1;
        let schema = format!("pq_bench_log_{}_{}", std::process::id(), counter);
        Pqueue::new(
            Arc::new(PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres")),
            Arc::new(SystemClock),
        )
    })
    .await;
    if cfg.has("recovery") {
        let schema = format!("pq_bench_log_recov_{}", std::process::id());
        let shape = &cfg.shapes[0];
        {
            let pq = Pqueue::new(
                Arc::new(
                    PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres"),
                ),
                Arc::new(SystemClock),
            );
            let q = qkey("recov");
            pq.create_queue(bench_qdef("bench", "recov", shape))
                .await
                .unwrap();
            ingest(&pq, &q, shape, cfg.items, cfg.batch).await;
        }
        let t = Instant::now();
        let pq = Pqueue::new(
            Arc::new(
                PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect postgres"),
            ),
            Arc::new(SystemClock),
        );
        report_recovery("postgres", LOG_FAMILY, t.elapsed(), &pq, cfg).await;
    }
}

async fn run_postgres_relational(cfg: &Config) {
    let Some(url) = cfg.pg_url.clone() else {
        println!(
            "{:<20} (SKIPPED — set --pg-url or PQUEUE_PG_TEST_URL to a live DB)",
            "postgres_relational"
        );
        return;
    };
    let mut counter = 0usize;
    run_shapes(cfg, "postgres_relational", REL_FAMILY, true, || {
        counter += 1;
        let schema = format!("pq_bench_rel_{}_{}", std::process::id(), counter);
        Pqueue::new(
            Arc::new(
                PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .expect("connect postgres relational"),
            ),
            Arc::new(SystemClock),
        )
    })
    .await;
    if cfg.has("recovery") {
        println!(
            "{:<20} {:<11} {:<16} {:<9} (DB-resident projection — no log replay)",
            "postgres_relational", REL_FAMILY, "-", "recovery"
        );
    }
}

/// Reopen a durable file-backed log backend and time the rebuild-from-log for the first shape.
async fn recovery_durable<B, F>(cfg: &Config, name: &str, family: &str, reopen: F)
where
    B: pqueue::LibBackend,
    F: Fn(&str) -> Arc<B>,
{
    let path = tmp(name, "recov").to_string_lossy().into_owned();
    let _ = std::fs::remove_file(&path);
    let shape = &cfg.shapes[0];
    {
        let pq = Pqueue::new(reopen(&path), Arc::new(SystemClock));
        let q = qkey("recov");
        pq.create_queue(bench_qdef("bench", "recov", shape))
            .await
            .unwrap();
        ingest(&pq, &q, shape, cfg.items, cfg.batch).await;
    }
    let t = Instant::now();
    let pq = Pqueue::new(reopen(&path), Arc::new(SystemClock));
    report_recovery(name, family, t.elapsed(), &pq, cfg).await;
    let _ = std::fs::remove_file(&path);
}

async fn report_recovery<B: pqueue::LibBackend>(
    name: &str,
    family: &str,
    elapsed: Duration,
    pq: &Pqueue<B>,
    cfg: &Config,
) {
    let resident = pq
        .metrics(&qkey("recov"))
        .await
        .map(|m| m.pending + m.leased)
        .unwrap_or(0);
    let ips = if elapsed.as_secs_f64() > 0.0 {
        cfg.items as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "{:<20} {:<11} {:<16} {:<9}   rebuilt {} resident in {} ({}/s replay)",
        name,
        family,
        cfg.shapes[0].name,
        "recovery",
        fmt_count(resident),
        fmt_dur(elapsed),
        fmt_count(ips as u64),
    );
}

/// Queue density: many concurrently-resident queues on one node; the hot queue still hits the floor.
async fn density(cfg: &Config) {
    println!("\nqueue density (single node, memory backend, minimal shape):");
    let shape = all_shapes()[0]; // minimal
    let pq = Pqueue::new(Arc::new(composed_memory_backend()), Arc::new(SystemClock));
    let cold_each = 100u64;
    let create_start = Instant::now();
    for i in 0..cfg.queues {
        let name = format!("q{i}");
        pq.create_queue(bench_qdef("bench", &name, &shape))
            .await
            .unwrap();
        ingest(&pq, &qkey(&name), &shape, cold_each, cfg.batch).await;
    }
    let setup = create_start.elapsed();
    let resident = (cfg.queues as u64) * cold_each;
    println!(
        "  created {} active queues, {} resident items, in {}",
        cfg.queues,
        fmt_count(resident),
        fmt_dur(setup)
    );

    let hot = format!("q{}", cfg.queues);
    pq.create_queue(bench_qdef("bench", &hot, &shape))
        .await
        .unwrap();
    let hk = qkey(&hot);
    let mut ing = ingest(&pq, &hk, &shape, cfg.items, cfg.batch).await;
    ing.op = "hot-ingest";
    print_row("density", LOG_FAMILY, "minimal", &mut ing);
    let (mut c, _a) = claim_ack(&pq, &hk, cfg.items, cfg.batch).await;
    c.op = "hot-claim";
    print_row("density", LOG_FAMILY, "minimal", &mut c);
    println!(
        "  -> hot queue floor check with {} other active queues resident.\n",
        cfg.queues
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
