//! e2e correctness over every DATA SHAPE across the embedded backends (both projection families), plus the
//! pg-gated backends. Runs the full [`pqueue_bench::lifecycle`] (push → claim → update_fields →
//! ack/nack(retry) → reclaim_expired → re-drain) at a SMALL scale and asserts the state-machine invariants.
//!
//! Embedded backends always run. The postgres / postgres_relational backends are gated on
//! `PQUEUE_PG_TEST_URL`; without it they LOUD-skip (printed), so the suite passes on a box with no DB.
//!
//! Run in isolation (this is a separate workspace):
//!   PQUEUE_PG_TEST_URL=postgres://... cargo test --manifest-path crates/pqueue-bench/Cargo.toml \
//!       --test e2e_shapes_tests

use std::sync::Arc;

use futures::executor::block_on;
use pqueue::Pqueue;
use pqueue_bench::{Shape, SystemClock, all_shapes, bench_qdef, lifecycle, qkey};
use pqueue_memory::MemoryBackend;
use pqueue_objectlog::ObjectLogBackend;
use pqueue_postgres::{PostgresBackend, PostgresRelationalBackend};
use pqueue_sqlite::{SqliteBackend, SqliteRelationalBackend};

/// Small but non-trivial: exercises batching + the 10%/10%/80% lifecycle partition with whole groups.
const ITEMS: u64 = 2_000;
const BATCH: usize = 250;

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "pqueue-bench-e2e-{tag}-{}-{}",
        std::process::id(),
        // a per-call nonce so repeated calls in one process never collide
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Run the lifecycle for one shape on a freshly-built backend handle, asserting it returns Ok.
fn run_one<B: pqueue::LibBackend>(
    backend: &str,
    pq: &Pqueue<B>,
    shape: &Shape,
    supports_update: bool,
) {
    let qn = format!("{backend}-{}", shape.name);
    let q = qkey(&qn);
    block_on(pq.create_queue(bench_qdef("bench", &qn, shape))).expect("create queue");
    let res = block_on(lifecycle(pq, &q, shape, ITEMS, BATCH, supports_update));
    let stats = res.unwrap_or_else(|e| panic!("[{backend}] lifecycle failed: {e}"));
    assert_eq!(
        stats.push.items, ITEMS,
        "[{backend}/{}] pushed all",
        shape.name
    );
    assert_eq!(
        stats.update_ran, supports_update,
        "[{backend}/{}] update step ran iff supported",
        shape.name
    );
}

#[test]
fn lifecycle_over_shapes_memory() {
    for shape in all_shapes() {
        let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(SystemClock));
        run_one("memory", &pq, &shape, true);
    }
}

#[test]
fn lifecycle_over_shapes_sqlite_log() {
    for shape in all_shapes() {
        let path = tmp(&format!("sqlite-{}", shape.name));
        let _ = std::fs::remove_file(&path);
        let pq = Pqueue::new(
            Arc::new(SqliteBackend::open(path.to_str().unwrap()).expect("open sqlite")),
            Arc::new(SystemClock),
        );
        run_one("sqlite", &pq, &shape, true);
        let _ = std::fs::remove_file(&path);
    }
}

#[test]
fn lifecycle_over_shapes_sqlite_relational() {
    for shape in all_shapes() {
        let pq = Pqueue::new(
            Arc::new(SqliteRelationalBackend::in_memory().expect("sqlite relational")),
            Arc::new(SystemClock),
        );
        run_one("sqlite_relational", &pq, &shape, true);
    }
}

#[test]
fn lifecycle_over_shapes_objectlog() {
    for shape in all_shapes() {
        let dir = tmp(&format!("objectlog-{}", shape.name));
        let _ = std::fs::remove_dir_all(&dir);
        let pq = Pqueue::new(
            Arc::new(ObjectLogBackend::open(&dir).expect("open objectlog")),
            Arc::new(SystemClock),
        );
        // Eventual-apply class: update_fields is refused, so the lifecycle skips it.
        run_one("objectlog", &pq, &shape, false);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn lifecycle_over_shapes_postgres_log() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "LOUD-SKIP: lifecycle_over_shapes_postgres_log — set PQUEUE_PG_TEST_URL to a live DB to run it"
        );
        return;
    };
    for shape in all_shapes() {
        let schema = format!(
            "pq_e2e_log_{}_{}",
            std::process::id(),
            shape.name.replace('-', "_")
        );
        let pq = Pqueue::new(
            Arc::new(PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres")),
            Arc::new(SystemClock),
        );
        run_one("postgres", &pq, &shape, true);
    }
}

#[test]
fn lifecycle_over_shapes_postgres_relational() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "LOUD-SKIP: lifecycle_over_shapes_postgres_relational — set PQUEUE_PG_TEST_URL to a live DB to run it"
        );
        return;
    };
    for shape in all_shapes() {
        let schema = format!(
            "pq_e2e_rel_{}_{}",
            std::process::id(),
            shape.name.replace('-', "_")
        );
        let pq = Pqueue::new(
            Arc::new(
                PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .expect("connect postgres relational"),
            ),
            Arc::new(SystemClock),
        );
        run_one("postgres_relational", &pq, &shape, true);
    }
}
