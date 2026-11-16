//! Runs the rule-pinning conformance tests against the PostgreSQL
//! implementation. Needs `FIREWEED_PG_TEST_URL` (for example
//! `postgres://postgres:fw@127.0.0.1:55432/fw`); without it the test
//! prints a notice and passes vacuously. Each test runs in its own
//! PostgreSQL schema, dropped afterwards.

use std::sync::Arc;

use fireweed_interface::{DataPlane, ImplementationLimits, ManualClock};
use fireweed_interface_conformance::{Controls, Instance, Status, run};
use fireweed_interface_postgres::PostgresDataPlane;

fn test_limits() -> ImplementationLimits {
    ImplementationLimits {
        max_ops_per_commit: 64,
        max_entries_per_commit: 1000,
        max_request_bytes: 1 << 20,
        max_in_list: 1000,
        max_writer_scan_rows: 50,
        max_read_scan_rows: 500,
        max_cohort_size: 0,
        max_selector_affected: 10_000,
        max_body_bytes: 4096,
        max_value_bytes: 256 << 10,
        max_set_values: 128,
        max_dry_run_count: 100_000,
        max_read_rows: 1000,
        max_claim_batch_size: 1000,
        max_lease_duration_ms: 3_600_000,
        max_request_id_retention_ms: 7 * 86_400_000,
        max_objects_per_space: 256,
    }
}

#[test]
fn rule_pinning_tests() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("FIREWEED_PG_TEST_URL is not set; skipping the PostgreSQL conformance run");
        return;
    };
    let factory = |clock: Arc<ManualClock>| -> Instance {
        let plane =
            Arc::new(PostgresDataPlane::isolated(&url, clock, test_limits()).expect("connect"));
        let primary = plane.clone();
        let survivor = plane.clone();
        // A client process that dies loses nothing the server committed, so
        // a new instance on the same database stands in for the restart.
        let controls = Controls {
            crash_and_reopen: Some(Arc::new(move |clock: Arc<ManualClock>| {
                Ok(Arc::new(survivor.peer(clock)?) as Arc<dyn DataPlane>)
            })),
            ..Controls::default()
        };
        Instance::new(plane)
            .with_peer(Arc::new(move |clock: Arc<ManualClock>| {
                Arc::new(primary.peer(clock).expect("connect a peer")) as Arc<dyn DataPlane>
            }))
            .with_controls(controls)
    };
    let report = run(&factory);
    println!("{}", report.summary());
    let failures = report.failures();
    assert!(
        failures.is_empty(),
        "{} rule-pinning tests failed:\n{}",
        failures.len(),
        report.summary()
    );
    assert!(
        report.count(|s| *s == Status::Pass) >= 60,
        "too few tests passed"
    );
}
