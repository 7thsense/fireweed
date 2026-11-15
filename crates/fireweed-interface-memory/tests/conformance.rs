//! Runs the rule-pinning conformance tests against the in-memory
//! implementation, with small limits so the scan and size tests run.

use std::sync::Arc;

use fireweed_interface::{DataPlane, ImplementationLimits, ManualClock};
use fireweed_interface_conformance::{Status, run};
use fireweed_interface_memory::{MemoryDataPlane, default_limits};

fn test_limits() -> ImplementationLimits {
    let mut l = default_limits();
    l.max_writer_scan_rows = 50;
    l.max_read_scan_rows = 500;
    l.max_entries_per_commit = 1000;
    l.max_body_bytes = 4096;
    l
}

#[test]
fn rule_pinning_tests() {
    let factory = |clock: Arc<ManualClock>| -> Arc<dyn DataPlane> {
        Arc::new(MemoryDataPlane::with_limits(clock, test_limits()))
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
