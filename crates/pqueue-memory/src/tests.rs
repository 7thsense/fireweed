//! Conformance for the memory backend.
//!
//! The full **port-level** behavioral no-stub suite is shared (`pqueue-conformance`) and run against
//! `MemoryBackend` by the `conformance_suite!` invocation below. The projection-internals white-box
//! tests (item_version monotonicity, high-water survives compaction) now live in `pqueue-projection`,
//! where that state lives. What remains here is the test-only `ManualClock`/`SeqIdGen` helpers, which
//! are memory-specific and not expressible through the ports.

use super::*;
use pqueue_conformance::ts;

// The full shared backend-conformance suite (16 port-level scenarios) against MemoryBackend.
pqueue_conformance::conformance_suite!(MemoryBackend::new);

#[tokio::test]
async fn manual_clock_and_idgen_are_real() {
    let clock = ManualClock::at(10);
    assert_eq!(clock.now(), ts(10));
    clock.set(20);
    assert_eq!(clock.now(), ts(20));

    let ids = SeqIdGen::default();
    let a = ids.next_item_id();
    let b = ids.next_item_id();
    assert_ne!(
        a.as_str(),
        b.as_str(),
        "ids must be unique, not a no-op constant"
    );
}
