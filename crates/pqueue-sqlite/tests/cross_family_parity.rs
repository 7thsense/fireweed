//! BQ-13 — head-to-head two-family CORE parity (ADR-008 §2 / TD-001).
//!
//! Runs ONE arbitrary command sequence against a representative of EACH projection family and asserts
//! their observable read-state (metrics, eligibility order, peek, the token-bearing pending set) is
//! identical at every step:
//!   - in-memory log-replay family  → `pqueue_memory::MemoryBackend` (shared `ProjectionData`)
//!   - relational DB-authoritative  → `pqueue_sqlite::SqliteRelationalBackend` (`pqueue_items` SQL)
//!
//! This is a stronger guarantee than each family independently passing the same fixed `core_suite!`
//! scenarios: it pins them identical on an arbitrary sequence, head to head. The postgres-relational half
//! of the matrix is exercised by the same `core_suite!`/reconnect scenarios under `PQUEUE_PG_TEST_URL`
//! (env-gated, deferred-with-reason here — no live DB); a postgres-vs-in-memory differential is the live-DB
//! extension of this test (convergence-review I3).

use pqueue_memory::MemoryBackend;
use pqueue_sqlite::SqliteRelationalBackend;

#[tokio::test]
async fn in_memory_and_relational_families_are_core_identical() {
    pqueue_conformance::scenarios::cross_family_core_parity(MemoryBackend::new, || {
        SqliteRelationalBackend::in_memory().expect("open :memory: relational backend")
    })
    .await;
}
