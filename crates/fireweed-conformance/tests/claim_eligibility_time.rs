//! Cross-family evidence for the explicit claim eligibility epoch (`ClaimRequest::eligibility_time`).
//!
//! [`scenarios::claim_with_explicit_eligibility_time`] is part of the CORE class, so every adapter already
//! runs it from its own suite invocation. It is pinned HERE as well because the two claim implementations
//! that must satisfy it are structurally different — the generic composition (`ComposedBackend`, which the
//! memory family is) selects candidates through the projection port, while the sqlite-relational monolith
//! selects them in its own claim transaction's SQL — and a regression in either one is a silent
//! wrong-work-selected bug for a caller scheduling at an explicit epoch. Running both from one file keeps
//! that pairing visible instead of leaving it implicit across two adapter crates.

use fireweed_memory::composed_memory_backend;
use fireweed_sqlite::{SqliteRelationalBackend, composed_sqlite_relational_in_memory};

/// The composition path (memory family): eligibility resolved through `ProjectionStore::eligible_candidates`.
#[tokio::test]
async fn claim_with_explicit_eligibility_time_memory() {
    fireweed_conformance::scenarios::claim_with_explicit_eligibility_time(composed_memory_backend)
        .await;
}

/// The native-SQL path: the sqlite-relational monolith selects inside its own claim transaction.
#[tokio::test]
async fn claim_with_explicit_eligibility_time_sqlite_relational() {
    fireweed_conformance::scenarios::claim_with_explicit_eligibility_time(|| {
        SqliteRelationalBackend::in_memory().expect("open in-memory relational backend")
    })
    .await;
}

/// The same relational store driven through the generic composition (ADR-012), which resolves eligibility
/// through the projection port rather than the monolith's claim SQL.
#[tokio::test]
async fn claim_with_explicit_eligibility_time_composed_sqlite_relational() {
    fireweed_conformance::scenarios::claim_with_explicit_eligibility_time(|| {
        composed_sqlite_relational_in_memory()
            .expect("compose in-memory unified sqlite-relational backend")
    })
    .await;
}
