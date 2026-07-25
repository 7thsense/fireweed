//! # Relational projection family (sqlite) — BQ-11a
//!
//! A second rebuildable projection family for sqlite (ADR-008 / TD-001 relational class), distinct from
//! the log-replay [`crate::ComposedSqliteBackend`]. Here the `fireweed_items` SQL table holds the durable
//! projection cache (TD-002 columns): every lifecycle command is applied as SQL INSERT/UPDATE/DELETE
//! against `fireweed_items` inside the unit of work, and reads (eligibility, peek, pending, metrics) are
//! SQL queries over it. There is **no** shared in-memory [`fireweed_projection::ProjectionData`] and **no**
//! command log - a reopen recovers committed state from the table itself (the relational-reconnect
//! class, proven in BQ-11d), not by replaying a log.
//!
//! Scope (plan §2): BQ-11a = the schema + the 14-arm apply-UoW. BQ-11b = the serialized claim CTE
//! (candidate-select + lease in one transaction) + Eligibility Precedence in SQL, wiring the full
//! `core_suite!(@atomic)` at parity with the in-memory reference. BQ-11c = `fireweed_group_summary`
//! (maintained in-transaction with every grouped-item mutation; consumer is BQ-14 g1/g4) + the
//! `client_item_key` retention tombstone (`fireweed_item_key_retention`) for duplicate-push convergence
//! across a purge. Still ahead: the relational-reconnect suite (BQ-11d) and group/cohort/gate selection
//! (BQ-14). `progress_guard_sort` bounded-relaxed promotion is a cross-family enhancement deferred so the
//! two projection families never diverge on the core class.
//!
//! RELATIONAL-ONLY (deliberately OUT of the shared core class): the retention tombstone makes
//! push→purge→re-push(same key) return `Terminal` here for every lifecycle state at removal, whereas the
//! log-replay/in-memory family
//! (no retention) would `Insert` a fresh item. No core conformance scenario exercises that sequence, so
//! the "two families identical on core" invariant holds; BQ-13 must keep retention (and `group_summary`)
//! a relational-class concern, NOT add it to the shared core suite — else the families would diverge.
//!
//! REQUEST-ID IDEMPOTENCY (BQ-11e slice): `fireweed_request_idempotency` is wired for the first
//! request-id-carrying data-plane paths, including BatchPush and ClaimByQuery. That proves the TD-002
//! relational table/replay flow without claiming full API-001 coverage for every mutating operation.
//! Finalize/update replay remain later request-id-carrying port work.
//!
//! ## Lease tokens (TD-004 §security / TD-002 parity)
//! The durable projection stores only the lease token **hash** (`lease_token_hash`, never the cleartext
//! token). Except for the bounded ClaimByQuery request-replay record, the cleartext token lives in an
//! ephemeral in-process map ([`Inner::live_tokens`]) so
//! `pending()` / `claimed_view()` return the real token at parity with the in-memory family. The at-rest
//! hash is currently inert (lease validation is by `(state, fenced, superseded)`, exactly like the
//! in-memory family — see [`validate_leased`] — never by presented-token comparison); it is persisted so
//! the column is populated for the production posture where an owner validates a presented token's hash.
//!
//! INTENTIONAL DIVERGENCE (flagged for BQ-11d reconnect): a crash/reopen drops the live tokens (only the
//! hash survives) while item *state* persists in `fireweed_items`. So a still-`Leased` item is present in
//! `fireweed_items` after reopen but is **omitted** from `pending()`/`claimed_view()` (its cleartext token
//! is gone) — unlike the log-replay family, which reconstructs the token by replaying the `Claim`
//! command. This is the relational family's by-design recovery semantics (the token is a worker
//! capability, not durable server state; a ClaimByQuery retry can reconstruct its retained token from
//! the request replay record, while any other tokenless in-flight lease is reclaimed by the epoch owner),
//! which is why the relational-reconnect conformance scenario asserts only pending-item state. BQ-11d

use fireweed_engine::{ComposedBackend, EngineResult, InProcessControlPlane};

mod apply;
mod backend;
mod checkpoint;
mod helpers;
mod hybrid;
mod monitor;
mod projection;
#[cfg(test)]
pub(crate) use projection::EXPIRED_LEASES_BOUNDED_SQL;
mod query;
mod recovery;
mod unified;

pub(crate) use apply::*;
pub use backend::*;
pub use checkpoint::*;
pub(crate) use helpers::*;
pub use hybrid::*;
pub use monitor::*;
pub use projection::*;
pub(crate) use query::*;
pub(crate) use recovery::*;
pub use unified::*;

/// The composed unified sqlite-relational backend (ADR-012 P1b-ii):
/// `ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>` — one relational store on
/// both the log and projection axes, so append+apply commit as one transaction. Capability-equivalent to
/// the monolithic [`SqliteRelationalBackend`] on the CORE conformance class.
pub type ComposedSqliteRelationalBackend =
    ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>;

/// Assemble a unified sqlite-relational composition over an ephemeral `:memory:` store. Both axes are clones
/// of the SAME store (shared connection), so the orthogonal `commit_locked` drives one durable transaction.
pub fn composed_sqlite_relational_in_memory() -> EngineResult<ComposedSqliteRelationalBackend> {
    let store = SqliteRelational::in_memory()?;
    Ok(ComposedBackend::new(
        store.clone(),
        store,
        InProcessControlPlane::new(),
    ))
}

/// Assemble a unified sqlite-relational composition over a DURABLE store at `path`. Runs recovery-on-open
/// (ADR-012 P2): the durable relational cursor provides the replay start, while recovery repopulates the
/// in-process control plane from the durable `queues` catalog and re-seeds the id-mint counters.
pub fn composed_sqlite_relational(path: &str) -> EngineResult<ComposedSqliteRelationalBackend> {
    let store = SqliteRelational::open(path)?;
    ComposedBackend::new(store.clone(), store, InProcessControlPlane::new()).recover()
}

#[cfg(test)]
mod group_summary_tests;
