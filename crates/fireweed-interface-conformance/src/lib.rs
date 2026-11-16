//! Rule-pinning conformance tests for the Fireweed data-plane interface
//! (API-006 §Rule-pinning tests).
//!
//! Every test runs through `DataPlane` only, so the same vectors check every
//! implementation. Expression rules are exercised through `Mutate` `Set`
//! (action position, where faults reject) and `Where` dry runs (predicate
//! position, where faults yield null), not by calling the reference
//! evaluator, so a SQL implementation is tested on its own evaluation.
//!
//! An implementation's test calls [`run`] with a factory and asserts that
//! no test failed. Tests that need a capability or harness control the
//! implementation does not declare are reported as skipped, by name.

#![allow(clippy::neg_cmp_op_on_partial_ord)]

mod features;
mod scenarios;
mod support;
mod tests;

use std::sync::Arc;

use fireweed_interface::{DataPlane, ManualClock};

pub use support::Fixture;

/// Outcome of one rule-pinning test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Pass,
    Fail(String),
    Skip(String),
}

pub struct Report {
    pub results: Vec<(&'static str, Status)>,
}

impl Report {
    pub fn failures(&self) -> Vec<(&'static str, String)> {
        self.results
            .iter()
            .filter_map(|(id, s)| match s {
                Status::Fail(m) => Some((*id, m.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn count(&self, pred: impl Fn(&Status) -> bool) -> usize {
        self.results.iter().filter(|(_, s)| pred(s)).count()
    }

    pub fn summary(&self) -> String {
        let mut s = format!(
            "{} passed, {} failed, {} skipped\n",
            self.count(|s| *s == Status::Pass),
            self.count(|s| matches!(s, Status::Fail(_))),
            self.count(|s| matches!(s, Status::Skip(_)))
        );
        for (id, st) in &self.results {
            match st {
                Status::Pass => s.push_str(&format!("  PASS {id}\n")),
                Status::Fail(m) => s.push_str(&format!("  FAIL {id}: {m}\n")),
                Status::Skip(m) => s.push_str(&format!("  SKIP {id}: {m}\n")),
            }
        }
        s
    }
}

/// Opens another instance on the same storage as the one it came from,
/// attached to `clock`.
pub type Peer = Arc<dyn Fn(Arc<ManualClock>) -> Arc<dyn DataPlane> + Send + Sync>;

/// A fresh implementation on fresh storage, and optionally a way to open
/// more instances on that storage (tests marked "needs a peer" use it).
pub struct Instance {
    pub plane: Arc<dyn DataPlane>,
    pub peer: Option<Peer>,
}

impl Instance {
    pub fn new(plane: Arc<dyn DataPlane>) -> Self {
        Self { plane, peer: None }
    }

    pub fn with_peer(mut self, peer: Peer) -> Self {
        self.peer = Some(peer);
        self
    }
}

/// Builds a fresh implementation for each test, attached to `clock`.
pub type Factory<'a> = &'a dyn Fn(Arc<ManualClock>) -> Instance;

/// Runs every rule-pinning test and every DSL scenario.
pub fn run(factory: Factory<'_>) -> Report {
    let mut results = vec![];
    for (id, test) in tests::ALL.iter().chain(features::ALL).chain(scenarios::ALL) {
        let fx = match Fixture::new(factory) {
            Ok(fx) => fx,
            Err(e) => {
                results.push((*id, Status::Fail(format!("fixture: {e}"))));
                continue;
            }
        };
        let status = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| test(&fx))) {
            Ok(Ok(())) => Status::Pass,
            Ok(Err(support::Fail::Failed(m))) => Status::Fail(m),
            Ok(Err(support::Fail::Skipped(m))) => Status::Skip(m),
            Err(p) => Status::Fail(format!(
                "panic: {}",
                p.downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default()
            )),
        };
        results.push((*id, status));
    }
    Report { results }
}
