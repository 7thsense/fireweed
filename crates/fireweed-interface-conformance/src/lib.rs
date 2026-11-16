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

use fireweed_interface::{DataPlane, DpResult, ManualClock};

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

/// Opens an instance on the fixture's storage, attached to `clock`, or
/// returns the error the open fails with.
pub type Opener = Arc<dyn Fn(Arc<ManualClock>) -> DpResult<Arc<dyn DataPlane>> + Send + Sync>;

/// Turns a fault on (`true`) or off (`false`).
pub type Switch = Arc<dyn Fn(bool) + Send + Sync>;

/// Engine controls for the durability and ownership tests (DUR-3 to DUR-6,
/// SHARE-1, FENCE-1). Each is optional: a test whose control the engine
/// does not supply is skipped and names the control, so every engine runs
/// the same tests and reports the same gaps.
#[derive(Clone, Default)]
pub struct Controls {
    /// `true` stops commits from reaching `Durable` (nothing new reaches
    /// storage outside the node); `false` releases them. DUR-3, DUR-6.
    pub hold_replication: Option<Switch>,
    /// Abandons the fixture's instance without a clean close, as a crash
    /// would (a killed child process where the engine can run one), and
    /// opens a new instance on the same storage. DUR-4, SHARE-1.
    pub crash_and_reopen: Option<Opener>,
    /// Abandons the fixture's instance, discards its local storage, and
    /// opens a new instance from what reached `Durable`. DUR-5.
    pub lose_local_storage: Option<Opener>,
    /// Opens the fixture's space a second time on the same host while the
    /// first instance lives, without a new ownership grant. SHARE-1
    /// expects it to fail.
    pub open_competing: Option<Opener>,
    /// The control plane grants the fixture's space to a new owner at a
    /// higher epoch, which takes it over; returns the new owner. FENCE-1.
    pub take_over: Option<Opener>,
}

/// A fresh implementation on fresh storage, optionally a way to open more
/// instances on that storage (tests marked "needs a peer" use it), and the
/// engine's controls.
pub struct Instance {
    pub plane: Arc<dyn DataPlane>,
    pub peer: Option<Peer>,
    pub controls: Controls,
}

impl Instance {
    pub fn new(plane: Arc<dyn DataPlane>) -> Self {
        Self {
            plane,
            peer: None,
            controls: Controls::default(),
        }
    }

    pub fn with_peer(mut self, peer: Peer) -> Self {
        self.peer = Some(peer);
        self
    }

    pub fn with_controls(mut self, controls: Controls) -> Self {
        self.controls = controls;
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
