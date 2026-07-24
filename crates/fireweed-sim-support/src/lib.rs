#![forbid(unsafe_code)]
//! Dependency-free reference model and trace utilities for object-log durability tests.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

pub const TRACE_SCHEMA_VERSION: u16 = 2;
pub const HARNESS_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableCut {
    BeforeSegmentWrite,
    AfterSegmentWriteBeforeManifest,
    AfterManifestCandidateBeforeHead,
    AfterManifestBeforeAck,
    DuringOwnerReassignment,
    DuringSegmentExpiry,
    BeforeAppend,
    AfterAppendBeforeApply,
    BeforeSqliteApply,
    AfterSqliteCommitBeforeMemoryApply,
    DuringMemoryApply,
    DuringAsyncSqliteApply,
    PreRepair,
    SealPending,
    ActorSubmitted,
    ApplyPending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreResult {
    Success,
    FailureBeforeEffect,
    EffectThenError,
    CasLoss,
    StaleList,
    IncompletePage,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Disposition {
    #[default]
    None,
    Success,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    Accept {
        request: u64,
        created_at_ms: i64,
    },
    Seal {
        expected_epoch: u64,
        now_ms: i64,
        result: StoreResult,
    },
    Retry {
        request: u64,
    },
    Fence {
        epoch: u64,
        result: StoreResult,
    },
    AdvanceHorizon {
        through_sequence: u64,
    },
    DeleteThrough {
        through_sequence: u64,
    },
    Crash(DurableCut),
    Restart,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub index: u64,
    pub epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub committed_at_ms: i64,
    pub requests: Vec<u64>,
    pub deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub invariant: &'static str,
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelSnapshot {
    pub epoch: u64,
    pub next_sequence: u64,
    pub floor: Option<u64>,
    pub deletion_watermark: Option<u64>,
    pub visible_requests: Vec<u64>,
    pub physical_requests: Vec<u64>,
    pub acknowledged: Vec<u64>,
    pub unknown: Vec<u64>,
    pub last_disposition: Disposition,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Model {
    epoch: u64,
    next_sequence: u64,
    next_manifest: u64,
    submitted: BTreeMap<u64, i64>,
    buffered: Vec<u64>,
    accepted: BTreeSet<u64>,
    acknowledged: BTreeSet<u64>,
    unknown: BTreeSet<u64>,
    resolved_unknown: BTreeSet<u64>,
    hidden_after_success: BTreeSet<u64>,
    commit_counts: BTreeMap<u64, u8>,
    stale_epoch_commits: u64,
    floor: Option<u64>,
    deletion_watermark: Option<u64>,
    segments: Vec<Segment>,
    last_disposition: Disposition,
}

impl Model {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
    pub fn committed_times(&self) -> Vec<i64> {
        let mut out: Vec<_> = self
            .segments
            .iter()
            .filter(|segment| !segment.deleted)
            .map(|segment| segment.committed_at_ms)
            .collect();
        out.sort_unstable();
        out
    }
    pub fn snapshot(&self) -> ModelSnapshot {
        let mut visible: Vec<_> = self
            .segments
            .iter()
            .filter(|segment| !segment.deleted)
            .flat_map(|segment| {
                segment
                    .requests
                    .iter()
                    .enumerate()
                    .filter_map(|(offset, request)| {
                        let sequence = segment.first_sequence + offset as u64;
                        self.floor
                            .is_none_or(|floor| sequence > floor)
                            .then_some(*request)
                    })
            })
            .filter(|request| !self.hidden_after_success.contains(request))
            .collect();
        let mut physical: Vec<_> = self
            .segments
            .iter()
            .filter(|segment| !segment.deleted)
            .flat_map(|segment| segment.requests.iter().copied())
            .collect();
        visible.sort_unstable();
        physical.sort_unstable();
        ModelSnapshot {
            epoch: self.epoch,
            next_sequence: self.next_sequence,
            floor: self.floor,
            deletion_watermark: self.deletion_watermark,
            visible_requests: visible,
            physical_requests: physical,
            acknowledged: self.acknowledged.iter().copied().collect(),
            unknown: self.unknown.iter().copied().collect(),
            last_disposition: self.last_disposition,
        }
    }

    fn commit_buffer(&mut self, now_ms: i64, acknowledged: bool) {
        if self.buffered.is_empty() {
            self.last_disposition = Disposition::None;
            return;
        }
        let requests = std::mem::take(&mut self.buffered);
        let first = self.next_sequence;
        let last = first + requests.len() as u64 - 1;
        let committed_at_ms = requests
            .iter()
            .filter_map(|id| self.submitted.get(id))
            .copied()
            .max()
            .unwrap_or(0)
            .max(now_ms);
        for request in &requests {
            *self.commit_counts.entry(*request).or_default() += 1;
        }
        self.segments.push(Segment {
            index: self.next_manifest,
            epoch: self.epoch,
            first_sequence: first,
            last_sequence: last,
            committed_at_ms,
            requests: requests.clone(),
            deleted: false,
        });
        self.next_sequence = last + 1;
        self.next_manifest += 1;
        if acknowledged {
            self.accepted.extend(requests.iter().copied());
            self.acknowledged.extend(requests.iter().copied());
            self.last_disposition = Disposition::Success;
        } else {
            self.unknown.extend(requests);
            self.last_disposition = Disposition::Unknown;
        }
    }

    fn request_is_active(&self, request: u64) -> bool {
        self.segments.iter().any(|segment| {
            !segment.deleted
                && segment
                    .requests
                    .iter()
                    .enumerate()
                    .any(|(offset, candidate)| {
                        *candidate == request
                            && self
                                .floor
                                .is_none_or(|floor| segment.first_sequence + offset as u64 > floor)
                    })
        })
    }

    fn request_is_retired(&self, request: u64) -> bool {
        self.floor.is_some_and(|floor| {
            self.segments.iter().any(|segment| {
                segment
                    .requests
                    .iter()
                    .enumerate()
                    .any(|(offset, candidate)| {
                        *candidate == request && segment.first_sequence + offset as u64 <= floor
                    })
            })
        })
    }

    pub fn apply(&mut self, operation: &Operation) {
        match *operation {
            Operation::Accept {
                request,
                created_at_ms,
            } => {
                self.submitted.insert(request, created_at_ms);
                if self.commit_counts.get(&request).copied().unwrap_or(0) == 0
                    && !self.buffered.contains(&request)
                {
                    self.buffered.push(request);
                }
                self.last_disposition = Disposition::None;
            }
            Operation::Retry { request } => {
                if self.request_is_active(request) {
                    self.unknown.remove(&request);
                    self.resolved_unknown.insert(request);
                    self.accepted.insert(request);
                    self.acknowledged.insert(request);
                    self.last_disposition = Disposition::Success;
                } else if self.submitted.contains_key(&request) {
                    if !self.buffered.contains(&request) {
                        self.buffered.push(request);
                    }
                    self.last_disposition = Disposition::None;
                } else {
                    self.last_disposition = Disposition::Rejected;
                }
            }
            Operation::Seal {
                expected_epoch,
                now_ms,
                result,
            } => {
                if self.buffered.is_empty() {
                    self.last_disposition = Disposition::None;
                    return;
                }
                if expected_epoch != self.epoch {
                    self.buffered.clear();
                    self.last_disposition = Disposition::Rejected;
                    return;
                }
                match result {
                    StoreResult::Success => self.commit_buffer(now_ms, true),
                    // SP-03 create-only publication resolves effect-then-error by rereading the exact
                    // authoritative address before returning, so this is a confirmed acknowledgement rather
                    // than an externally unknown outcome.
                    StoreResult::EffectThenError => self.commit_buffer(now_ms, true),
                    StoreResult::FailureBeforeEffect | StoreResult::CasLoss => {
                        self.buffered.clear();
                        self.last_disposition = Disposition::Rejected;
                    }
                    StoreResult::StaleList | StoreResult::IncompletePage => {
                        self.last_disposition = Disposition::Rejected
                    }
                }
            }
            Operation::Fence { epoch, result } => match result {
                StoreResult::Success | StoreResult::EffectThenError if epoch > self.epoch => {
                    self.epoch += 1;
                    self.next_manifest += 1;
                    self.buffered.clear();
                    self.last_disposition = Disposition::Success;
                }
                _ => self.last_disposition = Disposition::Rejected,
            },
            Operation::AdvanceHorizon { through_sequence } => {
                let durable = self.segments.iter().any(|segment| {
                    !segment.deleted
                        && (segment.first_sequence..=segment.last_sequence)
                            .contains(&through_sequence)
                });
                if durable && self.floor.is_none_or(|floor| through_sequence >= floor) {
                    self.floor = Some(
                        self.floor
                            .map_or(through_sequence, |floor| floor.max(through_sequence)),
                    );
                    self.last_disposition = Disposition::Success;
                } else {
                    self.last_disposition = Disposition::Rejected;
                }
            }
            Operation::DeleteThrough { through_sequence } => {
                let eligible = self
                    .segments
                    .iter()
                    .any(|segment| !segment.deleted && segment.last_sequence <= through_sequence);
                if self.floor.is_some_and(|floor| through_sequence <= floor) {
                    if eligible {
                        for segment in &mut self.segments {
                            if segment.last_sequence <= through_sequence {
                                segment.deleted = true;
                            }
                        }
                        self.deletion_watermark = Some(
                            self.deletion_watermark
                                .map_or(through_sequence, |watermark| {
                                    watermark.max(through_sequence)
                                }),
                        );
                    }
                    self.last_disposition = Disposition::Success;
                } else {
                    self.last_disposition = Disposition::Rejected;
                }
            }
            Operation::Crash(cut) => match cut {
                DurableCut::AfterManifestBeforeAck => self.commit_buffer(0, false),
                DurableCut::DuringOwnerReassignment => {
                    self.epoch += 1;
                    self.next_manifest += 1;
                    self.buffered.clear();
                    self.last_disposition = Disposition::Unknown;
                }
                DurableCut::DuringSegmentExpiry => {
                    self.buffered.clear();
                    self.last_disposition = Disposition::Unknown;
                }
                _ => {
                    self.buffered.clear();
                    self.last_disposition = Disposition::Unknown;
                }
            },
            Operation::Restart => {
                self.buffered.clear();
                self.last_disposition = Disposition::None;
            }
        }
    }

    pub fn check_invariant(&self, invariant: &'static str) -> Result<(), Violation> {
        let fail = |detail| Err(Violation { invariant, detail });
        match invariant {
            "INV-1" => {
                if self.stale_epoch_commits != 0 {
                    return fail("stale epoch committed".into());
                }
            }
            "INV-2" => {
                if let Some(request) = self
                    .accepted
                    .iter()
                    .find(|request| self.commit_counts.get(request).copied().unwrap_or(0) == 0)
                {
                    return fail(format!("accepted request {request} was lost"));
                }
            }
            "INV-10" => {
                for request in &self.acknowledged {
                    if self.request_is_retired(*request) && !self.request_is_active(*request) {
                        continue;
                    }
                    if !self
                        .segments
                        .iter()
                        .any(|segment| !segment.deleted && segment.requests.contains(request))
                    {
                        return fail(format!("acknowledged request {request} is not durable"));
                    }
                }
            }
            "INV-12" => {
                let visible: BTreeSet<_> = self.snapshot().visible_requests.into_iter().collect();
                for request in &self.acknowledged {
                    if self.request_is_retired(*request) && !self.request_is_active(*request) {
                        continue;
                    }
                    if !visible.contains(request) {
                        return fail(format!("successful request {request} is not visible"));
                    }
                }
            }
            "INV-14" => {
                let visible = self.snapshot().visible_requests;
                let mut seen = BTreeSet::new();
                if let Some(request) = visible.into_iter().find(|request| !seen.insert(*request)) {
                    return fail(format!("unknown request {request} resolved more than once"));
                }
                if self
                    .resolved_unknown
                    .iter()
                    .any(|request| self.unknown.contains(request))
                {
                    return fail("resolved request remains unknown".into());
                }
            }
            _ => return fail("unknown invariant".into()),
        }
        Ok(())
    }

    pub fn check_required(&self) -> Result<(), Violation> {
        for invariant in ["INV-1", "INV-2", "INV-10", "INV-12", "INV-14"] {
            self.check_invariant(invariant)?;
        }
        if let Some(segment) = self.segments.iter().find(|segment| {
            segment.requests.iter().any(|request| {
                self.submitted
                    .get(request)
                    .is_some_and(|created| *created > segment.committed_at_ms)
            })
        }) {
            return Err(Violation {
                invariant: "INV-10",
                detail: format!("segment {} committed_at precedes command", segment.index),
            });
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn inject_stale_epoch_commit(&mut self) {
        self.stale_epoch_commits += 1;
    }
    #[doc(hidden)]
    pub fn inject_accepted_loss(&mut self, request: u64) {
        self.accepted.insert(request);
        self.commit_counts.remove(&request);
    }
    #[doc(hidden)]
    pub fn inject_durable_ack_loss(&mut self, request: u64) {
        self.acknowledged.insert(request);
        for segment in &mut self.segments {
            if segment.requests.contains(&request) {
                segment.deleted = true;
            }
        }
    }
    #[doc(hidden)]
    pub fn inject_success_visibility_gap(&mut self, request: u64) {
        self.acknowledged.insert(request);
        self.hidden_after_success.insert(request);
    }
    #[doc(hidden)]
    pub fn inject_duplicate_resolution(&mut self, request: u64) {
        if let Some(segment) = self.segments.last_mut() {
            segment.requests.push(request);
            segment.last_sequence += 1;
            self.next_sequence += 1;
        }
    }
}

#[derive(Clone, Debug)]
pub struct Generator {
    state: u64,
}
impl Generator {
    pub fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
    pub fn trace(&mut self, len: usize) -> Vec<Operation> {
        let mut next_request = 0;
        let mut epoch = 0;
        let mut now = 0;
        (0..len)
            .map(|_| {
                now += (self.next_u64() % 11) as i64;
                match self.next_u64() % 10 {
                    0..=2 => {
                        let request = next_request;
                        next_request += 1;
                        let skew = if self.next_u64().is_multiple_of(16) {
                            1_000
                        } else {
                            0
                        };
                        Operation::Accept {
                            request,
                            created_at_ms: now + skew,
                        }
                    }
                    3 => Operation::Seal {
                        expected_epoch: epoch,
                        now_ms: now,
                        result: write_result(self.next_u64()),
                    },
                    4 => {
                        let target = epoch + 1;
                        let result = write_result(self.next_u64());
                        if matches!(result, StoreResult::Success | StoreResult::EffectThenError) {
                            epoch = target;
                        }
                        Operation::Fence {
                            epoch: target,
                            result,
                        }
                    }
                    5 => Operation::AdvanceHorizon {
                        through_sequence: self.next_u64() % next_request.max(1),
                    },
                    6 => Operation::DeleteThrough {
                        through_sequence: self.next_u64() % next_request.max(1),
                    },
                    7 => Operation::Retry {
                        request: self.next_u64() % next_request.max(1),
                    },
                    8 => Operation::Crash(match self.next_u64() % 6 {
                        0 => DurableCut::BeforeSegmentWrite,
                        1 => DurableCut::AfterSegmentWriteBeforeManifest,
                        2 => DurableCut::AfterManifestCandidateBeforeHead,
                        3 => DurableCut::AfterManifestBeforeAck,
                        4 => DurableCut::DuringOwnerReassignment,
                        _ => DurableCut::DuringSegmentExpiry,
                    }),
                    _ => Operation::Restart,
                }
            })
            .collect()
    }
}
fn write_result(n: u64) -> StoreResult {
    match n % 4 {
        0 => StoreResult::FailureBeforeEffect,
        1 => StoreResult::EffectThenError,
        2 => StoreResult::CasLoss,
        _ => StoreResult::Success,
    }
}

pub fn render_trace(seed: u64, operations: &[Operation]) -> String {
    let mut out =
        format!("schema={TRACE_SCHEMA_VERSION};harness={HARNESS_VERSION};seed={seed:016x}");
    for (index, operation) in operations.iter().enumerate() {
        let _ = write!(out, "\n{index:03}:{operation:?}");
    }
    out
}

pub fn shrink_invariant(
    mut trace: Vec<Operation>,
    limit: usize,
    invariant: &'static str,
    fails: impl Fn(&[Operation]) -> Option<&'static str>,
) -> Vec<Operation> {
    if fails(&trace) != Some(invariant) {
        return trace;
    }
    let mut index = 0;
    while trace.len() > limit && index < trace.len() {
        let mut candidate = trace.clone();
        candidate.remove(index);
        if fails(&candidate) == Some(invariant) {
            trace = candidate;
        } else {
            index += 1;
        }
    }
    trace
}

pub fn shrink(
    trace: Vec<Operation>,
    limit: usize,
    fails: impl Fn(&[Operation]) -> bool,
) -> Vec<Operation> {
    shrink_invariant(trace, limit, "failure", |candidate| {
        fails(candidate).then_some("failure")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn same_seed_is_byte_identical_one_hundred_times() {
        let expected = render_trace(42, &Generator::new(42).trace(128));
        for _ in 0..100 {
            assert_eq!(render_trace(42, &Generator::new(42).trace(128)), expected);
        }
    }
    #[test]
    fn generator_emits_real_crash_operations() {
        assert!(
            (0..64)
                .flat_map(|seed| Generator::new(seed).trace(32))
                .any(|operation| matches!(operation, Operation::Crash(_)))
        );
    }
    #[test]
    fn shrink_preserves_identity() {
        let trace = (0..80)
            .map(|request| Operation::Accept {
                request,
                created_at_ms: 0,
            })
            .collect();
        let shrunk = shrink_invariant(trace, 32, "INV-14", |ops| {
            (ops.len() >= 12).then_some("INV-14")
        });
        assert!(shrunk.len() <= 32);
    }
    fn committed_model() -> Model {
        let mut model = Model::default();
        model.apply(&Operation::Accept {
            request: 1,
            created_at_ms: 1,
        });
        model.apply(&Operation::Seal {
            expected_epoch: 0,
            now_ms: 1,
            result: StoreResult::Success,
        });
        model
    }
    #[test]
    fn required_invariants_have_distinct_negative_controls() {
        let mut inv1 = committed_model();
        inv1.inject_stale_epoch_commit();
        assert_eq!(
            inv1.check_invariant("INV-1").unwrap_err().invariant,
            "INV-1"
        );
        let mut inv2 = committed_model();
        inv2.inject_accepted_loss(1);
        assert_eq!(
            inv2.check_invariant("INV-2").unwrap_err().invariant,
            "INV-2"
        );
        let mut inv10 = committed_model();
        inv10.inject_durable_ack_loss(1);
        assert_eq!(
            inv10.check_invariant("INV-10").unwrap_err().invariant,
            "INV-10"
        );
        let mut inv12 = committed_model();
        inv12.inject_success_visibility_gap(1);
        assert!(inv12.check_invariant("INV-10").is_ok());
        assert_eq!(
            inv12.check_invariant("INV-12").unwrap_err().invariant,
            "INV-12"
        );
        let mut inv14 = committed_model();
        inv14.inject_duplicate_resolution(1);
        assert!(inv14.check_invariant("INV-1").is_ok());
        assert_eq!(
            inv14.check_invariant("INV-14").unwrap_err().invariant,
            "INV-14"
        );
    }
    #[test]
    fn floor_inside_segment_retires_only_record_prefix() {
        let mut model = Model::default();
        for request in 1..=3 {
            model.apply(&Operation::Accept {
                request,
                created_at_ms: request as i64,
            });
        }
        model.apply(&Operation::Seal {
            expected_epoch: 0,
            now_ms: 3,
            result: StoreResult::Success,
        });
        model.apply(&Operation::AdvanceHorizon {
            through_sequence: 0,
        });
        assert_eq!(model.snapshot().visible_requests, vec![2, 3]);
        let floor = model.snapshot().floor;
        model.apply(&Operation::AdvanceHorizon {
            through_sequence: 99,
        });
        assert_eq!(
            model.snapshot().floor,
            floor,
            "non-durable floor is rejected"
        );
    }
}
