//! Fixed-size internal recovery diagnostics. Live snapshots are approximate.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Instant;
#[derive(Default)]
pub struct RecoveryCounters {
    attempts: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    records: AtomicU64,
    bytes: AtomicU64,
    elapsed_ns: AtomicU64,
    phase_ns: [AtomicU64; 4],
    overflow: AtomicBool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySnapshot {
    pub attempts: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub abandoned: u64,
    pub admitted_raw_records: u64,
    pub admitted_raw_bytes: u64,
    pub summed_recovery_task_elapsed_ns: u64,
    pub fresh_authority_ns: u64,
    pub planned_read_ns: u64,
    pub checked_fold_ns: u64,
    pub native_apply_ns: u64,
    pub overflow: bool,
}
impl RecoverySnapshot {
    /// Valid only for quiescent endpoints. Saturated or inconsistent evidence
    /// cannot produce a numerical delta.
    pub fn checked_delta(self, before: Self) -> Option<Self> {
        for snapshot in [before, self] {
            let phases = [
                snapshot.fresh_authority_ns,
                snapshot.planned_read_ns,
                snapshot.checked_fold_ns,
                snapshot.native_apply_ns,
            ];
            if phases
                .into_iter()
                .try_fold(0u64, |n, phase| n.checked_add(phase))
                != Some(snapshot.summed_recovery_task_elapsed_ns)
            {
                return None;
            }
            if snapshot.overflow
                || snapshot
                    .succeeded
                    .checked_add(snapshot.failed)
                    .and_then(|n| n.checked_add(snapshot.abandoned))
                    != Some(snapshot.attempts)
            {
                return None;
            }
        }
        Some(Self {
            attempts: self.attempts.checked_sub(before.attempts)?,
            succeeded: self.succeeded.checked_sub(before.succeeded)?,
            failed: self.failed.checked_sub(before.failed)?,
            abandoned: self.abandoned.checked_sub(before.abandoned)?,
            admitted_raw_records: self
                .admitted_raw_records
                .checked_sub(before.admitted_raw_records)?,
            admitted_raw_bytes: self
                .admitted_raw_bytes
                .checked_sub(before.admitted_raw_bytes)?,
            summed_recovery_task_elapsed_ns: self
                .summed_recovery_task_elapsed_ns
                .checked_sub(before.summed_recovery_task_elapsed_ns)?,
            fresh_authority_ns: self
                .fresh_authority_ns
                .checked_sub(before.fresh_authority_ns)?,
            planned_read_ns: self.planned_read_ns.checked_sub(before.planned_read_ns)?,
            checked_fold_ns: self.checked_fold_ns.checked_sub(before.checked_fold_ns)?,
            native_apply_ns: self.native_apply_ns.checked_sub(before.native_apply_ns)?,
            overflow: false,
        })
    }
}
impl RecoveryCounters {
    pub fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            admitted_raw_records: self.records.load(Ordering::Relaxed),
            admitted_raw_bytes: self.bytes.load(Ordering::Relaxed),
            summed_recovery_task_elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
            fresh_authority_ns: self.phase_ns[0].load(Ordering::Relaxed),
            planned_read_ns: self.phase_ns[1].load(Ordering::Relaxed),
            checked_fold_ns: self.phase_ns[2].load(Ordering::Relaxed),
            native_apply_ns: self.phase_ns[3].load(Ordering::Relaxed),
            overflow: self.overflow.load(Ordering::Relaxed),
        }
    }
    fn add(&self, counter: &AtomicU64, value: u64) {
        let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
            Some(old.checked_add(value).unwrap_or_else(|| {
                self.overflow.store(true, Ordering::Relaxed);
                u64::MAX
            }))
        });
    }
    pub(super) fn begin(self: &Arc<Self>) -> RecoverySpan {
        self.add(&self.attempts, 1);
        let started = Instant::now();
        RecoverySpan {
            phase: PhaseLedger::new(started),
            counters: self.clone(),
            started,
            status: None,
            records: 0,
            bytes: 0,
            overflow: false,
        }
    }
}
/// Read includes planning, worker await, transfer checks, and worker-side parsing.
/// Fold covers root token parsing and independent history/proof observers.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryPhase {
    FreshAuthority,
    PlannedRead,
    CheckedFold,
    NativeApply,
}
struct PhaseLedger {
    phase: RecoveryPhase,
    at: Instant,
    ns: [u64; 4],
    overflow: bool,
}
impl PhaseLedger {
    fn new(at: Instant) -> Self {
        Self {
            phase: RecoveryPhase::FreshAuthority,
            at,
            ns: [0; 4],
            overflow: false,
        }
    }
    fn close_at(&mut self, at: Instant) {
        let elapsed = at
            .checked_duration_since(self.at)
            .and_then(|duration| u64::try_from(duration.as_nanos()).ok());
        let position = self.phase as usize;
        if let Some(value) = elapsed.and_then(|value| self.ns[position].checked_add(value)) {
            self.ns[position] = value;
        } else {
            self.overflow = true;
            self.ns[position] = u64::MAX;
        }
        self.at = at;
    }
    fn transition_at(&mut self, phase: RecoveryPhase, at: Instant) {
        self.close_at(at);
        self.phase = phase;
    }
}
pub(super) struct RecoverySpan {
    counters: Arc<RecoveryCounters>,
    started: Instant,
    phase: PhaseLedger,
    status: Option<bool>,
    records: u64,
    bytes: u64,
    overflow: bool,
}
impl RecoverySpan {
    pub(super) fn phase(&mut self, phase: RecoveryPhase) {
        if self.phase.phase != phase {
            self.phase.transition_at(phase, Instant::now());
        }
    }
    pub(super) fn record(&mut self, bytes: usize) {
        self.records = self.records.checked_add(1).unwrap_or_else(|| {
            self.overflow = true;
            u64::MAX
        });
        self.bytes = self.bytes.checked_add(bytes as u64).unwrap_or_else(|| {
            self.overflow = true;
            u64::MAX
        });
    }
    pub(super) fn finish(&mut self, succeeded: bool) {
        self.status = Some(succeeded);
    }
}
impl Drop for RecoverySpan {
    fn drop(&mut self) {
        let finished = Instant::now();
        self.phase.close_at(finished);
        self.overflow |= self.phase.overflow;
        let elapsed = u64::try_from(finished.duration_since(self.started).as_nanos())
            .unwrap_or_else(|_| {
                self.overflow = true;
                u64::MAX
            });
        if self.overflow {
            self.counters.overflow.store(true, Ordering::Relaxed);
        }
        self.counters.add(&self.counters.records, self.records);
        self.counters.add(&self.counters.bytes, self.bytes);
        self.counters.add(&self.counters.elapsed_ns, elapsed);
        for (counter, value) in self.counters.phase_ns.iter().zip(self.phase.ns) {
            self.counters.add(counter, value);
        }
        let terminal = match self.status {
            Some(true) => &self.counters.succeeded,
            Some(false) => &self.counters.failed,
            None => &self.counters.abandoned,
        };
        self.counters.add(terminal, 1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn panic_and_failure_merge_admitted_work_without_success() {
        let counters = Arc::new(RecoveryCounters::default());
        let initial = counters.snapshot();
        let _ = std::panic::catch_unwind({
            let counters = counters.clone();
            move || {
                let mut span = counters.begin();
                span.record(9);
                panic!("injected span failure");
            }
        });
        {
            let mut span = counters.begin();
            span.record(13);
            span.finish(false);
        }
        let end = counters.snapshot().checked_delta(initial).unwrap();
        assert_eq!(
            (
                end.attempts,
                end.succeeded,
                end.failed,
                end.abandoned,
                end.admitted_raw_records,
                end.admitted_raw_bytes
            ),
            (2, 0, 1, 1, 2, 22)
        );
    }
    #[test]
    fn saturation_and_backward_snapshots_cannot_fabricate_deltas() {
        let counters = Arc::new(RecoveryCounters::default());
        let initial = counters.snapshot();
        counters.records.store(u64::MAX, Ordering::Relaxed);
        {
            let mut span = counters.begin();
            span.record(1);
            span.finish(true);
        }
        assert!(counters.snapshot().overflow);
        assert!(counters.snapshot().checked_delta(initial).is_none());
        let pending = Arc::new(RecoveryCounters::default());
        let initial_pending = pending.snapshot();
        let span = pending.begin();
        assert!(pending.snapshot().checked_delta(initial_pending).is_none());
        drop(span);
        assert_eq!(
            pending
                .snapshot()
                .checked_delta(initial_pending)
                .unwrap()
                .abandoned,
            1
        );
        let newer = RecoverySnapshot {
            attempts: 1,
            succeeded: 1,
            ..initial
        };
        assert!(initial.checked_delta(newer).is_none());
    }
    #[test]
    fn phase_ledger_partitions_transitions_with_one_clock_sample() {
        use std::time::Duration;
        let start = Instant::now();
        let mut ledger = PhaseLedger::new(start);
        ledger.transition_at(RecoveryPhase::PlannedRead, start + Duration::from_nanos(10));
        ledger.transition_at(RecoveryPhase::CheckedFold, start + Duration::from_nanos(30));
        ledger.transition_at(RecoveryPhase::NativeApply, start + Duration::from_nanos(35));
        ledger.transition_at(RecoveryPhase::CheckedFold, start + Duration::from_nanos(60));
        ledger.close_at(start + Duration::from_nanos(70));
        assert_eq!(ledger.ns, [10, 20, 15, 25]);
        assert_eq!(ledger.ns.iter().sum::<u64>(), 70);
        assert!(!ledger.overflow);
        let mut backwards = PhaseLedger::new(start + Duration::from_nanos(1));
        backwards.close_at(start);
        assert!(backwards.overflow);
        let mut overflow = PhaseLedger::new(start);
        overflow.ns[0] = u64::MAX;
        overflow.close_at(start + Duration::from_nanos(1));
        assert!(overflow.overflow);
    }
    #[test]
    fn phase_totals_survive_failed_and_panicking_operations_and_reject_bad_snapshots() {
        let counters = Arc::new(RecoveryCounters::default());
        let before = counters.snapshot();
        {
            let mut span = counters.begin();
            span.phase(RecoveryPhase::PlannedRead);
            span.phase(RecoveryPhase::CheckedFold);
            span.finish(false);
        }
        let _ = std::panic::catch_unwind({
            let counters = counters.clone();
            move || {
                let mut span = counters.begin();
                span.phase(RecoveryPhase::NativeApply);
                panic!("injected physical recovery panic");
            }
        });
        let after = counters.snapshot();
        let delta = after.checked_delta(before).unwrap();
        assert_eq!((delta.attempts, delta.failed, delta.abandoned), (2, 1, 1));
        assert_eq!(
            delta.fresh_authority_ns
                + delta.planned_read_ns
                + delta.checked_fold_ns
                + delta.native_apply_ns,
            delta.summed_recovery_task_elapsed_ns
        );
        let mut inconsistent = after;
        inconsistent.planned_read_ns += 1;
        assert!(inconsistent.checked_delta(before).is_none());
        let mut backwards = after;
        backwards.planned_read_ns = 0;
        backwards.fresh_authority_ns = backwards.summed_recovery_task_elapsed_ns
            - backwards.checked_fold_ns
            - backwards.native_apply_ns;
        assert!(backwards.checked_delta(after).is_none());
        counters.phase_ns[2].store(u64::MAX, Ordering::Relaxed);
        {
            let mut span = counters.begin();
            span.phase(RecoveryPhase::CheckedFold);
            span.finish(true);
        }
        assert!(counters.snapshot().overflow);
        assert!(counters.snapshot().checked_delta(after).is_none());
    }
}
