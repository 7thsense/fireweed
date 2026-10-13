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
    pub overflow: bool,
}
impl RecoverySnapshot {
    /// Valid only for quiescent endpoints. Saturated or inconsistent evidence
    /// cannot produce a numerical delta.
    pub fn checked_delta(self, before: Self) -> Option<Self> {
        for snapshot in [before, self] {
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
        RecoverySpan {
            counters: self.clone(),
            started: Instant::now(),
            status: None,
            records: 0,
            bytes: 0,
            overflow: false,
        }
    }
}
pub(super) struct RecoverySpan {
    counters: Arc<RecoveryCounters>,
    started: Instant,
    status: Option<bool>,
    records: u64,
    bytes: u64,
    overflow: bool,
}
impl RecoverySpan {
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
        let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or_else(|_| {
            self.overflow = true;
            u64::MAX
        });
        if self.overflow {
            self.counters.overflow.store(true, Ordering::Relaxed);
        }
        self.counters.add(&self.counters.records, self.records);
        self.counters.add(&self.counters.bytes, self.bytes);
        self.counters.add(&self.counters.elapsed_ns, elapsed);
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
}
