use super::*;
use crate::queue_admission::QueueAdmission;
fn job() -> (Arc<QueueAdmission>, Arc<QueueReservation>) {
    let root = QueueAdmission::new(2, 8192).unwrap();
    let job = Arc::new(root.reserve(0).unwrap());
    (root, job)
}
fn units() -> QueueSourceSelectionWork {
    QueueSourceSelectionWork {
        rows: 2,
        copied_bytes: 5,
        scratch_steps: 3,
    }
}
#[test]
fn work_dimensions_are_atomic_at_exact_limit_and_one_less() {
    let (_, job) = job();
    let work = units();
    for limits in [
        work,
        QueueSourceSelectionWork { rows: 1, ..work },
        QueueSourceSelectionWork {
            copied_bytes: 4,
            ..work
        },
        QueueSourceSelectionWork {
            scratch_steps: 2,
            ..work
        },
    ] {
        let budget = QueueSourceSelectionBudget::new(limits, &job).unwrap();
        let result = budget.grant(&job, work);
        assert_eq!(result.is_ok(), limits == work);
        assert_eq!(
            budget.remaining(&job).unwrap(),
            if limits == work {
                QueueSourceSelectionWork::default()
            } else {
                limits
            }
        );
        if result.is_ok() {
            assert!(
                budget
                    .grant(
                        &job,
                        QueueSourceSelectionWork {
                            rows: 1,
                            ..Default::default()
                        }
                    )
                    .is_err()
            );
        }
    }
}
#[test]
fn attempt_handle_drop_does_not_refund_work_or_reset_the_request_ledger() {
    let (_, job) = job();
    let budget = QueueSourceSelectionBudget::new(
        QueueSourceSelectionWork {
            rows: 4,
            copied_bytes: 10,
            scratch_steps: 6,
        },
        &job,
    )
    .unwrap();
    let attempt = budget.clone();
    attempt.grant(&job, units()).unwrap();
    drop(attempt);
    let recapture = budget.clone();
    assert_eq!(recapture.remaining(&job).unwrap(), units());
    recapture.grant(&job, units()).unwrap();
    drop(recapture);
    assert_eq!(
        budget.remaining(&job).unwrap(),
        QueueSourceSelectionWork::default()
    );
    assert!(budget.grant(&job, units()).is_err());
}
#[test]
fn foreign_job_is_refused_before_any_counter_changes() {
    let (root, job) = job();
    let other = root.reserve(0).unwrap();
    let budget = QueueSourceSelectionBudget::new(units(), &job).unwrap();
    assert!(budget.grant(&other, units()).is_err());
    assert!(budget.remaining(&other).is_err());
    assert_eq!(budget.remaining(&job).unwrap(), units());
    budget.grant(&job, units()).unwrap();
}
#[test]
fn worker_held_ledger_retains_actual_memory_owner_after_caller_handles_drop() {
    let (root, job) = job();
    let budget = QueueSourceSelectionBudget::new(units(), &job).unwrap();
    let held = root.usage().unwrap();
    assert_eq!(
        held,
        (
            1,
            (std::mem::size_of::<QueueSourceSelectionBudget>() + 2 * std::mem::size_of::<usize>())
                as u64
        )
    );
    let weak = Arc::downgrade(&budget);
    let worker = budget.clone();
    let mut pending = Box::pin(async move {
        let _owner = worker;
        std::future::pending::<()>().await;
    });
    let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
    assert!(std::future::Future::poll(pending.as_mut(), &mut context).is_pending());
    // This is synthetic future retention, not a physical I/O retirement proof.
    drop(budget);
    assert!(weak.upgrade().is_some());
    assert_eq!(root.usage().unwrap(), held);
    drop(pending);
    assert!(weak.upgrade().is_none());
    assert_eq!(root.usage().unwrap(), (1, 0));
    drop(job);
    assert_eq!(root.usage().unwrap(), (0, 0));
}
#[test]
fn concurrent_grants_cannot_overspend_or_partially_debit_dimensions() {
    let (_, job) = job();
    let budget = QueueSourceSelectionBudget::new(
        QueueSourceSelectionWork {
            rows: 8,
            copied_bytes: 8,
            scratch_steps: 8,
        },
        &job,
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let budget = budget.clone();
            let job = job.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                budget
                    .grant(
                        &job,
                        QueueSourceSelectionWork {
                            rows: 1,
                            copied_bytes: 1,
                            scratch_steps: 1,
                        },
                    )
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|success| *success)
            .count(),
        8
    );
    assert_eq!(
        budget.remaining(&job).unwrap(),
        QueueSourceSelectionWork::default()
    );
}
