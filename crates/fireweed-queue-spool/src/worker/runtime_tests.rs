//! Native future entry preserves the existing physical owner and runtime scope.
use super::*;
use fireweed_engine::queue_admission::QueueAdmission;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
fn target() -> tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        })
        .handle()
        .clone()
}
async fn accepted_caller() {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let target = target();
    let expected = target.id();
    assert_ne!(tokio::runtime::Handle::current().id(), expected);
    let physical_pool = workers.inner.clone();
    let output = workers
        .run_on_runtime(target, move || {
            assert!(PhysicalScope::owns(&physical_pool));
            assert_eq!(tokio::runtime::Handle::current().id(), expected);
            async move {
                tokio::task::yield_now().await;
                assert!(PhysicalScope::owns(&physical_pool));
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                assert_eq!(tokio::runtime::Handle::current().id(), expected);
                assert_eq!(tokio::spawn(async { 19 }).await.unwrap(), 19);
                Ok(17)
            }
        })
        .await
        .unwrap();
    assert_eq!(output, 17);
    assert_eq!(workers.owned(), 0);
    workers.close_and_drain().await.unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn owned_runtime_entry_accepts_current_thread_caller() {
    accepted_caller().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_runtime_entry_accepts_multi_thread_caller() {
    accepted_caller().await;
}
#[tokio::test]
async fn owned_runtime_entry_rejects_current_thread_target_before_registration_and_factory() {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let admission = QueueAdmission::new(1, 100).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let charge = job.owned_buffer(40).unwrap();
    drop(job);
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let result = workers
        .run_on_runtime(tokio::runtime::Handle::current(), move || {
            mark.store(true, Ordering::SeqCst);
            async move {
                let _charge = charge;
                Ok(())
            }
        })
        .await;
    assert!(
        matches!(result, Err(QueueError::OutcomeUnknown(reason)) if reason.contains("multi-thread runtime target"))
    );
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(workers.owned(), 0);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    workers.close_and_drain().await.unwrap();
}
#[tokio::test]
async fn owned_runtime_entry_rejects_same_pool_recursion_before_factory() {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let nested = workers.clone();
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), workers.run_on_runtime(target(), move || async move {
        let result = nested.run_on_runtime(target(), move || { mark.store(true, Ordering::SeqCst); async move { Ok(()) } }).await;
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(reason)) if reason.contains("recursively drive")));
        assert_eq!(nested.owned(), 1);
        assert!(matches!(nested.close_and_drain().await, Err(QueueError::OutcomeUnknown(_))));
        Ok(())
    })).await.unwrap().unwrap();
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(workers.owned(), 0);
    workers.close_and_drain().await.unwrap();
}
#[tokio::test]
async fn owned_runtime_factory_and_poll_panics_release_resources_and_preserve_error_policy() {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let admission = QueueAdmission::new(1, 100).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let charge = job.owned_buffer(40).unwrap();
    drop(job);
    let result = workers
        .run_on_runtime::<(), _, std::future::Ready<QueueResult<()>>>(target(), move || {
            let _charge = charge;
            panic!("factory panic control")
        })
        .await;
    assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(workers.owned(), 0);
    let job = Arc::new(admission.reserve(0).unwrap());
    let charge = job.owned_buffer(40).unwrap();
    drop(job);
    let result = workers
        .run_on_runtime::<(), _, _>(target(), move || async move {
            let _charge = charge;
            tokio::task::yield_now().await;
            panic!("poll panic control")
        })
        .await;
    assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(workers.owned(), 0);
    assert!(matches!(
        workers
            .run_on_runtime::<(), _, _>(target(), || async { Err(QueueError::TooLarge) })
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    assert_eq!(
        workers
            .run_on_runtime(target(), || async { Ok(7) })
            .await
            .unwrap(),
        7
    );
    workers.close_and_drain().await.unwrap();
}
#[tokio::test]
async fn owned_runtime_caller_abort_retains_future_resources_until_physical_drain() {
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let admission = QueueAdmission::new(1, 100).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let charge = job.owned_buffer(40).unwrap();
    drop(job);
    let release = Arc::new(Notify::new());
    let hold = release.clone();
    let (entered, ready) = oneshot::channel();
    let active = workers.clone();
    let caller = tokio::spawn(async move {
        active
            .run_on_runtime(target(), move || async move {
                let _charge = charge;
                entered.send(()).unwrap();
                hold.notified().await;
                Ok(17)
            })
            .await
    });
    ready.await.unwrap();
    caller.abort();
    let _ = caller.await;
    let closing = workers.clone();
    let mut drain = tokio::spawn(async move { closing.close_and_drain().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut drain)
            .await
            .is_err()
    );
    assert_eq!(workers.owned(), 1);
    assert_eq!(admission.usage().unwrap(), (1, 40));
    release.notify_one();
    drain.await.unwrap().unwrap();
    assert_eq!(workers.owned(), 0);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn owned_runtime_cancelled_permit_waiter_never_constructs_future_and_releases_inputs() {
    let workers = ScratchWorkers::new(1, 2).unwrap();
    let (started, ready) = oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let active = workers.clone();
    let first = tokio::spawn(async move {
        active
            .run(move || {
                started.send(()).unwrap();
                held.recv().unwrap();
                Ok(())
            })
            .await
    });
    ready.await.unwrap();
    let admission = QueueAdmission::new(1, 100).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let charge = job.owned_buffer(40).unwrap();
    drop(job);
    let called = Arc::new(AtomicBool::new(false));
    let mark = called.clone();
    let waiting = workers.clone();
    let waiter = tokio::spawn(async move {
        waiting
            .run_on_runtime(target(), move || {
                mark.store(true, Ordering::SeqCst);
                async move {
                    let _charge = charge;
                    Ok(())
                }
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while workers.owned() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(admission.usage().unwrap(), (1, 40));
    waiter.abort();
    let _ = waiter.await;
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(workers.owned(), 1);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    workers.close_and_drain().await.unwrap();
}
