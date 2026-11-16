//! Shared final-reservation admission and draining. Backend task/connection
//! ownership stays in its adapter; the domain admission counters stay I/O-free.
use fireweed_core::queue_contract::QueueResult;
use fireweed_engine::queue_admission::{QueueAdmission, QueueReservation};
use std::sync::Arc;
use tokio::sync::Notify;

pub struct QueueOperationRuntime {
    admission: Arc<QueueAdmission>,
    done: Arc<Notify>,
}
impl QueueOperationRuntime {
    pub fn new(operations: usize, retained_bytes: u64) -> QueueResult<Arc<Self>> {
        Ok(Arc::new(Self {
            admission: QueueAdmission::new(operations, retained_bytes)?,
            done: Arc::new(Notify::new()),
        }))
    }
    /// Attach the terminal signal before exposing a reservation. Owned children
    /// retain this callback until the final physical owner releases capacity.
    pub fn reserve(&self, bytes: u64) -> QueueResult<QueueReservation> {
        let done = self.done.clone();
        self.admission
            .reserve(bytes)?
            .with_terminal_callback(move || done.notify_waiters())
    }
    pub fn usage(&self) -> QueueResult<(usize, u64)> {
        self.admission.usage()
    }
    /// Closing is permanent and shared. Cancellation can abandon this wait, but
    /// a subsequent drainer still observes all outstanding reservation owners.
    pub async fn close_and_drain(&self) -> QueueResult<()> {
        self.admission.close()?;
        loop {
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.admission.usage()?.0 == 0 {
                return Ok(());
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::QueueError;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn poll<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }
    #[tokio::test]
    async fn cancellation_and_multiple_drainers_wait_for_final_owned_child() {
        let runtime = QueueOperationRuntime::new(1, 100).unwrap();
        let job = Arc::new(runtime.reserve(7).unwrap());
        let child = job.owned_buffer(11).unwrap();
        drop(job);
        let mut first = Box::pin(runtime.close_and_drain());
        assert!(poll(first.as_mut()).is_pending());
        assert_eq!(runtime.usage().unwrap(), (1, 18));
        assert!(matches!(runtime.reserve(0), Err(QueueError::Busy)));
        drop(first);
        let mut second = Box::pin(runtime.close_and_drain());
        let mut third = Box::pin(runtime.close_and_drain());
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(third.as_mut().poll(&mut cx).is_pending());
        drop(child);
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            2,
            "the terminal callback wakes every registered drainer"
        );
        assert_eq!(runtime.usage().unwrap(), (0, 0));
        assert!(matches!(poll(second.as_mut()), Poll::Ready(Ok(()))));
        assert!(matches!(poll(third.as_mut()), Poll::Ready(Ok(()))));
        runtime.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn configured_capacity_and_shared_close_apply_to_every_clone() {
        let runtime = QueueOperationRuntime::new(2, 17).unwrap();
        let clone = runtime.clone();
        let first = runtime.reserve(7).unwrap();
        let second = clone.reserve(10).unwrap();
        assert!(matches!(runtime.reserve(0), Err(QueueError::Busy)));
        drop(first);
        assert!(matches!(runtime.reserve(8), Err(QueueError::Busy)));
        let replacement = runtime.reserve(7).unwrap();
        let mut drain = Box::pin(clone.close_and_drain());
        assert!(poll(drain.as_mut()).is_pending());
        assert!(matches!(runtime.reserve(0), Err(QueueError::Busy)));
        drop(replacement);
        drop(second);
        drain.await.unwrap();
        assert_eq!(runtime.usage().unwrap(), (0, 0));
    }
}
