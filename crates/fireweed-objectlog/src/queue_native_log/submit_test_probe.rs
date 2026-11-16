//! Test-only reply loss at the Root submit boundary, AFTER actual SDK Stored.
//! This does not emulate HTTP/provider reply loss or manufacture acceptance.
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
pub(super) struct SubmitProbe {
    calls: AtomicUsize,
    acquisitions: AtomicUsize,
    preparations: AtomicUsize,
    accepted: Mutex<Vec<QueueNativeHead>>,
    lose_stored_once: AtomicBool,
    injected: AtomicUsize,
    acquisition_pause: Mutex<Option<BoundaryPause>>,
    stored_pause: Mutex<Option<BoundaryPause>>,
}
struct BoundaryPause {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}
impl SubmitProbe {
    pub(super) fn lose_one_stored_reply() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            acquisitions: AtomicUsize::new(0),
            preparations: AtomicUsize::new(0),
            accepted: Mutex::new(Vec::new()),
            lose_stored_once: AtomicBool::new(true),
            injected: AtomicUsize::new(0),
            acquisition_pause: Mutex::new(None),
            stored_pause: Mutex::new(None),
        })
    }
    pub(super) fn pause_one_acquisition() -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let probe = Self::lose_one_stored_reply();
        probe.lose_stored_once.store(false, Ordering::SeqCst);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        *probe.acquisition_pause.lock().unwrap() = Some(BoundaryPause {
            entered,
            release: released,
        });
        (probe, started, release)
    }
    /// A Root-boundary error after REAL SDK acquisition, not provider reply loss.
    pub(super) async fn after_acquisition(&self) -> QueueResult<()> {
        let pause = { self.acquisition_pause.lock().unwrap().take() };
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            pause
                .release
                .await
                .map_err(|_| unknown("acquisition test gate abandoned"))?;
            return Err(unknown("injected Root acquisition-boundary error"));
        }
        Ok(())
    }
    pub(super) fn pause_one_stored_reply() -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let probe = Self::lose_one_stored_reply();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        *probe.stored_pause.lock().unwrap() = Some(BoundaryPause {
            entered,
            release: released,
        });
        (probe, started, release)
    }
    pub(super) fn acquiring(&self) {
        self.acquisitions.fetch_add(1, Ordering::SeqCst);
    }
    pub(super) fn acquisition_count(&self) -> usize {
        self.acquisitions.load(Ordering::SeqCst)
    }
    pub(super) fn preparing(&self) {
        self.preparations.fetch_add(1, Ordering::SeqCst);
    }
    pub(super) fn dispatching(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
    pub(super) async fn lose_reply_after(&self, actual: &QueueNativeSubmit) -> QueueResult<bool> {
        if let QueueNativeSubmit::Stored(head) = actual {
            self.accepted.lock().unwrap().push(*head);
            let pause = { self.stored_pause.lock().unwrap().take() };
            if let Some(pause) = pause {
                let _ = pause.entered.send(());
                pause
                    .release
                    .await
                    .map_err(|_| unknown("stored reply test gate abandoned"))?;
            }
            if self.lose_stored_once.swap(false, Ordering::SeqCst) {
                self.injected.fetch_add(1, Ordering::SeqCst);
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(super) fn snapshot(&self) -> (usize, usize, Vec<QueueNativeHead>, usize) {
        (
            self.calls.load(Ordering::SeqCst),
            self.preparations.load(Ordering::SeqCst),
            self.accepted.lock().unwrap().clone(),
            self.injected.load(Ordering::SeqCst),
        )
    }
}
