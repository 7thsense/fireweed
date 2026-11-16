//! Nondefault qualification probes. No production observer or global registry.
use super::*;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalizationStage {
    Validate,
    Reduce,
    AssembleFrame,
    EncodeRecord,
}
pub struct FinalizationProbe {
    stage: FinalizationStage,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: Condvar,
    timeout: Duration,
    controller_released: std::sync::atomic::AtomicBool,
}
/// Independent fixture controller; workers must never be their own releasers.
pub struct FinalizationRelease {
    probe: Arc<FinalizationProbe>,
}
impl FinalizationProbe {
    pub fn new(stage: FinalizationStage, timeout: Duration) -> (Arc<Self>, FinalizationRelease) {
        let probe = Arc::new(Self {
            stage,
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
            timeout,
            controller_released: std::sync::atomic::AtomicBool::new(false),
        });
        let release = FinalizationRelease {
            probe: probe.clone(),
        };
        (probe, release)
    }
    /// True only after the selected worker observed controller release.
    pub fn controller_released(&self) -> bool {
        self.controller_released
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn entered(&self) {
        self.entered.notified().await;
    }
    pub(crate) fn checkpoint(&self, stage: FinalizationStage) -> QueueResult<()> {
        if stage != self.stage {
            return Ok(());
        }
        let released = self.released.lock().map_err(unknown)?;
        self.entered.notify_one();
        let (released, _) = self
            .wake
            .wait_timeout_while(released, self.timeout, |released| !*released)
            .map_err(unknown)?;
        if !*released {
            return Err(unknown("finalization qualification probe timed out"));
        }
        self.controller_released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
impl FinalizationRelease {
    pub fn release(&self) {
        *self
            .probe
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.probe.wake.notify_all();
    }
}
impl Drop for FinalizationRelease {
    fn drop(&mut self) {
        self.release();
    }
}
