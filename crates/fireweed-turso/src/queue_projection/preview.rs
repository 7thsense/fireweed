//! Shared guarded preview result. No publication or receipt authority is exposed.
use super::*;
pub struct AdmittedQueuePreview {
    result: PreviewResult,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedQueuePreview {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<PreviewResult, Vec<OwnedQueueBufferCharge>>
    {
        fireweed_engine::queue_contract::OwnedQueueValue::new(self.result, self._charges)
    }

    pub(super) fn new(result: PreviewResult, charges: Vec<OwnedQueueBufferCharge>) -> Self {
        Self {
            result,
            _charges: charges,
        }
    }
    pub fn result(&self) -> &PreviewResult {
        &self.result
    }
}
