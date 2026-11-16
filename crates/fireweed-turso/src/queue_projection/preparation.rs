//! One provisional publication owner. Neither response access nor possession of
//! these bytes establishes durable authority; the orchestrator must publish and
//! resolve the original identity before acknowledging a consumer.
use super::*;
pub struct PreparedPublication<T> {
    record: OwnedQueueLogRecord,
    response: T,
    expected_next_offset: u64,
    writer_epoch: u64,
}
impl<T> PreparedPublication<T> {
    pub(super) fn new(
        record: OwnedQueueLogRecord,
        response: T,
        expected_next_offset: u64,
        writer_epoch: u64,
    ) -> Self {
        Self {
            record,
            response,
            expected_next_offset,
            writer_epoch,
        }
    }
    pub fn response(&self) -> &T {
        &self.response
    }
    pub fn expected_next_offset(&self) -> u64 {
        self.expected_next_offset
    }
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
    pub fn into_parts(self) -> (OwnedQueueLogRecord, T, u64) {
        (self.record, self.response, self.expected_next_offset)
    }
}
