//! Explicit bounded hydration under the metadata reader's ORIGINAL SDK capture.
use super::*;
use fireweed_engine::{
    queue_log_record::CheckedQueueLogRecord, queue_source_index::QueueSourceRecordReader,
};
pub struct QueueNativeRecordReader<'a> {
    inner: QueueNativeSourceReader<'a>,
    limits: QueueNativeSourceRecordLimits,
}
/// A private constructor binds actual retained storage, offset and source owner.
/// Equal bytes or equal numeric heads cannot manufacture this carrier.
pub struct QueueNativeBoundSourceRecord {
    record: OwnedQueueLogRecord,
    source: Arc<OwnedQueueBufferCharge>,
    offset: u64,
    _charge: OwnedQueueBufferCharge,
}
impl<'a> QueueNativeSourceReader<'a> {
    pub fn with_records(
        self,
        limits: QueueNativeSourceRecordLimits,
    ) -> QueueResult<QueueNativeRecordReader<'a>> {
        limits.validate()?;
        Ok(QueueNativeRecordReader {
            inner: self,
            limits,
        })
    }
}
impl QueueNativeRecordReader<'_> {
    fn original(&self, cut: &QueueSourceCut<'_>) -> QueueResult<()> {
        if !self.inner.cut()?.same_cut(cut) || !cut.belongs_to(self.inner.job) {
            return Err(unknown("record reader original cut or operation differs"));
        }
        Ok(())
    }
}
impl QueueSourceReader for QueueNativeRecordReader<'_> {
    type Point = QueueNativeSourcePoint;
    type Range = QueueNativeSourceRange;
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.inner.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        self.inner.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.inner.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.inner.visit_range(owner, lower, upper, visitor)
    }
}
impl QueueSourceRecordReader for QueueNativeRecordReader<'_> {
    type Record = QueueNativeBoundSourceRecord;
    async fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<Self::Record> {
        self.original(cut)?;
        let charge = self
            .inner
            .job
            .owned_buffer(std::mem::size_of::<Self::Record>() as u64)?;
        let record = self
            .inner
            .source
            .read_source_record(
                self.inner.log,
                offset,
                self.limits,
                &self.inner.budget,
                self.inner.job,
            )
            .await?;
        Ok(QueueNativeBoundSourceRecord {
            record,
            source: self.inner.source.semantic_identity.clone(),
            offset,
            _charge: charge,
        })
    }
    fn checked_record<'a>(
        &'a self,
        owner: &'a Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<CheckedQueueLogRecord<'a>> {
        self.original(cut)?;
        if !Arc::ptr_eq(&owner.source, &self.inner.source.semantic_identity)
            || owner.offset != offset
            || !owner.record.belongs_to(self.inner.job)
            || !std::ptr::eq(owner._charge.reservation(), self.inner.job.as_ref())
        {
            return Err(unknown(
                "returned record carrier cut, operation or offset differs",
            ));
        }
        owner.record.checked()
    }
}
