//! Allow exact verification funding at C2, then refuse its first DTO copy.
use super::*;
use crate::queue_admission::OwnedQueueBufferCharge;
struct Starve<'a> {
    inner: BodyReader<'a>,
    fill: std::sync::Mutex<Option<OwnedQueueBufferCharge>>,
}
impl QueueSourceReader for Starve<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
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
impl<'a> QueueSourceRecordReader for Starve<'a> {
    type Record = &'a OwnedQueueLogRecord;
    async fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<Self::Record> {
        self.inner.read_record(cut, offset).await
    }
    fn checked_record<'r>(
        &'r self,
        owner: &'r Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<crate::queue_log_record::CheckedQueueLogRecord<'r>> {
        let checked = self.inner.checked_record(owner, cut, offset)?;
        if offset == 2 {
            assert_eq!(self.inner.reads.load(Ordering::SeqCst), 2);
            let f = self.inner.fixture;
            let used = f.admission.usage().unwrap().1;
            // C2 contains one distinct requested value. This is the exact
            // verifier charge; its success leaves no credit for the DTO copy.
            *self.fill.lock().unwrap() =
                Some(f.job.owned_buffer((32u64 << 20) - used - 5120).unwrap());
        }
        Ok(checked)
    }
}
#[test]
fn source_state_read_late_copy_admission_returns_no_prefix_and_restores_owners() {
    futures::executor::block_on(async {
        let (f, request, _) = prepared();
        let baseline = f.admission.usage().unwrap();
        let reader = Starve {
            inner: reader(&f, 0),
            fill: Default::default(),
        };
        let result = load_source_state_records(
            &reader,
            reader.cut().unwrap(),
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await;
        assert!(
            matches!(result,Err(QueueError::OutcomeUnknown(ref message)) if message=="admitted buffer capacity exhausted; resolve owned operation")
        );
        let fill = reader.fill.lock().unwrap().take().unwrap();
        assert_eq!(
            f.admission.usage().unwrap(),
            (baseline.0, baseline.1 + fill.admitted_bytes()),
            "only the independent fixture filler remains"
        );
        drop(fill);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
