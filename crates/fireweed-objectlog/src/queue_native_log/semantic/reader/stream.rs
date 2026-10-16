//! One-page source cursors. A prefix is never a completion capability.
use super::*;
use fireweed_engine::queue_source_index::QueueSourceStreamingReader;

// Poison is the default during every external dispatch or fallible callback.
// No Drop implementation can reconstruct an issued/dropped page or refund work.
enum Stage {
    Ready(Option<QueueNativeSemanticContinuation>),
    Issued(u64),
    Complete,
    Poisoned,
}
pub struct QueueNativeSourceCursor {
    lower: Vec<u8>,
    upper: Vec<u8>,
    last: Option<(Vec<u8>, OwnedQueueBufferCharge)>,
    stage: Stage,
    pages: u64,
    rows: u64,
    bundle: Arc<OwnedQueueBufferCharge>,
    budget: Arc<QueueNativeSourceBudget>,
    limits: QueueNativeScanLimits,
    #[cfg(test)]
    after_read: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
    identity: Arc<OwnedQueueBufferCharge>,
}
pub struct QueueNativeSourceStreamPage {
    page: QueueNativeSemanticPage,
    ordinal: u64,
    cursor: Arc<OwnedQueueBufferCharge>,
    _owner: OwnedQueueBufferCharge,
}
pub struct QueueNativeSourceCompletion {
    lower: Vec<u8>,
    upper: Vec<u8>,
    pages: u64,
    bundle: Arc<OwnedQueueBufferCharge>,
    budget: Arc<QueueNativeSourceBudget>,
    _owner: Arc<OwnedQueueBufferCharge>,
}
impl QueueNativeSourceReader<'_> {
    fn stream_scope(&self, cursor: &QueueNativeSourceCursor) -> QueueResult<()> {
        self.source.semantic_scope(self.log, self.job)?;
        self.budget.scope(self.job)?;
        if !Arc::ptr_eq(&cursor.bundle, &self.source.semantic_identity)
            || !std::ptr::eq(cursor.identity.reservation(), self.job.as_ref())
            || !Arc::ptr_eq(&cursor.budget, &self.budget)
            || cursor.limits.rows != self.limits.page.rows
            || cursor.limits.output_bytes != self.limits.page.output_bytes
            || cursor.limits.work.pages != self.limits.page.work.pages
            || cursor.limits.work.hydrated_bytes != self.limits.page.work.hydrated_bytes
            || cursor.lower >= cursor.upper
        {
            return Err(unknown("source stream cut, job or bounds differ"));
        }
        Ok(())
    }
}
impl QueueSourceStreamingReader for QueueNativeSourceReader<'_> {
    type Cursor = QueueNativeSourceCursor;
    type Page = QueueNativeSourceStreamPage;
    type Completion = QueueNativeSourceCompletion;

    fn open_stream(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Cursor> {
        self.source.semantic_scope(self.log, self.job)?;
        self.budget.scope(self.job)?;
        if lower >= upper {
            return Err(unknown("source stream bounds invalid"));
        }
        let bytes = lower
            .len()
            .checked_add(upper.len())
            .and_then(|n| n.checked_add(std::mem::size_of::<Self::Cursor>()))
            .ok_or(QueueError::TooLarge)?;
        let identity = ownership::shared_charge(
            self.job,
            u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?,
        )?;
        Ok(QueueNativeSourceCursor {
            lower: lower.to_vec(),
            upper: upper.to_vec(),
            last: None,
            stage: Stage::Ready(None),
            pages: 0,
            rows: 0,
            bundle: self.source.semantic_identity.clone(),
            budget: self.budget.clone(),
            limits: self.limits.page,
            #[cfg(test)]
            after_read: None,
            identity,
        })
    }
    async fn next_stream_page(&self, cursor: &mut Self::Cursor) -> QueueResult<Option<Self::Page>> {
        let previous = std::mem::replace(&mut cursor.stage, Stage::Poisoned);
        self.stream_scope(cursor)?;
        let continuation = match previous {
            Stage::Complete => {
                cursor.stage = Stage::Complete;
                return Ok(None);
            }
            Stage::Ready(continuation) => continuation,
            _ => return Err(unknown("source stream is issued or poisoned")),
        };
        let owner = self
            .job
            .owned_buffer(std::mem::size_of::<Self::Page>() as u64)?;
        // This grant survives storage errors and dropped in-flight futures.
        // Existing range_pages bounds retained all-page images; streaming holds
        // one page and is instead cumulatively bounded by this request ledger.
        self.budget.grant(self.limits.page.work)?;
        let page = match continuation {
            Some(continuation) => {
                self.source
                    .semantic_scan_next(self.log, &continuation, self.limits.page, self.job)
                    .await?
            }
            None => {
                self.source
                    .semantic_scan(
                        self.log,
                        Bound::Included(&cursor.lower),
                        Bound::Excluded(&cursor.upper),
                        self.limits.page,
                        self.job,
                    )
                    .await?
            }
        };
        #[cfg(test)]
        if let Some((entered, release)) = cursor.after_read.take() {
            entered
                .send(())
                .map_err(|_| unknown("stream read observer dropped"))?;
            release
                .await
                .map_err(|_| unknown("stream read release dropped"))?;
        }
        let ordinal = cursor.pages.checked_add(1).ok_or(QueueError::TooLarge)?;
        cursor.stage = Stage::Issued(ordinal);
        Ok(Some(QueueNativeSourceStreamPage {
            page,
            ordinal,
            cursor: cursor.identity.clone(),
            _owner: owner,
        }))
    }
    fn visit_stream_page(
        &self,
        cursor: &mut Self::Cursor,
        page: Self::Page,
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        let previous = std::mem::replace(&mut cursor.stage, Stage::Poisoned);
        self.stream_scope(cursor)?;
        if !matches!(previous, Stage::Issued(ordinal) if ordinal == page.ordinal)
            || !Arc::ptr_eq(&page.cursor, &cursor.identity)
            || !Arc::ptr_eq(&page.page.bundle, &cursor.bundle)
            || !std::ptr::eq(page._owner.reservation(), self.job.as_ref())
            || !std::ptr::eq(page.page.charge.reservation(), self.job.as_ref())
        {
            return Err(unknown("source stream page is foreign or out of order"));
        }
        let complete = page.page.status() == QueueNativeScanStatus::Complete;
        let continuation = page.page.continuation();
        if complete == continuation.is_some() {
            return Err(unknown("source stream page completion differs"));
        }
        let mut last = cursor.last.as_ref().map(|(key, _)| key.as_slice());
        let mut rows = cursor.rows;
        for row in page.page.inner.rows() {
            let key = row.key();
            if key < cursor.lower.as_slice()
                || key >= cursor.upper.as_slice()
                || last.is_some_and(|old| old >= key)
            {
                return Err(unknown("source stream key order or bounds invalid"));
            }
            last = Some(key);
            rows = rows.checked_add(1).ok_or(QueueError::TooLarge)?;
        }
        // The next monotonic bound is separately admitted before copying.
        let next_last = match page.page.inner.rows().last() {
            Some(row) => {
                let owner = self.job.owned_buffer(
                    (row.key().len() as u64)
                        .checked_add(128)
                        .ok_or(QueueError::TooLarge)?,
                )?;
                Some((row.key().to_vec(), owner))
            }
            None => None,
        };
        let cut = self.cut()?;
        for row in page.page.inner.rows() {
            visitor(cut.point_from_source(row.key(), Some(row.value())))?;
        }
        if let Some(last) = next_last {
            cursor.last = Some(last);
        }
        cursor.pages = page.ordinal;
        cursor.rows = rows;
        cursor.stage = if complete {
            Stage::Complete
        } else {
            Stage::Ready(continuation)
        };
        Ok(())
    }
    fn finish_stream(&self, cursor: Self::Cursor) -> QueueResult<Self::Completion> {
        self.stream_scope(&cursor)?;
        if !matches!(cursor.stage, Stage::Complete) || cursor.pages == 0 {
            return Err(unknown("source stream has no visited completion"));
        }
        Ok(QueueNativeSourceCompletion {
            lower: cursor.lower,
            upper: cursor.upper,
            pages: cursor.pages,
            bundle: cursor.bundle,
            budget: cursor.budget,
            _owner: cursor.identity,
        })
    }
    fn validate_stream_completion(
        &self,
        completion: &Self::Completion,
        lower: &[u8],
        upper: &[u8],
    ) -> QueueResult<()> {
        self.source.semantic_scope(self.log, self.job)?;
        self.budget.scope(self.job)?;
        if completion.lower != lower
            || completion.upper != upper
            || lower >= upper
            || completion.pages == 0
            || !Arc::ptr_eq(&completion.budget, &self.budget)
            || !Arc::ptr_eq(&completion.bundle, &self.source.semantic_identity)
            || !std::ptr::eq(completion._owner.reservation(), self.job.as_ref())
        {
            return Err(unknown("source stream completion scope or bounds differ"));
        }
        Ok(())
    }
}
#[cfg(test)]
pub(super) mod tests;
