//! Request-scoped source reads; successful prefixes never stand in for a range.
use super::*;
use fireweed_engine::queue_source_index::{QueueSourceCut, QueueSourcePoint, QueueSourceReader};
use std::sync::Mutex;
mod payload;
pub use payload::{QueueNativeBoundSourceRecord, QueueNativeRecordReader};

/// One non-reusable ledger for an entire compilation request, including retries.
/// Keep and share this owner when constructing readers for that request.
pub struct QueueNativeSourceBudget {
    remaining: Mutex<QueueNativeSourceWork>,
    _charge: OwnedQueueBufferCharge,
}
/// Distinct cumulative work limits for the SAME request, including failed reads
/// and retries. Record attempts each bound an SDK lookup of at most 17 pages;
/// hydration counts bound SDK object reads/body caps, excluding transport retries.
/// Root and SDK memory pools are admitted independently of this work ledger.
#[derive(Clone, Copy)]
pub struct QueueNativeSourceWork {
    pub semantic: QueueNativeSemanticWork,
    pub record_attempts: u64,
    pub hydration_reads: u64,
    pub hydration_body_bytes: u64,
}
impl QueueNativeSourceBudget {
    pub fn new(work: QueueNativeSourceWork, job: &Arc<QueueReservation>) -> QueueResult<Arc<Self>> {
        let bytes = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        let charge = job.owned_buffer(bytes as u64)?;
        Ok(Arc::new(Self {
            remaining: Mutex::new(work),
            _charge: charge,
        }))
    }
    pub(super) fn grant(&self, work: QueueNativeSemanticWork) -> QueueResult<()> {
        let mut remaining = self
            .remaining
            .lock()
            .map_err(|_| unknown("source work ledger poisoned"))?;
        let pages = remaining.semantic.pages.checked_sub(work.pages);
        let bytes = remaining
            .semantic
            .hydrated_bytes
            .checked_sub(work.hydrated_bytes);
        match (pages, bytes) {
            (Some(pages), Some(hydrated_bytes)) => {
                remaining.semantic = QueueNativeSemanticWork {
                    pages,
                    hydrated_bytes,
                };
                Ok(())
            }
            _ => Err(unknown("request source work admission exhausted")),
        }
    }
    pub(super) fn scope(&self, job: &QueueReservation) -> QueueResult<()> {
        if !std::ptr::eq(self._charge.reservation(), job) {
            return Err(unknown("source work ledger belongs to another operation"));
        }
        Ok(())
    }
    /// Nonmutating entry preflight for mandatory adoption. This is not a
    /// reservation: exact nonrefundable grants still occur at dispatch, and
    /// protocol metadata has its own fixed bounds. No write allowance is needed.
    pub(super) fn ensure_adoption_capacity(
        &self,
        work: QueueNativeSemanticWork,
    ) -> QueueResult<()> {
        let remaining = self
            .remaining
            .lock()
            .map_err(|_| unknown("source work ledger poisoned"))?;
        if remaining.semantic.pages < work.pages
            || remaining.semantic.hydrated_bytes < work.hydrated_bytes
            || remaining.record_attempts == 0
            || remaining.hydration_reads == 0
            || remaining.hydration_body_bytes == 0
        {
            return Err(unknown("request source adoption admission exhausted"));
        }
        Ok(())
    }
    pub(super) fn grant_record_attempt(&self) -> QueueResult<()> {
        let mut remaining = self
            .remaining
            .lock()
            .map_err(|_| unknown("source work ledger poisoned"))?;
        remaining.record_attempts = remaining
            .record_attempts
            .checked_sub(1)
            .ok_or_else(|| unknown("request record attempts exhausted"))?;
        Ok(())
    }
    pub(super) fn grant_hydration(
        &self,
        requirements: object_log::NativeReadRequirements,
    ) -> QueueResult<()> {
        let mut remaining = self
            .remaining
            .lock()
            .map_err(|_| unknown("source work ledger poisoned"))?;
        let reads = remaining
            .hydration_reads
            .checked_sub(requirements.hydration_reads());
        let bytes = remaining
            .hydration_body_bytes
            .checked_sub(requirements.hydration_bytes());
        match (reads, bytes) {
            (Some(reads), Some(bytes)) => {
                remaining.hydration_reads = reads;
                remaining.hydration_body_bytes = bytes;
                Ok(())
            }
            _ => Err(unknown("request record hydration exhausted")),
        }
    }
    #[cfg(test)]
    pub(super) fn remaining_work(&self) -> QueueNativeSourceWork {
        *self.remaining.lock().unwrap()
    }
}

/// All range storage is admitted before allocation. Page limits also apply to
/// the final empty Complete page required at an exact row boundary.
#[derive(Clone, Copy)]
pub struct QueueNativeSourceReadLimits {
    pub page: QueueNativeScanLimits,
    pub range_pages: usize,
}
impl QueueNativeSourceReadLimits {
    pub(super) fn validate(self) -> QueueResult<()> {
        if self.range_pages == 0
            || self.page.output_bytes == 0
            || self.page.rows == 0
            || self.page.work.pages == 0
            || self.page.work.hydrated_bytes == 0
        {
            return Err(unknown("source read limits invalid"));
        }
        Ok(())
    }
}
pub struct QueueNativeSourceReader<'a> {
    source: &'a QueueNativeCaptured,
    log: &'a QueueNativeLog,
    job: &'a Arc<QueueReservation>,
    budget: Arc<QueueNativeSourceBudget>,
    limits: QueueNativeSourceReadLimits,
}
pub struct QueueNativeSourceRange {
    pages: Vec<QueueNativeSemanticPage>,
    lower: Vec<u8>,
    upper: Vec<u8>,
    bundle: Arc<OwnedQueueBufferCharge>,
    _charge: OwnedQueueBufferCharge,
}
impl<'a> QueueNativeSourceReader<'a> {
    pub fn new(
        source: &'a QueueNativeCaptured,
        log: &'a QueueNativeLog,
        job: &'a Arc<QueueReservation>,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeSourceReadLimits,
    ) -> QueueResult<Self> {
        source.semantic_scope(log, job)?;
        budget.scope(job)?;
        limits.validate()?;
        Ok(Self {
            source,
            log,
            job,
            budget,
            limits,
        })
    }
}
impl QueueNativeSourceRange {
    fn validate(
        &self,
        bundle: &Arc<OwnedQueueBufferCharge>,
        job: &Arc<QueueReservation>,
        lower: &[u8],
        upper: &[u8],
    ) -> QueueResult<()> {
        if !Arc::ptr_eq(&self.bundle, bundle)
            || !std::ptr::eq(self._charge.reservation(), job.as_ref())
            || self.lower != lower
            || self.upper != upper
            || lower >= upper
            || self.pages.is_empty()
        {
            return Err(unknown("source range scope, bounds or completion differs"));
        }
        let mut previous: Option<&[u8]> = None;
        for (i, page) in self.pages.iter().enumerate() {
            let final_page = i + 1 == self.pages.len();
            if !Arc::ptr_eq(&page.bundle, bundle)
                || !std::ptr::eq(page.charge.reservation(), job.as_ref())
                || (page.status() == QueueNativeScanStatus::Complete) != final_page
                || (!final_page && page.inner.continuation().is_none())
            {
                return Err(unknown("source range chain incomplete"));
            }
            for row in page.inner.rows() {
                let key = row.key();
                if key < lower || key >= upper || previous.is_some_and(|old| old >= key) {
                    return Err(unknown("source range key order or bounds invalid"));
                }
                previous = Some(key);
            }
        }
        Ok(())
    }
}
impl QueueSourceReader for QueueNativeSourceReader<'_> {
    type Point = QueueNativeSourcePoint;
    type Range = QueueNativeSourceRange;
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.source.source_cut(self.log, self.job)
    }
    async fn lookup(&self, full_key: &[u8]) -> QueueResult<Self::Point> {
        self.budget.grant(self.limits.page.work)?;
        self.source
            .source_lookup(self.log, full_key, self.limits.page.work, self.job)
            .await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        owner.evidence(self.source, self.log, self.job)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        if lower >= upper {
            return Err(unknown("source range bounds invalid"));
        }
        let bytes = self
            .limits
            .range_pages
            .checked_mul(std::mem::size_of::<QueueNativeSemanticPage>())
            .and_then(|n| n.checked_add(lower.len()))
            .and_then(|n| n.checked_add(upper.len()))
            .and_then(|n| n.checked_add(std::mem::size_of::<QueueNativeSourceRange>()))
            .ok_or(QueueError::TooLarge)?;
        let charge = self
            .job
            .owned_buffer(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)?;
        let mut range = QueueNativeSourceRange {
            pages: Vec::with_capacity(self.limits.range_pages),
            lower: lower.to_vec(),
            upper: upper.to_vec(),
            bundle: self.source.semantic_identity.clone(),
            _charge: charge,
        };
        self.budget.grant(self.limits.page.work)?;
        let mut page = self
            .source
            .semantic_scan(
                self.log,
                Bound::Included(lower),
                Bound::Excluded(upper),
                self.limits.page,
                self.job,
            )
            .await?;
        loop {
            let complete = page.status() == QueueNativeScanStatus::Complete;
            let continuation = if complete { None } else { page.continuation() };
            range.pages.push(page);
            if complete {
                range.validate(&self.source.semantic_identity, self.job, lower, upper)?;
                return Ok(range);
            }
            if range.pages.len() == self.limits.range_pages {
                return Err(unknown("source range page admission exhausted"));
            }
            let continuation =
                continuation.ok_or_else(|| unknown("source range continuation missing"))?;
            self.budget.grant(self.limits.page.work)?;
            page = self
                .source
                .semantic_scan_next(self.log, &continuation, self.limits.page, self.job)
                .await?;
        }
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        owner.validate(&self.source.semantic_identity, self.job, lower, upper)?;
        let cut = self.cut()?;
        for page in &owner.pages {
            for row in page.inner.rows() {
                visitor(cut.point_from_source(row.key(), Some(row.value())))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(super) async fn reader_contract(
    source: &QueueNativeCaptured,
    log: &QueueNativeLog,
    job: &Arc<QueueReservation>,
    foreign: &Arc<QueueReservation>,
    other: &QueueNativeCaptured,
    admission: &Arc<fireweed_engine::queue_admission::QueueAdmission>,
) {
    let work = QueueNativeSemanticWork {
        pages: 1000,
        hydrated_bytes: 1 << 16,
    };
    let limits = QueueNativeSourceReadLimits {
        page: QueueNativeScanLimits {
            rows: 1,
            output_bytes: 8192,
            work,
        },
        range_pages: 4,
    };
    let budget = |calls| {
        QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: QueueNativeSemanticWork {
                    pages: work.pages * calls,
                    hydrated_bytes: work.hydrated_bytes * calls,
                },
                record_attempts: 0,
                hydration_reads: 0,
                hydration_body_bytes: 0,
            },
            job,
        )
        .unwrap()
    };
    let shared = budget(2);
    let reader = QueueNativeSourceReader::new(source, log, job, shared.clone(), limits).unwrap();
    let retry = QueueNativeSourceReader::new(source, log, job, shared.clone(), limits).unwrap();
    let present = reader.lookup(b"a").await.unwrap();
    let absent = retry.lookup(b"absent").await.unwrap();
    assert_eq!(
        reader
            .point(&present)
            .unwrap()
            .value_for(&reader.cut().unwrap(), b"a")
            .unwrap(),
        Some(b"".as_slice())
    );
    assert_eq!(
        retry
            .point(&absent)
            .unwrap()
            .value_for(&retry.cut().unwrap(), b"absent")
            .unwrap(),
        None
    );
    assert!(reader.lookup(b"a").await.is_err()); // successful calls share one ledger
    assert!(QueueNativeSourceReader::new(source, log, foreign, shared, limits).is_err());
    let foreign_reader = QueueNativeSourceReader::new(other, log, job, budget(8), limits).unwrap();
    assert!(foreign_reader.point(&present).is_err());

    let reader = QueueNativeSourceReader::new(source, log, job, budget(8), limits).unwrap();
    let range = reader.complete_range(b"a", b"z").await.unwrap();
    assert_eq!(range.pages.len(), 4); // includes exact-boundary empty Complete
    let mut seen = Vec::new();
    reader
        .visit_range(&range, b"a", b"z", &mut |point| {
            seen.push(point.key().to_vec());
            assert!(point.value_for(&reader.cut()?, point.key())?.is_some());
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, [b"a".to_vec(), b"b\0full".to_vec(), b"c".to_vec()]);
    let mut visits = 0;
    let mut visit = |_: QueueSourcePoint<'_>| {
        visits += 1;
        Ok(())
    };
    assert!(reader.visit_range(&range, b"a", b"c", &mut visit).is_err());
    assert!(
        foreign_reader
            .visit_range(&range, b"a", b"z", &mut visit)
            .is_err()
    );
    assert_eq!(visits, 0);
    let mut visits = 0;
    assert!(
        reader
            .visit_range(&range, b"a", b"z", &mut |_| {
                visits += 1;
                Err(unknown("injected visitor failure"))
            })
            .is_err()
    );
    assert_eq!(visits, 1);
    let mut incomplete = range;
    incomplete.pages.pop();
    let mut visits = 0;
    assert!(
        reader
            .visit_range(&incomplete, b"a", b"z", &mut |_| {
                visits += 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(visits, 0); // missing completion cannot expose a prefix

    let reader = QueueNativeSourceReader::new(source, log, job, budget(8), limits).unwrap();
    let interior = reader.complete_range(b"b\0full", b"c").await.unwrap();
    let mut seen = Vec::new();
    reader
        .visit_range(&interior, b"b\0full", b"c", &mut |point| {
            seen.push(point.key().to_vec());
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, [b"b\0full".to_vec()]);
    let mut reordered = reader.complete_range(b"a", b"z").await.unwrap();
    reordered.pages.swap(0, 1);
    let mut visits = 0;
    assert!(
        reader
            .visit_range(&reordered, b"a", b"z", &mut |_| {
                visits += 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(visits, 0);

    // Three successful page reads exhaust the aggregate grant before Complete.
    let reader = QueueNativeSourceReader::new(source, log, job, budget(3), limits).unwrap();
    assert!(reader.complete_range(b"a", b"z").await.is_err());
    assert!(reader.lookup(b"a").await.is_err());
    let short = QueueNativeSourceReadLimits {
        range_pages: 3,
        ..limits
    };
    let reader = QueueNativeSourceReader::new(source, log, job, budget(8), short).unwrap();
    assert!(reader.complete_range(b"a", b"z").await.is_err());

    // Exactly the container grant reaches the per-page grant; N-1 fails before
    // consuming source work. Both failures release all transient container credit.
    let container = (limits.range_pages * std::mem::size_of::<QueueNativeSemanticPage>()
        + 2
        + std::mem::size_of::<QueueNativeSourceRange>()) as u64;
    for spare in [container - 1, container] {
        let ledger = budget(8);
        let reader =
            QueueNativeSourceReader::new(source, log, job, ledger.clone(), limits).unwrap();
        let before = admission.usage().unwrap();
        let padding = job.owned_buffer((32 << 20) - before.1 - spare).unwrap();
        let full = admission.usage().unwrap();
        assert!(reader.complete_range(b"a", b"z").await.is_err());
        assert_eq!(admission.usage().unwrap(), full);
        let remaining = *ledger.remaining.lock().unwrap();
        assert_eq!(
            remaining.semantic.pages,
            work.pages * if spare == container { 7 } else { 8 }
        );
        drop(padding);
        assert_eq!(admission.usage().unwrap(), before);
    }
    let reader = QueueNativeSourceReader::new(source, log, job, budget(8), limits).unwrap();
    let before = admission.usage().unwrap();
    let owned_range = reader.complete_range(b"a", b"z").await.unwrap();
    assert!(admission.usage().unwrap().1 > before.1);
    drop(owned_range);
    assert_eq!(admission.usage().unwrap(), before);

    // SDK work-budget exhaustion consumes its full grant; retries cannot refill it.
    let tiny = QueueNativeSemanticWork {
        pages: 1,
        hydrated_bytes: 1,
    };
    let tiny_limits = QueueNativeSourceReadLimits {
        page: QueueNativeScanLimits {
            work: tiny,
            ..limits.page
        },
        ..limits
    };
    let tiny_budget = QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: tiny,
            record_attempts: 0,
            hydration_reads: 0,
            hydration_body_bytes: 0,
        },
        job,
    )
    .unwrap();
    let reader =
        QueueNativeSourceReader::new(source, log, job, tiny_budget.clone(), tiny_limits).unwrap();
    assert!(reader.lookup(b"a").await.is_err());
    let remaining = *tiny_budget.remaining.lock().unwrap();
    assert_eq!(
        (remaining.semantic.pages, remaining.semantic.hydrated_bytes),
        (0, 0)
    );
    assert!(reader.lookup(b"a").await.is_err());
}
