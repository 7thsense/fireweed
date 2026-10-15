//! SDK-neutral source-map transport. These operations do not establish queue
//! index meaning: the engine must compile all resolved commands before activation.
use super::*;
use std::ops::Bound;
mod group;

#[derive(Clone, Copy, Debug)]
pub struct QueueNativeSemanticWork {
    pub pages: u64,
    pub hydrated_bytes: u64,
}
impl QueueNativeSemanticWork {
    fn sdk(self) -> object_log::NativeSemanticWork {
        object_log::NativeSemanticWork {
            pages: self.pages,
            hydrated_bytes: self.hydrated_bytes,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct QueueNativeEditLimits {
    pub edits: u64,
    pub input_bytes: u64,
    pub work: QueueNativeSemanticWork,
}
/// Bounds for one attempt; the caller-owned ledger bounds work across attempts.
#[derive(Clone, Copy)]
pub struct QueueNativeSourcePreparationLimits {
    pub compile: fireweed_engine::queue_source_index::QueueSourceCompileLimits,
    pub read: QueueNativeSourceReadLimits,
    pub prepare: QueueNativeEditLimits,
}
impl QueueNativeSourcePreparationLimits {
    fn validate(self) -> QueueResult<()> {
        self.read.validate()?;
        if self.compile.edits == 0
            || self.compile.input_bytes == 0
            || self.prepare.edits == 0
            || self.prepare.input_bytes == 0
            || self.prepare.work.pages == 0
            || self.prepare.work.hydrated_bytes == 0
        {
            return Err(unknown("source preparation limits invalid"));
        }
        Ok(())
    }
}
mod provision;
pub use provision::{QueueNativeGenesisIntent, QueueNativeProvisionLimits};
#[cfg(test)]
pub struct QueueNativeSemanticEdit<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}
pub struct QueueNativeSemanticValue {
    inner: object_log::NativeSemanticValue,
    _charge: Arc<OwnedQueueBufferCharge>,
}
impl QueueNativeSemanticValue {
    pub fn bytes(&self) -> &[u8] {
        self.inner.bytes()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct QueueNativeScanLimits {
    pub rows: usize,
    pub output_bytes: usize,
    pub work: QueueNativeSemanticWork,
}
impl QueueNativeScanLimits {
    fn sdk(self) -> object_log::NativeScanLimits {
        object_log::NativeScanLimits {
            rows: self.rows,
            output_bytes: self.output_bytes,
            work: self.work.sdk(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueNativeScanStatus {
    Complete,
    LimitReached,
}
pub struct QueueNativeSemanticPage {
    inner: object_log::NativeSemanticScan,
    bundle: Arc<OwnedQueueBufferCharge>,
    charge: Arc<OwnedQueueBufferCharge>,
    range_charge: Arc<OwnedQueueBufferCharge>,
}
/// Internal operation continuation; public QueryCursor semantics remain in the
/// queue contract. Resumption retains the original authenticated cut and range.
#[derive(Clone)]
pub struct QueueNativeSemanticContinuation {
    inner: object_log::NativeSemanticContinuation,
    bundle: Arc<OwnedQueueBufferCharge>,
    charge: Arc<OwnedQueueBufferCharge>,
    range_charge: Arc<OwnedQueueBufferCharge>,
}
pub struct QueueNativeSemanticRow<'a> {
    inner: &'a object_log::NativeSemanticRow,
}
impl QueueNativeSemanticRow<'_> {
    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }
    pub fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
impl QueueNativeSemanticPage {
    pub fn rows(&self) -> impl ExactSizeIterator<Item = QueueNativeSemanticRow<'_>> {
        self.inner
            .rows()
            .iter()
            .map(|inner| QueueNativeSemanticRow { inner })
    }
    pub fn status(&self) -> QueueNativeScanStatus {
        match self.inner.status() {
            object_log::NativeScanStatus::Complete => QueueNativeScanStatus::Complete,
            object_log::NativeScanStatus::LimitReached => QueueNativeScanStatus::LimitReached,
        }
    }
    pub fn continuation(&self) -> Option<QueueNativeSemanticContinuation> {
        self.inner
            .continuation()
            .map(|inner| QueueNativeSemanticContinuation {
                inner: inner.clone(),
                bundle: self.bundle.clone(),
                charge: self.charge.clone(),
                range_charge: self.range_charge.clone(),
            })
    }
}
impl QueueNativeAcquired {
    /// Compile every semantic fact and the checked record against this exact
    /// acquired cut, then prepare their single conditional publication. This
    /// never submits, recaptures authority, or creates a retry work allowance.
    pub async fn compile_and_prepare(
        &self,
        log: &QueueNativeLog,
        record: &OwnedQueueLogRecord,
        definition: &AdmittedQueueDefinition,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeSourcePreparationLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        self.source.semantic_scope(log, job)?;
        log.validate_prepare_record(&self.source.namespace, &self.writer, record, job)?;
        let checked = record.checked()?;
        if !definition.belongs_to(job)
            || checked.view().definition_pin_bytes() != *definition.pin().as_bytes()
        {
            return Err(unknown("source preparation definition or limits invalid"));
        }
        limits.validate()?;
        // Construct even for genesis: foreign ledgers and invalid read limits
        // must fail before either dispatch path allocates compiler output.
        let reader =
            QueueNativeSourceReader::new(self.as_source(), log, job, budget.clone(), limits.read)?;
        let compiled = match checked.view().payload() {
            QueueLogPayload::Genesis(_) => fireweed_engine::queue_source_index::compile_genesis(
                &checked,
                self.source.source_cut(log, job)?,
                definition,
                limits.compile,
                job,
            )?,
            QueueLogPayload::Publication(_) | QueueLogPayload::Claim(_) => {
                fireweed_engine::queue_source_index::compile_publication(
                    &reader,
                    &checked,
                    definition,
                    limits.compile,
                    job,
                )
                .await?
            }
        };
        // Debit BEFORE preparing; failure and cancellation never refund work.
        // This ledger is the same one used by every compiler source read.
        budget.grant(limits.prepare.work)?;
        let identity = self.record_identity(record, job)?;
        self.prepare_compiled(log, &compiled, &identity.value, limits.prepare, job)
            .await
    }
    /// The production transport accepts only the engine's sealed correspondence
    /// result; independently supplied record/edit arrays remain test fixtures.
    pub async fn prepare_compiled(
        &self,
        log: &QueueNativeLog,
        compiled: &fireweed_engine::queue_source_index::QueueSourceCompiled<'_>,
        identity: &str,
        limits: QueueNativeEditLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        let cut = self.source.source_cut(log, job)?;
        compiled.verify(&cut, job)?;
        self.prepare_semantic_entries(
            log,
            compiled.record(),
            identity,
            compiled.edits(),
            limits,
            job,
        )
        .await
    }
    #[cfg(test)]
    pub async fn prepare_with_edits(
        &self,
        log: &QueueNativeLog,
        record: &OwnedQueueLogRecord,
        identity: &str,
        edits: &[QueueNativeSemanticEdit<'_>],
        limits: QueueNativeEditLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        self.prepare_semantic_entries(
            log,
            record,
            identity,
            edits.iter().map(|edit| (edit.key, edit.value)),
            limits,
            job,
        )
        .await
    }
    async fn prepare_semantic_entries<'a>(
        &self,
        log: &QueueNativeLog,
        record: &OwnedQueueLogRecord,
        identity: &str,
        entries: impl ExactSizeIterator<Item = (&'a [u8], Option<&'a [u8]>)> + Clone,
        limits: QueueNativeEditLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        self.source.semantic_scope(log, job)?;
        log.validate_prepare_record(&self.source.namespace, &self.writer, record, job)?;
        if u64::try_from(entries.len()).map_err(|_| QueueError::TooLarge)? > limits.edits {
            return Err(unknown("semantic edit count admission exhausted"));
        }
        let mut previous = None;
        for (key, _) in entries.clone() {
            if previous.is_some_and(|old: &[u8]| old >= key) {
                return Err(unknown(
                    "semantic edits must be strictly ordered and unique",
                ));
            }
            previous = Some(key);
        }
        let input_bytes = entries.clone().try_fold(0u64, |total, (key, value)| {
            total
                .checked_add(key.len() as u64)
                .and_then(|n| n.checked_add(value.map_or(0, |value| value.len()) as u64))
                .ok_or(QueueError::TooLarge)
        })?;
        if input_bytes > limits.input_bytes {
            return Err(unknown("semantic edit input admission exhausted"));
        }
        let bytes = entries
            .len()
            .checked_mul(std::mem::size_of::<object_log::NativeSemanticEdit<'_>>())
            .ok_or(QueueError::TooLarge)?;
        let charged_bytes = u64::try_from(bytes)
            .map_err(|_| QueueError::TooLarge)?
            .checked_add(input_bytes)
            .ok_or(QueueError::TooLarge)?;
        #[cfg(test)]
        if let Some(probe) = &log.submit_probe {
            probe.preparing();
        }
        let charge = log.preparation_charge(record, identity, charged_bytes, job)?;
        let references: Vec<_> = entries
            .map(|(key, value)| object_log::NativeSemanticEdit { key, value })
            .collect();
        let publication = log
            .log
            .with_operation_owner(ownership::physical_owner([
                charge.clone(),
                self.source.semantic_identity.clone(),
                self.source.namespace._charge.clone(),
            ])?)
            .prepare_with_edits(
                &self.writer.writer,
                &self.source.cut.cut,
                &[NativeAppend {
                    identity,
                    payload: record.bytes(),
                    records: 1,
                }],
                &references,
                object_log::NativeEditLimits {
                    edits: limits.edits,
                    input_bytes: limits.input_bytes,
                    work: limits.work.sdk(),
                },
            )
            .await
            .map_err(unknown)?;
        Ok(QueueNativePublication {
            publication,
            log_identity: log.identity.clone(),
            _charge: charge,
        })
    }
}

#[cfg(test)]
mod tests;

/// Exact-key evidence owner, including authenticated absence. Unlike a naked
/// Option value this retains the originating full key, bundle and Root guard.
pub struct QueueNativeSourcePoint {
    value: Option<QueueNativeSemanticValue>,
    key: Vec<u8>,
    bundle: Arc<OwnedQueueBufferCharge>,
    _charge: OwnedQueueBufferCharge,
}
impl QueueNativeSourcePoint {
    pub fn evidence<'a>(
        &'a self,
        acquired: &'a QueueNativeCaptured,
        log: &QueueNativeLog,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<fireweed_engine::queue_source_index::QueueSourcePoint<'a>> {
        acquired.semantic_scope(log, job)?;
        if !Arc::ptr_eq(&self.bundle, &acquired.semantic_identity)
            || !std::ptr::eq(self._charge.reservation(), job.as_ref())
        {
            return Err(unknown(
                "source point belongs to another captured source or operation",
            ));
        }
        Ok(acquired.source_cut(log, job)?.point_from_source(
            &self.key,
            self.value.as_ref().map(QueueNativeSemanticValue::bytes),
        ))
    }
}
mod reader;
pub use reader::{
    QueueNativeSourceBudget, QueueNativeSourceRange, QueueNativeSourceReadLimits,
    QueueNativeSourceReader, QueueNativeSourceWork,
};
mod record;
pub use record::QueueNativeSourceRecordLimits;

impl QueueNativeCaptured {
    pub(super) fn identity_charge(
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Arc<OwnedQueueBufferCharge>> {
        let bytes =
            std::mem::size_of::<OwnedQueueBufferCharge>() + 2 * std::mem::size_of::<usize>();
        Ok(Arc::new(job.owned_buffer(bytes as u64)?))
    }
    fn semantic_scope(&self, log: &QueueNativeLog, job: &Arc<QueueReservation>) -> QueueResult<()> {
        if !Arc::ptr_eq(&self.log_identity, &log.identity)
            || !self.namespace.belongs_to(job)
            || !Arc::ptr_eq(&self.namespace.identity, &self.cut.namespace)
        {
            return Err(unknown("semantic namespace, cut or operation differs"));
        }
        Ok(())
    }
    pub async fn semantic_lookup(
        &self,
        log: &QueueNativeLog,
        key: &[u8],
        work: QueueNativeSemanticWork,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<QueueNativeSemanticValue>> {
        self.semantic_scope(log, job)?;
        let bytes = (std::mem::size_of::<QueueNativeSemanticValue>() as u64)
            .checked_add(work.hydrated_bytes)
            .and_then(|n| n.checked_add(key.len() as u64))
            .ok_or(QueueError::TooLarge)?;
        let charge = ownership::shared_charge(job, bytes)?;
        let owner = ownership::physical_owner([charge.clone(), self.semantic_identity.clone()])?;
        Ok(log
            .log
            .with_operation_owner(owner)
            .semantic_lookup(&self.cut.cut, key, work.sdk())
            .await
            .map_err(unknown)?
            .map(|inner| QueueNativeSemanticValue {
                inner,
                _charge: charge,
            }))
    }
    fn page_charge(
        job: &Arc<QueueReservation>,
        limits: QueueNativeScanLimits,
        bound_bytes: usize,
    ) -> QueueResult<Arc<OwnedQueueBufferCharge>> {
        let bytes = limits
            .rows
            .checked_mul(std::mem::size_of::<object_log::NativeSemanticRow>())
            .and_then(|n| {
                limits
                    .output_bytes
                    .checked_mul(2)
                    .and_then(|output| n.checked_add(output))
            })
            .and_then(|n| n.checked_add(bound_bytes))
            .and_then(|n| {
                n.checked_add(
                    std::mem::size_of::<QueueNativeSemanticPage>()
                        + std::mem::size_of::<QueueNativeSemanticContinuation>()
                        + std::mem::size_of::<OwnedQueueBufferCharge>()
                        + 2 * std::mem::size_of::<usize>(),
                )
            })
            .ok_or(QueueError::TooLarge)?;
        Ok(Arc::new(job.owned_buffer(
            u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?,
        )?))
    }
    pub async fn semantic_scan(
        &self,
        log: &QueueNativeLog,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        limits: QueueNativeScanLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeSemanticPage> {
        self.semantic_scope(log, job)?;
        let bound_len = |bound: Bound<&[u8]>| match bound {
            Bound::Included(key) | Bound::Excluded(key) => key.len(),
            Bound::Unbounded => 0,
        };
        let bound_bytes = bound_len(lower)
            .checked_add(bound_len(upper))
            .ok_or(QueueError::TooLarge)?;
        let charge = Self::page_charge(job, limits, bound_bytes)?;
        let owner = ownership::physical_owner([charge.clone(), self.semantic_identity.clone()])?;
        let inner = log
            .log
            .with_operation_owner(owner)
            .semantic_scan(
                &self.cut.cut,
                object_log::NativeSemanticRange { lower, upper },
                limits.sdk(),
            )
            .await
            .map_err(unknown)?;
        Ok(QueueNativeSemanticPage {
            inner,
            bundle: self.semantic_identity.clone(),
            range_charge: charge.clone(),
            charge,
        })
    }
    pub async fn semantic_scan_next(
        &self,
        log: &QueueNativeLog,
        continuation: &QueueNativeSemanticContinuation,
        limits: QueueNativeScanLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeSemanticPage> {
        self.semantic_scope(log, job)?;
        if !Arc::ptr_eq(&self.semantic_identity, &continuation.bundle)
            || !std::ptr::eq(continuation.charge.reservation(), job.as_ref())
        {
            return Err(unknown(
                "semantic continuation namespace or operation differs",
            ));
        }
        let charge = Self::page_charge(job, limits, 0)?;
        let owner = ownership::physical_owner([
            charge.clone(),
            continuation.bundle.clone(),
            continuation.charge.clone(),
            continuation.range_charge.clone(),
        ])?;
        let inner = log
            .log
            .with_operation_owner(owner)
            .semantic_scan_next(&continuation.inner, limits.sdk())
            .await
            .map_err(unknown)?;
        Ok(QueueNativeSemanticPage {
            inner,
            bundle: self.semantic_identity.clone(),
            range_charge: continuation.range_charge.clone(),
            charge,
        })
    }
    pub fn source_cut<'a>(
        &'a self,
        log: &QueueNativeLog,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<fireweed_engine::queue_source_index::QueueSourceCut<'a>> {
        self.semantic_scope(log, job)?;
        fireweed_engine::queue_source_index::QueueSourceCut::from_captured(
            &self.semantic_identity,
            view_address(&self.namespace.address)?,
            self.head().epoch(),
            self.head().bounds().1,
            job,
        )
    }
    pub async fn source_lookup(
        &self,
        log: &QueueNativeLog,
        key: &[u8],
        work: QueueNativeSemanticWork,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeSourcePoint> {
        self.semantic_scope(log, job)?;
        let bytes = key
            .len()
            .checked_add(std::mem::size_of::<QueueNativeSourcePoint>())
            .ok_or(QueueError::TooLarge)?;
        let charge = job.owned_buffer(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)?;
        let owned_key = key.to_vec();
        let value = self.semantic_lookup(log, &owned_key, work, job).await?;
        Ok(QueueNativeSourcePoint {
            value,
            key: owned_key,
            bundle: self.semantic_identity.clone(),
            _charge: charge,
        })
    }
}
