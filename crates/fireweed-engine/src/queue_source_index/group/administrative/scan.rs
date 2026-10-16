//! Exact source provenance minted only by a reader-driven completed scan.
use super::*;
use crate::queue_administration::{
    AdministrativeScanIdentity, AdmittedAdministrativeScan, ResolvedAdministrativeRow,
    addressed_administrative_source, scan_addressed_group_to_sink_bounded,
};
use fireweed_core::queue_administration::{AdministrativeIntent, AdministrativeOperation};

/// Successful source and awaited sink EOF, bound to the actual original source.
/// This is not business approval, storage freshness or publication authority.
pub struct QueueSourceAdministrativeScan {
    context: QueueSourceAdministrativeScanContext,
    summary: AdmittedAdministrativeScan,
}
/// Immutable provenance retained while the shared spool seals and finalizes.
/// No constructor accepts a separately assembled scan or a numeric source fence.
pub struct QueueSourceAdministrativeScanContext {
    binding: QueueSourceOrdinaryBinding,
    scan: AdministrativeScanIdentity,
    intent: [u8; 32],
    result_budget: u64,
    sequence: u64,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceAdministrativeScan {
    /// Decomposition preserves the opaque identity: consumers must validate the
    /// exact returned summary after sealing; another scan cannot be substituted.
    pub fn into_parts(
        self,
    ) -> (
        QueueSourceAdministrativeScanContext,
        AdmittedAdministrativeScan,
    ) {
        (self.context, self.summary)
    }
}
fn intent_binding(
    group: &CommitGroup,
    intent: &AdministrativeIntent,
    budget: u64,
) -> QueueResult<[u8; 32]> {
    let admin = group
        .administration
        .as_ref()
        .ok_or_else(|| unknown("source scan requires FP4 intent"))?;
    let actual = fingerprint("source-administrative-scan-intent", intent)?;
    if actual != fingerprint("source-administrative-scan-intent", &admin.intent)?
        || budget != admin.max_result_bytes
    {
        return Err(unknown(
            "source scan intent or result budget differs from request",
        ));
    }
    Ok(actual)
}
impl QueueSourceAdministrativeScanContext {
    /// Revalidate immutable provenance after a rejected finalizer has retired its
    /// summary. Consumers must validate the summary before finalization and
    /// retain this context through both result classifications.
    #[allow(clippy::too_many_arguments)]
    pub fn validate_authority(
        &self,
        original: QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        ordinary: &QueueSourceOrdinaryAuthority,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.binding
            .validate(original, queue, group, definition, job)?;
        ordinary.validate(original, queue, group, definition, job)?;
        let admin = group
            .administration
            .as_ref()
            .ok_or_else(|| unknown("source scan requires FP4 intent"))?;
        if self.intent != intent_binding(group, &admin.intent, admin.max_result_bytes)?
            || self.result_budget != admin.max_result_bytes
            || self.sequence != ordinary.snapshot().next_sequence
        {
            return Err(unknown("source scan sequence, intent or operation differs"));
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn validate(
        &self,
        original: QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        ordinary: &QueueSourceOrdinaryAuthority,
        summary: &AdmittedAdministrativeScan,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.validate_authority(original, queue, group, definition, ordinary, job)?;
        if self.sequence != summary.initial_sequence()
            || self.scan != summary.scan_identity()
            || !std::ptr::eq(summary.reservation(), job.as_ref())
        {
            return Err(unknown("source scan summary or operation differs"));
        }
        Ok(())
    }
}
impl<R: QueueSourceRecordReader> QueueSourceAdministrativeReader<'_, R> {
    /// Derive every input row from this reader and mint provenance only after
    /// the shared scanner has consumed the entire source and awaited every sink.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_addressed_to_sink<F, Fut>(
        &self,
        group: &CommitGroup,
        queue: &QueueAddress,
        intent: &AdministrativeIntent,
        max_result_bytes: u64,
        ordinary: &QueueSourceOrdinaryAuthority,
        limits: &QueueLimits,
        sink: F,
    ) -> QueueResult<QueueSourceAdministrativeScan>
    where
        F: FnMut(ResolvedAdministrativeRow) -> Fut,
        Fut: std::future::Future<Output = QueueResult<()>>,
    {
        // Fund retained provenance and hashing scratch before any new allocation
        // or row dispatch. Canonical hashing streams without cloning the intent.
        let charge = self.job.owned_buffer(4096)?;
        let original = self.header.cut;
        ordinary.validate(original, queue, group, self.definition, self.job)?;
        let intent_hash = intent_binding(group, intent, max_result_bytes)?;
        if ordinary.snapshot().next_sequence != self.header.snapshot.next_sequence
            || !self.reader.cut()?.same_cut(&original)
        {
            return Err(unknown("source scan ordinary header or cut differs"));
        }
        let binding = ordinary.retain_binding(self.job)?;
        let AdministrativeOperation::Addressed(entries) = &intent.operation else {
            return Err(unknown("source scan requires addressed intent"));
        };
        let rows = addressed_administrative_source(entries, |position, entry| {
            self.load_addressed_row(position, entry, intent.before_fields.as_ref())
        });
        let summary = scan_addressed_group_to_sink_bounded(
            group,
            intent,
            rows,
            ordinary.snapshot(),
            self.definition.schema(),
            limits,
            self.job.clone(),
            max_result_bytes,
            sink,
        )
        .await?;
        let context = QueueSourceAdministrativeScanContext {
            binding,
            scan: summary.scan_identity(),
            intent: intent_hash,
            result_budget: max_result_bytes,
            sequence: self.header.snapshot.next_sequence,
            _charge: charge,
        };
        if !self.reader.cut()?.same_cut(&original) {
            return Err(unknown("source scan reader changed captured cut"));
        }
        context.validate(
            original,
            queue,
            group,
            self.definition,
            ordinary,
            &summary,
            self.job,
        )?;
        Ok(QueueSourceAdministrativeScan { context, summary })
    }
}
