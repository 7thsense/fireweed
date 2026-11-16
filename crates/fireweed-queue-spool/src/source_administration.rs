//! Source-proven administration: only this finalizer can mint a publication.
use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    queue_administration::validate_ordinary_prospective_authority,
    queue_contract::OwnedQueueValue,
    queue_log_record::OwnedQueueLogRecord,
    queue_planner::RecordSnapshot,
    queue_publication::{
        AdministrativeCommandMeasurement, AdministrativePublicationDecision,
        assess_administrative_publication,
    },
    queue_schema_codec::{AdmittedQueueDefinition, DefinitionPin},
    queue_source_index::{
        QueueSourceAdministrativeScan, QueueSourceAdministrativeScanContext, QueueSourceCut,
        QueueSourceOrdinaryAuthority,
    },
};
mod output;

struct OrdinaryOwner {
    value: QueueSourceOrdinaryAuthority,
    _charge: OwnedQueueBufferCharge,
}
/// Shared immutable snapshot; its actual allocation is funded before Arc::new.
/// The coordinator independently retains the physical SDK source capsule.
#[derive(Clone)]
pub struct SourceAdministrativeAuthority {
    owner: Arc<OrdinaryOwner>,
    #[cfg(feature = "test-support")]
    probe: Option<Arc<FinalizationProbe>>,
}
impl SourceAdministrativeAuthority {
    pub fn new(
        value: QueueSourceOrdinaryAuthority,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        if !value.cut().belongs_to(job) {
            return Err(unknown("source ordinary authority operation differs"));
        }
        let charge = job.owned_buffer(
            (std::mem::size_of::<OrdinaryOwner>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        Ok(Self {
            owner: Arc::new(OrdinaryOwner {
                value,
                _charge: charge,
            }),
            #[cfg(feature = "test-support")]
            probe: None,
        })
    }
    pub fn ordinary(&self) -> &QueueSourceOrdinaryAuthority {
        &self.owner.value
    }
    #[cfg(feature = "test-support")]
    pub fn with_worker_probe(mut self, probe: Option<Arc<FinalizationProbe>>) -> Self {
        self.probe = probe;
        self
    }
}
impl AdministrativeOrdinaryAuthority for SourceAdministrativeAuthority {
    fn snapshot(&self) -> &RecordSnapshot {
        self.ordinary().snapshot()
    }
    #[cfg(feature = "test-support")]
    fn worker_probe(&self) -> Option<Arc<FinalizationProbe>> {
        self.probe.clone()
    }
}
/// Borrowed declarations are validated against reader-minted provenance before
/// sealing, worker finalization and later assembly. No raw frame is accepted.
pub struct SourceAdministrativeFinalization<'a> {
    pub original: QueueSourceCut<'a>,
    pub queue: &'a QueueAddress,
    pub group: &'a CommitGroup,
    pub definition: &'a AdmittedQueueDefinition,
    pub limits: QueueLimits,
    pub record_bytes: u64,
}
/// Private semantics prevent replacing the finalizer result or scratch contents.
pub struct SourceAdministrativeResolution {
    result: Result<FinalizedAdministrativeSemantics<SourceAdministrativeAuthority>, Rejection>,
    source: QueueSourceAdministrativeScanContext,
    initial: SourceAdministrativeAuthority,
    fit: Option<(u64, u64)>,
    request: [u8; 32],
    pin: DefinitionPin,
    limits: QueueLimits,
    publication_bytes: u64,
    record_bytes: u64,
}
/// Immutable checked record; construction is private to shared source assembly.
pub struct SourceAdministrativePublication {
    record: OwnedQueueLogRecord,
    source: QueueSourceAdministrativeScanContext,
    initial: SourceAdministrativeAuthority,
}
pub struct SourceAdministrativePreview {
    value: OwnedQueueValue<PreviewResult, Vec<OwnedQueueBufferCharge>>,
    source: QueueSourceAdministrativeScanContext,
    initial: SourceAdministrativeAuthority,
}
/// Inner publication cap includes the checked outer record's exact overhead.
pub fn source_administrative_publication_limit(
    limits: &QueueLimits,
    record_bytes: u64,
) -> QueueResult<u64> {
    record_bytes
        .checked_sub(fireweed_engine::queue_log_record::checked_publication_record_len(0)?)
        .map(|bytes| bytes.min(limits.max_group_bytes))
        .filter(|bytes| *bytes > 0)
        .ok_or(QueueError::TooLarge)
}
#[allow(clippy::too_many_arguments)]
pub async fn finalize_source_administration(
    sink: OwnedAdministrativeSink,
    scan: QueueSourceAdministrativeScan,
    initial: SourceAdministrativeAuthority,
    input: SourceAdministrativeFinalization<'_>,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
    directory: &Path,
    quota: Arc<ScratchDiskQuota>,
) -> QueueResult<SourceAdministrativeResolution> {
    let publication_bytes =
        source_administrative_publication_limit(&input.limits, input.record_bytes)?;
    let (source, summary) = scan.into_parts();
    source.validate(
        input.original,
        input.queue,
        input.group,
        input.definition,
        initial.ordinary(),
        &summary,
        job,
    )?;
    // seal validates the exact packet scan identity, job, sequence and EOF count.
    let (mut scratch, summary) = sink.seal(summary).await?;
    source.validate(
        input.original,
        input.queue,
        input.group,
        input.definition,
        initial.ordinary(),
        &summary,
        job,
    )?;
    scratch.verify_context(
        input.queue,
        input.definition.pin(),
        initial.snapshot().next_sequence,
    )?;
    validate_ordinary_prospective_authority(
        input.group,
        initial.snapshot(),
        input.definition.schema(),
        job,
    )?;
    let finalized = finalize_administrative_semantics(
        scratch,
        summary,
        initial.clone(),
        input.group,
        input.definition.schema(),
        ownership_now,
        job,
        workers,
        directory,
        quota,
    )
    .await?;
    source.validate_authority(
        input.original,
        input.queue,
        input.group,
        input.definition,
        initial.ordinary(),
        job,
    )?;
    let mut fit = None;
    let result = match finalized {
        Err(reason) => Err(reason),
        Ok(value) => {
            source.validate(
                input.original,
                input.queue,
                input.group,
                input.definition,
                value.initial.ordinary(),
                &value.summary,
                job,
            )?;
            value.scratch.validate_publication_context(
                input.queue,
                input.definition.pin(),
                value.initial.snapshot().next_sequence,
                job,
            )?;
            let admin = input
                .group
                .administration
                .as_ref()
                .ok_or_else(|| unknown("source finalization intent missing"))?;
            match assess_administrative_publication(
                input.queue,
                input.definition.pin(),
                input.group,
                &admin.intent,
                &value.ordinary,
                &value.summary,
                AdministrativeCommandMeasurement {
                    command_limit: value.scratch.command_limit(),
                    command_bytes: value.scratch.command_bytes(),
                    command_count: value.scratch.command_count(),
                    overflowed: value.scratch.publication_overflowed(),
                },
                input.group.fingerprint(input.queue)?,
                admin.max_result_bytes,
                publication_bytes,
                job,
            )? {
                AdministrativePublicationDecision::TooLarge => Err(Rejection::TooLarge),
                AdministrativePublicationDecision::Fits { bytes, commands } => {
                    fit = Some((bytes, commands));
                    Ok(value)
                }
            }
        }
    };
    Ok(SourceAdministrativeResolution {
        source,
        initial,
        result,
        fit,
        request: input.group.fingerprint(input.queue)?,
        pin: input.definition.pin(),
        limits: input.limits,
        publication_bytes,
        record_bytes: input.record_bytes,
    })
}
impl SourceAdministrativePublication {
    /// Verify actual opaque cut, complete declarations and job BEFORE any SDK
    /// compilation read/debit. Numeric source equality never suffices.
    pub fn record_for(
        &self,
        original: &QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<&OwnedQueueLogRecord> {
        self.source.validate_authority(
            *original,
            queue,
            group,
            definition,
            self.initial.ordinary(),
            job,
        )?;
        if !self.record.belongs_to(job) {
            return Err(unknown("source publication operation differs"));
        }
        let checked = self.record.checked()?;
        original.validate_publication(&checked, job)?;
        if checked.view().definition_pin_bytes() != *definition.pin().as_bytes() {
            return Err(unknown("source publication definition differs"));
        }
        Ok(&self.record)
    }
}
impl SourceAdministrativePreview {
    pub fn validate(
        &self,
        original: QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.source.validate_authority(
            original,
            queue,
            group,
            definition,
            self.initial.ordinary(),
            job,
        )
    }
    pub fn into_value(self) -> OwnedQueueValue<PreviewResult, Vec<OwnedQueueBufferCharge>> {
        self.value
    }
}
