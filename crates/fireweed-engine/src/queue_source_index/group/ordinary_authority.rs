//! Owned ordinary authority for the shared administrative worker finalizer.
use super::*;
use crate::queue_schema_codec::DefinitionPin;
use crate::queue_source_index::scope::OwnedSourceCutBinding;

/// Immutable ordinary preimage bound to the entire enclosing request and actual
/// source identity. The owned coordinator retains the physical recovered source
/// through both worker awaits, independently of this snapshot and caller future.
/// Validation is required around the worker handoff; identity is not freshness.
pub struct QueueSourceOrdinaryAuthority {
    snapshot: RecordSnapshot,
    context: QueueSourceOrdinaryBinding,
    _retained_charges: Vec<OwnedQueueBufferCharge>,
    _charge: OwnedQueueBufferCharge,
}
/// Independently funded context retained around worker rejection as well as
/// success. This binds identity, not current storage freshness or SDK lifetime.
pub struct QueueSourceOrdinaryBinding {
    binding: OwnedSourceCutBinding,
    request: [u8; 32],
    definition: DefinitionPin,
}
impl QueueSourceOrdinaryBinding {
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.binding.cut()
    }
    pub fn validate(
        &self,
        original: QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.binding.validate(original, job)?;
        if self.cut().address().tenant != queue.tenant
            || self.cut().address().queue != queue.queue
            || !definition.belongs_to(job)
            || self.definition != definition.pin()
            || self.request != group.fingerprint(queue)?
        {
            return Err(unknown(
                "owned ordinary request, definition or queue differs",
            ));
        }
        Ok(())
    }
}
impl QueueSourceOrdinaryAuthority {
    pub fn snapshot(&self) -> &RecordSnapshot {
        &self.snapshot
    }
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.context.cut()
    }
    pub fn validate(
        &self,
        original: QueueSourceCut<'_>,
        queue: &QueueAddress,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.context
            .validate(original, queue, group, definition, job)
    }
    pub fn retain_binding(
        &self,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueSourceOrdinaryBinding> {
        Ok(QueueSourceOrdinaryBinding {
            binding: OwnedSourceCutBinding::capture(self.cut(), job)?,
            request: self.context.request,
            definition: self.context.definition,
        })
    }
}
/// Full administrative validation/admission, performed before source dispatch.
/// Borrowing prevents declarations from changing while this proof is consumed.
pub struct QueueSourceAdministrativeAdmission<'a> {
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    definition: &'a AdmittedQueueDefinition,
    job: &'a Arc<QueueReservation>,
    request: [u8; 32],
    charge: OwnedQueueBufferCharge,
}
pub fn admit_administrative_group<'a>(
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    definition: &'a AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceAdministrativeAdmission<'a>> {
    let admin = group
        .administration
        .as_ref()
        .ok_or_else(|| unknown("ordinary worker authority requires administrative context"))?;
    // Ordinary scratch deliberately ignores administration. This loader reruns
    // full validation, so it independently funds those validation containers.
    let bytes = request_scratch(group, definition.schema())?
        .checked_add(crate::queue_memory::incoming_headers(&admin.intent)?)
        .ok_or(QueueError::TooLarge)?;
    let charge = job.owned_buffer(bytes)?;
    group.validate_for(queue, definition.schema(), limits)?;
    let request = group.fingerprint(queue)?;
    if !definition.belongs_to(job) {
        return Err(unknown("administrative definition operation differs"));
    }
    Ok(QueueSourceAdministrativeAdmission {
        queue,
        group,
        definition,
        job,
        request,
        charge,
    })
}
/// Load only explicit ordinary work/state and prospective unique authority for
/// an administrative group. This does not plan the group or accept/omit its
/// administrative effects; the shared scanner and finalizer remain mandatory.
pub async fn load_administrative_ordinary_authority<R: QueueSourceRecordReader>(
    reader: &R,
    cut: QueueSourceCut<'_>,
    queue: &QueueAddress,
    group: &CommitGroup,
    definition: &AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceOrdinaryAuthority> {
    load_with_source(
        reader,
        OrdinarySource::Captured(cut),
        queue,
        group,
        definition,
        limits,
        job,
    )
    .await
}
pub(super) enum OrdinarySource<'a, 's> {
    Captured(QueueSourceCut<'a>),
    Header(&'s SourceImage<'a>),
}
pub(super) async fn load_with_source<R: QueueSourceRecordReader>(
    reader: &R,
    source: OrdinarySource<'_, '_>,
    queue: &QueueAddress,
    group: &CommitGroup,
    definition: &AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceOrdinaryAuthority> {
    let admission = admit_administrative_group(queue, group, definition, limits, job)?;
    load_admitted(reader, source, admission).await
}
pub(super) async fn load_admitted<R: QueueSourceRecordReader>(
    reader: &R,
    source: OrdinarySource<'_, '_>,
    admission: QueueSourceAdministrativeAdmission<'_>,
) -> QueueResult<QueueSourceOrdinaryAuthority> {
    let cut = match &source {
        OrdinarySource::Captured(cut) => *cut,
        OrdinarySource::Header(header) => header.cut,
    };
    let QueueSourceAdministrativeAdmission {
        queue,
        group,
        definition,
        job,
        request,
        charge,
    } = admission;
    let binding = OwnedSourceCutBinding::capture(cut, job)?;
    let mut image = match source {
        OrdinarySource::Captured(_) => {
            load_image_at(reader, cut, queue, group, definition, job, charge).await?
        }
        OrdinarySource::Header(header) => {
            if header.pin != definition.pin()
                || !reader.cut()?.same_cut(&cut)
                || !definition.belongs_to(job)
                || cut.address().tenant != queue.tenant
                || cut.address().queue != queue.queue
            {
                return Err(unknown("ordinary header source or request differs"));
            }
            load_image_contents(
                reader,
                group,
                definition,
                job,
                SourceImage {
                    cut,
                    pin: header.pin,
                    snapshot: RecordSnapshot {
                        next_sequence: header.snapshot.next_sequence,
                        ..Default::default()
                    },
                    retained_charges: Vec::new(),
                    charge,
                },
            )
            .await?
        }
    };
    image.load_unique(reader, group, definition, job).await?;
    let authority = QueueSourceOrdinaryAuthority {
        snapshot: image.snapshot,
        context: QueueSourceOrdinaryBinding {
            binding,
            request,
            definition: definition.pin(),
        },
        _retained_charges: image.retained_charges,
        _charge: image.charge,
    };
    authority.validate(cut, queue, group, definition, job)?;
    Ok(authority)
}
