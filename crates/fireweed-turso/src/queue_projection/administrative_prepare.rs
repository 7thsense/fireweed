//! Private administrative producer. Selection/planning never mutates
//! projection authority; only replay of the conditional durable publication does.
use super::*;
use fireweed_core::queue_administration::{AdministrativeAction, AdministrativeOperation};
use fireweed_engine::{
    Clock,
    queue_administration::{
        AddressedAdministrativeInput, administrative_hydration_group,
        resolve_administrative_hydration, scan_addressed_group_to_sink_bounded,
        validate_ordinary_prospective_authority,
    },
    queue_log_record::encode_checked_publication_record,
    queue_memory::{group_expansion_budget, incoming_headers},
    queue_planner::{
        CommitDelta, RecordSnapshot, insert_unique_probes, prospective_unique_probes,
        required_unique_probes,
    },
    queue_publication::{
        AdministrativeCommandMeasurement, AdministrativePublicationBuilder,
        AdministrativePublicationDecision, AdministrativePublicationReceipt,
        AdministrativeReceiptOutcome, assess_administrative_publication,
        measure_administrative_success_floor,
    },
    queue_schema_codec::DefinitionPin,
};
use fireweed_queue_spool::ScratchRoot;
use fireweed_queue_spool::{
    AdministrativeOrdinaryAuthority, OwnedAdministrativeSink,
    ResolvedAdministrativePublicationInput, ScratchDiskQuota, finalize_administrative_semantics,
};

pub struct AdministrativePreparationInput {
    pub commit: CommitPreparationInput,
    pub scratch_root: Arc<ScratchRoot>,
    pub scratch_quota: Arc<ScratchDiskQuota>,
}
/// The key allocation is destroyed before its same-job admission guard.
#[cfg(target_os = "linux")]
struct CursorKey {
    bytes: Vec<u8>,
    _charge: Option<OwnedQueueBufferCharge>,
}
#[cfg(target_os = "linux")]
impl CursorKey {
    fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            _charge: None,
        }
    }
    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
    fn copy(key: &[u8], job: &Arc<QueueReservation>) -> QueueResult<Self> {
        let charge = job.owned_buffer(
            (key.len() as u64)
                .checked_mul(8)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        Ok(Self {
            bytes: key.to_vec(),
            _charge: Some(charge),
        })
    }
}
struct Authority {
    image: RecordSnapshot,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdministrativeOrdinaryAuthority for Authority {
    fn snapshot(&self) -> &RecordSnapshot {
        &self.image
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Publish { epoch: u64, max_record_bytes: u64 },
    Preview,
}
enum Parts {
    Publication {
        record: OwnedQueueLogRecord,
        receipt: CommitReceipt,
        head: u64,
        epoch: u64,
    },
    Preview(PreviewResult),
}
struct Completed {
    parts: Parts,
    charges: Vec<OwnedQueueBufferCharge>,
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: AdministrativePreparationInput,
    mode: Mode,
    clock: Arc<dyn Clock>,
    workers: ScratchWorkers,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Private prerequisite for administrative commits. Existing identities resolve
    /// through replay_commit. This does not activate a partial public backend.
    pub async fn prepare_administration(
        self: &Arc<Self>,
        input: AdministrativePreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedOrdinaryCommit>> {
        let result = self
            .dispatch_administration(
                input,
                Mode::Publish {
                    epoch,
                    max_record_bytes,
                },
                clock,
                job,
                workers,
            )
            .await?;
        match result {
            None => Ok(None),
            Some(Completed {
                parts:
                    Parts::Publication {
                        record,
                        receipt,
                        head,
                        epoch,
                    },
                charges,
            }) => Ok(Some(PreparedOrdinaryCommit::new(
                record,
                AdmittedCommitReceipt::new(receipt, charges),
                head,
                epoch,
            ))),
            Some(_) => Err(unknown("publication returned preview mode")),
        }
    }
    /// Same canonical proposal checks, without consulting execution identities
    /// or exposing any publishable record. Covered orchestration checks freshness.
    pub async fn preview_administration(
        self: &Arc<Self>,
        input: AdministrativePreparationInput,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedQueuePreview> {
        match self
            .dispatch_administration(input, Mode::Preview, clock, job, workers)
            .await?
        {
            Some(Completed {
                parts: Parts::Preview(result),
                charges,
            }) => Ok(AdmittedQueuePreview::new(result, charges)),
            _ => Err(unknown("preview did not return a complete preview")),
        }
    }
    async fn dispatch_administration(
        self: &Arc<Self>,
        input: AdministrativePreparationInput,
        mode: Mode,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<Completed>> {
        let commit = &input.commit;
        if !commit.genesis.belongs_to(job) {
            return Err(unknown(
                "administrative genesis belongs to another operation",
            ));
        }
        commit.group.preflight_for(&commit.queue, &commit.limits)?;
        fireweed_engine::queue_memory::administrative_groups_preflight(
            &commit.queue,
            std::slice::from_ref(&commit.group),
            &commit.limits,
        )?;
        let _admin = commit
            .group
            .administration
            .as_ref()
            .ok_or_else(|| QueueError::Invalid("administration required".into()))?;
        if matches!(
            mode,
            Mode::Publish { epoch: 0, .. }
                | Mode::Publish {
                    max_record_bytes: 0,
                    ..
                }
        ) {
            return Err(QueueError::Invalid(
                "invalid administrative preparation authority".into(),
            ));
        }
        #[cfg(not(target_os = "linux"))]
        if matches!(
            _admin.intent.operation,
            AdministrativeOperation::SelectFirst(_)
        ) {
            return Err(QueueError::Invalid(
                "native broad administration requires Linux".into(),
            ));
        }
        let headers = incoming_headers(&commit.group)?;
        let bytes = encoded_len(&(&commit.queue, &commit.group))?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| {
                n.checked_add((input.scratch_root.path().as_os_str().len() as u64).checked_mul(4)?)
            })
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            mode,
            clock,
            workers: workers.clone(),
            job: job.clone(),
            charges: vec![job.owned_buffer(bytes)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let parts = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(parts.map(|parts| Completed {
                        parts,
                        charges: std::mem::take(&mut worker.charges),
                    }))
                }))
            })
            .await?
    }
}
#[allow(clippy::too_many_arguments)]
async fn hydrate(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    sequence: u64,
    key: &str,
    action: &AdministrativeAction,
    evaluated_at: i64,
    job: &Arc<QueueReservation>,
) -> QueueResult<Authority> {
    let bytes = encoded_len(action)?
        .checked_mul(12)
        .and_then(|n| n.checked_add((key.len() as u64).checked_mul(32)?))
        .and_then(|n| n.checked_add(65536))
        .ok_or(QueueError::TooLarge)?;
    let mut charges = vec![job.owned_buffer(bytes)?];
    let mut group = administrative_hydration_group(key, action);
    let mut image = super::group_authority::load_group_with_fields(
        connection,
        queue_id,
        schema,
        &group,
        sequence,
        job,
        &mut charges,
        true,
    )
    .await?;
    if image.work.is_empty() {
        return Ok(Authority {
            image,
            _charges: charges,
        });
    }
    if image.work.len() != 1 || image.versions.len() != 1 {
        return Err(unknown("administrative hydration is not one row"));
    }
    resolve_administrative_hydration(&mut group, action, &image, schema)?;
    charges.push(
        job.owned_buffer(
            group_expansion_budget(&group, &image, schema)?
                .checked_mul(8)
                .ok_or(QueueError::TooLarge)?,
        )?,
    );
    let probes = if matches!(action, AdministrativeAction::Update { .. }) {
        required_unique_probes(&group, &image, evaluated_at, schema)?
    } else {
        insert_unique_probes(
            std::slice::from_ref(&image.work[key].record.work),
            &image,
            schema,
        )?
    };
    super::unique_authority::load_unique_authority(
        connection,
        queue_id,
        schema,
        &probes,
        &mut image,
        job,
        &mut charges,
    )
    .await?;
    image.unique_probes_loaded = probes;
    Ok(Authority {
        image,
        _charges: charges,
    })
}
impl NativeTransactionWork for Worker {
    type Output = Option<Parts>;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected administrative preparation precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let input = &self.input.commit;
        let native = super::snapshot::read_snapshot(connection, &input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("administrative projection missing"))?;
        native.validate_point(
            &input.genesis,
            &input.queue,
            (
                input.required_next_offset,
                input.required_insertion_sequence,
                input.required_last_record_epoch,
            ),
        )?;
        if matches!(self.mode, Mode::Publish { epoch, .. } if epoch < native.last_record_epoch) {
            return Err(unknown("administrative epoch precedes history"));
        }
        let schema = native.definition.schema();
        let group = &input.group;
        group.validate_for(&input.queue, schema, &input.limits)?;
        if matches!(self.mode, Mode::Publish { .. }) {
            match super::receipt_metadata::read_receipt_metadata(
                connection,
                native.queue_id,
                super::receipt_metadata::ReceiptDomain::Commit,
                &group.request_id,
                Some((4, group.fingerprint(&input.queue)?)),
            )
            .await?
            {
                super::receipt_metadata::ReceiptMetadata::Absent => {}
                super::receipt_metadata::ReceiptMetadata::Expired => {
                    return Err(QueueError::RequestExpired);
                }
                super::receipt_metadata::ReceiptMetadata::Present { .. } => return Ok(None),
            }
        }
        self.charges.push(
            self.job.owned_buffer(
                (native.definition.raw().len() as u64)
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let admin = group
            .administration
            .as_ref()
            .ok_or_else(|| unknown("administration disappeared"))?;
        let logical_max = input.limits.max_group_bytes;
        let floor =
            measure_administrative_success_floor(&input.queue, native.definition.pin(), group)
                .map_err(publication_error)?;
        let mut initial_charges = Vec::new();
        let mut initial = super::group_authority::load_group(
            connection,
            native.queue_id,
            schema,
            group,
            native.insertion_sequence,
            &self.job,
            &mut initial_charges,
        )
        .await?;
        initial_charges.push(
            self.job.owned_buffer(
                group_expansion_budget(group, &initial, schema)?
                    .checked_mul(8)
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let probes = prospective_unique_probes(group, &initial, schema)?;
        super::unique_authority::load_unique_authority(
            connection,
            native.queue_id,
            schema,
            &probes,
            &mut initial,
            &self.job,
            &mut initial_charges,
        )
        .await?;
        initial.unique_probes_loaded = probes;
        let sink = OwnedAdministrativeSink::new_with_command_limit(
            self.workers.clone(),
            self.input.scratch_root.path(),
            self.input.scratch_quota.clone(),
            &self.job,
            &input.queue,
            native.definition.pin(),
            native.insertion_sequence,
            logical_max.saturating_sub(floor),
        )
        .await?;
        let job = &self.job;
        let summary = match &admin.intent.operation {
            AdministrativeOperation::Addressed(entries) => {
                let source = fireweed_engine::queue_administration::addressed_administrative_source(
                    entries,
                    |position, entry| async move {
                        let row = hydrate(
                            connection,
                            native.queue_id,
                            schema,
                            native.insertion_sequence,
                            &entry.key,
                            &entry.action,
                            admin.intent.evaluated_at,
                            job,
                        )
                        .await?;
                        Ok(AddressedAdministrativeInput::OwnedAdmittedOneRow {
                            position,
                            key: entry.key.clone(),
                            image: row.image,
                            hydration: row._charges,
                        })
                    },
                );
                let delivery = sink.clone();
                scan_addressed_group_to_sink_bounded(
                    group,
                    &admin.intent,
                    source,
                    &initial,
                    schema,
                    &input.limits,
                    self.job.clone(),
                    admin.max_result_bytes,
                    move |packet| {
                        let sink = delivery.clone();
                        async move { sink.append(packet).await }
                    },
                )
                .await?
            }
            #[cfg(target_os = "linux")]
            AdministrativeOperation::SelectFirst(clauses) => {
                let selection = Box::pin(super::administrative_selection::Selection::prepare(
                    connection,
                    native.queue_id,
                    schema,
                    native.insertion_sequence,
                    clauses,
                    admin.intent.evaluated_at,
                    self.input.scratch_root.clone(),
                    self.input.scratch_quota.clone(),
                    job,
                ))
                .await?;
                let source = fireweed_engine::queue_administration::selected_administrative_source(
                    (selection, CursorKey::empty()),
                    move |(mut selection, after)| async move {
                        let selected = selection.next(after.as_slice()).await?;
                        drop(after);
                        let Some(selected) = selected else {
                            return Ok(None);
                        };
                        let clause = usize::try_from(selected.clause()).map_err(unknown)?;
                        let action = &clauses
                            .get(clause)
                            .ok_or_else(|| unknown("selected clause missing"))?
                            .action;
                        let key = std::str::from_utf8(selected.key()).map_err(unknown)?;
                        let identity = super::one(connection,
                            "SELECT key,version FROM q7_records WHERE id=? AND queue_id=? AND collection=0 AND active=1",
                            vec![Value::Integer(selected.id()), Value::Integer(native.queue_id)], 2).await?
                            .ok_or_else(|| unknown("selected identity disappeared"))?;
                        if identity
                            != vec![
                                blob(selected.key()),
                                blob(&selected.version().to_be_bytes()),
                            ]
                        {
                            return Err(unknown("selected physical identity changed"));
                        }
                        let row = hydrate(
                            connection,
                            native.queue_id,
                            schema,
                            native.insertion_sequence,
                            key,
                            action,
                            admin.intent.evaluated_at,
                            job,
                        )
                        .await?;
                        if row.image.work.get(key).map(|w| w.record.version)
                            != Some(selected.version())
                        {
                            return Err(unknown("selected hydrated version changed"));
                        }
                        let after = CursorKey::copy(selected.key(), job)?;
                        let hydration = row._charges;
                        Ok(Some((fireweed_engine::queue_administration::SelectedAdministrativeInput::OwnedAdmittedOneRow {
                            clause, image: row.image, hydration,
                        }, (selection, after))))
                    },
                );
                let delivery = sink.clone();
                fireweed_engine::queue_administration::scan_broad_group_to_sink(
                    group,
                    &admin.intent,
                    source,
                    &initial,
                    schema,
                    &input.limits,
                    self.job.clone(),
                    move |packet| {
                        let sink = delivery.clone();
                        async move { sink.append(packet).await }
                    },
                )
                .await?
            }
            #[cfg(not(target_os = "linux"))]
            AdministrativeOperation::SelectFirst(_) => {
                return Err(unknown("native broad administration requires Linux"));
            }
        };
        let (mut scratch, summary) = sink.seal(summary).await?;
        scratch.verify_context(
            &input.queue,
            native.definition.pin(),
            native.insertion_sequence,
        )?;
        validate_ordinary_prospective_authority(group, &initial, schema, &self.job)?;
        let finalized = finalize_administrative_semantics(
            scratch,
            summary,
            Authority {
                image: initial,
                _charges: initial_charges,
            },
            group,
            schema,
            || timestamp_nanos(self.clock.now()),
            &self.job,
            &self.workers,
            self.input.scratch_root.path(),
            self.input.scratch_quota.clone(),
        )
        .await?;
        let (frame, receipt, receipt_charge) = match finalized {
            Err(reason) => fallback(
                input,
                native.definition.pin(),
                native.insertion_sequence,
                reason,
                &self.job,
            )?,
            Ok(mut value) => {
                value.scratch.validate_publication_context(
                    &input.queue,
                    native.definition.pin(),
                    native.insertion_sequence,
                    &self.job,
                )?;
                let decision = assess_administrative_publication(
                    &input.queue,
                    native.definition.pin(),
                    group,
                    &admin.intent,
                    &value.ordinary,
                    &value.summary,
                    AdministrativeCommandMeasurement {
                        command_limit: value.scratch.command_limit(),
                        command_bytes: value.scratch.command_bytes(),
                        command_count: value.scratch.command_count(),
                        overflowed: value.scratch.publication_overflowed(),
                    },
                    group.fingerprint(&input.queue)?,
                    admin.max_result_bytes,
                    logical_max,
                    &self.job,
                )?;
                match decision {
                    AdministrativePublicationDecision::TooLarge => fallback(
                        input,
                        native.definition.pin(),
                        native.insertion_sequence,
                        Rejection::TooLarge,
                        &self.job,
                    )?,
                    AdministrativePublicationDecision::Fits { bytes, commands } => {
                        let administration = value
                            .summary
                            .result()
                            .ok_or_else(|| unknown("administrative result incomplete"))?;
                        let GroupOutcome::Committed { versions } =
                            &value.ordinary.delta().receipt.outcome
                        else {
                            return Err(unknown("ordinary proposal not committed"));
                        };
                        let frame = value.scratch.prepare_resolved_publication(
                            &ResolvedAdministrativePublicationInput {
                                queue: &input.queue,
                                pin: native.definition.pin(),
                                group,
                                intent: &admin.intent,
                                before: value.ordinary.initial_authority(),
                                ordinary: value.ordinary.delta(),
                                administration,
                                max_result_bytes: admin.max_result_bytes,
                                max_publication_bytes: logical_max,
                                expected_bytes: bytes,
                                expected_commands: commands,
                            },
                            &self.job,
                        )?;
                        let headers = fireweed_engine::queue_memory::administrative_result_headers(
                            administration,
                        )?
                        .checked_add(
                            (versions.len() as u64)
                                .checked_mul(256)
                                .ok_or(QueueError::TooLarge)?,
                        )
                        .ok_or(QueueError::TooLarge)?;
                        let receipt_charge = self.job.owned_buffer(
                            encoded_len(&(versions, administration))?
                                .checked_mul(16)
                                .and_then(|n| headers.checked_mul(4).and_then(|h| n.checked_add(h)))
                                .and_then(|n| n.checked_add(65536))
                                .ok_or(QueueError::TooLarge)?,
                        )?;
                        let receipt = CommitReceipt {
                            request_id: group.request_id.clone(),
                            outcome: GroupOutcome::AdministrativeCommitted {
                                versions: versions.clone(),
                                administration: administration.clone(),
                            },
                        };
                        (frame, receipt, receipt_charge)
                    }
                }
            }
        };
        self.charges.push(receipt_charge);
        match self.mode {
            Mode::Publish {
                epoch,
                max_record_bytes,
            } => {
                let record = encode_checked_publication_record(
                    epoch,
                    frame.as_bytes(),
                    max_record_bytes,
                    &self.job,
                )?;
                Ok(Some(Parts::Publication {
                    record,
                    receipt,
                    head: native.next_offset,
                    epoch,
                }))
            }
            Mode::Preview => {
                drop(frame);
                let result = PreviewResult::try_from(receipt.outcome)?;
                if encoded_len(&result)? > input.limits.max_response_bytes {
                    return Err(QueueError::TooLarge);
                }
                Ok(Some(Parts::Preview(result)))
            }
        }
    }
}
fn fallback(
    input: &CommitPreparationInput,
    pin: DefinitionPin,
    sequence: u64,
    reason: Rejection,
    job: &Arc<QueueReservation>,
) -> QueueResult<(
    fireweed_engine::queue_publication::AdministrativePublicationFrame,
    CommitReceipt,
    OwnedQueueBufferCharge,
)> {
    let group = &input.group;
    let admin = group
        .administration
        .as_ref()
        .ok_or_else(|| unknown("fallback lacks administration"))?;
    let outcome = AdministrativeReceiptOutcome::Rejected(&reason);
    let binding = AdministrativePublicationReceipt::new(
        &input.queue,
        group,
        &admin.intent,
        admin.max_result_bytes,
        &outcome,
        job,
    )
    .map_err(publication_error)?;
    let receipt_charge = job.owned_buffer(
        binding
            .complete_receipt_bytes()
            .map_err(publication_error)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let receipt = CommitReceipt {
        request_id: group.request_id.clone(),
        outcome: GroupOutcome::Rejected(reason.clone()),
    };
    let before = RecordSnapshot {
        next_sequence: sequence,
        ..Default::default()
    };
    let delta = CommitDelta {
        work: vec![],
        work_fields: Default::default(),
        work_bodies: Default::default(),
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: vec![],
        next_sequence: sequence,
        receipt: receipt.clone(),
    };
    let frame = AdministrativePublicationBuilder::new(
        &binding,
        pin,
        &before,
        &delta,
        0,
        0,
        input.limits.max_group_bytes,
        job,
    )
    .map_err(publication_error)?
    .finish()
    .map_err(publication_error)?;
    Ok((frame, receipt, receipt_charge))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod preview_tests;
