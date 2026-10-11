//! One poisoned operation-local owner for exact commands and unique evidence.
use super::evidence::BorrowedScratchEffect;
use super::*;
use fireweed_core::queue_contract::QueueAddress;
use fireweed_engine::queue_administration::{
    AdministrativeScanIdentity, AdmittedAdministrativeScan, ResolvedAdministrativeRow,
};
use fireweed_engine::queue_publication::{
    CommandRef, encode_administrative_work_fragment, measure_administrative_work_fragment,
    parse_administrative_work_fragment,
};
use fireweed_engine::queue_schema_codec::DefinitionPin;
use fireweed_engine::queue_unique_stream::{UniqueClaimOrigin, UniqueTupleDecision};
use sha2::{Digest, Sha256};
const MAGIC: &[u8] = b"FWWC1";
const HEADER: usize = 13;
const TRAILER: usize = 32;
struct Context {
    queue: QueueAddress,
    pin: DefinitionPin,
    initial: u64,
    scan: Option<AdministrativeScanIdentity>,
    delivered: u64,
    command_count: u64,
    command_bytes: u64,
    max_command_bytes: u64,
    publication_overflow: bool,
    binding: [u8; 32],
    _charge: OwnedQueueBufferCharge,
}
impl Context {
    fn verify(&self, queue: &QueueAddress, pin: DefinitionPin, initial: u64) -> QueueResult<()> {
        if &self.queue != queue || self.pin != pin || self.initial != initial {
            return Err(unknown("administrative scratch context mismatch"));
        }
        Ok(())
    }
}
/// Owns both streams; no independently finishable handle escapes. All synchronous
/// methods run under the backend's owned blocking boundary and queue fence.
pub struct AdministrativeScratch {
    commands: QueueSpool,
    evidence: QueueSpool,
    poisoned: bool,
    context: Context,
}
impl AdministrativeScratch {
    pub fn new(
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
    ) -> QueueResult<Self> {
        Self::new_with_command_limit(directory, quota, reservation, queue, pin, initial, u64::MAX)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_command_limit(
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
        max_command_bytes: u64,
    ) -> QueueResult<Self> {
        let bytes = add(queue.tenant.len() as u64, queue.queue.len() as u64)?
            .checked_mul(4)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| unknown("context admission overflow"))?;
        let charge = reservation.owned_buffer(bytes)?;
        let mut commands = QueueSpool::new(directory, Arc::clone(&quota), reservation)?;
        let mut evidence = QueueSpool::new(directory, quota, reservation)?;
        let mut hash = Sha256::new();
        hash.update(b"fireweed-administrative-scratch-owner-v1\0");
        for segment in [
            queue.tenant.as_bytes(),
            queue.queue.as_bytes(),
            pin.as_bytes().as_slice(),
        ] {
            hash.update((segment.len() as u64).to_be_bytes());
            hash.update(segment);
        }
        hash.update(initial.to_be_bytes());
        // Operation-local, held-alive identity: never a durable log format.
        hash.update((Arc::as_ptr(&commands.token) as usize).to_be_bytes());
        let binding = hash.finalize().into();
        commands.binding = binding;
        evidence.binding = binding;
        Ok(Self {
            commands,
            evidence,
            poisoned: false,
            context: Context {
                queue: queue.clone(),
                pin,
                initial,
                scan: None,
                delivered: 0,
                command_count: 0,
                command_bytes: 0,
                max_command_bytes,
                publication_overflow: false,
                binding,
                _charge: charge,
            },
        })
    }
    pub fn append(&mut self, packet: ResolvedAdministrativeRow) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("append to poisoned administrative scratch"));
        }
        self.poisoned = true;
        if !std::ptr::eq(packet.reservation(), self.context._charge.reservation())
            || packet.initial_authority().next_sequence != self.context.initial
            || packet.ordinal() != self.context.delivered
        {
            return Err(unknown("foreign administrative operation authority"));
        }
        let delivered = self
            .context
            .delivered
            .checked_add(1)
            .ok_or_else(|| unknown("administrative delivery count overflow"))?;
        let scan = packet.scan_identity();
        if self.context.scan.as_ref().is_some_and(|id| id != &scan) {
            return Err(unknown("foreign administrative scan packet"));
        }
        self.context.scan = Some(scan);
        let initial = packet.initial_authority();
        for tuple in &initial.unique_probes_loaded {
            self.evidence.append_borrowed_evidence(
                tuple,
                initial.unique_owners.get(tuple),
                BorrowedScratchEffect::Observe,
            )?;
        }
        if let Some(effects) = packet.accepted_unique_effects() {
            for (tuple, owner) in &effects.releases {
                self.evidence.append_borrowed_evidence(
                    tuple,
                    initial.unique_owners.get(tuple),
                    BorrowedScratchEffect::Release(owner),
                )?;
            }
            for (tuple, owner) in &effects.claims {
                self.evidence.append_borrowed_evidence(
                    tuple,
                    initial.unique_owners.get(tuple),
                    BorrowedScratchEffect::Claim {
                        owner,
                        origin: UniqueClaimOrigin::Administrative,
                    },
                )?;
            }
        }
        if let Some(delta) = packet.command() {
            // Even after permanent overflow, validate/count every accepted
            // command through the same canonical writer without payload copies.
            let size = measure_administrative_work_fragment(initial, delta)
                .map_err(|error| unknown(format!("resolved fragment: {error:?}")))?;
            self.context.command_count = add(self.context.command_count, 1)?;
            self.context.command_bytes = add(self.context.command_bytes, size)?;
            self.context.publication_overflow |=
                self.context.command_bytes > self.context.max_command_bytes;
            if !self.context.publication_overflow {
                let temporary = self.context._charge.child_buffer(0)?;
                let fragment =
                    encode_administrative_work_fragment(initial, delta, temporary.reservation())
                        .map_err(|error| unknown(format!("resolved fragment: {error:?}")))?;
                let command =
                    parse_administrative_work_fragment(fragment.as_bytes(), self.context.initial)
                        .map_err(|error| unknown(format!("resolved fragment: {error:?}")))?;
                let size = add(fragment.as_bytes().len() as u64, (HEADER + TRAILER) as u64)?;
                let _encoding = self.context._charge.child_buffer(
                    size.checked_mul(4)
                        .and_then(|n| n.checked_add(8192))
                        .ok_or_else(|| unknown("command encoding admission overflow"))?,
                )?;
                let mut payload = Vec::with_capacity(usize::try_from(size).map_err(unknown)?);
                payload.extend_from_slice(MAGIC);
                payload.extend_from_slice(&self.context.initial.to_le_bytes());
                payload.extend_from_slice(fragment.as_bytes());
                let digest = checksum(
                    command.identity.key,
                    self.commands.count,
                    &payload,
                    &self.context.binding,
                );
                payload.extend_from_slice(&digest);
                self.commands.append_components(
                    None,
                    "",
                    command.identity.key.as_bytes(),
                    &payload,
                )?;
            }
        }
        self.context.delivered = delivered;
        self.poisoned = false;
        Ok(())
    }
    /// Requires successful selected-source/sink EOF from the SAME scan, job and
    /// initial sequence. Sealing still does not validate final business outcomes.
    pub fn seal(
        mut self,
        summary: &AdmittedAdministrativeScan,
    ) -> QueueResult<SealedAdministrativeScratch> {
        if self.poisoned
            || !std::ptr::eq(summary.reservation(), self.context._charge.reservation())
            || summary.initial_sequence() != self.context.initial
            || summary.packet_count() != self.context.delivered
        {
            return Err(unknown("administrative scratch lacks its complete scan"));
        }
        use fireweed_core::queue_administration::AdministrativeOutcomeKind;
        let expected_commands = add(
            summary
                .histogram()
                .outcomes
                .get(&AdministrativeOutcomeKind::Changed)
                .copied()
                .unwrap_or(0),
            summary
                .histogram()
                .outcomes
                .get(&AdministrativeOutcomeKind::Deleted)
                .copied()
                .unwrap_or(0),
        )?;
        if expected_commands != self.context.command_count {
            return Err(unknown("administrative command/result count mismatch"));
        }
        let scan = summary.scan_identity();
        if self.context.scan.as_ref().is_some_and(|id| id != &scan) {
            return Err(unknown("foreign administrative scan summary"));
        }
        self.context.scan = Some(scan);
        Ok(SealedAdministrativeScratch {
            commands: self.commands.seal()?,
            evidence: self.evidence.seal()?,
            poisoned: false,
            predecision_validated: false,
            context: self.context,
        })
    }
}
fn checksum(key: &str, ordinal: u64, bytes: &[u8], binding: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"fireweed-scratch-work-command-v1\0");
    hash.update(binding);
    hash.update(ordinal.to_be_bytes());
    for segment in [key.as_bytes(), bytes] {
        hash.update((segment.len() as u64).to_be_bytes());
        hash.update(segment);
    }
    hash.finalize().into()
}
pub struct AdmittedScratchWorkCommand {
    bytes: Vec<u8>,
    initial: u64,
    _charge: OwnedQueueBufferCharge,
}
impl AdmittedScratchWorkCommand {
    pub fn command(&self) -> QueueResult<CommandRef<'_>> {
        parse_administrative_work_fragment(
            &self.bytes[HEADER..self.bytes.len() - TRAILER],
            self.initial,
        )
        .map_err(|error| unknown(format!("resolved fragment: {error:?}")))
    }
}
pub struct SealedAdministrativeScratch {
    commands: SortedSpool,
    evidence: SortedSpool,
    poisoned: bool,
    predecision_validated: bool,
    context: Context,
}
impl SealedAdministrativeScratch {
    pub fn validate_publication_context(
        &self,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
        reservation: &QueueReservation,
    ) -> QueueResult<()> {
        if self.poisoned
            || !self.predecision_validated
            || !std::ptr::eq(reservation, self.context._charge.reservation())
        {
            return Err(unknown("publication lacks validated scratch/job authority"));
        }
        self.context.verify(queue, pin, initial)
    }
    pub fn publication_overflowed(&self) -> bool {
        self.context.publication_overflow
    }
    pub fn command_limit(&self) -> u64 {
        self.context.max_command_bytes
    }
    pub fn command_bytes(&self) -> u64 {
        self.context.command_bytes
    }
    pub fn command_count(&self) -> u64 {
        self.context.command_count
    }
    /// Exhaust command integrity and the administrative/ordinary-authority union
    /// BEFORE overlap, sibling collision or ordinary CAS/lease decisions. Caller
    /// first verifies complete expected prospective ordinary probes under fence.
    pub fn validate_before_business(
        &mut self,
        ordinary: &fireweed_engine::queue_planner::RecordSnapshot,
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
    ) -> QueueResult<UniqueTupleDecision> {
        if self.poisoned {
            return Err(unknown("predecision on poisoned scratch"));
        }
        let result = (|| {
            self.rewind_commands()?;
            while let Some(command) = self.next_retained_command()? {
                drop(command);
            }
            self.rewind_commands()?;
            self.reduce_bound_ordinary(ordinary, None, directory, quota, reservation)
        })();
        if result.is_err() {
            self.poisoned = true;
        } else {
            self.predecision_validated = true;
        }
        result
    }
    /// Ordinary success remains provisional: this final union includes releases
    /// and claims without reselecting rows or prematurely checking initial owners.
    pub fn reduce_with_ordinary_effects(
        &mut self,
        ordinary: &fireweed_engine::queue_planner::RecordSnapshot,
        effects: &fireweed_engine::queue_fields::UniqueFieldEffects,
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
    ) -> QueueResult<UniqueTupleDecision> {
        if self.poisoned || !self.predecision_validated {
            self.poisoned = true;
            return Err(unknown("final union lacks complete predecision validation"));
        }
        let result =
            self.reduce_bound_ordinary(ordinary, Some(effects), directory, quota, reservation);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn reduce_bound_ordinary(
        &mut self,
        ordinary: &fireweed_engine::queue_planner::RecordSnapshot,
        effects: Option<&fireweed_engine::queue_fields::UniqueFieldEffects>,
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
    ) -> QueueResult<UniqueTupleDecision> {
        if ordinary.next_sequence != self.context.initial
            || !std::ptr::eq(reservation.as_ref(), self.context._charge.reservation())
        {
            return Err(unknown("foreign ordinary evidence context"));
        }
        let mut spool = QueueSpool::new(directory, quota, reservation)?;
        spool.binding = self.context.binding;
        for tuple in &ordinary.unique_probes_loaded {
            spool.append_borrowed_evidence(
                tuple,
                ordinary.unique_owners.get(tuple),
                BorrowedScratchEffect::Observe,
            )?;
        }
        if let Some(effects) = effects {
            for (tuple, owner) in &effects.releases {
                if !ordinary.unique_probes_loaded.contains(tuple) {
                    return Err(unknown("ordinary release authority missing"));
                }
                spool.append_borrowed_evidence(
                    tuple,
                    ordinary.unique_owners.get(tuple),
                    BorrowedScratchEffect::Release(owner),
                )?;
            }
            for (tuple, owner) in &effects.claims {
                if !ordinary.unique_probes_loaded.contains(tuple) {
                    return Err(unknown("ordinary claim authority missing"));
                }
                spool.append_borrowed_evidence(
                    tuple,
                    ordinary.unique_owners.get(tuple),
                    BorrowedScratchEffect::Claim {
                        owner,
                        origin: UniqueClaimOrigin::Ordinary,
                    },
                )?;
            }
        }
        let mut ordinary = spool.seal()?;
        self.evidence.reduce_evidence_union(&mut ordinary)
    }
    pub fn verify_context(
        &mut self,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned administrative scratch"));
        }
        let result = self.context.verify(queue, pin, initial);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    pub fn reduce_evidence(&mut self) -> QueueResult<UniqueTupleDecision> {
        if self.poisoned {
            return Err(unknown("poisoned administrative scratch"));
        }
        let result = self.evidence.reduce_unique_evidence();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    pub fn rewind_commands(&mut self) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned administrative scratch"));
        }
        let result = self.commands.rewind();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    /// Caller must exhaust and validate the entire command source before ACK.
    /// This method never reselects, reruns CAS/lease/time or allocates sequences.
    pub fn next_command(&mut self) -> QueueResult<Option<AdmittedScratchWorkCommand>> {
        if self.context.publication_overflow {
            self.poisoned = true;
            return Err(unknown("overflowed command source cannot be applied"));
        }
        self.next_retained_command()
    }
    fn next_retained_command(&mut self) -> QueueResult<Option<AdmittedScratchWorkCommand>> {
        if self.poisoned {
            return Err(unknown("poisoned administrative scratch"));
        }
        let result = self.next_checked();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn next_checked(&mut self) -> QueueResult<Option<AdmittedScratchWorkCommand>> {
        let previous = self.commands.previous;
        let Some(entry) = self.commands.next_entry()? else {
            return Ok(None);
        };
        let (collection, index, key_bytes, payload_bytes) = entry.lengths();
        if collection.is_some() || index != 0 || payload_bytes < (HEADER + TRAILER) as u64 {
            return Err(unknown("invalid command spool key/envelope"));
        }
        if let Some(previous) = previous
            && compare_segment(
                &mut self.commands.data,
                (previous.0[5], previous.0[6]),
                (entry.descriptor.0[5], entry.descriptor.0[6]),
            )? == Ordering::Equal
        {
            return Err(unknown("duplicate command spool identity"));
        }
        let charge = self.context._charge.child_buffer(
            payload_bytes
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| unknown("command decoding admission overflow"))?,
        )?;
        let mut bytes = vec![0; usize::try_from(payload_bytes).map_err(unknown)?];
        if self
            .commands
            .read_component(&entry, SpoolComponent::Payload, 0, &mut bytes)? as u64
            != payload_bytes
            || !bytes.starts_with(MAGIC)
            || u64::from_le_bytes(bytes[5..HEADER].try_into().unwrap()) != self.context.initial
        {
            return Err(unknown("invalid command spool envelope"));
        }
        let packet = AdmittedScratchWorkCommand {
            bytes,
            initial: self.context.initial,
            _charge: charge,
        };
        let command = packet.command()?;
        if key_bytes != command.identity.key.len() as u64 {
            return Err(unknown("command spool identity mismatch"));
        }
        let mut window = [0; WINDOW];
        let mut position = 0;
        while position < key_bytes {
            let length = usize::try_from((key_bytes - position).min(WINDOW as u64)).unwrap();
            if self.commands.read_component(
                &entry,
                SpoolComponent::Tuple,
                position,
                &mut window[..length],
            )? != length
                || window[..length]
                    != command.identity.key.as_bytes()
                        [position as usize..position as usize + length]
            {
                return Err(unknown("command spool identity mismatch"));
            }
            position += length as u64;
        }
        let raw = &packet.bytes;
        if checksum(
            command.identity.key,
            entry.descriptor.0[9],
            &raw[..raw.len() - TRAILER],
            &self.context.binding,
        )
        .as_slice()
            != &raw[raw.len() - TRAILER..]
        {
            return Err(unknown("command checksum mismatch"));
        }
        Ok(Some(packet))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use fireweed_core::queue_administration::*;
    use fireweed_core::queue_contract::*;
    use fireweed_engine::queue_administration::{
        SelectedAdministrativeInput, scan_broad_group_to_sink,
    };
    use fireweed_engine::queue_admission::QueueAdmission;
    use fireweed_engine::queue_fields::FieldCoverage;
    use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot, StoredWork};
    use fireweed_engine::queue_publication::{BodyCommandRef, EffectRef, FieldsCommandRef};
    use std::collections::{BTreeMap, BTreeSet};
    pub(crate) fn queue() -> QueueAddress {
        QueueAddress {
            tenant: "scratch".into(),
            queue: "administrative".into(),
        }
    }
    pub(crate) fn pin() -> DefinitionPin {
        DefinitionPin::from_persisted_bytes(b"raw-definition")
    }
    fn schema(unique: bool) -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::from([
                    ("a".into(), FieldType::Integer),
                    ("b".into(), FieldType::Bytes),
                ]),
                indexes: if unique {
                    BTreeMap::from([("ab".into(), vec!["a".into(), "b".into()])])
                } else {
                    BTreeMap::new()
                },
                unique_indexes: if unique {
                    BTreeSet::from(["ab".into()])
                } else {
                    BTreeSet::new()
                },
                additional_field_prefixes: BTreeSet::new(),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1_000_000_000,
            max_attempts: 3,
        }
    }
    pub(crate) fn run_scan(
        count: u64,
        large: usize,
        unique: bool,
        job: &Arc<QueueReservation>,
        sink: impl FnMut(ResolvedAdministrativeRow) -> QueueResult<()>,
    ) -> QueueResult<AdmittedAdministrativeScan> {
        run_scan_options(count, large, unique, job, false, sink)
    }
    fn run_scan_options(
        count: u64,
        large: usize,
        unique: bool,
        job: &Arc<QueueReservation>,
        reject_last: bool,
        sink: impl FnMut(ResolvedAdministrativeRow) -> QueueResult<()>,
    ) -> QueueResult<AdmittedAdministrativeScan> {
        run_scan_action(count, large, unique, job, reject_last, false, sink)
    }
    fn run_scan_action(
        count: u64,
        large: usize,
        unique: bool,
        job: &Arc<QueueReservation>,
        reject_last: bool,
        no_op: bool,
        sink: impl FnMut(ResolvedAdministrativeRow) -> QueueResult<()>,
    ) -> QueueResult<AdmittedAdministrativeScan> {
        run_scan_with_body(count, large, unique, job, reject_last, no_op, 0, sink)
    }
    #[allow(clippy::too_many_arguments)]
    fn run_scan_with_body(
        count: u64,
        large: usize,
        unique: bool,
        job: &Arc<QueueReservation>,
        reject_last: bool,
        no_op: bool,
        body_size: usize,
        mut sink: impl FnMut(ResolvedAdministrativeRow) -> QueueResult<()>,
    ) -> QueueResult<AdmittedAdministrativeScan> {
        let schema = schema(unique);
        let group = CommitGroup {
            request_id: "operation".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: if no_op {
                        WorkPatch::default()
                    } else if unique {
                        WorkPatch {
                            fields: BTreeMap::from([("a".into(), Some(FieldValue::Integer(999)))]),
                            ..Default::default()
                        }
                    } else {
                        WorkPatch {
                            priority: Some(17),
                            body: (body_size != 0).then(|| vec![5; body_size].into()),
                            ..Default::default()
                        }
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: BTreeSet::new(),
                },
            }]),
        };
        let before = RecordSnapshot {
            next_sequence: count,
            ..Default::default()
        };
        let source = futures::stream::iter((0..count).map(|index| {
            let hydration = job
                .buffer(if large == 0 { 65536 } else { 64 << 20 })
                .unwrap();
            let key = format!("row-{index:05}");
            let fields = BTreeMap::from([
                ("a".into(), FieldValue::Integer(index as i64)),
                ("b".into(), FieldValue::Bytes(vec![7; large])),
            ]);
            let mut image = RecordSnapshot {
                work: BTreeMap::from([(
                    key.clone(),
                    StoredWork {
                        record: WorkRecord {
                            work: NewWork {
                                key: key.clone(),
                                priority: 0,
                                available_at: 0,
                                fields,
                                body: vec![8; large].into(),
                            },
                            version: 1,
                            insertion_sequence: index + 1,
                            lifecycle: Lifecycle::Pending,
                            eligible_since: 0,
                            attempts: 0,
                            lease_expires_at: None,
                        },
                        lease_hash: None,
                        field_coverage: FieldCoverage::Complete,
                        body_loaded: true,
                    },
                )]),
                versions: BTreeMap::from([(
                    RecordIdentity {
                        collection: None,
                        key: key.clone(),
                    },
                    1,
                )]),
                next_sequence: count,
                ..Default::default()
            };
            if unique {
                let values = &image.work[&key].record.work.fields;
                let old = UniqueIdentity {
                    collection: None,
                    index: "ab".into(),
                    full_sort_key: fireweed_engine::queue_index::ordered_key(&[
                        values.get("a"),
                        values.get("b"),
                    ])
                    .unwrap(),
                };
                let new = UniqueIdentity {
                    collection: None,
                    index: "ab".into(),
                    full_sort_key: fireweed_engine::queue_index::ordered_key(&[
                        Some(&FieldValue::Integer(999)),
                        values.get("b"),
                    ])
                    .unwrap(),
                };
                image
                    .unique_probes_loaded
                    .extend([old.clone(), new.clone()]);
                if reject_last && index + 1 == count {
                    image.unique_owners.insert(
                        new,
                        RecordIdentity {
                            collection: None,
                            key: "foreign".into(),
                        },
                    );
                }
                image.unique_owners.insert(
                    old,
                    RecordIdentity {
                        collection: None,
                        key,
                    },
                );
            }
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image,
                hydration,
            })
        }));
        futures::executor::block_on(scan_broad_group_to_sink(
            &group,
            &intent,
            source,
            &before,
            &schema,
            &QueueLimits::default(),
            Arc::clone(job),
            |packet| futures::future::ready(sink(packet)),
        ))
    }
    #[test]
    fn oversized_body_group_does_not_fill_disk_after_command_overflow() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(1 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new_with_command_limit(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            32,
            300000,
        )
        .unwrap();
        let summary = run_scan_with_body(32, 0, false, &job, false, false, 262144, |packet| {
            scratch.append(packet)
        })
        .unwrap();
        let mut scratch = scratch.seal(&summary).unwrap();
        assert_eq!(summary.histogram().matched, 32);
        assert_eq!(scratch.command_count(), 32);
        assert!(scratch.command_bytes() > 8 << 20);
        assert!(scratch.publication_overflowed());
        assert!(scratch.commands.data_bytes < 300000);
        assert!(quota.usage().unwrap() < 1 << 20);
        scratch
            .validate_before_business(
                &RecordSnapshot {
                    next_sequence: 32,
                    ..Default::default()
                },
                directory.path(),
                Arc::clone(&quota),
                &job,
            )
            .unwrap();
        assert!(scratch.next_command().is_err());
        drop(scratch);
        drop(summary);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn command_capture_limits_are_exact_and_overflow_continues_all_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(32 << 20).unwrap();
        let size = std::cell::Cell::new(0);
        let summary = run_scan(1, 0, false, &job, |packet| {
            let measured = measure_administrative_work_fragment(
                packet.initial_authority(),
                packet.command().unwrap(),
            )
            .unwrap();
            let encoded = encode_administrative_work_fragment(
                packet.initial_authority(),
                packet.command().unwrap(),
                &job,
            )
            .unwrap();
            assert_eq!(measured, encoded.as_bytes().len() as u64);
            size.set(measured);
            Ok(())
        })
        .unwrap();
        drop(summary);
        for limit in [size.get(), size.get() - 1] {
            let mut scratch = AdministrativeScratch::new_with_command_limit(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
                limit,
            )
            .unwrap();
            let summary = run_scan(1, 0, false, &job, |packet| scratch.append(packet)).unwrap();
            let mut scratch = scratch.seal(&summary).unwrap();
            assert_eq!(scratch.command_count(), 1);
            assert_eq!(scratch.command_bytes(), size.get());
            assert_eq!(scratch.publication_overflowed(), limit < size.get());
            if limit == size.get() {
                assert!(scratch.next_command().unwrap().is_some());
                assert!(scratch.next_command().unwrap().is_none());
            } else {
                assert_eq!(scratch.commands.data_bytes, 0);
                assert!(scratch.next_command().is_err());
            }
            drop(scratch);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
        for corrupt in [false, true] {
            let mut scratch = AdministrativeScratch::new_with_command_limit(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                5001,
                256,
            )
            .unwrap();
            let summary = run_scan(5001, 0, true, &job, |packet| scratch.append(packet)).unwrap();
            let mut scratch = scratch.seal(&summary).unwrap();
            assert_eq!(scratch.command_count(), 5000); // row 999 already has its target value
            assert_eq!(summary.histogram().matched, 5001);
            assert_eq!(summary.packet_count(), 5000);
            assert!(scratch.publication_overflowed());
            assert!(scratch.commands.data_bytes < 1024);
            assert!(scratch.evidence.count > 5001);
            if corrupt {
                scratch
                    .evidence
                    .data
                    .set_len(scratch.evidence.data_bytes - 1)
                    .unwrap();
                assert!(
                    scratch
                        .validate_before_business(
                            &RecordSnapshot {
                                next_sequence: 5001,
                                ..Default::default()
                            },
                            directory.path(),
                            Arc::clone(&quota),
                            &job
                        )
                        .is_err()
                );
            } else {
                let decision = scratch
                    .validate_before_business(
                        &RecordSnapshot {
                            next_sequence: 5001,
                            ..Default::default()
                        },
                        directory.path(),
                        Arc::clone(&quota),
                        &job,
                    )
                    .unwrap();
                assert!(decision.administrative_collision); // Complete evidence still finds sibling conflict.
                assert!(scratch.next_command().is_err()); // Partial stream never applies.
            }
            drop(scratch);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn physical_command_sink_drains_5001_changes_and_replays_exact_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(8 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            5001,
        )
        .unwrap();
        let summary = run_scan(5001, 0, false, &job, |packet| scratch.append(packet)).unwrap();
        let mut sealed = scratch.seal(&summary).unwrap();
        sealed.verify_context(&queue(), pin(), 5001).unwrap();
        assert!(!sealed.reduce_evidence().unwrap().final_collision);
        for _ in 0..2 {
            let mut count = 0;
            while let Some(packet) = sealed.next_command().unwrap() {
                let command = packet.command().unwrap();
                assert_eq!(command.identity.key, format!("row-{count:05}"));
                assert_eq!(command.expected_version, Some(1));
                assert_eq!(command.prior_highwater, 1);
                assert_eq!(command.output_highwater, 2);
                let EffectRef::WorkPut { metadata, body, .. } = command.effect else {
                    panic!("expected put")
                };
                assert_eq!(metadata.priority, 17);
                assert_eq!(metadata.insertion_sequence, count + 1);
                assert!(matches!(body, BodyCommandRef::Preserve));
                count += 1;
            }
            assert_eq!(count, 5001);
            sealed.rewind_commands().unwrap();
        }
        drop(summary);
        drop(job);
        assert_eq!(admission.usage().unwrap().0, 1);
        drop(sealed);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn unchanged_one_mib_composite_dependency_and_body_are_absent_from_command() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(32 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            2,
        )
        .unwrap();
        let summary = run_scan(2, 1 << 20, true, &job, |packet| scratch.append(packet)).unwrap();
        assert!(scratch.evidence.data_bytes > 8 << 20);
        assert!(scratch.commands.data_bytes < 1024);
        let mut sealed = scratch.seal(&summary).unwrap();
        let decision = sealed.reduce_evidence().unwrap();
        assert!(decision.administrative_collision && decision.final_collision);
        let packet = sealed.next_command().unwrap().unwrap();
        let command = packet.command().unwrap();
        let EffectRef::WorkPut {
            fields: FieldsCommandRef::Edit(fields),
            body,
            ..
        } = command.effect
        else {
            panic!("expected edit")
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields.iter().next().unwrap().unwrap().0, "a");
        assert!(matches!(body, BodyCommandRef::Preserve));
        assert!(packet.bytes.len() < 256);
        drop(packet);
        drop(sealed);
        drop(summary);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn partial_command_append_after_evidence_permanently_invalidates_both_streams() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            1,
        )
        .unwrap();
        scratch.commands.fault = Some(TestFault::PartialAppend);
        let summary = run_scan(1, 0, true, &job, |packet| {
            assert!(scratch.append(packet).is_err());
            assert!(scratch.evidence.count > 0);
            assert!(scratch.poisoned);
            Ok(()) // Deliberately swallow source sink error: sealing must still fail.
        })
        .unwrap();
        assert!(scratch.seal(&summary).is_err());
        drop(summary);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn foreign_scan_job_summary_and_context_poison_the_operation() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let other_job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for mode in 0..5 {
            let mut scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
            )
            .unwrap();
            let summary = run_scan(1, 0, false, &job, |packet| scratch.append(packet)).unwrap();
            match mode {
                0 => {
                    assert!(run_scan(1, 0, false, &job, |packet| scratch.append(packet)).is_err());
                    assert!(scratch.seal(&summary).is_err());
                }
                1 => {
                    assert!(
                        run_scan(1, 0, false, &other_job, |packet| scratch.append(packet)).is_err()
                    );
                    assert!(scratch.seal(&summary).is_err());
                }
                2 => {
                    let foreign_summary = run_scan(1, 0, false, &job, |_| Ok(())).unwrap();
                    assert!(scratch.seal(&foreign_summary).is_err());
                }
                _ => {
                    let mut sealed = scratch.seal(&summary).unwrap();
                    let mut foreign = queue();
                    foreign.queue.push('!');
                    let result = if mode == 3 {
                        sealed.verify_context(&foreign, pin(), 1)
                    } else {
                        sealed.verify_context(
                            &queue(),
                            DefinitionPin::from_persisted_bytes(b"different"),
                            1,
                        )
                    };
                    assert!(result.is_err());
                    assert!(sealed.next_command().is_err() && sealed.reduce_evidence().is_err());
                }
            }
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (2, 0));
        }
    }
    #[test]
    fn malformed_identity_envelope_fragment_and_late_file_damage_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        for mode in 0..6 {
            let admission = QueueAdmission::new(1, 2 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(2 << 20).unwrap();
            let mut scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                2,
            )
            .unwrap();
            let summary = run_scan(2, 0, false, &job, |packet| scratch.append(packet)).unwrap();
            let mut sealed = scratch.seal(&summary).unwrap();
            let entry = descriptor(
                &mut sealed.commands.index,
                1,
                sealed.commands.data_bytes,
                sealed.commands.count,
            )
            .unwrap();
            match mode {
                0 => {
                    sealed
                        .commands
                        .data
                        .seek(SeekFrom::Start(entry.0[7]))
                        .unwrap();
                    sealed.commands.data.write_all(b"WRONG").unwrap();
                }
                1 => {
                    sealed
                        .commands
                        .data
                        .seek(SeekFrom::Start(entry.0[5]))
                        .unwrap();
                    sealed.commands.data.write_all(b"foreign!!").unwrap();
                }
                2 => {
                    sealed
                        .commands
                        .data
                        .seek(SeekFrom::Start(entry.0[7] + 5))
                        .unwrap();
                    sealed
                        .commands
                        .data
                        .write_all(&99u64.to_le_bytes())
                        .unwrap();
                }
                3 => {
                    sealed
                        .commands
                        .data
                        .seek(SeekFrom::Start(entry.0[7] + HEADER as u64))
                        .unwrap();
                    sealed.commands.data.write_all(&[255]).unwrap();
                }
                4 => {
                    sealed
                        .commands
                        .data
                        .seek(SeekFrom::Start(entry.0[7] + entry.0[8] - 1))
                        .unwrap();
                    sealed.commands.data.write_all(&[255]).unwrap();
                }
                _ => {
                    sealed
                        .commands
                        .data
                        .set_len(sealed.commands.data_bytes - 1)
                        .unwrap();
                }
            }
            // Earlier provisional command can be read; no partial source ACK.
            assert!(sealed.next_command().unwrap().is_some());
            assert!(sealed.next_command().is_err());
            assert!(sealed.rewind_commands().is_err() && sealed.reduce_evidence().is_err());
            drop(sealed);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn rejected_row_observations_override_provisional_sibling_collision() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            3,
        )
        .unwrap();
        let summary =
            run_scan_options(3, 0, true, &job, true, |packet| scratch.append(packet)).unwrap();
        assert_eq!(
            summary.result().unwrap().histogram.outcomes
                [&AdministrativeOutcomeKind::UniqueConflict],
            1
        );
        assert_eq!(scratch.commands.count, 2);
        assert_eq!(summary.packet_count(), 3);
        assert_eq!(scratch.context.delivered, 3);
        let mut sealed = scratch.seal(&summary).unwrap();
        assert!(matches!(
            sealed.reduce_evidence(),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(sealed.next_command().is_err());
        drop(sealed);
        drop(summary);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn valid_foreign_file_images_cannot_cross_operation_integrity_binding() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for commands in [false, true] {
            let mut a = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
            )
            .unwrap();
            let mut b = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
            )
            .unwrap();
            let summary_a = run_scan(1, 0, true, &job, |packet| a.append(packet)).unwrap();
            let summary_b = run_scan(1, 0, true, &job, |packet| b.append(packet)).unwrap();
            let mut a = a.seal(&summary_a).unwrap();
            let mut b = b.seal(&summary_b).unwrap();
            if commands {
                std::mem::swap(&mut a.commands.data, &mut b.commands.data);
                assert!(a.next_command().is_err() && b.next_command().is_err());
            } else {
                std::mem::swap(&mut a.evidence.data, &mut b.evidence.data);
                assert!(a.reduce_evidence().is_err() && b.reduce_evidence().is_err());
            }
            drop(a);
            drop(b);
            drop(summary_a);
            drop(summary_b);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn shared_fragment_codec_rejects_wrong_shape_view_noop_and_every_truncation() {
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let summary = run_scan(1, 0, false, &job, |packet| {
            let delta = packet.command().unwrap();
            let before = packet.initial_authority();
            let fragment = encode_administrative_work_fragment(before, delta, &job).unwrap();
            for length in 0..fragment.as_bytes().len() {
                assert!(
                    parse_administrative_work_fragment(&fragment.as_bytes()[..length], 1).is_err()
                );
            }
            let mut trailing = fragment.as_bytes().to_vec();
            trailing.push(0);
            assert!(parse_administrative_work_fragment(&trailing, 1).is_err());
            for mode in 0..4 {
                let mut bad = delta.clone();
                match mode {
                    0 => bad.versions.push(bad.versions[0].clone()),
                    1 => bad.next_sequence += 1,
                    2 => {
                        bad.versions[0].version = 1;
                        bad.work[0].record.version = 1;
                    }
                    _ => {
                        bad.work_fields.insert(
                            bad.versions[0].key.clone(),
                            fireweed_engine::queue_fields::ResolvedFields::Edit(BTreeMap::from([
                                ("a".into(), Some(FieldValue::Integer(99))),
                            ])),
                        );
                    }
                }
                assert!(encode_administrative_work_fragment(before, &bad, &job).is_err());
            }
            Ok(())
        })
        .unwrap();
        drop(summary);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn packet_ordinals_and_eof_count_reject_skipped_and_split_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for skip in 0..4 {
            let mut scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                3,
            )
            .unwrap();
            let summary = run_scan(3, 0, false, &job, |packet| {
                if skip == 3 || packet.ordinal() == skip {
                    return Ok(());
                }
                let _deliberately_swallowed = scratch.append(packet);
                Ok(())
            })
            .unwrap();
            assert_eq!(summary.packet_count(), 3);
            assert!(scratch.seal(&summary).is_err());
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
        let mut a = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            3,
        )
        .unwrap();
        let mut b = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            3,
        )
        .unwrap();
        let summary = run_scan(3, 0, false, &job, |packet| {
            let _deliberately_swallowed = if packet.ordinal() == 1 {
                b.append(packet)
            } else {
                a.append(packet)
            };
            Ok(())
        })
        .unwrap();
        assert!(a.seal(&summary).is_err() && b.seal(&summary).is_err());
        drop(summary);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn legitimate_empty_and_noop_scans_seal_without_packets() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for count in [0, 3] {
            let scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                count,
            )
            .unwrap();
            let summary = run_scan_action(count, 0, false, &job, false, true, |_| {
                panic!("no-op must not emit packet")
            })
            .unwrap();
            assert_eq!(summary.packet_count(), 0);
            assert_eq!(summary.result().unwrap().histogram.matched, count);
            let mut sealed = scratch.seal(&summary).unwrap();
            assert!(sealed.next_command().unwrap().is_none());
            assert_eq!(
                sealed.reduce_evidence().unwrap(),
                UniqueTupleDecision {
                    administrative_collision: false,
                    final_collision: false
                }
            );
            drop(sealed);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    fn initial_tuple(a: i64) -> UniqueIdentity {
        UniqueIdentity {
            collection: None,
            index: "ab".into(),
            full_sort_key: fireweed_engine::queue_index::ordered_key(&[
                Some(&FieldValue::Integer(a)),
                Some(&FieldValue::Bytes(vec![])),
            ])
            .unwrap(),
        }
    }
    #[test]
    fn bound_union_allows_ordinary_reuse_of_admin_release_and_local_equal_ordinals() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        let mut scratch = AdministrativeScratch::new(
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            1,
        )
        .unwrap();
        let summary = run_scan(1, 0, true, &job, |packet| scratch.append(packet)).unwrap();
        let mut scratch = scratch.seal(&summary).unwrap();
        let old = initial_tuple(0);
        let initial = RecordSnapshot {
            next_sequence: 1,
            unique_probes_loaded: BTreeSet::from([old.clone()]),
            unique_owners: BTreeMap::from([(
                old.clone(),
                RecordIdentity {
                    collection: None,
                    key: "row-00000".into(),
                },
            )]),
            ..Default::default()
        };
        // Each spool's first record has ordinal zero. Cross-source ties are legal.
        let first = scratch
            .validate_before_business(&initial, directory.path(), Arc::clone(&quota), &job)
            .unwrap();
        assert!(!first.administrative_collision && !first.final_collision);
        let effects = fireweed_engine::queue_fields::UniqueFieldEffects {
            releases: BTreeMap::new(),
            claims: vec![(
                old,
                RecordIdentity {
                    collection: None,
                    key: "next".into(),
                },
            )],
        };
        assert_eq!(
            effects.validate(&initial).unwrap(),
            Err(Rejection::UniqueConflict)
        );
        let final_decision = scratch
            .reduce_with_ordinary_effects(
                &initial,
                &effects,
                directory.path(),
                Arc::clone(&quota),
                &job,
            )
            .unwrap();
        assert!(!final_decision.final_collision);
        assert!(scratch.next_command().unwrap().is_some()); // predecision validation rewound
        drop(scratch);
        drop(summary);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn predecision_validates_commands_and_cross_source_authority_before_business() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for corrupt_command in [false, true] {
            let mut scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
            )
            .unwrap();
            let summary = run_scan(1, 0, true, &job, |packet| scratch.append(packet)).unwrap();
            let mut scratch = scratch.seal(&summary).unwrap();
            let mut ordinary = RecordSnapshot {
                next_sequence: 1,
                ..Default::default()
            };
            if corrupt_command {
                let descriptor = descriptor(
                    &mut scratch.commands.index,
                    0,
                    scratch.commands.data_bytes,
                    scratch.commands.count,
                )
                .unwrap();
                scratch
                    .commands
                    .data
                    .seek(SeekFrom::Start(descriptor.0[7]))
                    .unwrap();
                scratch.commands.data.write_all(b"BAD!!").unwrap();
            } else {
                // Admin has initial ownership of a=0; missing ownership from the
                // earlier ordinary load is an integrity failure, never CAS error.
                ordinary.unique_probes_loaded.insert(initial_tuple(0));
            }
            assert!(matches!(
                scratch.validate_before_business(
                    &ordinary,
                    directory.path(),
                    Arc::clone(&quota),
                    &job
                ),
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert!(scratch.next_command().is_err());
            drop(scratch);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn final_union_requires_predecision_and_same_operation_job_and_sequence() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let other = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        for mode in 0..3 {
            let mut scratch = AdministrativeScratch::new(
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                1,
            )
            .unwrap();
            let summary = run_scan(1, 0, false, &job, |packet| scratch.append(packet)).unwrap();
            let mut scratch = scratch.seal(&summary).unwrap();
            let ordinary = RecordSnapshot {
                next_sequence: if mode == 2 { 2 } else { 1 },
                ..Default::default()
            };
            let result = if mode == 0 {
                scratch.reduce_with_ordinary_effects(
                    &ordinary,
                    &Default::default(),
                    directory.path(),
                    Arc::clone(&quota),
                    &job,
                )
            } else {
                scratch.validate_before_business(
                    &ordinary,
                    directory.path(),
                    Arc::clone(&quota),
                    if mode == 1 { &other } else { &job },
                )
            };
            assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            assert!(scratch.next_command().is_err());
            drop(scratch);
            drop(summary);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (2, 0));
        }
    }
}
