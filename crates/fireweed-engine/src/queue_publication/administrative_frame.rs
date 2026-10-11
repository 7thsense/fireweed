//! One-logical-group FWQP2 preparation. Request/result binding and admitted byte
//! construction do not establish fenced source, ownership, storage or ACK authority.
use super::*;
use crate::queue_admission::OwnedQueueBufferCharge;
use fireweed_core::queue_administration::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub struct AdministrativePublicationReceipt<'a> {
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    intent: &'a AdministrativeIntent,
    max_result_bytes: u64,
    fingerprint: [u8; 32],
    complete_bytes: u64,
    outcome: &'a AdministrativeReceiptOutcome<'a>,
}
fn same_before(a: Option<&AdministrativeBefore>, b: Option<&AdministrativeBefore>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.version == b.version
                && a.lifecycle == b.lifecycle
                && a.available_at == b.available_at
                && a.eligible_since == b.eligible_since
                && a.attempts == b.attempts
                && a.lease_expires_at == b.lease_expires_at
                && a.fields.len() == b.fields.len()
                && a.fields.iter().zip(&b.fields).all(|((an, av), (bn, bv))| {
                    an == bn
                        && match (av, bv) {
                            (FieldValue::Float(a), FieldValue::Float(b)) => {
                                a.to_bits() == b.to_bits()
                            }
                            _ => av == bv,
                        }
                })
        }
        _ => false,
    }
}
impl<'a> AdministrativePublicationReceipt<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        queue: &'a QueueAddress,
        group: &'a CommitGroup,
        intent: &'a AdministrativeIntent,
        max_result_bytes: u64,
        outcome: &'a AdministrativeReceiptOutcome<'a>,
        reservation: &QueueReservation,
    ) -> PublicationResult<Self> {
        queue.validate().map_err(PublicationError::Admission)?;
        let complete_bytes = measure_complete_administrative_receipt(&group.request_id, outcome)?;
        if measure_administrative_receipt_fallbacks(&group.request_id)? > max_result_bytes {
            return Err(PublicationError::FrameLimit);
        }
        if let AdministrativeReceiptOutcome::Committed {
            versions,
            administration,
        } = outcome
        {
            validate_commit_result_versions(group, versions)?;
            measure_administrative_result(administration)?;
            let _binding = reservation
                .buffer(
                    (administration.items.len() as u64)
                        .checked_mul(256)
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(PublicationError::Overflow)?,
                )
                .map_err(PublicationError::Admission)?;
            match &intent.operation {
                AdministrativeOperation::SelectFirst(clauses) => {
                    if intent.before_fields.is_some() {
                        return invalid("broad administrative before projection");
                    }
                    if !administration.items.is_empty()
                        || clauses.len() != administration.clauses.len()
                        || clauses
                            .iter()
                            .zip(&administration.clauses)
                            .any(|(input, result)| input.id != result.id)
                    {
                        return invalid("administrative clause binding/order");
                    }
                }
                AdministrativeOperation::Addressed(entries) => {
                    if !administration.clauses.is_empty()
                        || entries.len() != administration.items.len()
                    {
                        return invalid("administrative addressed cardinality");
                    }
                    let mut seen: BTreeMap<&str, &AdministrativeItemResult> = BTreeMap::new();
                    for (entry, item) in entries.iter().zip(&administration.items) {
                        if entry.key != item.key {
                            return invalid("administrative addressed key/order");
                        }
                        match &intent.before_fields {
                            None if item.before.is_some() => {
                                return invalid("unrequested administrative before projection");
                            }
                            Some(names) => {
                                if item.before.as_ref().is_some_and(|before| {
                                    before.fields.keys().any(|name| !names.contains(name))
                                }) {
                                    return invalid(
                                        "administrative before projection has unrequested fields",
                                    );
                                }
                                if !matches!(
                                    item.outcome,
                                    AdministrativeOutcome::NotFound
                                        | AdministrativeOutcome::Invalid
                                ) && item.before.is_none()
                                {
                                    return invalid(
                                        "administrative present target lacks requested before",
                                    );
                                }
                            }
                            _ => {}
                        }
                        if let Some(first) = seen.get(item.key.as_str()) {
                            if item.outcome != AdministrativeOutcome::Invalid
                                || !same_before(first.before.as_ref(), item.before.as_ref())
                            {
                                return invalid(
                                    "administrative duplicate result changed initial authority",
                                );
                            }
                        } else {
                            seen.insert(&item.key, item);
                        }
                    }
                }
            }
        }
        let fingerprint = group
            .administrative_fingerprint(queue, intent, max_result_bytes)
            .map_err(PublicationError::Admission)?;
        Ok(Self {
            queue,
            group,
            intent,
            max_result_bytes,
            fingerprint,
            complete_bytes,
            outcome,
        })
    }
    pub fn complete_receipt_bytes(&self) -> PublicationResult<u64> {
        Ok(self.complete_bytes)
    }
    pub fn result_budget_exceeded(&self) -> PublicationResult<bool> {
        Ok(self.complete_receipt_bytes()? > self.max_result_bytes)
    }
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}
fn write_administrative_receipt(
    s: &mut Sink<'_>,
    receipt: &AdministrativePublicationReceipt<'_>,
) -> PublicationResult<()> {
    write_receipt_header(s, 2, &receipt.group.request_id, 4, &receipt.fingerprint)?;
    match receipt.outcome {
        AdministrativeReceiptOutcome::Committed {
            versions,
            administration,
        } => {
            s.tag(4)?;
            s.n(versions.len() as u64)?;
            for version in *versions {
                identity(s, version.collection.as_deref(), &version.key)?;
                s.n(version.version)?;
            }
            s.n(measure_administrative_result(administration)?)?;
            administrative_result::write_result(s, administration)
        }
        AdministrativeReceiptOutcome::Rejected(reason) => {
            s.tag(1)?;
            if matches!(reason, Rejection::TooLarge) {
                s.tag(8)
            } else {
                rejection(s, reason)
            }
        }
        AdministrativeReceiptOutcome::TooLarge => {
            s.tag(1)?;
            s.tag(8)
        }
    }
}
pub fn measure_administrative_publication_receipt(
    receipt: &AdministrativePublicationReceipt<'_>,
) -> PublicationResult<u64> {
    let mut s = Sink::Count(0);
    write_administrative_receipt(&mut s, receipt)?;
    let Sink::Count(bytes) = s else {
        unreachable!()
    };
    Ok(bytes)
}
/// Full zero-effect fallback floor with the maximum admitted Invalid diagnostic.
pub fn measure_administrative_publication_fallbacks(
    queue: &QueueAddress,
    pin: DefinitionPin,
    request_id: &str,
) -> PublicationResult<u64> {
    let mut sink = Sink::Count(0);
    write_frame_header_as(&mut sink, queue, pin, 0, 0, 0, MAGIC_V2)?;
    sink.n(1)?;
    write_receipt_header(&mut sink, 2, request_id, 4, &[0; 32])?;
    sink.tag(1)?;
    sink.tag(5)?;
    sink.text(std::str::from_utf8(&[b'x'; 256]).expect("ASCII"))?;
    let Sink::Count(bytes) = sink else {
        unreachable!()
    };
    Ok(bytes)
}
/// Lower bound for a successful FWQP2 frame excluding commands. Full ordinary
/// identities and fixed output versions are counted; every FWAR1 result is at
/// least its 37-byte empty form. This never certifies a completed publication.
pub fn measure_administrative_success_floor(
    queue: &QueueAddress,
    pin: DefinitionPin,
    group: &CommitGroup,
) -> PublicationResult<u64> {
    let count = group
        .claimed_updates
        .len()
        .checked_add(group.state_updates.len())
        .and_then(|n| n.checked_add(group.work_updates.len()))
        .and_then(|n| n.checked_add(group.work_inserts.len()))
        .ok_or(PublicationError::Overflow)?;
    let mut sink = Sink::Count(0);
    write_frame_header_as(&mut sink, queue, pin, 0, 0, 0, MAGIC_V2)?;
    sink.n(1)?;
    write_receipt_header(&mut sink, 2, &group.request_id, 4, &[0; 32])?;
    sink.tag(4)?;
    sink.n(count as u64)?;
    for (collection, key) in group
        .claimed_updates
        .iter()
        .map(|item| (None, item.key.as_str()))
        .chain(
            group
                .state_updates
                .iter()
                .map(|item| (Some(item.collection.as_str()), item.key.as_str())),
        )
        .chain(
            group
                .work_updates
                .iter()
                .map(|item| (None, item.key.as_str())),
        )
        .chain(
            group
                .work_inserts
                .iter()
                .map(|item| (None, item.key.as_str())),
        )
    {
        identity(&mut sink, collection, key)?;
        sink.n(1)?;
    }
    sink.n(37)?;
    sink.raw(&[0; 37])?;
    let Sink::Count(bytes) = sink else {
        unreachable!()
    };
    Ok(bytes)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedCommandMeasure {
    pub count: u64,
    pub bytes: u64,
}
pub fn measure_resolved_commands(
    before: &RecordSnapshot,
    delta: &CommitDelta,
) -> PublicationResult<ResolvedCommandMeasure> {
    validate_delta(before, delta)?;
    let mut authority = InitialAuthority::new(before);
    let mut sink = Sink::Count(0);
    let mut count = 0u64;
    for version in &delta.versions {
        let prior = authority.next(version.collection.as_deref(), &version.key);
        if version.version > prior.highwater {
            write_delta_mutation(&mut sink, version, &prior, delta)?;
            count = count.checked_add(1).ok_or(PublicationError::Overflow)?;
        }
    }
    let Sink::Count(bytes) = sink else {
        unreachable!()
    };
    Ok(ResolvedCommandMeasure { count, bytes })
}
/// Counts remain per logical group. This does not execute the required overflow
/// path; callers still exhaust source/evidence/business checks before TooLarge.
pub fn measure_administrative_publication(
    queue: &QueueAddress,
    pin: DefinitionPin,
    command_bytes: u64,
    receipt: &AdministrativePublicationReceipt<'_>,
) -> PublicationResult<u64> {
    if queue != receipt.queue {
        return invalid("administrative publication queue mismatch");
    }
    let receipt_bytes = measure_administrative_publication_receipt(receipt)?;
    measure_publication_header(queue, pin)?
        .checked_add(command_bytes)
        .and_then(|n| n.checked_add(receipt_bytes))
        .ok_or(PublicationError::Overflow)
}
pub struct AdministrativePublicationFrame {
    bytes: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
impl AdministrativePublicationFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
/// Output + immutable binary receipt + only input-bounded expected ordinary/
/// addressed identities. Broad target keys never accumulate: one last key only.
type ExpectedOrdinaryCommands = BTreeMap<Option<String>, BTreeMap<String, (u64, [u8; 32])>>;
pub struct AdministrativePublicationBuilder {
    bytes: Vec<u8>,
    receipt: Vec<u8>,
    ordinary: ExpectedOrdinaryCommands,
    ordinary_work: BTreeSet<String>,
    addressed: Option<BTreeMap<String, (u64, bool)>>,
    last: Option<(Option<String>, String)>,
    expected_size: u64,
    expected_commands: u64,
    expected_command_bytes: u64,
    appended: u64,
    command_bytes: u64,
    admin_expected: [u64; 2],
    admin_appended: [u64; 2],
    initial: u64,
    final_sequence: u64,
    poisoned: bool,
    _charge: OwnedQueueBufferCharge,
}
impl AdministrativePublicationBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receipt: &AdministrativePublicationReceipt<'_>,
        pin: DefinitionPin,
        before: &RecordSnapshot,
        ordinary: &CommitDelta,
        commands: u64,
        command_bytes: u64,
        max_frame_bytes: u64,
        reservation: &Arc<QueueReservation>,
    ) -> PublicationResult<Self> {
        validate_delta(before, ordinary)?;
        let mut admin_expected = [0u64; 2];
        match receipt.outcome {
            AdministrativeReceiptOutcome::Committed {
                versions,
                administration,
            } => {
                if versions.len() != ordinary.versions.len() {
                    return invalid("ordinary result/delta cardinality");
                }
                for version in *versions {
                    let index = ordinary
                        .versions
                        .binary_search_by(|v| {
                            (v.collection.as_deref(), v.key.as_str())
                                .cmp(&(version.collection.as_deref(), version.key.as_str()))
                        })
                        .map_err(|_| {
                            PublicationError::Invalid("ordinary receipt identity absent from delta")
                        })?;
                    if ordinary.versions[index].version != version.version {
                        return invalid("ordinary receipt/delta output version mismatch");
                    }
                }
                for (index, kind) in [
                    AdministrativeOutcomeKind::Changed,
                    AdministrativeOutcomeKind::Deleted,
                ]
                .iter()
                .enumerate()
                {
                    admin_expected[index] = administration
                        .histogram
                        .outcomes
                        .get(kind)
                        .copied()
                        .unwrap_or(0);
                }
            }
            _ if commands != 0
                || command_bytes != 0
                || !ordinary.versions.is_empty()
                || ordinary.next_sequence != before.next_sequence =>
            {
                return invalid("rejected administrative publication has effects");
            }
            _ => {}
        }
        if commands
            < admin_expected[0]
                .checked_add(admin_expected[1])
                .ok_or(PublicationError::Overflow)?
        {
            return invalid("administrative receipt mutations exceed frame commands");
        }
        if receipt.result_budget_exceeded()? {
            return Err(PublicationError::FrameLimit);
        }
        let expected_size =
            measure_administrative_publication(receipt.queue, pin, command_bytes, receipt)?;
        if expected_size > max_frame_bytes {
            return Err(PublicationError::FrameLimit);
        }
        let mut metadata = 65536u64;
        for version in &ordinary.versions {
            metadata = metadata
                .checked_add(
                    (version.key.len() as u64)
                        .checked_add(
                            version
                                .collection
                                .as_ref()
                                .map_or(0, |name| name.len() as u64),
                        )
                        .and_then(|n| n.checked_mul(8))
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(PublicationError::Overflow)?,
                )
                .ok_or(PublicationError::Overflow)?;
        }
        if let AdministrativeReceiptOutcome::Committed { administration, .. } = receipt.outcome {
            for item in &administration.items {
                metadata = metadata
                    .checked_add(
                        (item.key.len() as u64)
                            .checked_mul(8)
                            .and_then(|n| n.checked_add(512))
                            .ok_or(PublicationError::Overflow)?,
                    )
                    .ok_or(PublicationError::Overflow)?;
            }
        }
        let charge = reservation
            .owned_buffer(
                expected_size
                    .checked_mul(3)
                    .and_then(|n| n.checked_add(metadata))
                    .ok_or(PublicationError::Overflow)?,
            )
            .map_err(PublicationError::Admission)?;
        let capacity = usize::try_from(expected_size).map_err(|_| PublicationError::Overflow)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| PublicationError::Overflow)?;
        write_frame_header_as(
            &mut Sink::Buffer(&mut bytes),
            receipt.queue,
            pin,
            before.next_sequence,
            ordinary.next_sequence,
            commands,
            MAGIC_V2,
        )?;
        let size = measure_administrative_publication_receipt(receipt)?;
        let mut receipt_bytes = Vec::new();
        receipt_bytes
            .try_reserve_exact(usize::try_from(size).map_err(|_| PublicationError::Overflow)?)
            .map_err(|_| PublicationError::Overflow)?;
        write_administrative_receipt(&mut Sink::Buffer(&mut receipt_bytes), receipt)?;
        let mut expected_ordinary: ExpectedOrdinaryCommands = BTreeMap::new();
        let mut ordinary_work = BTreeSet::new();
        let mut initial = InitialAuthority::new(before);
        for version in &ordinary.versions {
            if version.collection.is_none() {
                ordinary_work.insert(version.key.clone());
            }
            let prior = initial.next(version.collection.as_deref(), &version.key);
            if version.version > prior.highwater {
                let mut sink = Sink::Digest(Sha256::new());
                write_delta_mutation(&mut sink, version, &prior, ordinary)?;
                let Sink::Digest(hash) = sink else {
                    unreachable!()
                };
                let digest: [u8; 32] = hash.finalize().into();
                if expected_ordinary
                    .entry(version.collection.clone())
                    .or_default()
                    .insert(version.key.clone(), (version.version, digest))
                    .is_some()
                {
                    return invalid("duplicate ordinary publication identity");
                }
            }
        }
        let addressed = if matches!(
            receipt.intent.operation,
            AdministrativeOperation::Addressed(_)
        ) {
            let mut expected = BTreeMap::new();
            if let AdministrativeReceiptOutcome::Committed { administration, .. } = receipt.outcome
            {
                for item in &administration.items {
                    let effect = match item.outcome {
                        AdministrativeOutcome::Changed { version } => Some((version, false)),
                        AdministrativeOutcome::Deleted { version } => Some((version, true)),
                        _ => None,
                    };
                    if let Some(effect) = effect
                        && expected.insert(item.key.clone(), effect).is_some()
                    {
                        return invalid("duplicate accepted addressed mutation");
                    }
                }
            }
            Some(expected)
        } else {
            None
        };
        Ok(Self {
            bytes,
            receipt: receipt_bytes,
            ordinary: expected_ordinary,
            ordinary_work,
            addressed,
            last: None,
            expected_size,
            expected_commands: commands,
            expected_command_bytes: command_bytes,
            appended: 0,
            command_bytes: 0,
            admin_expected,
            admin_appended: [0; 2],
            initial: before.next_sequence,
            final_sequence: ordinary.next_sequence,
            poisoned: false,
            _charge: charge,
        })
    }
    pub fn append_administrative(&mut self, raw: &[u8]) -> PublicationResult<()> {
        self.append(raw, true)
    }
    pub fn append_ordinary(&mut self, raw: &[u8]) -> PublicationResult<()> {
        self.append(raw, false)
    }
    fn append(&mut self, raw: &[u8], administrative: bool) -> PublicationResult<()> {
        if self.poisoned {
            return invalid("poisoned administrative publication builder");
        }
        self.poisoned = true;
        // Reject BEFORE any copy or owned-key allocation. The declared frame
        // size includes the untouched receipt tail, so Vec cannot reallocate.
        let next_bytes = self
            .command_bytes
            .checked_add(raw.len() as u64)
            .ok_or(PublicationError::Overflow)?;
        let end = (self.bytes.len() as u64)
            .checked_add(raw.len() as u64)
            .and_then(|n| n.checked_add(8))
            .and_then(|n| n.checked_add(self.receipt.len() as u64))
            .ok_or(PublicationError::Overflow)?;
        if next_bytes > self.expected_command_bytes
            || end > self.expected_size
            || self.appended >= self.expected_commands
        {
            return invalid("streamed command exceeds declared frame capacity/count");
        }
        let command = parse_resolved_command_fragment(raw, self.initial, self.final_sequence)?;
        let order = (command.identity.collection, command.identity.key);
        if self
            .last
            .as_ref()
            .is_some_and(|(collection, key)| (collection.as_deref(), key.as_str()) >= order)
        {
            return invalid("streamed command identity order/duplicate");
        }
        if administrative {
            parse_administrative_work_fragment(raw, self.initial)?;
            if self.ordinary_work.contains(command.identity.key) {
                return invalid("administrative command overlaps ordinary work identity");
            }
            if let Some(expected) = &mut self.addressed {
                let effect =
                    expected
                        .remove(command.identity.key)
                        .ok_or(PublicationError::Invalid(
                            "unbound addressed command identity",
                        ))?;
                if effect
                    != (
                        command.output_highwater,
                        matches!(command.effect, EffectRef::Delete),
                    )
                {
                    return invalid("addressed command/result effect mismatch");
                }
            }
            let kind = usize::from(matches!(command.effect, EffectRef::Delete));
            self.admin_appended[kind] = self.admin_appended[kind]
                .checked_add(1)
                .ok_or(PublicationError::Overflow)?;
            if self.admin_appended[kind] > self.admin_expected[kind] {
                return invalid("extra administrative mutation kind");
            }
        } else {
            let (_, keys) = self
                .ordinary
                .iter_mut()
                .find(|(collection, _)| collection.as_deref() == command.identity.collection)
                .ok_or(PublicationError::Invalid("unbound ordinary collection"))?;
            let digest: [u8; 32] = Sha256::digest(raw).into();
            if keys.remove(command.identity.key) != Some((command.output_highwater, digest)) {
                return invalid("ordinary command differs from resolved delta");
            }
        }
        self.last = Some((
            command.identity.collection.map(str::to_owned),
            command.identity.key.into(),
        ));
        self.bytes.extend_from_slice(raw);
        self.command_bytes = next_bytes;
        self.appended += 1;
        self.poisoned = false;
        Ok(())
    }
    /// Consume successful source EOF. No partially encoded frame is exposed.
    pub fn finish(mut self) -> PublicationResult<AdministrativePublicationFrame> {
        if self.poisoned
            || self.appended != self.expected_commands
            || self.command_bytes != self.expected_command_bytes
            || self.admin_appended != self.admin_expected
            || self.ordinary.values().any(|keys| !keys.is_empty())
            || self.addressed.as_ref().is_some_and(|keys| !keys.is_empty())
        {
            return invalid("incomplete administrative publication command EOF");
        }
        let end = (self.bytes.len() as u64)
            .checked_add(8)
            .and_then(|n| n.checked_add(self.receipt.len() as u64))
            .ok_or(PublicationError::Overflow)?;
        if end != self.expected_size {
            return invalid("administrative publication size changed");
        }
        self.bytes.extend_from_slice(&1u64.to_le_bytes());
        self.bytes.extend_from_slice(&self.receipt);
        view_resolved_publication(&self.bytes)?;
        Ok(AdministrativePublicationFrame {
            bytes: self.bytes,
            _charge: self._charge,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{queue, schema};
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_planner::{combine_deltas, plan_commit_group},
        queue_schema_codec::encode_definition,
    };
    struct Fixture {
        queue: QueueAddress,
        raw_definition: Vec<u8>,
        before: RecordSnapshot,
        group: CommitGroup,
        intent: AdministrativeIntent,
        ordinary: CommitDelta,
        versions: Vec<RecordVersion>,
        admin_before: RecordSnapshot,
        admin: CommitDelta,
        result: AdministrativeResult,
    }
    fn group(id: &str) -> CommitGroup {
        CommitGroup {
            administration: None,
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        }
    }
    fn fixture() -> Fixture {
        let schema = schema();
        let queue = queue();
        let raw_definition = encode_definition(&schema).unwrap();
        let mut seed = group("seed");
        seed.work_inserts = ["a", "z"]
            .into_iter()
            .map(|key| NewWork {
                key: key.into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            })
            .collect();
        let mut before = RecordSnapshot::default();
        plan_commit_group(&seed, &mut before, 0, &schema).unwrap();
        let mut ordinary_group = group("g");
        ordinary_group.state_updates.push(StateUpdate {
            collection: "instances".into(),
            key: "wf".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Default::default(),
                value: vec![9].into(),
            },
        });
        ordinary_group.work_updates.push(UnclaimedUpdate {
            key: "z".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(33),
                ..Default::default()
            }),
        });
        ordinary_group.work_inserts.push(NewWork {
            key: "next".into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: vec![7].into(),
        });
        let planned = plan_commit_group(&ordinary_group, &mut before.clone(), 0, &schema).unwrap();
        let GroupOutcome::Committed { versions } = &planned.receipt.outcome else {
            panic!("ordinary")
        };
        let versions = versions.clone();
        let ordinary =
            combine_deltas(std::slice::from_ref(&planned), before.next_sequence).unwrap();
        let admin_before = RecordSnapshot {
            next_sequence: before.next_sequence,
            work: BTreeMap::from([("a".into(), before.work["a"].clone())]),
            versions: BTreeMap::from([(
                RecordIdentity {
                    collection: None,
                    key: "a".into(),
                },
                1,
            )]),
            ..Default::default()
        };
        let mut request = group("admin");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(17),
                ..Default::default()
            }),
        });
        let admin = plan_commit_group(&request, &mut admin_before.clone(), 0, &schema).unwrap();
        let action = AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(17),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: Default::default(),
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action,
            }]),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram
            .record(&AdministrativeOutcome::Changed { version: 2 })
            .unwrap();
        let result = AdministrativeResult {
            histogram: histogram.clone(),
            items: vec![],
            clauses: vec![AdministrativeClauseResult {
                id: "all".into(),
                histogram,
            }],
        };
        Fixture {
            queue,
            raw_definition,
            before,
            group: ordinary_group,
            intent,
            ordinary,
            versions,
            admin_before,
            admin,
            result,
        }
    }
    #[test]
    fn publication_floors_and_command_counts_match_canonical_writers() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let pin = DefinitionPin::from_persisted_bytes(&f.raw_definition);
        let mut group = group("empty");
        group.request_id = "α\0".repeat(10000);
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![]),
        };
        let result = AdministrativeResult {
            histogram: Default::default(),
            items: vec![],
            clauses: vec![],
        };
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &[],
            administration: &result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue,
            &group,
            &intent,
            1 << 20,
            &outcome,
            &job,
        )
        .unwrap();
        assert_eq!(
            measure_administrative_success_floor(&f.queue, pin, &group).unwrap(),
            measure_administrative_publication(&f.queue, pin, 0, &receipt).unwrap()
        );
        let invalid = Rejection::Invalid("x".repeat(256));
        let rejected = AdministrativeReceiptOutcome::Rejected(&invalid);
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue,
            &group,
            &intent,
            1 << 20,
            &rejected,
            &job,
        )
        .unwrap();
        assert_eq!(
            measure_administrative_publication_fallbacks(&f.queue, pin, &group.request_id).unwrap(),
            measure_administrative_publication(&f.queue, pin, 0, &receipt).unwrap()
        );
        let ordinary = encode_publication(
            &f.queue,
            &f.raw_definition,
            &f.before,
            &f.ordinary,
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_publication(ordinary.as_bytes()).unwrap();
        let measured = measure_resolved_commands(&f.before, &f.ordinary).unwrap();
        assert_eq!(measured.count, 3);
        assert_eq!(
            measured.bytes,
            view.command_fragments()
                .map(|command| command.unwrap().as_bytes().len() as u64)
                .sum::<u64>()
        );
    }
    #[test]
    fn broad_effect_kinds_and_ordinary_noops_cannot_be_misbound() {
        for deleted in [false, true] {
            let mut f = fixture();
            if deleted {
                let mut request = group("delete");
                request.work_updates.push(UnclaimedUpdate {
                    key: "a".into(),
                    expect: StateExpect::Version(1),
                    change: UnclaimedChange::Delete,
                });
                f.admin =
                    plan_commit_group(&request, &mut f.admin_before.clone(), 0, &schema()).unwrap();
            } else {
                let mut histogram = AdministrativeHistogram::default();
                histogram
                    .record(&AdministrativeOutcome::Deleted { version: 2 })
                    .unwrap();
                f.result.histogram = histogram.clone();
                f.result.clauses[0].histogram = histogram;
            }
            let admission = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let outcome = AdministrativeReceiptOutcome::Committed {
                versions: &f.versions,
                administration: &f.result,
            };
            let receipt = AdministrativePublicationReceipt::new(
                &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
            )
            .unwrap();
            let admin =
                encode_administrative_work_fragment(&f.admin_before, &f.admin, &job).unwrap();
            let mut builder = AdministrativePublicationBuilder::new(
                &receipt,
                DefinitionPin::from_persisted_bytes(&f.raw_definition),
                &f.before,
                &f.ordinary,
                4,
                4096,
                1 << 20,
                &job,
            )
            .unwrap();
            assert!(builder.append_administrative(admin.as_bytes()).is_err());
            assert!(builder.finish().is_err());
        }
        let mut f = fixture();
        f.group = group("noop");
        f.group.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch::default()),
        });
        let delta = plan_commit_group(&f.group, &mut f.before.clone(), 0, &schema()).unwrap();
        let GroupOutcome::Committed { versions } = &delta.receipt.outcome else {
            panic!("noop")
        };
        f.versions = versions.clone();
        f.ordinary = combine_deltas(&[delta], f.before.next_sequence).unwrap();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &f.result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let admin = encode_administrative_work_fragment(&f.admin_before, &f.admin, &job).unwrap();
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            DefinitionPin::from_persisted_bytes(&f.raw_definition),
            &f.before,
            &f.ordinary,
            1,
            admin.as_bytes().len() as u64,
            1 << 20,
            &job,
        )
        .unwrap();
        assert!(builder.append_administrative(admin.as_bytes()).is_err());
        assert!(builder.finish().is_err());
    }
    #[test]
    fn complete_frame_roundtrip_binds_commands_versions_result_and_fingerprint() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &f.result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let admin = encode_administrative_work_fragment(&f.admin_before, &f.admin, &job).unwrap();
        let ordinary = encode_publication(
            &f.queue,
            &f.raw_definition,
            &f.before,
            &f.ordinary,
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_publication(ordinary.as_bytes()).unwrap();
        let commands = view
            .command_fragments()
            .collect::<PublicationResult<Vec<_>>>()
            .unwrap();
        let bytes = admin.as_bytes().len() as u64
            + commands
                .iter()
                .map(|command| command.as_bytes().len() as u64)
                .sum::<u64>();
        let pin = DefinitionPin::from_persisted_bytes(&f.raw_definition);
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            pin,
            &f.before,
            &f.ordinary,
            4,
            bytes,
            1 << 20,
            &job,
        )
        .unwrap();
        builder.append_administrative(admin.as_bytes()).unwrap();
        for command in commands {
            builder.append_ordinary(command.as_bytes()).unwrap();
        }
        let frame = builder.finish().unwrap();
        assert_eq!(frame.as_bytes().len(), 618);
        assert_eq!(
            Sha256::digest(frame.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "e89687ff3a745a648dfd204a47340dda6530c58c5ee5de3e73c69002c52b95a2"
        );
        assert!(view_publication(frame.as_bytes()).is_err());
        assert!(scan_publication(frame.as_bytes()).is_err());
        let view = view_resolved_publication(frame.as_bytes()).unwrap();
        assert_eq!(view.format(), PublicationFormat::V2);
        assert_eq!(view.summary().initial_sequence, 2);
        assert_eq!(view.summary().final_sequence, 3);
        assert_eq!(
            view.commands()
                .map(|command| command.unwrap().identity.key)
                .collect::<Vec<_>>(),
            vec!["a", "next", "z", "wf"]
        );
        for _ in 0..2 {
            let parsed = view.receipts().next().unwrap().unwrap();
            assert_eq!(parsed.fingerprint_format, 4);
            assert_eq!(*parsed.fingerprint, receipt.fingerprint());
            let ReceiptOutcomeRef::Commit(CommitOutcomeRef::AdministrativeCommitted {
                versions,
                administration,
            }) = parsed.outcome
            else {
                panic!("administration")
            };
            assert_eq!(
                versions
                    .iter()
                    .map(|version| {
                        let version = version.unwrap();
                        (
                            version.identity.collection,
                            version.identity.key,
                            version.version,
                        )
                    })
                    .collect::<Vec<_>>(),
                vec![
                    (Some("instances"), "wf", 1),
                    (None, "z", 2),
                    (None, "next", 1)
                ]
            );
            assert_eq!(administration.histogram.matched, 1);
            assert_eq!(
                administration.clauses.iter().next().unwrap().unwrap().id,
                "all"
            );
        }
        for n in 0..frame.as_bytes().len() {
            assert!(
                view_resolved_publication(&frame.as_bytes()[..n]).is_err(),
                "truncation {n}"
            );
        }
        let mut v1 = frame.as_bytes().to_vec();
        v1[4] = b'1';
        assert!(view_resolved_publication(&v1).is_err());
        let parsed = view.receipts().next().unwrap().unwrap();
        let id_offset = parsed.request_id.as_ptr() as usize - frame.as_bytes().as_ptr() as usize;
        for (offset, value) in [
            (id_offset + parsed.request_id.len(), 3),
            (id_offset - 9, 0),
            (id_offset + parsed.request_id.len() + 33, 0),
        ] {
            let mut corrupt = frame.as_bytes().to_vec();
            corrupt[offset] = value;
            assert!(view_resolved_publication(&corrupt).is_err());
        }
    }
    #[test]
    fn insufficient_declared_bytes_fail_before_copy_and_poison_partial_builder() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &f.result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let admin = encode_administrative_work_fragment(&f.admin_before, &f.admin, &job).unwrap();
        let ordinary = encode_publication(
            &f.queue,
            &f.raw_definition,
            &f.before,
            &f.ordinary,
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_publication(ordinary.as_bytes()).unwrap();
        let commands = view
            .command_fragments()
            .collect::<PublicationResult<Vec<_>>>()
            .unwrap();
        let bytes = admin.as_bytes().len() as u64
            + commands
                .iter()
                .map(|command| command.as_bytes().len() as u64)
                .sum::<u64>();
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            DefinitionPin::from_persisted_bytes(&f.raw_definition),
            &f.before,
            &f.ordinary,
            4,
            bytes - 1,
            1 << 20,
            &job,
        )
        .unwrap();
        let capacity = builder.bytes.capacity();
        builder.append_administrative(admin.as_bytes()).unwrap();
        for command in &commands[..2] {
            builder.append_ordinary(command.as_bytes()).unwrap();
        }
        let length = builder.bytes.len();
        let usage = admission.usage().unwrap();
        assert!(builder.append_ordinary(commands[2].as_bytes()).is_err());
        assert_eq!(builder.bytes.len(), length);
        assert_eq!(builder.bytes.capacity(), capacity);
        assert_eq!(admission.usage().unwrap(), usage);
        assert!(builder.finish().is_err());
    }
    #[test]
    fn ordinary_content_mismatch_duplicate_order_and_incomplete_eof_fail_closed() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &f.result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let admin = encode_administrative_work_fragment(&f.admin_before, &f.admin, &job).unwrap();
        let ordinary = encode_publication(
            &f.queue,
            &f.raw_definition,
            &f.before,
            &f.ordinary,
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_publication(ordinary.as_bytes()).unwrap();
        let commands = view
            .command_fragments()
            .collect::<PublicationResult<Vec<_>>>()
            .unwrap();
        let bytes = admin.as_bytes().len() as u64
            + commands
                .iter()
                .map(|command| command.as_bytes().len() as u64)
                .sum::<u64>();
        let pin = DefinitionPin::from_persisted_bytes(&f.raw_definition);
        let mut wrong = commands[0].as_bytes().to_vec();
        *wrong.last_mut().unwrap() = 8;
        assert!(parse_resolved_command_fragment(&wrong, 2, 3).is_ok());
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            pin,
            &f.before,
            &f.ordinary,
            4,
            bytes,
            1 << 20,
            &job,
        )
        .unwrap();
        builder.append_administrative(admin.as_bytes()).unwrap();
        assert!(builder.append_ordinary(&wrong).is_err());
        assert!(builder.finish().is_err());
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            pin,
            &f.before,
            &f.ordinary,
            4,
            bytes,
            1 << 20,
            &job,
        )
        .unwrap();
        builder.append_administrative(admin.as_bytes()).unwrap();
        assert!(builder.append_administrative(admin.as_bytes()).is_err());
        assert!(builder.finish().is_err());
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            pin,
            &f.before,
            &f.ordinary,
            4,
            bytes,
            1 << 20,
            &job,
        )
        .unwrap();
        builder.append_administrative(admin.as_bytes()).unwrap();
        assert!(builder.finish().is_err());
    }
    #[test]
    fn durable_rejections_have_no_commands_or_sequence_advance_and_v1_fp3_cannot_read_too_large() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let pin = DefinitionPin::from_persisted_bytes(&f.raw_definition);
        let outcome = AdministrativeReceiptOutcome::TooLarge;
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let empty = combine_deltas(&[], 2).unwrap();
        let builder = AdministrativePublicationBuilder::new(
            &receipt,
            pin,
            &f.before,
            &empty,
            0,
            0,
            1 << 20,
            &job,
        )
        .unwrap();
        let frame = builder.finish().unwrap();
        let view = view_resolved_publication(frame.as_bytes()).unwrap();
        assert_eq!(view.summary().commands, 0);
        assert_eq!(
            view.summary().initial_sequence,
            view.summary().final_sequence
        );
        assert!(matches!(
            view.receipts().next().unwrap().unwrap().outcome,
            ReceiptOutcomeRef::Commit(CommitOutcomeRef::Rejected(RejectionRef::TooLarge))
        ));
        let mut malformed = frame.as_bytes().to_vec();
        let initial_offset = 5 + 8 + f.queue.tenant.len() + 8 + f.queue.queue.len() + 32;
        malformed[initial_offset + 8..initial_offset + 16].copy_from_slice(&3u64.to_le_bytes());
        assert!(view_resolved_publication(&malformed).is_err());
        let mut advanced = empty.clone();
        advanced.next_sequence = 3;
        assert!(
            AdministrativePublicationBuilder::new(
                &receipt,
                pin,
                &f.before,
                &advanced,
                0,
                0,
                1 << 20,
                &job
            )
            .is_err()
        );
        assert!(
            AdministrativePublicationBuilder::new(
                &receipt,
                pin,
                &f.before,
                &empty,
                1,
                1,
                1 << 20,
                &job
            )
            .is_err()
        );
        let parsed = view.receipts().next().unwrap().unwrap();
        let offset = parsed.request_id.as_ptr() as usize - frame.as_bytes().as_ptr() as usize
            + parsed.request_id.len();
        let mut fp3 = frame.as_bytes().to_vec();
        fp3[offset] = 3;
        assert!(view_resolved_publication(&fp3).is_err());
        let mut v1 = frame.as_bytes().to_vec();
        v1[4] = b'1';
        assert!(view_resolved_publication(&v1).is_err());
    }
    #[test]
    fn receipt_binding_rejects_wrong_producer_positions_clauses_and_duplicate_before_bits() {
        let mut f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let mut wrong = f.versions.clone();
        wrong.swap(0, 1);
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &wrong,
            administration: &f.result,
        };
        assert!(
            AdministrativePublicationReceipt::new(
                &f.queue, &f.group, &f.intent, 4096, &outcome, &job
            )
            .is_err()
        );
        let mut result = f.result.clone();
        result.clauses[0].id = "wrong".into();
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &result,
        };
        assert!(
            AdministrativePublicationReceipt::new(
                &f.queue, &f.group, &f.intent, 4096, &outcome, &job
            )
            .is_err()
        );
        let AdministrativeOperation::SelectFirst(clauses) = &f.intent.operation else {
            unreachable!()
        };
        let action = clauses[0].action.clone();
        f.intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(std::collections::BTreeSet::from(["float".into()])),
            operation: AdministrativeOperation::Addressed(
                ["a", "a"]
                    .into_iter()
                    .map(|key| AdministrativeEntry {
                        key: key.into(),
                        expected_version: None,
                        precondition: WorkSelector::all(vec![]),
                        action: action.clone(),
                    })
                    .collect(),
            ),
        };
        let before = AdministrativeBefore {
            version: 1,
            lifecycle: Lifecycle::Pending,
            available_at: 0,
            eligible_since: 0,
            attempts: 0,
            lease_expires_at: None,
            fields: BTreeMap::from([("float".into(), FieldValue::Float(-0.0))]),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram
            .record(&AdministrativeOutcome::Changed { version: 2 })
            .unwrap();
        histogram.record(&AdministrativeOutcome::Invalid).unwrap();
        let mut result = AdministrativeResult {
            histogram,
            clauses: vec![],
            items: vec![
                AdministrativeItemResult {
                    key: "a".into(),
                    outcome: AdministrativeOutcome::Changed { version: 2 },
                    before: Some(before.clone()),
                },
                AdministrativeItemResult {
                    key: "a".into(),
                    outcome: AdministrativeOutcome::Invalid,
                    before: Some(before),
                },
            ],
        };
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &result,
        };
        assert!(
            AdministrativePublicationReceipt::new(
                &f.queue, &f.group, &f.intent, 4096, &outcome, &job
            )
            .is_ok()
        );
        result.items[1]
            .before
            .as_mut()
            .unwrap()
            .fields
            .insert("float".into(), FieldValue::Float(0.0));
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &f.versions,
            administration: &result,
        };
        assert!(
            AdministrativePublicationReceipt::new(
                &f.queue, &f.group, &f.intent, 4096, &outcome, &job
            )
            .is_err()
        );
        assert!(matches!(
            AdministrativePublicationReceipt::new(&f.queue, &f.group, &f.intent, 1, &outcome, &job),
            Err(PublicationError::FrameLimit) | Err(PublicationError::Invalid(_))
        ));
    }
    #[test]
    fn exact_frame_limit_boundaries_and_owned_job_lifetime_are_preserved() {
        let f = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let outcome = AdministrativeReceiptOutcome::TooLarge;
        let receipt = AdministrativePublicationReceipt::new(
            &f.queue, &f.group, &f.intent, 4096, &outcome, &job,
        )
        .unwrap();
        let empty = combine_deltas(&[], 2).unwrap();
        let pin = DefinitionPin::from_persisted_bytes(&f.raw_definition);
        let size = measure_administrative_publication(&f.queue, pin, 0, &receipt).unwrap();
        assert!(matches!(
            AdministrativePublicationBuilder::new(
                &receipt,
                pin,
                &f.before,
                &empty,
                0,
                0,
                size - 1,
                &job
            ),
            Err(PublicationError::FrameLimit)
        ));
        let builder = AdministrativePublicationBuilder::new(
            &receipt, pin, &f.before, &empty, 0, 0, size, &job,
        )
        .unwrap();
        admission.close().unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap().0, 1);
        let frame = builder.finish().unwrap();
        assert_eq!(frame.as_bytes().len() as u64, size);
        assert_eq!(admission.usage().unwrap().0, 1);
        drop(frame);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
