//! FWQC1 thin claim wrapper over frozen FWQP1 commands and FWCR1 receipts.
//! Structural views allocate nothing. Cross-binding is separately admitted and
//! still does not prove physical predecessor/selection or immutable identity.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_claim::claim_expiry,
    queue_claim_receipt::{ClaimReceiptView, OwnedClaimReceipt, view_claim_receipt},
    queue_planner::lease_hash,
    queue_publication::{
        BodyCommandRef, EffectRef, FieldsCommandRef, PublicationFormat, PublicationView,
        view_publication,
    },
    queue_schema_codec::AdmittedQueueDefinition,
};
use fireweed_core::queue_contract::*;
use std::{collections::BTreeMap, sync::Arc};
const MAGIC: &[u8; 5] = b"FWQC1";
fn invalid(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("claim publication: {message}"))
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> QueueResult<&'a [u8]> {
        let bytes = self
            .0
            .get(..len)
            .ok_or_else(|| invalid("truncated wrapper"))?;
        self.0 = &self.0[len..];
        Ok(bytes)
    }
    fn number(&mut self) -> QueueResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| invalid("integer"))?,
        ))
    }
    fn signed(&mut self) -> QueueResult<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| invalid("integer"))?,
        ))
    }
    fn blob(&mut self) -> QueueResult<&'a [u8]> {
        let len = usize::try_from(self.number()?).map_err(|_| invalid("length overflow"))?;
        self.take(len)
    }
}
pub struct ClaimPublicationView<'a> {
    claimed_at: i64,
    eligible_at: i64,
    lease_nanos: u64,
    expires_at: i64,
    request_id: &'a str,
    fingerprint: &'a [u8; 32],
    commands: PublicationView<'a>,
    receipt: ClaimReceiptView<'a>,
    receipt_bytes: &'a [u8],
}
impl<'a> ClaimPublicationView<'a> {
    pub fn claimed_at(&self) -> i64 {
        self.claimed_at
    }
    pub fn eligible_at(&self) -> i64 {
        self.eligible_at
    }
    pub fn lease_nanos(&self) -> u64 {
        self.lease_nanos
    }
    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }
    pub fn request_id(&self) -> &'a str {
        self.request_id
    }
    /// FWQC1 freezes the claim-domain canonical v3 fingerprint.
    pub fn fingerprint_format(&self) -> u8 {
        3
    }
    pub fn fingerprint(&self) -> &'a [u8; 32] {
        self.fingerprint
    }

    pub fn commands(&self) -> &PublicationView<'a> {
        &self.commands
    }
    pub fn receipt(&self) -> ClaimReceiptView<'a> {
        self.receipt
    }
    pub fn receipt_bytes(&self) -> &'a [u8] {
        self.receipt_bytes
    }
    /// Full-key, non-quadratic binding. Native storage must additionally check
    /// preserved metadata and the prior attempts against physical authority.
    pub fn validate_binding(&self, job: &Arc<QueueReservation>) -> QueueResult<()> {
        let _headers = job.owned_buffer(
            self.receipt
                .len()
                .checked_mul(256)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let mut entries = BTreeMap::new();
        for entry in self.receipt.entries() {
            let entry = entry?;
            if entries.insert(entry.key, entry).is_some() {
                return Err(invalid("duplicate selected key"));
            }
        }
        for command in self.commands.commands() {
            let command = command.map_err(|_| invalid("invalid embedded command"))?;
            if command.prior_highwater == 0
                || command.identity.collection.is_some()
                || command.expected_version != Some(command.prior_highwater)
                || command.prior_highwater.checked_add(1) != Some(command.output_highwater)
            {
                return Err(invalid("claim is not one live work increment"));
            }
            let entry = entries
                .remove(command.identity.key)
                .ok_or_else(|| invalid("command has no selected lease"))?;
            let EffectRef::WorkPut {
                metadata,
                fields: FieldsCommandRef::Preserve,
                body: BodyCommandRef::Preserve,
            } = command.effect
            else {
                return Err(invalid("claim must preserve work payload"));
            };
            if metadata.lifecycle != Lifecycle::Leased
                || metadata.lease_expires_at != Some(self.expires_at)
                || metadata.lease_hash != Some(&lease_hash(entry.token))
                || entry.version != command.output_highwater
                || metadata.attempts == 0
                || metadata.insertion_sequence == 0
                || metadata.insertion_sequence > self.commands.summary().initial_sequence
                || metadata.available_at > self.eligible_at
            {
                return Err(invalid("lease entry and resolved metadata differ"));
            }
        }
        if !entries.is_empty() {
            return Err(invalid("selected lease has no command"));
        }
        Ok(())
    }
}
/// Structure/EOF only; cross-binding requires validate_binding under admission.
pub fn view_claim_publication(raw: &[u8]) -> QueueResult<ClaimPublicationView<'_>> {
    let mut reader = Reader(raw);
    if reader.take(5)? != MAGIC {
        return Err(invalid("unsupported format"));
    }
    let claimed_at = reader.signed()?;
    let eligible_at = reader.signed()?;
    let lease_nanos = reader.number()?;
    lease_duration(lease_nanos).map_err(|_| invalid("invalid stored lease duration"))?;
    let expires_at = claim_expiry(claimed_at, lease_nanos)
        .map_err(|_| invalid("invalid stored lease expiry"))?;
    let request_id = std::str::from_utf8(reader.blob()?)
        .map_err(|_| invalid("invalid UTF-8 request identity"))?;
    if request_id.trim().is_empty() {
        return Err(invalid("empty request identity"));
    }
    let fingerprint = reader
        .take(32)?
        .try_into()
        .map_err(|_| invalid("fingerprint"))?;
    let commands =
        view_publication(reader.blob()?).map_err(|_| invalid("invalid frozen command frame"))?;
    let receipt_bytes = reader.blob()?;
    let receipt = view_claim_receipt(receipt_bytes)?;
    let summary = commands.summary();
    if !reader.0.is_empty()
        || commands.format() != PublicationFormat::V1
        || summary.receipts != 0
        || summary.initial_sequence != summary.final_sequence
        || summary.commands != receipt.len()
    {
        return Err(invalid("claim frame shape or EOF mismatch"));
    }
    Ok(ClaimPublicationView {
        claimed_at,
        eligible_at,
        lease_nanos,
        expires_at,
        request_id,
        fingerprint,
        commands,
        receipt,
        receipt_bytes,
    })
}
pub struct ClaimPublicationInput<'a> {
    pub request: &'a ClaimRequest,
    pub definition: &'a AdmittedQueueDefinition,
    pub commands: &'a [u8],
    pub receipt: &'a OwnedClaimReceipt,
    pub claimed_at: i64,
    pub limits: &'a QueueLimits,
}
pub struct OwnedClaimPublication {
    bytes: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
impl OwnedClaimPublication {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) -> QueueResult<()> {
    out.extend_from_slice(
        &u64::try_from(bytes.len())
            .map_err(|_| QueueError::TooLarge)?
            .to_le_bytes(),
    );
    out.extend_from_slice(bytes);
    Ok(())
}
pub fn encode_claim_publication(
    input: ClaimPublicationInput<'_>,
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedClaimPublication> {
    input
        .request
        .validate(input.definition.schema(), input.limits)?;
    claim_expiry(input.claimed_at, input.request.lease_nanos)?;
    let commands =
        view_publication(input.commands).map_err(|_| invalid("invalid command frame"))?;
    if commands.summary().queue
        != (
            input.request.queue.tenant.as_str(),
            input.request.queue.queue.as_str(),
        )
        || commands.summary().definition_pin != input.definition.pin().as_bytes()
    {
        return Err(invalid("request queue or raw definition pin mismatch"));
    }
    let receipt = view_claim_receipt(input.receipt.bytes())?;
    if receipt.len() > u64::from(input.request.max_records) {
        return Err(invalid("selection exceeds request count"));
    }
    let size = [
        input.request.request_id.len(),
        input.commands.len(),
        input.receipt.bytes().len(),
    ]
    .into_iter()
    .try_fold(85u64, |n, len| {
        n.checked_add(u64::try_from(len).map_err(|_| QueueError::TooLarge)?)
            .ok_or(QueueError::TooLarge)
    })?;
    if size > max_bytes {
        return Err(QueueError::TooLarge);
    }
    let charge = job.owned_buffer(
        size.checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut out = OwnedClaimPublication {
        bytes: Vec::new(),
        _charge: charge,
    };
    out.bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| QueueError::TooLarge)?)
        .map_err(|_| QueueError::TooLarge)?;
    out.bytes.extend_from_slice(MAGIC);
    out.bytes.extend_from_slice(&input.claimed_at.to_le_bytes());
    out.bytes
        .extend_from_slice(&input.request.eligible_at.to_le_bytes());
    out.bytes
        .extend_from_slice(&input.request.lease_nanos.to_le_bytes());
    blob(&mut out.bytes, input.request.request_id.as_bytes())?;
    out.bytes.extend_from_slice(&input.request.fingerprint()?);
    blob(&mut out.bytes, input.commands)?;
    blob(&mut out.bytes, input.receipt.bytes())?;
    let view = view_claim_publication(out.bytes())?;
    view.validate_binding(job)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_body::ResolvedBody,
        queue_claim::{ClaimContext, plan_claim},
        queue_claim_receipt::{ClaimLease, encode_claim_receipt},
        queue_fields::{FieldCoverage, ResolvedFields},
        queue_planner::{CommitDelta, RecordIdentity, RecordSnapshot, StoredWork, combine_deltas},
        queue_publication::encode_publication_pinned,
        queue_schema_codec::encode_definition,
    };
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    fn request() -> ClaimRequest {
        ClaimRequest {
            queue: QueueAddress {
                tenant: "t\0β".into(),
                queue: "q/λ".into(),
            },
            request_id: "claim".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 1 << 20,
            fields: Some(Default::default()),
            include_body: false,
        }
    }
    fn before() -> RecordSnapshot {
        let mut snapshot = RecordSnapshot {
            next_sequence: 2,
            ..Default::default()
        };
        for (key, sequence, priority) in [("z", 1, 0), ("a", 2, 1)] {
            snapshot.work.insert(
                key.into(),
                StoredWork {
                    record: WorkRecord {
                        work: NewWork {
                            key: key.into(),
                            priority,
                            available_at: 0,
                            fields: Default::default(),
                            body: bytes::Bytes::from_static(b"preserved opaque body"),
                        },
                        version: 1,
                        insertion_sequence: sequence,
                        lifecycle: Lifecycle::Pending,
                        eligible_since: -10,
                        attempts: 0,
                        lease_expires_at: None,
                    },
                    lease_hash: None,
                    field_coverage: FieldCoverage::Complete,
                    body_loaded: true,
                },
            );
            snapshot.versions.insert(
                RecordIdentity {
                    collection: None,
                    key: key.into(),
                },
                1,
            );
        }
        snapshot
    }
    fn delta(before: &RecordSnapshot, selected: bool) -> CommitDelta {
        let mut work = vec![];
        let mut versions = vec![];
        let mut fields = BTreeMap::new();
        let mut bodies = BTreeMap::new();
        if selected {
            for key in ["z", "a"] {
                let planned = plan_claim(
                    before.work[key].clone(),
                    vec![if key == "z" { 7 } else { 9 }; 32],
                    ClaimContext {
                        now: 10,
                        eligible_at: 0,
                        expires_at: 1_000_010,
                        insertion_head: 2,
                    },
                )
                .unwrap();
                work.push(planned.work);
                versions.push(RecordVersion {
                    collection: None,
                    key: key.into(),
                    version: 2,
                });
                fields.insert(key.into(), ResolvedFields::Preserve);
                bodies.insert(key.into(), ResolvedBody::Preserve);
            }
        }
        let delta = CommitDelta {
            work,
            work_fields: fields,
            work_bodies: bodies,
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: versions.clone(),
            next_sequence: 2,
            receipt: CommitReceipt {
                request_id: "unused".into(),
                outcome: GroupOutcome::Committed { versions },
            },
        };
        combine_deltas(&[delta], 2).unwrap()
    }
    #[test]
    fn thin_wrapper_binds_priority_order_to_sorted_commands_and_empty_claims() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let request = request();
        let before = before();
        let limits = QueueLimits::default();
        for selected in [false, true] {
            let delta = delta(&before, selected);
            let commands = encode_publication_pinned(
                &request.queue,
                definition.pin(),
                &before,
                &delta,
                &[],
                1 << 20,
                &job,
            )
            .unwrap();
            let entries = if selected {
                vec![
                    ClaimLease {
                        key: "z".into(),
                        token: vec![7; 32],
                        version: 2,
                    },
                    ClaimLease {
                        key: "a".into(),
                        token: vec![9; 32],
                        version: 2,
                    },
                ]
            } else {
                vec![]
            };
            let receipt = encode_claim_receipt(&entries, 1 << 20, &job).unwrap();
            let input = || ClaimPublicationInput {
                request: &request,
                definition: &definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: 10,
                limits: &limits,
            };
            let out = encode_claim_publication(input(), 1 << 20, &job).unwrap();
            let n = out.bytes().len() as u64;
            // Independently assembled in Python from the frozen field grammar,
            // canonical Postcard request bytes and SHA-256, not this writer.
            use sha2::{Digest, Sha256};
            if selected {
                assert_eq!(n, 544);
                assert_eq!(
                    Sha256::digest(out.bytes())
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                    "07be6c7890946d187d28d9ec51a3079648de8f9cc41589d75628ccbf1ddf7ca0"
                );
            } else {
                const EMPTY: &str = "46575143310a00000000000000000000000000000040420f00000000000500000000000000636c61696ddeff9504e3694d7fca65113892b75f4640c71515d003a56f26aa84363a4b4a0a5d00000000000000465751503104000000000000007400ceb20400000000000000712fcebbca34cd395acac8ddffbe8b24816de31c87573a5290d20f00070e62cf8ae9dfef02000000000000000200000000000000000000000000000000000000000000000d0000000000000046574352310000000000000000";
                let expected = EMPTY
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(out.bytes(), expected);
            }
            let before_binding = admission.usage().unwrap();
            let tight = QueueAdmission::new(1, 4095).unwrap();
            let tight_job = Arc::new(tight.reserve(0).unwrap());
            assert!(
                view_claim_publication(out.bytes())
                    .unwrap()
                    .validate_binding(&tight_job)
                    .is_err()
            );
            assert_eq!(tight.usage().unwrap(), (1, 0));
            assert_eq!(admission.usage().unwrap(), before_binding);

            assert!(encode_claim_publication(input(), n, &job).is_ok());
            assert!(matches!(
                encode_claim_publication(input(), n - 1, &job),
                Err(QueueError::TooLarge)
            ));
            let view = view_claim_publication(out.bytes()).unwrap();
            view.validate_binding(&job).unwrap();
            assert_eq!(view.expires_at, 1_000_010);
            assert_eq!(view.fingerprint, &request.fingerprint().unwrap());
            assert_eq!(
                view.receipt()
                    .entries()
                    .map(|entry| entry.unwrap().key)
                    .collect::<Vec<_>>(),
                if selected { vec!["z", "a"] } else { vec![] }
            );
            assert_eq!(
                view.commands()
                    .commands()
                    .map(|command| command.unwrap().identity.key)
                    .collect::<Vec<_>>(),
                if selected { vec!["a", "z"] } else { vec![] }
            );
            for end in 0..out.bytes().len() {
                assert!(view_claim_publication(&out.bytes()[..end]).is_err());
            }
            let mut trailing = out.bytes().to_vec();
            trailing.push(0);
            assert!(view_claim_publication(&trailing).is_err());
            if selected {
                let second = view.receipt().entries().nth(1).unwrap().unwrap();
                let key_position = second.key.as_ptr() as usize - out.bytes().as_ptr() as usize;
                let mut duplicate = out.bytes().to_vec();
                duplicate[key_position] = b'z';
                assert!(
                    view_claim_publication(&duplicate)
                        .unwrap()
                        .validate_binding(&job)
                        .is_err()
                );
                for variant in 0..4 {
                    let mut changed = delta.clone();
                    match variant {
                        0 => {
                            changed
                                .work_fields
                                .insert("a".into(), ResolvedFields::Replace(Default::default()));
                        }
                        1 => {
                            changed
                                .work
                                .iter_mut()
                                .find(|row| row.record.work.key == "a")
                                .unwrap()
                                .record
                                .work
                                .body = bytes::Bytes::new();
                            changed
                                .work_bodies
                                .insert("a".into(), ResolvedBody::Replace(bytes::Bytes::new()));
                        }
                        2 => {
                            let row = changed
                                .work
                                .iter_mut()
                                .find(|row| row.record.work.key == "a")
                                .unwrap();
                            row.record.lifecycle = Lifecycle::Completed;
                            row.record.lease_expires_at = None;
                            row.lease_hash = None;
                        }
                        _ => {
                            changed.work.retain(|row| row.record.work.key != "a");
                            changed.work_fields.remove("a");
                            changed.work_bodies.remove("a");
                            changed.deleted_work.push("a".into());
                        }
                    }
                    let changed_commands = encode_publication_pinned(
                        &request.queue,
                        definition.pin(),
                        &before,
                        &changed,
                        &[],
                        1 << 20,
                        &job,
                    )
                    .unwrap();
                    let changed_input = ClaimPublicationInput {
                        commands: changed_commands.as_bytes(),
                        ..input()
                    };
                    assert!(encode_claim_publication(changed_input, 1 << 20, &job).is_err());
                }
                let token = view.receipt().entries().next().unwrap().unwrap().token;
                let position = token.as_ptr() as usize - out.bytes().as_ptr() as usize;
                let mut wrong = out.bytes().to_vec();
                wrong[position] ^= 1;
                assert!(
                    view_claim_publication(&wrong)
                        .unwrap()
                        .validate_binding(&job)
                        .is_err()
                );
            }
        }
    }
}
