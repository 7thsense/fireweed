//! Backend-neutral conversion of an already authenticated original outcome.
use super::*;
use crate::{
    queue_admission::OwnedQueueBufferCharge,
    queue_storage_layout::{decode, encode},
};
use std::sync::Arc;

/// Copy a checked original result into an independently funded public DTO.
/// Callers must already establish exact source membership and matching stored
/// fingerprint: the stored fingerprint is absent from this function's inputs.
/// This establishes neither freshness nor durability. Retain the returned owner
/// with the DTO and enforce aggregate response limits.
pub fn decode_original_commit_outcome(
    queue: &QueueAddress,
    group: &CommitGroup,
    format: u8,
    raw: &[u8],
    job: &Arc<QueueReservation>,
) -> QueueResult<(CommitReceipt, OwnedQueueBufferCharge)> {
    fn invalid(error: PublicationError) -> QueueError {
        QueueError::OutcomeUnknown(format!("invalid original commit outcome: {error:?}"))
    }
    let expected_format = if group.administration.is_some() { 4 } else { 3 };
    if format != expected_format {
        return Err(QueueError::OutcomeUnknown(
            "original commit outcome fingerprint format differs".into(),
        ));
    }
    let outcome = view_receipt_outcome(2, format, raw).map_err(invalid)?;
    let ReceiptOutcomeRef::Commit(commit) = outcome else {
        return Err(QueueError::OutcomeUnknown(
            "original outcome domain differs".into(),
        ));
    };
    let heap = receipt_outcome_heap_bound(outcome, raw.len() as u64)
        .map_err(invalid)?
        .checked_add(group.request_id.len() as u64)
        .ok_or(QueueError::TooLarge)?;
    let owner = job.owned_buffer(heap)?;
    let projection = CommitReceiptProjection {
        request_id: &group.request_id,
        outcome: commit,
    };
    let encoded_bytes = encoded_len(&projection)?;
    let temporary = job.owned_buffer(
        encoded_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let encoded = encode(&projection)?;
    let receipt: CommitReceipt = decode(&encoded)?;
    validate_original_commit_receipt(queue, group, &receipt, job).map_err(invalid)?;
    // Encoded storage drops before its separate temporary admission; the DTO's
    // owner transfers intact and cannot borrow any source or scratch buffer.
    drop(encoded);
    drop(temporary);
    Ok((receipt, owner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    fn fixture() -> (QueueAddress, CommitGroup, CommitReceipt, Vec<u8>) {
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let group = CommitGroup {
            request_id: " literal\0α ".repeat(8192),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            administration: None,
            work_inserts: vec![NewWork {
                key: "work\0β".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            }],
        };
        let receipt = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Committed {
                versions: vec![RecordVersion {
                    collection: None,
                    key: "work\0β".into(),
                    version: 1,
                }],
            },
        };
        let binding = PublicationReceipt::commit(&queue, &group, &receipt).unwrap();
        let mut header = Sink::Count(0);
        write_receipt_header(&mut header, 2, &group.request_id, 3, &binding.fingerprint).unwrap();
        let Sink::Count(start) = header else {
            unreachable!()
        };
        let mut encoded = Vec::new();
        write_receipt(&mut Sink::Buffer(&mut encoded), &binding).unwrap();
        let raw = encoded[start as usize..].to_vec();
        (queue, group, receipt, raw)
    }
    fn peak(group: &CommitGroup, raw: &[u8]) -> (u64, u64) {
        let outcome = view_receipt_outcome(2, 3, raw).unwrap();
        let ReceiptOutcomeRef::Commit(commit) = outcome else {
            unreachable!()
        };
        let heap = receipt_outcome_heap_bound(outcome, raw.len() as u64).unwrap()
            + group.request_id.len() as u64;
        let temporary = encoded_len(&CommitReceiptProjection {
            request_id: &group.request_id,
            outcome: commit,
        })
        .unwrap()
            * 4
            + 4096;
        (heap, heap + temporary)
    }
    #[test]
    fn original_commit_dto_exact_admission_owns_long_literal_after_source_and_job_drop() {
        let (queue, group, expected, raw) = fixture();
        let (heap, total) = peak(&group, &raw);
        let admission = QueueAdmission::new(1, total).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (receipt, owner) =
            decode_original_commit_outcome(&queue, &group, 3, &raw, &job).unwrap();
        assert_eq!(receipt, expected);
        assert_eq!(owner.admitted_bytes(), heap);
        assert_eq!(admission.usage().unwrap(), (1, heap));
        drop((job, raw, group, queue, expected));
        assert!(receipt.request_id.contains('\0'));
        assert_eq!(admission.usage().unwrap(), (1, heap));
        drop(receipt);
        drop(owner);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn original_commit_dto_one_short_refuses_before_copy_and_restores_admission() {
        let (queue, group, _, raw) = fixture();
        let (_, total) = peak(&group, &raw);
        let admission = QueueAdmission::new(1, total - 1).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        assert!(
            matches!(decode_original_commit_outcome(&queue,&group,3,&raw,&job),
            Err(QueueError::OutcomeUnknown(ref m)) if m.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn original_commit_dto_rejects_trailing_truncated_format_and_wrong_group_without_leaks() {
        let (queue, group, _, raw) = fixture();
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let mut trailing = raw.clone();
        trailing.push(0);
        for (format, bytes) in [
            (3, &raw[..raw.len() - 1]),
            (3, trailing.as_slice()),
            (0, raw.as_slice()),
        ] {
            assert!(decode_original_commit_outcome(&queue, &group, format, bytes, &job).is_err());
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
        let mut wrong = group.clone();
        wrong.work_inserts[0].key = "other".into();
        assert!(decode_original_commit_outcome(&queue, &wrong, 3, &raw, &job).is_err());
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    fn administrative_fixture(
        job: &Arc<QueueReservation>,
    ) -> (QueueAddress, CommitGroup, CommitReceipt, Vec<u8>) {
        use fireweed_core::queue_administration::*;
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram.record(&AdministrativeOutcome::NoChange).unwrap();
        let result = AdministrativeResult {
            histogram,
            clauses: vec![],
            items: vec![AdministrativeItemResult {
                key: "a".into(),
                outcome: AdministrativeOutcome::NoChange,
                before: Some(AdministrativeBefore {
                    version: 1,
                    lifecycle: Lifecycle::Pending,
                    available_at: 0,
                    eligible_since: 0,
                    attempts: 0,
                    lease_expires_at: None,
                    fields: std::collections::BTreeMap::from([(
                        "text".into(),
                        FieldValue::Text("x".repeat(512)),
                    )]),
                }),
            }],
        };
        let mut group = CommitGroup {
            request_id: "admin\0α".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
            administration: Some(AdministrativeCommit {
                intent: AdministrativeIntent {
                    evaluated_at: 0,
                    before_fields: Some(std::collections::BTreeSet::from(["text".into()])),
                    operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                        key: "a".into(),
                        expected_version: Some(1),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch::default(),
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: Default::default(),
                        },
                    }]),
                },
                max_result_bytes: 65536,
            }),
        };
        let limit = measure_complete_administrative_receipt(
            &group.request_id,
            &AdministrativeReceiptOutcome::Committed {
                versions: &[],
                administration: &result,
            },
        )
        .unwrap();
        group.administration.as_mut().unwrap().max_result_bytes = limit;
        let component = encode_administrative_result(&result, job).unwrap();
        let mut raw = Vec::new();
        let mut sink = Sink::Buffer(&mut raw);
        sink.tag(4).unwrap();
        sink.n(0).unwrap();
        sink.bytes(component.as_bytes()).unwrap();
        let receipt = CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::AdministrativeCommitted {
                versions: vec![],
                administration: result,
            },
        };
        (queue, group, receipt, raw)
    }
    #[test]
    fn original_administrative_dto_preserves_nonempty_result_and_enforces_original_bound() {
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (queue, mut group, expected, raw) = administrative_fixture(&job);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        let (receipt, owner) =
            decode_original_commit_outcome(&queue, &group, 4, &raw, &job).unwrap();
        assert_eq!(receipt, expected);
        drop(receipt);
        drop(owner);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        group.administration.as_mut().unwrap().max_result_bytes -= 1;
        assert!(
            matches!(decode_original_commit_outcome(&queue,&group,4,&raw,&job),
            Err(QueueError::OutcomeUnknown(ref message)) if message.contains("exceeds original result bound"))
        );
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn original_commit_dto_checks_format_even_for_common_rejection_bytes() {
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (queue, ordinary, _, _) = fixture();
        let (_, admin, _, _) = administrative_fixture(&job);
        let mut raw = Vec::new();
        let mut sink = Sink::Buffer(&mut raw);
        sink.tag(1).unwrap();
        rejection(&mut sink, &Rejection::NotFound).unwrap();
        assert!(view_receipt_outcome(2, 3, &raw).is_ok());
        assert!(view_receipt_outcome(2, 4, &raw).is_ok());
        for (group, format) in [(&ordinary, 3), (&admin, 4)] {
            let (receipt, owner) =
                decode_original_commit_outcome(&queue, group, format, &raw, &job).unwrap();
            assert_eq!(receipt.outcome, GroupOutcome::Rejected(Rejection::NotFound));
            drop(receipt);
            drop(owner);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
        for (group, format) in [(&ordinary, 4), (&admin, 3)] {
            assert!(
                matches!(decode_original_commit_outcome(&queue,group,format,&raw,&job),
                Err(QueueError::OutcomeUnknown(ref message)) if message.contains("fingerprint format differs"))
            );
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
}
