//! Exact incremental counts for the same fixed serializer used by publication.
//! This is byte accounting, not business/storage-authority validation or an ACK.
use super::*;

fn counted(write: impl FnOnce(&mut Sink<'_>) -> PublicationResult<()>) -> PublicationResult<u64> {
    let mut sink = Sink::Count(0);
    write(&mut sink)?;
    let Sink::Count(bytes) = sink else {
        unreachable!("count sink")
    };
    Ok(bytes)
}
/// Fixed header including both list counts. Counts/sequences are fixed-width;
/// their eventual values do not change the byte footprint.
pub fn measure_publication_header(
    queue: &QueueAddress,
    pin: DefinitionPin,
) -> PublicationResult<u64> {
    counted(|sink| {
        write_frame_header(sink, queue, pin, 0, 0, 0)?;
        sink.n(0)
    })
}
pub fn measure_publication_receipt(receipt: &PublicationReceipt<'_>) -> PublicationResult<u64> {
    counted(|sink| write_receipt(sink, receipt))
}
/// Exact empty-publication floor for a fresh commit rejection, using the same
/// serializer as a complete frame. No request-id or outcome allocation is needed.
/// Preflight must reserve the maximum applicable fallback, not only the header.
pub fn measure_commit_rejection_floor(
    queue: &QueueAddress,
    pin: DefinitionPin,
    id: &str,
    reason: &Rejection,
) -> PublicationResult<u64> {
    counted(|sink| {
        write_frame_header(sink, queue, pin, 0, 0, 0)?;
        sink.n(1)?;
        write_receipt_header(sink, 2, id, 3, &[0; 32])?;
        sink.tag(1)?;
        rejection(sink, reason)
    })
}
fn mutation(
    version: &RecordVersion,
    predecessor: Option<u64>,
    highwater: u64,
) -> PublicationResult<bool> {
    if version.version == 0
        || version.version < highwater
        || predecessor.is_some_and(|previous| previous == 0 || previous != highwater)
    {
        return invalid("invalid measured predecessor/highwater");
    }
    if version.version == highwater {
        if predecessor.is_none() {
            return invalid("absent measured no-op");
        }
        return Ok(false);
    }
    Ok(true)
}
/// Caller supplies an already validated exact resolved command. Read-view
/// materialization and body_loaded/coverage certificates are not serialized.
/// None is deletion/tombstone; creation after prior absence requires replacement.
pub fn measure_work_mutation(
    version: &RecordVersion,
    predecessor: Option<u64>,
    highwater: u64,
    row: Option<(&StoredWork, &ResolvedFields, &ResolvedBody)>,
) -> PublicationResult<u64> {
    if version.collection.is_some() {
        return invalid("work measurement has collection");
    }
    let advances = mutation(version, predecessor, highwater)?;
    if !advances && row.is_none() {
        return invalid("measured no-op deletion");
    }
    if let Some((work, fields, body)) = row {
        if work.record.version != version.version
            || work.record.work.key != version.key
            || work.record.insertion_sequence == 0
        {
            return invalid("measured work identity/version");
        }
        if predecessor.is_none()
            && (!matches!(fields, ResolvedFields::Replace(_))
                || !matches!(body, ResolvedBody::Replace(_)))
        {
            return invalid("measured creation requires replacement");
        }
        if !advances
            && (!matches!(fields, ResolvedFields::Preserve)
                || !matches!(body, ResolvedBody::Preserve))
        {
            return invalid("non-advancing measured write");
        }
        validate_lease(
            work.record.lifecycle,
            work.record.lease_expires_at.is_some(),
            work.lease_hash.is_some(),
        )?;
    }
    if !advances {
        return Ok(0);
    }
    counted(|sink| {
        write_mutation_header(sink, version, predecessor, highwater)?;
        if let Some((work, fields, body)) = row {
            write_work_value(sink, work, fields, body)
        } else {
            sink.tag(0)
        }
    })
}
pub fn measure_state_mutation(
    version: &RecordVersion,
    predecessor: Option<u64>,
    highwater: u64,
    row: Option<&StateRecord>,
) -> PublicationResult<u64> {
    let Some(collection) = version.collection.as_deref() else {
        return invalid("state measurement lacks collection");
    };
    let advances = mutation(version, predecessor, highwater)?;
    if !advances && row.is_none() {
        return invalid("measured no-op state deletion");
    }
    if row.is_some_and(|row| {
        row.collection != collection || row.key != version.key || row.version != version.version
    }) {
        return invalid("measured state identity/version");
    }
    if !advances {
        return Ok(0);
    }
    counted(|sink| {
        write_mutation_header(sink, version, predecessor, highwater)?;
        write_state_value(sink, row)
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationBudgetStatus {
    WithinLimit,
    Exceeded,
}
/// One LOGICAL group, before physical packing. This counter owns no payloads.
/// Exceeding the limit is sticky, including checked-add overflow. The execution
/// layer must still exhaust selection, choose rejection precedence and publish
/// its complete rejection receipt; counting alone does not provide that path.
#[derive(Debug)]
pub struct PublicationByteBudget {
    limit: u64,
    required: Option<u64>,
    exceeded: bool,
}
impl PublicationByteBudget {
    pub fn new(queue: &QueueAddress, pin: DefinitionPin, limit: u64) -> PublicationResult<Self> {
        let required = measure_publication_header(queue, pin)?;
        if required > limit {
            return Err(PublicationError::FrameLimit);
        }
        Ok(Self {
            limit,
            required: Some(required),
            exceeded: false,
        })
    }
    pub fn add_measured(&mut self, bytes: u64) -> PublicationBudgetStatus {
        self.required = self
            .required
            .and_then(|required| required.checked_add(bytes));
        self.exceeded |= self.required.is_none_or(|required| required > self.limit);
        self.status()
    }
    pub fn required_bytes(&self) -> Option<u64> {
        self.required
    }
    pub fn status(&self) -> PublicationBudgetStatus {
        if self.exceeded {
            PublicationBudgetStatus::Exceeded
        } else {
            PublicationBudgetStatus::WithinLimit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{queue, schema, seed};
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_planner::{combine_deltas, plan_commit_group},
        queue_schema_codec::encode_definition,
    };
    use std::collections::BTreeMap;
    fn parts(
        before: &RecordSnapshot,
        delta: &CommitDelta,
        receipt: &PublicationReceipt<'_>,
        pin: DefinitionPin,
    ) -> u64 {
        let mut count = PublicationByteBudget::new(&queue(), pin, u64::MAX).unwrap();
        for version in &delta.versions {
            let id = RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            };
            let highwater = before.versions.get(&id).copied().unwrap_or(0);
            let bytes = if let Some(collection) = &version.collection {
                let key = (collection.clone(), version.key.clone());
                let old = before.state.get(&key).map(|row| row.version);
                let row = delta
                    .state
                    .iter()
                    .find(|row| row.collection == *collection && row.key == version.key);
                measure_state_mutation(version, old, highwater, row).unwrap()
            } else {
                let old = before.work.get(&version.key).map(|row| row.record.version);
                let row = delta
                    .work
                    .iter()
                    .find(|row| row.record.work.key == version.key)
                    .map(|row| {
                        (
                            row,
                            &delta.work_fields[&version.key],
                            &delta.work_bodies[&version.key],
                        )
                    });
                measure_work_mutation(version, old, highwater, row).unwrap()
            };
            assert_eq!(
                count.add_measured(bytes),
                PublicationBudgetStatus::WithinLimit
            );
        }
        count.add_measured(measure_publication_receipt(receipt).unwrap());
        count.required_bytes().unwrap()
    }
    #[test]
    fn incremental_parts_equal_frozen_complete_frames_and_exact_boundaries() {
        let q = queue();
        let definition = schema();
        let raw = encode_definition(&definition).unwrap();
        let pin = DefinitionPin::from_persisted_bytes(&raw);
        let before = RecordSnapshot::default();
        let mut after = before.clone();
        let group = seed();
        let delta = plan_commit_group(&group, &mut after, 0, &definition).unwrap();
        let result = delta.receipt.clone();
        let delta = combine_deltas(&[delta], 0).unwrap();
        let receipt = PublicationReceipt::commit(&q, &group, &result).unwrap();
        let measured = parts(&before, &delta, &receipt, pin);
        assert_eq!(measured, 489);
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let frame =
            encode_publication(&q, &raw, &before, &delta, &[receipt], measured, &job).unwrap();
        assert_eq!(frame.as_bytes().len() as u64, measured);
        drop(frame);
        let receipt = PublicationReceipt::commit(&q, &group, &result).unwrap();
        assert!(matches!(
            encode_publication(&q, &raw, &before, &delta, &[receipt], measured - 1, &job),
            Err(PublicationError::FrameLimit)
        ));
        let header = measure_publication_header(&q, pin).unwrap();
        let mut budget = PublicationByteBudget::new(&q, pin, measured).unwrap();
        assert_eq!(
            budget.add_measured(measured - header),
            PublicationBudgetStatus::WithinLimit
        );
        assert_eq!(budget.add_measured(1), PublicationBudgetStatus::Exceeded);
        assert_eq!(budget.add_measured(0), PublicationBudgetStatus::Exceeded);
        assert!(matches!(
            PublicationByteBudget::new(&q, pin, header - 1),
            Err(PublicationError::FrameLimit)
        ));
    }
    #[test]
    fn work_edits_empty_body_deletions_noops_and_state_tombstones_match_frames() {
        let q = queue();
        let definition = schema();
        let raw = encode_definition(&definition).unwrap();
        let pin = DefinitionPin::from_persisted_bytes(&raw);
        let mut current = RecordSnapshot::default();
        plan_commit_group(&seed(), &mut current, 0, &definition).unwrap();
        let work_key = seed().work_inserts[0].key.clone();
        let state_key = seed().state_updates[0].key.clone();
        for step in 0..4 {
            let before = current.clone();
            let mut group = CommitGroup {
                administration: None,
                request_id: format!("step-{step}"),
                claimed_updates: vec![],
                state_updates: vec![],
                work_inserts: vec![],
                work_updates: vec![],
            };
            let change = match step {
                0 => UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("gate".into(), Some(FieldValue::Boolean(true)))]),
                    body: Some(Default::default()),
                    ..Default::default()
                }),
                1 => UnclaimedChange::Administrative {
                    patch: WorkPatch::default(),
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
                _ => UnclaimedChange::Delete,
            };
            if step < 3 {
                group.work_updates.push(UnclaimedUpdate {
                    key: work_key.clone(),
                    expect: StateExpect::Version(current.work[&work_key].record.version),
                    change,
                });
            } else {
                group.state_updates.push(StateUpdate {
                    collection: "instances".into(),
                    key: state_key.clone(),
                    expect: StateExpect::Version(
                        current.state[&("instances".into(), state_key.clone())].version,
                    ),
                    change: StateChange::Delete,
                });
            }
            let delta = plan_commit_group(&group, &mut current, 0, &definition).unwrap();
            let result = delta.receipt.clone();
            let delta = combine_deltas(&[delta], before.next_sequence).unwrap();
            let receipt = PublicationReceipt::commit(&q, &group, &result).unwrap();
            let measured = parts(&before, &delta, &receipt, pin);
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = admission.reserve(0).unwrap();
            let frame =
                encode_publication(&q, &raw, &before, &delta, &[receipt], measured, &job).unwrap();
            assert_eq!(frame.as_bytes().len() as u64, measured);
        }
    }
    #[test]
    fn megabyte_body_counts_exceed_without_frame_or_retained_body_copies() {
        let q = queue();
        let pin = DefinitionPin::from_persisted_bytes(b"definition");
        let body = bytes::Bytes::from(vec![7; 1 << 20]);
        let row = StoredWork {
            record: WorkRecord {
                work: NewWork {
                    key: "row".into(),
                    priority: i64::MIN,
                    available_at: i64::MAX,
                    fields: Default::default(),
                    body: body.clone(),
                },
                version: 2,
                insertion_sequence: 1,
                lifecycle: Lifecycle::Pending,
                eligible_since: 0,
                attempts: 0,
                lease_expires_at: None,
            },
            lease_hash: None,
            field_coverage: crate::queue_fields::FieldCoverage::Complete,
            body_loaded: true,
        };
        let version = RecordVersion {
            collection: None,
            key: "row".into(),
            version: 2,
        };
        let change = ResolvedFields::Preserve;
        let body_change = ResolvedBody::Replace(body.clone());
        let measured =
            measure_work_mutation(&version, Some(1), 1, Some((&row, &change, &body_change)))
                .unwrap();
        assert!(measured > 1 << 20);
        let admission = QueueAdmission::new(1, 4096).unwrap();
        let job = admission.reserve(0).unwrap();
        let mut budget = PublicationByteBudget::new(&q, pin, 4096).unwrap();
        for _ in 0..5001 {
            assert_eq!(
                budget.add_measured(measured),
                PublicationBudgetStatus::Exceeded
            );
        }
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(body.as_ptr(), row.record.work.body.as_ptr());
    }
    #[test]
    fn rejection_floor_includes_unbounded_queue_and_request_id_bytes() {
        let q = QueueAddress {
            tenant: format!("{}\0α", "t".repeat(80 << 10)),
            queue: "queue".into(),
        };
        let pin = DefinitionPin::from_persisted_bytes(b"definition");
        let id = "request".repeat(12000);
        let reason = Rejection::Invalid("x".repeat(256));
        let floor = measure_commit_rejection_floor(&q, pin, &id, &reason).unwrap();
        assert!(floor > 160 << 10);
        let group = CommitGroup {
            administration: None,
            request_id: id.clone(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let result = CommitReceipt {
            request_id: id,
            outcome: GroupOutcome::Rejected(reason),
        };
        let delta = CommitDelta {
            work: vec![],
            work_fields: BTreeMap::new(),
            work_bodies: BTreeMap::new(),
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: vec![],
            next_sequence: 0,
            receipt: result.clone(),
        };
        let before = RecordSnapshot::default();
        let receipt = PublicationReceipt::commit(&q, &group, &result).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let frame = encode_publication(&q, b"definition", &before, &delta, &[receipt], floor, &job)
            .unwrap();
        assert_eq!(frame.as_bytes().len() as u64, floor);
    }
    #[test]
    fn count_overflow_is_sticky_and_malformed_metadata_is_not_size_rejection() {
        let q = queue();
        let pin = DefinitionPin::from_persisted_bytes(b"definition");
        let mut budget = PublicationByteBudget::new(&q, pin, u64::MAX).unwrap();
        assert_eq!(
            budget.add_measured(u64::MAX),
            PublicationBudgetStatus::Exceeded
        );
        assert_eq!(budget.required_bytes(), None);
        assert_eq!(budget.add_measured(0), PublicationBudgetStatus::Exceeded);
        let version = RecordVersion {
            collection: None,
            key: "row".into(),
            version: 1,
        };
        assert!(matches!(
            measure_work_mutation(&version, Some(1), 1, None),
            Err(PublicationError::Invalid(_))
        ));
        assert!(matches!(
            measure_work_mutation(&version, Some(2), 1, None),
            Err(PublicationError::Invalid(_))
        ));
    }
}
