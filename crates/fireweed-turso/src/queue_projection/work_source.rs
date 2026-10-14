//! Source correspondence for ordinary selected work, before planner decisions.
use super::*;
use fireweed_engine::queue_group_state_authority::{WorkDescriptor, WorkMetadata};
pub(super) fn validate_presence(
    source: &WorkDescriptor,
    cached: &work::WorkAuthority,
) -> QueueResult<()> {
    match (source.highwater, source.metadata, cached) {
        (0, None, work::WorkAuthority::Absent) => Ok(()),
        (highwater, None, work::WorkAuthority::Deleted { version }) if highwater == *version => {
            Ok(())
        }
        (_, Some(expected), work::WorkAuthority::Present { work, .. }) => {
            let record = &work.record;
            let actual = WorkMetadata {
                version: record.version,
                priority: record.work.priority,
                available_at: record.work.available_at,
                insertion_sequence: record.insertion_sequence,
                lifecycle: record.lifecycle,
                eligible_since: record.eligible_since,
                attempts: record.attempts,
                lease_expires_at: record.lease_expires_at,
                lease_hash: work.lease_hash,
            };
            if actual == expected {
                Ok(())
            } else {
                Err(unknown("cached work metadata differs from source"))
            }
        }
        _ => Err(unknown(
            "cached work presence or highwater differs from source",
        )),
    }
}
pub(super) async fn validate_payload_metadata(
    connection: &Connection,
    id: i64,
    definition: &CollectionDefinition,
    source: &WorkDescriptor,
    names: &std::collections::BTreeSet<String>,
    include_body: bool,
) -> QueueResult<()> {
    for name in names {
        let expected = source
            .fields
            .get(name)
            .ok_or_else(|| unknown("work field not covered by source"))?;
        let no = field_number(definition, name)?;
        let row = if no >= 0 {
            one(connection, "SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END, CASE WHEN typeof(value)='blob' THEN substr(value,1,1) ELSE NULL END FROM q7_fields WHERE record_id=?1 AND field=?2",
                vec![Value::Integer(id), Value::Integer(i64::from(no))], 2).await?
        } else {
            one(connection, "SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END, CASE WHEN typeof(value)='blob' THEN substr(value,1,1) ELSE NULL END FROM q7_additional WHERE record_id=?1 AND name_hash=?2 AND name=?3",
                vec![Value::Integer(id),blob(&discovery_digest(name.as_bytes())),blob(name.as_bytes())],2).await?
        };
        match (expected, row) {
            (None, None) => {}
            (Some(expected), Some(row))
                if u64::try_from(integer(&row[0])?).ok()
                    == Some(expected.fingerprint.encoded_bytes) =>
            {
                let tag = match expected.field_type {
                    FieldType::Integer => 0,
                    FieldType::Float => 1,
                    FieldType::Boolean => 2,
                    FieldType::Text => 3,
                    FieldType::Bytes => 4,
                };
                if bytes(&row[1])? != [tag] {
                    return Err(unknown("cached work field type differs from source"));
                }
            }
            _ => {
                return Err(unknown(
                    "cached work field presence or length differs from source",
                ));
            }
        }
    }
    if include_body {
        let expected = source
            .body
            .ok_or_else(|| unknown("work body not covered by source"))?;
        let row = one(connection,"SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_bodies WHERE record_id=?1",vec![Value::Integer(id)],1).await?
            .ok_or_else(|| unknown("source work body missing"))?;
        if u64::try_from(integer(&row[0])?).ok() != Some(expected.bytes) {
            return Err(unknown("cached work body length differs from source"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, execute_for_test, group, seed_record};
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_group_state_authority::{ColdGroupStateTracker, GroupDeclarations},
        queue_log_record::encode_checked_genesis,
        queue_recovery::ColdQueueHistory,
    };
    #[tokio::test]
    async fn required_work_body_corruption_is_unknown_before_or_after_admitted_fetch() {
        for case in ["valid", "same-size", "inflated", "missing"] {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 64 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let workers = ScratchWorkers::new(1, 1).unwrap();
            let (projection, definition) =
                bootstrap(&directory.path().join("projection.db"), &job, &workers).await;
            let genesis =
                encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job)
                    .unwrap();
            let (_, seed) = seed_record(&definition, &job, true);
            let copy = OwnedQueueLogRecord::from_admitted_bytes(
                bytes::Bytes::copy_from_slice(seed.bytes()),
                job.owned_buffer((seed.bytes().len() as u64) * 4 + 4096)
                    .unwrap(),
                &job,
            )
            .unwrap();
            projection.apply(copy, 1, &job, &workers).await.unwrap();
            let mut declared = group("needed-body");
            declared.work_updates.push(UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Administrative {
                    patch: WorkPatch {
                        body: Some(bytes::Bytes::from_static(b"next")),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Completed,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            });
            let declared = Arc::new(declared);
            let mut tracker = ColdGroupStateTracker::with_work(
                &address(),
                GroupDeclarations::Group(declared.clone()),
                &job,
            )
            .unwrap();
            let mut history = ColdQueueHistory::new(&address(), &job).unwrap();
            for (offset, record) in [&genesis, &seed].into_iter().enumerate() {
                let checked = record.checked().unwrap();
                history.observe_record(offset as u64, &checked).unwrap();
                if offset == 0 {
                    tracker.initialize_work(&history, &checked, &job).unwrap();
                }
                tracker.observe_record(offset as u64, &checked).unwrap();
            }
            let proof = Arc::new(tracker.finish(&history, &genesis, (2, 2, 1)).unwrap());
            let source = proof.group(0).unwrap();
            source
                .validate(&address(), &declared, &genesis, (2, 2, 1), &job)
                .unwrap();
            match case {
                "same-size" => execute_for_test(&projection,"UPDATE q7_bodies SET value=zeroblob(1048576) WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")]).await,
                "inflated" => execute_for_test(&projection,"UPDATE q7_bodies SET value=zeroblob(8388608) WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")]).await,
                "missing" => execute_for_test(&projection,"DELETE FROM q7_bodies WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")]).await,
                _ => {},
            }
            let owned_projection = projection.clone();
            let owned_job = job.clone();
            let result = tokio::task::spawn_blocking(move || {
                crate::tx::block_on_owned_apply(async {
                    let writer = owned_projection.writer.lock().await;
                    let mut charges = Vec::new();
                    let output = super::super::group_authority::load_group(
                        writer.as_ref().unwrap(),
                        1,
                        definition.schema(),
                        super::super::group_authority::GroupRead {
                            group: &declared,
                            states: Some(&source),
                            sequence: 2,
                        },
                        &owned_job,
                        &mut charges,
                    )
                    .await;
                    (output, charges)
                })
            })
            .await
            .unwrap();
            if case == "valid" {
                let output = result.0.as_ref().unwrap();
                assert!(output.work["a"].body_loaded);
                assert_eq!(output.work["a"].record.work.body.as_ref(), vec![9; 1 << 20]);
            } else {
                assert!(matches!(&result.0, Err(QueueError::OutcomeUnknown(_))), "{case}");
                if case == "inflated" {
                    assert!(matches!(&result.0, Err(QueueError::OutcomeUnknown(reason))
                        if reason.contains("cached work body length differs from source")),
                        "inflation must fail at source metadata before body admission");
                }
            }
            drop(result);
            drop(proof);
            drop(history);
            drop(seed);
            drop(genesis);
            drop(projection);
            drop(job);
            assert_eq!(admission.usage().unwrap(), (0, 0), "{case}");
        }
    }
}
