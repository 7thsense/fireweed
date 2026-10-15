use super::tests::{Reader, group, record};
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_group_state_authority::WorkMetadata,
    queue_log_record::{OwnedQueueLogRecord, encode_checked_genesis},
    queue_planner::RecordSnapshot,
    queue_publication::{BodyCommandRef, EffectRef},
    queue_schema_codec::encode_definition,
    queue_source_index::{QueueSourcePayload, QueueSourceSpan},
};
use fireweed_core::queue_contract::*;

pub(super) struct Fixture {
    pub(super) admission: Arc<QueueAdmission>,
    pub(super) job: Arc<QueueReservation>,
    pub(super) schema: QueueSchema,
    pub(super) definition: AdmittedQueueDefinition,
    pub(super) queue: QueueAddress,
    pub(super) reader: Reader,
    pub(super) snapshot: RecordSnapshot,
    _genesis: OwnedQueueLogRecord,
    _seed: OwnedQueueLogRecord,
}
pub(super) fn fixture(raw: Lifecycle) -> Fixture {
    let admission = QueueAdmission::new(2, 16 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let empty = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: empty.clone(),
        state: std::collections::BTreeMap::from([("s".into(), empty)]),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
    let queue = QueueAddress {
        tenant: "t".into(),
        queue: "q".into(),
    };
    let genesis = encode_checked_genesis(1, &queue, &definition, &[7; 32], 1 << 20, &job).unwrap();
    let checked = genesis.checked().unwrap();
    let genesis_fact = QueueSourceGenesis {
        source: QueueSourceSpan::from_checked(
            &checked,
            0,
            &genesis.bytes()[checked.payload_interval().unwrap()],
        )
        .unwrap(),
        definition_pin: definition.pin(),
    };
    let mut seed = group("seed-aggregate");
    for key in ["a\0α", "b\0β"] {
        seed.work_inserts.push(NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: vec![1].into(),
        });
    }
    let (seed_record, mut snapshot) =
        record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
    let checked_seed = seed_record.checked().unwrap();
    let QueueLogPayload::Publication(publication) = checked_seed.view().payload() else {
        panic!()
    };
    let mut facts = std::collections::BTreeMap::from([
        (
            QueueSourceKey::Genesis.encode().unwrap(),
            queue_storage_layout::encode(&genesis_fact).unwrap(),
        ),
        (
            QueueSourceKey::Sequence.encode().unwrap(),
            queue_storage_layout::encode(&snapshot.next_sequence).unwrap(),
        ),
    ]);
    for lifecycle in [
        Lifecycle::Pending,
        Lifecycle::Leased,
        Lifecycle::Completed,
        Lifecycle::Failed,
    ] {
        facts.insert(
            QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap(),
            queue_storage_layout::encode(&if lifecycle == raw { 2u64 } else { 0 }).unwrap(),
        );
    }
    for command in publication.commands() {
        let command = command.unwrap();
        let EffectRef::WorkPut {
            body: BodyCommandRef::Replace(body),
            ..
        } = command.effect
        else {
            panic!()
        };
        let row = snapshot.work.get_mut(command.identity.key).unwrap();
        row.record.lifecycle = raw;
        // Expired leased work remains raw Leased until a publication changes it.
        row.record.lease_expires_at = (raw == Lifecycle::Leased).then_some(-1);
        row.lease_hash = (raw == Lifecycle::Leased).then_some([3; 32]);
        let metadata = WorkMetadata {
            version: row.record.version,
            priority: row.record.work.priority,
            available_at: row.record.work.available_at,
            insertion_sequence: row.record.insertion_sequence,
            lifecycle: raw,
            eligible_since: row.record.eligible_since,
            attempts: row.record.attempts,
            lease_expires_at: row.record.lease_expires_at,
            lease_hash: row.lease_hash,
        };
        let fact = QueueSourceRecord {
            highwater: 1,
            field_count: 0,
            live: Some(QueueSourceLive::Work {
                metadata,
                body: QueueSourcePayload::from_checked(&checked_seed, 1, body).unwrap(),
            }),
        };
        facts.insert(
            QueueSourceKey::Record {
                collection: None,
                key: command.identity.key,
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&fact).unwrap(),
        );
    }
    Fixture {
        admission,
        reader: Reader {
            through: 2,
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            facts,
        },
        job,
        schema,
        definition,
        queue,
        snapshot,
        _genesis: genesis,
        _seed: seed_record,
    }
}
pub(super) fn request(kind: &str) -> CommitGroup {
    let mut request = group(kind);
    let change = match kind {
        "delete" => Some(UnclaimedChange::Delete),
        "preserve" => Some(UnclaimedChange::Patch(WorkPatch {
            priority: Some(7),
            ..Default::default()
        })),
        "replace" => Some(UnclaimedChange::Replace(NewWork {
            key: "a\0α".into(),
            priority: 3,
            available_at: 0,
            fields: Default::default(),
            body: vec![9].into(),
        })),
        "complete" | "fail" | "pending" => Some(UnclaimedChange::Administrative {
            patch: Default::default(),
            lifecycle: match kind {
                "complete" => AdministrativeLifecycle::Completed,
                "fail" => AdministrativeLifecycle::Failed,
                _ => AdministrativeLifecycle::Pending,
            },
            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
        }),
        "create" => {
            request.work_inserts.push(NewWork {
                key: "c\0γ".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            });
            None
        }
        "state" => {
            request.state_updates.push(StateUpdate {
                collection: "s".into(),
                key: "state".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Default::default(),
                    value: Default::default(),
                },
            });
            None
        }
        "rejected" => {
            request.work_updates.push(UnclaimedUpdate {
                key: "a\0α".into(),
                expect: StateExpect::Version(999),
                change: UnclaimedChange::Delete,
            });
            None
        }
        _ => panic!(),
    };
    if let Some(change) = change {
        request.work_updates.push(UnclaimedUpdate {
            key: "a\0α".into(),
            expect: StateExpect::Version(1),
            change,
        });
    }
    request
}
