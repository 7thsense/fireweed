use super::*;

pub(super) struct Harness {
    pub(super) admission: Arc<crate::queue_admission::QueueAdmission>,
    pub(super) job: Arc<QueueReservation>,
    pub(super) queue: QueueAddress,
    pub(super) schema: QueueSchema,
    pub(super) definition: AdmittedQueueDefinition,
    pub(super) history: Vec<OwnedQueueLogRecord>,
    pub(super) snapshot: RecordSnapshot,
    pub(super) reader: Reader,
}
impl Harness {
    pub(super) fn new(schema: QueueSchema) -> Self {
        let admission = crate::queue_admission::QueueAdmission::new(2, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let definition = AdmittedQueueDefinition::from_raw(
            &crate::queue_schema_codec::encode_definition(&schema).unwrap(),
            &job,
        )
        .unwrap();
        let genesis = crate::queue_log_record::encode_checked_genesis(
            1,
            &queue,
            &definition,
            &[7; 32],
            1 << 20,
            &job,
        )
        .unwrap();
        let identity = Arc::new(job.owned_buffer(128).unwrap());
        let cut = QueueSourceCut::from_captured(
            &identity,
            crate::queue_namespace::AddressRef {
                tenant: "t",
                queue: "q",
            },
            1,
            0,
            &job,
        )
        .unwrap();
        let checked = genesis.checked().unwrap();
        let compiled = compile_genesis(
            &checked,
            cut,
            &definition,
            QueueSourceCompileLimits {
                edits: 6,
                input_bytes: 1 << 20,
            },
            &job,
        )
        .unwrap();
        let facts = compiled
            .edits()
            .map(|(key, value)| (key.to_vec(), value.unwrap().to_vec()))
            .collect();
        assert_eq!(compiled.edits().len(), 6);
        drop(compiled);
        let snapshot = RecordSnapshot::default();
        let history = vec![genesis];
        assert_eq!(facts, oracle::fold(&history, &snapshot, &schema));
        let reader = Reader {
            through: 1,
            identity,
            facts,
        };
        Self {
            admission,
            job,
            queue,
            schema,
            definition,
            history,
            snapshot,
            reader,
        }
    }
    pub(super) async fn accept(&mut self, proposed: OwnedQueueLogRecord, after: RecordSnapshot) {
        let checked = proposed.checked().unwrap();
        let baseline = self.admission.usage().unwrap();
        let compiled = compile_publication(
            &self.reader,
            &checked,
            &self.definition,
            QueueSourceCompileLimits {
                edits: 10000,
                input_bytes: 1 << 20,
            },
            &self.job,
        )
        .await
        .unwrap();
        compiled
            .verify(&self.reader.cut().unwrap(), &self.job)
            .unwrap();
        let mut actual = self.reader.facts.clone();
        for (key, value) in compiled.edits() {
            if let Some(value) = value {
                actual.insert(key.to_vec(), value.to_vec());
            } else {
                assert!(actual.remove(key).is_some());
            }
        }
        drop(compiled);
        assert_eq!(self.admission.usage().unwrap(), baseline);
        self.history.push(proposed);
        self.snapshot = after;
        assert_eq!(
            actual,
            oracle::fold(&self.history, &self.snapshot, &self.schema)
        );
        self.reader = Reader {
            through: self.history.len() as u64,
            identity: self.reader.identity.clone(),
            facts: actual,
        };
    }
    pub(super) async fn apply(&mut self, proposal: &CommitGroup) {
        let (proposed, after) = record(
            &self.queue,
            &self.schema,
            &self.snapshot,
            proposal,
            &self.job,
        );
        let checked = proposed.checked().unwrap();
        let QueueLogPayload::Publication(publication) = checked.view().payload() else {
            panic!()
        };
        let outcome = publication.receipts().next().unwrap().unwrap().outcome;
        assert!(
            matches!(
                outcome,
                crate::queue_publication::ReceiptOutcomeRef::Commit(
                    crate::queue_publication::CommitOutcomeRef::Committed(_)
                )
            ),
            "{}: {outcome:?}",
            proposal.request_id
        );
        self.accept(proposed, after).await;
    }
    pub(super) async fn pack(&mut self, proposals: &[CommitGroup]) {
        let mut after = self.snapshot.clone();
        let mut deltas = Vec::new();
        for proposal in proposals {
            after.unique_probes_loaded.extend(
                crate::queue_planner::required_unique_probes(proposal, &after, 0, &self.schema)
                    .unwrap(),
            );
            let delta =
                crate::queue_planner::plan_commit_group(proposal, &mut after, 0, &self.schema)
                    .unwrap();
            assert!(
                matches!(delta.receipt.outcome, GroupOutcome::Committed { .. }),
                "{}",
                proposal.request_id
            );
            deltas.push(delta);
        }
        let mut ordered: Vec<_> = proposals.iter().zip(&deltas).collect();
        ordered.sort_by_key(|(proposal, _)| proposal.request_id.as_str());
        let receipts = ordered
            .into_iter()
            .map(|(proposal, delta)| {
                crate::queue_publication::PublicationReceipt::commit(
                    &self.queue,
                    proposal,
                    &delta.receipt,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let canonical =
            crate::queue_planner::combine_deltas(&deltas, self.snapshot.next_sequence).unwrap();
        let frame = crate::queue_publication::encode_publication_pinned(
            &self.queue,
            self.definition.pin(),
            &self.snapshot,
            &canonical,
            &receipts,
            1 << 20,
            &self.job,
        )
        .unwrap();
        let proposed = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &self.job,
        )
        .unwrap();
        drop(frame);
        drop(receipts);
        self.accept(proposed, after).await;
    }
}

pub(super) fn mixed_schema() -> QueueSchema {
    let collection = CollectionDefinition {
        fields: std::collections::BTreeMap::from([
            ("u".into(), FieldType::Integer),
            ("v".into(), FieldType::Float),
        ]),
        indexes: std::collections::BTreeMap::from([
            ("u".into(), vec!["u".into()]),
            ("vu".into(), vec!["v".into(), "u".into()]),
        ]),
        unique_indexes: std::collections::BTreeSet::from(["u".into()]),
        additional_field_prefixes: Default::default(),
    };
    QueueSchema {
        work: collection.clone(),
        state: std::collections::BTreeMap::from([("s\0α".into(), collection)]),
        progress_bound_nanos: 100,
        max_attempts: 3,
    }
}
pub(super) fn put_state(
    id: &str,
    key: &str,
    expect: StateExpect,
    value: i64,
    float: f64,
) -> CommitGroup {
    let mut proposal = group(id);
    proposal.state_updates.push(StateUpdate {
        collection: "s\0α".into(),
        key: key.into(),
        expect,
        change: StateChange::Put {
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(value)),
                ("v".into(), FieldValue::Float(float)),
            ]),
            value: vec![0, value as u8].into(),
        },
    });
    proposal
}
