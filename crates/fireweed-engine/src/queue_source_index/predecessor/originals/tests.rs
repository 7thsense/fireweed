use super::super::tests::{Reader, group};
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::{
        OwnedQueueLogRecord, encode_checked_genesis, encode_checked_publication_record,
    },
    queue_original_authority::ColdOriginalTracker,
    queue_planner::{RecordSnapshot, combine_deltas},
    queue_publication::{PublicationReceipt, encode_publication_pinned},
    queue_recovery::ColdQueueHistory,
    queue_schema_codec::encode_definition,
    queue_source_index::{QueueSourceGenesis, QueueSourceSpan},
};
use fireweed_core::queue_contract::*;
use std::collections::BTreeMap;
struct Fixture {
    admission: Arc<QueueAdmission>,
    job: Arc<QueueReservation>,
    queue: QueueAddress,
    definition: AdmittedQueueDefinition,
    reader: Reader,
    genesis: OwnedQueueLogRecord,
}
fn fixture() -> Fixture {
    let admission = QueueAdmission::new(2, 16 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let empty = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: empty,
        state: Default::default(),
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
    let fact = QueueSourceGenesis {
        source: QueueSourceSpan::from_checked(
            &checked,
            0,
            &genesis.bytes()[checked.payload_interval().unwrap()],
        )
        .unwrap(),
        definition_pin: definition.pin(),
    };
    let facts = BTreeMap::from([
        (
            QueueSourceKey::Genesis.encode().unwrap(),
            queue_storage_layout::encode(&fact).unwrap(),
        ),
        (
            QueueSourceKey::Sequence.encode().unwrap(),
            queue_storage_layout::encode(&0u64).unwrap(),
        ),
    ]);
    Fixture {
        admission,
        reader: Reader {
            through: 1,
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            facts,
        },
        job,
        queue,
        definition,
        genesis,
    }
}
fn ordinary(f: &Fixture, identities: &[(u8, &str)]) -> OwnedQueueLogRecord {
    let inserts: Vec<_> = identities
        .iter()
        .map(|(_, id)| InsertRequest {
            queue: f.queue.clone(),
            request_id: (*id).into(),
            mode: InsertMode::InsertOnly,
            items: vec![NewWork {
                key: "missing".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            }],
        })
        .collect();
    let insert_outcomes: Vec<_> = identities
        .iter()
        .map(|_| {
            vec![InsertOutcome::Rejected {
                key: "missing".into(),
                reason: Rejection::UniqueConflict,
            }]
        })
        .collect();
    let groups: Vec<_> = identities.iter().map(|(_, id)| group(id)).collect();
    let commits: Vec<_> = identities
        .iter()
        .map(|(_, id)| CommitReceipt {
            request_id: (*id).into(),
            outcome: GroupOutcome::Rejected(Rejection::NotFound),
        })
        .collect();
    let receipts: Vec<_> = identities
        .iter()
        .enumerate()
        .map(|(index, (domain, _))| match domain {
            0 => PublicationReceipt::insert(&inserts[index], &insert_outcomes[index]).unwrap(),
            2 => PublicationReceipt::commit(&f.queue, &groups[index], &commits[index]).unwrap(),
            _ => panic!(),
        })
        .collect();
    let frame = encode_publication_pinned(
        &f.queue,
        f.definition.pin(),
        &RecordSnapshot::default(),
        &combine_deltas(&[], 0).unwrap(),
        &receipts,
        1 << 20,
        &f.job,
    )
    .unwrap();
    encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &f.job).unwrap()
}
fn claim(f: &Fixture, id: &str) -> OwnedQueueLogRecord {
    let commands = encode_publication_pinned(
        &f.queue,
        f.definition.pin(),
        &RecordSnapshot::default(),
        &combine_deltas(&[], 0).unwrap(),
        &[],
        1 << 20,
        &f.job,
    )
    .unwrap();
    let request = ClaimRequest {
        queue: f.queue.clone(),
        request_id: id.into(),
        filter: Default::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 2,
        max_bytes: 1024,
        fields: None,
        include_body: true,
    };
    let receipt = crate::queue_claim_receipt::encode_claim_receipt(&[], 1 << 20, &f.job).unwrap();
    let wrapper = crate::queue_claim_publication::encode_claim_publication(
        crate::queue_claim_publication::ClaimPublicationInput {
            request: &request,
            definition: &f.definition,
            commands: commands.as_bytes(),
            receipt: &receipt,
            claimed_at: 0,
            limits: &QueueLimits::default(),
        },
        1 << 20,
        &f.job,
    )
    .unwrap();
    crate::queue_log_record::encode_checked_claim_record(1, &wrapper, 1 << 20, &f.job).unwrap()
}
fn administrative(f: &Fixture, id: &str, rejected: bool) -> OwnedQueueLogRecord {
    use crate::queue_publication::{
        AdministrativePublicationBuilder, AdministrativePublicationReceipt,
        AdministrativeReceiptOutcome,
    };
    use fireweed_core::queue_administration::*;
    let group = group(id);
    let intent = AdministrativeIntent {
        evaluated_at: 0,
        operation: AdministrativeOperation::SelectFirst(vec![]),
        before_fields: None,
    };
    let result = AdministrativeResult {
        histogram: Default::default(),
        items: vec![],
        clauses: vec![],
    };
    let rejection = Rejection::Invalid("administrative rejection".into());
    let outcome = if rejected {
        AdministrativeReceiptOutcome::Rejected(&rejection)
    } else {
        AdministrativeReceiptOutcome::Committed {
            versions: &[],
            administration: &result,
        }
    };
    let receipt =
        AdministrativePublicationReceipt::new(&f.queue, &group, &intent, 1 << 20, &outcome, &f.job)
            .unwrap();
    let frame = AdministrativePublicationBuilder::new(
        &receipt,
        f.definition.pin(),
        &RecordSnapshot::default(),
        &combine_deltas(&[], 0).unwrap(),
        0,
        0,
        1 << 20,
        &f.job,
    )
    .unwrap()
    .finish()
    .unwrap();
    encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &f.job).unwrap()
}
fn limits() -> QueueSourceCompileLimits {
    QueueSourceCompileLimits {
        edits: 100,
        input_bytes: 1 << 20,
    }
}
fn cold_descriptor(
    f: &Fixture,
    proposed: &OwnedQueueLogRecord,
    domain: u8,
    id: &str,
) -> crate::queue_original_authority::OriginalDescriptor {
    let mut tracker = ColdOriginalTracker::new_request(&f.queue, domain, id, &f.job).unwrap();
    let mut history = ColdQueueHistory::new(&f.queue, &f.job).unwrap();
    for (offset, record) in [&f.genesis, proposed].into_iter().enumerate() {
        history.observe(offset as u64, record.bytes()).unwrap();
        tracker.observe(offset as u64, record).unwrap();
    }
    tracker
        .finish(&history, &f.genesis, (2, 0, 1))
        .unwrap()
        .descriptor(0, id)
        .unwrap()
        .unwrap()
}
#[test]
fn every_original_matches_independent_cold_descriptors_in_all_domains() {
    futures::executor::block_on(async {
        let f = fixture();
        let id = "same\0α";
        for (proposed, identities, format) in [
            (
                ordinary(&f, &[(0, id), (2, id), (2, "z\0β")]),
                vec![(0, id), (2, id), (2, "z\0β")],
                3,
            ),
            (claim(&f, id), vec![(1, id)], 3),
            (administrative(&f, id, false), vec![(2, id)], 4),
            (administrative(&f, id, true), vec![(2, id)], 4),
        ] {
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let output = context.originals(limits()).await.unwrap();
            output.verify(&proposed, &context.cut, &f.job).unwrap();
            assert_eq!(output.edits().len(), identities.len());
            for (domain, id) in identities {
                let key = QueueSourceKey::Original {
                    domain,
                    request_id: id,
                }
                .encode()
                .unwrap();
                let value = output
                    .edits()
                    .find(|(actual, _)| *actual == key)
                    .unwrap()
                    .1
                    .unwrap();
                let fact: QueueSourceOriginal = postcard::from_bytes(value).unwrap();
                fact.validate(domain).unwrap();
                assert_eq!(fact.fingerprint_format, format);
                assert_eq!(
                    fact.descriptor(),
                    cold_descriptor(&f, &proposed, domain, id)
                );
                assert_eq!(fact.outcome.span().log_offset(), 1);
                let raw = fact.outcome.span().resolve(&checked, 1).unwrap();
                fact.outcome.verify_cached(raw).unwrap();
                let mut corrupted = raw.to_vec();
                corrupted.push(1);
                assert!(fact.outcome.verify_cached(&corrupted).is_err());
            }
            drop(output);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn ordinary_blank_identity_is_rejected_and_zero_receipts_are_complete() {
    futures::executor::block_on(async {
        let f = fixture();
        for id in ["", " \t\n"] {
            let proposed = ordinary(&f, &[(2, id)]);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            assert!(
                matches!(context.originals(limits()).await,Err(QueueError::OutcomeUnknown(message)) if message.contains("blank original"))
            );
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        let proposed = ordinary(&f, &[]);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        assert_eq!(
            context
                .originals(QueueSourceCompileLimits {
                    edits: 0,
                    input_bytes: 0
                })
                .await
                .unwrap()
                .edits()
                .len(),
            0
        );
    });
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Source,
    Cut,
}
struct FaultReader<'a> {
    base: &'a Reader,
    selected: Vec<u8>,
    fault: Fault,
    foreign: Arc<OwnedQueueBufferCharge>,
    reads: std::sync::atomic::AtomicUsize,
}
impl QueueSourceReader for FaultReader<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.base.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        if key.starts_with(&[1, 7]) {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if key == self.selected && matches!(self.fault, Fault::Source) {
            return Err(unknown("injected late original source failure"));
        }
        self.base.lookup(key).await
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<crate::queue_source_index::QueueSourcePoint<'a>> {
        if owner == &self.selected && matches!(self.fault, Fault::Cut) {
            let cut = QueueSourceCut::from_captured(
                &self.foreign,
                self.cut()?.address(),
                1,
                self.cut()?.through(),
                self.foreign.reservation(),
            )?;
            return Ok(cut.point_from_source(owner, self.base.facts.get(owner).map(Vec::as_slice)));
        }
        self.base.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.base.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(
            crate::queue_source_index::QueueSourcePoint<'a>,
        ) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.base.visit_range(owner, lower, upper, visitor)
    }
}
fn fault_reader(f: &Fixture, selected: Vec<u8>, fault: Fault) -> FaultReader<'_> {
    FaultReader {
        base: &f.reader,
        selected,
        fault,
        foreign: Arc::new(f.job.owned_buffer(128).unwrap()),
        reads: std::sync::atomic::AtomicUsize::new(0),
    }
}
#[test]
fn late_absence_lookup_error_or_wrong_cut_discards_every_prior_original() {
    futures::executor::block_on(async {
        let f = fixture();
        let proposed = ordinary(&f, &[(2, "a\0α"), (2, "b\0β")]);
        let checked = proposed.checked().unwrap();
        for fault in [Fault::Source, Fault::Cut] {
            let reader = fault_reader(
                &f,
                QueueSourceKey::Original {
                    domain: 2,
                    request_id: "b\0β",
                }
                .encode()
                .unwrap(),
                fault,
            );
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            assert!(matches!(
                context.originals(limits()).await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert_eq!(reader.reads.load(std::sync::atomic::Ordering::SeqCst), 2);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn any_present_original_including_identical_bytes_rejects_without_cross_domain_aliasing() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let proposed = ordinary(&f, &[(2, "a\0α"), (2, "b\0β")]);
        let checked = proposed.checked().unwrap();
        let retained = {
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let output = context.originals(limits()).await.unwrap();
            output.edits().nth(1).unwrap().1.unwrap().to_vec()
        };
        let selected = QueueSourceKey::Original {
            domain: 2,
            request_id: "b\0β",
        }
        .encode()
        .unwrap();
        f.reader.through = 2;
        for value in [retained, Vec::new(), vec![255; 32]] {
            f.reader.facts.insert(selected.clone(), value);
            let reader = fault_reader(&f, selected.clone(), Fault::None);
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            assert!(
                matches!(context.originals(limits()).await,Err(QueueError::OutcomeUnknown(message)) if message.contains("duplicate source original identity"))
            );
            assert_eq!(reader.reads.load(std::sync::atomic::Ordering::SeqCst), 2);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        let mut tracker = ColdOriginalTracker::new_request(&f.queue, 2, "b\0β", &f.job).unwrap();
        tracker.observe(0, &f.genesis).unwrap();
        tracker.observe(1, &proposed).unwrap();
        assert!(
            matches!(tracker.observe(2,&proposed),Err(QueueError::OutcomeUnknown(message)) if message.contains("duplicate source original identity"))
        );
        let mut f = fixture();
        let id = "same\0α";
        f.reader.facts.insert(
            QueueSourceKey::Original {
                domain: 0,
                request_id: id,
            }
            .encode()
            .unwrap(),
            vec![255],
        );
        for proposed in [ordinary(&f, &[(2, id)]), claim(&f, id)] {
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            assert_eq!(context.originals(limits()).await.unwrap().edits().len(), 1);
        }
        let proposed = ordinary(&f, &[(0, id)]);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        assert!(context.originals(limits()).await.is_err());
    });
}
#[test]
fn original_wire_limits_scope_and_late_root_admission_are_exact() {
    futures::executor::block_on(async {
        let f = fixture();
        let ids = ["a\0α".repeat(2048), "b\0β".repeat(2048)];
        let proposed = ordinary(&f, &[(2, &ids[0]), (2, &ids[1])]);
        let checked = proposed.checked().unwrap();
        let reader = fault_reader(&f, Vec::new(), Fault::None);
        let context =
            QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let output = context.originals(limits()).await.unwrap();
        let exact = output
            .edits()
            .map(|(key, value)| (key.len() + value.unwrap().len()) as u64)
            .sum::<u64>();
        let twin = ordinary(&f, &[(2, &ids[0]), (2, &ids[1])]);
        assert_eq!(twin.bytes(), proposed.bytes());
        assert!(output.verify(&twin, &context.cut, &f.job).is_err());
        let foreign = f.admission.reserve(0).unwrap();
        assert!(output.verify(&proposed, &context.cut, &foreign).is_err());
        drop(foreign);
        drop(twin);
        drop(output);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        for (limits, success) in [
            (
                QueueSourceCompileLimits {
                    edits: 2,
                    input_bytes: exact,
                },
                true,
            ),
            (
                QueueSourceCompileLimits {
                    edits: 1,
                    input_bytes: exact,
                },
                false,
            ),
            (
                QueueSourceCompileLimits {
                    edits: 2,
                    input_bytes: exact - 1,
                },
                false,
            ),
        ] {
            let result = context.originals(limits).await;
            assert_eq!(result.is_ok(), success);
            drop(result);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        async fn probe(
            f: &Fixture,
            context: &QueueSourcePublicationContext<'_, FaultReader<'_>>,
            available: u64,
        ) -> (Option<String>, usize) {
            let baseline = f.admission.usage().unwrap();
            let filler = f
                .job
                .owned_buffer((16 << 20) - baseline.1 - available)
                .unwrap();
            context
                .reader
                .reads
                .store(0, std::sync::atomic::Ordering::SeqCst);
            let error = match context.originals(limits()).await {
                Ok(output) => {
                    drop(output);
                    None
                }
                Err(error) => Some(format!("{error:?}")),
            };
            let reads = context
                .reader
                .reads
                .load(std::sync::atomic::Ordering::SeqCst);
            drop(filler);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            (error, reads)
        }
        let (mut lower, mut upper) = (0, 1 << 20);
        assert!(probe(&f, &context, lower).await.0.is_some());
        assert!(probe(&f, &context, upper).await.0.is_none());
        while upper - lower > 1 {
            let middle = lower + (upper - lower) / 2;
            if probe(&f, &context, middle).await.0.is_none() {
                upper = middle;
            } else {
                lower = middle;
            }
        }
        assert!(probe(&f, &context, upper).await.0.is_none());
        let (error, reads) = probe(&f, &context, upper - 1).await;
        assert_eq!(reads, 2);
        assert!(error.unwrap().contains("original output admission"));
    });
}

#[test]
fn two_lease_claim_retains_complete_receipt_in_priority_order_as_one_original() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let mut seed = group("seed-two-lease-claim");
        for (key, priority) in [("a\0α", 9), ("z\0β", -9)] {
            seed.work_inserts.push(NewWork {
                key: key.into(),
                priority,
                available_at: 0,
                fields: Default::default(),
                body: vec![5; 64].into(),
            });
        }
        let (seed_record, snapshot) = super::super::tests::record(
            &f.queue,
            f.definition.schema(),
            &RecordSnapshot::default(),
            &seed,
            &f.job,
        );
        f.reader.through = 2;
        f.reader.facts.insert(
            QueueSourceKey::Sequence.encode().unwrap(),
            queue_storage_layout::encode(&snapshot.next_sequence).unwrap(),
        );
        let mut work = Vec::new();
        let mut leases = Vec::new();
        for (key, token) in [("z\0β", vec![17; 32]), ("a\0α", vec![29; 32])] {
            let claimed = crate::queue_claim::plan_claim(
                snapshot.work[key].clone(),
                token,
                crate::queue_claim::ClaimContext {
                    now: 10,
                    eligible_at: 0,
                    expires_at: 1_000_010,
                    insertion_head: 2,
                },
            )
            .unwrap();
            leases.push(crate::queue_claim_receipt::ClaimLease {
                key: key.into(),
                token: claimed.lease.token.clone(),
                version: claimed.work.record.version,
            });
            work.push(claimed.work);
        }
        let versions: Vec<_> = snapshot
            .work
            .keys()
            .map(|key| RecordVersion {
                collection: None,
                key: key.clone(),
                version: 2,
            })
            .collect();
        let delta = crate::queue_planner::CommitDelta {
            work,
            work_fields: snapshot
                .work
                .keys()
                .map(|key| (key.clone(), crate::queue_fields::ResolvedFields::Preserve))
                .collect(),
            work_bodies: snapshot
                .work
                .keys()
                .map(|key| (key.clone(), crate::queue_body::ResolvedBody::Preserve))
                .collect(),
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: versions.clone(),
            next_sequence: 2,
            receipt: CommitReceipt {
                request_id: String::new(),
                outcome: GroupOutcome::Committed { versions },
            },
        };
        let canonical = combine_deltas(&[delta], snapshot.next_sequence).unwrap();
        let commands = encode_publication_pinned(
            &f.queue,
            f.definition.pin(),
            &snapshot,
            &canonical,
            &[],
            1 << 20,
            &f.job,
        )
        .unwrap();
        let view = crate::queue_publication::view_publication(commands.as_bytes()).unwrap();
        assert_eq!(
            view.commands()
                .map(|command| command.unwrap().identity.key)
                .collect::<Vec<_>>(),
            ["a\0α", "z\0β"]
        );
        let request = ClaimRequest {
            queue: f.queue.clone(),
            request_id: "two\0leases".into(),
            filter: Default::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 1 << 20,
            fields: None,
            include_body: true,
        };
        let receipt =
            crate::queue_claim_receipt::encode_claim_receipt(&leases, 1 << 20, &f.job).unwrap();
        let wrapper = crate::queue_claim_publication::encode_claim_publication(
            crate::queue_claim_publication::ClaimPublicationInput {
                request: &request,
                definition: &f.definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: 10,
                limits: &QueueLimits::default(),
            },
            1 << 20,
            &f.job,
        )
        .unwrap();
        let proposed =
            crate::queue_log_record::encode_checked_claim_record(1, &wrapper, 1 << 20, &f.job)
                .unwrap();
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let output = context.originals(limits()).await.unwrap();
        assert_eq!(output.edits().len(), 1);
        let (key, value) = output.edits().next().unwrap();
        assert_eq!(
            key,
            QueueSourceKey::Original {
                domain: 1,
                request_id: &request.request_id
            }
            .encode()
            .unwrap()
        );
        let fact: QueueSourceOriginal = postcard::from_bytes(value.unwrap()).unwrap();
        let QueueLogPayload::Claim(claim) = checked.view().payload() else {
            panic!()
        };
        assert_eq!(fact.outcome.span().log_offset(), 2);
        assert_eq!(
            fact.outcome.span().resolve(&checked, 2).unwrap(),
            claim.receipt_bytes()
        );
        assert_eq!(
            claim
                .receipt()
                .entries()
                .map(|entry| entry.unwrap().key)
                .collect::<Vec<_>>(),
            ["z\0β", "a\0α"]
        );
        assert_eq!(claim.receipt().len(), 2);
        let descriptor = {
            let mut history = ColdQueueHistory::new(&f.queue, &f.job).unwrap();
            let mut tracker =
                ColdOriginalTracker::new_request(&f.queue, 1, &request.request_id, &f.job).unwrap();
            for (offset, record) in [&f.genesis, &seed_record, &proposed]
                .into_iter()
                .enumerate()
            {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            tracker
                .finish(&history, &f.genesis, (3, 2, 1))
                .unwrap()
                .descriptor(0, &request.request_id)
                .unwrap()
                .unwrap()
        };
        assert_eq!(fact.descriptor(), descriptor);
        assert_eq!(descriptor.outcome_bytes, claim.receipt_bytes().len() as u64);
        fact.outcome.verify_cached(claim.receipt_bytes()).unwrap();
        drop(output);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
