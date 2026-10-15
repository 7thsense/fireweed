use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::{OwnedQueueLogRecord, encode_checked_publication_record},
    queue_planner::{RecordSnapshot, combine_deltas},
    queue_publication::{PublicationReceipt, encode_publication_pinned},
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    queue_source_index::{
        QueueSourcePayload,
        predecessor::tests::{Reader, group},
    },
    queue_storage_layout::encode,
};
use fireweed_core::queue_contract::*;

fn fixture() -> (
    Arc<QueueReservation>,
    AdmittedQueueDefinition,
    Reader,
    OwnedQueueLogRecord,
) {
    let admission = QueueAdmission::new(2, 4 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let schema = QueueSchema {
        work: CollectionDefinition {
            fields: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        },
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
    let groups = [group("a\0α"), group("b\0β")];
    let receipts = groups.each_ref().map(|g| CommitReceipt {
        request_id: g.request_id.clone(),
        outcome: GroupOutcome::Rejected(Rejection::NotFound),
    });
    let bindings: Vec<_> = groups
        .iter()
        .zip(&receipts)
        .map(|(g, r)| PublicationReceipt::commit(&queue, g, r).unwrap())
        .collect();
    let encoded_frame = encode_publication_pinned(
        &queue,
        definition.pin(),
        &RecordSnapshot::default(),
        &combine_deltas(&[], 0).unwrap(),
        &bindings,
        1 << 20,
        &job,
    )
    .unwrap();
    let record =
        encode_checked_publication_record(1, encoded_frame.as_bytes(), 1 << 20, &job).unwrap();
    let checked = record.checked().unwrap();
    let QueueLogPayload::Publication(frame) = checked.view().payload() else {
        panic!()
    };
    let facts = frame
        .receipts()
        .map(|r| {
            let r = r.unwrap();
            let fact = QueueSourceOriginal {
                fingerprint_format: r.fingerprint_format,
                fingerprint: *r.fingerprint,
                outcome: QueueSourcePayload::from_checked(&checked, 1, r.encoded_outcome())
                    .unwrap(),
            };
            (
                QueueSourceKey::Original {
                    domain: r.domain,
                    request_id: r.request_id,
                }
                .encode()
                .unwrap(),
                encode(&fact).unwrap(),
            )
        })
        .collect();
    let reader = Reader {
        through: 2,
        identity: Arc::new(job.owned_buffer(128).unwrap()),
        facts,
    };
    drop(encoded_frame);
    (job, definition, reader, record)
}
fn key(domain: u8, id: &str) -> Vec<u8> {
    QueueSourceKey::Original {
        domain,
        request_id: id,
    }
    .encode()
    .unwrap()
}

#[test]
fn source_original_retains_literal_domain_and_checked_historical_receipt() {
    futures::executor::block_on(
        source_original_retains_literal_domain_and_checked_historical_receipt_async(),
    );
}
async fn source_original_retains_literal_domain_and_checked_historical_receipt_async() {
    let (job, definition, reader, record) = fixture();
    for id in ["a\0α", "b\0β"] {
        let evidence = load_source_original(&reader, 2, id, &job).await.unwrap();
        assert_eq!(
            (
                evidence.domain(),
                evidence.request_id(),
                evidence.source_offset()
            ),
            (2, id, Some(1))
        );
        assert_eq!(evidence.full_key(), key(2, id));
        let fact: QueueSourceOriginal =
            crate::queue_storage_layout::decode(&reader.facts[&key(2, id)]).unwrap();
        evidence
            .check_fingerprint(fact.fingerprint_format, &fact.fingerprint)
            .unwrap();
        assert_eq!(
            evidence.check_fingerprint(3, &[0; 32]),
            Err(QueueError::RequestConflict)
        );
        assert!(evidence.check_fingerprint(2, &fact.fingerprint).is_err());
        let outcome = evidence
            .verify_record(&record.checked().unwrap(), 1, definition.pin(), &job)
            .unwrap();
        fact.outcome.verify_cached(outcome).unwrap();
    }
    for (domain, id) in [(0, "a\0α"), (1, "a\0α"), (2, "a"), (2, "absent")] {
        let evidence = load_source_original(&reader, domain, id, &job)
            .await
            .unwrap();
        assert_eq!(evidence.source_offset(), None);
        evidence.check_fingerprint(3, &[0; 32]).unwrap();
        assert!(
            evidence
                .verify_record(&record.checked().unwrap(), 1, definition.pin(), &job)
                .is_err()
        );
    }
}

#[test]
fn original_outcome_cannot_substitute_another_equal_receipt_or_valid_subspan() {
    futures::executor::block_on(
        original_outcome_cannot_substitute_another_equal_receipt_or_valid_subspan_async(),
    );
}
async fn original_outcome_cannot_substitute_another_equal_receipt_or_valid_subspan_async() {
    for fault in [
        "other-equal",
        "subspan",
        "offset-zero",
        "future",
        "bad-format",
        "trailing",
    ] {
        let (job, definition, mut reader, record) = fixture();
        let checked = record.checked().unwrap();
        let QueueLogPayload::Publication(frame) = checked.view().payload() else {
            panic!()
        };
        let receipts: Vec<_> = frame.receipts().map(Result::unwrap).collect();
        assert_eq!(receipts[0].encoded_outcome(), receipts[1].encoded_outcome());
        let mut fact: QueueSourceOriginal =
            crate::queue_storage_layout::decode(&reader.facts[&key(2, "a\0α")]).unwrap();
        match fault {
            "other-equal" => {
                fact.outcome =
                    QueueSourcePayload::from_checked(&checked, 1, receipts[1].encoded_outcome())
                        .unwrap()
            }
            "subspan" => {
                fact.outcome = QueueSourcePayload::from_checked(
                    &checked,
                    1,
                    &receipts[0].encoded_outcome()[..1],
                )
                .unwrap()
            }
            "offset-zero" => {
                fact.outcome =
                    QueueSourcePayload::from_checked(&checked, 0, receipts[0].encoded_outcome())
                        .unwrap()
            }
            "future" => {
                fact.outcome =
                    QueueSourcePayload::from_checked(&checked, 2, receipts[0].encoded_outcome())
                        .unwrap()
            }
            "bad-format" => fact.fingerprint_format = 0,
            _ => (),
        }
        let mut raw = encode(&fact).unwrap();
        if fault == "trailing" {
            raw.push(0);
        }
        reader.facts.insert(key(2, "a\0α"), raw);
        let loaded = load_source_original(&reader, 2, "a\0α", &job).await;
        if matches!(fault, "other-equal" | "subspan") {
            let evidence = loaded.unwrap();
            // These spans and isolated digests are valid; only exact literal
            // receipt membership distinguishes the forged reference.
            fact.outcome
                .verify_cached(fact.outcome.span().resolve(&checked, 1).unwrap())
                .unwrap();
            assert!(
                matches!(evidence.verify_record(&checked,1,definition.pin(),&job),Err(QueueError::OutcomeUnknown(m)) if m.contains("exact matching receipt span"))
            );
        } else {
            assert!(
                matches!(
                    loaded.and_then(|e| e.check_fingerprint(3, &[0; 32])),
                    Err(QueueError::OutcomeUnknown(_))
                ),
                "{fault}"
            );
        }
    }
}

#[test]
fn original_scope_pin_offset_and_constructor_negatives_are_not_conflicts() {
    futures::executor::block_on(
        original_scope_pin_offset_and_constructor_negatives_are_not_conflicts_async(),
    );
}
async fn original_scope_pin_offset_and_constructor_negatives_are_not_conflicts_async() {
    let (job, definition, reader, record) = fixture();
    for (domain, id) in [(3, "a"), (2, ""), (2, " \t\n")] {
        assert!(matches!(
            load_source_original(&reader, domain, id, &job).await,
            Err(QueueError::Invalid(_))
        ));
    }
    let other_admission = QueueAdmission::new(1, 1 << 20).unwrap();
    let other = Arc::new(other_admission.reserve(0).unwrap());
    assert!(matches!(
        load_source_original(&reader, 2, "a\0α", &other).await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    let evidence = load_source_original(&reader, 2, "a\0α", &job)
        .await
        .unwrap();
    let checked = record.checked().unwrap();
    for (offset, pin, owner) in [
        (0, definition.pin(), &job),
        (2, definition.pin(), &job),
        (1, DefinitionPin::from_persisted_bytes(b"other"), &job),
        (1, definition.pin(), &other),
    ] {
        assert!(matches!(
            evidence.verify_record(&checked, offset, pin, owner),
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
    let future = encode_checked_publication_record(
        2,
        &record.bytes()[checked.payload_interval().unwrap()],
        1 << 20,
        &job,
    )
    .unwrap();
    assert!(matches!(
        evidence.verify_record(&future.checked().unwrap(), 1, definition.pin(), &job),
        Err(QueueError::OutcomeUnknown(_))
    ));
}

struct BoundReader {
    inner: Reader,
    foreign: Arc<OwnedQueueBufferCharge>,
    cuts: std::sync::atomic::AtomicUsize,
    lookups: std::sync::atomic::AtomicUsize,
    fault: &'static str,
    epoch: u64,
}
impl BoundReader {
    fn original_cut(&self, foreign: bool) -> QueueResult<QueueSourceCut<'_>> {
        QueueSourceCut::from_captured(
            if foreign {
                &self.foreign
            } else {
                &self.inner.identity
            },
            crate::queue_namespace::AddressRef {
                tenant: "t",
                queue: "q",
            },
            self.epoch,
            self.inner.through,
            self.inner.identity.reservation(),
        )
    }
}
impl QueueSourceReader for BoundReader {
    type Point = Vec<u8>;
    type Range = ();
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.cuts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.original_cut(false)
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Vec<u8>> {
        assert_eq!(self.cuts.load(std::sync::atomic::Ordering::SeqCst), 1);
        self.lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, key: &'a Vec<u8>) -> QueueResult<super::super::QueueSourcePoint<'a>> {
        Ok(self
            .original_cut(self.fault == "foreign")?
            .point_from_source(
                if self.fault == "key" { b"prefix" } else { key },
                self.inner.facts.get(key).map(Vec::as_slice),
            ))
    }
    async fn complete_range(&self, _lower: &[u8], _upper: &[u8]) -> QueueResult<()> {
        panic!("point loader must not scan")
    }
    fn visit_range(
        &self,
        _owner: &(),
        _lower: &[u8],
        _upper: &[u8],
        _visitor: &mut dyn for<'a> FnMut(super::super::QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        panic!("point loader must not scan")
    }
}

#[test]
fn original_lookup_keeps_one_opaque_cut_for_presence_absence_and_advanced_epochs() {
    futures::executor::block_on(async {
        for fault in [
            "none",
            "foreign",
            "key",
            "empty",
            "invalid-domain",
            "invalid-id",
            "job",
        ] {
            for domain in [0, 2] {
                let (job, definition, mut inner, record) = fixture();
                if fault == "empty" {
                    inner.through = 0;
                }
                let reader = BoundReader {
                    inner,
                    foreign: Arc::new(job.owned_buffer(128).unwrap()),
                    cuts: Default::default(),
                    lookups: Default::default(),
                    fault,
                    epoch: 3,
                };
                let admission = QueueAdmission::new(1, 1 << 20).unwrap();
                let other = Arc::new(admission.reserve(0).unwrap());
                let result = load_source_original(
                    &reader,
                    if fault == "invalid-domain" { 3 } else { domain },
                    if fault == "invalid-id" {
                        " \t"
                    } else {
                        "a\0α"
                    },
                    if fault == "job" { &other } else { &job },
                )
                .await;
                if fault == "none" {
                    let evidence = result.unwrap();
                    if domain == 2 {
                        evidence
                            .verify_record(&record.checked().unwrap(), 1, definition.pin(), &job)
                            .unwrap();
                    } else {
                        assert_eq!(evidence.source_offset(), None);
                    }
                } else {
                    assert!(result.is_err(), "{fault}/{domain}");
                }
                assert_eq!(reader.cuts.load(std::sync::atomic::Ordering::SeqCst), 1);
                assert_eq!(
                    reader.lookups.load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(matches!(fault, "none" | "foreign" | "key"))
                );
            }
        }
    });
}

fn domain_record(
    job: &Arc<QueueReservation>,
    definition: &AdmittedQueueDefinition,
    domain: u8,
    id: &str,
) -> OwnedQueueLogRecord {
    let queue = QueueAddress {
        tenant: "t".into(),
        queue: "q".into(),
    };
    if domain == 0 {
        let request = InsertRequest {
            queue: queue.clone(),
            request_id: id.into(),
            mode: InsertMode::InsertOnly,
            items: vec![NewWork {
                key: "k".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            }],
        };
        let outcomes = [InsertOutcome::Rejected {
            key: "k".into(),
            reason: Rejection::UniqueConflict,
        }];
        let binding = PublicationReceipt::insert(&request, &outcomes).unwrap();
        let frame = encode_publication_pinned(
            &queue,
            definition.pin(),
            &RecordSnapshot::default(),
            &combine_deltas(&[], 0).unwrap(),
            &[binding],
            1 << 20,
            job,
        )
        .unwrap();
        return encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, job).unwrap();
    }
    let commands = encode_publication_pinned(
        &queue,
        definition.pin(),
        &RecordSnapshot::default(),
        &combine_deltas(&[], 0).unwrap(),
        &[],
        1 << 20,
        job,
    )
    .unwrap();
    let request = ClaimRequest {
        queue,
        request_id: id.into(),
        filter: Default::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 2,
        max_bytes: 4096,
        fields: None,
        include_body: true,
    };
    let receipt = crate::queue_claim_receipt::encode_claim_receipt(&[], 1 << 20, job).unwrap();
    let wrapper = crate::queue_claim_publication::encode_claim_publication(
        crate::queue_claim_publication::ClaimPublicationInput {
            request: &request,
            definition,
            commands: commands.as_bytes(),
            receipt: &receipt,
            claimed_at: 0,
            limits: &QueueLimits::default(),
        },
        1 << 20,
        job,
    )
    .unwrap();
    crate::queue_log_record::encode_checked_claim_record(1, &wrapper, 1 << 20, job).unwrap()
}

#[test]
fn original_insert_and_claim_receipts_preserve_domain_and_literal_whitespace() {
    futures::executor::block_on(async {
        for domain in [0, 1] {
            let (job, definition, mut reader, _) = fixture();
            let id = " a\0α ";
            let record = domain_record(&job, &definition, domain, id);
            let checked = record.checked().unwrap();
            let (format, fp, outcome) = match checked.view().payload() {
                QueueLogPayload::Publication(frame) => {
                    let r = frame.receipts().next().unwrap().unwrap();
                    (r.fingerprint_format, *r.fingerprint, r.encoded_outcome())
                }
                QueueLogPayload::Claim(claim) => (
                    claim.fingerprint_format(),
                    *claim.fingerprint(),
                    claim.receipt_bytes(),
                ),
                _ => panic!(),
            };
            let fact = QueueSourceOriginal {
                fingerprint_format: format,
                fingerprint: fp,
                outcome: QueueSourcePayload::from_checked(&checked, 1, outcome).unwrap(),
            };
            reader.facts.insert(key(domain, id), encode(&fact).unwrap());
            let evidence = load_source_original(&reader, domain, id, &job)
                .await
                .unwrap();
            evidence.check_fingerprint(format, &fp).unwrap();
            assert_eq!(
                evidence
                    .verify_record(&checked, 1, definition.pin(), &job)
                    .unwrap(),
                outcome
            );
            assert!(matches!(
                evidence.verify_record(
                    &checked,
                    1,
                    DefinitionPin::from_persisted_bytes(b"other persisted definition"),
                    &job
                ),
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert_eq!(
                load_source_original(&reader, domain, id.trim(), &job)
                    .await
                    .unwrap()
                    .source_offset(),
                None
            );
            let wrong_domain = if domain == 0 { 1 } else { 0 };
            reader
                .facts
                .insert(key(wrong_domain, id), encode(&fact).unwrap());
            let wrong = load_source_original(&reader, wrong_domain, id, &job)
                .await
                .unwrap();
            assert!(matches!(
                wrong.verify_record(&checked, 1, definition.pin(), &job),
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
    });
}

#[test]
fn original_claim_requires_command_to_lease_binding_after_valid_span_digest_and_decode() {
    use crate::{
        queue_body::ResolvedBody,
        queue_claim::{ClaimContext, plan_claim},
        queue_claim_publication::{
            ClaimPublicationInput, encode_claim_publication, view_claim_publication,
        },
        queue_claim_receipt::{ClaimLease, encode_claim_receipt},
        queue_fields::ResolvedFields,
        queue_log_record::encode_checked_claim_record,
        queue_planner::CommitDelta,
    };
    futures::executor::block_on(async {
        let (job, definition, mut reader, _) = fixture();
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let mut seed = group("seed");
        seed.work_inserts.push(NewWork {
            key: "k".into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: Default::default(),
        });
        let (_seed, before) = super::super::predecessor::tests::record(
            &queue,
            definition.schema(),
            &RecordSnapshot::default(),
            &seed,
            &job,
        );
        let planned = plan_claim(
            before.work["k"].clone(),
            vec![7; 32],
            ClaimContext {
                now: 0,
                eligible_at: 0,
                expires_at: 1_000_000,
                insertion_head: 1,
            },
        )
        .unwrap();
        let version = RecordVersion {
            collection: None,
            key: "k".into(),
            version: 2,
        };
        let delta = CommitDelta {
            work: vec![planned.work],
            work_fields: std::collections::BTreeMap::from([("k".into(), ResolvedFields::Preserve)]),
            work_bodies: std::collections::BTreeMap::from([("k".into(), ResolvedBody::Preserve)]),
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: vec![version.clone()],
            next_sequence: 1,
            receipt: CommitReceipt {
                request_id: "unused".into(),
                outcome: GroupOutcome::Committed {
                    versions: vec![version],
                },
            },
        };
        let commands = encode_publication_pinned(
            &queue,
            definition.pin(),
            &before,
            &delta,
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let request = ClaimRequest {
            queue,
            request_id: "claim\0α".into(),
            filter: Default::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: None,
            include_body: true,
        };
        let receipt = encode_claim_receipt(
            &[ClaimLease {
                key: "k".into(),
                token: vec![7; 32],
                version: 2,
            }],
            1 << 20,
            &job,
        )
        .unwrap();
        let wrapper = encode_claim_publication(
            ClaimPublicationInput {
                request: &request,
                definition: &definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: 0,
                limits: &QueueLimits::default(),
            },
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_claim_publication(wrapper.bytes()).unwrap();
        let token = view.receipt().entries().next().unwrap().unwrap().token;
        let position = token.as_ptr().addr() - wrapper.bytes().as_ptr().addr();
        let mut changed = wrapper.bytes().to_vec();
        changed[position] ^= 1;
        assert!(
            view_claim_publication(&changed)
                .unwrap()
                .validate_binding(&job)
                .is_err()
        );
        // The producer correctly refuses an invalid binding. Model stored
        // authenticated corruption at ingress instead: preserve the framing,
        // replace its structurally valid wrapper, and recompute FWQL2 checksum.
        let valid_record = encode_checked_claim_record(1, &wrapper, 1 << 20, &job).unwrap();
        let interval = valid_record.checked().unwrap().payload_interval().unwrap();
        let charge = job
            .owned_buffer(valid_record.bytes().len() as u64 * 4 + 4096)
            .unwrap();
        let mut raw = valid_record.bytes().to_vec();
        raw[interval].copy_from_slice(&changed);
        let end = raw.len() - 32;
        use sha2::{Digest, Sha256};
        let checksum = Sha256::digest(&raw[..end]);
        raw[end..].copy_from_slice(&checksum);
        let invalid_record =
            OwnedQueueLogRecord::from_admitted_bytes(bytes::Bytes::from(raw), charge, &job)
                .unwrap();
        for (record, valid) in [(&valid_record, true), (&invalid_record, false)] {
            let checked = record.checked().unwrap();
            let QueueLogPayload::Claim(claim) = checked.view().payload() else {
                panic!()
            };
            let fact = QueueSourceOriginal {
                fingerprint_format: 3,
                fingerprint: *claim.fingerprint(),
                outcome: QueueSourcePayload::from_checked(&checked, 2, claim.receipt_bytes())
                    .unwrap(),
            };
            reader.through = 3;
            reader.facts.clear();
            reader
                .facts
                .insert(key(1, &request.request_id), encode(&fact).unwrap());
            let evidence = load_source_original(&reader, 1, &request.request_id, &job)
                .await
                .unwrap();
            evidence.check_fingerprint(3, claim.fingerprint()).unwrap();
            fact.outcome
                .verify_cached(fact.outcome.span().resolve(&checked, 2).unwrap())
                .unwrap();
            crate::queue_claim_receipt::view_claim_receipt(claim.receipt_bytes()).unwrap();
            let result = evidence.verify_record(&checked, 2, definition.pin(), &job);
            if valid {
                assert_eq!(result.unwrap(), claim.receipt_bytes());
            } else {
                assert!(
                    matches!(result,Err(QueueError::OutcomeUnknown(ref m))if m.contains("lease entry and resolved metadata differ"))
                );
            }
        }
    });
}
