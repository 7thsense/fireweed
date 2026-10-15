use super::*;
#[test]
fn complete_publications_match_an_independent_cold_fold_after_every_change() {
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let mut snapshot = f.snapshot;
        let mut history = vec![f._genesis, f._seed];
        let mut reader = Reader {
            through: 2,
            identity: f.reader.identity.clone(),
            facts: oracle::fold(&history, &snapshot, &f.schema),
        };
        let mut proposals = vec![
            request("create"),
            request("preserve"),
            request("replace"),
            request("complete"),
            request("pending"),
            request("fail"),
            request("pending"),
            request("delete"),
        ];
        let mut recreate = group("recreate-after-delete");
        recreate.work_inserts.push(NewWork {
            key: "a\0α".into(),
            priority: i64::MIN,
            available_at: i64::MAX,
            fields: Default::default(),
            body: vec![9, 0].into(),
        });
        proposals.push(recreate);
        proposals.push(group("receipt-only"));
        let mut rejected = group("rejected-original");
        rejected.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "missing".into(),
            expect: StateExpect::Version(99),
            change: StateChange::Delete,
        });
        proposals.push(rejected);
        for (index, mut proposal) in proposals.into_iter().enumerate() {
            let rejection = proposal.request_id == "rejected-original";
            for update in &mut proposal.work_updates {
                update.expect = StateExpect::Version(snapshot.work[&update.key].record.version);
            }
            proposal.request_id = format!("ordinary-{index}-{}", proposal.request_id);
            let (proposed, after) = record(&f.queue, &f.schema, &snapshot, &proposal, &f.job);
            let checked = proposed.checked().unwrap();
            let QueueLogPayload::Publication(publication) = checked.view().payload() else {
                panic!()
            };
            let outcome = publication.receipts().next().unwrap().unwrap().outcome;
            if rejection {
                assert!(matches!(
                    outcome,
                    crate::queue_publication::ReceiptOutcomeRef::Commit(
                        crate::queue_publication::CommitOutcomeRef::Rejected(_)
                    )
                ));
            } else {
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
            }
            let baseline = f.admission.usage().unwrap();
            let compiled = compile_publication(
                &reader,
                &checked,
                &f.definition,
                QueueSourceCompileLimits {
                    edits: 10000,
                    input_bytes: 1 << 20,
                },
                &f.job,
            )
            .await
            .unwrap();
            compiled.verify(&reader.cut().unwrap(), &f.job).unwrap();
            let mut actual = reader.facts.clone();
            for (key, value) in compiled.edits() {
                match value {
                    Some(value) => {
                        actual.insert(key.to_vec(), value.to_vec());
                    }
                    None => {
                        assert!(actual.remove(key).is_some());
                    }
                }
            }
            drop(compiled);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            history.push(proposed);
            snapshot = after;
            assert_eq!(
                actual,
                oracle::fold(&history, &snapshot, &f.schema),
                "{}",
                proposal.request_id
            );
            reader = Reader {
                through: history.len() as u64,
                identity: reader.identity.clone(),
                facts: actual,
            };
        }
    });
}
#[test]
fn mixed_state_unique_swaps_signed_zero_and_packed_aba_match_cold_fold() {
    futures::executor::block_on(async {
        let mut h = Harness::new(mixed_schema());
        h.apply(&put_state("state-a", "a\0", StateExpect::Absent, 1, -0.0))
            .await;
        h.apply(&put_state("state-b", "b\0", StateExpect::Absent, 2, 1.0))
            .await;
        let mut swap = put_state("swap", "a\0", StateExpect::Version(1), 2, -0.0);
        swap.state_updates
            .extend(put_state("unused", "b\0", StateExpect::Version(1), 1, 1.0).state_updates);
        h.apply(&swap).await;
        h.apply(&put_state(
            "signed-zero",
            "a\0",
            StateExpect::Version(2),
            2,
            0.0,
        ))
        .await;
        let put = put_state("aba-put", "c\0", StateExpect::Absent, 3, 3.0);
        let mut delete = group("aba-delete");
        delete.state_updates.push(StateUpdate {
            collection: "s\0α".into(),
            key: "c\0".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Delete,
        });
        let recreate = put_state("aba-recreate", "c\0", StateExpect::Absent, 4, -1.0);
        h.pack(&[put, delete, recreate]).await;
        assert_eq!(h.snapshot.state[&("s\0α".into(), "c\0".into())].version, 3);
        let mut first = group("gap-insert");
        first.work_inserts.push(NewWork {
            key: "gap".into(),
            priority: i64::MAX,
            available_at: i64::MIN,
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(10)),
                ("v".into(), FieldValue::Float(-0.0)),
            ]),
            body: vec![1, 2].into(),
        });
        let mut remove = group("gap-delete");
        remove.work_updates.push(UnclaimedUpdate {
            key: "gap".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let mut last = group("gap-recreate");
        last.work_inserts.push(NewWork {
            key: "gap".into(),
            priority: i64::MIN,
            available_at: i64::MAX,
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(11)),
                ("v".into(), FieldValue::Float(0.0)),
            ]),
            body: vec![3, 4].into(),
        });
        h.pack(&[first, remove, last]).await;
        assert_eq!(h.snapshot.next_sequence, 2);
        assert_eq!(h.snapshot.work["gap"].record.insertion_sequence, 2);
        assert_eq!(h.snapshot.work["gap"].record.version, 3);
        h.apply(&group("empty-after-mixed")).await;
    });
}
