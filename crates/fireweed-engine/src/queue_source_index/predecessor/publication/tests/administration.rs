use super::*;
#[test]
fn fp4_administration_and_atomic_state_change_match_the_cold_fold() {
    use crate::queue_publication::*;
    use fireweed_core::queue_administration::*;
    futures::executor::block_on(async {
        let mut h = Harness::new(mixed_schema());
        let mut seed = group("fp4-seed");
        seed.work_inserts.push(NewWork {
            key: "claim".into(),
            priority: 0,
            available_at: 0,
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(1)),
                ("v".into(), FieldValue::Float(-0.0)),
            ]),
            body: vec![1, 0].into(),
        });
        h.apply(&seed).await;
        h.claim(false).await;
        let patch = WorkPatch {
            priority: Some(17),
            ..Default::default()
        };
        let mut admin_request = group("admin-source-plan");
        admin_request.work_updates.push(UnclaimedUpdate {
            key: "claim".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Administrative {
                patch: patch.clone(),
                lifecycle: AdministrativeLifecycle::Pending,
                lease_guard: AdministrativeLeaseGuard::InvalidateActive,
            },
        });
        let admin = crate::queue_planner::plan_commit_group(
            &admin_request,
            &mut h.snapshot.clone(),
            0,
            &h.schema,
        )
        .unwrap();
        assert!(matches!(
            admin.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        let ordinary_group = put_state(
            "fp4-release-and-state",
            "state",
            StateExpect::Absent,
            2,
            0.0,
        );
        let mut after = h.snapshot.clone();
        after.unique_probes_loaded.extend(
            crate::queue_planner::required_unique_probes(&ordinary_group, &after, 0, &h.schema)
                .unwrap(),
        );
        let ordinary =
            crate::queue_planner::plan_commit_group(&ordinary_group, &mut after, 0, &h.schema)
                .unwrap();
        let GroupOutcome::Committed { versions } = &ordinary.receipt.outcome else {
            panic!()
        };
        for row in &admin.work {
            after.work.insert(row.record.work.key.clone(), row.clone());
        }
        for version in &admin.versions {
            after.versions.insert(
                crate::queue_planner::RecordIdentity {
                    collection: version.collection.clone(),
                    key: version.key.clone(),
                },
                version.version,
            );
        }
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch,
                    lifecycle: AdministrativeLifecycle::Pending,
                    lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        };
        let mut logical_group = ordinary_group.clone();
        logical_group.administration = Some(AdministrativeCommit {
            intent: intent.clone(),
            max_result_bytes: 1 << 20,
        });
        let mut histogram = AdministrativeHistogram::default();
        histogram
            .record(&AdministrativeOutcome::Changed { version: 3 })
            .unwrap();
        let result = AdministrativeResult {
            histogram: histogram.clone(),
            items: vec![],
            clauses: vec![AdministrativeClauseResult {
                id: "all".into(),
                histogram,
            }],
        };
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions,
            administration: &result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &h.queue,
            &logical_group,
            &intent,
            1 << 20,
            &outcome,
            &h.job,
        )
        .unwrap();
        let fragment = encode_administrative_work_fragment(&h.snapshot, &admin, &h.job).unwrap();
        let ordinary_frame = encode_publication_pinned(
            &h.queue,
            h.definition.pin(),
            &h.snapshot,
            &ordinary,
            &[],
            1 << 20,
            &h.job,
        )
        .unwrap();
        let view = view_publication(ordinary_frame.as_bytes()).unwrap();
        let commands = view
            .command_fragments()
            .collect::<PublicationResult<Vec<_>>>()
            .unwrap();
        let command_bytes = fragment.as_bytes().len() as u64
            + commands
                .iter()
                .map(|command| command.as_bytes().len() as u64)
                .sum::<u64>();
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            h.definition.pin(),
            &h.snapshot,
            &ordinary,
            1 + commands.len() as u64,
            command_bytes,
            1 << 20,
            &h.job,
        )
        .unwrap();
        builder.append_administrative(fragment.as_bytes()).unwrap();
        for command in &commands {
            builder.append_ordinary(command.as_bytes()).unwrap();
        }
        let frame = builder.finish().unwrap();
        let proposed = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &h.job,
        )
        .unwrap();
        let checked = proposed.checked().unwrap();
        let QueueLogPayload::Publication(publication) = checked.view().payload() else {
            panic!()
        };
        assert_eq!(
            publication
                .receipts()
                .next()
                .unwrap()
                .unwrap()
                .fingerprint_format,
            4
        );
        drop(commands);
        drop(frame);
        drop(ordinary_frame);
        drop(fragment);
        h.accept(proposed, after).await;
        assert_eq!(
            h.snapshot.work["claim"].record.lifecycle,
            Lifecycle::Pending
        );
        assert_eq!(h.snapshot.work["claim"].record.work.priority, 17);
        assert!(
            h.snapshot
                .state
                .contains_key(&("s\0α".into(), "state".into()))
        );
    });
}
