use super::*;

impl Harness {
    pub(super) async fn claim(&mut self, empty: bool) {
        use crate::{
            queue_body::ResolvedBody, queue_fields::ResolvedFields, queue_planner::CommitDelta,
        };
        let mut after = self.snapshot.clone();
        let mut work = Vec::new();
        let mut versions = Vec::new();
        let mut leases = Vec::new();
        let mut work_fields = std::collections::BTreeMap::new();
        let mut work_bodies = std::collections::BTreeMap::new();
        if !empty {
            let claimed = crate::queue_claim::plan_claim(
                self.snapshot.work["claim"].clone(),
                vec![7; 32],
                crate::queue_claim::ClaimContext {
                    now: 10,
                    eligible_at: 0,
                    expires_at: 1_000_010,
                    insertion_head: self.snapshot.next_sequence,
                },
            )
            .unwrap();
            let version = claimed.work.record.version;
            versions.push(RecordVersion {
                collection: None,
                key: "claim".into(),
                version,
            });
            after.versions.insert(
                crate::queue_planner::RecordIdentity {
                    collection: None,
                    key: "claim".into(),
                },
                version,
            );
            after.work.insert("claim".into(), claimed.work.clone());
            work.push(claimed.work);
            work_fields.insert("claim".into(), ResolvedFields::Preserve);
            work_bodies.insert("claim".into(), ResolvedBody::Preserve);
            leases.push(crate::queue_claim_receipt::ClaimLease {
                key: "claim".into(),
                token: vec![7; 32],
                version,
            });
        }
        let delta = CommitDelta {
            work,
            work_fields,
            work_bodies,
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: versions.clone(),
            next_sequence: self.snapshot.next_sequence,
            receipt: CommitReceipt {
                request_id: String::new(),
                outcome: GroupOutcome::Committed { versions },
            },
        };
        let request = ClaimRequest {
            queue: self.queue.clone(),
            request_id: if empty {
                "empty-claim"
            } else {
                "nonempty-claim"
            }
            .into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 1,
            max_bytes: 1 << 20,
            fields: Some(Default::default()),
            include_body: false,
        };
        let limits = QueueLimits {
            max_records: 100,
            max_groups: 100,
            max_request_bytes: 1 << 20,
            max_group_bytes: 1 << 20,
            max_response_bytes: 1 << 20,
            max_body_bytes: 1 << 20,
            max_state_bytes: 1 << 20,
        };
        let receipt =
            crate::queue_claim_receipt::encode_claim_receipt(&leases, 1 << 20, &self.job).unwrap();
        let commands = crate::queue_publication::encode_publication_pinned(
            &self.queue,
            self.definition.pin(),
            &self.snapshot,
            &delta,
            &[],
            1 << 20,
            &self.job,
        )
        .unwrap();
        let publication = crate::queue_claim_publication::encode_claim_publication(
            crate::queue_claim_publication::ClaimPublicationInput {
                request: &request,
                definition: &self.definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: 10,
                limits: &limits,
            },
            1 << 20,
            &self.job,
        )
        .unwrap();
        let proposed = crate::queue_log_record::encode_checked_claim_record(
            1,
            &publication,
            1 << 20,
            &self.job,
        )
        .unwrap();
        drop((publication, commands, receipt));
        self.accept(proposed, after).await;
    }
}

#[test]
fn empty_and_nonempty_claims_match_the_cold_fold_and_retain_original_receipts() {
    futures::executor::block_on(async {
        let mut h = Harness::new(mixed_schema());
        h.claim(true).await;
        let mut seed = group("claim-seed");
        seed.work_inserts.push(NewWork {
            key: "claim".into(),
            priority: -1,
            available_at: 0,
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(1)),
                ("v".into(), FieldValue::Float(-0.0)),
            ]),
            body: vec![0, 1, 2].into(),
        });
        h.apply(&seed).await;
        h.claim(false).await;
        assert_eq!(h.snapshot.work["claim"].record.lifecycle, Lifecycle::Leased);
        assert_eq!(h.snapshot.work["claim"].record.attempts, 1);
        assert!(
            h.reader.facts.contains_key(
                &QueueSourceKey::Original {
                    domain: 1,
                    request_id: "empty-claim"
                }
                .encode()
                .unwrap()
            )
        );
        assert!(
            h.reader.facts.contains_key(
                &QueueSourceKey::Original {
                    domain: 1,
                    request_id: "nonempty-claim"
                }
                .encode()
                .unwrap()
            )
        );
        let mut renew = group("renew-source");
        renew.claimed_updates.push(ClaimedUpdate {
            key: "claim".into(),
            lease: LeaseCredential { token: vec![7; 32] },
            expected_version: 2,
            patch: WorkPatch::default(),
            disposition: Disposition::Retain {
                renew_nanos: Some(2_000_000),
            },
        });
        h.apply(&renew).await;
    });
}
