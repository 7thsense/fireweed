use super::*;
use crate::queue_planner::RecordSnapshot;
use std::collections::BTreeMap;

// Independent complete inventory from the ordinary planner snapshot: no call to
// WorkIndex::posting or the delta visitor participates in the reference.
fn inventory(snapshot: &RecordSnapshot) -> BTreeMap<Vec<u8>, Vec<u8>> {
    fn escaped(out: &mut Vec<u8>, value: &str) {
        for byte in value.bytes() {
            if byte == 0 {
                out.extend_from_slice(&[0, 255]);
            } else {
                out.push(byte);
            }
        }
        out.extend_from_slice(&[0, 0]);
    }
    fn signed(n: i64) -> [u8; 8] {
        ((n as u64) ^ (1u64 << 63)).to_be_bytes()
    }
    let mut out = BTreeMap::new();
    for (literal, stored) in &snapshot.work {
        let row = &stored.record;
        let mut key = vec![1, 10, 0];
        escaped(&mut key, literal);
        out.insert(key, vec![]);
        let lifecycle = match row.lifecycle {
            fireweed_core::queue_contract::Lifecycle::Pending => 0,
            fireweed_core::queue_contract::Lifecycle::Leased => 1,
            fireweed_core::queue_contract::Lifecycle::Completed => 2,
            fireweed_core::queue_contract::Lifecycle::Failed => 3,
        };
        let mut tuples = vec![
            (0, vec![lifecycle]),
            (1, row.attempts.to_be_bytes().to_vec()),
        ];
        if let Some(expiry) = row.lease_expires_at {
            tuples.push((2, signed(expiry).to_vec()));
        }
        let candidate = lifecycle <= 1;
        for (kind, overdue, descending, scheduled) in [
            (3, false, false, false),
            (4, false, true, false),
            (5, false, false, true),
            (6, true, false, false),
            (7, true, true, false),
            (8, true, false, true),
        ] {
            if kind != 5 && !candidate {
                continue;
            }
            let mut tuple = Vec::new();
            if overdue {
                tuple.extend_from_slice(&signed(row.eligible_since));
            }
            let mut primary = signed(if scheduled {
                row.work.available_at
            } else {
                row.work.priority
            });
            if descending {
                for byte in &mut primary {
                    *byte = !*byte;
                }
            }
            tuple.extend_from_slice(&primary);
            tuple.extend_from_slice(&row.insertion_sequence.to_be_bytes());
            tuples.push((kind, tuple));
        }
        for (kind, tuple) in tuples {
            let mut key = vec![1, 11, kind];
            key.extend_from_slice(&tuple);
            escaped(&mut key, literal);
            assert!(out.insert(key, vec![]).is_none());
        }
    }
    for (collection, literal) in snapshot.state.keys() {
        let mut key = vec![1, 10, 1];
        escaped(&mut key, collection);
        escaped(&mut key, literal);
        assert!(out.insert(key, vec![]).is_none());
    }
    out
}
pub(in crate::queue_source_index::predecessor) async fn assert_inventory_delta<
    R: QueueSourceReader,
>(
    context: &QueueSourcePublicationContext<'_, R>,
    before: &RecordSnapshot,
    after: &RecordSnapshot,
) {
    let mut builder = context
        .intrinsics(QueueSourceCompileLimits {
            edits: 10000,
            input_bytes: 1 << 20,
        })
        .unwrap();
    for command in context.commands() {
        let command = command.unwrap();
        let predecessor = context.predecessor(&command).await.unwrap();
        let transition = context.record_transition(&predecessor).await.unwrap();
        builder.observe(&transition).unwrap();
    }
    let output = builder.finish().unwrap();
    output
        .verify(context.checked.owner(), &context.cut, context.job)
        .unwrap();
    let mut actual = inventory(before);
    let expected = inventory(after);
    for (key, value) in output.edits() {
        if let Some(value) = value {
            assert!(value.is_empty());
            assert!(actual.insert(key.to_vec(), value.to_vec()).is_none());
        } else {
            assert!(actual.remove(key).is_some());
        }
    }
    assert_eq!(actual, expected);
    drop(output);
}

#[test]
fn output_limits_coverage_scope_and_late_failure_restore_all_credit() {
    use super::super::{
        test_fixture::{fixture, request},
        tests::{group, record},
    };
    use fireweed_core::queue_contract::*;
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let mut two = group("two-intrinsic-creates");
        for literal in ["c\0γ", "d\0δ"] {
            two.work_inserts.push(NewWork {
                key: literal.into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            });
        }
        for proposal in [request("complete"), two] {
            let (proposed, after) = record(&f.queue, &f.schema, &f.snapshot, &proposal, &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            assert_inventory_delta(&context, &f.snapshot, &after).await;
            let baseline = f.admission.usage().unwrap();
            let mut builder = context
                .intrinsics(QueueSourceCompileLimits {
                    edits: 1000,
                    input_bytes: 1 << 20,
                })
                .unwrap();
            for command in context.commands() {
                let command = command.unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                builder.observe(&transition).unwrap();
            }
            let output = builder.finish().unwrap();
            let count = output.edits().len() as u64;
            let bytes = output
                .edits()
                .map(|(key, value)| key.len() as u64 + value.map_or(0, |v| v.len() as u64))
                .sum::<u64>();
            assert!(count > 1 && bytes > 0);
            let foreign = f.admission.reserve(0).unwrap();
            assert!(output.verify(&proposed, &context.cut, &foreign).is_err());
            drop(foreign);
            let (twin, _) = record(&f.queue, &f.schema, &f.snapshot, &proposal, &f.job);
            assert!(output.verify(&twin, &context.cut, &f.job).is_err());
            drop(twin);
            drop(output);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            for limits in [
                QueueSourceCompileLimits {
                    edits: count,
                    input_bytes: bytes,
                },
                QueueSourceCompileLimits {
                    edits: count - 1,
                    input_bytes: bytes,
                },
                QueueSourceCompileLimits {
                    edits: count,
                    input_bytes: bytes - 1,
                },
            ] {
                let mut builder = context.intrinsics(limits).unwrap();
                let mut failed = false;
                for command in context.commands() {
                    let command = command.unwrap();
                    let predecessor = context.predecessor(&command).await.unwrap();
                    let transition = context.record_transition(&predecessor).await.unwrap();
                    if builder.observe(&transition).is_err() {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    assert!(builder.finish().is_err());
                } else {
                    assert_eq!(builder.finish().unwrap().edits().len() as u64, count);
                }
                assert_eq!(failed, limits.edits < count || limits.input_bytes < bytes);
                assert_eq!(f.admission.usage().unwrap(), baseline);
            }
            let mut builder = context
                .intrinsics(QueueSourceCompileLimits {
                    edits: 1000,
                    input_bytes: 1 << 20,
                })
                .unwrap();
            let command = context.commands().next().unwrap().unwrap();
            let predecessor = context.predecessor(&command).await.unwrap();
            let transition = context.record_transition(&predecessor).await.unwrap();
            builder.observe(&transition).unwrap();
            assert!(builder.observe(&transition).is_err());
            assert!(builder.finish().is_err());
            drop(transition);
            drop(predecessor);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            let builder = context
                .intrinsics(QueueSourceCompileLimits {
                    edits: 1000,
                    input_bytes: 1 << 20,
                })
                .unwrap();
            assert!(builder.finish().is_err());
            assert_eq!(f.admission.usage().unwrap(), baseline);
            let remaining = (16 << 20) - baseline.1;
            let blocker = f.job.owned_buffer(remaining - 1).unwrap();
            assert!(
                context
                    .intrinsics(QueueSourceCompileLimits {
                        edits: 1000,
                        input_bytes: 1 << 20
                    })
                    .is_err()
            );
            drop(blocker);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn version_only_updates_and_receipt_only_records_need_no_output_credit() {
    use super::super::{
        test_fixture::fixture,
        tests::{group, record},
    };
    use fireweed_core::queue_contract::*;
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let mut version = group("version-only");
        version.work_updates.push(UnclaimedUpdate {
            key: "a\0α".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(Default::default()),
        });
        for request in [version, group("receipt-only")] {
            let (proposed, after) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            assert_inventory_delta(&context, &f.snapshot, &after).await;
            let baseline = f.admission.usage().unwrap();
            let mut builder = context
                .intrinsics(QueueSourceCompileLimits {
                    edits: 0,
                    input_bytes: 0,
                })
                .unwrap();
            for command in context.commands() {
                let command = command.unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                builder.observe(&transition).unwrap();
            }
            assert_eq!(builder.finish().unwrap().edits().len(), 0);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

#[test]
fn retained_output_admission_has_an_exact_root_boundary() {
    use super::super::{
        test_fixture::{fixture, request},
        tests::record,
    };
    use fireweed_core::queue_contract::Lifecycle;
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let proposal = request("complete");
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &proposal, &f.job);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let available = (16 << 20) - baseline.1;
        async fn attempt<R: QueueSourceReader>(
            context: &QueueSourcePublicationContext<'_, R>,
        ) -> (QueueResult<()>, usize) {
            let mut builder = match context.intrinsics(QueueSourceCompileLimits {
                edits: 1000,
                input_bytes: 1 << 20,
            }) {
                Ok(builder) => builder,
                Err(error) => return (Err(error), 0),
            };
            let result = async {
                for command in context.commands() {
                    let command = command?;
                    let predecessor = context.predecessor(&command).await?;
                    let transition = context.record_transition(&predecessor).await?;
                    builder.observe(&transition)?;
                }
                Ok(())
            }
            .await;
            let count = builder.output.edits().len();
            if let Err(error) = result {
                return (Err(error), count);
            }
            (builder.finish().map(drop), count)
        }
        let mut low = 1;
        let mut high = available;
        while low < high {
            let mid = low + (high - low) / 2;
            let blocker = f.job.owned_buffer(available - mid).unwrap();
            let ok = attempt(&context).await.0.is_ok();
            drop(blocker);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            if ok {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        for (allowance, expected) in [(low - 1, false), (low, true)] {
            let blocker = f.job.owned_buffer(available - allowance).unwrap();
            let (result, retained) = attempt(&context).await;
            if expected {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(error, QueueError::OutcomeUnknown(ref message) if message.contains("intrinsic output admission")),
                    "{error:?}"
                );
                assert!(retained > 0);
            }
            drop(blocker);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

pub(in crate::queue_source_index::predecessor) async fn assert_one_claim_and_renewal(
    context: &QueueSourcePublicationContext<'_, super::super::tests::Reader>,
    before: &RecordSnapshot,
    claimed: &RecordSnapshot,
    token: &[u8],
) {
    use super::super::tests::{Reader, group, record};
    use fireweed_core::queue_contract::*;
    let command = context.commands().next().unwrap().unwrap();
    let predecessor = context.predecessor(&command).await.unwrap();
    let transition = context.record_transition(&predecessor).await.unwrap();
    let mut builder = context
        .intrinsics(QueueSourceCompileLimits {
            edits: 5,
            input_bytes: 4096,
        })
        .unwrap();
    builder.observe(&transition).unwrap();
    assert_eq!(builder.finish().unwrap().edits().len(), 5);
    let mut reader = Reader {
        through: context.cut.through() + 1,
        identity: context.reader.identity.clone(),
        facts: context.reader.facts.clone(),
    };
    reader.facts.insert(
        QueueSourceKey::Record {
            collection: None,
            key: command.resolved.identity.key,
        }
        .encode()
        .unwrap(),
        queue_storage_layout::encode(&transition.fact()).unwrap(),
    );
    let address = context.cut.address();
    let queue = QueueAddress {
        tenant: address.tenant.into(),
        queue: address.queue.into(),
    };
    let mut renew = group("renew-intrinsic");
    renew.claimed_updates.push(ClaimedUpdate {
        key: command.resolved.identity.key.into(),
        lease: LeaseCredential {
            token: token.to_vec(),
        },
        expected_version: transition.fact().highwater,
        patch: Default::default(),
        disposition: Disposition::Retain {
            renew_nanos: Some(2_000_000),
        },
    });
    let (renewed, after) = record(
        &queue,
        context.definition.schema(),
        claimed,
        &renew,
        context.job,
    );
    let checked = renewed.checked().unwrap();
    let renewed_context =
        QueueSourcePublicationContext::begin(&reader, &checked, context.definition, context.job)
            .await
            .unwrap();
    assert_inventory_delta(&renewed_context, claimed, &after).await;
    let command = renewed_context.commands().next().unwrap().unwrap();
    let predecessor = renewed_context.predecessor(&command).await.unwrap();
    let transition = renewed_context
        .record_transition(&predecessor)
        .await
        .unwrap();
    let mut builder = renewed_context
        .intrinsics(QueueSourceCompileLimits {
            edits: 2,
            input_bytes: 4096,
        })
        .unwrap();
    builder.observe(&transition).unwrap();
    assert_eq!(builder.finish().unwrap().edits().len(), 2);
    assert_inventory_delta(context, before, claimed).await;
}
