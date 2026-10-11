//! Positional, input-bounded addressed administrative preparation. The backend
//! certifies one exact lookup per position under its unchanged queue fence.
use super::stream::{
    add, intent_headers, ordinary_bound, resolve_candidate, row_bound, scale, selected_row, wire,
    work_keys,
};
use super::*;
use crate::queue_admission::{QueueBufferCharge, QueueReservation};
use crate::queue_planner::{RecordIdentity, RecordSnapshot, validate_snapshot};
use std::sync::Arc;

/// Owned authority includes known absence/tombstone, not just a live selected row.
/// Borrowed full images are caller-admitted reference fixtures. Production uses
/// one guarded exact-key lookup. Ordinal and key certify positional completeness.
pub enum AddressedAdministrativeInput<'s, 'r> {
    BorrowedFullImage {
        position: usize,
        key: &'s str,
        image: &'s RecordSnapshot,
    },
    OwnedOneRow {
        position: usize,
        key: String,
        image: RecordSnapshot,
        hydration: QueueBufferCharge<'r>,
    },
}
fn retained_result_bound(
    key: &str,
    row: Option<&StoredWork>,
    fields: Option<&BTreeSet<String>>,
) -> QueueResult<u64> {
    let mut bytes = add(scale(key.len() as u64, 12)?, 4096)?;
    if let (Some(row), Some(names)) = (row, fields) {
        for name in names {
            if let Some(value) = row.record.work.fields.get(name) {
                bytes = add(
                    bytes,
                    add(
                        scale(wire(value)?, 12)?,
                        add(scale(name.len() as u64, 12)?, 1024)?,
                    )?,
                )?;
            }
        }
    }
    Ok(bytes)
}
/// Resolve all positions and await each packet without retaining row images.
/// Returned before values are separately admitted before cloning and remain
/// charged after every hydration input has dropped. Logical result limits and
/// complete combined business/publication decisions follow in the enclosing job.
#[allow(clippy::too_many_arguments)]
pub async fn scan_addressed_group_to_sink<'s, 'r, F, Fut>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    inputs: impl futures::Stream<Item = QueueResult<AddressedAdministrativeInput<'s, 'r>>>,
    ordinary_image: &RecordSnapshot,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: Arc<QueueReservation>,
    sink: F,
) -> QueueResult<AdmittedAdministrativeScan>
where
    F: FnMut(ResolvedAdministrativeRow) -> Fut,
    Fut: std::future::Future<Output = QueueResult<()>>,
{
    scan_addressed_group_to_sink_bounded(
        ordinary,
        intent,
        inputs,
        ordinary_image,
        schema,
        limits,
        reservation,
        u64::MAX,
        sink,
    )
    .await
}

/// Count the complete receipt's monotone lower bound before retaining results.
/// Request identity/variant/ordinary-version framing is derived without copies;
/// the enclosing job performs the final exact count after business validation.
#[allow(clippy::too_many_arguments)]
pub async fn scan_addressed_group_to_sink_bounded<'s, 'r, F, Fut>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    inputs: impl futures::Stream<Item = QueueResult<AddressedAdministrativeInput<'s, 'r>>>,
    ordinary_image: &RecordSnapshot,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: Arc<QueueReservation>,
    max_result_bytes: u64,
    mut sink: F,
) -> QueueResult<AdmittedAdministrativeScan>
where
    F: FnMut(ResolvedAdministrativeRow) -> Fut,
    Fut: std::future::Future<Output = QueueResult<()>>,
{
    use futures::StreamExt;
    let AdministrativeOperation::Addressed(entries) = &intent.operation else {
        return Err(QueueError::Invalid(
            "addressed sink requires addressed intent".into(),
        ));
    };
    intent.validate(schema, limits)?;
    let receipt_base_lower_bound =
        crate::queue_publication::measure_administrative_receipt_base_lower_bound(ordinary)
            .map_err(|_| unknown("administrative receipt framing measurement failed"))?;
    let request_fingerprint = fingerprint("administrative-result-retention", ordinary)?;
    validate_snapshot(ordinary_image)?;
    let bytes = add(
        intent_headers(intent)?,
        add(
            ordinary_bound(ordinary, ordinary_image)?,
            add(
                scale(wire(intent)?, 12)?,
                add(
                    scale(wire(schema)?, 12)?,
                    add(scale(entries.len() as u64, 8192)?, 8192)?,
                )?,
            )?,
        )?,
    )?;
    let mut root = reservation.buffer(bytes)?;
    let scan = AdministrativeScanIdentity(Arc::new(()));
    // Declared before payloads: error cleanup drops result copies first.
    let mut retention = Some(reservation.buffer(0)?);
    let mut result = AdministrativeResult {
        histogram: Default::default(),
        items: Vec::new(),
        clauses: vec![],
    };
    result
        .items
        .try_reserve_exact(entries.len())
        .map_err(|_| unknown("addressed result allocation failed"))?;
    let mut overflow = false;
    let mut item_bytes = 0u64;
    let mut seen = BTreeSet::new();
    let explicit: BTreeSet<_> = work_keys(ordinary).collect();
    let mut positions = 0usize;
    let mut packets = 0u64;
    let mut overlap = false;
    futures::pin_mut!(inputs);
    while let Some(input) = inputs.next().await {
        let input = input.map_err(|_| unknown("addressed source failed before EOF"))?;
        let (position, key, image) = match &input {
            AddressedAdministrativeInput::BorrowedFullImage {
                position,
                key,
                image,
            } => (*position, *key, *image),
            AddressedAdministrativeInput::OwnedOneRow {
                position,
                key,
                image,
                hydration,
            } => {
                if !hydration.belongs_to(&reservation)
                    || image.work.len() > 1
                    || image.versions.len() > 1
                    || !image.state.is_empty()
                {
                    return Err(unknown("invalid addressed one-row ownership"));
                }
                if image.work.keys().any(|actual| actual != key)
                    || image
                        .versions
                        .keys()
                        .any(|id| id.collection.is_some() || id.key != *key)
                {
                    return Err(unknown("addressed lookup supplied another identity"));
                }
                (*position, key.as_str(), image)
            }
        };
        let entry = entries
            .get(positions)
            .ok_or_else(|| unknown("extra addressed source position"))?;
        if position != positions
            || key != entry.key
            || image.next_sequence != ordinary_image.next_sequence
        {
            return Err(unknown("addressed source position/key/sequence mismatch"));
        }
        validate_snapshot(image)?;
        if image
            .versions
            .iter()
            .any(|(id, version)| id.collection.is_none() && id.key == key && *version == 0)
        {
            return Err(unknown("addressed durable tombstone highwater is zero"));
        }
        let target = image
            .work
            .get(key)
            .map(|_| selected_row(key, image, ordinary_image.next_sequence))
            .transpose()?;
        let _planning = reservation.buffer(target.map(row_bound).transpose()?.unwrap_or(4096))?;
        overlap |= explicit.contains(key);
        let duplicate = !seen.insert(entry.key.as_str());
        let request = TargetRequest {
            expected_version: if duplicate {
                Some(0)
            } else {
                entry.expected_version
            },
            ..entry.into()
        };
        let id = RecordIdentity {
            collection: None,
            key: key.into(),
        };
        let mut proposal = propose_target_view(
            request,
            target,
            image.versions.get(&id).copied(),
            intent.evaluated_at,
            None,
            schema,
            limits,
        )?;
        if duplicate {
            proposal.outcome = AdministrativeOutcome::Invalid;
        }
        if let Some(row) = target
            && let Some(candidate) =
                resolve_candidate(key, row, image, &mut proposal, schema, &reservation)?
        {
            let packet = ResolvedAdministrativeRow {
                scan: scan.clone(),
                ordinal: packets,
                outcome: proposal.outcome.clone(),
                clause: position,
                before: candidate.before,
                delta: candidate.delta,
                effects: candidate.effects,
                accepted: candidate.accepted,
                _charge: candidate._charge.into_owned(Arc::clone(&reservation))?,
            };
            sink(packet)
                .await
                .map_err(|_| unknown("addressed sink failed before EOF"))?;
            packets = packets
                .checked_add(1)
                .ok_or_else(|| unknown("addressed packet ordinal exhausted"))?;
        }
        // Certification remains mandatory even once result retention overflows.
        let before = target
            .map(|row| {
                BorrowedAdministrativeBefore::checked(row, intent.before_fields.as_ref(), schema)
            })
            .transpose()?
            .flatten();
        result.histogram.record(&proposal.outcome)?;
        if !overflow {
            #[derive(serde::Serialize)]
            struct BorrowedItem<'a> {
                key: &'a str,
                outcome: &'a AdministrativeOutcome,
                before: Option<&'a BorrowedAdministrativeBefore<'a>>,
            }
            let item = BorrowedItem {
                key,
                outcome: &proposal.outcome,
                before: before.as_ref(),
            };
            item_bytes = add(item_bytes, wire(&item)?)?;
            let lower = add(
                receipt_base_lower_bound,
                add(
                    item_bytes,
                    add(wire(&result.histogram)?, add(wire(&entries.len())?, 1)?)?,
                )?,
            )?;
            if lower > max_result_bytes {
                overflow = true;
                result.items.clear();
                drop(retention.take());
            } else {
                let retained_bytes =
                    retained_result_bound(key, target, intent.before_fields.as_ref())?;
                let retained_charge = reservation.buffer(retained_bytes)?;
                retention = Some(
                    retention
                        .take()
                        .expect("complete result retains its charge")
                        .merge(retained_charge)?,
                );
                result.items.push(AdministrativeItemResult {
                    key: key.into(),
                    outcome: proposal.outcome,
                    before: before.map(|projection| projection.to_owned()),
                });
            }
        }
        positions = positions
            .checked_add(1)
            .ok_or_else(|| unknown("addressed source count overflow"))?;
    }
    if positions != entries.len() {
        return Err(unknown(
            "addressed source ended before complete positional authority",
        ));
    }
    if let Some(retention) = retention {
        root = root.merge(retention)?;
    }
    Ok(AdmittedAdministrativeScan {
        scan,
        initial_sequence: ordinary_image.next_sequence,
        packet_count: packets,
        result: if overflow {
            super::stream::AdministrativeScanResult::Overflow {
                histogram: result.histogram,
                request_fingerprint,
                max_result_bytes,
            }
        } else {
            super::stream::AdministrativeScanResult::Complete(result)
        },
        overlap,
        _charge: root.into_owned(Arc::clone(&reservation))?,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{ordinary, row, schema};
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use std::collections::BTreeMap;
    fn image() -> RecordSnapshot {
        RecordSnapshot {
            work: BTreeMap::from([("a".into(), row())]),
            versions: BTreeMap::from([
                (
                    RecordIdentity {
                        collection: None,
                        key: "a".into(),
                    },
                    1,
                ),
                (
                    RecordIdentity {
                        collection: None,
                        key: "dead".into(),
                    },
                    7,
                ),
            ]),
            next_sequence: 1,
            ..Default::default()
        }
    }
    fn intent(keys: &[&str]) -> AdministrativeIntent {
        AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(BTreeSet::new()),
            operation: AdministrativeOperation::Addressed(
                keys.iter()
                    .map(|key| AdministrativeEntry {
                        key: (*key).into(),
                        expected_version: None,
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(17),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: Default::default(),
                        },
                    })
                    .collect(),
            ),
        }
    }
    fn borrowed<'a>(
        keys: &'a [&'a str],
        image: &'a RecordSnapshot,
    ) -> impl futures::Stream<Item = QueueResult<AddressedAdministrativeInput<'a, 'a>>> {
        futures::stream::iter(keys.iter().enumerate().map(move |(position, key)| {
            Ok(AddressedAdministrativeInput::BorrowedFullImage {
                position,
                key,
                image,
            })
        }))
    }
    #[test]
    fn overflow_retains_no_partial_result_and_still_requires_full_eof() {
        futures::executor::block_on(async {
            let image = image();
            let keys = ["a", "missing", "a", "dead"];
            let intent = intent(&keys);
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let summary = scan_addressed_group_to_sink_bounded(
                &ordinary(),
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                1,
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            assert!(summary.result_overflowed());
            assert!(summary.result().is_none());
            assert_eq!(summary.histogram().matched, 4);
            assert_eq!(summary.packet_count(), 1);
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
            let truncated = ["a"];
            assert!(
                scan_addressed_group_to_sink_bounded(
                    &ordinary(),
                    &intent,
                    borrowed(&truncated, &image),
                    &image,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    1,
                    |_| std::future::ready(Ok(()))
                )
                .await
                .is_err()
            );
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn discarded_result_proof_cannot_be_reused_under_new_budget_or_shorter_id() {
        futures::executor::block_on(async {
            use crate::queue_publication::{
                AdministrativeResultLimit, assess_administrative_result_limit,
            };
            let image = image();
            let keys = ["a"];
            let intent = intent(&keys);
            let mut group = ordinary();
            group.request_id = "α".repeat(10000);
            let admission = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let summary = scan_addressed_group_to_sink_bounded(
                &group,
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                1024,
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            assert_eq!(
                assess_administrative_result_limit(&group, &summary, &[], 1024).unwrap(),
                AdministrativeResultLimit::TooLarge
            );
            assert!(assess_administrative_result_limit(&group, &summary, &[], u64::MAX).is_err());
            group.request_id = "short".into();
            assert!(assess_administrative_result_limit(&group, &summary, &[], 1024).is_err());
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn final_gate_counts_empty_scans_and_actual_ordinary_version_widths() {
        futures::executor::block_on(async {
            use crate::queue_publication::{
                AdministrativeReceiptOutcome, AdministrativeResultLimit,
                assess_administrative_result_limit, measure_complete_administrative_receipt,
            };
            let image = image();
            let keys = [];
            let intent = intent(&keys);
            let mut group = ordinary();
            group.request_id = "α".repeat(10000);
            group.work_inserts.push(NewWork {
                key: "output\0α".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            });
            let admission = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let summary = scan_addressed_group_to_sink_bounded(
                &group,
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                1,
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            assert!(!summary.result_overflowed()); // No incremental visit; final gate is authoritative.
            for version in [1, 127, 128, u64::MAX] {
                let versions = [RecordVersion {
                    collection: None,
                    key: group.work_inserts[0].key.clone(),
                    version,
                }];
                let size = measure_complete_administrative_receipt(
                    &group.request_id,
                    &AdministrativeReceiptOutcome::Committed {
                        versions: &versions,
                        administration: summary.result().unwrap(),
                    },
                )
                .unwrap();
                assert_eq!(
                    assess_administrative_result_limit(&group, &summary, &versions, size).unwrap(),
                    AdministrativeResultLimit::Fits {
                        complete_receipt_bytes: size
                    }
                );
                assert_eq!(
                    assess_administrative_result_limit(&group, &summary, &versions, size - 1)
                        .unwrap(),
                    AdministrativeResultLimit::TooLarge
                );
            }
            assert!(assess_administrative_result_limit(&group, &summary, &[], u64::MAX).is_err());
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn exact_complete_receipt_boundary_preserves_positions_and_before_values() {
        futures::executor::block_on(async {
            use crate::queue_publication::{
                AdministrativeReceiptOutcome, measure_complete_administrative_receipt,
            };
            let image = image();
            let keys = ["a", "missing", "a", "dead"];
            let intent = intent(&keys);
            let group = ordinary();
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let complete = scan_addressed_group_to_sink(
                &group,
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            let size = measure_complete_administrative_receipt(
                &group.request_id,
                &AdministrativeReceiptOutcome::Committed {
                    versions: &[],
                    administration: complete.result().unwrap(),
                },
            )
            .unwrap();
            let expected = complete.result().unwrap().clone();
            drop(complete);
            for budget in [size, size - 1] {
                let summary = scan_addressed_group_to_sink_bounded(
                    &group,
                    &intent,
                    borrowed(&keys, &image),
                    &image,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    budget,
                    |_| std::future::ready(Ok(())),
                )
                .await
                .unwrap();
                if budget == size {
                    assert_eq!(summary.result(), Some(&expected));
                } else {
                    assert!(summary.result_overflowed());
                    assert!(summary.result().is_none());
                }
                assert_eq!(summary.histogram().matched, 4);
                drop(summary);
                assert_eq!(admission.usage().unwrap(), (1, 0));
            }
        });
    }
    #[test]
    fn overflow_releases_previous_before_charges_before_later_hydration() {
        futures::executor::block_on(async {
            use futures::StreamExt;
            let mut image = image();
            image
                .work
                .get_mut("a")
                .unwrap()
                .record
                .work
                .fields
                .insert("/payload/large".into(), FieldValue::Bytes(vec![3; 262144]));
            let keys = ["a", "a", "missing"];
            let mut intent = intent(&keys);
            intent.before_fields = Some(BTreeSet::from(["/payload/large".into()]));
            let admission = QueueAdmission::new(1, 24 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let retained = std::cell::Cell::new(0u64);
            let released = std::cell::Cell::new(0u64);
            let source = borrowed(&keys, &image).inspect(|input| {
                let Ok(AddressedAdministrativeInput::BorrowedFullImage { position, .. }) = input
                else {
                    unreachable!()
                };
                if *position == 1 {
                    retained.set(admission.usage().unwrap().1);
                }
                if *position == 2 {
                    let current = admission.usage().unwrap().1;
                    released.set(current);
                    assert!(retained.get() > current + (2 << 20));
                    // This working allocation could not fit with discarded
                    // result charges still retained, but now can proceed.
                    let working = job.buffer((24 << 20) - current - 1).unwrap();
                    drop(working);
                }
            });
            let summary = scan_addressed_group_to_sink_bounded(
                &ordinary(),
                &intent,
                source,
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                300000,
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            assert!(summary.result_overflowed());
            assert_eq!(summary.histogram().matched, 3);
            assert!(admission.usage().unwrap().1 <= released.get());
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn late_before_corruption_is_not_hidden_by_overflow() {
        futures::executor::block_on(async {
            let good = image();
            let mut corrupt = image();
            corrupt
                .work
                .get_mut("a")
                .unwrap()
                .record
                .work
                .fields
                .insert("/payload/bad".into(), FieldValue::Float(f64::NAN));
            let keys = ["a", "a"];
            let mut intent = intent(&keys);
            intent.before_fields = Some(BTreeSet::from(["/payload/bad".into()]));
            let inputs = futures::stream::iter([
                Ok(AddressedAdministrativeInput::BorrowedFullImage {
                    position: 0,
                    key: "a",
                    image: &good,
                }),
                Ok(AddressedAdministrativeInput::BorrowedFullImage {
                    position: 1,
                    key: "a",
                    image: &corrupt,
                }),
            ]);
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            assert!(
                scan_addressed_group_to_sink_bounded(
                    &ordinary(),
                    &intent,
                    inputs,
                    &good,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    1,
                    |_| std::future::ready(Ok(()))
                )
                .await
                .is_err()
            );
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn borrowed_before_serialization_matches_owned_projection() {
        let mut image = image();
        image
            .work
            .get_mut("a")
            .unwrap()
            .record
            .work
            .fields
            .insert("/payload/large".into(), FieldValue::Bytes(vec![9; 65536]));
        image
            .work
            .get_mut("a")
            .unwrap()
            .record
            .work
            .fields
            .insert("/payload/zero".into(), FieldValue::Float(-0.0));
        let row = &image.work["a"];
        let names = BTreeSet::from([
            "/payload/large".into(),
            "/payload/zero".into(),
            "/payload/absent".into(),
        ]);
        let schema = schema();
        let borrowed = BorrowedAdministrativeBefore::checked(row, Some(&names), &schema)
            .unwrap()
            .unwrap();
        assert_eq!(
            postcard::to_allocvec(&borrowed).unwrap(),
            postcard::to_allocvec(&borrowed.to_owned()).unwrap()
        );
    }
    #[test]
    fn positions_keep_initial_before_and_duplicate_missing_tombstone_outcomes() {
        futures::executor::block_on(async {
            let image = image();
            let keys = ["a", "a", "missing", "missing", "dead", "dead"];
            let intent = intent(&keys);
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let count = std::cell::Cell::new(0);
            let summary = scan_addressed_group_to_sink(
                &ordinary(),
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                |packet| {
                    assert_eq!(packet.ordinal(), 0);
                    assert_eq!(packet.clause(), 0);
                    assert_eq!(packet.command().unwrap().versions[0].version, 2);
                    count.set(count.get() + 1);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();
            assert_eq!(count.get(), 1);
            assert_eq!(summary.packet_count(), 1);
            let items = &summary.result().unwrap().items;
            assert_eq!(
                items
                    .iter()
                    .map(|item| item.outcome.clone())
                    .collect::<Vec<_>>(),
                vec![
                    AdministrativeOutcome::Changed { version: 2 },
                    AdministrativeOutcome::Invalid,
                    AdministrativeOutcome::NotFound,
                    AdministrativeOutcome::Invalid,
                    AdministrativeOutcome::NotFound,
                    AdministrativeOutcome::Invalid
                ]
            );
            assert_eq!(items[0].before.as_ref().unwrap().version, 1);
            assert_eq!(items[1].before.as_ref().unwrap().version, 1);
            assert!(items[2..].iter().all(|item| item.before.is_none()));
            assert_eq!(summary.result().unwrap().histogram.matched, 6);
            assert_eq!(image.work["a"].record.version, 1);
            assert_eq!(image.work["a"].record.work.priority, 0);
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn truncated_reordered_extra_or_wrong_key_sources_fail_without_summary() {
        futures::executor::block_on(async {
            let image = image();
            let keys = ["a", "missing"];
            let intent = intent(&keys);
            for supplied in [
                vec![(0, "a")],
                vec![(1, "missing"), (0, "a")],
                vec![(0, "missing"), (1, "a")],
                vec![(0, "a"), (1, "missing"), (2, "extra")],
            ] {
                let admission = QueueAdmission::new(1, 1 << 20).unwrap();
                let job = Arc::new(admission.reserve(0).unwrap());
                let source = futures::stream::iter(supplied.into_iter().map(|(position, key)| {
                    Ok(AddressedAdministrativeInput::BorrowedFullImage {
                        position,
                        key,
                        image: &image,
                    })
                }));
                let result = scan_addressed_group_to_sink(
                    &ordinary(),
                    &intent,
                    source,
                    &image,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    |_| std::future::ready(Ok(())),
                )
                .await;
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
                assert_eq!(admission.usage().unwrap(), (1, 0));
            }
        });
    }
    #[test]
    fn retained_before_fields_outlive_hydration_and_capacity_failure_releases_every_copy() {
        futures::executor::block_on(async {
            let mut image = image();
            image
                .work
                .get_mut("a")
                .unwrap()
                .record
                .work
                .fields
                .insert("/payload/large".into(), FieldValue::Bytes(vec![3; 262144]));
            let mut second = image.work["a"].clone();
            second.record.work.key = "b".into();
            second.record.insertion_sequence = 2;
            image.work.insert("b".into(), second);
            image.versions.insert(
                RecordIdentity {
                    collection: None,
                    key: "b".into(),
                },
                1,
            );
            image.next_sequence = 2;
            let keys = ["a", "b"];
            let mut intent = intent(&keys);
            intent.before_fields = Some(BTreeSet::from(["/payload/large".into()]));
            if let AdministrativeOperation::Addressed(entries) = &mut intent.operation {
                for entry in entries {
                    entry.action = super::super::tests::update();
                }
            }
            let admission = QueueAdmission::new(1, 24 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let summary = scan_addressed_group_to_sink(
                &ordinary(),
                &intent,
                borrowed(&keys, &image),
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&job),
                |_| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
            assert!(admission.usage().unwrap().1 > 524288);
            let limited = QueueAdmission::new(1, 8 << 20).unwrap();
            let limited_job = Arc::new(limited.reserve(0).unwrap());
            use futures::StreamExt;
            let produced = std::cell::Cell::new(0);
            let limited_source =
                borrowed(&keys, &image).inspect(|_| produced.set(produced.get() + 1));
            let result = scan_addressed_group_to_sink(
                &ordinary(),
                &intent,
                limited_source,
                &image,
                &schema(),
                &QueueLimits::default(),
                Arc::clone(&limited_job),
                |_| std::future::ready(Ok(())),
            )
            .await;
            assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            assert_eq!(limited.usage().unwrap(), (1, 0));
            assert_eq!(produced.get(), 2); // failure follows a retained first result
            drop(image);
            let FieldValue::Bytes(value) = &summary.result().unwrap().items[1]
                .before
                .as_ref()
                .unwrap()
                .fields["/payload/large"]
            else {
                panic!("bytes")
            };
            assert_eq!(value.len(), 262144);
            drop(summary);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        });
    }
    #[test]
    fn overlap_and_duplicates_do_not_hide_late_corrupt_absence_or_source_error() {
        futures::executor::block_on(async {
            let image = image();
            let keys = ["a", "a", "missing"];
            let intent = intent(&keys);
            for corruption in [false, true] {
                let admission = QueueAdmission::new(1, 1 << 20).unwrap();
                let job = Arc::new(admission.reserve(0).unwrap());
                let mut ordinary = ordinary();
                ordinary.work_updates.push(UnclaimedUpdate {
                    key: "a".into(),
                    expect: StateExpect::Version(1),
                    change: UnclaimedChange::Delete,
                });
                let mut bad = RecordSnapshot {
                    next_sequence: 1,
                    ..Default::default()
                };
                bad.versions.insert(
                    RecordIdentity {
                        collection: None,
                        key: "missing".into(),
                    },
                    0,
                );
                let final_input = if corruption {
                    Ok(AddressedAdministrativeInput::OwnedOneRow {
                        position: 2,
                        key: "missing".into(),
                        image: bad,
                        hydration: job.buffer(4096).unwrap(),
                    })
                } else {
                    Err(unknown("late source fault"))
                };
                let source = futures::stream::iter(vec![
                    Ok(AddressedAdministrativeInput::BorrowedFullImage {
                        position: 0,
                        key: "a",
                        image: &image,
                    }),
                    Ok(AddressedAdministrativeInput::BorrowedFullImage {
                        position: 1,
                        key: "a",
                        image: &image,
                    }),
                    final_input,
                ]);
                let result = scan_addressed_group_to_sink(
                    &ordinary,
                    &intent,
                    source,
                    &image,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    |_| std::future::ready(Ok(())),
                )
                .await;
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
                assert_eq!(admission.usage().unwrap(), (1, 0));
            }
        });
    }
    #[test]
    fn owned_authority_rejects_foreign_job_identity_and_sequence_before_duplicate_classification() {
        futures::executor::block_on(async {
            let keys = ["missing", "missing"];
            let intent = intent(&keys);
            let initial = RecordSnapshot {
                next_sequence: 1,
                ..Default::default()
            };
            for kind in 0..4 {
                let admission = QueueAdmission::new(2, 1 << 20).unwrap();
                let job = Arc::new(admission.reserve(0).unwrap());
                let foreign = Arc::new(admission.reserve(0).unwrap());
                let first = AddressedAdministrativeInput::OwnedOneRow {
                    position: 0,
                    key: "missing".into(),
                    image: initial.clone(),
                    hydration: job.buffer(4096).unwrap(),
                };
                let mut image = initial.clone();
                if kind == 1 {
                    image.versions.insert(
                        RecordIdentity {
                            collection: None,
                            key: "other".into(),
                        },
                        1,
                    );
                }
                if kind == 2 {
                    image.next_sequence = 2;
                }
                if kind == 3 {
                    image.versions.insert(
                        RecordIdentity {
                            collection: Some("foreign".into()),
                            key: "missing".into(),
                        },
                        1,
                    );
                }
                let hydration = if kind == 0 {
                    foreign.buffer(4096).unwrap()
                } else {
                    job.buffer(4096).unwrap()
                };
                let second = AddressedAdministrativeInput::OwnedOneRow {
                    position: 1,
                    key: "missing".into(),
                    image,
                    hydration,
                };
                let source = futures::stream::iter(vec![Ok(first), Ok(second)]);
                let result = scan_addressed_group_to_sink(
                    &ordinary(),
                    &intent,
                    source,
                    &initial,
                    &schema(),
                    &QueueLimits::default(),
                    Arc::clone(&job),
                    |_| std::future::ready(Ok(())),
                )
                .await;
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
                assert_eq!(admission.usage().unwrap(), (2, 0));
            }
        });
    }
    #[test]
    fn suspended_sink_applies_backpressure_and_owner_drop_releases_retained_before() {
        use std::future::Future;
        use std::task::{Context, Poll};
        let mut image = image();
        image
            .work
            .get_mut("a")
            .unwrap()
            .record
            .work
            .fields
            .insert("/payload/large".into(), FieldValue::Bytes(vec![3; 65536]));
        let keys = ["a", "a"];
        let mut intent = intent(&keys);
        intent.before_fields = Some(BTreeSet::from(["/payload/large".into()]));
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let produced = std::cell::Cell::new(0);
        use futures::StreamExt;
        let source = borrowed(&keys, &image).inspect(|_| produced.set(produced.get() + 1));
        let ordinary = ordinary();
        let schema = schema();
        let limits = QueueLimits::default();
        let mut owner = Box::pin(scan_addressed_group_to_sink(
            &ordinary,
            &intent,
            source,
            &image,
            &schema,
            &limits,
            Arc::clone(&job),
            |_| std::future::pending::<QueueResult<()>>(),
        ));
        assert!(matches!(
            owner
                .as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref())),
            Poll::Pending
        ));
        assert_eq!(produced.get(), 1);
        assert!(admission.usage().unwrap().1 > 65536);
        drop(owner);
        assert_eq!(produced.get(), 1);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
}
