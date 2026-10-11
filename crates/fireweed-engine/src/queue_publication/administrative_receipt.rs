//! Private future complete-receipt layout. Public activation must append the
//! corresponding variants without altering legacy ones. These bytes carry no
//! execution/source/request authority and must not be acknowledged standalone.
use super::*;
use fireweed_core::queue_administration::AdministrativeResult;
use serde::{Serialize, ser::SerializeStructVariant};

pub enum AdministrativeReceiptOutcome<'a> {
    Committed {
        versions: &'a [RecordVersion],
        administration: &'a AdministrativeResult,
    },
    Rejected(&'a Rejection),
    TooLarge,
}
struct TooLargeTag;
impl Serialize for TooLargeTag {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_unit_variant("Rejection", 8, "TooLarge")
    }
}
impl Serialize for AdministrativeReceiptOutcome<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Committed {
                versions,
                administration,
            } => {
                let mut variant =
                    s.serialize_struct_variant("GroupOutcome", 4, "AdministrativeCommitted", 2)?;
                variant.serialize_field("versions", versions)?;
                variant.serialize_field("administration", administration)?;
                variant.end()
            }
            Self::Rejected(reason) => {
                s.serialize_newtype_variant("GroupOutcome", 3, "Rejected", reason)
            }
            Self::TooLarge => {
                s.serialize_newtype_variant("GroupOutcome", 3, "Rejected", &TooLargeTag)
            }
        }
    }
}
#[derive(Serialize)]
struct CompleteReceipt<'a, T: Serialize + ?Sized> {
    request_id: &'a str,
    outcome: &'a T,
}
fn checked(request_id: &str, outcome: &AdministrativeReceiptOutcome<'_>) -> PublicationResult<()> {
    if request_id.trim().is_empty() {
        return invalid("empty administrative receipt ID");
    }
    if let AdministrativeReceiptOutcome::Rejected(Rejection::Invalid(reason)) = outcome
        && reason.len() > 256
    {
        return invalid("administrative rejection diagnostic exceeds 256 UTF-8 bytes");
    }
    if let AdministrativeReceiptOutcome::Committed {
        versions,
        administration,
    } = outcome
    {
        if versions.iter().any(|version| version.version == 0) {
            return invalid("zero ordinary administrative receipt version");
        }
        measure_administrative_result(administration)?;
    }
    Ok(())
}
/// Monotone lower bound for complete successful receipt framing, excluding the
/// administrative result itself. Every ordinary output keeps its full identity;
/// its positive version costs at least the one byte used by version 1.
pub fn measure_administrative_receipt_base_lower_bound(
    group: &CommitGroup,
) -> PublicationResult<u64> {
    let count = group
        .claimed_updates
        .len()
        .checked_add(group.state_updates.len())
        .and_then(|n| n.checked_add(group.work_updates.len()))
        .and_then(|n| n.checked_add(group.work_inserts.len()))
        .ok_or(PublicationError::Overflow)?;
    let mut bytes = encoded_len(&(&group.request_id, 4u32, count as u64))
        .map_err(PublicationError::Admission)?;
    for (collection, key) in group
        .claimed_updates
        .iter()
        .map(|item| (None, item.key.as_str()))
        .chain(
            group
                .state_updates
                .iter()
                .map(|item| (Some(item.collection.as_str()), item.key.as_str())),
        )
        .chain(
            group
                .work_updates
                .iter()
                .map(|item| (None, item.key.as_str())),
        )
        .chain(
            group
                .work_inserts
                .iter()
                .map(|item| (None, item.key.as_str())),
        )
    {
        bytes = bytes
            .checked_add(
                encoded_len(&(collection, key, 1u64)).map_err(PublicationError::Admission)?,
            )
            .ok_or(PublicationError::Overflow)?;
    }
    Ok(bytes)
}
/// Result-size decision after complete business validation. Overflow summaries
/// cannot be serialized; complete summaries count the actual ordinary versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdministrativeResultLimit {
    Fits { complete_receipt_bytes: u64 },
    TooLarge,
}
pub fn assess_administrative_result_limit(
    group: &CommitGroup,
    scan: &crate::queue_administration::AdmittedAdministrativeScan,
    versions: &[RecordVersion],
    max_result_bytes: u64,
) -> PublicationResult<AdministrativeResultLimit> {
    validate_commit_result_versions(group, versions)?;
    scan.verify_overflow_context(group, max_result_bytes)
        .map_err(PublicationError::Admission)?;
    let Some(administration) = scan.result() else {
        return Ok(AdministrativeResultLimit::TooLarge);
    };
    let bytes = measure_complete_administrative_receipt(
        &group.request_id,
        &AdministrativeReceiptOutcome::Committed {
            versions,
            administration,
        },
    )?;
    Ok(if bytes > max_result_bytes {
        AdministrativeResultLimit::TooLarge
    } else {
        AdministrativeResultLimit::Fits {
            complete_receipt_bytes: bytes,
        }
    })
}
/// Exact full Postcard CommitReceipt size: request ID, outcome, all ordinary
/// producer-order versions and complete administrative result. This differs from
/// FWAR1's fixed-width canonical binary component and includes no envelope list.
pub fn measure_complete_administrative_receipt(
    request_id: &str,
    outcome: &AdministrativeReceiptOutcome<'_>,
) -> PublicationResult<u64> {
    checked(request_id, outcome)?;
    encoded_len(&CompleteReceipt {
        request_id,
        outcome,
    })
    .map_err(PublicationError::Admission)
}
/// Preflight floor for either complete durable fallback. The maximum Invalid
/// message is fixed at 256 bytes; long request identities are counted in full.
pub fn measure_administrative_receipt_fallbacks(request_id: &str) -> PublicationResult<u64> {
    struct InvalidReason<'a>(&'a str);
    impl Serialize for InvalidReason<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_newtype_variant("Rejection", 5, "Invalid", self.0)
        }
    }
    struct RejectedInvalid<'a>(&'a str);
    impl Serialize for RejectedInvalid<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_newtype_variant("GroupOutcome", 3, "Rejected", &InvalidReason(self.0))
        }
    }
    checked(request_id, &AdministrativeReceiptOutcome::TooLarge)?;
    let reason = [b'x'; 256];
    let reason = std::str::from_utf8(&reason).expect("ASCII fallback");
    let invalid = encoded_len(&CompleteReceipt {
        request_id,
        outcome: &RejectedInvalid(reason),
    })
    .map_err(PublicationError::Admission)?;
    Ok(invalid.max(measure_complete_administrative_receipt(
        request_id,
        &AdministrativeReceiptOutcome::TooLarge,
    )?))
}

pub struct EncodedAdministrativeReceipt<'r> {
    bytes: Vec<u8>,
    _charge: QueueBufferCharge<'r>,
}
impl EncodedAdministrativeReceipt<'_> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub fn encode_complete_administrative_receipt<'r>(
    request_id: &str,
    outcome: &AdministrativeReceiptOutcome<'_>,
    reservation: &'r QueueReservation,
) -> PublicationResult<EncodedAdministrativeReceipt<'r>> {
    let size = measure_complete_administrative_receipt(request_id, outcome)?;
    let charge = reservation
        .buffer(size.checked_add(128).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| PublicationError::Overflow)?)
        .map_err(|_| PublicationError::Overflow)?;
    postcard::to_io(
        &CompleteReceipt {
            request_id,
            outcome,
        },
        &mut bytes,
    )
    .map_err(|_| PublicationError::Invalid("administrative receipt encoding failed"))?;
    if bytes.len() as u64 != size {
        return invalid("administrative receipt size changed");
    }
    Ok(EncodedAdministrativeReceipt {
        bytes,
        _charge: charge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use serde::Deserialize;
    use sha2::Digest;
    #[test]
    fn receipt_base_preserves_full_identities_and_is_monotone_across_versions() {
        let group = CommitGroup {
            request_id: "α\0".repeat(10000),
            claimed_updates: vec![ClaimedUpdate {
                key: "claimedα".into(),
                lease: LeaseCredential { token: vec![1] },
                expected_version: 1,
                patch: WorkPatch::default(),
                disposition: Disposition::Complete,
            }],
            state_updates: vec![StateUpdate {
                collection: "stateα".repeat(1000),
                key: "state\0".into(),
                expect: StateExpect::Version(1),
                change: StateChange::Delete,
            }],
            work_updates: vec![UnclaimedUpdate {
                key: "updated".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Delete,
            }],
            work_inserts: vec![NewWork {
                key: "inserted".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            }],
        };
        let result = AdministrativeResult {
            histogram: Default::default(),
            items: vec![],
            clauses: vec![],
        };
        let result_size = encoded_len(&result).unwrap();
        let floor = measure_administrative_receipt_base_lower_bound(&group).unwrap();
        for version in [1, 127, 128, u64::MAX] {
            let versions = vec![
                RecordVersion {
                    collection: None,
                    key: group.claimed_updates[0].key.clone(),
                    version,
                },
                RecordVersion {
                    collection: Some(group.state_updates[0].collection.clone()),
                    key: group.state_updates[0].key.clone(),
                    version,
                },
                RecordVersion {
                    collection: None,
                    key: group.work_updates[0].key.clone(),
                    version,
                },
                RecordVersion {
                    collection: None,
                    key: group.work_inserts[0].key.clone(),
                    version,
                },
            ];
            let actual = measure_complete_administrative_receipt(
                &group.request_id,
                &AdministrativeReceiptOutcome::Committed {
                    versions: &versions,
                    administration: &result,
                },
            )
            .unwrap()
                - result_size;
            assert!(floor <= actual);
            if version <= 127 {
                assert_eq!(floor, actual);
            } else {
                assert!(floor < actual);
            }
        }
    }
    // Independent future public layout; never derived from the live projection.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum FutureReason {
        StaleLease,
        VersionConflict,
        AlreadyExists,
        NotFound,
        Leased,
        Invalid(String),
        UniqueConflict,
        Terminal,
        TooLarge,
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum FutureOutcome {
        RequestConflict,
        RequestExpired,
        Committed {
            versions: Vec<RecordVersion>,
        },
        Rejected(FutureReason),
        AdministrativeCommitted {
            versions: Vec<RecordVersion>,
            administration: AdministrativeResult,
        },
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct FutureReceipt {
        request_id: String,
        outcome: FutureOutcome,
    }
    fn empty() -> AdministrativeResult {
        AdministrativeResult {
            histogram: Default::default(),
            items: vec![],
            clauses: vec![],
        }
    }
    #[test]
    fn appended_tags_and_complete_empty_receipt_have_literal_wire_goldens() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let result = empty();
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &[],
            administration: &result,
        };
        let encoded = encode_complete_administrative_receipt("receipt\0α", &outcome, &job).unwrap();
        assert_eq!(encoded.as_bytes(), b"\x0areceipt\0\xce\xb1\x04\0\0\0\0\0");
        assert_eq!(
            measure_complete_administrative_receipt("receipt\0α", &outcome).unwrap(),
            17
        );
        let decoded: FutureReceipt = postcard::from_bytes(encoded.as_bytes()).unwrap();
        assert_eq!(
            decoded,
            FutureReceipt {
                request_id: "receipt\0α".into(),
                outcome: FutureOutcome::AdministrativeCommitted {
                    versions: vec![],
                    administration: result
                }
            }
        );
        let too_large = encode_complete_administrative_receipt(
            "r",
            &AdministrativeReceiptOutcome::TooLarge,
            &job,
        )
        .unwrap();
        assert_eq!(too_large.as_bytes(), b"\x01r\x03\x08");
        assert_eq!(
            postcard::from_bytes::<FutureReceipt>(too_large.as_bytes())
                .unwrap()
                .outcome,
            FutureOutcome::Rejected(FutureReason::TooLarge)
        );
    }
    #[test]
    fn rich_complete_receipt_matches_future_derived_layout_with_producer_order_versions() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let administration = super::super::administrative_result::tests::rich_result();
        let versions = vec![
            RecordVersion {
                collection: Some("instances".into()),
                key: "state\0β".into(),
                version: u64::MAX,
            },
            RecordVersion {
                collection: None,
                key: "next".into(),
                version: 1,
            },
        ];
        let projected = AdministrativeReceiptOutcome::Committed {
            versions: &versions,
            administration: &administration,
        };
        let encoded = encode_complete_administrative_receipt("rich", &projected, &job).unwrap();
        let future = FutureReceipt {
            request_id: "rich".into(),
            outcome: FutureOutcome::AdministrativeCommitted {
                versions: versions.clone(),
                administration: administration.clone(),
            },
        };
        assert_eq!(encoded.as_bytes(), postcard::to_allocvec(&future).unwrap());
        assert_eq!(encoded.as_bytes().len(), 365);
        assert_eq!(
            postcard::from_bytes::<FutureReceipt>(encoded.as_bytes()).unwrap(),
            future
        );
        assert_eq!(
            measure_complete_administrative_receipt("rich", &projected).unwrap(),
            encoded.as_bytes().len() as u64
        );
        assert_ne!(
            measure_administrative_result(&administration).unwrap(),
            encoded.as_bytes().len() as u64
        );
        assert_eq!(
            sha2::Sha256::digest(encoded.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "ebca62cc4a0ce01e572f6107f3a59ecf96ccc5384614c4c187f991b32e7e57b8"
        );
    }
    #[test]
    fn fallback_measures_full_long_identity_and_worst_bounded_invalid_without_clones() {
        let id = format!("id\0{}α", "x".repeat(80000));
        let future = FutureReceipt {
            request_id: id.clone(),
            outcome: FutureOutcome::Rejected(FutureReason::Invalid("x".repeat(256))),
        };
        assert_eq!(
            measure_administrative_receipt_fallbacks(&id).unwrap(),
            postcard::to_allocvec(&future).unwrap().len() as u64
        );
        assert!(measure_administrative_receipt_fallbacks(&id).unwrap() > 80000 + 256);
        let admission = QueueAdmission::new(1, 1000).unwrap();
        let job = admission.reserve(0).unwrap();
        assert!(matches!(
            encode_complete_administrative_receipt(
                &id,
                &AdministrativeReceiptOutcome::TooLarge,
                &job
            ),
            Err(PublicationError::Admission(QueueError::OutcomeUnknown(_)))
        ));
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn all_legacy_rejected_reason_ordinals_are_unchanged_in_complete_future_layout() {
        let reasons = [
            Rejection::StaleLease,
            Rejection::VersionConflict,
            Rejection::AlreadyExists,
            Rejection::NotFound,
            Rejection::Leased,
            Rejection::Invalid("why".into()),
            Rejection::UniqueConflict,
            Rejection::Terminal,
        ];
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        for (tag, reason) in reasons.iter().enumerate() {
            let encoded = encode_complete_administrative_receipt(
                "r",
                &AdministrativeReceiptOutcome::Rejected(reason),
                &job,
            )
            .unwrap();
            assert_eq!(encoded.as_bytes()[2], 3);
            assert_eq!(encoded.as_bytes()[3], tag as u8);
            let legacy = CommitReceipt {
                request_id: "r".into(),
                outcome: GroupOutcome::Rejected(reason.clone()),
            };
            assert_eq!(encoded.as_bytes(), postcard::to_allocvec(&legacy).unwrap());
        }
    }
    #[test]
    fn fallback_is_an_upper_bound_on_accepted_utf8_invalid_diagnostics() {
        let accepted = Rejection::Invalid("α".repeat(128));
        let outcome = AdministrativeReceiptOutcome::Rejected(&accepted);
        assert_eq!(
            measure_complete_administrative_receipt("r", &outcome).unwrap(),
            measure_administrative_receipt_fallbacks("r").unwrap()
        );
        for text in ["x".repeat(257), "α".repeat(129)] {
            let rejected = Rejection::Invalid(text);
            let outcome = AdministrativeReceiptOutcome::Rejected(&rejected);
            assert!(measure_complete_administrative_receipt("r", &outcome).is_err());
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = admission.reserve(0).unwrap();
            assert!(encode_complete_administrative_receipt("r", &outcome, &job).is_err());
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
}
