//! Exact scalar probes. Bounds retain their original operation admission; they
//! establish neither source range completion nor a selected-row authority seal.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_index::{borrowed_value, ordered_key_ref, ordered_key_ref_len},
    queue_schema_codec::{AdmittedQueueDefinition, DefinitionPin},
    queue_source_index::{QueueSourcePostingFamily, prefix_successor},
};
use fireweed_core::queue_contract::{
    Bound, FieldType, FieldValue, Predicate, QueueError, QueueResult, encoded_len,
};
use std::sync::Arc;

pub struct QueueSourceScalarRange {
    lower: Vec<u8>,
    upper: Vec<u8>,
    kind: FieldType,
}
impl QueueSourceScalarRange {
    pub fn lower(&self) -> &[u8] {
        &self.lower
    }
    pub fn upper(&self) -> &[u8] {
        &self.upper
    }
    pub fn field_type(&self) -> FieldType {
        self.kind
    }
}
pub struct QueueSourceScalarRanges<'a> {
    collection: Option<&'a str>,
    field: &'a str,
    pin: DefinitionPin,
    ranges: Vec<QueueSourceScalarRange>,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceScalarRanges<'_> {
    pub fn family(&self) -> QueueSourcePostingFamily<'_> {
        QueueSourcePostingFamily::Scalar {
            collection: self.collection,
            field: self.field,
        }
    }
    pub fn definition_pin(&self) -> DefinitionPin {
        self.pin
    }
    pub fn ranges(&self) -> &[QueueSourceScalarRange] {
        &self.ranges
    }
    pub fn belongs_to(&self, job: &QueueReservation) -> bool {
        std::ptr::eq(self._charge.reservation(), job)
    }
}
/// Conservative allocation/copy ceiling shared with the driven producer.
/// The producer spends work before this planner allocates any range buffers.
pub(super) fn scalar_plan_capacity(
    family: QueueSourcePostingFamily<'_>,
    predicate: &Predicate,
) -> QueueResult<u64> {
    let count = match predicate {
        Predicate::In(values) => values.len(),
        _ => 1,
    };
    encoded_len(predicate)?
        .checked_mul(32)
        .and_then(|n| {
            (family.prefix_len().ok()? as u64)
                .checked_mul(2)
                .and_then(|prefix| prefix.checked_add(512))
                .and_then(|per| per.checked_mul(count as u64))
                .and_then(|extra| n.checked_add(extra))
        })
        .and_then(|n| n.checked_add(4096))
        .ok_or(QueueError::TooLarge)
}

/// Use the admitted persisted definition and common predicate validation. The
/// selection driver must additionally bind this plan to its original header,
/// request, evaluation time, scratch producer and complete range manifest.
pub fn plan_source_scalar_ranges<'a>(
    definition: &AdmittedQueueDefinition,
    collection: Option<&'a str>,
    field: &'a str,
    predicate: &Predicate,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceScalarRanges<'a>> {
    if !definition.belongs_to(job) {
        return Err(unknown("definition operation differs"));
    }
    let declaration = match collection {
        None => &definition.schema().work,
        Some(name) => definition
            .schema()
            .state
            .get(name)
            .ok_or_else(|| unknown("collection differs"))?,
    };
    declaration.validate_predicate(field, predicate)?;
    let family = QueueSourcePostingFamily::Scalar { collection, field };
    let count = match predicate {
        Predicate::In(values) => values.len(),
        _ => 1,
    };
    // Ordered values can expand twice (nested zero escaping), then both bounds
    // and a temporary encoding coexist. Include full vector/header capacity.
    let capacity = scalar_plan_capacity(family, predicate)?;
    let charge = job.owned_buffer(capacity)?;
    let mut ranges = Vec::with_capacity(count);
    match predicate {
        Predicate::Equal(value) => ranges.push(equal(family, value)?),
        Predicate::In(values) => {
            for value in values {
                ranges.push(equal(family, value)?);
            }
            ranges.sort_unstable_by(|a, b| a.lower.cmp(&b.lower));
            ranges.dedup_by(|a, b| a.lower == b.lower);
        }
        Predicate::Range { lower, upper } => {
            let kind = lower
                .as_ref()
                .or(upper.as_ref())
                .map(bound_value)
                .ok_or_else(|| unknown("empty range"))?
                .field_type();
            // Construct only the chosen bound. Replacing preallocated type
            // bounds would keep a third full namespace buffer alive on the RHS.
            let low = match lower {
                Some(bound) => {
                    let mut bytes = value_prefix(family, bound_value(bound))?;
                    if matches!(bound, Bound::Excluded(_)) {
                        successor(&mut bytes)?;
                    }
                    bytes
                }
                None => family.scalar_type_prefix(kind)?,
            };
            let high = match upper {
                Some(bound) => {
                    let mut bytes = value_prefix(family, bound_value(bound))?;
                    if matches!(bound, Bound::Included(_)) {
                        successor(&mut bytes)?;
                    }
                    bytes
                }
                None => {
                    let mut bytes = family.scalar_type_prefix(kind)?;
                    successor(&mut bytes)?;
                    bytes
                }
            };
            if low < high {
                ranges.push(QueueSourceScalarRange {
                    lower: low,
                    upper: high,
                    kind,
                });
            }
        }
    }
    Ok(QueueSourceScalarRanges {
        collection,
        field,
        pin: definition.pin(),
        ranges,
        _charge: charge,
    })
}
fn bound_value(bound: &Bound) -> &FieldValue {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => value,
    }
}
fn value_prefix(family: QueueSourcePostingFamily<'_>, value: &FieldValue) -> QueueResult<Vec<u8>> {
    let values = [Some(borrowed_value(value))];
    // Validate before the codec allocates. The root admission already covers
    // the complete measured encoding and nested escaped bounds.
    ordered_key_ref_len(&values)?;
    family.value_prefix(&ordered_key_ref(&values)?)
}
fn equal(
    family: QueueSourcePostingFamily<'_>,
    value: &FieldValue,
) -> QueueResult<QueueSourceScalarRange> {
    let lower = value_prefix(family, value)?;
    let mut upper = lower.clone();
    successor(&mut upper)?;
    Ok(QueueSourceScalarRange {
        lower,
        upper,
        kind: value.field_type(),
    })
}
fn successor(bytes: &mut Vec<u8>) -> QueueResult<()> {
    if prefix_successor(bytes) {
        Ok(())
    } else {
        Err(unknown("range has no finite upper bound"))
    }
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source scalar plan: {reason}"))
}
#[cfg(test)]
mod tests;

/// Presence covers one declared type, or every admitted type for an additional
/// field namespace. A value predicate's range is not a presence substitute.
pub(super) fn presence_plan_capacity(family: QueueSourcePostingFamily<'_>) -> QueueResult<u64> {
    (family.prefix_len()? as u64)
        .checked_add(32)
        .and_then(|n| n.checked_mul(10))
        .and_then(|n| n.checked_add(4096))
        .ok_or(QueueError::TooLarge)
}
pub(super) fn plan_source_presence_ranges<'a>(
    definition: &AdmittedQueueDefinition,
    collection: Option<&'a str>,
    field: &'a str,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceScalarRanges<'a>> {
    if !definition.belongs_to(job) {
        return Err(unknown("presence definition operation differs"));
    }
    let declaration = match collection {
        None => &definition.schema().work,
        Some(name) => definition
            .schema()
            .state
            .get(name)
            .ok_or_else(|| unknown("presence collection differs"))?,
    };
    if !declaration.admits_field(field) {
        return Err(unknown("presence field is undeclared"));
    }
    let family = QueueSourcePostingFamily::Scalar { collection, field };
    let charge = job.owned_buffer(presence_plan_capacity(family)?)?;
    let mut ranges = Vec::with_capacity(5);
    for kind in [
        FieldType::Integer,
        FieldType::Float,
        FieldType::Boolean,
        FieldType::Text,
        FieldType::Bytes,
    ] {
        if declaration
            .fields
            .get(field)
            .is_some_and(|expected| *expected != kind)
        {
            continue;
        }
        let lower = family.scalar_type_prefix(kind)?;
        let mut upper = lower.clone();
        successor(&mut upper)?;
        ranges.push(QueueSourceScalarRange { lower, upper, kind });
    }
    Ok(QueueSourceScalarRanges {
        collection,
        field,
        pin: definition.pin(),
        ranges,
        _charge: charge,
    })
}
