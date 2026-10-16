//! Admitted selective field preimages for the resolved-command compiler.
mod selection;
use super::{
    QueueSourceKey, QueueSourceReader, prefix_successor, record_fields_prefix,
    record_fields_prefix_len,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_fields::{CheckedFields, FieldCoverage},
    queue_publication::{CommandRef, EffectRef, FieldsCommandRef, IdentityRef, ValueRef},
};
use fireweed_core::queue_contract::{
    CollectionDefinition, FieldValue, Fields, QueueError, QueueResult,
};
pub use selection::{QueueSourceFieldSelection, load_selected_fields};
use serde::Deserialize;
use std::{collections::BTreeSet, sync::Arc};
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source fields: {message}"))
}

// Same Postcard variant order as FieldValue, with borrowed payloads. Malformed
// lengths cannot allocate an oversized String/Vec before Root admits output.
#[derive(Deserialize)]
enum Value<'a> {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(&'a str),
    Bytes(&'a [u8]),
}
impl Value<'_> {
    fn owned(self) -> FieldValue {
        match self {
            Self::Integer(v) => FieldValue::Integer(v),
            Self::Float(v) => FieldValue::Float(v),
            Self::Boolean(v) => FieldValue::Boolean(v),
            Self::Text(v) => FieldValue::Text(v.into()),
            Self::Bytes(v) => FieldValue::Bytes(v.into()),
        }
    }
}
fn value(bytes: &[u8]) -> QueueResult<Value<'_>> {
    let (value, rest) =
        postcard::take_from_bytes(bytes).map_err(|_| unknown("invalid field fact"))?;
    if !rest.is_empty() {
        return Err(unknown("trailing field fact bytes"));
    }
    Ok(value)
}

pub struct QueueSourceFields {
    fields: Fields,
    coverage: FieldCoverage,
    // Last guards outlive both maps and strings throughout destruction.
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceFields {
    pub(super) fn into_parts(self) -> (Fields, FieldCoverage, OwnedQueueBufferCharge) {
        (self.fields, self.coverage, self._charge)
    }
    pub fn checked(&self) -> QueueResult<CheckedFields<'_>> {
        CheckedFields::new(&self.fields, &self.coverage)
    }
    pub(super) fn complete_fields(&self) -> QueueResult<&Fields> {
        if !matches!(self.coverage, FieldCoverage::Complete) {
            return Err(unknown("complete enumeration outside loaded authority"));
        }
        Ok(&self.fields)
    }
    pub(super) fn complete_entries(
        &self,
    ) -> QueueResult<impl Iterator<Item = (&str, &FieldValue)>> {
        Ok(self
            .complete_fields()?
            .iter()
            .map(|(name, value)| (name.as_str(), value)))
    }
    pub fn present_count(&self) -> usize {
        self.fields.len()
    }
    fn charge(&mut self, bytes: usize, job: &Arc<QueueReservation>) -> QueueResult<()> {
        let bytes = bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        if !std::ptr::eq(self._charge.reservation(), job.as_ref()) {
            return Err(unknown("field output operation differs"));
        }
        self._charge
            .grow(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)
    }
    fn retain(
        &mut self,
        name: &str,
        bytes: &[u8],
        definition: &CollectionDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        // Parse borrowing first; owned payloads and both tree nodes are admitted.
        let parsed = value(bytes)?;
        self.charge(
            name.len()
                .checked_add(bytes.len())
                .ok_or(QueueError::TooLarge)?,
            job,
        )?;
        let owned = parsed.owned();
        definition
            .validate_value(name, &owned)
            .map_err(|_| unknown("field fact violates queue schema"))?;
        if self.fields.insert(name.into(), owned).is_some() {
            return Err(unknown("duplicate field fact"));
        }
        Ok(())
    }
    fn name(&mut self, name: &str, job: &Arc<QueueReservation>) -> QueueResult<()> {
        let FieldCoverage::Selected { names, .. } = &self.coverage else {
            return Ok(());
        };
        if names.contains(name) {
            return Ok(());
        }
        self.charge(name.len(), job)?;
        let FieldCoverage::Selected { names, .. } = &mut self.coverage else {
            unreachable!()
        };
        names.insert(name.into());
        Ok(())
    }
}
/// No query/cache facts enter here. Replace and delete require complete source
/// membership; Edit reads only explicit names and columns of affected indexes;
/// Preserve performs no field I/O. The caller retains the record fact proving
/// expected_count at this same reader cut before compiling any output.
pub async fn load_command_fields<R: QueueSourceReader>(
    reader: &R,
    command: CommandRef<'_>,
    expected_count: u64,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceFields> {
    let cut = reader.cut()?;
    if !cut.belongs_to(job) {
        return Err(unknown("reader operation differs"));
    }
    let charge = job.owned_buffer((std::mem::size_of::<QueueSourceFields>() + 1024) as u64)?;
    let mut output = QueueSourceFields {
        fields: Fields::new(),
        coverage: FieldCoverage::Selected {
            names: BTreeSet::new(),
            prefixes: BTreeSet::new(),
        },
        _charge: charge,
    };
    let fields = match command.effect {
        EffectRef::Delete | EffectRef::StatePut { .. } => None,
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Replace(_),
            ..
        } => None,
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Preserve,
            ..
        } => return Ok(output),
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Edit(edits),
            ..
        } => Some(edits),
    };
    if let Some(edits) = fields {
        for entry in edits.iter() {
            output.name(
                entry
                    .map_err(|_| unknown("invalid resolved field command"))?
                    .0,
                job,
            )?;
        }
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let explicit_bytes = names.iter().try_fold(0usize, |n, name| {
            n.checked_add(name.len())
                .and_then(|n| n.checked_add(1024))
                .ok_or(QueueError::TooLarge)
        })?;
        output.charge(explicit_bytes, job)?;
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let explicit = names.clone();
        for columns in definition.indexes.values() {
            if columns.iter().any(|name| explicit.contains(name)) {
                for name in columns {
                    output.name(name, job)?;
                }
            }
        }
        fill_selected(reader, cut, command.identity, definition, job, &mut output).await?;
    } else {
        fill_complete(
            reader,
            cut,
            command.identity,
            expected_count,
            definition,
            job,
            &mut output,
        )
        .await?;
    }
    Ok(output)
}
// Shared source-loading machinery. A planning request declares field knowledge
// directly; it must not manufacture a resolved write command to read a preimage.
pub(super) async fn load_group_fields<R: QueueSourceReader>(
    reader: &R,
    cut: super::QueueSourceCut<'_>,
    identity: IdentityRef<'_>,
    expected_count: u64,
    definition: &CollectionDefinition,
    names: Option<&BTreeSet<String>>,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceFields> {
    if !cut.belongs_to(job) {
        return Err(unknown("reader operation differs"));
    }
    if let Some(names) = names {
        return load_selected_fields(
            reader,
            cut,
            identity,
            expected_count,
            definition,
            QueueSourceFieldSelection {
                names,
                prefixes: &BTreeSet::new(),
            },
            job,
        )
        .await;
    }
    let charge = job.owned_buffer((std::mem::size_of::<QueueSourceFields>() + 1024) as u64)?;
    let mut output = QueueSourceFields {
        fields: Fields::new(),
        coverage: FieldCoverage::Selected {
            names: BTreeSet::new(),
            prefixes: BTreeSet::new(),
        },
        _charge: charge,
    };
    fill_complete(
        reader,
        cut,
        identity,
        expected_count,
        definition,
        job,
        &mut output,
    )
    .await?;
    if output.present_count() as u64 > expected_count {
        return Err(unknown(
            "selected fields exceed authenticated membership count",
        ));
    }
    Ok(output)
}
async fn fill_selected<R: QueueSourceReader>(
    reader: &R,
    cut: super::QueueSourceCut<'_>,
    identity: IdentityRef<'_>,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
    output: &mut QueueSourceFields,
) -> QueueResult<()> {
    // Names are already charged. Borrow them independently of the output
    // field tree so retaining values cannot invalidate the traversal.
    let FieldCoverage::Selected { names, .. } = &output.coverage else {
        unreachable!()
    };
    let names_bytes = names.iter().try_fold(0usize, |n, name| {
        n.checked_add(name.len())
            .and_then(|n| n.checked_add(std::mem::size_of::<String>()))
            .ok_or(QueueError::TooLarge)
    })?;
    output.charge(names_bytes, job)?;
    let FieldCoverage::Selected { names, .. } = &output.coverage else {
        unreachable!()
    };
    let names: Vec<_> = names.iter().cloned().collect();
    for name in names {
        let key = QueueSourceKey::Field {
            collection: identity.collection,
            key: identity.key,
            field: &name,
        };
        let _key_charge = job.owned_buffer(key.encoded_len()? as u64)?;
        let key = key.encode()?;
        let owner = reader.lookup(&key).await?;
        let point = reader.point(&owner)?;
        if let Some(bytes) = point.value_for(&cut, &key)? {
            output.retain(&name, bytes, definition, job)?;
        }
    }
    Ok(())
}
async fn fill_complete<R: QueueSourceReader>(
    reader: &R,
    cut: super::QueueSourceCut<'_>,
    identity: IdentityRef<'_>,
    expected_count: u64,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
    output: &mut QueueSourceFields,
) -> QueueResult<()> {
    let len = record_fields_prefix_len(identity.collection, identity.key)?;
    let _bounds = job.owned_buffer(len.checked_mul(2).ok_or(QueueError::TooLarge)? as u64)?;
    let lower = record_fields_prefix(identity.collection, identity.key)?;
    let mut upper = lower.clone();
    if !prefix_successor(&mut upper) {
        return Err(unknown("field family has no upper bound"));
    }
    let range = reader.complete_range(&lower, &upper).await?;
    reader.visit_range(&range, &lower, &upper, &mut |point| {
        let bytes = point
            .value_for(&cut, point.key())?
            .ok_or_else(|| unknown("absent field inside membership range"))?;
        let suffix = point
            .key()
            .strip_prefix(lower.as_slice())
            .ok_or_else(|| unknown("field range prefix differs"))?;
        let _name_charge = job.owned_buffer(
            suffix
                .len()
                .checked_add(std::mem::size_of::<String>())
                .ok_or(QueueError::TooLarge)? as u64,
        )?;
        let name = field_name(suffix)?;
        output.retain(&name, bytes, definition, job)
    })?;
    if output.fields.len() as u64 != expected_count {
        return Err(unknown("complete field membership count differs"));
    }
    output.coverage = FieldCoverage::Complete;
    Ok(())
}
fn field_name(mut suffix: &[u8]) -> QueueResult<String> {
    let mut bytes = Vec::with_capacity(suffix.len());
    while let Some((&byte, rest)) = suffix.split_first() {
        suffix = rest;
        if byte != 0 {
            bytes.push(byte);
            continue;
        }
        match suffix.split_first() {
            Some((&255, rest)) => {
                bytes.push(0);
                suffix = rest;
            }
            Some((&0, [])) => {
                return String::from_utf8(bytes).map_err(|_| unknown("invalid field name UTF-8"));
            }
            _ => return Err(unknown("noncanonical field name terminator")),
        }
    }
    Err(unknown("unterminated field name"))
}

/// Borrow the source scalar without a second allocation when compiling postings.
pub fn source_field_value(bytes: &[u8]) -> QueueResult<ValueRef<'_>> {
    Ok(match value(bytes)? {
        Value::Integer(v) => ValueRef::Integer(v),
        Value::Float(v) => ValueRef::Float(v),
        Value::Boolean(v) => ValueRef::Boolean(v),
        Value::Text(v) => ValueRef::Text(v),
        Value::Bytes(v) => ValueRef::Bytes(v),
    })
}

#[cfg(test)]
mod tests;
