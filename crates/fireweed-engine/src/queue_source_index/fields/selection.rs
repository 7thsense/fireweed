//! Declared field-name and prefix authority for administrative hydration.
use super::*;
use crate::queue_source_index::{record_field_name_prefix, record_field_name_prefix_len};

/// Borrowed field declarations; retained coverage and values receive their own
/// operation funding before copying.
#[derive(Clone, Copy)]
pub struct QueueSourceFieldSelection<'a> {
    pub names: &'a BTreeSet<String>,
    pub prefixes: &'a BTreeSet<String>,
}

/// Load only declared literal names and complete literal-name prefixes from
/// the original metadata cut. The caller supplies the record identity, membership
/// count and schema authenticated at that cut. Reader identity is checked at entry;
/// it never replaces the original cut used for all subsequent evidence checks.
/// No absent name or prefix is certified until every source read succeeds.
pub async fn load_selected_fields<R: QueueSourceReader>(
    reader: &R,
    cut: crate::queue_source_index::QueueSourceCut<'_>,
    identity: IdentityRef<'_>,
    expected_count: u64,
    definition: &CollectionDefinition,
    selection: QueueSourceFieldSelection<'_>,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceFields> {
    let QueueSourceFieldSelection { names, prefixes } = selection;
    if !cut.belongs_to(job) {
        return Err(unknown("reader operation differs"));
    }
    // Bind even an empty range to the original metadata authority. This checks
    // the reader identity; it never replaces the caller's comparison cut.
    if !reader.cut()?.same_cut(&cut) {
        return Err(unknown("reader cut differs from original field authority"));
    }
    if prefixes.iter().any(|prefix| prefix.trim().is_empty()) {
        return Err(unknown("empty field-coverage prefix"));
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
    // Sorted prefixes need only one pass to discard descendants. The admitted
    // borrowed vector also supports logarithmic coverage checks for exact names.
    let _normalized_charge = job.owned_buffer(
        prefixes
            .len()
            .checked_mul(std::mem::size_of::<&str>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Vec<&str>>()))
            .ok_or(QueueError::TooLarge)? as u64,
    )?;
    let mut normalized: Vec<&str> = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !normalized
            .last()
            .is_some_and(|parent| prefix.starts_with(parent))
        {
            normalized.push(prefix);
        }
    }
    for name in names {
        let preceding = normalized.partition_point(|prefix| *prefix <= name.as_str());
        if preceding == 0 || !name.starts_with(normalized[preceding - 1]) {
            output.name(name, job)?;
        }
    }
    fill_selected(reader, cut, identity, definition, job, &mut output).await?;
    if normalized.is_empty() {
        if output.present_count() as u64 > expected_count {
            return Err(unknown(
                "selected fields exceed authenticated membership count",
            ));
        }
        return Ok(output);
    }
    let record_len = record_fields_prefix_len(identity.collection, identity.key)?;
    let _record_charge = job.owned_buffer(record_len as u64)?;
    let record_prefix = record_fields_prefix(identity.collection, identity.key)?;
    for prefix in normalized {
        let len = record_field_name_prefix_len(identity.collection, identity.key, prefix)?;
        let _bounds = job.owned_buffer(len.checked_mul(2).ok_or(QueueError::TooLarge)? as u64)?;
        let lower = record_field_name_prefix(identity.collection, identity.key, prefix)?;
        let mut upper = lower.clone();
        if !prefix_successor(&mut upper) {
            return Err(unknown("field prefix has no upper bound"));
        }
        let range = reader.complete_range(&lower, &upper).await?;
        reader.visit_range(&range, &lower, &upper, &mut |point| {
            let bytes = point
                .value_for(&cut, point.key())?
                .ok_or_else(|| unknown("absent field inside prefix range"))?;
            let suffix = point
                .key()
                .strip_prefix(record_prefix.as_slice())
                .ok_or_else(|| unknown("field record prefix differs"))?;
            let _name_charge = job.owned_buffer(
                suffix
                    .len()
                    .checked_add(std::mem::size_of::<String>())
                    .ok_or(QueueError::TooLarge)? as u64,
            )?;
            let name = field_name(suffix)?;
            if !name.starts_with(prefix) {
                return Err(unknown("field outside declared literal prefix"));
            }
            output.retain(&name, bytes, definition, job)?;
            if output.present_count() as u64 > expected_count {
                return Err(unknown(
                    "selected fields exceed authenticated membership count",
                ));
            }
            Ok(())
        })?;
        // This declaration becomes authority only after the complete-range
        // carrier has been validated and fully visited without error.
        output.charge(prefix.len(), job)?;
        let FieldCoverage::Selected { prefixes, .. } = &mut output.coverage else {
            unreachable!()
        };
        prefixes.insert(prefix.to_owned());
    }
    if output.present_count() as u64 > expected_count {
        return Err(unknown(
            "selected fields exceed authenticated membership count",
        ));
    }
    Ok(output)
}
