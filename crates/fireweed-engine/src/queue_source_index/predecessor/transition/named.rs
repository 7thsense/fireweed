//! Named query tuples and unique eligibility; owner reduction is publication-wide.
use super::*;
use crate::{
    queue_fields::{encode_index_tuple, index_is_affected},
    queue_index::{borrowed_value, ordered_key_ref_len},
};
use std::collections::{BTreeMap, BTreeSet};

pub struct QueueSourceNamedEffect<'a> {
    pub index: &'a str,
    pub before: Option<&'a [u8]>,
    pub after: Option<&'a [u8]>,
    pub before_unique: bool,
    pub after_unique: bool,
}
impl QueueSourceRecordTransition<'_, '_> {
    /// Final dependencies share one publication traversal before tuple emission.
    /// Old values come from the retained field tree. Missing components still
    /// make a live query tuple, but cannot own a unique index.
    /// Caller-local output is unpublishable and must be discarded on any error.
    pub fn visit_named_postings<F>(
        &self,
        job: &Arc<QueueReservation>,
        visitor: &mut F,
    ) -> QueueResult<()>
    where
        F: FnMut(QueueSourceNamedEffect<'_>) -> QueueResult<()>,
    {
        if !std::ptr::eq(self._charge.reservation(), job.as_ref()) {
            return Err(unknown("named posting operation differs"));
        }
        let edits = match self.resolved().effect {
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Preserve,
                ..
            } => return Ok(()),
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Edit(edits),
                ..
            } => Some(edits),
            _ => None,
        };
        // Conservative tree/node and vector capacity credits. Payloads and names
        // are borrowed from retained source/definition/publication owners.
        let mut charge = job.owned_buffer(1024)?;
        let mut explicit = BTreeSet::new();
        if let Some(edits) = edits {
            for entry in edits.iter() {
                let (name, _) = entry.map_err(|_| unknown("invalid resolved index edit"))?;
                charge.grow(1024)?;
                explicit.insert(name);
            }
        }
        let mut affected = Vec::new();
        let mut after = BTreeMap::new();
        for (index, columns) in &self.definition.indexes {
            if !index_is_affected(columns, edits.is_none(), |name| explicit.contains(name)) {
                continue;
            }
            if columns.is_empty() || columns.len() > 8 {
                return Err(unknown("authenticated index width differs"));
            }
            charge.grow(1024)?;
            affected.push((index.as_str(), columns));
            for name in columns {
                if !after.contains_key(name.as_str()) {
                    charge.grow(1024)?;
                    after.insert(name.as_str(), None);
                }
            }
        }
        charge.grow(
            u64::try_from(
                after
                    .len()
                    .checked_mul(std::mem::size_of::<&str>())
                    .ok_or(QueueError::TooLarge)?,
            )
            .map_err(|_| QueueError::TooLarge)?,
        )?;
        let mut names = Vec::with_capacity(after.len());
        names.extend(after.keys().copied());
        self.visit_after_fields_borrowed(&names, &mut |name, value| {
            *after
                .get_mut(name)
                .ok_or_else(|| unknown("index dependency missing"))? = value;
            Ok(())
        })?;
        let before_exists = self
            .predecessor
            .fact
            .is_some_and(|fact| fact.live.is_some());
        let after_exists = self.fact.live.is_some();
        let before = self.fields.checked()?;
        for (index, columns) in affected {
            let mut old_values = [None; 8];
            let mut new_values = [None; 8];
            for (position, name) in columns.iter().enumerate() {
                if before_exists {
                    old_values[position] = before.get(name)?.map(borrowed_value);
                }
                if after_exists {
                    new_values[position] = *after
                        .get(name.as_str())
                        .ok_or_else(|| unknown("index dependency missing"))?;
                }
            }
            let old_values = &old_values[..columns.len()];
            let new_values = &new_values[..columns.len()];
            if before_exists == after_exists && old_values == new_values {
                continue;
            }
            let bytes = if before_exists {
                ordered_key_ref_len(old_values)?
            } else {
                0
            }
            .checked_add(if after_exists {
                ordered_key_ref_len(new_values)?
            } else {
                0
            })
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<Vec<u8>>()))
            .ok_or(QueueError::TooLarge)?;
            let _tuple_charge =
                job.owned_buffer(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)?;
            let old = before_exists
                .then(|| encode_index_tuple(old_values.iter().copied()))
                .transpose()?;
            let new = after_exists
                .then(|| encode_index_tuple(new_values.iter().copied()))
                .transpose()?;
            let old_sort = old.as_ref().map(|tuple| tuple.sort.as_slice());
            let new_sort = new.as_ref().map(|tuple| tuple.sort.as_slice());
            if old_sort == new_sort {
                continue;
            }
            let unique = self.definition.unique_indexes.contains(index);
            visitor(QueueSourceNamedEffect {
                index,
                before: old_sort,
                after: new_sort,
                before_unique: unique && old.as_ref().is_some_and(|tuple| tuple.complete),
                after_unique: unique && new.as_ref().is_some_and(|tuple| tuple.complete),
            })?;
        }
        Ok(())
    }
}
