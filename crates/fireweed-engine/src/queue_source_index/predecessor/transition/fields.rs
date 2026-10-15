//! Borrowed field changes and dependency values for index compilation.
use super::*;

impl QueueSourceRecordTransition<'_, '_> {
    /// Visit each explicit field effect once, retaining borrowed source and
    /// publication owners throughout. Index dependency columns loaded only for
    /// tuple reconstruction never become field write intent.
    pub fn visit_field_changes<F>(&self, visitor: &mut F) -> QueueResult<()>
    where
        F: FnMut(&str, Option<ValueRef<'_>>, Option<ValueRef<'_>>) -> QueueResult<()>,
    {
        use crate::queue_index::borrowed_value;
        let replacement = match self.resolved().effect {
            EffectRef::Delete => {
                for (name, value) in self.fields.complete_entries()? {
                    visitor(name, Some(borrowed_value(value)), None)?;
                }
                return Ok(());
            }
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Preserve,
                ..
            } => return Ok(()),
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Edit(edits),
                ..
            } => {
                let before = self.fields.checked()?;
                for entry in edits.iter() {
                    let (name, after) =
                        entry.map_err(|_| unknown("invalid resolved field entry"))?;
                    visitor(name, before.get(name)?.map(borrowed_value), after)?;
                }
                return Ok(());
            }
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Replace(fields),
                ..
            }
            | EffectRef::StatePut { fields, .. } => fields,
        };
        let mut old = self.fields.complete_entries()?.peekable();
        let mut new = replacement.iter();
        let mut next = new
            .next()
            .transpose()
            .map_err(|_| unknown("invalid replacement field"))?;
        loop {
            match (old.peek(), next) {
                (None, None) => return Ok(()),
                (Some(&(name, value)), Some((new_name, after))) if name == new_name => {
                    visitor(name, Some(borrowed_value(value)), after)?;
                    old.next();
                    next = new
                        .next()
                        .transpose()
                        .map_err(|_| unknown("invalid replacement field"))?;
                }
                (Some(&(name, value)), Some((new_name, _))) if name < new_name => {
                    visitor(name, Some(borrowed_value(value)), None)?;
                    old.next();
                }
                (Some(&(name, value)), None) => {
                    visitor(name, Some(borrowed_value(value)), None)?;
                    old.next();
                }
                (_, Some((name, after))) => {
                    visitor(name, None, after)?;
                    next = new
                        .next()
                        .transpose()
                        .map_err(|_| unknown("invalid replacement field"))?;
                }
            }
        }
    }
    /// Final values for a sorted, unique dependency set. The publication is
    /// traversed once, rather than rescanned for every index column. Unedited
    /// names require authenticated source coverage and never become false absence.
    pub fn visit_after_fields(
        &self,
        names: &[&str],
        visitor: &mut dyn FnMut(&str, Option<ValueRef<'_>>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.visit_after_fields_borrowed(names, visitor)
    }
    pub(super) fn visit_after_fields_borrowed<'s, F>(
        &'s self,
        names: &[&str],
        visitor: &mut F,
    ) -> QueueResult<()>
    where
        F: FnMut(&str, Option<ValueRef<'s>>) -> QueueResult<()> + ?Sized,
    {
        use crate::queue_index::borrowed_value;
        if names.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(unknown("dependency names must be sorted and unique"));
        }
        let (fields, complete) = match self.resolved().effect {
            EffectRef::Delete => {
                for name in names {
                    visitor(name, None)?;
                }
                return Ok(());
            }
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Preserve,
                ..
            } => {
                let before = self.fields.checked()?;
                for name in names {
                    visitor(name, before.get(name)?.map(borrowed_value))?;
                }
                return Ok(());
            }
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Edit(edits),
                ..
            } => (edits, false),
            EffectRef::WorkPut {
                fields: FieldsCommandRef::Replace(fields),
                ..
            }
            | EffectRef::StatePut { fields, .. } => (fields, true),
        };
        let before = self.fields.checked()?;
        let mut entries = fields.iter();
        let mut next = entries
            .next()
            .transpose()
            .map_err(|_| unknown("invalid resolved field entry"))?;
        for name in names {
            while next.is_some_and(|(field, _)| field < *name) {
                next = entries
                    .next()
                    .transpose()
                    .map_err(|_| unknown("invalid resolved field entry"))?;
            }
            let value = match next {
                Some((field, value)) if field == *name => value,
                _ if complete => None,
                _ => before.get(name)?.map(borrowed_value),
            };
            visitor(name, value)?;
        }
        Ok(())
    }
}
