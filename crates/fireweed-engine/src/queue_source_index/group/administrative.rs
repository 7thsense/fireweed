//! Addressed administrative authority from the same row loader as ordinary work.
use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeEntry, SelectorNode, WorkSelector,
};

/// Borrowed, prevalidated declarations from the enclosing administrative intent.
/// Selectors include every predicate whose field knowledge this row must supply.
pub struct QueueSourceAdministrativeRead<'a> {
    pub key: &'a str,
    pub action: &'a AdministrativeAction,
    pub before_fields: Option<&'a BTreeSet<String>>,
    pub selectors: &'a [&'a WorkSelector],
}
/// A single exact-key result, including known absence and tombstone highwater.
/// This is hydration authority, not business acceptance or publication authority.
pub struct QueueSourceAdministrativeRow<'a> {
    key: String,
    image: SourceImage<'a>,
}
impl QueueSourceAdministrativeRow<'_> {
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.image.cut
    }
    pub fn image(&self) -> &RecordSnapshot {
        &self.image.snapshot
    }
    /// Consume each hydrated result at its original addressed position. Repeated
    /// input keys remain separate calls/positions, never collapsed into a map.
    pub(super) fn into_addressed<'s, 'r>(
        self,
        position: usize,
    ) -> crate::queue_administration::AddressedAdministrativeInput<'s, 'r> {
        let mut hydration = self.image.retained_charges;
        hydration.push(self.image.charge);
        crate::queue_administration::AddressedAdministrativeInput::OwnedAdmittedOneRow {
            position,
            key: self.key,
            image: self.image.snapshot,
            hydration,
        }
    }
}
fn scratch(read: &QueueSourceAdministrativeRead<'_>, schema: &QueueSchema) -> QueueResult<u64> {
    let mut bytes = (read.key.len() as u64)
        .checked_mul(16)
        .and_then(|n| n.checked_add(16384))
        .ok_or(QueueError::TooLarge)?;
    let mut name = |name: &str| -> QueueResult<()> {
        bytes = bytes
            .checked_add(
                (name.len() as u64)
                    .checked_add(256)
                    .and_then(|n| n.checked_mul(4))
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)?;
        Ok(())
    };
    for column in schema.work.indexes.values().flatten() {
        name(column)?;
    }
    for field in read.before_fields.into_iter().flatten() {
        name(field)?;
    }
    for selector in read.selectors {
        for node in &selector.nodes {
            if let SelectorNode::Field { name: field, .. }
            | SelectorNode::Present { name: field, .. } = node
            {
                name(field)?;
            }
        }
    }
    if let AdministrativeAction::Update {
        patch,
        remove_field_prefixes,
        ..
    } = read.action
    {
        for field in patch.fields.keys().chain(remove_field_prefixes.iter()) {
            name(field)?;
        }
    }
    Ok(bytes)
}
/// One group-local original source. Genesis and sequence are authenticated once,
/// never recaptured for each selected or addressed row.
pub struct QueueSourceAdministrativeReader<'a, R> {
    reader: &'a R,
    definition: &'a AdmittedQueueDefinition,
    job: &'a Arc<QueueReservation>,
    header: SourceImage<'a>,
}
pub async fn open_administrative_source<'a, R: QueueSourceRecordReader>(
    reader: &'a R,
    cut: QueueSourceCut<'a>,
    queue: &QueueAddress,
    definition: &'a AdmittedQueueDefinition,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceAdministrativeReader<'a, R>> {
    if !reader.cut()?.same_cut(&cut) {
        return Err(unknown("administrative reader differs from original cut"));
    }
    let charge = job.owned_buffer(4096)?;
    let header = load_image_header(reader, cut, queue, definition, job, charge).await?;
    Ok(QueueSourceAdministrativeReader {
        reader,
        definition,
        job,
        header,
    })
}
impl<'a, R: QueueSourceRecordReader> QueueSourceAdministrativeReader<'a, R> {
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.header.cut
    }
    /// Reuse this group's authenticated header for explicit ordinary authority.
    pub async fn load_ordinary_authority(
        &self,
        queue: &QueueAddress,
        group: &CommitGroup,
        limits: &QueueLimits,
    ) -> QueueResult<QueueSourceOrdinaryAuthority> {
        super::ordinary_authority::load_with_source(
            self.reader,
            super::ordinary_authority::OrdinarySource::Header(&self.header),
            queue,
            group,
            self.definition,
            limits,
            self.job,
        )
        .await
    }
    pub async fn load_admitted_ordinary_authority(
        &self,
        admission: QueueSourceAdministrativeAdmission<'_>,
    ) -> QueueResult<QueueSourceOrdinaryAuthority> {
        super::ordinary_authority::load_admitted(
            self.reader,
            super::ordinary_authority::OrdinarySource::Header(&self.header),
            admission,
        )
        .await
    }
    /// Derive the complete addressed read declaration from this exact entry,
    /// then consume it directly into its original scanner position. Callers
    /// cannot retarget a separately hydrated row with another action/predicate.
    pub async fn load_addressed_row<'s, 'r>(
        &self,
        position: usize,
        entry: &AdministrativeEntry,
        before_fields: Option<&BTreeSet<String>>,
    ) -> QueueResult<crate::queue_administration::AddressedAdministrativeInput<'s, 'r>> {
        let row = self
            .load_row(QueueSourceAdministrativeRead {
                key: &entry.key,
                action: &entry.action,
                before_fields,
                selectors: &[&entry.precondition],
            })
            .await?;
        Ok(row.into_addressed(position))
    }
    /// Hydrate one row after enclosing intent validation/admission. Metadata-only
    /// actions never open record bodies. This performs no business evaluation,
    /// clock sampling, state mutation, writer acquisition or publication.
    pub async fn load_row(
        &self,
        read: QueueSourceAdministrativeRead<'_>,
    ) -> QueueResult<QueueSourceAdministrativeRow<'a>> {
        let reader = self.reader;
        let definition = self.definition;
        let job = self.job;
        let cut = self.header.cut;
        if !reader.cut()?.same_cut(&cut) {
            return Err(unknown("administrative reader differs from original cut"));
        }
        let charge = job.owned_buffer(scratch(&read, definition.schema())?)?;
        let mut image = SourceImage {
            cut,
            pin: self.header.pin,
            snapshot: RecordSnapshot {
                next_sequence: self.header.snapshot.next_sequence,
                ..Default::default()
            },
            retained_charges: Vec::new(),
            charge,
        };
        let mut names = BTreeSet::new();
        names.extend(read.before_fields.into_iter().flatten().cloned());
        for selector in read.selectors {
            for node in &selector.nodes {
                if let SelectorNode::Field { name, .. } | SelectorNode::Present { name, .. } = node
                {
                    names.insert(name.clone());
                }
            }
        }
        let empty = BTreeSet::new();
        let (prefixes, needs_body) = match read.action {
            AdministrativeAction::Update {
                patch,
                remove_field_prefixes,
                ..
            } => {
                names.extend(patch.fields.keys().cloned());
                for columns in definition.schema().work.indexes.values() {
                    if columns.iter().any(|name| {
                        patch.fields.contains_key(name)
                            || remove_field_prefixes
                                .iter()
                                .any(|prefix| name.starts_with(prefix))
                    }) {
                        names.extend(columns.iter().cloned());
                    }
                }
                (remove_field_prefixes, patch.body.is_some())
            }
            AdministrativeAction::Delete { .. } => {
                for index in &definition.schema().work.unique_indexes {
                    names.extend(
                        definition
                            .schema()
                            .work
                            .indexes
                            .get(index)
                            .ok_or_else(|| unknown("missing unique index definition"))?
                            .iter()
                            .cloned(),
                    );
                }
                (&empty, false)
            }
        };
        let body = image
            .load_work(
                reader,
                read.key,
                needs_body,
                QueueSourceFieldSelection {
                    names: &names,
                    prefixes,
                },
                definition,
                job,
            )
            .await?;
        if image.snapshot.work.contains_key(read.key) {
            // Admit the field-only proxy before copying any request field value.
            let patch_bytes = match read.action {
                AdministrativeAction::Update { patch, .. } => encoded_len(&patch.fields)?,
                AdministrativeAction::Delete { .. } => 0,
            };
            image.charge.grow(
                patch_bytes
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?;
            let mut proxy = crate::queue_administration::administrative_unique_hydration_group(
                read.key,
                read.action,
            );
            image
                .charge
                .grow(crate::queue_memory::group_expansion_budget(
                    &proxy,
                    &image.snapshot,
                    definition.schema(),
                )?)?;
            crate::queue_administration::resolve_administrative_hydration(
                &mut proxy,
                read.action,
                &image.snapshot,
                definition.schema(),
            )?;
            image.load_unique(reader, &proxy, definition, job).await?;
        }
        if let Some(body) = body {
            image.load_bodies(reader, &[body], definition, job).await?;
        }
        if !reader.cut()?.same_cut(&cut) {
            return Err(unknown("administrative reader changed captured cut"));
        }
        Ok(QueueSourceAdministrativeRow {
            key: read.key.into(),
            image,
        })
    }
}
