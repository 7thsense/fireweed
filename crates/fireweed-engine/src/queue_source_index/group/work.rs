//! Shared exact-key work hydration for ordinary and administrative source reads.
use super::*;
impl<'a> SourceImage<'a> {
    pub(super) async fn load_work<R: QueueSourceRecordReader>(
        &mut self,
        reader: &'a R,
        key: &str,
        needs_body: bool,
        selection: QueueSourceFieldSelection<'_>,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<QueueSourceWorkBodyEvidence<'a>>> {
        let (fact, evidence) = body::work_fact(reader, self.cut, key, needs_body, job).await?;
        let Some(fact) = fact else {
            return Ok(evidence);
        };
        fact.validate(None)?;
        self.snapshot.versions.insert(
            RecordIdentity {
                collection: None,
                key: key.into(),
            },
            fact.highwater,
        );
        if let Some(QueueSourceLive::Work { metadata, body }) = fact.live {
            if metadata.insertion_sequence > self.snapshot.next_sequence
                || body.span().log_offset() >= self.cut.through()
            {
                return Err(unknown(
                    "work sequence or body lies outside captured source",
                ));
            }
            let (fields, coverage, guard) = load_selected_fields(
                reader,
                self.cut,
                IdentityRef {
                    collection: None,
                    key,
                },
                fact.field_count,
                &definition.schema().work,
                selection,
                job,
            )
            .await?
            .into_parts();
            self.retained_charges.push(guard);
            self.snapshot.work.insert(
                key.into(),
                StoredWork {
                    record: WorkRecord {
                        work: NewWork {
                            key: key.into(),
                            priority: metadata.priority,
                            available_at: metadata.available_at,
                            fields,
                            body: Default::default(),
                        },
                        version: metadata.version,
                        insertion_sequence: metadata.insertion_sequence,
                        lifecycle: metadata.lifecycle,
                        eligible_since: metadata.eligible_since,
                        attempts: metadata.attempts,
                        lease_expires_at: metadata.lease_expires_at,
                    },
                    lease_hash: metadata.lease_hash,
                    field_coverage: coverage,
                    body_loaded: false,
                },
            );
        }
        Ok(evidence)
    }
}
