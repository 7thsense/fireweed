//! Complete live identity domain for complement; never restricted by eligibility.
use super::*;
use crate::queue_source_index::{
    decode_live_key, live_keys_prefix, live_keys_prefix_len, prefix_successor,
};

impl<'a> QueueSourceSelectionRequest<'a> {
    fn collection(self) -> Option<&'a str> {
        match self {
            Self::Query(request) => match &request.source {
                RecordSource::Work => None,
                RecordSource::State(name) => Some(name.as_str()),
            },
            Self::Claim(_) | Self::Administration { .. } => None,
        }
    }
}
impl<'a, R: QueueSourceStreamingReader + Send, S: QueueSourceCandidateScratch>
    QueueSourceCandidateProducer<'a, R, S>
{
    /// Derive the full complement domain from this actual request. Even an empty
    /// universe needs the real original cursor's terminal completion.
    pub async fn universe(
        mut self,
    ) -> QueueResult<(Self, QueueSourceUniverseLeaf<'a, R::Completion, S::Set>)> {
        self.validate()?;
        let collection = self._request.collection();
        let bounds = live_keys_prefix_len(collection)?
            .checked_mul(2)
            .ok_or(QueueError::TooLarge)?;
        let capacity = bounds
            .checked_add(std::mem::size_of::<
                QueueSourceUniverseLeaf<'a, R::Completion, S::Set>,
            >())
            .and_then(|n| n.checked_add(2048))
            .ok_or(QueueError::TooLarge)?;
        let memory = self.job.owned_buffer(capacity as u64)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                copied_bytes: bounds as u64,
                ..Default::default()
            },
        )?;
        let lower = live_keys_prefix(collection)?;
        let mut upper = lower.clone();
        if !prefix_successor(&mut upper) {
            return Err(unknown("live-key namespace has no successor"));
        }
        let mut builder = self.scratch.begin_set()?;
        let original = self.original.cut();
        let mut cursor = self.reader.open_stream(&lower, &upper)?;
        while let Some(page) = self.reader.next_stream_page(&mut cursor).await? {
            scope(&self.reader, original, &self.job)?;
            let mut batch = Batch {
                rows: Vec::new(),
                _memory: self.job.owned_buffer(std::mem::size_of::<Batch>() as u64)?,
            };
            self.reader
                .visit_stream_page(&mut cursor, page, &mut |point| {
                    self.budget.grant(
                        &self.job,
                        QueueSourceSelectionWork {
                            rows: 1,
                            ..Default::default()
                        },
                    )?;
                    if point.key() < lower.as_slice() || point.key() >= upper.as_slice() {
                        return Err(unknown("universe page escaped original range"));
                    }
                    let identity = decode_live_key(&original, point, collection)?;
                    let copied = identity.key_len();
                    let capacity = copied
                        .checked_add(4 * std::mem::size_of::<Row>())
                        .and_then(|n| n.checked_add(512))
                        .ok_or(QueueError::TooLarge)?;
                    batch._memory.grow(capacity as u64)?;
                    self.budget.grant(
                        &self.job,
                        QueueSourceSelectionWork {
                            copied_bytes: copied as u64,
                            ..Default::default()
                        },
                    )?;
                    batch
                        .rows
                        .try_reserve_exact(1)
                        .map_err(|_| QueueError::TooLarge)?;
                    let mut key = Vec::with_capacity(copied);
                    key.extend(identity.key_bytes());
                    let key = String::from_utf8(key)
                        .map_err(|_| unknown("invalid universe record key"))?;
                    batch.rows.push(Row {
                        key,
                        tuple: Vec::new(),
                    });
                    Ok(())
                })?;
            for row in &batch.rows {
                record::live_record(
                    &self.reader,
                    original,
                    &self.header,
                    collection,
                    &row.key,
                    &self.job,
                    &self.budget,
                )
                .await?;
                self.scratch
                    .insert(&mut builder, collection, &row.key)
                    .await?;
            }
        }
        scope(&self.reader, original, &self.job)?;
        let completion = self.reader.finish_stream(cursor)?;
        self.reader
            .validate_stream_completion(&completion, &lower, &upper)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                copied_bytes: std::mem::size_of::<R::Completion>() as u64,
                ..Default::default()
            },
        )?;
        let set = self.scratch.seal(builder)?;
        let leaf = QueueSourceUniverseLeaf {
            collection,
            lower,
            upper,
            completion,
            set,
            identity: self.identity.clone(),
            _memory: memory,
        };
        self.validate_universe(&leaf)?;
        Ok((self, leaf))
    }
    pub fn validate_universe(
        &self,
        leaf: &QueueSourceUniverseLeaf<'_, R::Completion, S::Set>,
    ) -> QueueResult<()> {
        self.validate()?;
        if !Arc::ptr_eq(&leaf.identity, &self.identity)
            || leaf.collection != self._request.collection()
        {
            return Err(unknown("universe producer or collection differs"));
        }
        self.reader
            .validate_stream_completion(&leaf.completion, &leaf.lower, &leaf.upper)
    }
    /// Completed physical universe cardinality, not selected response authority.
    pub fn universe_len(
        &self,
        leaf: &QueueSourceUniverseLeaf<'_, R::Completion, S::Set>,
    ) -> QueueResult<u64> {
        self.validate_universe(leaf)?;
        self.scratch.count(&leaf.set)
    }
}
