//! Private correspondence bridge between actual source streams and disposable
//! candidate sets. A scalar leaf is not a completed selector or response authority.
//! The owned coordinator must retain the real source capsule: a borrowed reader
//! and OwnedSourceCutBinding alone do not own that capsule.
use super::{
    QueueSourceCandidateScratch, QueueSourceScalarRanges, QueueSourceSelectionBudget,
    QueueSourceSelectionWork, plan_source_scalar_ranges,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_index::{ordered_key_ref, ordered_key_ref_len},
    queue_schema_codec::AdmittedQueueDefinition,
    queue_source_index::{
        QueueSourceCut, QueueSourceHeader, QueueSourceKey, QueueSourceLive, QueueSourceRecord,
        QueueSourceStreamingReader, header::load_source_header_at, scope::OwnedSourceCutBinding,
        source_field_value,
    },
    queue_storage_layout,
};
use fireweed_core::{
    queue_administration::{AdministrativeIntent, AdministrativeOperation, SelectorNode},
    queue_contract::{
        ClaimRequest, Predicate, QueryRequest, QueueAddress, QueueError, QueueLimits, QueueResult,
        RecordSource,
    },
};
use std::sync::Arc;
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source candidate producer: {reason}"))
}

/// References must come from the coordinator's admitted immutable request and
/// outlive every leaf. Plans borrow these external references, never producer
/// fields that could make the producer self-referential.
#[derive(Clone, Copy)]
pub enum QueueSourceSelectionRequest<'a> {
    Query(&'a QueryRequest),
    Claim(&'a ClaimRequest),
    Administration {
        queue: &'a QueueAddress,
        intent: &'a AdministrativeIntent,
    },
}
#[derive(Clone, Copy)]
struct Declaration<'a> {
    collection: Option<&'a str>,
    field: &'a str,
    predicate: &'a Predicate,
}
impl<'a> QueueSourceSelectionRequest<'a> {
    fn validate(
        self,
        definition: &AdmittedQueueDefinition,
        limits: &QueueLimits,
    ) -> QueueResult<()> {
        match self {
            Self::Query(request) => request.validate(definition.schema(), limits),
            Self::Claim(request) => request.validate(definition.schema(), limits),
            Self::Administration { intent, .. } => intent.validate(definition.schema(), limits),
        }
    }
    fn queue(self) -> &'a QueueAddress {
        match self {
            Self::Query(request) => &request.queue,
            Self::Claim(request) => &request.queue,
            Self::Administration { queue, .. } => queue,
        }
    }
    fn declarations(
        self,
        mut visit: impl FnMut(Declaration<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        match self {
            Self::Query(request) => {
                let collection = match &request.source {
                    RecordSource::Work => None,
                    RecordSource::State(name) => Some(name.as_str()),
                };
                for (field, predicate) in &request.filter.fields {
                    visit(Declaration {
                        collection,
                        field,
                        predicate,
                    })?;
                }
            }
            Self::Claim(request) => {
                for (field, predicate) in &request.filter.fields {
                    visit(Declaration {
                        collection: None,
                        field,
                        predicate,
                    })?;
                }
            }
            Self::Administration { intent, .. } => {
                if let AdministrativeOperation::SelectFirst(clauses) = &intent.operation {
                    // Preconditions do not define ownership. The shared FP4
                    // scanner applies them only after the first clause owns a key.
                    for clause in clauses {
                        for node in &clause.selector.nodes {
                            if let SelectorNode::Field { name, predicate } = node {
                                visit(Declaration {
                                    collection: None,
                                    field: name,
                                    predicate,
                                })?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
struct Identity {
    _memory: OwnedQueueBufferCharge,
}
/// This producer owns its ports, but R may itself borrow the actual capsule.
/// Keep that capsule in the admitted owned coordinator until physical retirement.
/// Every async step consumes Self and returns it only on success. Error/drop
/// therefore cannot leave a resumable producer, even during a source await.
pub struct QueueSourceCandidateProducer<
    'a,
    R: QueueSourceStreamingReader,
    S: QueueSourceCandidateScratch,
> {
    reader: R,
    scratch: S,
    original: OwnedSourceCutBinding,
    definition: &'a AdmittedQueueDefinition,
    declarations: Vec<Declaration<'a>>,
    header: QueueSourceHeader,
    identity: Arc<Identity>,
    job: Arc<QueueReservation>,
    budget: Arc<QueueSourceSelectionBudget>,
    _request: QueueSourceSelectionRequest<'a>,
    _evaluated_at: i64,
    _memory: OwnedQueueBufferCharge,
}
/// Only this producer can mint a leaf after all its actual cursors and writes
/// complete. The private occurrence distinguishes even equal-family predicates.
/// No constructor accepts separately assembled completions or a physical EOF.
pub struct QueueSourceScalarLeaf<'a, C, Set> {
    plan: QueueSourceScalarRanges<'a>,
    completions: Vec<C>,
    set: Set,
    identity: Arc<Identity>,
    occurrence: usize,
    validated_empty: bool,
    _memory: OwnedQueueBufferCharge,
}
pub struct QueueSourceUniverseLeaf<'a, C, Set> {
    collection: Option<&'a str>,
    lower: Vec<u8>,
    upper: Vec<u8>,
    completion: C,
    set: Set,
    identity: Arc<Identity>,
    _memory: OwnedQueueBufferCharge,
}
pub struct QueueSourcePresenceLeaf<'a, C, Set> {
    plan: QueueSourceScalarRanges<'a>,
    completions: Vec<C>,
    set: Set,
    identity: Arc<Identity>,
    clause: usize,
    node: usize,
    present: bool,
    _memory: OwnedQueueBufferCharge,
}
/// Every requested point is authenticated by this producer before construction,
/// including absent and tombstoned keys. No range/universe scan is needed.
pub struct QueueSourceKeysLeaf<'a, Set> {
    observations: Vec<KeyObservation<'a>>,
    set: Set,
    identity: Arc<Identity>,
    clause: usize,
    node: usize,
    _memory: OwnedQueueBufferCharge,
}
struct KeyObservation<'a> {
    key: &'a str,
    highwater: Option<u64>,
    live: bool,
}
enum CandidateOperand<Set> {
    Value(usize),
    Presence(usize),
    Keys(usize),
    Universe,
    Empty,
    Owned(Set),
}
/// Exact candidates for one supported actual administrative Boolean clause.
/// This does not seal first-clause ownership, preconditions, materialization,
/// freshness, Claim eligibility or ordering.
pub struct QueueSourceBooleanClause<'a, C, Set> {
    result: CandidateOperand<Set>,
    values: Vec<QueueSourceScalarLeaf<'a, C, Set>>,
    presence: Vec<QueueSourcePresenceLeaf<'a, C, Set>>,
    keys: Vec<QueueSourceKeysLeaf<'a, Set>>,
    universe: Option<QueueSourceUniverseLeaf<'a, C, Set>>,
    empty: Option<Set>,
    identity: Arc<Identity>,
    clause: usize,
    _memory: OwnedQueueBufferCharge,
}
struct Row {
    key: String,
    tuple: Vec<u8>,
}
struct Batch {
    rows: Vec<Row>,
    // Last: all row buffers and the Vec allocation retire before admission.
    _memory: OwnedQueueBufferCharge,
}
fn scope<R: QueueSourceStreamingReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    job: &QueueReservation,
) -> QueueResult<()> {
    if !original.belongs_to(job) || !reader.cut()?.same_cut(&original) {
        return Err(unknown("original source or job differs"));
    }
    Ok(())
}
impl<'a, R: QueueSourceStreamingReader + Send, S: QueueSourceCandidateScratch>
    QueueSourceCandidateProducer<'a, R, S>
{
    #[allow(clippy::too_many_arguments)]
    pub async fn begin(
        reader: R,
        scratch: S,
        definition: &'a AdmittedQueueDefinition,
        request: QueueSourceSelectionRequest<'a>,
        evaluated_at: i64,
        limits: &QueueLimits,
        job: Arc<QueueReservation>,
        budget: Arc<QueueSourceSelectionBudget>,
    ) -> QueueResult<Self> {
        if let QueueSourceSelectionRequest::Administration { intent, .. } = request
            && evaluated_at != intent.evaluated_at
        {
            return Err(unknown(
                "evaluation time differs from actual administrative intent",
            ));
        }
        let cut = reader.cut()?;
        let queue = request.queue();
        if !definition.belongs_to(&job)
            || !cut.belongs_to(&job)
            || cut.address().tenant != queue.tenant
            || cut.address().queue != queue.queue
        {
            return Err(unknown("request queue, definition or operation differs"));
        }
        scratch.validate_operation(&job, &budget)?;
        // Common schema validation builds one temporary distinct-column set
        // (at most eight borrowed names) at a time. Its peak is independent of
        // schema width; selector validation additionally uses a fixed stack.
        let validation_memory = job.owned_buffer(4096)?;
        let validation_copies = fireweed_core::queue_contract::encoded_len(definition.schema())?
            .checked_mul(16)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        budget.grant(
            &job,
            QueueSourceSelectionWork {
                copied_bytes: validation_copies,
                ..Default::default()
            },
        )?;
        request.validate(definition, limits)?;
        drop(validation_memory);
        let mut count = 0usize;
        request.declarations(|_| {
            count = count.checked_add(1).ok_or(QueueError::TooLarge)?;
            Ok(())
        })?;
        let capacity = count
            .checked_mul(std::mem::size_of::<Declaration<'a>>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        let memory = job.owned_buffer(capacity as u64)?;
        let token_memory = job.owned_buffer(
            (std::mem::size_of::<Identity>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        let copied = (count as u64)
            .checked_mul(std::mem::size_of::<Declaration<'a>>() as u64)
            .and_then(|n| n.checked_add(queue.tenant.len() as u64))
            .and_then(|n| n.checked_add(queue.queue.len() as u64))
            .and_then(|n| n.checked_add(4))
            .ok_or(QueueError::TooLarge)?;
        // Two fixed header probes; source SDK I/O has its separate same-request
        // ledger. These producer copies/probes also spend selection work.
        budget.grant(
            &job,
            QueueSourceSelectionWork {
                rows: 2,
                copied_bytes: copied,
                scratch_steps: 0,
            },
        )?;
        let original = OwnedSourceCutBinding::capture(cut, &job)?;
        let mut declarations = Vec::with_capacity(count);
        request.declarations(|declaration| {
            declarations.push(declaration);
            Ok(())
        })?;
        let header = load_source_header_at(&reader, original.cut(), &job).await?;
        scope(&reader, original.cut(), &job)?;
        if header.genesis().definition_pin != definition.pin() {
            return Err(unknown("persisted definition differs"));
        }
        Ok(Self {
            reader,
            scratch,
            original,
            definition,
            declarations,
            header,
            identity: Arc::new(Identity {
                _memory: token_memory,
            }),
            job,
            budget,
            _request: request,
            _evaluated_at: evaluated_at,
            _memory: memory,
        })
    }
    pub fn scalar_declarations(&self) -> usize {
        self.declarations.len()
    }
    /// Choose an occurrence from this producer's actual immutable request, never
    /// a caller-supplied replacement predicate or separately built range plan.
    pub async fn scalar(
        mut self,
        occurrence: usize,
    ) -> QueueResult<(Self, QueueSourceScalarLeaf<'a, R::Completion, S::Set>)> {
        self.validate()?;
        let declaration = *self
            .declarations
            .get(occurrence)
            .ok_or_else(|| unknown("unknown declaration occurrence"))?;
        let plan_copy_ceiling = super::scalar::scalar_plan_capacity(
            crate::queue_source_index::QueueSourcePostingFamily::Scalar {
                collection: declaration.collection,
                field: declaration.field,
            },
            declaration.predicate,
        )?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                copied_bytes: plan_copy_ceiling,
                ..Default::default()
            },
        )?;
        let plan = plan_source_scalar_ranges(
            self.definition,
            declaration.collection,
            declaration.field,
            declaration.predicate,
            &self.job,
        )?;
        let slots = plan
            .ranges()
            .len()
            .checked_mul(std::mem::size_of::<R::Completion>())
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    QueueSourceScalarLeaf<'a, R::Completion, S::Set>,
                >())
            })
            .and_then(|n| n.checked_add(2048))
            .ok_or(QueueError::TooLarge)?;
        let memory = self.job.owned_buffer(slots as u64)?;
        let (completions, set) = self.scan_ranges(&plan).await?;
        let validated_empty = plan.ranges().is_empty();
        let leaf = QueueSourceScalarLeaf {
            plan,
            completions,
            set,
            identity: self.identity.clone(),
            occurrence,
            validated_empty,
            _memory: memory,
        };
        self.validate_scalar(&leaf, occurrence)?;
        Ok((self, leaf))
    }
    // Caller admits the leaf and complete manifest slots before dispatch.
    // Only actual-request-derived plans enter this private helper.
    async fn scan_ranges(
        &mut self,
        plan: &QueueSourceScalarRanges<'a>,
    ) -> QueueResult<(Vec<R::Completion>, S::Set)> {
        let crate::queue_source_index::QueueSourcePostingFamily::Scalar { collection, .. } =
            plan.family()
        else {
            return Err(unknown("scalar scan family differs"));
        };
        let mut completions = Vec::with_capacity(plan.ranges().len());
        let mut builder = self.scratch.begin_set()?;
        let original = self.original.cut();
        for range in plan.ranges() {
            scope(&self.reader, original, &self.job)?;
            let mut cursor = self.reader.open_stream(range.lower(), range.upper())?;
            while let Some(page) = self.reader.next_stream_page(&mut cursor).await? {
                scope(&self.reader, original, &self.job)?;
                let mut batch = Batch {
                    rows: Vec::new(),
                    _memory: self.job.owned_buffer(std::mem::size_of::<Batch>() as u64)?,
                };
                self.reader
                    .visit_stream_page(&mut cursor, page, &mut |point| {
                        // Spend the visit before parsing or admission can fail.
                        // Copy work is separate and granted before allocation.
                        self.budget.grant(
                            &self.job,
                            QueueSourceSelectionWork {
                                rows: 1,
                                ..Default::default()
                            },
                        )?;
                        if point.key() < range.lower() || point.key() >= range.upper() {
                            return Err(unknown("page escaped original range"));
                        }
                        let posting =
                            plan.family()
                                .decode(&original, point, &[range.field_type()])?;
                        let identity = posting.identity();
                        let copied = identity
                            .key_len()
                            .checked_add(posting.sort_len())
                            .ok_or(QueueError::TooLarge)?;
                        // reserve_exact may temporarily retain both old and new Vec
                        // storage. Four row slots per row cover that overlap and its
                        // first allocation. This is owned accounting, not RSS proof.
                        let capacity = copied
                            .checked_add(4 * std::mem::size_of::<Row>())
                            .and_then(|n| n.checked_add(512))
                            .ok_or(QueueError::TooLarge)?;
                        batch._memory.grow(capacity as u64)?;
                        self.budget.grant(
                            &self.job,
                            QueueSourceSelectionWork {
                                rows: 0,
                                copied_bytes: copied as u64,
                                scratch_steps: 0,
                            },
                        )?;
                        batch
                            .rows
                            .try_reserve_exact(1)
                            .map_err(|_| QueueError::TooLarge)?;
                        let mut key = Vec::with_capacity(identity.key_len());
                        key.extend(identity.key_bytes());
                        let key =
                            String::from_utf8(key).map_err(|_| unknown("invalid record key"))?;
                        let mut tuple = Vec::with_capacity(posting.sort_len());
                        tuple.extend(posting.sort_bytes());
                        batch.rows.push(Row { key, tuple });
                        Ok(())
                    })?;
                // The SDK page was consumed above. Only independent admitted
                // copies cross these asynchronous correspondence/scratch calls.
                for row in &batch.rows {
                    correspondence(
                        &self.reader,
                        original,
                        &self.header,
                        plan.family(),
                        range.field_type(),
                        row,
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
            // All rows, including the final page's last write, are terminal.
            let completion = self.reader.finish_stream(cursor)?;
            self.reader
                .validate_stream_completion(&completion, range.lower(), range.upper())?;
            self.budget.grant(
                &self.job,
                QueueSourceSelectionWork {
                    copied_bytes: std::mem::size_of::<R::Completion>() as u64,
                    ..Default::default()
                },
            )?;
            completions.push(completion);
        }
        let set = self.scratch.seal(builder)?;
        Ok((completions, set))
    }
    fn validate(&self) -> QueueResult<()> {
        self.original.validate(self.reader.cut()?, &self.job)?;
        self.scratch.validate_operation(&self.job, &self.budget)
    }
    pub fn validate_scalar(
        &self,
        leaf: &QueueSourceScalarLeaf<'_, R::Completion, S::Set>,
        expected_occurrence: usize,
    ) -> QueueResult<()> {
        self.validate()?;
        if !Arc::ptr_eq(&self.identity, &leaf.identity)
            || leaf.occurrence != expected_occurrence
            || expected_occurrence >= self.declarations.len()
            || !leaf.plan.belongs_to(&self.job)
            || leaf.plan.definition_pin() != self.definition.pin()
            || leaf.completions.len() != leaf.plan.ranges().len()
            || leaf.validated_empty != leaf.plan.ranges().is_empty()
        {
            return Err(unknown(
                "leaf declaration, producer or complete manifest differs",
            ));
        }
        for (range, completion) in leaf.plan.ranges().iter().zip(&leaf.completions) {
            self.reader
                .validate_stream_completion(completion, range.lower(), range.upper())?;
        }
        Ok(())
    }
    /// Physical leaf cardinality after its exact source ranges complete; this
    /// does not assert full-selector, payload/history or freshness authority.
    pub fn scalar_len(
        &self,
        leaf: &QueueSourceScalarLeaf<'_, R::Completion, S::Set>,
        expected_occurrence: usize,
    ) -> QueueResult<u64> {
        self.validate_scalar(leaf, expected_occurrence)?;
        self.scratch.count(&leaf.set)
    }
}
// Invalid is for caller input. These values came from authenticated persisted
// facts, so a malformed scalar is corruption; keep resource limits distinct.
fn persisted_codec<T>(result: QueueResult<T>) -> QueueResult<T> {
    result.map_err(|error| match error {
        QueueError::Invalid(_) => unknown("noncanonical persisted field value"),
        other => other,
    })
}
#[allow(clippy::too_many_arguments)]
async fn correspondence<R: QueueSourceStreamingReader>(
    reader: &R,
    original: QueueSourceCut<'_>,
    header: &QueueSourceHeader,
    family: crate::queue_source_index::QueueSourcePostingFamily<'_>,
    kind: fireweed_core::queue_contract::FieldType,
    row: &Row,
    job: &Arc<QueueReservation>,
    budget: &QueueSourceSelectionBudget,
) -> QueueResult<()> {
    let crate::queue_source_index::QueueSourcePostingFamily::Scalar { collection, field } = family
    else {
        return Err(unknown("scalar correspondence family differs"));
    };
    let record =
        record::live_record(reader, original, header, collection, &row.key, job, budget).await?;
    // Scalar membership requires an actual field. The complete live-key
    // universe also contains valid records with no fields.
    if record.field_count == 0 {
        return Err(unknown("posting record has no fields"));
    }
    scope(reader, original, job)?;
    let key = QueueSourceKey::Field {
        collection,
        key: &row.key,
        field,
    };
    let _field_key_memory = job.owned_buffer(key.encoded_len()? as u64)?;
    budget.grant(
        job,
        QueueSourceSelectionWork {
            rows: 1,
            copied_bytes: key.encoded_len()? as u64,
            scratch_steps: 0,
        },
    )?;
    let key = key.encode()?;
    let owner = reader.lookup(&key).await?;
    let point = reader.point(&owner)?;
    let bytes = point
        .value_for(&original, &key)?
        .ok_or_else(|| unknown("posting has no field fact"))?;
    let value = source_field_value(bytes)?;
    if value.field_type() != kind {
        return Err(unknown("posting and field types differ"));
    }
    let values = [Some(value)];
    let len = persisted_codec(ordered_key_ref_len(&values))?;
    let _tuple_memory = job.owned_buffer(len as u64)?;
    budget.grant(
        job,
        QueueSourceSelectionWork {
            copied_bytes: len as u64,
            ..Default::default()
        },
    )?;
    if persisted_codec(ordered_key_ref(&values))? != row.tuple {
        return Err(unknown("posting tuple differs from field fact"));
    }
    scope(reader, original, job)
}

#[cfg(test)]
mod tests;

mod record;
mod universe;

mod presence;

mod boolean;
mod keys;
