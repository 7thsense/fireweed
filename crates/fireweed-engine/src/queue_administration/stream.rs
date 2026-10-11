//! Internal admitted administrative planning. Sources own fenced completeness;
//! this driver finishes only after successful EOF. No durable control API yet.
use super::*;
use crate::queue_admission::{QueueBufferCharge, QueueReservation};
use crate::queue_planner::*;
use std::collections::BTreeMap;

/// The owned route must reserve hydration bytes BEFORE constructing its image.
/// Its charge remains live through validation and planning of that row. The
/// borrowed route is a caller-admitted complete-image reference adapter only.
pub enum SelectedAdministrativeInput<'s, 'r> {
    BorrowedFullImage {
        key: &'s str,
        clause: usize,
        image: &'s RecordSnapshot,
    },
    OwnedOneRow {
        clause: usize,
        image: RecordSnapshot,
        hydration: QueueBufferCharge<'r>,
    },
}
#[derive(Default)]
struct ChargeList<'r> {
    head: Option<Box<ChargeNode<'r>>>,
}
struct ChargeNode<'r> {
    next: Option<Box<ChargeNode<'r>>>,
    _charge: QueueBufferCharge<'r>,
}
impl<'r> ChargeList<'r> {
    fn push(&mut self, charge: QueueBufferCharge<'r>) {
        self.head = Some(Box::new(ChargeNode {
            next: self.head.take(),
            _charge: charge,
        }));
    }
}
impl Drop for ChargeList<'_> {
    fn drop(&mut self) {
        while let Some(mut node) = self.head.take() {
            self.head = node.next.take();
        }
    }
}
/// Owned output retains its admission guards. Explicit copies through borrowed
/// accessors require their own caller admission. Its lifetime depends only on
/// the reservation, never on a source row or the complete-image adapter.
pub struct AdmittedAdministrativeGroupProposal<'r> {
    proposal: AdministrativeGroupProposal,
    initial: RecordSnapshot,
    _retained: ChargeList<'r>,
    _assembly: Option<QueueBufferCharge<'r>>,
    _root: QueueBufferCharge<'r>,
}
impl AdmittedAdministrativeGroupProposal<'_> {
    pub fn proposal(&self) -> &AdministrativeGroupProposal {
        &self.proposal
    }
    pub fn initial_authority(&self) -> &RecordSnapshot {
        &self.initial
    }
}
pub(super) fn add(a: u64, b: u64) -> QueueResult<u64> {
    a.checked_add(b)
        .ok_or_else(|| unknown("administrative allocation bound exhausted"))
}
pub(super) fn scale(a: u64, b: u64) -> QueueResult<u64> {
    a.checked_mul(b)
        .ok_or_else(|| unknown("administrative allocation bound exhausted"))
}
pub(super) fn wire<T: serde::Serialize>(value: &T) -> QueueResult<u64> {
    encoded_len(value).map_err(|_| unknown("administrative allocation measurement failed"))
}
pub(super) fn row_bound(row: &StoredWork) -> QueueResult<u64> {
    let certificates = match &row.field_coverage {
        crate::queue_fields::FieldCoverage::Complete => 0,
        crate::queue_fields::FieldCoverage::Selected { names, prefixes } => {
            add(names.len() as u64, prefixes.len() as u64)?
        }
    };
    add(
        scale(certificates, 1024)?,
        add(
            scale(wire(row)?, 12)?,
            add(2048, scale(row.record.work.fields.len() as u64, 1024)?)?,
        )?,
    )
}
pub(super) fn tuple_bound(
    tuple: &UniqueIdentity,
    owner: Option<&RecordIdentity>,
) -> QueueResult<u64> {
    let mut bytes = add(tuple.index.len() as u64, tuple.full_sort_key.len() as u64)?;
    bytes = add(
        bytes,
        tuple
            .collection
            .as_ref()
            .map_or(0, |name| name.len() as u64),
    )?;
    if let Some(owner) = owner {
        bytes = add(bytes, owner.key.len() as u64)?;
        bytes = add(
            bytes,
            owner
                .collection
                .as_ref()
                .map_or(0, |name| name.len() as u64),
        )?;
    }
    add(scale(bytes, 12)?, 2048)
}
fn snapshot_bound(image: &RecordSnapshot) -> QueueResult<u64> {
    let mut bytes = 2048;
    for row in image.work.values() {
        bytes = add(bytes, row_bound(row)?)?;
    }
    for row in image.state.values() {
        bytes = add(
            bytes,
            add(
                scale(wire(row)?, 12)?,
                add(2048, scale(row.fields.len() as u64, 1024)?)?,
            )?,
        )?;
    }
    for id in image.versions.keys() {
        bytes = add(bytes, add(scale(wire(id)?, 12)?, 512)?)?;
    }
    for tuple in &image.unique_probes_loaded {
        bytes = add(bytes, tuple_bound(tuple, image.unique_owners.get(tuple))?)?;
    }
    Ok(bytes)
}
pub(super) fn delta_bound(delta: &CommitDelta) -> QueueResult<u64> {
    let count = delta.work.len()
        + delta.state.len()
        + delta.versions.len()
        + delta.work_fields.len()
        + delta.work_bodies.len()
        + delta.deleted_work.len()
        + delta.deleted_state.len();
    add(
        scale(wire(delta)?, 12)?,
        add(2048, scale(count as u64, 1024)?)?,
    )
}
fn empty_delta(id: &str, next_sequence: u64) -> CommitDelta {
    CommitDelta {
        work: vec![],
        work_fields: BTreeMap::new(),
        work_bodies: BTreeMap::new(),
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: vec![],
        next_sequence,
        receipt: CommitReceipt {
            request_id: id.into(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        },
    }
}
pub(super) fn work_keys(group: &CommitGroup) -> impl Iterator<Item = &str> {
    group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
}
pub(super) fn ordinary_bound(group: &CommitGroup, image: &RecordSnapshot) -> QueueResult<u64> {
    let headers = crate::queue_memory::incoming_headers(group)
        .map_err(|_| unknown("ordinary container bound exhausted"))?;
    let mut bytes = add(add(scale(wire(group)?, 12)?, scale(headers, 3)?)?, 2048)?;
    for key in work_keys(group) {
        if let Some(row) = image.work.get(key) {
            bytes = add(bytes, row_bound(row)?)?;
        }
    }
    for update in &group.state_updates {
        if let Some(row) = image
            .state
            .iter()
            .find(|((collection, key), _)| collection == &update.collection && key == &update.key)
            .map(|(_, row)| row)
        {
            bytes = add(
                bytes,
                add(
                    scale(wire(row)?, 12)?,
                    add(2048, scale(row.fields.len() as u64, 1024)?)?,
                )?,
            )?;
        }
    }
    Ok(bytes)
}
pub(super) fn intent_headers(intent: &AdministrativeIntent) -> QueueResult<u64> {
    fn action(action: &AdministrativeAction) -> QueueResult<u64> {
        match action {
            AdministrativeAction::Update {
                patch,
                remove_field_prefixes,
                ..
            } => scale(
                add(
                    patch.fields.len() as u64,
                    remove_field_prefixes.len() as u64,
                )?,
                1024,
            ),
            AdministrativeAction::Delete { .. } => Ok(0),
        }
    }
    let mut bytes = scale(
        intent
            .before_fields
            .as_ref()
            .map_or(0, |fields| fields.len()) as u64,
        1024,
    )?;
    match &intent.operation {
        AdministrativeOperation::Addressed(entries) => {
            for entry in entries {
                bytes = add(bytes, action(&entry.action)?)?;
            }
        }
        AdministrativeOperation::SelectFirst(clauses) => {
            for clause in clauses {
                bytes = add(bytes, action(&clause.action)?)?;
            }
        }
    }
    Ok(bytes)
}
pub(super) fn tuple_identity_bound(delta: &CommitDelta, schema: &QueueSchema) -> QueueResult<u64> {
    delta.versions.iter().try_fold(0, |bytes, version| {
        let def = match &version.collection {
            None => &schema.work,
            Some(collection) => schema
                .state
                .get(collection)
                .ok_or_else(|| unknown("missing identity collection"))?,
        };
        let bound = crate::queue_memory::identity_expansion(
            def,
            &version.key,
            version.collection.as_deref(),
        )
        .map_err(|_| unknown("tuple identity bound exhausted"))?;
        // Both release and claim may copy the owner identity for every index.
        add(bytes, scale(bound, 2)?)
    })
}
struct Accumulator<'a, 'r> {
    ordinary: &'a CommitGroup,
    intent: &'a AdministrativeIntent,
    schema: &'a QueueSchema,
    limits: &'a QueueLimits,
    reservation: &'r QueueReservation,
    initial: RecordSnapshot,
    delta: CommitDelta,
    result: AdministrativeResult,
    explicit: BTreeSet<&'a str>,
    last: Option<(String, QueueBufferCharge<'r>)>,
    claims: BTreeMap<UniqueIdentity, RecordIdentity>,
    overlap: bool,
    collision: bool,
    poisoned: bool,
    retained: ChargeList<'r>,
    root: QueueBufferCharge<'r>,
}
impl<'a, 'r> Accumulator<'a, 'r> {
    fn new(
        ordinary: &'a CommitGroup,
        intent: &'a AdministrativeIntent,
        image: &RecordSnapshot,
        schema: &'a QueueSchema,
        limits: &'a QueueLimits,
        reservation: &'r QueueReservation,
    ) -> QueueResult<Self> {
        intent.validate(schema, limits)?;
        let root_bytes = add(
            intent_headers(intent)?,
            add(
                ordinary_bound(ordinary, image)?,
                add(
                    scale(wire(intent)?, 12)?,
                    add(scale(wire(schema)?, 12)?, 8192)?,
                )?,
            )?,
        )?;
        let result_roots = match &intent.operation {
            AdministrativeOperation::SelectFirst(clauses) => scale(clauses.len() as u64, 8192)?,
            AdministrativeOperation::Addressed(entries) => scale(entries.len() as u64, 512)?,
        };
        let root = reservation.buffer(add(root_bytes, result_roots)?)?;
        let clauses = match &intent.operation {
            AdministrativeOperation::SelectFirst(clauses) => clauses
                .iter()
                .map(|clause| AdministrativeClauseResult {
                    id: clause.id.clone(),
                    histogram: AdministrativeHistogram::default(),
                })
                .collect(),
            AdministrativeOperation::Addressed(_) => vec![],
        };
        Ok(Self {
            ordinary,
            intent,
            schema,
            limits,
            reservation,
            root,
            retained: Default::default(),
            initial: RecordSnapshot {
                next_sequence: image.next_sequence,
                ..Default::default()
            },
            delta: empty_delta(&ordinary.request_id, image.next_sequence),
            result: AdministrativeResult {
                histogram: AdministrativeHistogram::default(),
                items: vec![],
                clauses,
            },
            explicit: work_keys(ordinary).collect(),
            last: None,
            claims: BTreeMap::new(),
            overlap: false,
            collision: false,
            poisoned: false,
        })
    }
    fn doom(&mut self, overlap: bool) {
        let already_doomed = self.overlap || self.collision;
        self.overlap |= overlap;
        self.collision |= !overlap;
        if already_doomed {
            return;
        }
        let sequence = self.initial.next_sequence;
        // Release both containers and their capacities before releasing charges.
        self.initial = RecordSnapshot {
            next_sequence: sequence,
            ..Default::default()
        };
        self.delta = empty_delta(&self.ordinary.request_id, sequence);
        self.claims = BTreeMap::new();
        self.result.items = vec![];
        self.retained = Default::default();
    }
}

/// Iterator sources are consumed to EOF before private finalization. Native
/// activation still requires a directly streamed, exhausted fenced SQL source;
/// the complete-image adapter must never be used as its replacement.
#[allow(clippy::too_many_arguments)]
pub fn plan_broad_group<'s, 'r>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    targets: impl IntoIterator<Item = QueueResult<SelectedAdministrativeInput<'s, 'r>>>,
    ordinary_image: &RecordSnapshot,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<AdmittedAdministrativeGroupProposal<'r>> {
    if !matches!(intent.operation, AdministrativeOperation::SelectFirst(_)) {
        return Err(QueueError::Invalid(
            "broad driver requires selector administration".into(),
        ));
    }
    validate_snapshot(ordinary_image)?;
    let mut state = Accumulator::new(
        ordinary,
        intent,
        ordinary_image,
        schema,
        limits,
        reservation,
    )?;
    for target in targets {
        let target = target.map_err(|_| unknown("administrative source failed before EOF"))?;
        state.visit(target)?;
    }
    state.finish(ordinary_image, ownership_now)
}

/// The backend retains its queue fence through selection, final planning and
/// atomic publication; stream EOF alone never grants publication authority.
/// Every yielded owned row must already hold its hydration charge. This driver
/// shares the synchronous adapter's accumulator and finalizer; it does not
/// collect a queue image or expose intermediate administrative successes.
#[allow(clippy::too_many_arguments)]
pub async fn plan_broad_group_stream<'s, 'r>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    targets: impl futures::Stream<Item = QueueResult<SelectedAdministrativeInput<'s, 'r>>>,
    ordinary_image: &RecordSnapshot,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<AdmittedAdministrativeGroupProposal<'r>> {
    use futures::StreamExt;
    if !matches!(intent.operation, AdministrativeOperation::SelectFirst(_)) {
        return Err(QueueError::Invalid(
            "broad driver requires selector administration".into(),
        ));
    }
    validate_snapshot(ordinary_image)?;
    let mut state = Accumulator::new(
        ordinary,
        intent,
        ordinary_image,
        schema,
        limits,
        reservation,
    )?;
    futures::pin_mut!(targets);
    while let Some(target) = targets.next().await {
        let target = target.map_err(|_| unknown("administrative source failed before EOF"))?;
        state.visit(target)?;
    }
    state.finish(ordinary_image, ownership_now)
}

fn dependency_bound(proposal: &AdministrativeProposal, schema: &QueueSchema) -> QueueResult<u64> {
    let mut bytes = 2048;
    if let Some(ResolvedFields::Edit(edits)) = &proposal.resolved_fields {
        for name in edits.keys() {
            bytes = add(bytes, add(scale(name.len() as u64, 12)?, 512)?)?;
        }
        for columns in schema.work.indexes.values() {
            if columns.iter().any(|name| edits.contains_key(name)) {
                for name in columns {
                    bytes = add(bytes, add(scale(name.len() as u64, 12)?, 512)?)?;
                }
            }
        }
    } else if matches!(proposal.outcome, AdministrativeOutcome::Deleted { .. }) {
        for index in &schema.work.unique_indexes {
            for name in &schema.work.indexes[index] {
                bytes = add(bytes, add(scale(name.len() as u64, 12)?, 512)?)?;
            }
        }
    }
    Ok(bytes)
}
fn dependencies(proposal: &AdministrativeProposal, schema: &QueueSchema) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    if let Some(ResolvedFields::Edit(edits)) = &proposal.resolved_fields {
        names.extend(edits.keys().cloned());
        for columns in schema.work.indexes.values() {
            if columns.iter().any(|name| edits.contains_key(name)) {
                names.extend(columns.iter().cloned());
            }
        }
    } else if matches!(proposal.outcome, AdministrativeOutcome::Deleted { .. }) {
        for index in &schema.work.unique_indexes {
            names.extend(schema.work.indexes[index].iter().cloned());
        }
    }
    names
}
fn prune(row: &StoredWork, names: &BTreeSet<String>, body: Option<&ResolvedBody>) -> StoredWork {
    let (body, body_loaded) = match body {
        Some(ResolvedBody::Replace(bytes)) => (bytes.clone(), true),
        _ => (Default::default(), false),
    };
    StoredWork {
        record: WorkRecord {
            work: NewWork {
                key: row.record.work.key.clone(),
                priority: row.record.work.priority,
                available_at: row.record.work.available_at,
                fields: names
                    .iter()
                    .filter_map(|name| {
                        row.record
                            .work
                            .fields
                            .get(name)
                            .map(|value| (name.clone(), value.clone()))
                    })
                    .collect(),
                body,
            },
            version: row.record.version,
            insertion_sequence: row.record.insertion_sequence,
            lifecycle: row.record.lifecycle,
            eligible_since: row.record.eligible_since,
            attempts: row.record.attempts,
            lease_expires_at: row.record.lease_expires_at,
        },
        lease_hash: row.lease_hash,
        body_loaded,
        field_coverage: crate::queue_fields::FieldCoverage::Selected {
            names: names.clone(),
            prefixes: BTreeSet::new(),
        },
    }
}
pub(super) fn required_tuples(
    effects: &crate::queue_fields::UniqueFieldEffects,
) -> impl Iterator<Item = &UniqueIdentity> {
    effects
        .releases
        .keys()
        .chain(effects.claims.iter().map(|(tuple, _)| tuple))
}
pub(super) fn copy_probe(
    target: &mut RecordSnapshot,
    source: &RecordSnapshot,
    tuple: &UniqueIdentity,
) -> QueueResult<()> {
    if !source.unique_probes_loaded.contains(tuple) {
        return Err(unknown("administrative unique probe not loaded"));
    }
    let owner = source.unique_owners.get(tuple);
    if target.unique_probes_loaded.contains(tuple) && target.unique_owners.get(tuple) != owner {
        return Err(unknown(
            "contradictory initial administrative unique authority",
        ));
    }
    target.unique_probes_loaded.insert(tuple.clone());
    if let Some(owner) = owner {
        target.unique_owners.insert(tuple.clone(), owner.clone());
    }
    Ok(())
}
fn selected_clause<'a>(
    row: &StoredWork,
    clause: usize,
    intent: &'a AdministrativeIntent,
    schema: &QueueSchema,
) -> QueueResult<&'a AdministrativeClause> {
    let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
        return Err(unknown("selection driver lacks selector intent"));
    };
    if clause >= clauses.len() {
        return Err(unknown("administrative clause outside initial ownership"));
    }
    let mut first = None;
    for (index, definition) in clauses.iter().enumerate().take(clause + 1) {
        if checked_selector_matches(&definition.selector, row, intent.evaluated_at, &schema.work)? {
            first = Some(index);
            break;
        }
    }
    if first != Some(clause) {
        return Err(unknown("administrative first-clause ownership mismatch"));
    }
    Ok(&clauses[clause])
}

pub(super) fn selected_row<'s>(
    key: &str,
    image: &'s RecordSnapshot,
    next_sequence: u64,
) -> QueueResult<&'s StoredWork> {
    let row = image
        .work
        .get(key)
        .ok_or_else(|| unknown("selected administrative row missing"))?;
    let id = RecordIdentity {
        collection: None,
        key: key.into(),
    };
    if image.next_sequence != next_sequence
        || row.record.work.key != key
        || row.record.version == 0
        || image.versions.get(&id) != Some(&row.record.version)
        || row.record.insertion_sequence == 0
        || row.record.insertion_sequence > image.next_sequence
    {
        return Err(unknown("inconsistent streamed administrative authority"));
    }
    CheckedFields::new(&row.record.work.fields, &row.field_coverage)?;
    Ok(row)
}

/// One row's already resolved mutation and exact initial unique authority.
/// Payloads are dropped before their admission guard. This ephemeral candidate
/// grants neither selected-source completeness nor publication authority.
pub(super) struct AcceptedCandidate<'r> {
    pub(super) accepted: bool,
    pub(super) before: RecordSnapshot,
    pub(super) delta: CommitDelta,
    pub(super) effects: crate::queue_fields::UniqueFieldEffects,
    pub(super) _charge: QueueBufferCharge<'r>,
}
pub(super) fn resolve_candidate<'r>(
    key: &str,
    row: &StoredWork,
    image: &RecordSnapshot,
    proposal: &mut AdministrativeProposal,
    schema: &QueueSchema,
    reservation: &'r QueueReservation,
) -> QueueResult<Option<AcceptedCandidate<'r>>> {
    if !matches!(
        proposal.outcome,
        AdministrativeOutcome::Changed { .. } | AdministrativeOutcome::Deleted { .. }
    ) {
        return Ok(None);
    }
    // Reserve copies of old/new dependency values and metadata before pruning.
    let row_charge = reservation.buffer(row_bound(row)?)?;
    let names_charge = reservation.buffer(dependency_bound(proposal, schema)?)?;
    let names = dependencies(proposal, schema);
    let old = prune(row, &names, None);
    let new = proposal
        .replacement
        .as_ref()
        .map(|row| prune(row, &names, proposal.resolved_body.as_ref()));
    let version = match proposal.outcome {
        AdministrativeOutcome::Changed { version } | AdministrativeOutcome::Deleted { version } => {
            version
        }
        _ => unreachable!(),
    };
    let id = RecordIdentity {
        collection: None,
        key: key.into(),
    };
    let mut before = RecordSnapshot {
        next_sequence: image.next_sequence,
        ..Default::default()
    };
    before.versions.insert(id.clone(), row.record.version);
    before.work.insert(key.into(), old);
    let mut delta = empty_delta("", image.next_sequence);
    if let Some(new) = new {
        delta.work.push(new);
        delta.work_fields.insert(
            key.into(),
            proposal
                .resolved_fields
                .take()
                .ok_or_else(|| unknown("changed proposal lacks fields"))?,
        );
        delta.work_bodies.insert(
            key.into(),
            proposal
                .resolved_body
                .take()
                .ok_or_else(|| unknown("changed proposal lacks body intent"))?,
        );
    } else {
        delta.deleted_work.push(key.into());
    }
    delta.versions.push(RecordVersion {
        collection: None,
        key: key.into(),
        version,
    });
    let mut scratch_bytes = add(delta_bound(&delta)?, tuple_identity_bound(&delta, schema)?)?;
    for row in before.work.values().chain(delta.work.iter()) {
        scratch_bytes = add(
            scratch_bytes,
            crate::queue_memory::fields_expansion(
                row.record
                    .work
                    .fields
                    .iter()
                    .map(|(name, value)| (name.as_str(), Some(value))),
                &schema.work,
            )
            .map_err(|_| unknown("administrative tuple bound exhausted"))?,
        )?;
    }
    let tuple_charge = reservation.buffer(scale(scratch_bytes, 4)?)?;
    let effects = unique_effects(&delta, &before, schema)?;
    let mut probe_bytes = 2048;
    for tuple in required_tuples(&effects) {
        probe_bytes = add(
            probe_bytes,
            tuple_bound(tuple, image.unique_owners.get(tuple))?,
        )?;
    }
    let probe_charge = reservation.buffer(probe_bytes)?;
    // External owner identifiers can dwarf this row. Own every charge before
    // fallible probe copies so failure drops payloads before their guards.
    let mut candidate = AcceptedCandidate {
        accepted: true,
        before,
        delta,
        effects,
        _charge: row_charge
            .merge(names_charge)?
            .merge(tuple_charge)?
            .merge(probe_charge)?,
    };
    for tuple in required_tuples(&candidate.effects) {
        copy_probe(&mut candidate.before, image, tuple)?;
    }
    candidate.accepted = match candidate.effects.validate(&candidate.before)? {
        Err(Rejection::UniqueConflict) => {
            proposal.outcome = AdministrativeOutcome::UniqueConflict;
            false
        }
        Err(_) => return Err(unknown("unexpected administrative unique rejection")),
        Ok(()) => true,
    };
    // Keep consulted initial authority even for a per-row business rejection.
    Ok(Some(candidate))
}

fn append_delta(target: &mut CommitDelta, mut delta: CommitDelta) {
    target.work.append(&mut delta.work);
    target.work_fields.append(&mut delta.work_fields);
    target.work_bodies.append(&mut delta.work_bodies);
    target.deleted_work.append(&mut delta.deleted_work);
    target.state.append(&mut delta.state);
    target.deleted_state.append(&mut delta.deleted_state);
    target.versions.append(&mut delta.versions);
}
impl Accumulator<'_, '_> {
    fn visit(&mut self, input: SelectedAdministrativeInput<'_, '_>) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("administrative scan is permanently poisoned"));
        }
        self.poisoned = true;
        let result = match &input {
            SelectedAdministrativeInput::BorrowedFullImage { key, clause, image } => {
                self.visit_broad(key, *clause, image)
            }
            SelectedAdministrativeInput::OwnedOneRow {
                clause,
                image,
                hydration: _,
            } => {
                if image.work.len() != 1 || image.versions.len() != 1 || !image.state.is_empty() {
                    Err(unknown(
                        "streamed administrative authority is not one live row",
                    ))
                } else {
                    let key = image.work.keys().next().expect("one row");
                    self.visit_broad(key, *clause, image)
                }
            }
        };
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    fn row<'s>(&self, key: &str, image: &'s RecordSnapshot) -> QueueResult<&'s StoredWork> {
        selected_row(key, image, self.initial.next_sequence)
    }
    fn visit_broad(&mut self, key: &str, clause: usize, image: &RecordSnapshot) -> QueueResult<()> {
        let _key_scratch = self
            .reservation
            .buffer(add(scale(key.len() as u64, 4)?, 2048)?)?;
        if self
            .last
            .as_ref()
            .is_some_and(|(previous, _)| previous.as_bytes() >= key.as_bytes())
        {
            return Err(unknown(
                "administrative stream keys are not strictly ordered",
            ));
        }
        let last_charge = self.reservation.buffer(add(key.len() as u64, 256)?)?;
        self.last = Some((key.into(), last_charge));
        let row = self.row(key, image)?;
        let definition = selected_clause(row, clause, self.intent, self.schema)?;
        if self.explicit.contains(key) {
            self.doom(true);
        }
        if self.overlap || self.collision {
            return Ok(());
        }
        let _planning = self.reservation.buffer(row_bound(row)?)?;
        let proposal = propose_target_view(
            TargetRequest {
                key,
                expected_version: None,
                precondition: &definition.precondition,
                action: &definition.action,
            },
            Some(row),
            Some(row.record.version),
            self.intent.evaluated_at,
            None,
            self.schema,
            self.limits,
        )?;
        self.accept(key, row, image, proposal, Some(clause))
    }
    fn accept(
        &mut self,
        key: &str,
        row: &StoredWork,
        image: &RecordSnapshot,
        mut proposal: AdministrativeProposal,
        clause: Option<usize>,
    ) -> QueueResult<()> {
        if let Some(AcceptedCandidate {
            mut before,
            delta,
            effects,
            accepted,
            _charge,
        }) = resolve_candidate(
            key,
            row,
            image,
            &mut proposal,
            self.schema,
            self.reservation,
        )? && accepted
        {
            let mut retained_bytes = add(snapshot_bound(&before)?, delta_bound(&delta)?)?;
            for tuple in required_tuples(&effects) {
                retained_bytes = add(
                    retained_bytes,
                    tuple_bound(tuple, before.unique_owners.get(tuple))?,
                )?;
            }
            let charge = self.reservation.buffer(retained_bytes)?;
            for (tuple, owner) in &effects.claims {
                if self
                    .claims
                    .get(tuple)
                    .is_some_and(|previous| previous != owner)
                {
                    self.doom(false);
                    return Ok(());
                }
            }
            for tuple in &before.unique_probes_loaded {
                copy_probe(&mut self.initial, &before, tuple)?;
            }
            self.initial.work.append(&mut before.work);
            self.initial.versions.append(&mut before.versions);
            for (tuple, owner) in effects.claims {
                self.claims.insert(tuple, owner);
            }
            append_delta(&mut self.delta, delta);
            self.retained.push(charge);
        }
        self.result.histogram.record(&proposal.outcome)?;
        if let Some(clause) = clause {
            self.result.clauses[clause]
                .histogram
                .record(&proposal.outcome)?;
        } else {
            let bytes = add(
                scale(wire(&proposal.before)?, 12)?,
                add(scale(key.len() as u64, 12)?, 2048)?,
            )?;
            let charge = self.reservation.buffer(bytes)?;
            self.result.items.push(AdministrativeItemResult {
                key: key.into(),
                outcome: proposal.outcome,
                before: proposal.before,
            });
            self.retained.push(charge);
        }
        Ok(())
    }
}

impl<'r> Accumulator<'_, 'r> {
    fn rejected(mut self, reason: Rejection) -> AdmittedAdministrativeGroupProposal<'r> {
        let sequence = self.initial.next_sequence;
        self.initial = RecordSnapshot {
            next_sequence: sequence,
            ..Default::default()
        };
        self.delta = empty_delta(&self.ordinary.request_id, sequence);
        self.delta.receipt.outcome = GroupOutcome::Rejected(reason);
        self.claims = BTreeMap::new();
        self.result.items = vec![];
        self.retained = Default::default();
        AdmittedAdministrativeGroupProposal {
            proposal: AdministrativeGroupProposal {
                delta: self.delta,
                administration: None,
            },
            initial: self.initial,
            _root: self.root,
            _retained: Default::default(),
            _assembly: None,
        }
    }
    fn finish(
        mut self,
        ordinary_image: &RecordSnapshot,
        ownership_now: impl FnOnce() -> QueueResult<i64>,
    ) -> QueueResult<AdmittedAdministrativeGroupProposal<'r>> {
        if self.poisoned {
            return Err(unknown("poisoned administrative scan cannot finish"));
        }
        if self.overlap {
            return Ok(self.rejected(Rejection::Invalid(
                "administrative target overlaps explicit group work".into(),
            )));
        }
        if self.collision {
            return Ok(self.rejected(Rejection::UniqueConflict));
        }
        let normal_charge = self
            .reservation
            .buffer(ordinary_bound(self.ordinary, ordinary_image)?)?;
        let mut candidate = addressed_snapshot(self.ordinary, ordinary_image);
        validate_group_snapshot(&candidate, self.ordinary)?;
        let now = ownership_now()
            .map_err(|_| unknown("operational clock failed during administrative finalization"))?;
        let mut normal = match apply_group(self.ordinary, &mut candidate, now, self.schema) {
            Ok(delta) => delta,
            Err(reason) => return Ok(self.rejected(reason)),
        };
        resolve_group_writes(self.ordinary, ordinary_image, &mut normal, self.schema)?;
        let assembly_bytes = add(
            add(delta_bound(&self.delta)?, delta_bound(&normal)?)?,
            add(
                snapshot_bound(&self.initial)?,
                ordinary_bound(self.ordinary, ordinary_image)?,
            )?,
        )?;
        let assembly = self.reservation.buffer(assembly_bytes)?;
        let normal_before = addressed_snapshot(self.ordinary, ordinary_image);
        // Overlap has already been checked, including skipped and absent targets.
        self.initial.work.extend(normal_before.work);
        self.initial.state.extend(normal_before.state);
        self.initial.versions.extend(normal_before.versions);
        let receipt = normal.receipt.clone();
        let administrative = std::mem::replace(
            &mut self.delta,
            empty_delta(&self.ordinary.request_id, self.initial.next_sequence),
        );
        let mut combined = combine_deltas(&[administrative, normal], self.initial.next_sequence)?;
        let mut tuple_bytes = add(2048, tuple_identity_bound(&combined, self.schema)?)?;
        for row in self.initial.work.values().chain(combined.work.iter()) {
            tuple_bytes = add(
                tuple_bytes,
                crate::queue_memory::fields_expansion(
                    row.record
                        .work
                        .fields
                        .iter()
                        .map(|(name, value)| (name.as_str(), Some(value))),
                    &self.schema.work,
                )
                .map_err(|_| unknown("final administrative tuple bound exhausted"))?,
            )?;
        }
        for row in self.initial.state.values().chain(combined.state.iter()) {
            tuple_bytes = add(
                tuple_bytes,
                crate::queue_memory::fields_expansion(
                    row.fields
                        .iter()
                        .map(|(name, value)| (name.as_str(), Some(value))),
                    &self.schema.state[&row.collection],
                )
                .map_err(|_| unknown("final state tuple bound exhausted"))?,
            )?;
        }
        let _tuple_scratch = self.reservation.buffer(scale(tuple_bytes, 4)?)?;
        let effects = unique_effects(&combined, &self.initial, self.schema)?;
        let mut probe_bytes = 2048;
        for tuple in required_tuples(&effects) {
            let owner = if ordinary_image.unique_probes_loaded.contains(tuple) {
                ordinary_image.unique_owners.get(tuple)
            } else {
                self.initial.unique_owners.get(tuple)
            };
            probe_bytes = add(probe_bytes, tuple_bound(tuple, owner)?)?;
        }
        let probe_charge = self.reservation.buffer(probe_bytes)?;
        for tuple in required_tuples(&effects) {
            if ordinary_image.unique_probes_loaded.contains(tuple) {
                copy_probe(&mut self.initial, ordinary_image, tuple)?;
            } else if !self.initial.unique_probes_loaded.contains(tuple) {
                return Err(unknown("final ordinary unique probe not loaded"));
            }
        }
        if let Err(reason) = effects.validate(&self.initial)? {
            // The aggregate was moved into combine_deltas; restore only the
            // empty receipt carrier before discarding every candidate effect.
            self.delta = empty_delta(&self.ordinary.request_id, self.initial.next_sequence);
            drop(combined);
            return Ok(self.rejected(reason));
        }
        combined.receipt = receipt;
        self.retained.push(normal_charge);
        self.retained.push(probe_charge);
        Ok(AdmittedAdministrativeGroupProposal {
            proposal: AdministrativeGroupProposal {
                delta: combined,
                administration: Some(self.result),
            },
            initial: self.initial,
            _root: self.root,
            _retained: self.retained,
            _assembly: Some(assembly),
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan_complete_group<'r>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    selected: &[SelectedAdministrativeTarget],
    image: &RecordSnapshot,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<AdmittedAdministrativeGroupProposal<'r>> {
    intent.validate(schema, limits)?;
    validate_snapshot(image)?;
    if image.work.values().any(|row| {
        !matches!(
            row.field_coverage,
            crate::queue_fields::FieldCoverage::Complete
        )
    }) {
        return Err(unknown(
            "complete-image administrative adapter requires complete authority",
        ));
    }
    match &intent.operation {
        AdministrativeOperation::SelectFirst(_) => {
            let _sorting = reservation.buffer(add(scale(selected.len() as u64, 32)?, 2048)?)?;
            let mut ordered: Vec<_> = selected.iter().collect();
            ordered.sort_unstable_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
            plan_broad_group(
                ordinary,
                intent,
                ordered.into_iter().map(|target| {
                    Ok(SelectedAdministrativeInput::BorrowedFullImage {
                        key: &target.key,
                        clause: target.clause,
                        image,
                    })
                }),
                image,
                ownership_now,
                schema,
                limits,
                reservation,
            )
        }
        AdministrativeOperation::Addressed(entries) => {
            if !selected.is_empty() {
                return Err(unknown("addressed administration has selector targets"));
            }
            let mut state = Accumulator::new(ordinary, intent, image, schema, limits, reservation)?;
            if entries
                .iter()
                .any(|entry| state.explicit.contains(entry.key.as_str()))
            {
                state.doom(true);
                return state.finish(image, ownership_now);
            }
            let mut seen = BTreeSet::new();
            for entry in entries {
                state.poisoned = true;
                let target = image.work.get(&entry.key);
                let planning_bytes = if let Some(row) = target {
                    row_bound(row)?
                } else {
                    2048
                };
                let _planning = reservation.buffer(planning_bytes)?;
                let duplicate = !seen.insert(entry.key.as_str());
                let request = TargetRequest {
                    expected_version: if duplicate {
                        Some(0)
                    } else {
                        entry.expected_version
                    },
                    ..entry.into()
                };
                let id = RecordIdentity {
                    collection: None,
                    key: entry.key.clone(),
                };
                let mut proposal = propose_target_view(
                    request,
                    target,
                    image.versions.get(&id).copied(),
                    intent.evaluated_at,
                    intent.before_fields.as_ref(),
                    schema,
                    limits,
                )?;
                if duplicate {
                    proposal.outcome = AdministrativeOutcome::Invalid;
                }
                if let Some(row) = target {
                    state.accept(&entry.key, row, image, proposal, None)?;
                } else {
                    let charge =
                        reservation.buffer(add(scale(entry.key.len() as u64, 12)?, 2048)?)?;
                    state.result.histogram.record(&proposal.outcome)?;
                    state.result.items.push(AdministrativeItemResult {
                        key: entry.key.clone(),
                        outcome: proposal.outcome,
                        before: None,
                    });
                    state.retained.push(charge);
                }
                state.poisoned = false;
                if state.collision {
                    break;
                }
            }
            state.finish(image, ownership_now)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{ordinary, row, schema, update};
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use std::cell::Cell;
    fn intent(action: AdministrativeAction) -> AdministrativeIntent {
        AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action,
            }]),
        }
    }
    fn one_row(mut row: StoredWork, key: &str, sequence: u64) -> RecordSnapshot {
        row.record.work.key = key.into();
        row.record.insertion_sequence = 1;
        RecordSnapshot {
            work: BTreeMap::from([(key.into(), row)]),
            versions: BTreeMap::from([(
                RecordIdentity {
                    collection: None,
                    key: key.into(),
                },
                1,
            )]),
            next_sequence: sequence,
            ..Default::default()
        }
    }
    #[test]
    fn asynchronous_source_suspension_failure_and_owner_drop_never_finish_early() {
        use futures::{FutureExt, task::noop_waker_ref};
        use std::task::{Context, Poll};
        let limits = QueueLimits::default();
        for fail in [false, true] {
            let admission = QueueAdmission::new(1, 8 << 20).unwrap();
            let job = admission.reserve(0).unwrap();
            let phase = Cell::new(0);
            let clock_calls = Cell::new(0);
            let definition = schema();
            let request = ordinary();
            let mut action = update();
            if let AdministrativeAction::Update { patch, .. } = &mut action {
                patch.priority = Some(7);
            }
            let operation = intent(action);
            let before = RecordSnapshot {
                next_sequence: 1,
                ..Default::default()
            };
            let source = futures::stream::poll_fn(|cx| match phase.get() {
                0 => {
                    phase.set(1);
                    let hydration = job.buffer(64 << 10).unwrap();
                    Poll::Ready(Some(Ok(SelectedAdministrativeInput::OwnedOneRow {
                        clause: 0,
                        image: one_row(row(), "row", 1),
                        hydration,
                    })))
                }
                1 => {
                    phase.set(2);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                2 if fail => {
                    phase.set(3);
                    Poll::Ready(Some(Err(QueueError::Busy)))
                }
                _ => Poll::Ready(None),
            });
            let future = plan_broad_group_stream(
                &request,
                &operation,
                source,
                &before,
                || {
                    assert!(phase.get() >= 2);
                    clock_calls.set(clock_calls.get() + 1);
                    Ok(0)
                },
                &definition,
                &limits,
                &job,
            );
            futures::pin_mut!(future);
            assert!(
                future
                    .as_mut()
                    .poll_unpin(&mut Context::from_waker(noop_waker_ref()))
                    .is_pending()
            );
            assert_eq!(clock_calls.get(), 0);
            assert!(admission.usage().unwrap().1 > 0);
            let result = futures::executor::block_on(future);
            if fail {
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
                assert_eq!(clock_calls.get(), 0);
            } else {
                let plan = result.unwrap();
                assert_eq!(clock_calls.get(), 1);
                assert_eq!(plan.proposal().delta.work.len(), 1);
                assert_eq!(plan.proposal().delta.work[0].record.work.priority, 7);
                assert!(admission.usage().unwrap().1 > 0);
                drop(plan);
            }
            assert_eq!(admission.usage().unwrap().1, 0);
        }
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let definition = schema();
        let request = ordinary();
        let operation = intent(update());
        let before = RecordSnapshot::default();
        let calls = Cell::new(0);
        let source = futures::stream::pending::<QueueResult<SelectedAdministrativeInput<'_, '_>>>();
        let mut future = Box::pin(plan_broad_group_stream(
            &request,
            &operation,
            source,
            &before,
            || {
                calls.set(calls.get() + 1);
                Ok(0)
            },
            &definition,
            &limits,
            &job,
        ));
        assert!(
            future
                .as_mut()
                .poll_unpin(&mut Context::from_waker(noop_waker_ref()))
                .is_pending()
        );
        assert!(admission.usage().unwrap().1 > 0);
        drop(future);
        assert_eq!(calls.get(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn sparse_absence_certificates_are_charged_before_proposal_clones() {
        let admission = QueueAdmission::new(1, 24 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let hydration = job.buffer(16 << 20).unwrap();
        let mut target = row();
        target.field_coverage = crate::queue_fields::FieldCoverage::Selected {
            names: (0..10000).map(|n| format!("/payload/{n}")).collect(),
            prefixes: BTreeSet::new(),
        };
        assert!(scale(wire(&target).unwrap(), 12).unwrap() < 8 << 20);
        assert!(row_bound(&target).unwrap() > 8 << 20);
        let definition = schema();
        let request = ordinary();
        let operation = intent(update());
        let before = RecordSnapshot {
            next_sequence: 1,
            ..Default::default()
        };
        let clocks = Cell::new(0);
        let result = plan_broad_group(
            &request,
            &operation,
            [Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image: one_row(target, "row", 1),
                hydration,
            })],
            &before,
            || {
                clocks.set(clocks.get() + 1);
                Ok(0)
            },
            &definition,
            &QueueLimits::default(),
            &job,
        );
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
        assert_eq!(clocks.get(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn long_identity_index_multiplicity_is_admitted_before_tuple_generation() {
        let mut definition = schema();
        definition
            .work
            .fields
            .insert("index".into(), FieldType::Text);
        for n in 0..100 {
            let name = format!("index-{n}");
            definition
                .work
                .indexes
                .insert(name.clone(), vec!["index".into()]);
            definition.work.unique_indexes.insert(name);
        }
        let mut delta = empty_delta("request", 0);
        delta.versions.push(RecordVersion {
            collection: None,
            key: "k".repeat(80 << 10),
            version: 1,
        });
        let required = scale(tuple_identity_bound(&delta, &definition).unwrap(), 4).unwrap();
        assert!(required > 60 << 20);
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        assert!(matches!(
            job.buffer(required),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap().1, 0);
        // Exercise both production precharges; empty probe authority would instead
        // fail later if tuple generation were allowed to run before admission.
        let operation = intent(update());
        let mut request = ordinary();
        let mut inserted = row().record.work;
        inserted.key = "k".repeat(80 << 10);
        inserted
            .fields
            .insert("index".into(), FieldValue::Text("new".into()));
        request.work_inserts.push(inserted);
        let before = RecordSnapshot::default();
        let error = plan_broad_group(
            &request,
            &operation,
            std::iter::empty::<QueueResult<SelectedAdministrativeInput<'_, '_>>>(),
            &before,
            || Ok(0),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .err()
        .unwrap();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains("buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap().1, 0);
        let hydration = job.buffer(4 << 20).unwrap();
        let mut target = row();
        target
            .record
            .work
            .fields
            .insert("index".into(), FieldValue::Text("old".into()));
        let mut action = update();
        if let AdministrativeAction::Update { patch, .. } = &mut action {
            patch
                .fields
                .insert("index".into(), Some(FieldValue::Text("new".into())));
        }
        let operation = intent(action);
        let before = RecordSnapshot {
            next_sequence: 1,
            ..Default::default()
        };
        let error = plan_broad_group(
            &ordinary(),
            &operation,
            [Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image: one_row(target, &"k".repeat(80 << 10), 1),
                hydration,
            })],
            &before,
            || Ok(0),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .err()
        .unwrap();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains("buffer capacity exhausted"))
        );
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn generated_5001_noops_retain_constant_usage_and_output_owns_charges() {
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let template = row();
        let definition = schema();
        let request = ordinary();
        let operation = intent(update());
        let before = RecordSnapshot {
            next_sequence: 5001,
            ..Default::default()
        };
        let observed = Cell::new(None);
        let clocks = Cell::new(0);
        let targets = (0..5001).map(|index| {
            // Read-side hydration is admitted before cloning/constructing a row.
            let hydration = job.buffer(row_bound(&template).unwrap()).unwrap();
            let mut image = one_row(template.clone(), &format!("row-{index:05}"), 5001);
            image
                .work
                .values_mut()
                .next()
                .unwrap()
                .record
                .insertion_sequence = index as u64 + 1;
            let usage = admission.usage().unwrap().1;
            if index > 1 {
                assert_eq!(observed.get().unwrap(), usage);
            } else if index == 1 {
                observed.set(Some(usage));
            }
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image,
                hydration,
            })
        });
        let plan = plan_broad_group(
            &request,
            &operation,
            targets,
            &before,
            || {
                clocks.set(clocks.get() + 1);
                Ok(0)
            },
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .unwrap();
        assert_eq!(clocks.get(), 1);
        assert_eq!(
            plan.proposal()
                .administration
                .as_ref()
                .unwrap()
                .histogram
                .matched,
            5001
        );
        assert!(plan.proposal().delta.work.is_empty() && plan.proposal().delta.versions.is_empty());
        assert!(
            plan.initial_authority().work.is_empty()
                && plan.initial_authority().unique_probes_loaded.is_empty()
        );
        assert!(admission.usage().unwrap().1 > 0);
        drop(plan);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn changed_metadata_discards_large_selector_fields_prefixes_and_body() {
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let hydration = job.buffer(32 << 20).unwrap();
        let mut template = row();
        template
            .record
            .work
            .fields
            .insert("/payload/large".into(), FieldValue::Bytes(vec![9; 1 << 20]));
        template.record.work.body = bytes::Bytes::from(vec![8; 1 << 20]);
        template.body_loaded = true;
        let mut image = one_row(template.clone(), "a", 2);
        let mut other = one_row(template, "b", 2);
        other
            .work
            .values_mut()
            .next()
            .unwrap()
            .record
            .insertion_sequence = 2;
        image.work.extend(other.work);
        image.versions.extend(other.versions);
        let mut operation = intent(AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(9),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        });
        let AdministrativeOperation::SelectFirst(clauses) = &mut operation.operation else {
            unreachable!()
        };
        clauses[0].selector = WorkSelector::leaf(SelectorNode::Present {
            name: "/payload/large".into(),
            present: true,
        });
        let definition = schema();
        let mut request = ordinary();
        request.request_id = "i".repeat(80 << 10);
        let normal = RecordSnapshot {
            next_sequence: 2,
            ..Default::default()
        };
        let plan = plan_broad_group(
            &request,
            &operation,
            ["a", "b"].into_iter().map(|key| {
                Ok(SelectedAdministrativeInput::BorrowedFullImage {
                    key,
                    clause: 0,
                    image: &image,
                })
            }),
            &normal,
            || Ok(0),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .unwrap();
        // The result type does not borrow the source: it can be destroyed now.
        drop(image);
        drop(hydration);
        assert_eq!(plan.proposal().delta.work.len(), 2);
        for target in plan
            .initial_authority()
            .work
            .values()
            .chain(plan.proposal().delta.work.iter())
        {
            assert!(target.record.work.fields.is_empty());
            assert!(target.record.work.body.is_empty() && !target.body_loaded);
            assert!(
                matches!(&target.field_coverage, crate::queue_fields::FieldCoverage::Selected { names, prefixes } if names.is_empty() && prefixes.is_empty())
            );
        }
        assert!(
            plan.proposal()
                .delta
                .work_fields
                .values()
                .all(|command| *command == ResolvedFields::Preserve)
        );
        assert!(
            plan.proposal()
                .delta
                .work_bodies
                .values()
                .all(|command| *command == ResolvedBody::Preserve)
        );
        assert_eq!(plan.proposal().delta.receipt.request_id.len(), 80 << 10);
        assert!(
            admission.usage().unwrap().1 < 12 << 20,
            "opaque inputs and repeated request IDs must not remain charged"
        );
        drop(plan);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn source_order_capacity_and_swallowed_visit_errors_cannot_finish() {
        let definition = schema();
        let operation = intent(update());
        let request = ordinary();
        let image = one_row(row(), "a", 1);
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let clocks = Cell::new(0);
        let source = [
            Ok(SelectedAdministrativeInput::BorrowedFullImage {
                key: "a",
                clause: 0,
                image: &image,
            }),
            Err(QueueError::Busy),
        ];
        assert!(matches!(
            plan_broad_group(
                &request,
                &operation,
                source,
                &image,
                || {
                    clocks.set(1);
                    Ok(0)
                },
                &definition,
                &QueueLimits::default(),
                &job
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(clocks.get(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
        let limits = QueueLimits::default();
        let mut state =
            Accumulator::new(&request, &operation, &image, &definition, &limits, &job).unwrap();
        state
            .visit(SelectedAdministrativeInput::BorrowedFullImage {
                key: "a",
                clause: 0,
                image: &image,
            })
            .unwrap();
        assert!(
            state
                .visit(SelectedAdministrativeInput::BorrowedFullImage {
                    key: "a",
                    clause: 0,
                    image: &image
                })
                .is_err()
        );
        assert!(
            state
                .visit(SelectedAdministrativeInput::BorrowedFullImage {
                    key: "a",
                    clause: 0,
                    image: &image
                })
                .is_err()
        );
        assert!(matches!(
            state.finish(&image, || panic!("poisoned scan sampled ordinary clock")),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap().1, 0);
        let saturated = job.buffer((1 << 20) - 32).unwrap();
        assert!(matches!(
            plan_broad_group(
                &request,
                &operation,
                std::iter::empty(),
                &image,
                || panic!("capacity failure sampled ordinary clock"),
                &definition,
                &QueueLimits::default(),
                &job
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(saturated);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    fn unique_schema() -> QueueSchema {
        let mut definition = schema();
        definition
            .work
            .fields
            .insert("/payload/u".into(), FieldType::Text);
        definition
            .work
            .indexes
            .insert("u".into(), vec!["/payload/u".into()]);
        definition.work.unique_indexes.insert("u".into());
        definition.validate().unwrap();
        definition
    }
    fn tuple(value: &str) -> UniqueIdentity {
        UniqueIdentity {
            collection: None,
            index: "u".into(),
            full_sort_key: crate::queue_index::ordered_key(&[Some(&FieldValue::Text(
                value.into(),
            ))])
            .unwrap(),
        }
    }
    fn unique_row(key: &str, value: &str, sequence: u64) -> RecordSnapshot {
        let mut target = row();
        target
            .record
            .work
            .fields
            .insert("/payload/u".into(), FieldValue::Text(value.into()));
        let mut image = one_row(target, key, sequence);
        image
            .work
            .values_mut()
            .next()
            .unwrap()
            .record
            .insertion_sequence = match key {
            "b" => 2,
            "c" => 3,
            _ => 1,
        };
        image.unique_probes_loaded.insert(tuple(value));
        image.unique_owners.insert(
            tuple(value),
            RecordIdentity {
                collection: None,
                key: key.into(),
            },
        );
        image
    }
    fn rekey(value: &str) -> AdministrativeAction {
        AdministrativeAction::Update {
            patch: WorkPatch {
                fields: BTreeMap::from([(
                    "/payload/u".into(),
                    Some(FieldValue::Text(value.into())),
                )]),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        }
    }
    #[test]
    fn late_overlap_dominates_deferred_collision_without_sampling_ordinary_clock() {
        let definition = unique_schema();
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let hydration = job.buffer(1 << 20).unwrap();
        let mut image = unique_row("a", "x", 3);
        for (key, value) in [("b", "y"), ("c", "q")] {
            let other = unique_row(key, value, 3);
            image.work.extend(other.work);
            image.versions.extend(other.versions);
            image
                .unique_probes_loaded
                .extend(other.unique_probes_loaded);
            image.unique_owners.extend(other.unique_owners);
        }
        image.unique_probes_loaded.insert(tuple("z"));
        let operation = intent(rekey("z"));
        let mut request = ordinary();
        request.work_updates.push(UnclaimedUpdate {
            key: "c".into(),
            expect: StateExpect::Version(999),
            change: UnclaimedChange::Delete,
        });
        let plan = plan_broad_group(
            &request,
            &operation,
            ["a", "b", "c"].into_iter().map(|key| {
                Ok(SelectedAdministrativeInput::BorrowedFullImage {
                    key,
                    clause: 0,
                    image: &image,
                })
            }),
            &image,
            || panic!("overlap must precede ordinary clock/CAS"),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .unwrap();
        assert!(matches!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::Invalid(_))
        ));
        assert!(
            plan.proposal().delta.versions.is_empty() && plan.proposal().administration.is_none()
        );
        let selected: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|key| SelectedAdministrativeTarget {
                key: key.into(),
                clause: 0,
            })
            .collect();
        let old = super::super::propose_group_legacy(
            &request,
            &operation,
            &selected,
            &image,
            0,
            &definition,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_eq!(old.delta.receipt, plan.proposal().delta.receipt);
        drop(plan);
        drop(hydration);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn discarded_noop_owner_still_blocks_ordinary_insert() {
        let definition = unique_schema();
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let hydration = job.buffer(1 << 20).unwrap();
        let image = unique_row("a", "x", 1);
        let operation = intent(update());
        let mut request = ordinary();
        request.work_inserts.push(NewWork {
            key: "new".into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([("/payload/u".into(), FieldValue::Text("x".into()))]),
            body: Default::default(),
        });
        let plan = plan_broad_group(
            &request,
            &operation,
            [Ok(SelectedAdministrativeInput::BorrowedFullImage {
                key: "a",
                clause: 0,
                image: &image,
            })],
            &image,
            || Ok(0),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .unwrap();
        assert_eq!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert!(plan.proposal().delta.work.is_empty() && plan.initial_authority().work.is_empty());
        drop(plan);
        drop(hydration);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn contradictory_initial_probe_observations_are_unknown_and_release_all_output() {
        let definition = unique_schema();
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let first_charge = job.buffer(1 << 20).unwrap();
        let second_charge = job.buffer(1 << 20).unwrap();
        let mut first = unique_row("a", "x", 2);
        first.unique_probes_loaded.insert(tuple("z"));
        let mut second = unique_row("b", "z", 2);
        second.unique_probes_loaded.insert(tuple("w"));
        let mut operation = intent(rekey("z"));
        let AdministrativeOperation::SelectFirst(clauses) = &mut operation.operation else {
            unreachable!()
        };
        clauses[0].selector = WorkSelector::leaf(SelectorNode::Keys(BTreeSet::from(["a".into()])));
        clauses.push(AdministrativeClause {
            id: "second".into(),
            selector: WorkSelector::all(vec![]),
            precondition: WorkSelector::all(vec![]),
            action: rekey("w"),
        });
        let request = ordinary();
        let normal = RecordSnapshot {
            next_sequence: 2,
            ..Default::default()
        };
        let source = [
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image: first,
                hydration: first_charge,
            }),
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 1,
                image: second,
                hydration: second_charge,
            }),
        ];
        assert!(matches!(
            plan_broad_group(
                &request,
                &operation,
                source,
                &normal,
                || panic!("contradictory probe reached ordinary planning"),
                &definition,
                &QueueLimits::default(),
                &job
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn ordinary_ownership_clock_is_sampled_only_after_successful_selection_eof() {
        let definition = schema();
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let hydration = job.buffer(1 << 20).unwrap();
        let mut claimed = row();
        claimed.record.lifecycle = Lifecycle::Leased;
        claimed.record.lease_expires_at = Some(10);
        claimed.lease_hash = Some(lease_hash(&[1, 2, 3]));
        let normal = one_row(claimed, "claimed", 2);
        let mut selected = one_row(row(), "selected", 2);
        selected
            .work
            .values_mut()
            .next()
            .unwrap()
            .record
            .insertion_sequence = 2;
        let mut request = ordinary();
        request.claimed_updates.push(ClaimedUpdate {
            key: "claimed".into(),
            expected_version: 1,
            lease: LeaseCredential {
                token: vec![1, 2, 3],
            },
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        });
        let now = Cell::new(0);
        let operation = intent(update());
        let source = std::iter::once_with(|| {
            now.set(20);
            Ok(SelectedAdministrativeInput::BorrowedFullImage {
                key: "selected",
                clause: 0,
                image: &selected,
            })
        });
        let plan = plan_broad_group(
            &request,
            &operation,
            source,
            &normal,
            || Ok(now.get()),
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .unwrap();
        assert_eq!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::StaleLease)
        );
        assert!(
            plan.proposal().delta.versions.is_empty() && plan.proposal().administration.is_none()
        );
        drop(plan);
        drop(hydration);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn sink_drains_5001_accepted_rows_with_constant_retained_driver_memory() {
        use std::sync::Arc;
        for deleting in [false, true] {
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let definition = schema();
            let request = ordinary();
            let action = if deleting {
                AdministrativeAction::Delete {
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                }
            } else {
                AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(17),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: BTreeSet::new(),
                }
            };
            let operation = intent(action);
            let before = RecordSnapshot {
                next_sequence: 5001,
                ..Default::default()
            };
            let template = row();
            let observed = Cell::new(None);
            let delivered = Cell::new(0);
            let source = futures::stream::iter((0..5001).map(|index| {
                let hydration = job.buffer(row_bound(&template).unwrap()).unwrap();
                let image = one_row(template.clone(), &format!("row-{index:05}"), 5001);
                if index > 1 {
                    assert_eq!(observed.get().unwrap(), admission.usage().unwrap().1);
                } else if index == 1 {
                    observed.set(Some(admission.usage().unwrap().1));
                }
                Ok(SelectedAdministrativeInput::OwnedOneRow {
                    clause: 0,
                    image,
                    hydration,
                })
            }));
            let scan = futures::executor::block_on(scan_broad_group_to_sink(
                &request,
                &operation,
                source,
                &before,
                &definition,
                &QueueLimits::default(),
                Arc::clone(&job),
                |packet| {
                    let delta = packet.command().unwrap();
                    assert_eq!(packet.clause(), 0);
                    assert_eq!(delta.versions.len(), 1);
                    assert_eq!(delta.versions[0].version, 2);
                    if deleting {
                        assert_eq!(delta.deleted_work.len(), 1);
                        assert!(delta.work.is_empty());
                    } else {
                        assert_eq!(delta.work.len(), 1);
                        assert_eq!(delta.work[0].record.work.priority, 17);
                        assert!(delta.work[0].record.work.fields.is_empty());
                    }
                    assert_eq!(packet.initial_authority().work.len(), 1);
                    delivered.set(delivered.get() + 1);
                    futures::future::ready(Ok(()))
                },
            ))
            .unwrap();
            assert_eq!(delivered.get(), 5001);
            assert_eq!(scan.result().unwrap().histogram.matched, 5001);
            assert_eq!(scan.result().unwrap().clauses[0].histogram.matched, 5001);
            assert!(scan.result().unwrap().items.is_empty());
            assert!(!scan.overlaps_explicit_work());
            assert!(admission.usage().unwrap().1 < (1 << 16));
            drop(scan);
            assert_eq!(admission.usage().unwrap(), (1, 0));
            drop(job);
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
    }

    #[test]
    fn sink_receives_rejected_authority_after_overlap_and_detects_late_contradiction() {
        use crate::queue_unique_stream::{
            UniqueEvidence, UniqueEvidenceEffect, UniqueTupleReducer,
        };
        use std::sync::Arc;
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition = unique_schema();
        let operation = intent(rekey("z"));
        let mut request = ordinary();
        request.work_inserts.push(row().record.work); // explicit a overlaps first row
        let before = RecordSnapshot {
            next_sequence: 3,
            ..Default::default()
        };
        let target_tuple = tuple("z");
        let foreign = RecordIdentity {
            collection: None,
            key: "foreign".into(),
        };
        let delivered = Cell::new(0);
        let mut reducer = UniqueTupleReducer::new(&target_tuple, true, None, &job).unwrap();
        let source = futures::stream::iter(["a", "b", "c"].into_iter().map(|key| {
            let hydration = job.buffer(64 << 10).unwrap();
            let mut image = unique_row(key, key, 3);
            image.unique_probes_loaded.insert(target_tuple.clone());
            if key == "c" {
                image
                    .unique_owners
                    .insert(target_tuple.clone(), foreign.clone());
            }
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image,
                hydration,
            })
        }));
        let scan = futures::executor::block_on(scan_broad_group_to_sink(
            &request,
            &operation,
            source,
            &before,
            &definition,
            &QueueLimits::default(),
            Arc::clone(&job),
            |packet| {
                delivered.set(delivered.get() + 1);
                let authority = packet.initial_authority();
                if delivered.get() == 3 {
                    assert_eq!(packet.outcome(), &AdministrativeOutcome::UniqueConflict);
                    assert!(
                        packet.command().is_none() && packet.accepted_unique_effects().is_none()
                    );
                    assert_eq!(authority.unique_owners.get(&target_tuple), Some(&foreign));
                } else {
                    assert!(packet.command().is_some());
                }
                futures::future::ready(reducer.observe(UniqueEvidence {
                    tuple: &target_tuple,
                    initial_loaded: authority.unique_probes_loaded.contains(&target_tuple),
                    initial_owner: authority.unique_owners.get(&target_tuple),
                    effect: UniqueEvidenceEffect::Observe,
                }))
            },
        ));
        assert_eq!(delivered.get(), 3);
        assert!(matches!(scan, Err(QueueError::OutcomeUnknown(_))));
        assert!(reducer.finish().is_err());
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }

    #[test]
    fn sink_and_source_failures_or_invalid_late_authority_never_return_scan_summary() {
        use std::sync::Arc;
        for failure in 0..3 {
            let admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let definition = schema();
            let request = ordinary();
            let operation = intent(AdministrativeAction::Update {
                patch: WorkPatch {
                    priority: Some(7),
                    ..Default::default()
                },
                lifecycle: AdministrativeLifecycle::Keep,
                lease_guard: AdministrativeLeaseGuard::RejectActive,
                remove_field_prefixes: BTreeSet::new(),
            });
            let before = RecordSnapshot {
                next_sequence: 2,
                ..Default::default()
            };
            let delivered = Cell::new(0);
            let source =
                futures::stream::iter(["a", "b"].into_iter().enumerate().map(|(index, key)| {
                    if failure == 0 && index == 1 {
                        return Err(QueueError::Busy);
                    }
                    let hydration = job.buffer(64 << 10).unwrap();
                    let mut image = one_row(row(), key, 2);
                    if failure == 2 && index == 1 {
                        image.next_sequence = 3;
                    }
                    Ok(SelectedAdministrativeInput::OwnedOneRow {
                        clause: 0,
                        image,
                        hydration,
                    })
                }));
            let scan = futures::executor::block_on(scan_broad_group_to_sink(
                &request,
                &operation,
                source,
                &before,
                &definition,
                &QueueLimits::default(),
                Arc::clone(&job),
                |_| {
                    delivered.set(delivered.get() + 1);
                    futures::future::ready(if failure == 1 {
                        Err(QueueError::Invalid("sink fault".into()))
                    } else {
                        Ok(())
                    })
                },
            ));
            assert!(matches!(scan, Err(QueueError::OutcomeUnknown(_))));
            assert_eq!(delivered.get(), 1);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn suspended_sink_blocks_source_advance_and_owner_drop_releases_packet() {
        use futures::{FutureExt, task::noop_waker_ref};
        use std::sync::Arc;
        use std::task::{Context, Poll};
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition = schema();
        let request = ordinary();
        let operation = intent(AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(7),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        });
        let before = RecordSnapshot {
            next_sequence: 2,
            ..Default::default()
        };
        let produced = Cell::new(0);
        let source = futures::stream::iter(["a", "b"].into_iter().map(|key| {
            let hydration = job.buffer(64 << 10).unwrap();
            produced.set(produced.get() + 1);
            Ok(SelectedAdministrativeInput::OwnedOneRow {
                clause: 0,
                image: one_row(row(), key, 2),
                hydration,
            })
        }));
        let limits = QueueLimits::default();
        let mut owner = scan_broad_group_to_sink(
            &request,
            &operation,
            source,
            &before,
            &definition,
            &limits,
            Arc::clone(&job),
            |packet| async move {
                futures::future::pending::<()>().await;
                // Keep the guarded packet owned throughout the suspended await.
                assert!(packet.command().is_some());
                Ok(())
            },
        )
        .boxed_local();
        assert!(matches!(
            owner
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending
        ));
        assert_eq!(produced.get(), 1);
        assert!(admission.usage().unwrap().1 > 65536);
        drop(owner);
        assert_eq!(produced.get(), 1);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}

/// Opaque identity of one completed or in-progress selected-source scan.
#[derive(Clone)]
pub struct AdministrativeScanIdentity(pub(super) std::sync::Arc<()>);
impl PartialEq for AdministrativeScanIdentity {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for AdministrativeScanIdentity {}

/// Transient row resolved against immutable fenced authority. Only accepted
/// rows have a command. No-op, precondition or lease-only rejections update
/// histograms without emitting a packet. Emitted packets expose consulted unique authority, including
/// per-target UniqueConflict rejections. A sink must retain those observations
/// separately from accepted effects and later validate complete evidence EOF.
/// Copying an accessor's payload requires separate admission.
pub struct ResolvedAdministrativeRow {
    pub(super) scan: AdministrativeScanIdentity,
    pub(super) ordinal: u64,
    pub(super) outcome: AdministrativeOutcome,
    pub(super) clause: usize,
    pub(super) before: RecordSnapshot,
    pub(super) delta: CommitDelta,
    pub(super) effects: crate::queue_fields::UniqueFieldEffects,
    pub(super) accepted: bool,
    pub(super) _charge: crate::queue_admission::OwnedQueueBufferCharge,
}
impl ResolvedAdministrativeRow {
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn scan_identity(&self) -> AdministrativeScanIdentity {
        self.scan.clone()
    }
    pub fn reservation(&self) -> &QueueReservation {
        self._charge.reservation()
    }
    pub fn outcome(&self) -> &AdministrativeOutcome {
        &self.outcome
    }
    /// First-clause ordinal for SelectFirst; request entry position for Addressed.
    /// This is independent of the consecutive emitted-packet ordinal.
    pub fn clause(&self) -> usize {
        self.clause
    }
    pub fn initial_authority(&self) -> &RecordSnapshot {
        &self.before
    }
    pub fn command(&self) -> Option<&CommitDelta> {
        self.accepted.then_some(&self.delta)
    }
    pub fn accepted_unique_effects(&self) -> Option<&crate::queue_fields::UniqueFieldEffects> {
        self.accepted.then_some(&self.effects)
    }
}
/// Successful selected-source and awaited-sink EOF. This does not certify
/// complete uniqueness reduction, ordinary CAS/lease checks or publication.
/// The backend keeps its fence until all those checks and atomic storage finish.
pub enum AdministrativeScanResult {
    Complete(AdministrativeResult),
    Overflow {
        histogram: AdministrativeHistogram,
        request_fingerprint: [u8; 32],
        max_result_bytes: u64,
    },
}

pub struct AdmittedAdministrativeScan {
    pub(super) scan: AdministrativeScanIdentity,
    pub(super) initial_sequence: u64,
    pub(super) packet_count: u64,
    pub(super) result: AdministrativeScanResult,
    pub(super) overlap: bool,
    pub(super) _charge: crate::queue_admission::OwnedQueueBufferCharge,
}
impl AdmittedAdministrativeScan {
    pub fn packet_count(&self) -> u64 {
        self.packet_count
    }
    pub fn scan_identity(&self) -> AdministrativeScanIdentity {
        self.scan.clone()
    }
    pub fn initial_sequence(&self) -> u64 {
        self.initial_sequence
    }
    pub fn reservation(&self) -> &QueueReservation {
        self._charge.reservation()
    }
    pub fn result(&self) -> Option<&AdministrativeResult> {
        match &self.result {
            AdministrativeScanResult::Complete(result) => Some(result),
            AdministrativeScanResult::Overflow { .. } => None,
        }
    }
    pub fn histogram(&self) -> &AdministrativeHistogram {
        match &self.result {
            AdministrativeScanResult::Complete(result) => &result.histogram,
            AdministrativeScanResult::Overflow { histogram, .. } => histogram,
        }
    }
    pub fn verify_overflow_context(
        &self,
        group: &CommitGroup,
        max_result_bytes: u64,
    ) -> QueueResult<()> {
        if let AdministrativeScanResult::Overflow {
            request_fingerprint,
            max_result_bytes: original_budget,
            ..
        } = &self.result
        {
            let current = fingerprint("administrative-result-retention", group)?;
            if current != *request_fingerprint || max_result_bytes != *original_budget {
                return Err(unknown(
                    "overflow proof belongs to a different request or result budget",
                ));
            }
        }
        Ok(())
    }
    pub fn result_overflowed(&self) -> bool {
        matches!(self.result, AdministrativeScanResult::Overflow { .. })
    }
    pub fn overlaps_explicit_work(&self) -> bool {
        self.overlap
    }
}

/// Resolve and await one owned row at a time. The scan retains only an admitted
/// previous key, input-sized explicit identities and clause histograms. A sink
/// must spill rather than collect row payloads; it owns command/evidence storage.
/// Continue full row validation after provisional overlap/collision/overflow.
/// Any source, resolution, admission or sink failure returns OutcomeUnknown and
/// grants no scan summary. No ordinary ownership clock is sampled here.
#[allow(clippy::too_many_arguments)]
pub async fn scan_broad_group_to_sink<'s, 'r, F, Fut>(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    targets: impl futures::Stream<Item = QueueResult<SelectedAdministrativeInput<'s, 'r>>>,
    ordinary_image: &RecordSnapshot,
    schema: &QueueSchema,
    limits: &QueueLimits,
    reservation: std::sync::Arc<QueueReservation>,
    mut sink: F,
) -> QueueResult<AdmittedAdministrativeScan>
where
    F: FnMut(ResolvedAdministrativeRow) -> Fut,
    Fut: std::future::Future<Output = QueueResult<()>>,
{
    use futures::StreamExt;
    let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
        return Err(QueueError::Invalid(
            "sink scan requires selector administration".into(),
        ));
    };
    intent.validate(schema, limits)?;
    validate_snapshot(ordinary_image)?;
    let root_bytes = add(
        intent_headers(intent)?,
        add(
            ordinary_bound(ordinary, ordinary_image)?,
            add(
                scale(wire(intent)?, 12)?,
                add(scale(wire(schema)?, 12)?, 8192)?,
            )?,
        )?,
    )?;
    let root = reservation.buffer(add(root_bytes, scale(clauses.len() as u64, 8192)?)?)?;
    let scan = AdministrativeScanIdentity(std::sync::Arc::new(()));
    let mut packet_count = 0u64;
    let mut result = AdministrativeResult {
        histogram: Default::default(),
        items: vec![],
        clauses: clauses
            .iter()
            .map(|clause| AdministrativeClauseResult {
                id: clause.id.clone(),
                histogram: Default::default(),
            })
            .collect(),
    };
    let explicit: BTreeSet<&str> = work_keys(ordinary).collect();
    let mut last: Option<(String, QueueBufferCharge<'_>)> = None;
    let mut overlap = false;
    futures::pin_mut!(targets);
    while let Some(input) = targets.next().await {
        let input = input.map_err(|_| unknown("administrative source failed before EOF"))?;
        let (key, clause, image) = match &input {
            SelectedAdministrativeInput::BorrowedFullImage { key, clause, image } => {
                (*key, *clause, *image)
            }
            SelectedAdministrativeInput::OwnedOneRow { clause, image, .. } => {
                if image.work.len() != 1 || image.versions.len() != 1 || !image.state.is_empty() {
                    return Err(unknown(
                        "streamed administrative authority is not one live row",
                    ));
                }
                (
                    image.work.keys().next().expect("one row").as_str(),
                    *clause,
                    image,
                )
            }
        };
        let _key_scratch = reservation.buffer(add(scale(key.len() as u64, 4)?, 2048)?)?;
        if last
            .as_ref()
            .is_some_and(|(previous, _)| previous.as_bytes() >= key.as_bytes())
        {
            return Err(unknown(
                "administrative stream keys are not strictly ordered",
            ));
        }
        let last_charge = reservation.buffer(add(key.len() as u64, 256)?)?;
        last = Some((key.into(), last_charge));
        let row = selected_row(key, image, ordinary_image.next_sequence)?;
        let definition = selected_clause(row, clause, intent, schema)?;
        overlap |= explicit.contains(key);
        let _planning = reservation.buffer(row_bound(row)?)?;
        let mut proposal = propose_target_view(
            TargetRequest {
                key,
                expected_version: None,
                precondition: &definition.precondition,
                action: &definition.action,
            },
            Some(row),
            Some(row.record.version),
            intent.evaluated_at,
            None,
            schema,
            limits,
        )?;
        if let Some(candidate) =
            resolve_candidate(key, row, image, &mut proposal, schema, &reservation)?
        {
            let owned = ResolvedAdministrativeRow {
                scan: scan.clone(),
                ordinal: packet_count,
                outcome: proposal.outcome.clone(),
                clause,
                before: candidate.before,
                delta: candidate.delta,
                effects: candidate.effects,
                accepted: candidate.accepted,
                _charge: candidate
                    ._charge
                    .into_owned(std::sync::Arc::clone(&reservation))?,
            };
            sink(owned)
                .await
                .map_err(|_| unknown("administrative sink failed before EOF"))?;
            packet_count = packet_count
                .checked_add(1)
                .ok_or_else(|| unknown("administrative packet count exhausted"))?;
        }
        result.histogram.record(&proposal.outcome)?;
        result.clauses[clause].histogram.record(&proposal.outcome)?;
    }
    Ok(AdmittedAdministrativeScan {
        scan,
        initial_sequence: ordinary_image.next_sequence,
        packet_count,
        result: AdministrativeScanResult::Complete(result),
        overlap,
        _charge: root.into_owned(std::sync::Arc::clone(&reservation))?,
    })
}
