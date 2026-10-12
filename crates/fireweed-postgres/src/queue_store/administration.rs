//! Native fenced administration shared by public commit and fresh preview.
use super::*;
use fireweed_core::queue_administration::{Comparison, SelectorNode, WorkSelector};

struct BytePool {
    values: Vec<Vec<u8>>,
}
impl FieldSqlParameters for BytePool {
    fn bytes(&mut self, value: Vec<u8>) -> String {
        self.values.push(value);
        format!("($2::bytea[])[{}]", self.values.len())
    }
    fn byte_list(&mut self, values: Vec<Vec<u8>>) -> String {
        if values.is_empty() {
            return "ARRAY[]::bytea[]".into();
        }
        let first = self.values.len() + 1;
        self.values.extend(values);
        format!("($2::bytea[])[{first}:{}]", self.values.len())
    }
    fn integer(&mut self, value: i32) -> String {
        value.to_string()
    }
}
fn comparison(value: Comparison) -> &'static str {
    match value {
        Comparison::Equal => "=",
        Comparison::Before => "<",
        Comparison::BeforeOrEqual => "<=",
        Comparison::After => ">",
        Comparison::AfterOrEqual => ">=",
    }
}
fn lifecycle_set(expression: &str, values: &[Lifecycle]) -> String {
    if values.is_empty() {
        return "FALSE".into();
    }
    let values = values
        .iter()
        .map(|state| life(*state).to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("({expression} IN ({values}))")
}
fn selector_sql(
    selector: &WorkSelector,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<String> {
    selector.validate(definition)?;
    let active = "(r.lifecycle=1 AND COALESCE(r.lease_until>$3::bigint,FALSE))";
    fireweed_engine::queue_sql::administrative_selector_sql(selector, |node| {
        Ok(match node {
            SelectorNode::Field { name, predicate } => {
                let (table, identity) = field_identity_sql(name, definition, pool)?;
                let predicate = field_predicate_sql(predicate, pool)?;
                format!(
                    "EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND {identity}{predicate})"
                )
            }
            SelectorNode::Present { name, present } => {
                let (table, identity) = field_identity_sql(name, definition, pool)?;
                format!(
                    "{}EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND {identity})",
                    if *present { "" } else { "NOT " }
                )
            }
            SelectorNode::Keys(keys) => {
                let values: Vec<_> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
                let hashes = values.iter().map(|key| probe_hash(key)).collect();
                let hp = pool.byte_list(hashes);
                let kp = pool.byte_list(values);
                format!("(r.key_hash=ANY({hp}) AND r.key=ANY({kp}))")
            }
            SelectorNode::StoredLifecycleIn(states) => lifecycle_set("r.lifecycle", states),
            SelectorNode::EffectiveLifecycleIn(states) => lifecycle_set(
                &format!("CASE WHEN r.lifecycle=1 AND NOT {active} THEN 0 ELSE r.lifecycle END"),
                states,
            ),
            SelectorNode::Attempts {
                comparison: op,
                value,
            } => format!("(r.attempts{}{}::bigint)", comparison(*op), value),
            SelectorNode::AvailableAt {
                comparison: op,
                value,
            } => format!("(r.available_at{}({})::bigint)", comparison(*op), value),
            SelectorNode::LeaseActive(expected) => {
                if *expected {
                    active.into()
                } else {
                    format!("(NOT {active})")
                }
            }
            SelectorNode::All(_) | SelectorNode::Any(_) | SelectorNode::Not => {
                return Err(storage("boolean selector reached leaf emitter"));
            }
        })
    })
}

struct NativeSelection<'r> {
    sql: String,
    params: Parameters,
    _charge: QueueBufferCharge<'r>,
}
/// Private preparation is physically qualified before adding public control
/// input, durable results and preview in one coherent activation.
fn prepare_selection<'r>(
    queue: &QueueRow<'_>,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<NativeSelection<'r>> {
    use fireweed_core::queue_administration::AdministrativeOperation;
    intent.validate(&queue.schema, limits)?;
    let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
        return Err(QueueError::Invalid(
            "native broad selection requires clauses".into(),
        ));
    };
    // Covers balanced expression copies (bounded selector depth), parameter
    // arrays, Strings/Vec/BTree headers and final query composition BEFORE build.
    let nodes = clauses.iter().try_fold(0u64, |total, clause| {
        total
            .checked_add(clause.selector.nodes.len() as u64)
            .ok_or(QueueError::TooLarge)
    })?;
    let budget = encoded_len(intent)?
        .checked_mul(256)
        .and_then(|bytes| bytes.checked_add(nodes.checked_mul(16384)?))
        .and_then(|bytes| bytes.checked_add(16384))
        .ok_or(QueueError::TooLarge)?;
    let charge = reservation.buffer(budget)?;
    let mut pool = BytePool { values: Vec::new() };
    let owner = if clauses.is_empty() {
        "NULL::integer".into()
    } else {
        let mut sql = "CASE".to_string();
        for (ordinal, clause) in clauses.iter().enumerate() {
            let selector = selector_sql(&clause.selector, &queue.schema.work, &mut pool)?;
            sql.push_str(&format!(" WHEN {selector} THEN {ordinal}::integer"));
        }
        sql.push_str(" ELSE NULL::integer END");
        sql
    };
    let sql = format!(
        "SELECT id,key_size,owner FROM (SELECT r.id,r.key,octet_length(r.key) AS key_size,{owner} AS owner FROM fireweed_api007.records r WHERE r.queue_id=$1 AND r.collection=0 AND r.active AND $2::bytea[] IS NOT NULL AND $3::bigint IS NOT NULL) selected WHERE owner IS NOT NULL ORDER BY key,id"
    );
    let mut params = Vec::new();
    parameter(&mut params, queue.id);
    parameter(&mut params, pool.values);
    parameter(&mut params, intent.evaluated_at);
    Ok(NativeSelection {
        sql,
        params,
        _charge: charge,
    })
}

#[cfg(test)]
type RowHook = (i64, Box<dyn Fn(&str)>);
#[cfg(test)]
thread_local! {
    static ROW_BOUNDARY: std::cell::RefCell<Option<RowHook>> = const { std::cell::RefCell::new(None) };
    static SQL_FAILURE_AFTER: std::cell::Cell<Option<(i64,u64)>> = const { std::cell::Cell::new(None) };
}
/// The transaction's queue-row lock is retained by the caller until publication
/// or rollback. The exhausted server cursor is selection-completeness authority.
/// Any error or cancellation of this owner future requires rollback of the
/// entire containing transaction. Callers must not continue sibling groups in
/// that transaction, even when a non-SQL error left it otherwise usable.
/// One fenced native source shared by reference and physical sink drivers.
/// Successful EOF closes its cursor. Any failure/drop requires outer rollback.
async fn native_broad_source<'s, 'r: 's>(
    tx: &'s Transaction<'_>,
    queue: &'s QueueRow<'_>,
    intent: &'s fireweed_core::queue_administration::AdministrativeIntent,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<
    impl futures::Stream<
        Item = QueueResult<
            fireweed_engine::queue_administration::SelectedAdministrativeInput<'s, 'r>,
        >,
    > + 's,
> {
    use fireweed_core::queue_administration::AdministrativeOperation;
    let prepared = prepare_selection(queue, intent, limits, reservation)?;
    // Fixed metadata charge covers one FETCH result and command/driver scratch;
    // variable key and hydration bytes are charged after lengths, before data.
    let cursor_charge = reservation.buffer(65536)?;
    tx.execute(
        &format!(
            "DECLARE fw_api007_admin NO SCROLL CURSOR FOR {}",
            prepared.sql
        ),
        &refs(&prepared.params),
    )
    .await
    .map_err(storage)?;
    let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
        unreachable!("validated broad selection")
    };
    let source = futures::stream::try_unfold((prepared, cursor_charge), move |state| async move {
        let rows = tx
            .query("FETCH FORWARD 1 FROM fw_api007_admin", &[])
            .await
            .map_err(storage)?;
        if rows.is_empty() {
            tx.batch_execute("CLOSE fw_api007_admin")
                .await
                .map_err(storage)?;
            return Ok(None);
        }
        if rows.len() != 1 {
            return Err(storage("native administrative fetch is not one row"));
        }
        let metadata = &rows[0];
        let id: i64 = metadata.get(0);
        let key_bytes = u64::try_from(metadata.get::<_, i32>(1)).map_err(storage)?;
        let clause = usize::try_from(metadata.get::<_, i32>(2)).map_err(storage)?;
        let action = &clauses
            .get(clause)
            .ok_or_else(|| storage("invalid native clause ordinal"))?
            .action;
        let hydration = reservation.buffer(
            key_bytes
                .checked_mul(32)
                .and_then(|bytes| bytes.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let row = tx.query_one("SELECT key,version::text FROM fireweed_api007.records WHERE id=$1 AND queue_id=$2 AND collection=0 AND active", &[&id,&queue.id]).await.map_err(storage)?;
        let record_key = key(row.get(0))?;
        if record_key.len() as u64 != key_bytes {
            return Err(storage("native administrative key length changed"));
        }
        let highwater = version(row.get(1))?;
        let loaded = hydrate_administrative_row(
            tx,
            queue,
            &record_key,
            action,
            intent.evaluated_at,
            false,
            reservation,
        )
        .await?;
        if loaded.snapshot.work[&record_key].record.version != highwater
            || loaded.ids.get(&RecordIdentity {
                collection: None,
                key: record_key.clone(),
            }) != Some(&id)
        {
            return Err(storage("native selected physical identity changed"));
        }
        let Loaded {
            _charge,
            _selection_charge,
            _probe_charges,
            snapshot,
            ids,
        } = loaded;
        drop(ids);
        let mut hydration = hydration.merge(_charge)?;
        if let Some(charge) = _selection_charge {
            hydration = hydration.merge(charge)?;
        }
        for charge in _probe_charges {
            hydration = hydration.merge(charge)?;
        }
        let input =
            fireweed_engine::queue_administration::SelectedAdministrativeInput::OwnedOneRow {
                clause,
                image: snapshot,
                hydration,
            };
        #[cfg(test)]
        ROW_BOUNDARY.with(|hook| {
            if let Some((id, callback)) = &*hook.borrow()
                && *id == queue.id
            {
                callback(&record_key);
            }
        });
        #[cfg(test)]
        if SQL_FAILURE_AFTER.with(|failure| {
            if let Some((id, remaining)) = failure.get()
                && id == queue.id
            {
                if remaining == 1 {
                    failure.set(None);
                    true
                } else {
                    failure.set(Some((id, remaining - 1)));
                    false
                }
            } else {
                false
            }
        }) {
            tx.query_one("SELECT 1 / 0", &[]).await.map_err(storage)?;
        }
        Ok(Some((input, state)))
    });
    Ok(source)
}

/// Shared exact-key hydration and prospective authority for broad and addressed
/// controls. Incoming action/key clones are admitted independently of SQL scan
/// preparation. Absence/tombstone is valid only for an addressed point lookup.
#[allow(clippy::too_many_arguments)]
async fn hydrate_administrative_row<'r>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    record_key: &str,
    action: &fireweed_core::queue_administration::AdministrativeAction,
    evaluated_at: i64,
    allow_absent: bool,
    reservation: &'r QueueReservation,
) -> QueueResult<Loaded<'r>> {
    let input_bytes = encoded_len(action)?
        .checked_mul(12)
        .and_then(|n| n.checked_add((record_key.len() as u64).checked_mul(32)?))
        .and_then(|n| n.checked_add(65536))
        .ok_or_else(|| storage("administrative hydration input bound"))?;
    let input_charge = reservation.buffer(input_bytes)?;
    let mut group =
        fireweed_engine::queue_administration::administrative_hydration_group(record_key, action);
    // Full field authority for ONE server-selected row; opaque bodies are
    // loaded only for an explicit comparison. Selective field hydration is
    // a later performance gate, not a whole-queue/client filtering fallback.
    let mut loaded = load_group_with_fields(tx, queue, &group, reservation, None).await?;
    if loaded.snapshot.work.is_empty() {
        if !allow_absent {
            return Err(storage("native selected row disappeared"));
        }
        loaded._probe_charges.push(input_charge);
        return Ok(loaded);
    }
    if loaded.snapshot.work.len() != 1 || loaded.snapshot.versions.len() != 1 {
        return Err(storage("native hydration is not one row"));
    }
    fireweed_engine::queue_administration::resolve_administrative_hydration(
        &mut group,
        action,
        &loaded.snapshot,
        &queue.schema,
    )?;
    if matches!(
        action,
        fireweed_core::queue_administration::AdministrativeAction::Update { .. }
    ) {
        // The shared probe planner clones before/after identities across
        // indexes. Admit that transient amplification before generating any
        // tuples; retained owner coverage is charged by the native loader.
        let _planning = reservation.buffer(
            group_expansion_budget(&group, &loaded.snapshot, &queue.schema)?
                .checked_mul(8)
                .ok_or(QueueError::TooLarge)?,
        )?;
        load_unique_probes(tx, queue, &mut loaded, &group, evaluated_at, reservation).await?;
    } else {
        // Deletion may invalidate an active lease, so tuple discovery must
        // not reuse ordinary delete's lease rejection. The common complete
        // record codec supplies exactly the existing eligible unique tuples.
        let tuple_charge = reservation.buffer(
            group_expansion_budget(&group, &loaded.snapshot, &queue.schema)?
                .checked_mul(8)
                .ok_or(QueueError::TooLarge)?,
        )?;
        loaded._probe_charges.push(tuple_charge);
        let target = &loaded.snapshot.work[record_key];
        let probes = insert_unique_probes(
            std::slice::from_ref(&target.record.work),
            &loaded.snapshot,
            &queue.schema,
        )?
        .into_iter()
        .collect();
        load_unique_authority(tx, queue, &mut loaded, probes, reservation).await?;
    }
    loaded._probe_charges.push(input_charge);
    Ok(loaded)
}

fn native_addressed_source<'s, 'r>(
    tx: &'s Transaction<'_>,
    queue: &'s QueueRow<'_>,
    intent: &'s fireweed_core::queue_administration::AdministrativeIntent,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<
    impl futures::Stream<
        Item = QueueResult<
            fireweed_engine::queue_administration::AddressedAdministrativeInput<'s, 'r>,
        >,
    > + 's,
>
where
    'r: 's,
{
    use fireweed_core::queue_administration::AdministrativeOperation;
    intent.validate(&queue.schema, limits)?;
    let AdministrativeOperation::Addressed(entries) = &intent.operation else {
        return Err(storage("addressed source lacks addressed intent"));
    };
    Ok(
        fireweed_engine::queue_administration::addressed_administrative_source(
            entries,
            move |position, entry| async move {
                let loaded = hydrate_administrative_row(
                    tx,
                    queue,
                    &entry.key,
                    &entry.action,
                    intent.evaluated_at,
                    true,
                    reservation,
                )
                .await?;
                let Loaded {
                    snapshot,
                    ids,
                    _charge,
                    _selection_charge,
                    _probe_charges,
                } = loaded;
                drop(ids);
                let mut hydration = _charge;
                if let Some(charge) = _selection_charge {
                    hydration = hydration.merge(charge)?;
                }
                for charge in _probe_charges {
                    hydration = hydration.merge(charge)?;
                }
                let input =
                fireweed_engine::queue_administration::AddressedAdministrativeInput::OwnedOneRow {
                    position,
                    key: entry.key.clone(),
                    image: snapshot,
                    hydration,
                };
                Ok(input)
            },
        ),
    )
}
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn plan_native_broad<'r>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    ordinary: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<fireweed_engine::queue_administration::AdmittedAdministrativeGroupProposal<'r>> {
    // Prospective ordinary tuples are independent of lease/CAS eligibility.
    // Eligibility is decided once, with the fresh clock after successful EOF.
    let mut ordinary_loaded = load_group(tx, queue, ordinary, reservation).await?;
    load_prospective_unique_probes(tx, queue, &mut ordinary_loaded, ordinary, reservation).await?;
    let source = native_broad_source(tx, queue, intent, limits, reservation).await?;
    fireweed_engine::queue_administration::plan_broad_group_stream(
        ordinary,
        intent,
        source,
        &ordinary_loaded.snapshot,
        ownership_now,
        &queue.schema,
        limits,
        reservation,
    )
    .await
}

struct NativeAdministrativePreparation<'r> {
    scratch: fireweed_queue_spool::SealedAdministrativeScratch,
    summary: fireweed_engine::queue_administration::AdmittedAdministrativeScan,
    ordinary: Loaded<'r>,
    request_fingerprint: [u8; 32],
    max_result_bytes: u64,
    max_publication_bytes: u64,
}
/// Preparation retains explicit ordinary authority and returns only bounded
/// scratch plus compact results. No ordinary ownership clock or SQL write occurs.
/// Any source/sink error requires whole outer transaction rollback.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn prepare_native_scratch<'r>(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    queue: &QueueRow<'_>,
    ordinary: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    limits: &QueueLimits,
    reservation: &'r Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
    directory: &std::path::Path,
    quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
) -> QueueResult<NativeAdministrativePreparation<'r>> {
    prepare_native_scratch_bounded(
        tx,
        address,
        queue,
        ordinary,
        intent,
        limits,
        u64::MAX,
        u64::MAX,
        reservation,
        workers,
        directory,
        quota,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
async fn prepare_native_scratch_bounded<'r>(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    queue: &QueueRow<'_>,
    ordinary: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    limits: &QueueLimits,
    max_result_bytes: u64,
    max_publication_bytes: u64,
    reservation: &'r Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
    directory: &std::path::Path,
    quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
) -> QueueResult<NativeAdministrativePreparation<'r>> {
    use fireweed_engine::queue_publication::{
        measure_administrative_publication_fallbacks, measure_administrative_receipt_fallbacks,
        measure_administrative_success_floor,
    };
    if measure_administrative_receipt_fallbacks(&ordinary.request_id)
        .map_err(|_| storage("result fallback measurement failed"))?
        > max_result_bytes
        || measure_administrative_publication_fallbacks(
            address,
            queue.definition_pin,
            &ordinary.request_id,
        )
        .map_err(|_| storage("publication fallback measurement failed"))?
            > max_publication_bytes
    {
        return Err(QueueError::Invalid(
            "administrative limits cannot fit zero-effect fallbacks".into(),
        ));
    }
    let request_fingerprint =
        ordinary.administrative_fingerprint(address, intent, max_result_bytes)?;
    let floor = measure_administrative_success_floor(address, queue.definition_pin, ordinary)
        .map_err(|_| storage("publication floor measurement failed"))?;
    let command_allowance = max_publication_bytes.saturating_sub(floor);
    let mut ordinary_loaded = load_group(tx, queue, ordinary, reservation).await?;
    load_prospective_unique_probes(tx, queue, &mut ordinary_loaded, ordinary, reservation).await?;
    let sink = fireweed_queue_spool::OwnedAdministrativeSink::new_with_command_limit(
        workers.clone(),
        directory,
        quota,
        reservation,
        address,
        queue.definition_pin,
        queue.next_sequence,
        command_allowance,
    )
    .await?;
    let summary = match &intent.operation {
        fireweed_core::queue_administration::AdministrativeOperation::SelectFirst(_) => {
            let source = native_broad_source(tx, queue, intent, limits, reservation).await?;
            let delivery = sink.clone();
            fireweed_engine::queue_administration::scan_broad_group_to_sink(
                ordinary,
                intent,
                source,
                &ordinary_loaded.snapshot,
                &queue.schema,
                limits,
                Arc::clone(reservation),
                move |packet| {
                    let sink = delivery.clone();
                    async move { sink.append(packet).await }
                },
            )
            .await?
        }
        fireweed_core::queue_administration::AdministrativeOperation::Addressed(_) => {
            let source = native_addressed_source(tx, queue, intent, limits, reservation)?;
            let delivery = sink.clone();
            fireweed_engine::queue_administration::scan_addressed_group_to_sink_bounded(
                ordinary,
                intent,
                source,
                &ordinary_loaded.snapshot,
                &queue.schema,
                limits,
                Arc::clone(reservation),
                max_result_bytes,
                move |packet| {
                    let sink = delivery.clone();
                    async move { sink.append(packet).await }
                },
            )
            .await?
        }
    };
    let (scratch, summary) = sink.seal(summary).await?;
    Ok(NativeAdministrativePreparation {
        scratch,
        summary,
        ordinary: ordinary_loaded,
        request_fingerprint,
        max_result_bytes,
        max_publication_bytes,
    })
}

/// Input-bounded ordinary command plus external administrative commands, still
/// awaiting byte/result limits and atomic application. No publication approval.
struct NativeProvisionalGroup {
    scratch: fireweed_queue_spool::SealedAdministrativeScratch,
    summary: fireweed_engine::queue_administration::AdmittedAdministrativeScan,
    ordinary: fireweed_engine::queue_administration::ProvisionalOrdinaryCandidate,
    initial: OwnedLoaded,
    request_fingerprint: [u8; 32],
    max_result_bytes: u64,
    max_publication_bytes: u64,
}
use fireweed_engine::queue_publication::AdministrativePublicationDecision as NativePublicationDecision;
impl NativeProvisionalGroup {
    fn assess_publication_limit(
        &self,
        address: &QueueAddress,
        queue: &QueueRow<'_>,
        group: &CommitGroup,
        intent: &fireweed_core::queue_administration::AdministrativeIntent,
        reservation: &Arc<QueueReservation>,
    ) -> QueueResult<NativePublicationDecision> {
        use fireweed_engine::queue_publication::*;
        self.scratch.validate_publication_context(
            address,
            queue.definition_pin,
            queue.next_sequence,
            reservation,
        )?;
        assess_administrative_publication(
            address,
            queue.definition_pin,
            group,
            intent,
            &self.ordinary,
            &self.summary,
            AdministrativeCommandMeasurement {
                command_limit: self.scratch.command_limit(),
                command_bytes: self.scratch.command_bytes(),
                command_count: self.scratch.command_count(),
                overflowed: self.scratch.publication_overflowed(),
            },
            self.request_fingerprint,
            self.max_result_bytes,
            self.max_publication_bytes,
            reservation,
        )
    }

    #[cfg(test)]
    fn assess_result_limit(
        &self,
        group: &CommitGroup,
        max_result_bytes: u64,
    ) -> QueueResult<fireweed_engine::queue_publication::AdministrativeResultLimit> {
        let GroupOutcome::Committed { versions } = &self.ordinary.delta().receipt.outcome else {
            return Err(storage("provisional ordinary result is not committed"));
        };
        fireweed_engine::queue_publication::assess_administrative_result_limit(
            group,
            &self.summary,
            versions,
            max_result_bytes,
        )
        .map_err(|_| storage("exact administrative result measurement failed"))
    }
}
#[allow(clippy::too_many_arguments)]
async fn finalize_native_semantics(
    prepared: NativeAdministrativePreparation<'_>,
    address: &QueueAddress,
    group: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    queue: &QueueRow<'_>,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    reservation: &Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
    directory: &std::path::Path,
    quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
) -> QueueResult<Result<NativeProvisionalGroup, Rejection>> {
    use fireweed_engine::queue_administration::validate_ordinary_prospective_authority;
    let NativeAdministrativePreparation {
        mut scratch,
        summary,
        ordinary,
        request_fingerprint,
        max_result_bytes,
        max_publication_bytes,
    } = prepared;
    if group.administrative_fingerprint(address, intent, max_result_bytes)? != request_fingerprint {
        return Err(storage("native preparation belongs to a different request"));
    }
    scratch.verify_context(address, queue.definition_pin, queue.next_sequence)?;
    validate_ordinary_prospective_authority(group, &ordinary.snapshot, &queue.schema, reservation)?;
    let ordinary = ordinary.into_owned(reservation)?;
    let finalized = fireweed_queue_spool::finalize_administrative_semantics(
        scratch,
        summary,
        ordinary,
        group,
        &queue.schema,
        ownership_now,
        reservation,
        workers,
        directory,
        quota,
    )
    .await?;
    let finalized = match finalized {
        Ok(value) => value,
        Err(reason) => return Ok(Err(reason)),
    };
    Ok(Ok(NativeProvisionalGroup {
        scratch: finalized.scratch,
        summary: finalized.summary,
        ordinary: finalized.ordinary,
        initial: finalized.initial,
        request_fingerprint,
        max_result_bytes,
        max_publication_bytes,
    }))
}

struct OwnedNativePublicationRequest {
    address: QueueAddress,
    group: CommitGroup,
    intent: fireweed_core::queue_administration::AdministrativeIntent,
    pin: fireweed_engine::queue_schema_codec::DefinitionPin,
    _charge: fireweed_engine::queue_admission::OwnedQueueBufferCharge,
}
struct NativePreparedPublication {
    provisional: NativeProvisionalGroup,
    frame: fireweed_engine::queue_publication::AdministrativePublicationFrame,
}
/// None is a fully validated logical TooLarge decision, never a partial frame.
#[allow(clippy::too_many_arguments)]
async fn prepare_native_publication(
    provisional: NativeProvisionalGroup,
    address: &QueueAddress,
    queue: &QueueRow<'_>,
    group: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    reservation: &Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
) -> QueueResult<Option<NativePreparedPublication>> {
    let NativePublicationDecision::Fits {
        bytes: expected_bytes,
        commands,
    } = provisional.assess_publication_limit(address, queue, group, intent, reservation)?
    else {
        return Ok(None);
    };
    let bytes =
        fireweed_engine::queue_memory::publication_request_copy_bound(address, group, intent)?;
    let charge = reservation.owned_buffer(bytes)?;
    let request = OwnedNativePublicationRequest {
        address: address.clone(),
        group: group.clone(),
        intent: intent.clone(),
        pin: queue.definition_pin,
        _charge: charge,
    };
    let job = Arc::clone(reservation);
    workers
        .run(move || {
            let mut provisional = provisional;
            let administration = provisional
                .summary
                .result()
                .ok_or_else(|| storage("publication lacks complete administration"))?;
            let input = fireweed_queue_spool::ResolvedAdministrativePublicationInput {
                queue: &request.address,
                pin: request.pin,
                group: &request.group,
                intent: &request.intent,
                before: provisional.ordinary.initial_authority(),
                ordinary: provisional.ordinary.delta(),
                administration,
                max_result_bytes: provisional.max_result_bytes,
                max_publication_bytes: provisional.max_publication_bytes,
                expected_bytes,
                expected_commands: commands,
            };
            let frame = provisional
                .scratch
                .prepare_resolved_publication(&input, &job)?;
            drop(request);
            Ok(Some(NativePreparedPublication { provisional, frame }))
        })
        .await
}

pub(super) struct AdmittedAdministrativeReceipt {
    pub(super) receipt: CommitReceipt,
    pub(super) next_sequence: Option<u64>,
    _charge: fireweed_engine::queue_admission::OwnedQueueBufferCharge,
}
impl AdmittedAdministrativeReceipt {
    pub(super) fn into_parts(
        self,
    ) -> (
        CommitReceipt,
        Option<u64>,
        fireweed_engine::queue_admission::OwnedQueueBufferCharge,
    ) {
        (self.receipt, self.next_sequence, self._charge)
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_administrative_group(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    queue: &QueueRow<'_>,
    group: &CommitGroup,
    limits: &QueueLimits,
    clock: &dyn Clock,
    reservation: &Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
    quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
    directory: &std::path::Path,
    apply: bool,
) -> QueueResult<AdmittedAdministrativeReceipt> {
    use fireweed_engine::queue_publication::{
        AdministrativeReceiptOutcome, measure_complete_administrative_receipt,
    };
    let admin = group
        .administration
        .as_ref()
        .ok_or_else(|| storage("administrative job lacks its proposal"))?;
    let prepared = prepare_native_scratch_bounded(
        tx,
        address,
        queue,
        group,
        &admin.intent,
        limits,
        admin.max_result_bytes,
        limits.max_group_bytes,
        reservation,
        workers,
        directory,
        Arc::clone(&quota),
    )
    .await?;
    let finalized = finalize_native_semantics(
        prepared,
        address,
        group,
        &admin.intent,
        queue,
        || timestamp_nanos(clock.now()),
        reservation,
        workers,
        directory,
        quota,
    )
    .await?;
    let finalized = match finalized {
        Ok(finalized) => finalized,
        Err(reason) => {
            let size = measure_complete_administrative_receipt(
                &group.request_id,
                &AdministrativeReceiptOutcome::Rejected(&reason),
            )
            .map_err(|_| storage("administrative rejection measurement failed"))?;
            let charge = reservation.owned_buffer(
                size.checked_mul(4)
                    .and_then(|n| n.checked_add(1024))
                    .ok_or_else(|| storage("receipt admission overflow"))?,
            )?;
            return Ok(AdmittedAdministrativeReceipt {
                receipt: CommitReceipt {
                    request_id: group.request_id.clone(),
                    outcome: GroupOutcome::Rejected(reason),
                },
                next_sequence: None,
                _charge: charge,
            });
        }
    };
    let prepared = prepare_native_publication(
        finalized,
        address,
        queue,
        group,
        &admin.intent,
        reservation,
        workers,
    )
    .await?;
    let Some(NativePreparedPublication { provisional, frame }) = prepared else {
        let size = measure_complete_administrative_receipt(
            &group.request_id,
            &AdministrativeReceiptOutcome::TooLarge,
        )
        .map_err(|_| storage("TooLarge receipt measurement failed"))?;
        let charge = reservation.owned_buffer(
            size.checked_mul(4)
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(|| storage("receipt admission overflow"))?,
        )?;
        return Ok(AdmittedAdministrativeReceipt {
            receipt: CommitReceipt {
                request_id: group.request_id.clone(),
                outcome: GroupOutcome::Rejected(Rejection::TooLarge),
            },
            next_sequence: None,
            _charge: charge,
        });
    };
    let NativeProvisionalGroup {
        scratch,
        summary,
        ordinary,
        initial,
        ..
    } = provisional;
    let GroupOutcome::Committed { versions } = &ordinary.delta().receipt.outcome else {
        return Err(storage("ordinary result is not committed"));
    };
    let result = summary
        .result()
        .ok_or_else(|| storage("complete receipt lacks complete result"))?;
    let size = measure_complete_administrative_receipt(
        &group.request_id,
        &AdministrativeReceiptOutcome::Committed {
            versions,
            administration: result,
        },
    )
    .map_err(|_| storage("complete receipt measurement failed"))?;
    let headers = fireweed_engine::queue_memory::administrative_result_headers(result)?
        .checked_add(
            (versions.len() as u64)
                .checked_mul(256)
                .ok_or(QueueError::TooLarge)?,
        )
        .ok_or(QueueError::TooLarge)?;
    let charge = reservation.owned_buffer(
        size.checked_mul(4)
            .and_then(|n| headers.checked_mul(4).and_then(|h| n.checked_add(h)))
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(|| storage("receipt admission overflow"))?,
    )?;
    let receipt = CommitReceipt {
        request_id: group.request_id.clone(),
        outcome: GroupOutcome::AdministrativeCommitted {
            versions: versions.clone(),
            administration: result.clone(),
        },
    };
    let next_sequence = ordinary.delta().next_sequence;
    if apply {
        let scratch = apply_resolved_administrative_commands(
            tx,
            address,
            queue,
            scratch,
            reservation,
            workers,
        )
        .await?;
        drop(scratch);
        let mut ids = load_delta_ids(
            tx,
            queue,
            ordinary.delta(),
            ordinary.initial_authority(),
            reservation,
        )
        .await?;
        persist_delta(
            tx,
            queue,
            ordinary.delta(),
            ordinary.initial_authority(),
            &mut ids,
        )
        .await?;
    } else {
        drop(scratch);
    }
    drop(frame);
    drop(summary);
    drop(ordinary);
    drop(initial);
    Ok(AdmittedAdministrativeReceipt {
        receipt,
        next_sequence: Some(next_sequence),
        _charge: charge,
    })
}

/// Apply a sealed resolved stream under the original queue fence. This private
/// prerequisite does not create a receipt or establish publication-size approval.
/// Every command targets an existing record; no physical ID may be allocated.
/// On OutcomeUnknown the enclosing owner must immediately abandon this Rust
/// Transaction handle: server-side ROLLBACK has ended its transaction. Never
/// continue siblings or write a rejection receipt through that borrowed handle.
/// Cancelling the owner/application future requires dropping or rolling back
/// the containing Transaction; cancellation bypasses the result-error block.
/// Public caller cancellation must leave the admitted backend job running.
async fn apply_resolved_administrative_commands(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    queue: &QueueRow<'_>,
    mut scratch: fireweed_queue_spool::SealedAdministrativeScratch,
    reservation: &Arc<QueueReservation>,
    workers: &fireweed_queue_spool::ScratchWorkers,
) -> QueueResult<fireweed_queue_spool::SealedAdministrativeScratch> {
    use fireweed_engine::queue_publication::{
        EffectRef, FieldsCommandRef, materialize_administrative_work,
    };
    let result = async {
        scratch.verify_context(address, queue.definition_pin, queue.next_sequence)?;
        scratch = workers
            .run(move || {
                scratch.rewind_commands()?;
                Ok(scratch)
            })
            .await?;
        loop {
            let (next, packet) = workers
                .run(move || {
                    let packet = scratch.next_command()?;
                    Ok((scratch, packet))
                })
                .await?;
            scratch = next;
            let Some(packet) = packet else { break };
            let command = packet.command()?;
            let mut bound = (command.identity.key.len() as u64)
                .checked_mul(16)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| storage("resolved application identity bound"))?;
            for name in queue.schema.work.indexes.values().flatten() {
                bound = bound
                    .checked_add(
                        (name.len() as u64)
                            .checked_mul(8)
                            .ok_or_else(|| storage("resolved application field bound"))?,
                    )
                    .and_then(|n| n.checked_add(1024))
                    .ok_or_else(|| storage("resolved application field bound"))?;
            }
            if let EffectRef::WorkPut {
                fields: FieldsCommandRef::Edit(fields),
                ..
            } = command.effect
            {
                for entry in fields.iter() {
                    let (name, _) = entry.map_err(|error| storage(format!("{error:?}")))?;
                    bound = bound
                        .checked_add(
                            (name.len() as u64)
                                .checked_mul(8)
                                .ok_or_else(|| storage("resolved application edit bound"))?,
                        )
                        .and_then(|n| n.checked_add(1024))
                        .ok_or_else(|| storage("resolved application edit bound"))?;
                }
            }
            let _input = reservation.buffer(bound)?;
            let mut names = BTreeSet::new();
            if let EffectRef::WorkPut {
                fields: FieldsCommandRef::Edit(fields),
                ..
            } = command.effect
            {
                for entry in fields.iter() {
                    names.insert(
                        entry
                            .map_err(|error| storage(format!("{error:?}")))?
                            .0
                            .to_owned(),
                    );
                }
                let edited = names.clone();
                for columns in queue.schema.work.indexes.values() {
                    if columns.iter().any(|name| edited.contains(name)) {
                        names.extend(columns.iter().cloned());
                    }
                }
            }
            // Empty patch is only an addressed hydration carrier. No planner or
            // user expectation evaluates it; resolved predecessor checks follow.
            let carrier = CommitGroup {
                administration: None,
                request_id: String::new(),
                claimed_updates: vec![],
                state_updates: vec![],
                work_inserts: vec![],
                work_updates: vec![UnclaimedUpdate {
                    key: command.identity.key.into(),
                    expect: StateExpect::Version(command.prior_highwater),
                    change: UnclaimedChange::Patch(WorkPatch::default()),
                }],
            };
            let mut loaded =
                load_group_with_fields(tx, queue, &carrier, reservation, Some(&names)).await?;
            if loaded.ids.len() != 1 || loaded.snapshot.work.len() != 1 {
                return Err(storage("resolved administrative record must exist"));
            }
            let before = std::mem::take(&mut loaded.snapshot);
            let materialized =
                materialize_administrative_work(command, &before, &queue.schema.work, reservation)
                    .map_err(|error| storage(format!("{error:?}")))?;
            if !loaded.ids.contains_key(&RecordIdentity {
                collection: None,
                key: command.identity.key.into(),
            }) {
                return Err(storage("resolved administrative physical ID missing"));
            }
            persist_delta(tx, queue, materialized.delta(), &before, &mut loaded).await?;
            #[cfg(test)]
            APPLICATION_BOUNDARY.with(|hook| {
                if let Some((id, callback)) = &*hook.borrow()
                    && *id == queue.id
                {
                    callback();
                }
            });
            #[cfg(test)]
            check_resolved_application_boundary(tx, queue.id).await?;
        }
        workers
            .run(move || {
                scratch.rewind_commands()?;
                Ok(scratch)
            })
            .await
    }
    .await;
    if result.is_err() {
        // Infrastructure/predecessor failure discards earlier sibling SQL too.
        let _ = tx.batch_execute("ROLLBACK").await;
    }
    result
}
#[cfg(test)]
type ApplicationBoundary = Option<(i64, Box<dyn Fn()>)>;
#[cfg(test)]
thread_local! {
    static APPLICATION_FAILURE_AFTER: std::cell::Cell<Option<(i64,u64)>> = const { std::cell::Cell::new(None) };
    static APPLICATION_BOUNDARY: std::cell::RefCell<ApplicationBoundary> = const {std::cell::RefCell::new(None)};
}
#[cfg(test)]
async fn check_resolved_application_boundary(
    tx: &Transaction<'_>,
    queue_id: i64,
) -> QueueResult<()> {
    let fail = APPLICATION_FAILURE_AFTER.with(|fault| match fault.get() {
        Some((id, 1)) if id == queue_id => {
            fault.set(None);
            true
        }
        Some((id, n)) if id == queue_id => {
            fault.set(Some((id, n - 1)));
            false
        }
        _ => false,
    });
    if fail {
        tx.query_one("SELECT 1/0", &[]).await.map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_administration::*;
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn native_owned_scratch_streams_5001_changes_without_queue_writes_and_rolls_back_source_failure()
     {
        use fireweed_engine::queue_publication::EffectRef;
        use fireweed_queue_spool::{ScratchDiskQuota, ScratchWorkers};
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::new(),
            },
            state: BTreeMap::from([(
                "instances".into(),
                CollectionDefinition {
                    fields: BTreeMap::new(),
                    indexes: BTreeMap::new(),
                    unique_indexes: BTreeSet::new(),
                    additional_field_prefixes: BTreeSet::new(),
                },
            )]),
            progress_bound_nanos: 1_000_000_000,
            max_attempts: 3,
        };
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let address = QueueAddress {
            tenant: "native-scratch".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: address.clone(),
                schema,
            })
            .await
            .unwrap();
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let seed_tx = client.transaction().await.unwrap();
        let seed = queue_row(&seed_tx, &address, &job).await.unwrap();
        let queue_id = seed.id;
        seed_tx.execute("INSERT INTO fireweed_api007.records(queue_id,collection,key_hash,key,version,active,insertion_sequence) SELECT $1,0,sha256(convert_to('bulk-'||lpad(n::text,5,'0'),'UTF8')),convert_to('bulk-'||lpad(n::text,5,'0'),'UTF8'),1,TRUE,n::numeric FROM generate_series(1,5001) n", &[&queue_id]).await.unwrap();
        seed_tx
            .execute(
                "UPDATE fireweed_api007.queues SET next_sequence=5001 WHERE id=$1",
                &[&queue_id],
            )
            .await
            .unwrap();
        drop(seed);
        seed_tx.commit().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let fenced = queue_row(&tx, &address, &job).await.unwrap();
        let sequence_before: i64 = tx
            .query_one("SELECT last_value FROM fireweed_api007.records_id_seq", &[])
            .await
            .unwrap()
            .get(0);
        let group = CommitGroup {
            administration: None,
            request_id: "native-scratch-state-and-next".into(),
            claimed_updates: vec![],
            work_updates: vec![],
            state_updates: vec![StateUpdate {
                collection: "instances".into(),
                key: "workflow".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: BTreeMap::new(),
                    value: vec![1].into(),
                },
            }],
            work_inserts: vec![NewWork {
                key: "next".into(),
                priority: 0,
                available_at: 0,
                fields: BTreeMap::new(),
                body: Default::default(),
            }],
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(17),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: BTreeSet::new(),
                },
            }]),
        };
        let directory = tempfile::tempdir().unwrap();
        let quota = ScratchDiskQuota::new(8 << 20).unwrap();
        let workers = ScratchWorkers::new(2, 4).unwrap();
        let observed = std::rc::Rc::new(std::cell::Cell::new(None));
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        let count_hook = count.clone();
        let observed_hook = observed.clone();
        let admission_hook = admission.clone();
        ROW_BOUNDARY.with(|hook| {
            *hook.borrow_mut() = Some((
                queue_id,
                Box::new(move |_| {
                    let n = count_hook.get();
                    count_hook.set(n + 1);
                    let bytes = admission_hook.usage().unwrap().1;
                    if n == 1 {
                        observed_hook.set(Some(bytes));
                    } else if n > 1 {
                        assert_eq!(observed_hook.get(), Some(bytes));
                    }
                }),
            ))
        });
        struct ResetBoundary;
        impl Drop for ResetBoundary {
            fn drop(&mut self) {
                ROW_BOUNDARY.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let reset = ResetBoundary;
        {
            let prepared = prepare_native_scratch(
                &tx,
                &address,
                &fenced,
                &group,
                &intent,
                &QueueLimits::default(),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            assert_eq!(count.get(), 5001);
            drop(reset);
            let NativeAdministrativePreparation {
                mut scratch,
                summary,
                ordinary,
                ..
            } = prepared;
            assert_eq!(summary.packet_count(), 5001);
            assert_eq!(summary.result().unwrap().histogram.matched, 5001);
            assert_eq!(ordinary.snapshot.next_sequence, 5001);
            assert!(ordinary.snapshot.work.is_empty() && ordinary.snapshot.state.is_empty());
            let expected_address = address.clone();
            let pin = fenced.definition_pin;
            workers
                .run(move || {
                    scratch.verify_context(&expected_address, pin, 5001)?;
                    assert!(!scratch.reduce_evidence()?.final_collision);
                    let mut n = 0;
                    while let Some(packet) = scratch.next_command()? {
                        let command = packet.command()?;
                        assert_eq!(command.identity.key, format!("bulk-{:05}", n + 1));
                        assert_eq!(command.expected_version, Some(1));
                        assert_eq!(command.output_highwater, 2);
                        let EffectRef::WorkPut { metadata, .. } = command.effect else {
                            panic!("put")
                        };
                        assert_eq!(metadata.priority, 17);
                        assert_eq!(metadata.insertion_sequence, n + 1);
                        n += 1;
                    }
                    assert_eq!(n, 5001);
                    Ok(())
                })
                .await
                .unwrap();
            drop(summary);
            drop(ordinary);
        }
        assert_eq!(quota.usage().unwrap(), 0);
        {
            let prepared = prepare_native_scratch(
                &tx,
                &address,
                &fenced,
                &group,
                &intent,
                &QueueLimits::default(),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            let clocks = std::cell::Cell::new(0);
            let finalized = finalize_native_semantics(
                prepared,
                &address,
                &group,
                &intent,
                &fenced,
                || {
                    clocks.set(clocks.get() + 1);
                    Ok(0)
                },
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap()
            .unwrap_or_else(|reason| panic!("unexpected ordinary rejection: {reason:?}"));
            assert_eq!(clocks.get(), 1);
            assert_eq!(finalized.summary.packet_count(), 5001);
            assert_eq!(finalized.ordinary.delta().next_sequence, 5002);
            assert_eq!(finalized.ordinary.delta().state[0].key, "workflow");
            assert_eq!(finalized.ordinary.delta().work[0].record.work.key, "next");
            assert_eq!(finalized.initial.snapshot.next_sequence, 5001);
            assert!(finalized.initial.ids.is_empty());
            let NativeProvisionalGroup {
                scratch,
                summary,
                ordinary,
                initial,
                ..
            } = finalized;
            let application_bytes = std::rc::Rc::new(std::cell::Cell::new(None));
            let observed_bytes = application_bytes.clone();
            let application_admission = admission.clone();
            APPLICATION_BOUNDARY.with(|hook| {
                *hook.borrow_mut() = Some((
                    queue_id,
                    Box::new(move || {
                        let bytes = application_admission.usage().unwrap().1;
                        assert!(bytes < 1 << 20);
                        if let Some(previous) = observed_bytes.get() {
                            assert_eq!(previous, bytes);
                        } else {
                            observed_bytes.set(Some(bytes));
                        }
                    }),
                ))
            });
            struct ResetApplication;
            impl Drop for ResetApplication {
                fn drop(&mut self) {
                    APPLICATION_BOUNDARY.with(|hook| *hook.borrow_mut() = None);
                }
            }
            let reset_application = ResetApplication;
            tx.batch_execute("SAVEPOINT resolved_application")
                .await
                .unwrap();
            let scratch = apply_resolved_administrative_commands(
                &tx, &address, &fenced, scratch, &job, &workers,
            )
            .await
            .unwrap();
            drop(reset_application);
            assert!(application_bytes.get().is_some());
            let changed:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=2 AND priority=17",&[&queue_id]).await.unwrap().get(0);
            assert_eq!(changed, 5001);
            let logical: String = tx
                .query_one(
                    "SELECT next_sequence::text FROM fireweed_api007.queues WHERE id=$1",
                    &[&queue_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(logical, "5001");
            // Administrative replay does not allocate physical IDs or publish
            // the later ordinary sequence. Retain rewindable command authority.
            drop(scratch);
            tx.batch_execute("ROLLBACK TO SAVEPOINT resolved_application")
                .await
                .unwrap();
            drop(summary);
            drop(ordinary);
            drop(initial);
        }
        assert_eq!(quota.usage().unwrap(), 0);
        let unchanged: i64 = tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=1 AND priority=0", &[&queue_id]).await.unwrap().get(0);
        assert_eq!(unchanged, 5001);
        let count_all: i64 = tx
            .query_one(
                "SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1",
                &[&queue_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count_all, 5001); // neither workflow state nor next work was inserted
        let sequence_after: i64 = tx
            .query_one("SELECT last_value FROM fireweed_api007.records_id_seq", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(sequence_before, sequence_after);
        // Earlier tentative sibling SQL effects cannot survive a late source fault.
        tx.execute(
            "UPDATE fireweed_api007.records SET priority=99 WHERE queue_id=$1 AND key=$2",
            &[&queue_id, &b"bulk-00001".as_slice()],
        )
        .await
        .unwrap();
        let before_failure = admission.usage().unwrap().1;
        SQL_FAILURE_AFTER.with(|fault| fault.set(Some((queue_id, 2))));
        struct ResetFailure;
        impl Drop for ResetFailure {
            fn drop(&mut self) {
                SQL_FAILURE_AFTER.with(|fault| fault.set(None));
            }
        }
        let reset = ResetFailure;
        assert!(matches!(
            prepare_native_scratch(
                &tx,
                &address,
                &fenced,
                &group,
                &intent,
                &QueueLimits::default(),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota)
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(reset);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, before_failure);
        tx.rollback().await.unwrap();
        let priority: i64 = client
            .query_one(
                "SELECT priority FROM fireweed_api007.records WHERE queue_id=$1 AND key=$2",
                &[&queue_id, &b"bulk-00001".as_slice()],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(priority, 0);
        drop(fenced);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn resolved_application_preserves_opaque_data_and_rolls_back_late_failure() {
        use fireweed_queue_spool::{ScratchDiskQuota, ScratchWorkers};
        let _fixture = physical_fixture().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COLLIDING_PROBES.set(false);
                APPLICATION_FAILURE_AFTER.set(None);
            }
        }
        let _reset = Reset;
        COLLIDING_PROBES.set(true);
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let mut schema = fireweed_conformance::queue_contract::schema();
        schema.work.fields = BTreeMap::from([
            ("target".into(), FieldType::Text),
            ("gate".into(), FieldType::Boolean),
            ("opaque".into(), FieldType::Bytes),
        ]);
        schema.work.indexes = BTreeMap::from([
            ("compound".into(), vec!["target".into(), "gate".into()]),
            ("opaque".into(), vec!["opaque".into()]),
        ]);
        schema.work.unique_indexes = BTreeSet::from(["compound".into()]);
        schema.work.additional_field_prefixes = BTreeSet::from(["/payload/".into()]);
        let address = QueueAddress {
            tenant: "resolved-application".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: address.clone(),
                schema: schema.clone(),
            })
            .await
            .unwrap();
        store
            .insert(InsertRequest {
                queue: address.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items: (0..3)
                    .map(|n| NewWork {
                        key: format!("key-{n}"),
                        priority: 0,
                        available_at: 0,
                        fields: BTreeMap::from([
                            ("target".into(), FieldValue::Text(format!("u-{n}"))),
                            ("gate".into(), FieldValue::Boolean(true)),
                            ("opaque".into(), FieldValue::Bytes(vec![n; 32768])),
                            ("/payload/keep".into(), FieldValue::Text("retained".into())),
                        ]),
                        body: vec![n; 262144].into(),
                    })
                    .collect(),
            })
            .await
            .unwrap();
        store
            .insert(InsertRequest {
                queue: address.clone(),
                request_id: "seed-tombstone".into(),
                mode: InsertMode::InsertOnly,
                items: vec![NewWork {
                    key: "dead".into(),
                    priority: 0,
                    available_at: 0,
                    fields: Default::default(),
                    body: Default::default(),
                }],
            })
            .await
            .unwrap();
        store
            .commit(CommitBatch {
                queue: address.clone(),
                groups: vec![CommitGroup {
                    administration: None,
                    request_id: "delete-tombstone".into(),
                    claimed_updates: vec![],
                    state_updates: vec![],
                    work_inserts: vec![],
                    work_updates: vec![UnclaimedUpdate {
                        key: "dead".into(),
                        expect: StateExpect::Version(1),
                        change: UnclaimedChange::Delete,
                    }],
                }],
            })
            .await
            .unwrap();
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(2, 4).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let quota = ScratchDiskQuota::new(8 << 20).unwrap();
        let group = CommitGroup {
            administration: None,
            request_id: "resolved".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
        };
        let update = AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(19),
                fields: BTreeMap::from([("target".into(), None)]),
                body: Some(Vec::new().into()),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        };
        let mut queue_id = 0;
        for scenario in 0..6 {
            let tx = client.transaction().await.unwrap();
            let queue = queue_row(&tx, &address, &job).await.unwrap();
            queue_id = queue.id;
            let intent = AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                    id: "all".into(),
                    selector: WorkSelector::all(vec![]),
                    precondition: WorkSelector::all(vec![]),
                    action: if scenario == 1 {
                        AdministrativeAction::Delete {
                            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                        }
                    } else if scenario == 5 {
                        AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(19),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: BTreeSet::new(),
                        }
                    } else {
                        update.clone()
                    },
                }]),
            };
            let unchanged=tx.query("SELECT r.key,f.ctid::text,i.ctid::text,a.ctid::text FROM fireweed_api007.records r JOIN fireweed_api007.fields f ON f.record_id=r.id AND f.field=1 JOIN fireweed_api007.indexes i ON i.record_id=r.id AND i.index_no=1 JOIN fireweed_api007.additional_fields a ON a.record_id=r.id WHERE r.queue_id=$1 ORDER BY r.key",&[&queue.id]).await.unwrap();
            let prepared = prepare_native_scratch(
                &tx,
                &address,
                &queue,
                &group,
                &intent,
                &QueueLimits::default(),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            let finalized = finalize_native_semantics(
                prepared,
                &address,
                &group,
                &intent,
                &queue,
                || Ok(0),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap()
            .unwrap_or_else(|reason| panic!("unexpected rejection {reason:?}"));
            let NativeProvisionalGroup {
                scratch,
                summary,
                ordinary,
                initial,
                ..
            } = finalized;
            if (2..5).contains(&scenario) {
                tx.execute(
                    "UPDATE fireweed_api007.records SET priority=99 WHERE queue_id=$1 AND key=$2",
                    &[&queue.id, &b"key-0".as_slice()],
                )
                .await
                .unwrap();
                if scenario == 2 {
                    APPLICATION_FAILURE_AFTER.set(Some((queue.id, 2)));
                }
                if scenario == 3 {
                    tx.execute(
                        "UPDATE fireweed_api007.records SET version=2 WHERE queue_id=$1 AND key=$2",
                        &[&queue.id, &b"key-1".as_slice()],
                    )
                    .await
                    .unwrap();
                }
                if scenario == 4 {
                    tx.execute("UPDATE fireweed_api007.records SET active=FALSE WHERE queue_id=$1 AND key=$2",&[&queue.id,&b"key-1".as_slice()]).await.unwrap();
                }
            }
            let result = apply_resolved_administrative_commands(
                &tx, &address, &queue, scratch, &job, &workers,
            )
            .await;
            if (2..5).contains(&scenario) {
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            } else {
                let scratch = result.unwrap();
                drop(scratch);
                if scenario == 0 {
                    let changed:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=2 AND priority=19",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(changed, 3);
                    let empty:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.bodies b JOIN fireweed_api007.records r ON r.id=b.record_id WHERE r.queue_id=$1 AND octet_length(b.value)=0",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(empty, 3);
                    let removed:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.fields f JOIN fireweed_api007.records r ON r.id=f.record_id WHERE r.queue_id=$1 AND f.field=2",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(removed, 0);
                    let unique:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.indexes WHERE queue_id=$1 AND index_no=0 AND is_unique",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(unique, 0);
                    let retained=tx.query("SELECT r.key,f.ctid::text,i.ctid::text,a.ctid::text FROM fireweed_api007.records r JOIN fireweed_api007.fields f ON f.record_id=r.id AND f.field=1 JOIN fireweed_api007.indexes i ON i.record_id=r.id AND i.index_no=1 JOIN fireweed_api007.additional_fields a ON a.record_id=r.id WHERE r.queue_id=$1 ORDER BY r.key",&[&queue.id]).await.unwrap();
                    assert_eq!(retained.len(), 3);
                    for (old, new) in unchanged.iter().zip(&retained) {
                        for col in 1..4 {
                            assert_eq!(old.get::<_, String>(col), new.get::<_, String>(col));
                        }
                    }
                } else if scenario == 5 {
                    let bodies:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.bodies b JOIN fireweed_api007.records r ON r.id=b.record_id WHERE r.queue_id=$1 AND octet_length(b.value)=262144",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(bodies, 3);
                    let complete:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.indexes WHERE queue_id=$1 AND index_no=0 AND is_unique",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(complete, 3);
                    let retained=tx.query("SELECT r.key,f.ctid::text,i.ctid::text,a.ctid::text FROM fireweed_api007.records r JOIN fireweed_api007.fields f ON f.record_id=r.id AND f.field=1 JOIN fireweed_api007.indexes i ON i.record_id=r.id AND i.index_no=1 JOIN fireweed_api007.additional_fields a ON a.record_id=r.id WHERE r.queue_id=$1 ORDER BY r.key",&[&queue.id]).await.unwrap();
                    for (old, new) in unchanged.iter().zip(&retained) {
                        for col in 1..4 {
                            assert_eq!(old.get::<_, String>(col), new.get::<_, String>(col));
                        }
                    }
                } else {
                    for table in ["bodies", "fields", "additional_fields", "indexes"] {
                        let count:i64=tx.query_one(&format!("SELECT count(*) FROM fireweed_api007.{table} p JOIN fireweed_api007.records r ON p.record_id=r.id WHERE r.queue_id=$1"),&[&queue.id]).await.unwrap().get(0);
                        assert_eq!(count, 0);
                    }
                    let tombstones:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND NOT active AND version=2",&[&queue.id]).await.unwrap().get(0);
                    assert_eq!(tombstones, 4); // three new deletions plus the original tombstone
                }
            }
            drop(summary);
            drop(ordinary);
            drop(initial);
            drop(queue);
            tx.rollback().await.unwrap();
            let original:i64=client.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=1 AND priority=0",&[&queue_id]).await.unwrap().get(0);
            assert_eq!(original, 3);
            assert_eq!(quota.usage().unwrap(), 0);
        }
        // The addressed route shares this exact physical pipeline and retains
        // initial before data for duplicate positions, never a speculative row.
        for overlap in [false, true] {
            let tx = client.transaction().await.unwrap();
            let queue = queue_row(&tx, &address, &job).await.unwrap();
            let addressed = AdministrativeIntent {
                evaluated_at: 0,
                before_fields: Some(BTreeSet::from(["opaque".into()])),
                operation: AdministrativeOperation::Addressed(
                    [
                        "key-0", "key-0", "missing", "missing", "dead", "dead", "key-1",
                    ]
                    .into_iter()
                    .map(|key| AdministrativeEntry {
                        key: key.into(),
                        expected_version: None,
                        precondition: WorkSelector::all(vec![]),
                        action: update.clone(),
                    })
                    .collect(),
                ),
            };
            let mut ordinary = group.clone();
            ordinary.state_updates.push(StateUpdate {
                collection: "instances".into(),
                key: "workflow".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Default::default(),
                    value: vec![1].into(),
                },
            });
            ordinary.work_inserts.push(NewWork {
                key: if overlap { "missing" } else { "next" }.into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            });
            let result_floor =
                fireweed_engine::queue_publication::measure_administrative_receipt_fallbacks(
                    &ordinary.request_id,
                )
                .unwrap();
            let publication_floor =
                fireweed_engine::queue_publication::measure_administrative_publication_fallbacks(
                    &address,
                    queue.definition_pin,
                    &ordinary.request_id,
                )
                .unwrap();
            let usage_before_preflight = admission.usage().unwrap();
            for (result_limit, publication_limit) in [
                (result_floor - 1, u64::MAX),
                (u64::MAX, publication_floor - 1),
            ] {
                assert!(matches!(
                    prepare_native_scratch_bounded(
                        &tx,
                        &address,
                        &queue,
                        &ordinary,
                        &addressed,
                        &QueueLimits::default(),
                        result_limit,
                        publication_limit,
                        &job,
                        &workers,
                        directory.path(),
                        Arc::clone(&quota)
                    )
                    .await,
                    Err(QueueError::Invalid(_))
                ));
                assert_eq!(admission.usage().unwrap(), usage_before_preflight);
            }
            if !overlap {
                tx.batch_execute("SAVEPOINT ordinary_corruption")
                    .await
                    .unwrap();
                tx.execute(
                    "UPDATE fireweed_api007.records SET lease_hash=$2 WHERE queue_id=$1 AND key=$3",
                    &[&queue.id, &vec![0u8; 32], &b"key-2".as_slice()],
                )
                .await
                .unwrap();
                let mut corrupt_group = ordinary.clone();
                corrupt_group.work_updates.push(UnclaimedUpdate {
                    key: "key-2".into(),
                    expect: StateExpect::Version(1),
                    change: UnclaimedChange::Patch(WorkPatch {
                        priority: Some(23),
                        ..Default::default()
                    }),
                });
                // Raw authority must fail before an early CAS rejection or deletion
                // can conceal an inconsistent pending-row lease hash.
                for delete in [false, true] {
                    let mut intent = addressed.clone();
                    if delete {
                        corrupt_group.work_updates.clear();
                        corrupt_group.state_updates[0].expect = StateExpect::Absent;
                        intent.operation =
                            AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                                key: "key-2".into(),
                                expected_version: None,
                                precondition: WorkSelector::all(vec![]),
                                action: AdministrativeAction::Delete {
                                    lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                                },
                            }]);
                    } else {
                        corrupt_group.state_updates[0].expect = StateExpect::Version(99);
                    }
                    match prepare_native_scratch_bounded(
                        &tx,
                        &address,
                        &queue,
                        &corrupt_group,
                        &intent,
                        &QueueLimits::default(),
                        1024,
                        publication_floor,
                        &job,
                        &workers,
                        directory.path(),
                        Arc::clone(&quota),
                    )
                    .await
                    {
                        Err(QueueError::OutcomeUnknown(message))
                            if (!delete && message.contains("raw work lease authority"))
                                || (delete && message == "addressed source failed before EOF") => {}
                        Err(error) => panic!("delete={delete}: unexpected error {error:?}"),
                        Ok(_) => panic!("delete={delete}: corrupt authority was accepted"),
                    }
                }
                tx.batch_execute("ROLLBACK TO SAVEPOINT ordinary_corruption; RELEASE SAVEPOINT ordinary_corruption").await.unwrap();
            }
            if !overlap {
                let mut bad_cas = ordinary.clone();
                bad_cas.state_updates[0].expect = StateExpect::Version(99);
                let bad = prepare_native_scratch_bounded(
                    &tx,
                    &address,
                    &queue,
                    &bad_cas,
                    &addressed,
                    &QueueLimits::default(),
                    1024,
                    publication_floor,
                    &job,
                    &workers,
                    directory.path(),
                    Arc::clone(&quota),
                )
                .await
                .unwrap();
                assert!(bad.summary.result_overflowed());
                let bad = finalize_native_semantics(
                    bad,
                    &address,
                    &bad_cas,
                    &addressed,
                    &queue,
                    || Ok(0),
                    &job,
                    &workers,
                    directory.path(),
                    Arc::clone(&quota),
                )
                .await
                .unwrap();
                assert!(matches!(bad, Err(Rejection::NotFound))); // CAS failure precedes both size overflows.
            }
            let mismatched = prepare_native_scratch_bounded(
                &tx,
                &address,
                &queue,
                &ordinary,
                &addressed,
                &QueueLimits::default(),
                1024,
                u64::MAX,
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            let mut changed_request = ordinary.clone();
            changed_request.request_id.push_str("changed");
            changed_request.work_inserts[0].key = "different-unselected-key".into();
            let mismatch_clock = std::cell::Cell::new(0);
            let mismatch = finalize_native_semantics(
                mismatched,
                &address,
                &changed_request,
                &addressed,
                &queue,
                || {
                    mismatch_clock.set(mismatch_clock.get() + 1);
                    Ok(0)
                },
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await;
            assert!(
                matches!(mismatch, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("different request"))
            );
            assert_eq!(mismatch_clock.get(), 0);
            let bounded = prepare_native_scratch_bounded(
                &tx,
                &address,
                &queue,
                &ordinary,
                &addressed,
                &QueueLimits::default(),
                1024,
                u64::MAX,
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            assert!(bounded.summary.result_overflowed());
            let bounded_clock = std::cell::Cell::new(0);
            let bounded = finalize_native_semantics(
                bounded,
                &address,
                &ordinary,
                &addressed,
                &queue,
                || {
                    bounded_clock.set(bounded_clock.get() + 1);
                    Ok(0)
                },
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            if overlap {
                assert!(matches!(bounded, Err(Rejection::Invalid(_))));
                assert_eq!(bounded_clock.get(), 0);
            } else {
                let bounded = bounded.unwrap_or_else(|reason| {
                    panic!("unexpected bounded business rejection {reason:?}")
                });
                assert_eq!(bounded_clock.get(), 1);
                assert_eq!(
                    bounded.assess_result_limit(&ordinary, 1024).unwrap(),
                    fireweed_engine::queue_publication::AdministrativeResultLimit::TooLarge
                );
            }
            let untouched: i64 = tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=2 AND priority=19", &[&queue.id]).await.unwrap().get(0);
            assert_eq!(untouched, 0);
            let prepared = prepare_native_scratch(
                &tx,
                &address,
                &queue,
                &ordinary,
                &addressed,
                &QueueLimits::default(),
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            let items = &prepared.summary.result().unwrap().items;
            assert_eq!(
                items
                    .iter()
                    .map(|item| item.outcome.clone())
                    .collect::<Vec<_>>(),
                vec![
                    AdministrativeOutcome::Changed { version: 2 },
                    AdministrativeOutcome::Invalid,
                    AdministrativeOutcome::NotFound,
                    AdministrativeOutcome::Invalid,
                    AdministrativeOutcome::NotFound,
                    AdministrativeOutcome::Invalid,
                    AdministrativeOutcome::Changed { version: 2 }
                ]
            );
            assert_eq!(items[0].before.as_ref().unwrap().version, 1);
            assert_eq!(items[1].before.as_ref().unwrap().version, 1);
            assert_eq!(
                items[0].before.as_ref().unwrap().fields["opaque"],
                items[1].before.as_ref().unwrap().fields["opaque"]
            );
            let clocks = std::cell::Cell::new(0);
            let finalized = finalize_native_semantics(
                prepared,
                &address,
                &ordinary,
                &addressed,
                &queue,
                || {
                    clocks.set(clocks.get() + 1);
                    Ok(0)
                },
                &job,
                &workers,
                directory.path(),
                Arc::clone(&quota),
            )
            .await
            .unwrap();
            if overlap {
                assert!(matches!(finalized, Err(Rejection::Invalid(_))));
                assert_eq!(clocks.get(), 0);
            } else {
                let finalized = finalized.unwrap_or_else(|reason| panic!("unexpected {reason:?}"));
                assert_eq!(clocks.get(), 1);
                let fireweed_engine::queue_publication::AdministrativeResultLimit::Fits {
                    complete_receipt_bytes,
                } = finalized.assess_result_limit(&ordinary, u64::MAX).unwrap()
                else {
                    panic!("unbounded result")
                };
                assert_eq!(
                    finalized
                        .assess_result_limit(&ordinary, complete_receipt_bytes)
                        .unwrap(),
                    fireweed_engine::queue_publication::AdministrativeResultLimit::Fits {
                        complete_receipt_bytes
                    }
                );
                assert_eq!(
                    finalized
                        .assess_result_limit(&ordinary, complete_receipt_bytes - 1)
                        .unwrap(),
                    fireweed_engine::queue_publication::AdministrativeResultLimit::TooLarge
                );
                let NativePublicationDecision::Fits {
                    bytes: publication_bytes,
                    commands,
                } = finalized
                    .assess_publication_limit(&address, &queue, &ordinary, &addressed, &job)
                    .unwrap()
                else {
                    panic!("unbounded publication")
                };
                assert_eq!(commands, 4); // two administrative rows + state and next work
                let fallback = fireweed_engine::queue_publication::measure_administrative_publication_fallbacks(&address, queue.definition_pin, &ordinary.request_id).unwrap();
                for publication_limit in [publication_bytes, publication_bytes - 1, fallback] {
                    let prepared = prepare_native_scratch_bounded(
                        &tx,
                        &address,
                        &queue,
                        &ordinary,
                        &addressed,
                        &QueueLimits::default(),
                        u64::MAX,
                        publication_limit,
                        &job,
                        &workers,
                        directory.path(),
                        Arc::clone(&quota),
                    )
                    .await
                    .unwrap();
                    let prepared = finalize_native_semantics(
                        prepared,
                        &address,
                        &ordinary,
                        &addressed,
                        &queue,
                        || Ok(0),
                        &job,
                        &workers,
                        directory.path(),
                        Arc::clone(&quota),
                    )
                    .await
                    .unwrap()
                    .unwrap_or_else(|reason| panic!("publication trial {reason:?}"));
                    if publication_limit == fallback {
                        assert!(prepared.scratch.publication_overflowed());
                    }
                    let decision = prepared
                        .assess_publication_limit(&address, &queue, &ordinary, &addressed, &job)
                        .unwrap();
                    if publication_limit == publication_bytes {
                        assert_eq!(
                            decision,
                            NativePublicationDecision::Fits {
                                bytes: publication_bytes,
                                commands
                            }
                        );
                    } else {
                        assert_eq!(decision, NativePublicationDecision::TooLarge);
                    }
                    let mut changed = ordinary.clone();
                    changed.request_id.push_str("changed");
                    assert!(
                        prepared
                            .assess_publication_limit(&address, &queue, &changed, &addressed, &job)
                            .is_err()
                    );
                    let prepared = prepare_native_publication(
                        prepared, &address, &queue, &ordinary, &addressed, &job, &workers,
                    )
                    .await
                    .unwrap();
                    if publication_limit == publication_bytes {
                        let prepared = prepared.expect("exact-limit full frame");
                        assert_eq!(prepared.frame.as_bytes().len() as u64, publication_bytes);
                    } else {
                        assert!(prepared.is_none());
                    }
                }
                let NativePreparedPublication {
                    provisional: finalized,
                    frame,
                } = prepare_native_publication(
                    finalized, &address, &queue, &ordinary, &addressed, &job, &workers,
                )
                .await
                .unwrap()
                .expect("complete resolved publication");
                assert_eq!(frame.as_bytes().len() as u64, publication_bytes);
                assert!(
                    fireweed_engine::queue_publication::view_publication(frame.as_bytes()).is_err()
                );
                let view =
                    fireweed_engine::queue_publication::view_resolved_publication(frame.as_bytes())
                        .unwrap();
                assert_eq!(view.summary().commands, 4);
                assert_eq!(view.summary().initial_sequence, 4);
                assert_eq!(view.summary().final_sequence, 5);
                let receipt = view.receipts().next().unwrap().unwrap();
                assert_eq!(
                    *receipt.fingerprint,
                    ordinary
                        .administrative_fingerprint(&address, &addressed, u64::MAX)
                        .unwrap()
                );
                let fireweed_engine::queue_publication::ReceiptOutcomeRef::Commit(
                    fireweed_engine::queue_publication::CommitOutcomeRef::AdministrativeCommitted {
                        versions,
                        ..
                    },
                ) = receipt.outcome
                else {
                    panic!("administrative receipt")
                };
                assert_eq!(
                    versions
                        .iter()
                        .map(|version| version.unwrap().identity.key)
                        .collect::<Vec<_>>(),
                    vec!["workflow", "next"]
                );

                assert_eq!(
                    view.commands()
                        .map(|command| command.unwrap().identity.key)
                        .collect::<Vec<_>>(),
                    vec!["key-0", "key-1", "next", "workflow"]
                );
                let NativeProvisionalGroup {
                    scratch,
                    summary,
                    ordinary,
                    initial,
                    ..
                } = finalized;
                let scratch = apply_resolved_administrative_commands(
                    &tx, &address, &queue, scratch, &job, &workers,
                )
                .await
                .unwrap();
                drop(scratch);
                let mut ids = load_delta_ids(
                    &tx,
                    &queue,
                    ordinary.delta(),
                    ordinary.initial_authority(),
                    &job,
                )
                .await
                .unwrap();
                persist_delta(
                    &tx,
                    &queue,
                    ordinary.delta(),
                    ordinary.initial_authority(),
                    &mut ids,
                )
                .await
                .unwrap();
                drop(ids);
                let changed:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=2 AND priority=19",&[&queue.id]).await.unwrap().get(0);
                assert_eq!(changed, 2);
                let logical: String = tx
                    .query_one(
                        "SELECT next_sequence::text FROM fireweed_api007.queues WHERE id=$1",
                        &[&queue.id],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(logical, "5");
                let added:i64=tx.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND key=ANY($2::bytea[])",&[&queue.id,&vec![b"workflow".to_vec(),b"next".to_vec()]]).await.unwrap().get(0);
                assert_eq!(added, 2);
                drop(frame);
                drop(summary);
                drop(ordinary);
                drop(initial);
            }
            drop(queue);
            tx.rollback().await.unwrap();
            assert_eq!(quota.usage().unwrap(), 0);
            let original:i64=client.query_one("SELECT count(*) FROM fireweed_api007.records WHERE queue_id=$1 AND active AND version=1 AND priority=0",&[&queue_id]).await.unwrap().get(0);
            assert_eq!(original, 3);
        }
        assert_ne!(queue_id, 0);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        store.shutdown().await.unwrap();
    }
    fn operation(selectors: Vec<WorkSelector>, time: i64) -> AdministrativeIntent {
        AdministrativeIntent {
            evaluated_at: time,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(
                selectors
                    .into_iter()
                    .enumerate()
                    .map(|(n, selector)| AdministrativeClause {
                        id: format!("clause-{n}"),
                        selector,
                        // First ownership precedes this deliberately false precondition.
                        precondition: WorkSelector::any(vec![]),
                        action: AdministrativeAction::Delete {
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                        },
                    })
                    .collect(),
            ),
        }
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn native_selector_sql_matches_every_leaf_and_first_owner() {
        native_fixture(false).await;
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn native_selector_sql_disambiguates_forced_probe_collisions() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COLLIDING_PROBES.set(false);
            }
        }
        COLLIDING_PROBES.set(true);
        let _reset = Reset;
        native_fixture(true).await;
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn native_provider_preserves_initial_unique_owners_prefixes_and_active_delete() {
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let queue = QueueAddress {
            tenant: "api007-native-proposals".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        let mut schema = fireweed_conformance::queue_contract::schema();
        schema.work.fields = BTreeMap::from([
            ("value".into(), FieldType::Text),
            ("other".into(), FieldType::Text),
        ]);
        schema.work.indexes = BTreeMap::from([("unique".into(), vec!["value".into()])]);
        schema
            .work
            .indexes
            .insert("unrelated_unique".into(), vec!["other".into()]);
        schema.work.unique_indexes = BTreeSet::from(["unique".into(), "unrelated_unique".into()]);
        schema
            .work
            .additional_field_prefixes
            .insert("/attrs/".into());
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: schema.clone(),
            })
            .await
            .unwrap();
        let items = [("a", "x"), ("b", "y"), ("c", "z")]
            .into_iter()
            .map(|(key, value)| NewWork {
                key: key.into(),
                priority: 0,
                available_at: 0,
                body: vec![9; 1 << 20].into(),
                fields: BTreeMap::from([
                    ("value".into(), FieldValue::Text(value.into())),
                    ("other".into(), FieldValue::Text(format!("other-{key}"))),
                    ("/attrs/old".into(), FieldValue::Text("old".into())),
                ]),
            })
            .collect();
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items,
            })
            .await
            .unwrap();
        let claimed = store
            .claim(ClaimRequest {
                queue: queue.clone(),
                request_id: "lease-c".into(),
                filter: Filter {
                    keys: Some(BTreeSet::from(["c".into()])),
                    ..Default::default()
                },
                order: ClaimOrder::PriorityAscending,
                eligible_at: 0,
                lease_nanos: 1_000_000,
                max_records: 1,
                max_bytes: 1 << 20,
                fields: Some(BTreeSet::new()),
                include_body: false,
            })
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let tx = client.transaction().await.unwrap();
        let fenced = queue_row(&tx, &queue, &job).await.unwrap();
        let ordinary = CommitGroup {
            administration: None,
            request_id: "control".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let clause = |key: &str, value: &str| AdministrativeClause {
            id: key.into(),
            selector: WorkSelector::leaf(SelectorNode::Keys(BTreeSet::from([key.into()]))),
            precondition: WorkSelector::all(vec![]),
            action: AdministrativeAction::Update {
                patch: WorkPatch {
                    fields: BTreeMap::from([(
                        "value".into(),
                        Some(FieldValue::Text(value.into())),
                    )]),
                    ..Default::default()
                },
                lifecycle: AdministrativeLifecycle::Keep,
                lease_guard: AdministrativeLeaseGuard::RejectActive,
                remove_field_prefixes: BTreeSet::new(),
            },
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![
                clause("a", "fresh"),
                clause("b", "x"),
            ]),
        };
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        let result = plan.proposal().administration.as_ref().unwrap();
        assert_eq!(
            result.histogram.outcomes[&AdministrativeOutcomeKind::Changed],
            1
        );
        assert_eq!(
            result.histogram.outcomes[&AdministrativeOutcomeKind::UniqueConflict],
            1
        );
        assert_eq!(plan.proposal().delta.work.len(), 1);
        assert_eq!(plan.proposal().delta.work[0].record.work.key, "a");
        drop(plan);
        let intent = AdministrativeIntent {
            operation: AdministrativeOperation::SelectFirst(vec![
                clause("a", "fresh"),
                clause("b", "fresh"),
            ]),
            ..intent
        };
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert!(plan.proposal().delta.work.is_empty());
        drop(plan);
        let mut prefix = clause("a", "x");
        if let AdministrativeAction::Update {
            patch,
            remove_field_prefixes,
            ..
        } = &mut prefix.action
        {
            remove_field_prefixes.insert("/attrs/".into());
            patch
                .fields
                .insert("/attrs/old".into(), Some(FieldValue::Text("new".into())));
            patch
                .fields
                .insert("/attrs/new".into(), Some(FieldValue::Integer(1)));
        }
        let intent = AdministrativeIntent {
            operation: AdministrativeOperation::SelectFirst(vec![prefix]),
            ..intent
        };
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        let mut fields = BTreeMap::from([
            ("/attrs/old".into(), FieldValue::Text("old".into())),
            ("value".into(), FieldValue::Text("x".into())),
        ]);
        plan.proposal().delta.work_fields["a"]
            .clone()
            .apply(&mut fields);
        assert_eq!(fields["/attrs/old"], FieldValue::Text("new".into()));
        assert_eq!(fields["/attrs/new"], FieldValue::Integer(1));
        assert_eq!(
            plan.proposal().delta.work_bodies["a"],
            ResolvedBody::Preserve
        );
        drop(plan);
        let mut body_intent = intent.clone();
        let AdministrativeOperation::SelectFirst(clauses) = &mut body_intent.operation else {
            unreachable!()
        };
        if let AdministrativeAction::Update { patch, .. } = &mut clauses[0].action {
            patch.body = Some(Default::default());
        }
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &body_intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(
            plan.proposal().delta.work_bodies["a"],
            ResolvedBody::Replace(Default::default())
        );
        assert!(
            plan.proposal().delta.work[0].body_loaded
                && plan.proposal().delta.work[0].record.work.body.is_empty()
        );
        drop(plan);
        let delete = AdministrativeClause {
            action: AdministrativeAction::Delete {
                lease_guard: AdministrativeLeaseGuard::InvalidateActive,
            },
            ..clause("c", "unused")
        };
        let intent = AdministrativeIntent {
            operation: AdministrativeOperation::SelectFirst(vec![delete]),
            ..intent
        };
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(plan.proposal().delta.deleted_work, vec!["c"]);
        assert!(!plan.initial_authority().unique_probes_loaded.is_empty());
        drop(plan);
        // All results above remain speculative: native selection itself writes
        // no records or receipts, even for an active-lease deletion proposal.
        let current=tx.query_one("SELECT active,version::text FROM fireweed_api007.records WHERE queue_id=$1 AND key=$2",&[&fenced.id,&b"c".as_slice()]).await.unwrap();
        assert!(current.get::<_, bool>(0));
        assert_eq!(
            current.get::<_, String>(1),
            claimed[0].record.version.to_string()
        );
        let mut selected = clause("a", "x");
        if let AdministrativeAction::Update { patch, .. } = &mut selected.action {
            patch.priority = Some(7);
        }
        let rekey_intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![selected]),
        };
        for administrative in [false, true] {
            let patch = WorkPatch {
                fields: BTreeMap::from([("value".into(), Some(FieldValue::Text("free".into())))]),
                ..Default::default()
            };
            let group = CommitGroup {
                administration: None,
                work_updates: vec![UnclaimedUpdate {
                    key: "c".into(),
                    expect: StateExpect::Version(claimed[0].record.version),
                    change: if administrative {
                        UnclaimedChange::Administrative {
                            patch,
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                        }
                    } else {
                        UnclaimedChange::Patch(patch)
                    },
                }],
                ..ordinary.clone()
            };
            let loaded = load_group(&tx, &fenced, &group, &job).await.unwrap();
            assert!(
                required_unique_probes(&group, &loaded.snapshot, 0, &fenced.schema)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                prospective_unique_probes(&group, &loaded.snapshot, &fenced.schema)
                    .unwrap()
                    .len(),
                2
            );
            drop(loaded);
            let rejected = plan_native_broad(
                &tx,
                &fenced,
                &group,
                &rekey_intent,
                || Ok(0),
                &QueueLimits::default(),
                &job,
            )
            .await
            .unwrap();
            assert_eq!(
                rejected.proposal().delta.receipt.outcome,
                GroupOutcome::Rejected(Rejection::Leased)
            );
            assert!(rejected.proposal().delta.work.is_empty());
            drop(rejected);
            let clocks = std::cell::Cell::new(0);
            let plan = plan_native_broad(
                &tx,
                &fenced,
                &group,
                &rekey_intent,
                || {
                    clocks.set(clocks.get() + 1);
                    Ok(1_000_000)
                },
                &QueueLimits::default(),
                &job,
            )
            .await
            .unwrap();
            assert_eq!(clocks.get(), 1);
            assert!(matches!(
                plan.proposal().delta.receipt.outcome,
                GroupOutcome::Committed { .. }
            ));
            assert_eq!(plan.proposal().delta.work.len(), 2);
            let rekeyed = plan
                .proposal()
                .delta
                .work
                .iter()
                .find(|row| row.record.work.key == "c")
                .unwrap();
            assert_eq!(
                rekeyed.record.work.fields["value"],
                FieldValue::Text("free".into())
            );
            assert_eq!(
                plan.proposal()
                    .administration
                    .as_ref()
                    .unwrap()
                    .histogram
                    .outcomes[&AdministrativeOutcomeKind::Changed],
                1
            );
            drop(plan);
        }
        let mut deletion = ordinary.clone();
        deletion.claimed_updates.push(ClaimedUpdate {
            key: "c".into(),
            lease: claimed[0].lease.clone(),
            expected_version: claimed[0].record.version,
            patch: WorkPatch::default(),
            disposition: Disposition::Delete,
        });
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &deletion,
            &rekey_intent,
            || Ok(0),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert!(matches!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(plan.proposal().delta.deleted_work, vec!["c"]);
        assert_eq!(plan.proposal().delta.work.len(), 1);
        assert!(
            plan.initial_authority()
                .unique_probes_loaded
                .iter()
                .any(|tuple| tuple.index == "unrelated_unique")
        );
        // Planning retained no physical scan IDs. Resolve only accepted outputs,
        // then exercise the actual writer without changing the surrounding fixture.
        tx.batch_execute("SAVEPOINT accepted_identity_fixture")
            .await
            .unwrap();
        let old_ids: Vec<i64> = tx.query("SELECT id FROM fireweed_api007.records WHERE queue_id=$1 AND collection=0 AND key=ANY($2) ORDER BY key", &[&fenced.id, &vec![b"a".to_vec(), b"c".to_vec()]]).await.unwrap().iter().map(|row| row.get(0)).collect();
        let mut accepted = load_delta_ids(
            &tx,
            &fenced,
            &plan.proposal().delta,
            plan.initial_authority(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(accepted.ids.len(), 2);
        assert_eq!(
            accepted.ids.values().copied().collect::<BTreeSet<_>>(),
            old_ids.iter().copied().collect()
        );
        assert!(accepted.snapshot.work.is_empty());
        persist_delta(
            &tx,
            &fenced,
            &plan.proposal().delta,
            plan.initial_authority(),
            &mut accepted,
        )
        .await
        .unwrap();
        let persisted = tx.query("SELECT id,key,active,version::text FROM fireweed_api007.records WHERE queue_id=$1 AND collection=0 AND key=ANY($2) ORDER BY key", &[&fenced.id, &vec![b"a".to_vec(), b"c".to_vec()]]).await.unwrap();
        assert_eq!(persisted.len(), 2);
        assert_eq!(
            persisted
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<Vec<_>>(),
            old_ids
        );
        assert!(persisted[0].get::<_, bool>(2));
        assert!(!persisted[1].get::<_, bool>(2));
        for row in &persisted {
            let key = String::from_utf8(row.get(1)).unwrap();
            let version = plan
                .proposal()
                .delta
                .versions
                .iter()
                .find(|version| version.key == key)
                .unwrap();
            assert_eq!(row.get::<_, String>(3), version.version.to_string());
        }
        // The inactive delete row remains the same durable identity. Reloading
        // predecessor IDs must include tombstones and must allocate no sequence.
        let current = RecordSnapshot {
            work: plan
                .proposal()
                .delta
                .work
                .iter()
                .map(|row| (row.record.work.key.clone(), row.clone()))
                .collect(),
            state: plan
                .proposal()
                .delta
                .state
                .iter()
                .map(|row| ((row.collection.clone(), row.key.clone()), row.clone()))
                .collect(),
            versions: plan
                .proposal()
                .delta
                .versions
                .iter()
                .map(|version| {
                    (
                        RecordIdentity {
                            collection: version.collection.clone(),
                            key: version.key.clone(),
                        },
                        version.version,
                    )
                })
                .collect(),
            ..Default::default()
        };
        let sequence_before: i64 = tx
            .query_one("SELECT last_value FROM fireweed_api007.records_id_seq", &[])
            .await
            .unwrap()
            .get(0);
        let tombstones = load_delta_ids(&tx, &fenced, &plan.proposal().delta, &current, &job)
            .await
            .unwrap();
        assert_eq!(tombstones.ids, accepted.ids);
        let wrong_absence = RecordSnapshot {
            versions: current.versions.clone(),
            ..Default::default()
        };
        assert!(matches!(
            load_delta_ids(&tx, &fenced, &plan.proposal().delta, &wrong_absence, &job).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let mut wrong_presence = current.clone();
        let mut deleted = plan.initial_authority().work["c"].clone();
        deleted.record.version = current.versions[&RecordIdentity {
            collection: None,
            key: "c".into(),
        }];
        wrong_presence.work.insert("c".into(), deleted);
        assert!(matches!(
            load_delta_ids(&tx, &fenced, &plan.proposal().delta, &wrong_presence, &job).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        // Chunk boundaries are transport batching, not a changed-identity cap.
        // The same key in a declared state collection must remain distinct.
        let mut chunked = plan.proposal().delta.clone();
        chunked.versions.extend((0..513).map(|index| RecordVersion {
            collection: None,
            key: format!("absent-{index}"),
            version: 1,
        }));
        chunked.versions.push(RecordVersion {
            collection: Some("instances".into()),
            key: "a".into(),
            version: 1,
        });
        let chunked_ids = load_delta_ids(&tx, &fenced, &chunked, &current, &job)
            .await
            .unwrap();
        assert_eq!(chunked_ids.ids, accepted.ids);
        drop(chunked_ids);
        let sequence_after: i64 = tx
            .query_one("SELECT last_value FROM fireweed_api007.records_id_seq", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(sequence_before, sequence_after);
        let mismatch = load_delta_ids(
            &tx,
            &fenced,
            &plan.proposal().delta,
            plan.initial_authority(),
            &job,
        )
        .await;
        assert!(matches!(mismatch, Err(QueueError::OutcomeUnknown(_))));
        drop(tombstones);
        drop(accepted);
        tx.batch_execute("ROLLBACK TO SAVEPOINT accepted_identity_fixture")
            .await
            .unwrap();
        drop(plan);
        let mut failing = operation(vec![WorkSelector::all(vec![])], 0);
        let AdministrativeOperation::SelectFirst(clauses) = &mut failing.operation else {
            unreachable!()
        };
        clauses[0].precondition = WorkSelector::all(vec![]);
        clauses[0].action = AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(7),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        };
        let base_usage = admission.usage().unwrap().1;
        SQL_FAILURE_AFTER.with(|failure| failure.set(Some((fenced.id, 2))));
        struct ResetFailure;
        impl Drop for ResetFailure {
            fn drop(&mut self) {
                SQL_FAILURE_AFTER.with(|failure| failure.set(None));
            }
        }
        let reset = ResetFailure;
        let clocks = std::cell::Cell::new(0);
        let error = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &failing,
            || {
                clocks.set(clocks.get() + 1);
                Ok(0)
            },
            &QueueLimits::default(),
            &job,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, QueueError::OutcomeUnknown(_)));
        assert_eq!(clocks.get(), 0);
        assert_eq!(admission.usage().unwrap().1, base_usage);
        drop(reset);
        tx.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn native_definition_pin_retains_original_legacy_and_framed_bytes() {
        let _fixture = physical_fixture().await;
        #[derive(Serialize)]
        struct OldDefinition<'a> {
            fields: &'a BTreeMap<String, FieldType>,
            indexes: &'a BTreeMap<String, Vec<String>>,
            unique_indexes: &'a BTreeSet<String>,
        }
        #[derive(Serialize)]
        struct OldSchema<'a> {
            work: OldDefinition<'a>,
            state: BTreeMap<String, OldDefinition<'a>>,
            progress_bound_nanos: u64,
            max_attempts: u32,
        }
        fn old(def: &CollectionDefinition) -> OldDefinition<'_> {
            OldDefinition {
                fields: &def.fields,
                indexes: &def.indexes,
                unique_indexes: &def.unique_indexes,
            }
        }
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let schema = fireweed_conformance::queue_contract::schema();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let queue = QueueAddress {
            tenant: "api007-native-definition-pin".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: schema.clone(),
            })
            .await
            .unwrap();
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let tx = client.transaction().await.unwrap();
        let framed = queue_row(&tx, &queue, &job).await.unwrap();
        let raw: Vec<u8> = tx
            .query_one(
                "SELECT definition FROM fireweed_api007.queues WHERE id=$1",
                &[&framed.id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            framed.definition_pin,
            DefinitionPin::from_persisted_bytes(&raw)
        );
        let legacy = postcard::to_allocvec(&OldSchema {
            work: old(&schema.work),
            state: schema
                .state
                .iter()
                .map(|(name, def)| (name.clone(), old(def)))
                .collect(),
            progress_bound_nanos: schema.progress_bound_nanos,
            max_attempts: schema.max_attempts,
        })
        .unwrap();
        assert_eq!(decode_definition(&legacy).unwrap(), schema);
        tx.execute(
            "UPDATE fireweed_api007.queues SET definition=$2 WHERE id=$1",
            &[&framed.id, &legacy],
        )
        .await
        .unwrap();
        let unframed = queue_row(&tx, &queue, &job).await.unwrap();
        assert_eq!(unframed.schema, framed.schema);
        assert_eq!(
            unframed.definition_pin,
            DefinitionPin::from_persisted_bytes(&legacy)
        );
        assert_ne!(unframed.definition_pin, framed.definition_pin);
        assert_ne!(
            unframed.definition_pin,
            DefinitionPin::from_persisted_bytes(&encode_definition(&unframed.schema).unwrap())
        );
        tx.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
    async fn native_fixture(collisions: bool) {
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let queue = QueueAddress {
            tenant: format!("api007-native-selectors-{collisions}"),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        let mut schema = fireweed_conformance::queue_contract::schema();
        schema.work.fields = BTreeMap::from([
            ("integer".into(), FieldType::Integer),
            ("float".into(), FieldType::Float),
            ("text".into(), FieldType::Text),
            ("bytes".into(), FieldType::Bytes),
            ("flag".into(), FieldType::Boolean),
        ]);
        schema.work.indexes.clear();
        schema.work.unique_indexes.clear();
        schema
            .work
            .additional_field_prefixes
            .insert("/attrs/".into());
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: schema.clone(),
            })
            .await
            .unwrap();
        let long_name = format!("/attrs/{}\0α", "x".repeat(80 << 10));
        let mut records = Vec::new();
        for n in 0..6 {
            let key = if n == 5 {
                "z\0α".into()
            } else {
                format!("row-{n}")
            };
            let fields = if n == 0 {
                Fields::new()
            } else {
                BTreeMap::from([
                    ("integer".into(), FieldValue::Integer(n - 3)),
                    (
                        "float".into(),
                        FieldValue::Float(if n == 1 { -0.0 } else { n as f64 }),
                    ),
                    (
                        "text".into(),
                        FieldValue::Text(if n == 2 {
                            "literal\0α".into()
                        } else {
                            format!("value-{n}")
                        }),
                    ),
                    ("bytes".into(), FieldValue::Bytes(vec![n as u8, 0, 255])),
                    ("flag".into(), FieldValue::Boolean(n % 2 == 0)),
                    (long_name.clone(), FieldValue::Integer(n)),
                    ("/attrs/name\0α".into(), FieldValue::Integer(n)),
                    ("/attrs/name\0β".into(), FieldValue::Integer(n + 10)),
                ])
            };
            let work = NewWork {
                key,
                fields,
                priority: 0,
                available_at: if n == 1 {
                    i64::MIN
                } else if n == 5 {
                    i64::MAX
                } else {
                    n - 3
                },
                body: if n == 0 {
                    vec![9; 1 << 20].into()
                } else {
                    Default::default()
                },
            };
            records.push(WorkRecord {
                work,
                version: 1,
                insertion_sequence: n as u64 + 1,
                lifecycle: lifecycle((n % 4) as i16).unwrap(),
                eligible_since: 0,
                attempts: n as u32,
                lease_expires_at: if n % 4 == 1 {
                    Some(if n == 1 { 10 } else { 11 })
                } else {
                    None
                },
            });
        }
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items: records.iter().map(|row| row.work.clone()).collect(),
            })
            .await
            .unwrap();
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let admission = QueueAdmission::new(1, 256 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let tx = client.transaction().await.unwrap();
        let mut fenced = queue_row(&tx, &queue, &job).await.unwrap();
        for record in &records {
            tx.execute("UPDATE fireweed_api007.records SET lifecycle=$3,attempts=$4,lease_until=$5,lease_hash=CASE WHEN $3::smallint=1::smallint THEN $6::bytea ELSE NULL END WHERE queue_id=$1 AND key=$2", &[&fenced.id,&record.work.key.as_bytes(),&life(record.lifecycle),&(record.attempts as i64),&record.lease_expires_at,&lease_hash(&[42;32]).to_vec()]).await.unwrap();
        }
        let identities: BTreeMap<i64,String> = tx.query("SELECT id,key FROM fireweed_api007.records WHERE queue_id=$1 AND collection=0 AND active", &[&fenced.id]).await.unwrap().into_iter().map(|row| (row.get(0), key(row.get(1)).unwrap())).collect();
        let leaf = WorkSelector::leaf;
        let mut selectors = vec![
            WorkSelector::all(vec![]),
            WorkSelector::any(vec![]),
            leaf(SelectorNode::Keys(BTreeSet::new())),
            leaf(SelectorNode::Keys(BTreeSet::from([
                "z\0α".into(),
                "missing".into(),
            ]))),
            leaf(SelectorNode::Present {
                name: "integer".into(),
                present: true,
            }),
            leaf(SelectorNode::Present {
                name: "/attrs/name\0α".into(),
                present: false,
            }),
            leaf(SelectorNode::StoredLifecycleIn(vec![])),
            leaf(SelectorNode::StoredLifecycleIn(vec![Lifecycle::Leased])),
            leaf(SelectorNode::EffectiveLifecycleIn(vec![Lifecycle::Pending])),
            leaf(SelectorNode::LeaseActive(true)),
            leaf(SelectorNode::LeaseActive(false)),
        ];
        selectors.push(leaf(SelectorNode::Present {
            name: long_name.clone(),
            present: true,
        }));
        selectors.push(leaf(SelectorNode::Field {
            name: long_name.clone(),
            predicate: Predicate::Range {
                lower: Some(Bound::Included(FieldValue::Float(0.0))),
                upper: None,
            },
        }));
        for comparison in [
            Comparison::Equal,
            Comparison::Before,
            Comparison::BeforeOrEqual,
            Comparison::After,
            Comparison::AfterOrEqual,
        ] {
            selectors.push(leaf(SelectorNode::Attempts {
                comparison,
                value: 2,
            }));
            selectors.push(leaf(SelectorNode::AvailableAt {
                comparison,
                value: i64::MIN,
            }));
            selectors.push(leaf(SelectorNode::AvailableAt {
                comparison,
                value: 0,
            }));
            selectors.push(leaf(SelectorNode::AvailableAt {
                comparison,
                value: i64::MAX,
            }));
        }
        for (name, value) in [
            ("integer", FieldValue::Integer(-1)),
            ("float", FieldValue::Float(0.0)),
            ("text", FieldValue::Text("literal\0α".into())),
            ("bytes", FieldValue::Bytes(vec![3, 0, 255])),
            ("flag", FieldValue::Boolean(true)),
            ("/attrs/name\0α", FieldValue::Integer(2)),
        ] {
            selectors.push(leaf(SelectorNode::Field {
                name: name.into(),
                predicate: Predicate::Equal(value.clone()),
            }));
            selectors.push(leaf(SelectorNode::Field {
                name: name.into(),
                predicate: Predicate::In(vec![value.clone()]),
            }));
            selectors.push(leaf(SelectorNode::Field {
                name: name.into(),
                predicate: Predicate::Range {
                    lower: Some(Bound::Included(value.clone())),
                    upper: None,
                },
            }));
            selectors.push(leaf(SelectorNode::Field {
                name: name.into(),
                predicate: Predicate::Range {
                    lower: None,
                    upper: Some(Bound::Excluded(value)),
                },
            }));
        }
        selectors.push(WorkSelector::negate(WorkSelector::all(vec![
            selectors[3].clone(),
            WorkSelector::any(vec![selectors[4].clone(), selectors[9].clone()]),
        ])));
        let invalid = operation(
            vec![leaf(SelectorNode::Field {
                name: "integer".into(),
                predicate: Predicate::In(vec![]),
            })],
            10,
        );
        let usage = admission.usage().unwrap();
        assert!(matches!(
            prepare_selection(&fenced, &invalid, &QueueLimits::default(), &job),
            Err(QueueError::Invalid(_))
        ));
        assert_eq!(admission.usage().unwrap(), usage);
        for time in [9, 10, 11] {
            for selector in &selectors {
                for candidates in [
                    vec![selector.clone()],
                    vec![selector.clone(), WorkSelector::all(vec![])],
                ] {
                    let intent = operation(candidates, time);
                    let prepared =
                        prepare_selection(&fenced, &intent, &QueueLimits::default(), &job).unwrap();
                    assert_eq!(prepared.params.len(), 3);
                    let actual: Vec<(String, usize)> = tx
                        .query(&prepared.sql, &refs(&prepared.params))
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|row| {
                            let id: i64 = row.get(0);
                            (identities[&id].clone(), row.get::<_, i32>(2) as usize)
                        })
                        .collect();
                    let mut expected: Vec<_> = records
                        .iter()
                        .filter_map(|record| {
                            intent
                                .first_clause(record)
                                .map(|owner| (record.work.key.clone(), owner))
                        })
                        .collect();
                    expected.sort();
                    assert_eq!(actual, expected, "operator time {time}; {intent:?}");
                }
            }
        }
        // Membership may contain more values than the protocol permits scalar
        // parameters. It remains one pooled byte-array parameter.
        let membership = operation(
            vec![leaf(SelectorNode::Field {
                name: "integer".into(),
                predicate: Predicate::In((-10000..30000).map(FieldValue::Integer).collect()),
            })],
            10,
        );
        let prepared =
            prepare_selection(&fenced, &membership, &QueueLimits::default(), &job).unwrap();
        assert_eq!(prepared.params.len(), 3);
        assert_eq!(
            tx.query(&prepared.sql, &refs(&prepared.params))
                .await
                .unwrap()
                .len(),
            5
        );
        drop(prepared);
        // A wide boolean expression stays balanced and uses three parameters.
        let wide = operation(
            vec![WorkSelector::any(
                (0..10000)
                    .map(|_| {
                        leaf(SelectorNode::Attempts {
                            comparison: Comparison::AfterOrEqual,
                            value: 0,
                        })
                    })
                    .collect(),
            )],
            10,
        );
        let prepared = prepare_selection(&fenced, &wide, &QueueLimits::default(), &job).unwrap();
        assert_eq!(prepared.params.len(), 3);
        assert_eq!(
            tx.query(&prepared.sql, &refs(&prepared.params))
                .await
                .unwrap()
                .len(),
            6
        );
        drop(prepared);
        let small = QueueAdmission::new(1, 8192).unwrap();
        let small_job = small.reserve(0).unwrap();
        assert!(matches!(
            prepare_selection(
                &fenced,
                &operation(vec![WorkSelector::all(vec![])], 10),
                &QueueLimits::default(),
                &small_job
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(small.usage().unwrap().1, 0);
        // Server selection is not limited by the ordinary addressed record cap.
        tx.execute("INSERT INTO fireweed_api007.records(queue_id,collection,key_hash,key,version,active,insertion_sequence) SELECT $1,0,CASE WHEN $2::boolean THEN decode(repeat('00',32),'hex') ELSE sha256(convert_to('bulk-'||lpad(n::text,5,'0'),'UTF8')) END,convert_to('bulk-'||lpad(n::text,5,'0'),'UTF8'),1,TRUE,(n+6)::numeric FROM generate_series(1,5001) n", &[&fenced.id,&collisions]).await.unwrap();
        let many = operation(
            vec![WorkSelector::all(vec![]), WorkSelector::all(vec![])],
            10,
        );
        let prepared = prepare_selection(&fenced, &many, &QueueLimits::default(), &job).unwrap();
        let matches = tx
            .query(&prepared.sql, &refs(&prepared.params))
            .await
            .unwrap();
        assert_eq!(matches.len(), 5007);
        assert!(matches.iter().all(|row| row.get::<_, i32>(2) == 0));
        drop(matches);
        drop(prepared);
        tx.execute(
            "UPDATE fireweed_api007.queues SET next_sequence=5007 WHERE id=$1",
            &[&fenced.id],
        )
        .await
        .unwrap();
        fenced.next_sequence = 5007;
        let ordinary = CommitGroup {
            administration: None,
            request_id: "native-noops".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let mut noops = operation(
            vec![WorkSelector::leaf(SelectorNode::Present {
                name: "integer".into(),
                present: false,
            })],
            10,
        );
        let AdministrativeOperation::SelectFirst(clauses) = &mut noops.operation else {
            unreachable!()
        };
        clauses[0].precondition = WorkSelector::all(vec![]);
        clauses[0].action = AdministrativeAction::Update {
            patch: WorkPatch::default(),
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        };
        let bulk_count = std::rc::Rc::new(std::cell::Cell::new(0));
        let usage = std::rc::Rc::new(std::cell::Cell::new(None));
        let count_hook = bulk_count.clone();
        let usage_hook = usage.clone();
        let admission_hook = admission.clone();
        ROW_BOUNDARY.with(|hook| {
            *hook.borrow_mut() = Some((
                fenced.id,
                Box::new(move |key| {
                    if key == "row-0" {
                        assert!(admission_hook.usage().unwrap().1 < 1 << 20);
                    }
                    if key.starts_with("bulk-") {
                        let index = count_hook.get();
                        count_hook.set(index + 1);
                        let current = admission_hook.usage().unwrap().1;
                        if index == 1 {
                            usage_hook.set(Some(current));
                        } else if index > 1 {
                            assert_eq!(usage_hook.get(), Some(current));
                        }
                    }
                }),
            ))
        });
        struct ResetBoundary;
        impl Drop for ResetBoundary {
            fn drop(&mut self) {
                ROW_BOUNDARY.with(|hook| *hook.borrow_mut() = None);
            }
        }
        let reset = ResetBoundary;
        let clock_calls = std::cell::Cell::new(0);
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &noops,
            || {
                clock_calls.set(clock_calls.get() + 1);
                assert_eq!(bulk_count.get(), 5001);
                Ok(10)
            },
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(clock_calls.get(), 1);
        assert_eq!(
            plan.proposal()
                .administration
                .as_ref()
                .unwrap()
                .histogram
                .matched,
            5002
        );
        assert!(plan.proposal().delta.work.is_empty() && plan.proposal().delta.versions.is_empty());
        assert!(
            plan.initial_authority().work.is_empty()
                && plan.initial_authority().unique_probes_loaded.is_empty()
        );
        drop(plan);
        drop(reset);
        // Exhaustion closes the cursor, so another operation in the same fence
        // can open it again without leaking selection state.
        let empty = operation(vec![], 10);
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &empty,
            || Ok(10),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(
            plan.proposal()
                .administration
                .as_ref()
                .unwrap()
                .histogram
                .matched,
            0
        );
        drop(plan);
        let mut expired_group = ordinary.clone();
        expired_group.request_id = "expires-during-selection".into();
        expired_group.claimed_updates.push(ClaimedUpdate {
            key: "row-1".into(),
            lease: LeaseCredential {
                token: vec![42; 32],
            },
            expected_version: 1,
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        });
        let mut control = noops.clone();
        let AdministrativeOperation::SelectFirst(clauses) = &mut control.operation else {
            unreachable!()
        };
        clauses[0].selector =
            WorkSelector::leaf(SelectorNode::Keys(BTreeSet::from(["row-0".into()])));
        if let AdministrativeAction::Update { patch, .. } = &mut clauses[0].action {
            patch.priority = Some(7);
        }
        let before_usage = admission.usage().unwrap().1;
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &ordinary,
            &control,
            || Ok(9),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(plan.proposal().delta.work.len(), 1);
        assert!(
            !plan.proposal().delta.work[0].body_loaded
                && plan.proposal().delta.work[0].record.work.body.is_empty()
        );
        assert_eq!(
            plan.proposal().delta.work_bodies["row-0"],
            ResolvedBody::Preserve
        );
        drop(plan);
        assert_eq!(admission.usage().unwrap().1, before_usage);
        let plan = plan_native_broad(
            &tx,
            &fenced,
            &expired_group,
            &control,
            || Ok(20),
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        assert_eq!(
            plan.proposal().delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::StaleLease)
        );
        assert!(plan.proposal().delta.work.is_empty() && plan.proposal().administration.is_none());
        drop(plan);
        assert_eq!(admission.usage().unwrap().1, before_usage);
        tx.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
}
