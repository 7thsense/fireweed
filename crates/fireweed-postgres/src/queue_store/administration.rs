//! Private native selector preparation; no public administrative activation.
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
// Balanced SQL avoids turning a wide n-ary selector into a parser/planner's
// left-deep binary expression. Selector ownership and traversal remain flat.
fn balanced(mut expressions: Vec<String>, all: bool) -> String {
    if expressions.is_empty() {
        return if all { "TRUE" } else { "FALSE" }.into();
    }
    while expressions.len() > 1 {
        let mut next = Vec::with_capacity(expressions.len().div_ceil(2));
        let mut items = expressions.into_iter();
        while let Some(left) = items.next() {
            next.push(match items.next() {
                Some(right) => format!("({left} {} {right})", if all { "AND" } else { "OR" }),
                None => left,
            });
        }
        expressions = next;
    }
    expressions.pop().expect("one expression")
}
fn selector_sql(
    selector: &WorkSelector,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<String> {
    selector.validate(definition)?;
    let active = "(r.lifecycle=1 AND COALESCE(r.lease_until>$3::bigint,FALSE))";
    let mut stack = Vec::new();
    for node in &selector.nodes {
        let sql = match node {
            SelectorNode::All(arity) | SelectorNode::Any(arity) => {
                let start = stack
                    .len()
                    .checked_sub(*arity as usize)
                    .ok_or_else(|| storage("invalid selector arity"))?;
                balanced(stack.split_off(start), matches!(node, SelectorNode::All(_)))
            }
            SelectorNode::Not => format!(
                "(NOT {})",
                stack
                    .pop()
                    .ok_or_else(|| storage("invalid selector negation"))?
            ),
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
        };
        stack.push(sql);
    }
    if stack.len() != 1 {
        return Err(storage("invalid selector roots"));
    }
    Ok(stack.pop().expect("one root"))
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "private native provider qualification precedes public activation"
    )
)]
#[allow(clippy::too_many_arguments)]
async fn plan_native_broad<'r>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    ordinary: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    ordinary_image: &RecordSnapshot,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    limits: &QueueLimits,
    reservation: &'r QueueReservation,
) -> QueueResult<fireweed_engine::queue_administration::AdmittedAdministrativeGroupProposal<'r>> {
    use fireweed_core::queue_administration::AdministrativeOperation;
    if ordinary_image.next_sequence != queue.next_sequence {
        return Err(storage(
            "native ordinary authority has a different insertion sequence",
        ));
    }
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
        let mut group = CommitGroup {
            request_id: String::new(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: record_key.clone(),
                expect: StateExpect::Version(highwater),
                change: match action {
                    fireweed_core::queue_administration::AdministrativeAction::Delete {
                        ..
                    } => UnclaimedChange::Delete,
                    fireweed_core::queue_administration::AdministrativeAction::Update {
                        patch,
                        lifecycle,
                        lease_guard,
                        ..
                    } => UnclaimedChange::Administrative {
                        patch: patch.clone(),
                        lifecycle: *lifecycle,
                        lease_guard: *lease_guard,
                    },
                },
            }],
        };
        // Full field authority for ONE server-selected row; opaque bodies are
        // loaded only for an explicit comparison. Selective field hydration is
        // a later performance gate, not a whole-queue/client filtering fallback.
        let mut loaded = load_group_with_fields(tx, queue, &group, reservation, None).await?;
        if loaded.snapshot.work.len() != 1 || loaded.snapshot.versions.len() != 1 {
            return Err(storage("native selected row disappeared"));
        }
        if let fireweed_core::queue_administration::AdministrativeAction::Update {
            patch,
            remove_field_prefixes,
            ..
        } = action
        {
            let target = &loaded.snapshot.work[&record_key];
            let view = CheckedFields::new(&target.record.work.fields, &target.field_coverage)?;
            let resolved = fireweed_engine::queue_fields::resolve_edits(
                &view,
                &patch.fields,
                remove_field_prefixes,
                &queue.schema.work,
            )?;
            let UnclaimedChange::Administrative { patch, .. } = &mut group.work_updates[0].change
            else {
                unreachable!("update proxy")
            };
            patch.fields = match resolved {
                ResolvedFields::Preserve => Default::default(),
                ResolvedFields::Edit(edits) => edits,
                ResolvedFields::Replace(_) => {
                    return Err(storage(
                        "administrative prefix unexpectedly replaced fields",
                    ));
                }
            };
            // The shared probe planner clones before/after identities across
            // indexes. Admit that transient amplification before generating any
            // tuples; retained owner coverage is charged by the native loader.
            let _planning = reservation.buffer(
                group_expansion_budget(&group, &loaded.snapshot, &queue.schema)?
                    .checked_mul(8)
                    .ok_or(QueueError::TooLarge)?,
            )?;
            load_unique_probes(
                tx,
                queue,
                &mut loaded,
                &group,
                intent.evaluated_at,
                reservation,
            )
            .await?;
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
            let target = &loaded.snapshot.work[&record_key];
            let probes = insert_unique_probes(
                std::slice::from_ref(&target.record.work),
                &loaded.snapshot,
                &queue.schema,
            )?
            .into_iter()
            .collect();
            load_unique_authority(tx, queue, &mut loaded, probes, reservation).await?;
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
    fireweed_engine::queue_administration::plan_broad_group_stream(
        ordinary,
        intent,
        source,
        ordinary_image,
        ownership_now,
        &queue.schema,
        limits,
        reservation,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_administration::*;
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
        schema.work.fields = BTreeMap::from([("value".into(), FieldType::Text)]);
        schema.work.indexes = BTreeMap::from([("unique".into(), vec!["value".into()])]);
        schema.work.unique_indexes = BTreeSet::from(["unique".into()]);
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
            request_id: "control".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let before = RecordSnapshot {
            next_sequence: fenced.next_sequence,
            ..Default::default()
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
            &before,
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
            &before,
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
            &before,
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
            &before,
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
            &before,
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
            &before,
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
            request_id: "native-noops".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let before = RecordSnapshot {
            next_sequence: 5007,
            ..Default::default()
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
            &before,
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
            &before,
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
        let loaded = load_group(&tx, &fenced, &expired_group, &job)
            .await
            .unwrap();
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
            &before,
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
            &loaded.snapshot,
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
