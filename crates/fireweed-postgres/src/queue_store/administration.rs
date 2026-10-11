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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "private native control qualification precedes activation"
    )
)]
struct NativeSelection<'r> {
    sql: String,
    params: Parameters,
    _charge: QueueBufferCharge<'r>,
}
/// Private preparation is physically qualified before adding public control
/// input, durable results and preview in one coherent activation.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "private native control qualification precedes activation"
    )
)]
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
    async fn native_fixture(collisions: bool) {
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
                body: Default::default(),
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
        let fenced = queue_row(&tx, &queue, &job).await.unwrap();
        for record in &records {
            tx.execute("UPDATE fireweed_api007.records SET lifecycle=$3,attempts=$4,lease_until=$5 WHERE queue_id=$1 AND key=$2", &[&fenced.id,&record.work.key.as_bytes(),&life(record.lifecycle),&(record.attempts as i64),&record.lease_expires_at]).await.unwrap();
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
        tx.execute("INSERT INTO fireweed_api007.records(queue_id,collection,key_hash,key,version,active,insertion_sequence) SELECT $1,0,decode(repeat('00',32),'hex'),convert_to('bulk-'||lpad(n::text,5,'0'),'UTF8'),1,TRUE,(n+6)::numeric FROM generate_series(1,5001) n", &[&fenced.id]).await.unwrap();
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
        tx.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
}
