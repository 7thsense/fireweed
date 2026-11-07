//! Bounded SQL correspondence over synthetic index rows, not a proof that cached
//! rows came from the source log. The oracle includes the exact digest/full-key
//! relation; mismatched digests do not establish logical uniqueness authority.
//! The probe checks one unique entry; applying a record probes each entry it wrote.
use super::*;

#[derive(Clone)]
struct IndexRow {
    record: i64,
    queue: i64,
    collection: i64,
    index: i64,
    hash: Vec<u8>,
    full: Vec<u8>,
    unique: bool,
}
fn conflict(rows: &[IndexRow], queue: i64) -> bool {
    rows.iter().any(|left| {
        left.queue == queue
            && left.unique
            && rows.iter().any(|right| {
                right.unique
                    && left.record < right.record
                    && (
                        left.queue,
                        left.collection,
                        left.index,
                        &left.hash,
                        &left.full,
                    ) == (
                        right.queue,
                        right.collection,
                        right.index,
                        &right.hash,
                        &right.full,
                    )
            })
    })
}
async fn insert(connection: &Connection, row: &IndexRow) {
    connection.execute(
        "INSERT INTO q7_indexes(record_id,queue_id,collection,index_no,prefix,hash,full_key,is_unique) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        vec![Value::Integer(row.record),Value::Integer(row.queue),Value::Integer(row.collection),Value::Integer(row.index),blob(&row.full),blob(&row.hash),blob(&row.full),Value::Integer(i64::from(row.unique))],
    ).await.unwrap();
}
/// Rows that own `row`'s unique position in `queue`, as the apply path counts them.
async fn owners(connection: &Connection, sql: &str, row: &IndexRow, queue: i64) -> usize {
    let mut rows = connection
        .query(
            sql,
            vec![
                Value::Integer(queue),
                Value::Integer(row.collection),
                Value::Integer(row.index),
                blob(&row.hash),
                blob(&row.full),
            ],
        )
        .await
        .unwrap();
    let mut count = 0;
    while rows.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}
async fn plan(connection: &Connection, sql: &str) -> Vec<String> {
    let mut rows = connection
        .query(
            format!("EXPLAIN QUERY PLAN {sql}"),
            vec![
                Value::Integer(101),
                Value::Integer(0),
                Value::Integer(0),
                blob(b"hash"),
                blob(b"full"),
            ],
        )
        .await
        .unwrap();
    let mut output = vec![];
    while let Some(row) = rows.next().await.unwrap() {
        output.push(row.get::<String>(3).unwrap());
    }
    output
}
fn hash_bounded(plan: &[String]) -> bool {
    plan.iter().any(|step| {
        step.contains("SEARCH q7_indexes USING INDEX q7_index_equal")
            && step.contains("queue_id=?")
            && step.contains("hash=?")
    })
}

#[tokio::test]
async fn unique_probe_seeks_the_equal_index_by_digest_instead_of_scanning_the_queue() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("plan.db"), &job, &workers).await;
    tokio::task::spawn_blocking({
        let projection = projection.clone();
        let job = job.clone();
        move || {
            crate::tx::block_on_owned_apply(async {
                let _containers = job.owned_buffer(512 << 10).unwrap();
                let mut writer = projection.writer.lock().await;
                let transaction = writer
                    .as_mut()
                    .unwrap()
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await
                    .unwrap();
                let connection: &Connection = &transaction;
                for record in 1i64..=512 {
                    let full = ordered_key(&[Some(&FieldValue::Integer(record))]).unwrap();
                    insert(
                        connection,
                        &IndexRow {
                            record,
                            queue: 101,
                            collection: 0,
                            index: 0,
                            hash: discovery_digest(&full).to_vec(),
                            full,
                            unique: true,
                        },
                    )
                    .await;
                }
                let production = plan(connection, UNIQUE_PROBE_SQL).await;
                assert!(hash_bounded(&production), "{production:?}");
                assert!(
                    production.iter().all(|step| !step.contains("SCAN")),
                    "{production:?}"
                );
                let wrong = UNIQUE_PROBE_SQL
                    .replace("INDEXED BY q7_index_equal", "INDEXED BY q7_index_order");
                assert!(
                    !hash_bounded(&plan(connection, &wrong).await),
                    "an index name alone is insufficient"
                );
                transaction.commit().await.unwrap();
            })
        }
    })
    .await
    .unwrap();
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn unique_probe_matches_independent_pair_oracle_and_predicate_mutants() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("oracle.db"), &job, &workers).await;
    tokio::task::spawn_blocking({
        let projection = projection.clone();
        let job = job.clone();
        move || {
            crate::tx::block_on_owned_apply(async {
                let _containers = job.owned_buffer(64 << 10).unwrap();
                let mut writer = projection.writer.lock().await;
                let transaction = writer
                    .as_mut()
                    .unwrap()
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await
                    .unwrap();
                let connection: &Connection = &transaction;
                // Each predicate replaced by a tautology of the same parameter.
                let predicates = [
                    ("queue_id=?1", "?1=?1"),
                    ("collection=?2", "?2=?2"),
                    ("index_no=?3", "?3=?3"),
                    ("hash=?4", "?4=?4"),
                    ("is_unique=1", "1=1"),
                    ("full_key=?5", "?5=?5"),
                ];
                let mutants = predicates.map(|(predicate, tautology)| {
                    assert_eq!(UNIQUE_PROBE_SQL.matches(predicate).count(), 1);
                    UNIQUE_PROBE_SQL.replace(predicate, tautology)
                });
                let mut detected = [false; 6];
                let left = IndexRow {
                    record: 11,
                    queue: 101,
                    collection: 0,
                    index: 0,
                    hash: vec![9],
                    full: b"same".to_vec(),
                    unique: true,
                };
                assert_eq!(
                    owners(connection, UNIQUE_PROBE_SQL, &left, 101).await,
                    0,
                    "zero rows"
                );
                insert(connection, &left).await;
                assert_eq!(
                    owners(connection, UNIQUE_PROBE_SQL, &left, 101).await,
                    1,
                    "one row cannot conflict with itself"
                );
                // The apply path probes the position a record wrote. All seven
                // independent equality/scope/uniqueness flags: 128 complete two-row
                // cases, including collisions and each one-sided nonunique row.
                for flags in 0u8..128 {
                    connection
                        .execute("DELETE FROM q7_indexes", ())
                        .await
                        .unwrap();
                    let mut left = left.clone();
                    left.unique = flags & 32 != 0;
                    let right = IndexRow {
                        record: 22,
                        queue: if flags & 1 != 0 { 101 } else { 202 },
                        collection: if flags & 2 != 0 { 0 } else { 1 },
                        index: if flags & 4 != 0 { 0 } else { 1 },
                        hash: if flags & 8 != 0 { vec![9] } else { vec![10] },
                        full: if flags & 16 != 0 {
                            b"same".to_vec()
                        } else {
                            b"other".to_vec()
                        },
                        unique: flags & 64 != 0,
                    };
                    insert(connection, &left).await;
                    insert(connection, &right).await;
                    let expected = conflict(&[left.clone(), right], 101);
                    // A touched entry conflicts only when the probe finds two owners.
                    let probe = |count: usize| count > 1;
                    assert_eq!(
                        probe(owners(connection, UNIQUE_PROBE_SQL, &left, 101).await),
                        expected,
                        "flags {flags}"
                    );
                    for (index, sql) in mutants.iter().enumerate() {
                        // Every mutant executes successfully; parser failure is not a
                        // counterexample to the independent data/predicate oracle.
                        detected[index] |=
                            probe(owners(connection, sql, &left, 101).await) != expected;
                    }
                }
                assert!(
                    detected.into_iter().all(|found| found),
                    "every weakened predicate must be detected"
                );
                transaction.commit().await.unwrap();
            })
        }
    })
    .await
    .unwrap();
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn missing_equality_index_rolls_back_effects_and_reopen_restores_the_checked_path() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing-index.db");
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) = bootstrap(&path, &job, &workers).await;
    let snapshot = seed(&projection, &definition, &job, &workers, false).await;
    let mut change = group("missing-equality");
    // The change writes a unique entry, so the apply path must probe it.
    change
        .work_updates
        .push(patch("a", 1, Some(17), Some(Some(5))));
    execute_for_test(&projection, "DROP INDEX q7_index_equal", vec![]).await;
    assert!(
        matches!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot.clone(),
                &change,
                2,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ),
        "missing required index must never fall back to an unbounded join"
    );
    let row = inspect(&projection, "SELECT (SELECT version FROM q7_records WHERE key=?1),(SELECT priority FROM q7_records WHERE key=?1),applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=2 AND identity=?2) FROM q7_queues", vec![blob(b"a"),blob(change.request_id.as_bytes())],4).await;
    assert_eq!(unsigned(&row[0]).unwrap(), 1);
    assert_eq!(integer(&row[1]).unwrap(), 0);
    assert_eq!(integer(&row[2]).unwrap(), 2);
    assert_eq!(integer(&row[3]).unwrap(), 0);
    drop(projection);
    // Projection open executes the existing idempotent DDL. No durable log or
    // definition format migration is required to recreate this physical index.
    let projection = QueueProjection::open_keeping_files(&path, &job, &workers)
        .await
        .unwrap();
    assert_eq!(
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot.clone(),
            &change,
            2,
            &job,
            &workers
        )
        .await
        .unwrap()
        .next_offset,
        3
    );
    let row = inspect(
        &projection,
        "SELECT version,priority FROM q7_records WHERE key=?1",
        vec![blob(b"a")],
        2,
    )
    .await;
    assert_eq!(unsigned(&row[0]).unwrap(), 2);
    assert_eq!(integer(&row[1]).unwrap(), 17);
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn forged_duplicate_unique_key_is_refused_and_a_swap_in_one_record_passes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("forged.db");
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) = bootstrap(&path, &job, &workers).await;
    let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
    // The planner never saw this: the record sets a's unique value to b's.
    let mut forged = group("forged-duplicate");
    forged.work_updates = vec![patch("a", 1, None, Some(Some(2)))];
    let before = inspect(
        &projection,
        "SELECT applied_next_offset FROM q7_queues",
        vec![],
        1,
    )
    .await;
    // Planning would reject this: forget who owns the value so it passes.
    let mut forgetful = snapshot.clone();
    authorize(&mut forgetful, &forged, &schema());
    forgetful.unique_owners.remove(&UniqueIdentity {
        collection: None,
        index: "u".into(),
        full_sort_key: ordered_key(&[Some(&FieldValue::Integer(2))]).unwrap(),
    });
    let before_plan = forgetful.clone();
    let delta = plan_commit_group(&forged, &mut forgetful, 20, &schema()).unwrap();
    let queue = address();
    let receipt = PublicationReceipt::commit(&queue, &forged, &delta.receipt).unwrap();
    let record = publication(&before_plan, &delta, &[receipt], &definition, &job);
    let refused = projection.apply(record, 2, &job, &workers).await;
    assert!(
        matches!(&refused, Err(QueueError::OutcomeUnknown(reason)) if reason.contains("final projection unique ownership conflict")),
        "{:?}",
        refused.as_ref().err()
    );
    let after = inspect(
        &projection,
        "SELECT applied_next_offset FROM q7_queues",
        vec![],
        1,
    )
    .await;
    assert_eq!(integer(&before[0]).unwrap(), integer(&after[0]).unwrap());
    // The same values exchanged inside one record are a valid final state.
    let mut swap = group("swap-in-one-record");
    swap.work_updates = vec![
        patch("a", 1, None, Some(Some(2))),
        patch("b", 1, None, Some(Some(1))),
    ];
    assert_eq!(
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &swap,
            2,
            &job,
            &workers
        )
        .await
        .unwrap()
        .next_offset,
        3
    );
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn unique_owner_lookups_seek_the_equal_index_by_digest() {
    use super::super::unique_authority::{UNIQUE_OWNER_KEY_SQL, UNIQUE_OWNER_METADATA_SQL};
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("owner-plan.db"), &job, &workers).await;
    tokio::task::spawn_blocking({
        let projection = projection.clone();
        move || {
            crate::tx::block_on_owned_apply(async {
                let writer = projection.writer.lock().await;
                let connection = writer.as_ref().unwrap();
                for sql in [UNIQUE_OWNER_METADATA_SQL, UNIQUE_OWNER_KEY_SQL] {
                    let production = plan(connection, sql).await;
                    assert!(
                        production
                            .iter()
                            .any(|step| step.contains("SEARCH i USING INDEX q7_index_equal")
                                && step.contains("hash=?")),
                        "{production:?}"
                    );
                    let unhinted = sql.replace(" INDEXED BY q7_index_equal", "");
                    assert!(
                        !plan(connection, &unhinted)
                            .await
                            .iter()
                            .any(|step| step.contains("q7_index_equal") && step.contains("hash=?")),
                        "unhinted control must reproduce the optimizer regression"
                    );
                }
            })
        }
    })
    .await
    .unwrap();
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
