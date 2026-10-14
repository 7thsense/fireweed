//! Bounded SQL correspondence over synthetic index rows, not a proof that cached
//! rows came from the source log. The oracle includes the exact digest/full-key
//! relation; mismatched digests do not establish logical uniqueness authority.
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
async fn existence(connection: &Connection, sql: &str, queue: i64) -> bool {
    // LIMIT1 is unordered; only conflict existence belongs to the contract.
    one(connection, sql, vec![Value::Integer(queue)], 1)
        .await
        .unwrap()
        .is_some()
}
async fn plan(connection: &Connection, sql: &str) -> Vec<String> {
    let mut rows = connection
        .query(format!("EXPLAIN QUERY PLAN {sql}"), [101])
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
        step.contains("SEARCH b USING INDEX q7_index_equal")
            && step.contains("hash=?")
            && step.contains("record_id>?")
    })
}

#[tokio::test]
async fn final_unique_check_seeks_digest_and_record_range_instead_of_domain_scan() {
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
                let production = plan(connection, FINAL_UNIQUE_OWNERSHIP_SQL).await;
                assert!(hash_bounded(&production), "{production:?}");
                assert!(!existence(connection, FINAL_UNIQUE_OWNERSHIP_SQL, 101).await);
                let unhinted = FINAL_UNIQUE_OWNERSHIP_SQL.replace(" INDEXED BY q7_index_equal", "");
                assert!(
                    !hash_bounded(&plan(connection, &unhinted).await),
                    "unhinted control must reproduce the pinned optimizer regression"
                );
                let wrong = FINAL_UNIQUE_OWNERSHIP_SQL
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
async fn final_unique_check_matches_independent_pair_oracle_and_predicate_mutants() {
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
                let removed = [
                    "a.queue_id=b.queue_id AND ",
                    "a.collection=b.collection AND ",
                    "a.index_no=b.index_no AND ",
                    "a.hash=b.hash AND ",
                    "a.full_key=b.full_key AND ",
                    " AND a.is_unique=1",
                    " AND b.is_unique=1",
                    " AND a.record_id<b.record_id",
                ];
                let mutants = removed.map(|predicate| {
                    assert_eq!(FINAL_UNIQUE_OWNERSHIP_SQL.matches(predicate).count(), 1);
                    FINAL_UNIQUE_OWNERSHIP_SQL.replace(predicate, "")
                });
                let mut mutants = mutants.to_vec();
                assert_eq!(
                    FINAL_UNIQUE_OWNERSHIP_SQL.matches("a.queue_id=?1").count(),
                    1
                );
                mutants.push(FINAL_UNIQUE_OWNERSHIP_SQL.replace("a.queue_id=?1", "?1=?1"));
                let mut detected = [false; 9];
                let left = IndexRow {
                    record: 11,
                    queue: 101,
                    collection: 0,
                    index: 0,
                    hash: vec![9],
                    full: b"same".to_vec(),
                    unique: true,
                };
                assert!(
                    !existence(connection, FINAL_UNIQUE_OWNERSHIP_SQL, 101).await,
                    "zero rows"
                );
                insert(connection, &left).await;
                assert!(
                    !existence(connection, FINAL_UNIQUE_OWNERSHIP_SQL, 101).await,
                    "one row cannot conflict with itself"
                );
                // All seven independent equality/scope/uniqueness flags: 128 complete
                // two-row cases, including collisions and each one-sided nonunique row.
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
                    let expected = conflict(&[left, right], 101);
                    assert_eq!(
                        existence(connection, FINAL_UNIQUE_OWNERSHIP_SQL, 101).await,
                        expected,
                        "flags {flags}"
                    );
                    for (index, sql) in mutants.iter().enumerate() {
                        // Every mutant executes successfully; parser failure is not a
                        // counterexample to the independent data/predicate oracle.
                        detected[index] |= existence(connection, sql, 101).await != expected;
                    }
                }
                connection
                    .execute("DELETE FROM q7_indexes", ())
                    .await
                    .unwrap();
                let mut other_left = left.clone();
                other_left.queue = 202;
                let mut other_right = other_left.clone();
                other_right.record = 22;
                insert(connection, &other_left).await;
                insert(connection, &other_right).await;
                for queue in [101, 202] {
                    let expected = conflict(&[other_left.clone(), other_right.clone()], queue);
                    assert_eq!(
                        existence(connection, FINAL_UNIQUE_OWNERSHIP_SQL, queue).await,
                        expected,
                        "requested queue {queue}"
                    );
                    for (index, sql) in mutants.iter().enumerate() {
                        detected[index] |= existence(connection, sql, queue).await != expected;
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
    change.work_updates.push(patch("a", 1, Some(17), None));
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
    let projection = QueueProjection::open(&path, &job, &workers).await.unwrap();
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
