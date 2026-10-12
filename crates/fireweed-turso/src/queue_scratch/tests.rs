use super::*;
use fireweed_engine::queue_admission::QueueAdmission;
use fireweed_queue_spool::ScratchWorkers;

fn forbidden(opcode: &str) -> bool {
    opcode.starts_with("RowSet")
        || opcode.starts_with("Sorter")
        || opcode.starts_with("Hash")
        || matches!(
            opcode,
            "OpenEphemeral" | "OpenAutoindex" | "Filter" | "FilterAdd"
        )
}
async fn opcodes(
    connection: &Connection,
    sql: &str,
    params: Vec<Value>,
) -> QueueResult<Vec<String>> {
    let mut rows = connection
        .query(&format!("EXPLAIN {sql}"), params_from_iter(params))
        .await
        .map_err(|_| unknown())?;
    let mut result = vec![];
    while let Some(row) = rows.next().await.map_err(|_| unknown())? {
        let Value::Text(opcode) = row.get_value(1).map_err(|_| unknown())? else {
            return Err(unknown());
        };
        result.push(opcode);
    }
    Ok(result)
}
#[tokio::test]
async fn stock_sdk_fixed_statements_use_indexed_sets_without_hidden_materialization() {
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    workers
        .run({
            let root = root.clone();
            let quota = quota.clone();
            let job = job.clone();
            move || {
                crate::tx::block_on_owned_apply(async move {
                    let mut db = NativeScratchDatabase::open(root, quota, &job).await?;
                    for (sql, params) in [
                        (INSERT_RECORD, vec![Value::Integer(1), Value::Blob(vec![1]), Value::Blob(1u64.to_be_bytes().to_vec())]),
                        (CHECK_RECORD, vec![Value::Integer(1), Value::Blob(vec![1]), Value::Blob(1u64.to_be_bytes().to_vec())]),
                        (FIND_RECORD, vec![Value::Integer(1)]),
                        (FIND_OWNER, vec![Value::Integer(1)]),
                        ("PRAGMA wal_checkpoint(TRUNCATE)", vec![]),
                        (INSERT_SET, vec![Value::Integer(0), Value::Integer(1)]),
                        (INSERT_EDGE, vec![Value::Integer(3), Value::Integer(0)]),
                        (
                            UNION,
                            vec![Value::Integer(2), Value::Integer(1), Value::Integer(0)],
                        ),
                        (INTERSECTION, vec![Value::Integer(3), Value::Integer(1)]),
                        (
                            COMPLEMENT,
                            vec![Value::Integer(4), Value::Integer(1), Value::Integer(0)],
                        ),
                        (INSERT_OWNER, vec![Value::Integer(1), Value::Integer(0)]),
                        (NEXT_ACCUMULATOR, vec![Value::Integer(0), Value::Integer(0)]),
                        (NEXT_SET, vec![Value::Integer(0), Value::Integer(0)]),
                        (NEXT_OWNER, vec![Value::Blob(vec![])]),
                        (READ_OWNER_KEY, vec![Value::Integer(1)]),
                    ] {
                        let instructions = opcodes(&db.connection, sql, params).await?;
                        assert!(!instructions.is_empty());
                        assert!(!instructions.iter().any(|opcode| forbidden(opcode)), "unqualified plan for {sql}: {instructions:?}");
                    }
                    // Real negative controls: these former shapes create hidden
                    // target collections even with otherwise indexed predicates.
                    let self_read = opcodes(&db.connection,
                        "INSERT OR IGNORE INTO sets(node,record) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM sets WHERE node=?3 AND record=?2)",
                        vec![Value::Integer(2),Value::Integer(1),Value::Integer(0)]).await?;
                    assert!(self_read.iter().any(|op| op=="OpenEphemeral"));
                    let delete = opcodes(&db.connection,
                        "DELETE FROM sets WHERE EXISTS(SELECT 1 FROM edges WHERE child=sets.node)",vec![]).await?;
                    assert!(delete.iter().any(|op| op.starts_with("RowSet")));
                    // No explicit BEGIN/savepoint is exposed. In stock 0.8.2,
                    // vdbe::ProgramState::begin_statement opens its MemoryIO subjournal
                    // only for write && !connection.auto_commit. Check every statement
                    // at both ends; EXPLAIN does not expose this hidden flag.
                    db.edge(3, 0).await?;
                    db.edge(3, 1).await?;
                    for id in 1..=5001 {
                        db.record(id, &id.to_be_bytes(), 1).await?;
                        if id % 2 == 0 {
                            db.leaf(0, id).await?;
                        }
                        if id % 3 == 0 {
                            db.leaf(1, id).await?;
                        }
                        db.union_child(2, id, 0).await?;
                        db.union_child(2, id, 1).await?;
                        db.intersection(3, id).await?;
                        db.complement(4, id, 0).await?;
                        db.intersection(5, id).await?; // Empty All is the active universe.
                    }
                    for node in 2..=5 {
                        db.finish_node(node).await?;
                    }
                    for (node, expected) in [
                        (
                            2,
                            (1..=5001)
                                .filter(|n| n % 2 == 0 || n % 3 == 0)
                                .collect::<Vec<_>>(),
                        ),
                        (3, (1..=5001).filter(|n| n % 6 == 0).collect()),
                        (4, (1..=5001).filter(|n| n % 2 != 0).collect()),
                        (5, (1..=5001).collect()),
                    ] {
                        let mut actual = vec![];
                        let mut after = 0;
                        while let Some(id) = db.next_set(node, after).await? {
                            actual.push(id);
                            after = id;
                        }
                        assert_eq!(actual, expected);
                    }
                    db.owner(4, 0).await?;
                    db.owner(4, 1).await?;
                    db.owner(2, 1).await?;
                    let row = db.next_owner(&[]).await?.unwrap();
                    assert_eq!(
                        (row.id(), row.clause(), row.key()),
                        (2, 1, 2i64.to_be_bytes().as_slice())
                    );
                    drop(row);
                    let row = db.next_owner(&2i64.to_be_bytes()).await?.unwrap();
                    assert_eq!(
                        (row.id(), row.clause(), row.key()),
                        (4, 0, 4i64.to_be_bytes().as_slice())
                    );
                    drop(row);
                    assert!(db.next_owner(&4i64.to_be_bytes()).await?.is_none());
                    Ok(())
                })
            }
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn same_virtual_path_has_independent_sdk_owners_without_registry_retention() {
    use turso_core::IO;
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    workers
        .run({
            let quota = quota.clone();
            let job = job.clone();
            move || {
                crate::tx::block_on_owned_apply(async move {
                    let _headers = job.owned_buffer(16 << 20)?;
                    let first_io =
                        ScratchIo::new(root.clone(), quota.clone(), "/private-sdk-same".into())
                            .map_err(|_| unknown())?;
                    let second_io =
                        ScratchIo::new(root.clone(), quota.clone(), "/private-sdk-same".into())
                            .map_err(|_| unknown())?;
                    assert!(first_io.file_id("/private-sdk-same/main.db").is_err());
                    let first = Builder::new_local("/private-sdk-same/main.db")
                        .with_io_impl(first_io.clone())
                        .build()
                        .await
                        .map_err(|_| unknown())?;
                    let second = Builder::new_local("/private-sdk-same/main.db")
                        .with_io_impl(second_io.clone())
                        .build()
                        .await
                        .map_err(|_| unknown())?;
                    let a = first.connect().map_err(|_| unknown())?;
                    let b = second.connect().map_err(|_| unknown())?;
                    a.execute("CREATE TABLE isolated(value INTEGER)", ())
                        .await
                        .map_err(|_| unknown())?;
                    b.execute("CREATE TABLE isolated(value INTEGER)", ())
                        .await
                        .map_err(|_| unknown())?;
                    a.execute("INSERT INTO isolated VALUES(7)", ())
                        .await
                        .map_err(|_| unknown())?;
                    b.execute("INSERT INTO isolated VALUES(11)", ())
                        .await
                        .map_err(|_| unknown())?;
                    for (connection, expected) in [(&a, 7), (&b, 11)] {
                        let mut rows = connection
                            .query("SELECT value FROM isolated", ())
                            .await
                            .map_err(|_| unknown())?;
                        assert_eq!(
                            rows.next()
                                .await
                                .map_err(|_| unknown())?
                                .unwrap()
                                .get_value(0)
                                .unwrap(),
                            Value::Integer(expected)
                        );
                        assert!(rows.next().await.map_err(|_| unknown())?.is_none());
                    }
                    let first_weak = Arc::downgrade(&first_io);
                    let second_weak = Arc::downgrade(&second_io);
                    drop(a);
                    drop(b);
                    drop(first);
                    drop(second);
                    drop(first_io);
                    drop(second_io);
                    assert!(first_weak.upgrade().is_none());
                    assert!(second_weak.upgrade().is_none());
                    assert_eq!(quota.usage()?, 0);
                    for _ in 0..128 {
                        let db =
                            NativeScratchDatabase::open(root.clone(), quota.clone(), &job).await?;
                        let weak = Arc::downgrade(&db._io);
                        drop(db);
                        assert!(weak.upgrade().is_none());
                        assert_eq!(quota.usage()?, 0);
                    }
                    Ok(())
                })
            }
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn selected_key_retains_job_after_sdk_files_and_worker_drain() {
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let (terminal_tx, mut terminal_rx) = tokio::sync::oneshot::channel();
    let job = Arc::new(
        admission
            .reserve(0)
            .unwrap()
            .with_terminal_callback(move || {
                let _ = terminal_tx.send(());
            })
            .unwrap(),
    );
    let row = workers
        .run({
            let quota = quota.clone();
            let job = job.clone();
            move || {
                crate::tx::block_on_owned_apply(async move {
                    let mut db = NativeScratchDatabase::open(root, quota, &job).await?;
                    db.record(1, b"admitted-key", 1).await?;
                    db.owner(1, 0).await?;
                    let row = db.next_owner(&[]).await?.unwrap();
                    drop(db);
                    Ok(row)
                })
            }
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(admission.usage().unwrap().0, 1);
    assert!(matches!(
        terminal_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(row.key(), b"admitted-key");
    drop(row);
    terminal_rx.await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn cancelled_and_panicking_sdk_owners_close_files_before_final_reservation() {
    for panic in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
            .await
            .unwrap();
        let quota = ScratchDiskQuota::new(64 << 20).unwrap();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let (terminal_tx, mut terminal_rx) = tokio::sync::oneshot::channel();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || {
                    let _ = terminal_tx.send(());
                })
                .unwrap(),
        );
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut caller = tokio::spawn({
            let workers = workers.clone();
            let quota = quota.clone();
            let job = job.clone();
            async move {
                workers
                    .run(move || {
                        crate::tx::block_on_owned_apply(async move {
                            let mut db = NativeScratchDatabase::open(root, quota, &job).await?;
                            db.record(1, b"owned-through-cancellation", 1).await?;
                            entered_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                            if panic {
                                panic!("injected private scratch SDK panic");
                            }
                            drop(db);
                            Ok(())
                        })
                    })
                    .await
            }
        });
        entered_rx.await.unwrap();
        drop(job);
        assert!(quota.usage().unwrap() > 0);
        if !panic {
            caller.abort();
            assert!(matches!((&mut caller).await,Err(error)if error.is_cancelled()));
        }
        assert_eq!(admission.usage().unwrap().0, 1);
        assert!(matches!(
            terminal_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        if panic {
            assert!(matches!(
                caller.await.unwrap(),
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), terminal_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn failures_poison_identity_admission_and_partial_sql_sources() {
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    workers
        .run({
            let quota = quota.clone();
            let job = job.clone();
            move || {
                crate::tx::block_on_owned_apply(async move {
                    for case in 0..10 {
                        let mut db =
                            NativeScratchDatabase::open(root.clone(), quota.clone(), &job).await?;
                        db.record(1, b"first", 1)
                            .await
                            .unwrap_or_else(|error| panic!("case {case} first record: {error:?}"));
                        db.record(1, b"first", 1).await.unwrap_or_else(|error| {
                            panic!("case {case} duplicate record: {error:?}")
                        });
                        db.owner(1, 0)
                            .await
                            .unwrap_or_else(|error| panic!("case {case} owner: {error:?}"));
                        let failed = match case {
                            0 => db.record(0, b"zero", 1).await,
                            1 => db.record(2, b"", 1).await,
                            2 => db.record(1, b"contradictory", 1).await,
                            3 => db.record(2, b"first", 1).await,
                            4 => db.owner(999, 0).await,
                            5 => db.leaf(-1, 1).await,
                            6 => db.record(2, &vec![7; 1 << 20], 1).await,
                            8 => db.record(1, b"first", 2).await,
                            9 => db.record(2, b"second", 0).await,
                            _ => {
                                // Inject a real late SQL error after a provisional set write.
                                db.leaf(0, 1).await?;
                                db.execute(
                                    "INSERT INTO absent_source VALUES(?1)",
                                    vec![Value::Integer(1)],
                                )
                                .await
                            }
                        };
                        assert!(failed.is_err(), "case {case} must fail");
                        assert!(db.next_owner(&[]).await.is_err());
                        assert!(db.next_set(0, 0).await.is_err());
                        assert!(db.finish_node(0).await.is_err());
                        assert!(db.record(2, b"after-failure", 1).await.is_err());
                        drop(db);
                        assert_eq!(quota.usage()?, 0);
                    }
                    Ok(())
                })
            }
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn large_key_peaks_stay_admitted_until_sdk_release_and_tiny_disk_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    workers
        .run({
            let quota = quota.clone();
            let job = job.clone();
            let admission = admission.clone();
            move || {
                crate::tx::block_on_owned_apply(async move {
                    let tiny = ScratchDiskQuota::new(1)?;
                    assert!(
                        NativeScratchDatabase::open(root.clone(), tiny.clone(), &job)
                            .await
                            .is_err()
                    );
                    assert_eq!(tiny.usage()?, 0);
                    let mut db = NativeScratchDatabase::open(root, quota.clone(), &job).await?;
                    let input = job.owned_buffer(1 << 20)?;
                    let key = vec![7; 1 << 20];
                    db.record(1, &key, u64::MAX).await?;
                    db.owner(1, 0).await?;
                    let row = db.next_owner(&[]).await?.unwrap();
                    assert_eq!(row.key(), key.as_slice());
                    assert_eq!(row.version(), u64::MAX);
                    drop(row);
                    drop(key);
                    drop(input);
                    let retained = admission.usage()?.1;
                    assert!(
                        retained > 128 << 20,
                        "SDK/key peak remains retained after caller input drops"
                    );
                    drop(db);
                    assert_eq!(quota.usage()?, 0);
                    assert_eq!(admission.usage()?.1, 0);
                    Ok(())
                })
            }
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
