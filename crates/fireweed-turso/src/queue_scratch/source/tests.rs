use super::*;
use fireweed_engine::queue_admission::QueueAdmission;
use fireweed_queue_spool::ScratchWorkers;
use std::{collections::BTreeSet, future::Future};
fn limits() -> QueueSourceSelectionWork {
    QueueSourceSelectionWork {
        rows: 100_000,
        copied_bytes: 64 << 20,
        scratch_steps: 100_000,
    }
}
async fn fixture<F, Fut>(run: F)
where
    F: FnOnce(
            Arc<ScratchRoot>,
            Arc<ScratchDiskQuota>,
            Arc<QueueReservation>,
            Arc<QueueSourceSelectionBudget>,
        ) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = QueueResult<()>>,
{
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let budget = QueueSourceSelectionBudget::new(limits(), &job).unwrap();
    workers
        .run({
            let quota = quota.clone();
            move || crate::tx::block_on_owned_apply(run(root, quota, job, budget))
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
async fn rows(
    db: &mut SourceCandidateScratch,
    set: &SourceCandidateSet,
) -> QueueResult<BTreeSet<Vec<u8>>> {
    let mut result = BTreeSet::new();
    let mut after = None;
    while let Some(row) = db.next(set, after.as_ref()).await? {
        assert!(result.insert(row.encoded_identity().to_vec()));
        after = Some(row);
    }
    Ok(result)
}
#[tokio::test]
async fn full_identity_dedup_and_immutable_algebra_match_literal_sets() {
    fixture(|root, quota, job, budget| async move {
        let mut db = SourceCandidateScratch::open(root, quota.clone(), job.clone(), budget).await?;
        let mut left = db.begin_set()?;
        let mut right = db.begin_set()?;
        let mut expected_left = BTreeSet::new();
        let mut expected_right = BTreeSet::new();
        for collection in [None, Some(""), Some("a\0b"), Some("a")] {
            for n in 0..12 {
                let key = format!("same\0{n}");
                let encoded = QueueSourceKey::Record {
                    collection,
                    key: &key,
                }
                .encode()?;
                if n % 2 == 0 {
                    db.insert(&mut left, collection, &key).await?;
                    db.insert(&mut left, collection, &key).await?;
                    expected_left.insert(encoded.clone());
                }
                if n % 3 == 0 {
                    db.insert(&mut right, collection, &key).await?;
                    expected_right.insert(encoded);
                }
            }
        }
        let left = db.seal(left)?;
        let right = db.seal(right)?;
        assert_eq!(left.len(), expected_left.len() as u64);
        assert_eq!(left.len(), 24);
        assert_eq!(right.len(), 16);
        let union = db.union(&left, &right).await?;
        let intersection = db.intersection(&left, &right).await?;
        let difference = db.difference(&left, &right).await?;
        assert_eq!(union.len(), 32);
        assert_eq!(intersection.len(), 8);
        assert_eq!(difference.len(), 16);
        assert_eq!(
            rows(&mut db, &union).await?,
            expected_left.union(&expected_right).cloned().collect()
        );
        assert_eq!(
            rows(&mut db, &intersection).await?,
            expected_left
                .intersection(&expected_right)
                .cloned()
                .collect()
        );
        assert_eq!(
            rows(&mut db, &difference).await?,
            expected_left.difference(&expected_right).cloned().collect()
        );
        assert_eq!(rows(&mut db, &left).await?, expected_left);
        assert_eq!(rows(&mut db, &right).await?, expected_right);
        let empty_builder = db.begin_set()?;
        let empty = db.seal(empty_builder)?;
        assert!(db.intersection(&left, &empty).await?.is_empty());
        let carrier = db.next(&left, None).await?.unwrap();
        drop(db);
        assert_eq!(quota.usage()?, 0);
        assert!(!carrier.encoded_identity().is_empty());
        Ok(())
    })
    .await;
}
#[tokio::test]
async fn foreign_handles_and_job_are_refused_without_spending_or_sql() {
    fixture(|root, quota, job, budget| async move {
        let mut db =
            SourceCandidateScratch::open(root.clone(), quota.clone(), job.clone(), budget.clone())
                .await?;
        let mut other =
            SourceCandidateScratch::open(root.clone(), quota.clone(), job.clone(), budget.clone())
                .await?;
        let mut builder = other.begin_set()?;
        other.insert(&mut builder, Some("c"), "same").await?;
        let set = other.seal(builder)?;
        let row = other.next(&set, None).await?.unwrap();
        let own_builder = db.begin_set()?;
        let own = db.seal(own_builder)?;
        let before = budget.remaining(&job)?;
        let foreign_admission = QueueAdmission::new(1, 128 << 20)?;
        let foreign_job = Arc::new(foreign_admission.reserve(0)?);
        assert!(
            SourceCandidateScratch::open(root, quota, foreign_job.clone(), budget.clone())
                .await
                .is_err()
        );
        assert_eq!(foreign_admission.usage()?, (1, 0));
        drop(foreign_job);
        assert!(db.next(&set, None).await.is_err());
        assert!(db.next(&own, Some(&row)).await.is_err());
        assert!(db.union(&own, &set).await.is_err());
        let mut builder = other.begin_set()?;
        assert!(db.insert(&mut builder, None, "foreign").await.is_err());
        assert_eq!(budget.remaining(&job)?, before);
        assert!(db.ready().is_ok());
        assert_eq!(rows(&mut other, &set).await?.len(), 1);
        Ok(())
    })
    .await;
}
#[tokio::test]
async fn cancellation_after_actual_partial_sql_write_retires_owner_without_refund() {
    fixture(|root, quota, job, budget| async move {
        let mut db =
            SourceCandidateScratch::open(root, quota.clone(), job.clone(), budget.clone()).await?;
        let mut leaf = db.begin_set()?;
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        db.pause_after_write = Some(paused.clone());
        let before = budget.remaining(&job)?;
        let mut pending = Box::pin(db.insert(&mut leaf, Some("c"), "partial"));
        std::future::poll_fn(|cx| {
            let result = pending.as_mut().poll(cx);
            assert!(result.is_pending());
            if paused.load(std::sync::atomic::Ordering::Acquire) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        assert!(quota.usage()? > 0);
        drop(pending);
        assert!(db.owner.is_none());
        assert_eq!(quota.usage()?, 0);
        assert!(db.seal(leaf).is_err());
        assert!(db.begin_set().is_err());
        let after = budget.remaining(&job)?;
        assert!(after.rows < before.rows);
        assert!(after.copied_bytes < before.copied_bytes);
        assert!(after.scratch_steps < before.scratch_steps);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn exact_insert_work_and_each_one_less_fail_without_partial_output() {
    for dimension in 0..4 {
        fixture(move |root, quota, job, _fixture_budget| async move {
            let bytes = QueueSourceKey::Record {
                collection: Some("c"),
                key: "k",
            }
            .encoded_len()? as u64;
            let mut limit = QueueSourceSelectionWork {
                rows: 2,
                copied_bytes: 3 * bytes,
                scratch_steps: 35,
            };
            match dimension {
                1 => limit.rows -= 1,
                2 => limit.copied_bytes -= 1,
                3 => limit.scratch_steps -= 1,
                _ => {}
            }
            // Independent trial, not a reset or replacement in an existing session.
            let budget = QueueSourceSelectionBudget::new(limit, &job)?;
            let mut db =
                SourceCandidateScratch::open(root, quota.clone(), job.clone(), budget.clone())
                    .await?;
            let mut leaf = db.begin_set()?;
            let result = db.insert(&mut leaf, Some("c"), "k").await;
            assert_eq!(result.is_ok(), dimension == 0);
            if dimension == 0 {
                assert_eq!(db.seal(leaf)?.len(), 1);
                assert_eq!(budget.remaining(&job)?, QueueSourceSelectionWork::default());
            } else {
                assert!(db.owner.is_none());
                assert!(db.seal(leaf).is_err());
                assert_eq!(quota.usage()?, 0);
            }
            Ok(())
        })
        .await;
    }
}

#[tokio::test]
async fn tiny_disk_and_exact_setup_memory_fail_closed_and_release_capacity() {
    fixture(|root, _quota, job, budget| async move {
        let tiny = ScratchDiskQuota::new(1)?;
        assert!(
            SourceCandidateScratch::open(root.clone(), tiny.clone(), job, budget)
                .await
                .is_err()
        );
        assert_eq!(tiny.usage()?, 0);
        let setup = root.path().as_os_str().len() as u64 * 16
            + (8 << 20)
            + (std::mem::size_of::<SourceCandidateScratch>()
                + std::mem::size_of::<Scope>()
                + 2 * std::mem::size_of::<usize>()) as u64
            + (std::mem::size_of::<QueueSourceSelectionBudget>() + 2 * std::mem::size_of::<usize>())
                as u64;
        for capacity in [setup, setup - 1] {
            let admission = QueueAdmission::new(1, capacity)?;
            let job = Arc::new(admission.reserve(0)?);
            let budget = QueueSourceSelectionBudget::new(limits(), &job)?;
            let quota = ScratchDiskQuota::new(64 << 20)?;
            let result = SourceCandidateScratch::open(
                root.clone(),
                quota.clone(),
                job.clone(),
                budget.clone(),
            )
            .await;
            assert_eq!(result.is_ok(), capacity == setup);
            if capacity == setup {
                assert_eq!(admission.usage()?, (1, setup));
            }
            drop(result);
            assert_eq!(quota.usage()?, 0);
            drop(budget);
            drop(job);
            assert_eq!(admission.usage()?, (0, 0));
        }
        // This is exact owned-charge accounting, not an allocator/RSS ceiling.
        Ok(())
    })
    .await;
}

#[test]
fn physical_owner_and_handles_can_move_into_owned_workers() {
    fn require_send<T: Send>() {}
    require_send::<SourceCandidateScratch>();
    require_send::<SourceSetBuilder>();
    require_send::<SourceCandidateSet>();
    require_send::<SourceCandidateIdentity>();
}

#[tokio::test]
async fn source_only_schema_and_fixed_statements_avoid_hidden_materialization() {
    fixture(|root, quota, job, budget| async move {
        fn send_future<F: Future + Send>(future: F) -> F {
            future
        }
        let mut db = send_future(SourceCandidateScratch::open(root, quota, job, budget)).await?;
        let physical = db.owner.as_ref().unwrap();
        assert_eq!(
            physical
                .one(
                    "SELECT count(*) FROM sqlite_master WHERE type='table'",
                    vec![]
                )
                .await?,
            Some(vec![Value::Integer(1)])
        );
        for (sql, params) in [
            (INSERT, vec![Value::Integer(1), Value::Blob(vec![1, 2])]),
            (CONTAINS, vec![Value::Integer(1), Value::Blob(vec![1, 2])]),
            (NEXT_LEN, vec![Value::Integer(1), Value::Blob(vec![])]),
            (NEXT_KEY, vec![Value::Integer(1), Value::Blob(vec![])]),
        ] {
            if sql != INSERT {
                let mut plan = physical
                    .connection
                    .query(
                        &format!("EXPLAIN QUERY PLAN {sql}"),
                        turso::params_from_iter(params.clone()),
                    )
                    .await
                    .map_err(|_| unknown())?;
                let mut details = Vec::new();
                while let Some(row) = plan.next().await.map_err(|_| unknown())? {
                    let Value::Text(detail) = row.get_value(3).map_err(|_| unknown())? else {
                        return Err(unknown());
                    };
                    details.push(detail);
                }
                let bound = if sql == CONTAINS {
                    "node=? AND key=?"
                } else {
                    "node=? AND key>?"
                };
                assert_eq!(details.len(), 1, "{sql}: {details:?}");
                assert!(
                    details[0].starts_with("SEARCH source_sets USING COVERING INDEX")
                        && details[0].contains(bound),
                    "{sql}: {details:?}"
                );
            }
            let opcodes =
                crate::queue_scratch::tests::opcodes(&physical.connection, sql, params).await?;
            assert!(!opcodes.is_empty());
            assert!(
                !opcodes
                    .iter()
                    .any(|opcode| crate::queue_scratch::tests::forbidden(opcode)),
                "{sql}: {opcodes:?}"
            );
        }
        let mut leaf = db.begin_set()?;
        send_future(db.insert(&mut leaf, Some("c"), "key")).await?;
        let set = db.seal(leaf)?;
        let row = send_future(db.next(&set, None)).await?.unwrap();
        assert!(send_future(db.next(&set, Some(&row))).await?.is_none());
        let _union = send_future(db.union(&set, &set)).await?;
        Ok(())
    })
    .await;
}
