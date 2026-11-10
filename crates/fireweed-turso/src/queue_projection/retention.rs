//! Cleanup applied from a logged `Retention` record. Applying one reads no
//! clock and no randomness: the horizon, the rows it removes and the version
//! floor follow from the record and the projection alone, so rebuilding the log
//! reaches the same horizon and the same rows.
use super::*;
use fireweed_engine::{
    queue_log_record::RetentionView,
    queue_retention::{RetentionState, live_claim_cutoff, plan_retention},
};

/// Where cleanup stands for one queue: the horizon, the deleted key cutoff and
/// the highest version a removed row held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RetentionRow {
    pub state: RetentionState,
    pub version_floor: u64,
}
pub(super) const ROW_SQL: &str = "SELECT CASE WHEN typeof(horizon)='integer' THEN horizon ELSE NULL END,CASE WHEN typeof(deleted_before)='integer' THEN deleted_before ELSE NULL END,CASE WHEN typeof(version_floor)='blob' AND length(version_floor)=8 THEN version_floor ELSE NULL END FROM q7_queues WHERE id=?1";
pub(super) fn parse_row(values: &[Value]) -> QueueResult<RetentionRow> {
    Ok(RetentionRow {
        state: RetentionState {
            horizon: integer(&values[0])?,
            deleted_before: integer(&values[1])?,
        },
        version_floor: unsigned(&values[2])?,
    })
}
pub(super) async fn read_row(connection: &Connection, queue_id: i64) -> QueueResult<RetentionRow> {
    let row = one(connection, ROW_SQL, vec![Value::Integer(queue_id)], 3)
        .await?
        .ok_or_else(|| unknown("retention queue authority disappeared"))?;
    parse_row(&row)
}
async fn ids(
    connection: &Connection,
    sql: &str,
    params: Vec<Value>,
    columns: usize,
) -> QueueResult<Vec<Vec<Value>>> {
    let mut rows = connection.query(sql, params).await.map_err(unknown)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(unknown)? {
        out.push(
            (0..columns)
                .map(|n| row.get_value(n).map_err(unknown))
                .collect::<QueueResult<Vec<_>>>()?,
        );
    }
    Ok(out)
}
/// Advance the horizon and remove at most `budget` rows: requests, receipts and
/// claim records issued before the horizon first, then retired deleted work. A
/// record that cannot finish leaves the rest to the next one. Returns how many
/// rows were removed.
pub(super) async fn apply_retention(
    connection: &Connection,
    queue_id: i64,
    retention: &RetentionView<'_>,
    job: &Arc<QueueReservation>,
) -> QueueResult<u64> {
    let definition_bytes = one(
        connection,
        "SELECT CASE WHEN typeof(definition)='blob' THEN length(definition) ELSE -1 END FROM q7_queues WHERE id=?1",
        vec![Value::Integer(queue_id)],
        1,
    )
    .await?
    .ok_or_else(|| unknown("queue authority disappeared"))?;
    let definition_bytes = u64::try_from(integer(&definition_bytes[0])?).map_err(unknown)?;
    let _definition_fetch = job.owned_buffer(
        definition_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let raw = one(
        connection,
        "SELECT definition FROM q7_queues WHERE id=?1",
        vec![Value::Integer(queue_id)],
        1,
    )
    .await?
    .ok_or_else(|| unknown("queue authority disappeared"))?;
    let definition = AdmittedQueueDefinition::from_raw(bytes(&raw[0])?, job)?;
    if retention.definition_pin().as_slice() != definition.pin().as_bytes().as_slice() {
        return Err(unknown("retention definition pin mismatch"));
    }
    let current = read_row(connection, queue_id).await?;
    let oldest_live_claim = one(
        connection,
        "SELECT MIN(issued_at) FROM q7_requests WHERE queue_id=?1 AND domain=1 AND live_until>?2",
        vec![
            Value::Integer(queue_id),
            Value::Integer(live_claim_cutoff(retention.at())),
        ],
        1,
    )
    .await?
    .and_then(|row| match row[0] {
        Value::Integer(issued_at) => Some(issued_at),
        _ => None,
    });
    let next = plan_retention(
        &definition.schema().retention,
        current.state,
        retention.at(),
        oldest_live_claim,
    );
    if next != current.state
        && connection
            .execute(
                "UPDATE q7_queues SET horizon=?2,deleted_before=?3 WHERE id=?1",
                vec![
                    Value::Integer(queue_id),
                    Value::Integer(next.horizon),
                    Value::Integer(next.deleted_before),
                ],
            )
            .await
            .map_err(unknown)?
            != 1
    {
        return Err(unknown("retention horizon lost authority"));
    }
    let mut left = retention.budget();
    let mut removed = 0u64;
    let limit = i64::try_from(left.min(i64::MAX as u64)).map_err(unknown)?;
    for row in ids(
        connection,
        "SELECT id FROM q7_requests WHERE queue_id=?1 AND issued_at<?2 ORDER BY issued_at,id LIMIT ?3",
        vec![
            Value::Integer(queue_id),
            Value::Integer(next.horizon),
            Value::Integer(limit),
        ],
        1,
    )
    .await?
    {
        connection
            .execute(
                "DELETE FROM q7_requests WHERE id=?1 AND queue_id=?2",
                vec![row[0].clone(), Value::Integer(queue_id)],
            )
            .await
            .map_err(unknown)?;
        removed += 1;
        left -= 1;
    }
    let mut floor = current.version_floor;
    if left > 0 && next.deleted_before > i64::MIN {
        for row in ids(
            connection,
            "SELECT id,version FROM q7_records WHERE queue_id=?1 AND collection=0 AND active=0 AND deleted_at<?2 ORDER BY deleted_at,id LIMIT ?3",
            vec![
                Value::Integer(queue_id),
                Value::Integer(next.deleted_before),
                Value::Integer(i64::try_from(left).map_err(unknown)?),
            ],
            2,
        )
        .await?
        {
            floor = floor.max(unsigned(&row[1])?);
            for table in ["q7_bodies", "q7_fields", "q7_additional", "q7_indexes"] {
                connection
                    .execute(
                        &format!("DELETE FROM {table} WHERE record_id=?1"),
                        vec![row[0].clone()],
                    )
                    .await
                    .map_err(unknown)?;
            }
            connection
                .execute(
                    "DELETE FROM q7_records WHERE id=?1 AND queue_id=?2",
                    vec![row[0].clone(), Value::Integer(queue_id)],
                )
                .await
                .map_err(unknown)?;
            removed += 1;
        }
    }
    if floor != current.version_floor
        && connection
            .execute(
                "UPDATE q7_queues SET version_floor=?2 WHERE id=?1",
                vec![Value::Integer(queue_id), number(floor)],
            )
            .await
            .map_err(unknown)?
            != 1
    {
        return Err(unknown("retention version floor lost authority"));
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, authorize, inspect, publication, work};
    use super::*;
    use bytes::Bytes;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_log_record::{encode_checked_genesis, encode_checked_retention},
        queue_planner::{
            RecordSnapshot, combine_deltas, insert_unique_probes, plan_commit_group,
            plan_insert_vector,
        },
        queue_publication::PublicationReceipt,
        queue_schema_codec::encode_definition,
    };

    const HOUR: i64 = 3_600_000_000_000;
    /// Request ids last two hours, and a retired key is refused for three.
    fn schema(deleted_keys: DeletedKeyPolicy) -> QueueSchema {
        QueueSchema {
            retention: QueueRetention {
                request_id_nanos: 2 * HOUR as u64,
                deleted_keys,
            },
            ..super::super::tests::schema()
        }
    }
    struct Log {
        projection: Arc<QueueProjection>,
        definition: AdmittedQueueDefinition,
        schema: QueueSchema,
        snapshot: RecordSnapshot,
        job: Arc<QueueReservation>,
        workers: ScratchWorkers,
        /// Every record applied so far, in order, so a second projection can fold the same log.
        log: Vec<Vec<u8>>,
        /// Work carries a unique field, so every insert takes a value of its own.
        next_unique: i64,
    }
    impl Log {
        async fn new(path: &Path, deleted_keys: DeletedKeyPolicy) -> Self {
            let admission = QueueAdmission::new(1, 256 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let workers = ScratchWorkers::new(1, 1).unwrap();
            let schema = schema(deleted_keys);
            schema.validate().unwrap();
            let definition =
                AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job)
                    .unwrap();
            let projection = QueueProjection::open(path, &job, &workers).await.unwrap();
            let mut log = Self {
                projection,
                definition,
                schema,
                snapshot: RecordSnapshot::default(),
                job,
                workers,
                log: vec![],
                next_unique: 0,
            };
            let genesis = encode_checked_genesis(
                1,
                &address(),
                &log.definition,
                &[31; 32],
                16 << 20,
                &log.job,
            )
            .unwrap();
            log.apply(genesis).await;
            log
        }
        async fn apply(&mut self, record: OwnedQueueLogRecord) -> u64 {
            self.log.push(record.bytes().to_vec());
            let offset = (self.log.len() - 1) as u64;
            self.projection
                .apply(record, offset, &self.job, &self.workers)
                .await
                .unwrap()
                .removed
        }
        /// One insert request of one new key, issued at `issued_at`.
        async fn insert(&mut self, id: &str, key: &str, issued_at: i64) -> u64 {
            self.next_unique += 1;
            let request = InsertRequest {
                queue: address(),
                request_id: id.into(),
                items: vec![work(key, self.next_unique, false)],
                mode: InsertMode::Upsert,
                issued_at,
            };
            self.snapshot.unique_probes_loaded =
                insert_unique_probes(&request.items, &self.snapshot, &self.schema).unwrap();
            let before = self.snapshot.clone();
            let (deltas, outcomes) =
                plan_insert_vector(&request, &mut self.snapshot, &self.schema, || Ok(issued_at))
                    .unwrap();
            let delta = combine_deltas(&deltas, before.next_sequence).unwrap();
            let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
            let record = publication(&before, &delta, &[receipt], &self.definition, &self.job);
            self.apply(record).await
        }
        /// Delete `key` with a group issued at `issued_at`.
        async fn delete(&mut self, id: &str, key: &str, issued_at: i64) {
            let version = self.snapshot.work[key].record.version;
            let group = CommitGroup {
                request_id: id.into(),
                claimed_updates: vec![],
                state_updates: vec![],
                work_inserts: vec![],
                work_updates: vec![UnclaimedUpdate {
                    key: key.into(),
                    expect: StateExpect::Version(version),
                    change: UnclaimedChange::Delete,
                }],
                administration: None,
                issued_at,
            };
            authorize(&mut self.snapshot, &group, &self.schema);
            let before = self.snapshot.clone();
            let delta =
                plan_commit_group(&group, &mut self.snapshot, issued_at, &self.schema).unwrap();
            let queue = address();
            let receipt = PublicationReceipt::commit(&queue, &group, &delta.receipt).unwrap();
            let record = publication(&before, &delta, &[receipt], &self.definition, &self.job);
            self.apply(record).await;
        }
        async fn retire(&mut self, at: i64, budget: u64) -> u64 {
            let record = encode_checked_retention(
                1,
                &address(),
                &self.definition,
                at,
                budget,
                16 << 20,
                &self.job,
            )
            .unwrap();
            let removed = self.apply(record).await;
            // The planner's view follows the stored one, as the owner's would: the
            // floor, and no authority for a row that is gone.
            let row = inspect(&self.projection, ROW_SQL, vec![Value::Integer(1)], 3).await;
            self.snapshot.version_floor = parse_row(&row).unwrap().version_floor;
            let keys = inspect(
                &self.projection,
                "SELECT ifnull(group_concat(CAST(key AS TEXT),','),'') FROM q7_records WHERE collection=0",
                vec![],
                1,
            )
            .await;
            let keys: std::collections::BTreeSet<String> = match &keys[0] {
                Value::Text(text) => text.split(',').map(str::to_owned).collect(),
                other => panic!("{other:?}"),
            };
            self.snapshot
                .versions
                .retain(|id, _| id.collection.is_some() || keys.contains(&id.key));
            self.snapshot.deleted_at.retain(|key, _| keys.contains(key));
            removed
        }
        /// Horizon, deleted cutoff, floor, request rows and record rows.
        async fn state(&self) -> (i64, i64, u64, i64, i64) {
            let row = inspect(
                &self.projection,
                "SELECT horizon,deleted_before,version_floor,(SELECT COUNT(*) FROM q7_requests),(SELECT COUNT(*) FROM q7_records WHERE collection=0) FROM q7_queues",
                vec![],
                5,
            )
            .await;
            (
                integer(&row[0]).unwrap(),
                integer(&row[1]).unwrap(),
                unsigned(&row[2]).unwrap(),
                integer(&row[3]).unwrap(),
                integer(&row[4]).unwrap(),
            )
        }
    }

    #[tokio::test]
    async fn a_pass_advances_the_horizon_and_removes_only_requests_older_than_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = Log::new(
            &directory.path().join("horizon.db"),
            DeletedKeyPolicy::Reusable,
        )
        .await;
        for n in 0..6 {
            log.insert(&format!("r{n}"), &format!("k{n}"), n * HOUR)
                .await;
        }
        let (horizon, _, _, requests, records) = log.state().await;
        assert_eq!((horizon, requests, records), (i64::MIN, 6, 6));
        // Horizon: 5h - 2h = 3h. Requests issued at 0h, 1h and 2h go; 3h stays.
        assert_eq!(log.retire(5 * HOUR, 1000).await, 3);
        let (horizon, deleted_before, floor, requests, records) = log.state().await;
        assert_eq!((horizon, deleted_before, floor), (3 * HOUR, i64::MIN, 0));
        assert_eq!((requests, records), (3, 6));
        // A pass at an earlier time lowers nothing.
        assert_eq!(log.retire(HOUR, 1000).await, 0);
        assert_eq!(log.state().await.0, 3 * HOUR);
        // Live work is never removed, only requests.
        assert_eq!(log.retire(100 * HOUR, 1000).await, 3);
        assert_eq!(log.state().await.3, 0);
        assert_eq!(log.state().await.4, 6);
    }

    #[tokio::test]
    async fn one_record_removes_at_most_its_budget() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = Log::new(
            &directory.path().join("budget.db"),
            DeletedKeyPolicy::Reusable,
        )
        .await;
        for n in 0..25 {
            log.insert(&format!("r{n}"), &format!("k{n}"), n).await;
        }
        let mut removed = vec![];
        for _ in 0..4 {
            removed.push(log.retire(100 * HOUR, 10).await);
        }
        assert_eq!(removed, [10, 10, 5, 0]);
        assert_eq!(log.state().await.3, 0);
    }

    #[tokio::test]
    async fn retired_rows_go_after_their_window_and_the_floor_keeps_versions_rising() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = Log::new(
            &directory.path().join("retired.db"),
            DeletedKeyPolicy::Retired {
                window_nanos: 3 * HOUR as u64,
            },
        )
        .await;
        log.insert("seed", "a", 10 * HOUR).await;
        let deleted_version = log.snapshot.work["a"].record.version;
        log.delete("delete", "a", 10 * HOUR).await;
        // Still inside the window: the row stays, and so does the floor.
        log.retire(13 * HOUR, 1000).await;
        let (_, deleted_before, floor, _, records) = log.state().await;
        assert_eq!((deleted_before, floor, records), (10 * HOUR, 0, 1));
        // After it the row goes and its version moves into the floor.
        assert_eq!(log.retire(13 * HOUR + 1, 1000).await, 1);
        let (_, _, floor, requests, records) = log.state().await;
        assert!(floor > deleted_version, "{floor} after {deleted_version}");
        assert_eq!((requests, records), (0, 0));
        // The key returns above every version it had.
        log.insert("again", "a", 13 * HOUR + 1).await;
        assert!(
            log.snapshot.work["a"].record.version > floor,
            "{} after {floor}; versions {:?}",
            log.snapshot.work["a"].record.version,
            log.snapshot.versions
        );
    }

    #[tokio::test]
    async fn the_same_log_rebuilds_to_the_same_horizon_and_rows() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = Log::new(
            &directory.path().join("live.db"),
            DeletedKeyPolicy::Retired {
                window_nanos: 3 * HOUR as u64,
            },
        )
        .await;
        for n in 0..8 {
            log.insert(&format!("r{n}"), &format!("k{n}"), n * HOUR)
                .await;
        }
        log.delete("d0", "k0", 0).await;
        log.delete("d1", "k1", HOUR).await;
        log.retire(6 * HOUR, 3).await;
        log.retire(6 * HOUR, 3).await;
        log.insert("late", "k0", 6 * HOUR).await;
        log.retire(20 * HOUR, 1000).await;
        let dump = |projection: Arc<QueueProjection>| async move {
            let mut out = vec![];
            for sql in [
                "SELECT group_concat(horizon||','||deleted_before||','||hex(version_floor)||','||applied_next_offset||','||hex(next_sequence),';') FROM q7_queues",
                "SELECT group_concat(hex(identity)||','||domain||','||issued_at,';') FROM (SELECT * FROM q7_requests ORDER BY identity)",
                "SELECT group_concat(hex(key)||','||hex(version)||','||active||','||ifnull(deleted_at,'-'),';') FROM (SELECT * FROM q7_records WHERE collection=0 ORDER BY key)",
            ] {
                out.push(format!("{:?}", inspect(&projection, sql, vec![], 1).await));
            }
            out
        };
        let live = dump(log.projection.clone()).await;
        // A second projection folds the same bytes, as a rebuild does.
        let admission = QueueAdmission::new(1, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let rebuilt = QueueProjection::open(&directory.path().join("rebuilt.db"), &job, &workers)
            .await
            .unwrap();
        for (offset, bytes) in log.log.iter().enumerate() {
            let record = OwnedQueueLogRecord::from_admitted_bytes(
                Bytes::copy_from_slice(bytes),
                job.owned_buffer(bytes.len() as u64 * 4 + 4096).unwrap(),
                &job,
            )
            .unwrap();
            rebuilt
                .apply(record, offset as u64, &job, &workers)
                .await
                .unwrap();
        }
        assert_eq!(dump(rebuilt).await, live);
        // The pass was not a no-op.
        assert!(log.state().await.0 > i64::MIN);
    }

    mod large {
        use super::*;

        /// A long churn of requests and of retired keys keeps the retained rows
        /// at what the windows hold, however many have come and gone.
        #[tokio::test]
        async fn retained_rows_stay_flat_under_churn() {
            let directory = tempfile::tempdir().unwrap();
            let mut log = Log::new(
                &directory.path().join("churn.db"),
                DeletedKeyPolicy::Retired {
                    window_nanos: 3 * HOUR as u64,
                },
            )
            .await;
            let mut peak = 0;
            let mut versions = std::collections::BTreeMap::new();
            for round in 0..1500i64 {
                let at = round * HOUR / 10;
                // Forty keys take turns, each back after more than its three hour
                // window, so a key's version only rises.
                let key = format!("k{}", round % 40);
                log.insert(&format!("insert-{round}"), &key, at).await;
                let version = log.snapshot.work[&key].record.version;
                let last = versions.insert(key.clone(), version).unwrap_or(0);
                assert!(version > last, "round {round}: {version} after {last}");
                log.delete(&format!("delete-{round}"), &key, at).await;
                log.retire(at, 1000).await;
                let (_, _, _, requests, records) = log.state().await;
                peak = peak.max(requests + records);
            }
            // Two requests a round for two hours of six-minute rounds, and at most
            // forty deleted rows: nothing that grows with 1500 rounds.
            assert!(peak < 120, "peak {peak}");
            let (_, _, floor, requests, records) = log.state().await;
            assert!(requests + records < 120 && floor > 0);
        }
    }
}
