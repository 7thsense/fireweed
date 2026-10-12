//! Covered native totals and numeric-bin aggregation without queue payload reads.
use super::*;
use fireweed_engine::{Clock, queue_memory::incoming_headers};
pub struct StatsReadInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: StatsRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedQueueStats {
    stats: QueueStats,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedQueueStats {
    pub fn stats(&self) -> &QueueStats {
        &self.stats
    }
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: StatsReadInput,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Internal covered aggregation, not consumer QueueStore activation.
    pub async fn read_queue_stats(
        self: &Arc<Self>,
        input: StatsReadInput,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedQueueStats> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("stats genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        let probes = super::filter_sql::filter_build_charge(&input.request.filter)?;
        let grouping = (input.request.group_by.len() as u64)
            .checked_mul(2048)
            .ok_or(QueueError::TooLarge)?;
        let bins = input
            .request
            .buckets
            .as_ref()
            .map_or(0, |dimension| dimension.buckets.len() + 1) as u64;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(probes))
            .and_then(|n| n.checked_add(grouping))
            .and_then(|n| n.checked_add(bins.checked_mul(16384)?))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            clock,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let stats = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedQueueStats {
                        stats,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = QueueStats;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected stats read precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<QueueStats> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("stats projection missing"))?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        let request = &self.input.request;
        request.validate(snapshot.definition.schema(), &self.input.limits)?;
        let now = timestamp_nanos(self.clock.now())?;
        let (sql, params) = totals_sql(
            request,
            &snapshot.definition.schema().work,
            snapshot.queue_id,
            now,
        )?;
        let bins = request
            .buckets
            .as_ref()
            .map_or(0, |dimension| dimension.buckets.len() + 1);
        let row = one(connection, &sql, params, 5 * (bins + 1) + 1)
            .await?
            .ok_or_else(|| unknown("stats aggregate missing"))?;
        if integer(&row[5 * (bins + 1)])? != 0 {
            return Err(unknown("invalid aggregate work authority"));
        }
        let totals = counts(&row, 0)?;
        let mut buckets = Vec::new();
        if let Some(dimension) = &request.buckets {
            for (n, label) in dimension
                .buckets
                .iter()
                .map(|bin| &bin.label)
                .chain(std::iter::once(&dimension.missing_label))
                .enumerate()
            {
                buckets.push(BucketCounts {
                    label: label.clone(),
                    counts: counts(&row, 5 * (n + 1))?,
                });
            }
        }
        let groups = super::stats_group::load_groups(
            connection,
            &snapshot,
            request,
            now,
            &self.input.limits,
            &self.job,
            &mut self.charges,
        )
        .await?;
        let stats = QueueStats {
            totals,
            groups,
            buckets,
            as_of: now,
        };
        if encoded_len(&stats)? > self.input.limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(stats)
    }
}
pub(super) fn counts(row: &[Value], offset: usize) -> QueueResult<Counts> {
    let count = |n| u64::try_from(integer(&row[offset + n])?).map_err(unknown);
    Ok(Counts {
        pending: count(0)?,
        leased: count(1)?,
        completed: count(2)?,
        failed: count(3)?,
        oldest_eligible_since: match row[offset + 4] {
            Value::Null => None,
            _ => Some(integer(&row[offset + 4])?),
        },
    })
}
pub(super) fn aggregate(effective: &str, eligible_at: i64, predicate: &str) -> String {
    format!(
        "count(*) FILTER(WHERE ({predicate}) AND ({effective})=0),count(*) FILTER(WHERE ({predicate}) AND ({effective})=1),count(*) FILTER(WHERE ({predicate}) AND ({effective})=2),count(*) FILTER(WHERE ({predicate}) AND ({effective})=3),min(CASE WHEN typeof(r.eligible_since)='integer' THEN r.eligible_since ELSE NULL END) FILTER(WHERE ({predicate}) AND ({effective})=0 AND r.available_at<={eligible_at})"
    )
}
fn totals_sql(
    request: &StatsRequest,
    definition: &CollectionDefinition,
    queue_id: i64,
    now: i64,
) -> QueueResult<(String, Vec<Value>)> {
    let mut pool = super::filter_sql::BytePool(Vec::new());
    let (with, mut filter) =
        super::filter_sql::filter_sql(&request.filter, definition, queue_id, 0, &mut pool)?;
    let effective = effective_lifecycle(now);
    let base_filter = filter.clone();
    if let Some(lifecycle) = request.filter.lifecycle {
        filter.push_str(&format!(
            " AND ({effective})={}",
            encode_lifecycle(lifecycle)
        ));
    }
    let mut columns = vec![aggregate(&effective, request.eligible_at, "1")];
    if let Some(dimension) = &request.buckets {
        let number = field_number(definition, &dimension.field)?;
        let (table, identity) = if number >= 0 {
            ("q7_fields", format!("f.field={number}"))
        } else {
            let hash = pool.bytes(&discovery_digest(dimension.field.as_bytes()));
            let name = pool.bytes(dimension.field.as_bytes());
            (
                "q7_additional",
                format!("f.name_hash={hash} AND f.name={name}"),
            )
        };
        let scope =
            format!("f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=0 AND {identity}");
        for bin in &dimension.buckets {
            let lower = ordered_key(&[Some(&bin.lower_inclusive)])?;
            let upper = ordered_key(&[Some(&bin.upper_exclusive)])?;
            let lp = pool.bytes(&discovery_prefix(&lower));
            let up = pool.bytes(&discovery_prefix(&upper));
            let lk = pool.bytes(&lower);
            let uk = pool.bytes(&upper);
            let predicate = format!(
                "EXISTS(SELECT 1 FROM {table} f WHERE {scope} AND f.prefix>={lp} AND f.prefix<={up} AND f.full_key>={lk} AND f.full_key<{uk})"
            );
            columns.push(aggregate(&effective, request.eligible_at, &predicate));
        }
        columns.push(aggregate(
            &effective,
            request.eligible_at,
            &format!("NOT EXISTS(SELECT 1 FROM {table} f WHERE {scope})"),
        ));
    }
    columns.push(format!("(SELECT count(*) FROM q7_records r WHERE r.queue_id={queue_id} AND r.collection=0 AND r.active=1{base_filter} AND NOT ({}))",raw_valid_authority()));
    Ok((
        format!(
            "{with}SELECT {} FROM q7_records r WHERE r.queue_id={queue_id} AND r.collection=0 AND r.active=1{filter}",
            columns.join(",")
        ),
        if pool.0.is_empty() {
            vec![]
        } else {
            vec![Value::Blob(pool.0)]
        },
    ))
}

pub(super) fn raw_valid_authority() -> &'static str {
    "typeof(r.lifecycle)='integer' AND r.lifecycle IN (0,1,2,3) AND typeof(r.available_at)='integer' AND typeof(r.eligible_since)='integer' AND ((r.lifecycle=1 AND typeof(r.lease_until)='integer' AND typeof(r.lease_hash)='blob' AND length(r.lease_hash)=32) OR (r.lifecycle<>1 AND r.lease_until IS NULL AND r.lease_hash IS NULL))"
}
pub(super) fn effective_lifecycle(now: i64) -> String {
    format!(
        "CASE WHEN NOT ({}) THEN NULL WHEN r.lifecycle=1 AND r.lease_until<={now} THEN 0 ELSE r.lifecycle END",
        raw_valid_authority()
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, seed};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn request() -> StatsRequest {
        StatsRequest {
            queue: address(),
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 10,
            buckets: Some(NumericBucketDimension {
                field: "u".into(),
                buckets: vec![
                    NumericBucket {
                        label: "same".into(),
                        lower_inclusive: FieldValue::Integer(0),
                        upper_exclusive: FieldValue::Integer(2),
                    },
                    NumericBucket {
                        label: "same".into(),
                        lower_inclusive: FieldValue::Integer(2),
                        upper_exclusive: FieldValue::Integer(3),
                    },
                    NumericBucket {
                        label: "zero".into(),
                        lower_inclusive: FieldValue::Integer(8),
                        upper_exclusive: FieldValue::Integer(9),
                    },
                ],
                missing_label: "missing".into(),
            }),
        }
    }
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        request: StatsRequest,
    ) -> StatsReadInput {
        let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
        StatsReadInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            request,
            limits: QueueLimits::default(),
            required_next_offset: 2,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn native_totals_and_bins_preserve_order_zero_missing_and_filter_without_bodies() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("stats.db"), &job, &workers).await;
        seed(&projection, &definition, &job, &workers, true).await;
        let tight = QueueAdmission::new(1, 1 << 20).unwrap();
        let tiny = Arc::new(tight.reserve(0).unwrap());
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let result = projection
            .read_queue_stats(
                input(&definition, &tiny, request()),
                clock.clone(),
                &tiny,
                &workers,
            )
            .await
            .unwrap();
        let response_bytes = encoded_len(result.stats()).unwrap();
        assert_eq!(result.stats().as_of, 100);
        assert_eq!(result.stats().totals.pending, 2);
        assert_eq!(result.stats().totals.oldest_eligible_since, Some(10));
        assert_eq!(
            result
                .stats()
                .buckets
                .iter()
                .map(|bin| (bin.label.as_str(), bin.counts.pending))
                .collect::<Vec<_>>(),
            [("same", 1), ("same", 1), ("zero", 0), ("missing", 0)]
        );
        assert!(result.stats().groups.is_empty());
        drop(result);
        for shorter in [false, true] {
            let mut bounded = input(&definition, &tiny, request());
            bounded.limits.max_response_bytes = response_bytes - u64::from(shorter);
            let response = projection
                .read_queue_stats(bounded, clock.clone(), &tiny, &workers)
                .await;
            if shorter {
                assert!(matches!(response, Err(QueueError::TooLarge)));
            } else {
                assert_eq!(response.as_ref().unwrap().stats().as_of, 100);
            }
            drop(response);
        }
        clock.set(-1);
        let negative = projection
            .read_queue_stats(
                input(&definition, &tiny, request()),
                clock.clone(),
                &tiny,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(negative.stats().as_of, -1);
        drop(negative);
        clock.set(100);
        let held = projection.writer.lock().await;
        let mut waiting = Box::pin(projection.read_queue_stats(
            input(&definition, &tiny, request()),
            clock.clone(),
            &tiny,
            &workers,
        ));
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(waiting.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        clock.set(200);
        drop(held);
        let fresh = waiting.await.unwrap();
        assert_eq!(fresh.stats().as_of, 200);
        drop(fresh);
        clock.set(100);
        let mut grouped = request();
        grouped.group_by = vec!["u".into(), "meta/missing".into()];
        let result = projection
            .read_queue_stats(
                input(&definition, &job, grouped),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.stats().groups.len(), 2);
        assert_eq!(
            result.stats().groups[0].values,
            vec![Some(FieldValue::Integer(1)), None]
        );
        assert_eq!(
            result.stats().groups[1].values,
            vec![Some(FieldValue::Integer(2)), None]
        );
        assert!(
            result
                .stats()
                .groups
                .iter()
                .all(|group| group.counts.pending == 1)
        );
        assert_eq!(result.stats().buckets[0].counts.pending, 1);
        drop(result);
        let mut filtered = request();
        filtered
            .filter
            .fields
            .insert("u".into(), Predicate::Equal(FieldValue::Integer(2)));
        let result = projection
            .read_queue_stats(
                input(&definition, &tiny, filtered),
                clock.clone(),
                &tiny,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.stats().totals.pending, 1);
        assert_eq!(result.stats().buckets[0].counts.pending, 0);
        assert_eq!(result.stats().buckets[1].counts.pending, 1);
        drop(result);
        let mut missing = request();
        let dimension = missing.buckets.as_mut().unwrap();
        dimension.field = "meta/missing".into();
        dimension.buckets.clear();
        let result = projection
            .read_queue_stats(
                input(&definition, &tiny, missing),
                clock.clone(),
                &tiny,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.stats().buckets.len(), 1);
        assert_eq!(result.stats().buckets[0].counts.pending, 2);
        drop(result);
        let mut many = request();
        let dimension = many.buckets.as_mut().unwrap();
        dimension.buckets = (0..50)
            .map(|n| NumericBucket {
                label: format!("b{n}"),
                lower_inclusive: FieldValue::Integer(n),
                upper_exclusive: FieldValue::Integer(n + 1),
            })
            .collect();
        let result = projection
            .read_queue_stats(
                input(&definition, &job, many.clone()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.stats().buckets.len(), 51);
        assert_eq!(result.stats().buckets[1].counts.pending, 1);
        assert_eq!(result.stats().buckets[2].counts.pending, 1);
        drop(result);
        many.buckets.as_mut().unwrap().buckets.push(NumericBucket {
            label: "invalid".into(),
            lower_inclusive: FieldValue::Integer(51),
            upper_exclusive: FieldValue::Integer(52),
        });
        let held = projection.writer.lock().await;
        let rejected = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            projection.read_queue_stats(input(&definition, &tiny, many), clock, &tiny, &workers),
        )
        .await
        .expect("51-bin envelope rejected before dispatch");
        assert!(matches!(rejected, Err(QueueError::Invalid(_))));
        drop(held);
        drop(tiny);
        assert_eq!(tight.usage().unwrap(), (0, 0));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn aggregate_metadata_corruption_is_bounded_before_lifecycle_filtering() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("stats-corrupt.db"), &job, &workers).await;
        seed(&projection, &definition, &job, &workers, true).await;
        let tight = QueueAdmission::new(1, 1 << 20).unwrap();
        let tiny = Arc::new(tight.reserve(0).unwrap());
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        super::super::tests::execute_for_test(
            &projection,
            "UPDATE q7_records SET eligible_since=?1 WHERE key=?2",
            vec![blob(&vec![7; 4 << 20]), blob(b"a")],
        )
        .await;
        for grouped in [false, true] {
            let mut query = request();
            query.buckets = None;
            if grouped {
                query.group_by = vec!["u".into()];
            }
            assert!(
                matches!(projection.read_queue_stats(input(&definition,&tiny,query),clock.clone(),&tiny,&workers).await,Err(QueueError::OutcomeUnknown(message)) if message.contains("invalid aggregate work authority"))
            );
        }
        super::super::tests::execute_for_test(
            &projection,
            "UPDATE q7_records SET eligible_since=10,lease_until=500 WHERE key=?1",
            vec![blob(b"a")],
        )
        .await;
        let mut filtered = request();
        filtered.filter.lifecycle = Some(Lifecycle::Pending);
        filtered.buckets = None;
        assert!(matches!(
            projection
                .read_queue_stats(
                    input(&definition, &tiny, filtered.clone()),
                    clock.clone(),
                    &tiny,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        super::super::tests::execute_for_test(
            &projection,
            "UPDATE q7_records SET lifecycle=1,lease_until=NULL WHERE key=?1",
            vec![blob(b"a")],
        )
        .await;
        assert!(matches!(
            projection
                .read_queue_stats(
                    input(&definition, &tiny, filtered),
                    clock.clone(),
                    &tiny,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        super::super::tests::execute_for_test(
            &projection,
            "UPDATE q7_records SET lifecycle=0 WHERE key=?1",
            vec![blob(b"a")],
        )
        .await;
        let result = projection
            .read_queue_stats(input(&definition, &tiny, request()), clock, &tiny, &workers)
            .await
            .unwrap();
        assert_eq!(result.stats().totals.pending, 2);
        drop(result);
        drop(tiny);
        assert_eq!(tight.usage().unwrap(), (0, 0));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn native_multidimension_order_matches_tuple_oracle_for_missing_empty_and_nul_values() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("stats-order.db"), &job, &workers).await;
        seed(&projection, &definition, &job, &workers, true).await;
        let pairs = vec![
            (
                Some(FieldValue::Text("z".into())),
                Some(FieldValue::Text("".into())),
            ),
            (Some(FieldValue::Text("a".into())), None),
            (
                Some(FieldValue::Text("".into())),
                Some(FieldValue::Text("a\0z".into())),
            ),
            (
                Some(FieldValue::Bytes(vec![0, 255])),
                Some(FieldValue::Bytes(vec![0])),
            ),
        ];
        let items = pairs
            .iter()
            .enumerate()
            .map(|(n, (first, second))| {
                let mut fields: Fields = [("u".into(), FieldValue::Integer(n as i64 + 10))].into();
                if let Some(value) = first {
                    fields.insert("meta/first".into(), value.clone());
                }
                if let Some(value) = second {
                    fields.insert("meta/second".into(), value.clone());
                }
                NewWork {
                    key: format!("order{n}"),
                    priority: 0,
                    available_at: 0,
                    fields,
                    body: bytes::Bytes::new(),
                }
            })
            .collect();
        let prepared = projection
            .prepare_insert(
                InsertReplayInput {
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        &definition,
                        &[31; 32],
                        16 << 20,
                        &job,
                    )
                    .unwrap(),
                    request: InsertRequest {
                        queue: address(),
                        request_id: "stats-order-seed".into(),
                        mode: InsertMode::InsertOnly,
                        items,
                    },
                    limits: QueueLimits::default(),
                    required_next_offset: 2,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                1,
                16 << 20,
                Arc::new(TestClock::default()),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let mut expected = std::collections::BTreeMap::new();
        expected.insert(ordered_key(&[None, None]).unwrap(), (vec![None, None], 2));
        for (first, second) in pairs {
            let key = ordered_key(&[second.as_ref(), first.as_ref()]).unwrap();
            expected.insert(key, (vec![second, first], 1));
        }
        let mut query = request();
        query.group_by = vec!["meta/second".into(), "meta/first".into()];
        query.buckets = None;
        let mut input = input(&definition, &job, query);
        input.required_next_offset = 3;
        input.required_insertion_sequence = 6;
        let result = projection
            .read_queue_stats(input, Arc::new(TestClock::default()), &job, &workers)
            .await
            .unwrap();
        assert_eq!(
            result
                .stats()
                .groups
                .iter()
                .map(|group| (group.values.clone(), group.counts.pending))
                .collect::<Vec<_>>(),
            expected.into_values().collect::<Vec<_>>()
        );
        assert_eq!(result.stats().totals.pending, 6);
        drop(result);
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
