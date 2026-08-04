use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef, ts};
use fireweed_core::{ClientItemKey, ItemId, Metadata, PriorityValue, RequestId};
use fireweed_engine::{
    AsyncProjectionStore, BatchUpdateOutcome, BatchUpdateResponse, CommandEnvelope,
    CommandPosition, PayloadUpdate, PushCommand, QueueCommand, QueueKey, RequestOutcome,
    ScheduleUpdate, UpdateFieldsCommand,
};
use fireweed_relational::{fields_from_json, metadata_from_json, parse_priority};
use fireweed_turso::{
    JournalMode, TURSO_SUPPORTED_BOUNDARY, TURSO_SUPPORTED_VERSION, TursoConfig, TursoRelational,
    verify_local_wal_benchmark_evidence,
};

#[tokio::test]
async fn turso_projection_full_shared_conformance() {
    let store = TursoRelational::in_memory().await.unwrap();
    fireweed_conformance::async_projection::run_full_async_projection_conformance(&store).await;
}

fn batch_fixture(
    count: usize,
) -> (
    Vec<fireweed_engine::PushItem>,
    Vec<ItemId>,
    Vec<UpdateFieldsCommand>,
) {
    let mut pushed = Vec::with_capacity(count);
    let mut ids = Vec::with_capacity(count);
    let mut updates = Vec::with_capacity(count);
    for index in 0..count {
        let id = ItemId::new((count as u64 * 10_000 + index as u64 + 1).to_string()).unwrap();
        let key = format!("batch-key-{count}-{index}");
        pushed.push(item(&id.to_string(), &key, index as i64));
        ids.push(id);
        updates.push(UpdateFieldsCommand {
            item_id: id,
            field_ops: BTreeMap::new(),
            payload: PayloadUpdate::Set(Some(Bytes::from(format!("payload-{index}")))),
            set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(index as i64 + 1))),
            set_not_before: ScheduleUpdate::Set(Some(ts(100))),
            set_entity_document: None,
            set_fields: Some(BTreeMap::from([(
                "field".to_string(),
                Bytes::from(format!("value-{index}")),
            )])),
            set_metadata: Some(Metadata::default()),
            set_gate_keys: Some(vec![format!("gate-{index}")]),
            api001_batch: true,
        });
    }
    (pushed, ids, updates)
}

async fn apply_measured_batch(count: usize) -> fireweed_turso::TursoBatchUpdateStatementShape {
    let mut definition = qdef();
    definition.max_push_batch_size = 1_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let (pushed, ids, updates) = batch_fixture(count);
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids.clone(),
        )],
    )
    .await
    .unwrap();

    let request_id = RequestId::new(format!("batch-request-{count}")).unwrap();
    let response = BatchUpdateResponse {
        request_id: request_id.clone(),
        results: ids
            .iter()
            .enumerate()
            .map(|(index, item_id)| BatchUpdateOutcome::Updated {
                item_id: *item_id,
                client_item_key: ClientItemKey::new(format!("batch-key-{count}-{index}")).unwrap(),
                item_version: 2,
            })
            .collect(),
    };
    let mut commands = updates
        .into_iter()
        .map(|update| {
            let item_id = update.item_id;
            envelope(QueueCommand::UpdateFields(update), vec![item_id])
        })
        .collect::<Vec<CommandEnvelope>>();
    commands[0].request_id = Some(request_id);
    commands[0].request_fingerprint = Some(42);
    commands[0].request_outcome = Some(RequestOutcome::BatchUpdate {
        response_payload: serde_json::to_string(&response).unwrap(),
    });
    let positions = (1..=count)
        .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence as u64))
        .collect::<Vec<_>>();
    let replay_positions = positions.clone();
    let replay_commands = commands.clone();
    AsyncProjectionStore::apply_live(&store, positions, commands)
        .await
        .unwrap();
    let shape = store.last_batch_update_statement_shape().unwrap();
    for index in [0, count - 1] {
        let item = store
            .query(
                "SELECT item_version,fields,payload,metadata,priority,not_before,eligible_since \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    ids[index].to_string().into(),
                ],
            )
            .await
            .unwrap();
        let values = &item[0].values;
        assert_eq!(values[0], turso::Value::Integer(2));
        let turso::Value::Text(fields) = &values[1] else {
            panic!("fields were not text")
        };
        assert_eq!(
            fields_from_json(fields.clone()).unwrap().get("field"),
            Some(&Bytes::from(format!("value-{index}")))
        );
        let turso::Value::Blob(payload) = &values[2] else {
            panic!("payload was not a blob")
        };
        assert_eq!(payload, &Bytes::from(format!("payload-{index}")).to_vec());
        let turso::Value::Text(metadata) = &values[3] else {
            panic!("metadata was not text")
        };
        assert_eq!(
            metadata_from_json(metadata.clone()).unwrap(),
            Metadata::default()
        );
        let turso::Value::Text(priority) = &values[4] else {
            panic!("priority was not text")
        };
        assert_eq!(
            parse_priority(Some(priority.clone())).unwrap(),
            Some(PriorityValue::Int64(index as i64 + 1))
        );
        assert_eq!(values[5], turso::Value::Integer(100_000_000_000));
        assert_eq!(values[6], turso::Value::Integer(0));
        let gates = store
            .query(
                "SELECT gate_key FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 \
                 AND item_id=?3 ORDER BY gate_key",
                vec![
                    shard.tenant_id.as_str().to_string().into(),
                    shard.queue_id.as_str().to_string().into(),
                    ids[index].to_string().into(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            gates[0].values,
            [turso::Value::Text(format!("gate-{index}"))]
        );
    }
    AsyncProjectionStore::apply_recovery(&store, replay_positions, replay_commands)
        .await
        .unwrap();
    let replayed_versions = store
        .query(
            "SELECT COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND item_version<>2",
            vec![
                shard.tenant_id.as_str().to_string().into(),
                shard.queue_id.as_str().to_string().into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(replayed_versions[0].values, [turso::Value::Integer(0)]);
    assert_eq!(store.last_batch_update_statement_shape(), None);
    shape
}

#[tokio::test]
async fn turso_batch_update_statement_shape_is_bind_bounded() {
    for (count, exact_statement_bound) in [(1, 10), (100, 11), (1_000, 28)] {
        let shape = apply_measured_batch(count).await;
        assert_eq!(shape.item_count, count);
        assert_eq!(
            shape.statement_count, exact_statement_bound,
            "statement shape drifted at {count} items"
        );
        assert!(
            shape.max_bind_count <= 900,
            "{} binds exceeded the explicit 900-bind boundary at {count} items",
            shape.max_bind_count
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn turso_projection_keeps_single_thread_heartbeat_live() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>(_: T) {}
    assert_send_sync::<TursoRelational>();

    let mut definition = qdef();
    definition.max_push_batch_size = 1_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = Arc::new(TursoRelational::in_memory().await.unwrap());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();
    let (pushed, ids, _) = batch_fixture(1_000);
    let future = AsyncProjectionStore::apply_live(
        store.as_ref(),
        vec![CommandPosition::new(shard, 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            ids,
        )],
    );
    assert_send(future);

    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let heartbeat_finished = finished.clone();
    let heartbeat_ticks = ticks.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        while !heartbeat_finished.load(Ordering::Acquire) {
            interval.tick().await;
            heartbeat_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let ticks_before_apply = ticks.load(Ordering::Relaxed);
    let apply_store = store.clone();
    let apply_finished = finished.clone();
    let apply = tokio::spawn(async move {
        let (pushed, ids, _) = batch_fixture(1_000);
        let result = AsyncProjectionStore::apply_live(
            apply_store.as_ref(),
            vec![CommandPosition::new(
                QueueKey::new(qdef().tenant_id, qdef().queue_id),
                0,
                0,
            )],
            vec![envelope(
                QueueCommand::Push(PushCommand { items: pushed }),
                ids,
            )],
        )
        .await;
        apply_finished.store(true, Ordering::Release);
        result
    });
    tokio::time::timeout(Duration::from_secs(15), apply)
        .await
        .expect("Turso apply exceeded heartbeat deadline")
        .unwrap()
        .unwrap();
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > ticks_before_apply);
}

fn benchmark_evidence() -> serde_json::Value {
    serde_json::json!({
        "turso_version": TURSO_SUPPORTED_VERSION,
        "turso_features": ["local"],
        "boundary": TURSO_SUPPORTED_BOUNDARY,
        "batch_sizes": [1, 100, 1000],
        "operations_per_second": 1.0,
        "p50_us": 1.0,
        "p95_us": 2.0,
        "p99_us": 3.0,
        "database_bytes": 1.0,
        "cpu_time_ms": 1.0,
        "peak_rss_bytes": 1.0,
        "excluded_time": {"cold_open": true, "fixture_generation": true},
        "regression_limits": {
            "min_operations_per_second_ratio": 0.8,
            "max_p99_ratio": 1.25
        }
    })
}

#[test]
fn turso_local_wal_benchmark_evidence_verifier() {
    let valid = benchmark_evidence();
    verify_local_wal_benchmark_evidence(&valid).unwrap();
    let baseline: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/turso-local-wal-baseline.json")).unwrap();
    verify_local_wal_benchmark_evidence(&baseline).unwrap();
    for field in [
        "batch_sizes",
        "operations_per_second",
        "p50_us",
        "p95_us",
        "p99_us",
        "database_bytes",
        "cpu_time_ms",
        "peak_rss_bytes",
        "turso_version",
        "turso_features",
        "boundary",
        "regression_limits",
    ] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            verify_local_wal_benchmark_evidence(&missing).is_err(),
            "missing {field} was accepted"
        );
    }
    for field in [
        "operations_per_second",
        "p50_us",
        "p95_us",
        "p99_us",
        "database_bytes",
        "cpu_time_ms",
        "peak_rss_bytes",
    ] {
        let mut nonpositive = valid.clone();
        nonpositive[field] = serde_json::json!(0);
        assert!(
            verify_local_wal_benchmark_evidence(&nonpositive).is_err(),
            "nonpositive {field} was accepted"
        );
    }
    let mut zero_batch = valid.clone();
    zero_batch["batch_sizes"] = serde_json::json!([1, 0, 1000]);
    assert!(verify_local_wal_benchmark_evidence(&zero_batch).is_err());
    for (field, invalid) in [
        ("turso_version", serde_json::json!("0.8.0")),
        ("turso_features", serde_json::json!(["local", "sync"])),
        ("boundary", serde_json::json!("embedded_replica")),
    ] {
        let mut evidence = valid.clone();
        evidence[field] = invalid;
        assert!(
            verify_local_wal_benchmark_evidence(&evidence).is_err(),
            "invalid {field} was accepted"
        );
    }
    for field in ["min_operations_per_second_ratio", "max_p99_ratio"] {
        let mut evidence = valid.clone();
        evidence["regression_limits"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            verify_local_wal_benchmark_evidence(&evidence).is_err(),
            "missing regression limit {field} was accepted"
        );
    }
}

fn process_cpu_ms() -> f64 {
    let stat = fs::read_to_string("/proc/self/stat").expect("Linux /proc is required by this cut");
    let fields = stat
        .rsplit_once(')')
        .expect("valid /proc/self/stat command field")
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    let ticks_per_second = Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(100.0);
    ticks as f64 * 1_000.0 / ticks_per_second
}

fn peak_rss_bytes() -> u64 {
    fs::read_to_string("/proc/self/status")
        .expect("Linux /proc is required by this cut")
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .expect("VmHWM is present")
        * 1_024
}

fn local_database_bytes(directory: &Path, stem: &str) -> u64 {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(stem))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

#[tokio::test]
async fn turso_local_wal_benchmark_smoke_produces_verifiable_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection.db");
    let mut definition = qdef();
    definition.max_push_batch_size = 2_000;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());

    // Cold open and fixture construction are intentionally complete before either clock starts.
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    let mut pushed = Vec::with_capacity(1_101);
    let mut batches = Vec::new();
    for count in [1, 100, 1_000] {
        let (items, _, updates) = batch_fixture(count);
        pushed.extend(items);
        batches.push(updates);
    }
    let pushed_ids = pushed.iter().map(|item| item.item_id).collect::<Vec<_>>();
    AsyncProjectionStore::apply_live(
        &store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand { items: pushed }),
            pushed_ids,
        )],
    )
    .await
    .unwrap();

    let cpu_start = process_cpu_ms();
    let total_start = Instant::now();
    let mut next_sequence = 1_u64;
    let mut per_operation_us = Vec::new();
    for updates in batches {
        let count = updates.len();
        let commands = updates
            .into_iter()
            .map(|update| {
                let item_id = update.item_id;
                envelope(QueueCommand::UpdateFields(update), vec![item_id])
            })
            .collect::<Vec<_>>();
        let positions = (0..count)
            .map(|offset| CommandPosition::new(shard.clone(), 0, next_sequence + offset as u64))
            .collect();
        let batch_start = Instant::now();
        AsyncProjectionStore::apply_live(&store, positions, commands)
            .await
            .unwrap();
        let elapsed_us = batch_start.elapsed().as_secs_f64() * 1_000_000.0;
        per_operation_us.push(elapsed_us / count as f64);
        next_sequence += count as u64;
    }
    let total = total_start.elapsed();
    let cpu_time_ms = process_cpu_ms() - cpu_start;
    per_operation_us.sort_by(f64::total_cmp);
    let evidence = serde_json::json!({
        "turso_version": TURSO_SUPPORTED_VERSION,
        "turso_features": ["local"],
        "boundary": TURSO_SUPPORTED_BOUNDARY,
        "batch_sizes": [1, 100, 1000],
        "operations_per_second": 1101.0 / total.as_secs_f64(),
        "p50_us": per_operation_us[1],
        "p95_us": per_operation_us[2],
        "p99_us": per_operation_us[2],
        "database_bytes": local_database_bytes(directory.path(), "projection.db"),
        "cpu_time_ms": cpu_time_ms,
        "peak_rss_bytes": peak_rss_bytes(),
        "excluded_time": {"cold_open": true, "fixture_generation": true},
        "regression_limits": {
            "min_operations_per_second_ratio": 0.8,
            "max_p99_ratio": 1.25
        },
        "measurement": {
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "latency_unit": "microseconds_per_updated_item",
            "database_bytes_include": ["main", "wal", "shm"],
            "cpu_source": "/proc/self/stat",
            "rss_source": "/proc/self/status VmHWM"
        }
    });
    verify_local_wal_benchmark_evidence(&evidence).unwrap();
    println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
}

#[tokio::test]
async fn unqualified_mvcc_mode_fails_closed() {
    let error =
        TursoRelational::open(TursoConfig::in_memory().with_journal_mode(JournalMode::Mvcc))
            .await
            .err()
            .expect("MVCC must remain outside the qualified boundary");
    assert!(error.to_string().contains("MVCC is unsupported"));
}
