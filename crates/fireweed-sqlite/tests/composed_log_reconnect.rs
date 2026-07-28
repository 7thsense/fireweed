//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed sqlite-LOG +
//! in-memory-projection backend (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`).
//!
//! Each scenario opens the SAME durable sqlite log, commits, drops the handle (simulated crash), then
//! REOPENS a fresh `ComposedBackend` over the same file. The composition's `recover()` enumerates the
//! durable queue catalog (the log's `queue_defs` table) and rebuilds the fresh in-memory projection by
//! replaying the durable command log — recovering identically to the monolithic `SqliteBackend`, which
//! `reconnect_smoke.rs` runs the same suite against. Proves the bare composition no longer loses state on
//! restart (the gap ADR-012 P2 closes). The db path is keyed by the test's thread id (see reconnect_smoke).

use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{GateKeyPolicy, GroupKey, ItemId, PriorityValue, QueueId, RequestId, TenantId};
use fireweed_engine::{
    Backend, ChangeRecordSink, ClaimPort, ControlPlaneStore, DiscoveryGranularity, DiscoveryPort,
    EngineError, LogStore, ProjectionRead, PushPort, PushSpec, SetGatesCommand, SetGatesPort,
};
use fireweed_sqlite::composed_sqlite_backend;
use std::cell::Cell;
use std::sync::Mutex;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "fireweed-composed-log-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> fireweed_sqlite::ComposedSqliteBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_backend(&p).expect("open composed sqlite-log reconnect db")
}

fireweed_conformance::durable_reconnect_suite!(make);

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "fireweed-composed-log-reconnect-{}-{}.db",
            std::process::id(),
            tag
        ))
        .to_str()
        .unwrap()
        .to_string()
}

type SinkBatch = Vec<(TenantId, QueueId, Option<ItemId>, u64, u64)>;

#[derive(Default)]
struct RecordingSink {
    state: Mutex<RecordingSinkState>,
}

#[derive(Default)]
struct RecordingSinkState {
    batches: Vec<SinkBatch>,
}

impl RecordingSink {
    fn batches(&self) -> Vec<SinkBatch> {
        self.state.lock().expect("sink poisoned").batches.clone()
    }
}

impl ChangeRecordSink for RecordingSink {
    fn emit(
        &self,
        _shard: &fireweed_engine::QueueKey,
        records: &[fireweed_engine::ChangeRecord],
    ) -> fireweed_engine::EngineResult<()> {
        let mut state = self.state.lock().expect("sink poisoned");
        state
            .batches
            .push(records.iter().map(|r| r.idempotency_key()).collect());
        Ok(())
    }
}

#[tokio::test]
async fn emission_cursor_persists_across_reopen_sqlite_log() {
    let path = unique_path("emission-cursor");
    let _ = std::fs::remove_file(&path);

    let backend = composed_sqlite_backend(&path).expect("open composed sqlite-log db");
    backend.create_queue(qdef()).await.unwrap();
    let first = backend
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let second = backend
        .push(&shard(), vec![PushSpec::default()], ts(1), None)
        .await
        .unwrap();

    let sink = RecordingSink::default();
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        None
    );
    assert_eq!(
        backend
            .emit_change_record_tail(&shard(), &sink, 1, ts(2), None)
            .unwrap(),
        1
    );
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        Some(fireweed_engine::CommandPosition::new(shard(), 0, 0))
    );
    assert_eq!(
        sink.batches(),
        vec![vec![(
            shard().tenant_id.clone(),
            shard().queue_id.clone(),
            Some(first[0]),
            0,
            0
        )]]
    );

    drop(backend);

    let reopened = composed_sqlite_backend(&path).expect("reopen composed sqlite-log db");
    assert_eq!(
        reopened.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        Some(fireweed_engine::CommandPosition::new(shard(), 0, 0))
    );

    let reopened_sink = RecordingSink::default();
    assert_eq!(
        reopened
            .emit_change_record_tail(&shard(), &reopened_sink, 10, ts(3), None)
            .unwrap(),
        1
    );
    assert_eq!(
        reopened.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        Some(fireweed_engine::CommandPosition::new(shard(), 0, 1))
    );
    assert_eq!(
        reopened_sink.batches(),
        vec![vec![(
            shard().tenant_id.clone(),
            shard().queue_id.clone(),
            Some(second[0]),
            0,
            1
        )]]
    );
}

#[tokio::test]
async fn request_id_replay_and_conflict_survive_composed_sqlite_log_reopen() {
    let path = unique_path("request-id-reopen");
    let _ = std::fs::remove_file(&path);
    let request_id = RequestId::new("sqlite-compose-log-request-1").unwrap();
    let body = vec![PushSpec {
        priority: Some(PriorityValue::Int64(11)),
        ..PushSpec::default()
    }];

    let first = {
        let backend = composed_sqlite_backend(&path).expect("open composed sqlite-log db");
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
            .await
            .unwrap()
    };

    let reopened = composed_sqlite_backend(&path).expect("reopen composed sqlite-log db");
    let replay = reopened
        .push_with_request_id(&shard(), request_id.clone(), body, ts(0), None)
        .await
        .unwrap();
    assert!(first.is_fresh());
    assert!(replay.is_replayed());
    assert_eq!(replay.item_ids, first.item_ids);
    assert_eq!(reopened.metrics(&shard()).await.unwrap().pending, 1);

    let conflict = reopened
        .push_with_request_id(
            &shard(),
            request_id,
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(12)),
                ..PushSpec::default()
            }],
            ts(0),
            None,
        )
        .await;
    assert_eq!(conflict, Err(EngineError::RequestIdConflict));
    assert_eq!(reopened.metrics(&shard()).await.unwrap().pending, 1);

    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn gate_state_and_item_membership_survive_log_replay() {
    let path = unique_path("gates-reopen");
    let _ = std::fs::remove_file(&path);
    let item_id = {
        let backend = composed_sqlite_backend(&path).expect("open composed sqlite-log db");
        let mut definition = qdef();
        definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
        definition.eligibility_policy.max_gate_keys_per_item = Some(4);
        definition.eligibility_policy.max_gates_per_request = Some(4);
        backend.create_queue(definition).await.unwrap();
        assert!(backend.supports_gates());
        let id = backend
            .push(
                &shard(),
                vec![PushSpec {
                    gate_keys: vec!["hold".to_string()],
                    ..Default::default()
                }],
                ts(0),
                None,
            )
            .await
            .unwrap()[0];
        backend
            .set_gates(
                &shard(),
                SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: true,
                },
                ts(1),
                None,
            )
            .await
            .unwrap();
        assert!(
            backend
                .claim(fireweed_conformance::claim_req(1, 60, 2))
                .await
                .unwrap()
                .items
                .is_empty()
        );
        id
    };

    let reopened = composed_sqlite_backend(&path).expect("reopen composed sqlite-log db");
    assert!(reopened.supports_gates());
    assert!(
        reopened
            .claim(fireweed_conformance::claim_req(1, 60, 3))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    reopened
        .set_gates(
            &shard(),
            SetGatesCommand {
                gate_keys: vec!["hold".to_string()],
                blocked: false,
            },
            ts(4),
            None,
        )
        .await
        .unwrap();
    let claimed = reopened
        .claim(fireweed_conformance::claim_req(1, 60, 5))
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, item_id);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn active_scope_timestamps_and_counts_survive_log_replay() {
    let path = unique_path("discovery-reopen");
    let _ = std::fs::remove_file(&path);
    {
        let backend = composed_sqlite_backend(&path).expect("open composed sqlite-log db");
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push(
                &shard(),
                vec![
                    PushSpec {
                        group_key: Some(GroupKey::new("older").unwrap()),
                        ..Default::default()
                    },
                    PushSpec {
                        group_key: Some(GroupKey::new("older").unwrap()),
                        ..Default::default()
                    },
                ],
                ts(10),
                None,
            )
            .await
            .unwrap();
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    group_key: Some(GroupKey::new("scheduled").unwrap()),
                    not_before: Some(ts(50)),
                    ..Default::default()
                }],
                ts(20),
                None,
            )
            .await
            .unwrap();
    }

    let reopened = composed_sqlite_backend(&path).expect("reopen composed sqlite-log db");
    let scopes = reopened
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(100))
        .await
        .unwrap();
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].group_key.as_deref(), Some("older"));
    assert_eq!(scopes[0].oldest_eligible_age_ms, 90_000);
    assert_eq!(scopes[0].eligible_count, Some(2));
    assert_eq!(scopes[1].group_key.as_deref(), Some("scheduled"));
    assert_eq!(scopes[1].oldest_eligible_age_ms, 50_000);
    assert_eq!(scopes[1].eligible_count, Some(1));

    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
