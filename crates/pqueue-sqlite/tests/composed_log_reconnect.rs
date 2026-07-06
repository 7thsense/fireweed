//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed sqlite-LOG +
//! in-memory-projection backend (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`).
//!
//! Each scenario opens the SAME durable sqlite log, commits, drops the handle (simulated crash), then
//! REOPENS a fresh `ComposedBackend` over the same file. The composition's `recover()` enumerates the
//! durable queue catalog (the log's `queue_defs` table) and rebuilds the fresh in-memory projection by
//! replaying the durable command log — recovering identically to the monolithic `SqliteBackend`, which
//! `reconnect_smoke.rs` runs the same suite against. Proves the bare composition no longer loses state on
//! restart (the gap ADR-012 P2 closes). The db path is keyed by the test's thread id (see reconnect_smoke).

use pqueue_conformance::{qdef, shard, ts};
use pqueue_core::{ItemId, QueueId, TenantId};
use pqueue_engine::{ChangeRecordSink, ControlPlaneStore, LogStore, PushPort, PushSpec};
use pqueue_sqlite::composed_sqlite_backend;
use std::cell::Cell;
use std::sync::Mutex;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-log-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> pqueue_sqlite::ComposedSqliteBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_backend(&p).expect("open composed sqlite-log reconnect db")
}

pqueue_conformance::durable_reconnect_suite!(make);

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-log-reconnect-{}-{}.db",
            std::process::id(),
            tag
        ))
        .to_str()
        .unwrap()
        .to_string()
}

#[derive(Default)]
struct RecordingSink {
    state: Mutex<RecordingSinkState>,
}

#[derive(Default)]
struct RecordingSinkState {
    batches: Vec<Vec<(TenantId, QueueId, Option<ItemId>, u64, u64)>>,
}

impl RecordingSink {
    fn batches(&self) -> Vec<Vec<(TenantId, QueueId, Option<ItemId>, u64, u64)>> {
        self.state.lock().expect("sink poisoned").batches.clone()
    }
}

impl ChangeRecordSink for RecordingSink {
    fn emit(
        &self,
        _shard: &pqueue_engine::QueueKey,
        records: &[pqueue_engine::ChangeRecord],
    ) -> pqueue_engine::EngineResult<()> {
        let mut state = self.state.lock().expect("sink poisoned");
        state
            .batches
            .push(records.iter().map(|r| r.idempotency_key()).collect());
        Ok(())
    }
}

#[tokio::test]
async fn TestEmissionCursorPersistsAcrossReopen_SqliteLog() {
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
        Some(pqueue_engine::CommandPosition::new(shard(), 0, 0))
    );
    assert_eq!(
        sink.batches(),
        vec![vec![(
            shard().tenant_id.clone(),
            shard().queue_id.clone(),
            Some(first[0].clone()),
            0,
            0
        )]]
    );

    drop(backend);

    let reopened = composed_sqlite_backend(&path).expect("reopen composed sqlite-log db");
    assert_eq!(
        reopened.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        Some(pqueue_engine::CommandPosition::new(shard(), 0, 0))
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
        Some(pqueue_engine::CommandPosition::new(shard(), 0, 1))
    );
    assert_eq!(
        reopened_sink.batches(),
        vec![vec![(
            shard().tenant_id.clone(),
            shard().queue_id.clone(),
            Some(second[0].clone()),
            0,
            1
        )]]
    );
}
