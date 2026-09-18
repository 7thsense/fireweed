//! Native Turso implementation of the shared relational apply/query interface.

use fireweed_engine::{EngineError, EngineResult};
use fireweed_relational::{RelRow, RelTx, RelValue};
use turso::{Connection, Value};

pub struct TursoRel<'a>(pub &'a Connection);

/// Reuse execution state only within one owned apply transaction. Compiled-SQL
/// caching alone still allocates a new VM and tracked statement for every row.
pub(crate) struct ApplyTursoRel<'a> {
    inner: TursoRel<'a>,
    statements: std::cell::RefCell<std::collections::HashMap<String, (usize, turso::Statement)>>,
}

impl<'a> ApplyTursoRel<'a> {
    pub(crate) fn new(connection: &'a Connection) -> Self {
        Self {
            inner: TursoRel(connection),
            statements: Default::default(),
        }
    }
}

impl ApplyTursoRel<'_> {
    fn execute_values(&self, sql: &str, params: Vec<Value>) -> EngineResult<usize> {
        if !USE_LOCAL_RT.get() {
            return self.inner.execute_values(sql, params);
        }
        // Keep a fixed positional-bind shape for each reused execution object.
        // The SDK resets both VM state and bindings before execute.
        let cached = self
            .statements
            .borrow()
            .get(sql)
            .filter(|(count, _)| *count == params.len())
            .map(|(_, statement)| statement.clone());
        block_on_local(async {
            let mut statement = match cached {
                Some(statement) => statement,
                None => {
                    let statement = self.inner.0.prepare_cached(sql).await.map_err(storage)?;
                    let mut cache = self.statements.borrow_mut();
                    if cache.len() >= 32 {
                        cache.clear();
                    }
                    cache.insert(sql.to_owned(), (params.len(), statement.clone()));
                    statement
                }
            };
            // execute resets VM cursors before rebinding; nothing escapes the
            // apply's connection/transaction or overlaps another statement.
            statement
                .execute(params)
                .await
                .map(|changed| changed as usize)
                .map_err(storage)
        })
    }
}

impl RelTx for ApplyTursoRel<'_> {
    fn prefer_point_updates(&self) -> bool {
        true
    }

    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let started = std::time::Instant::now();
        let result = self.execute_values(sql, params.iter().map(to_turso).collect());
        crate::projection::trace_sql(
            sql,
            params.len(),
            result.as_ref().copied().unwrap_or(0),
            started.elapsed(),
        );
        result
    }

    fn execute_owned(&self, sql: &str, params: Vec<RelValue>) -> EngineResult<usize> {
        let started = std::time::Instant::now();
        let binds = params.len();
        let result = self.execute_values(sql, params.into_iter().map(into_turso).collect());
        crate::projection::trace_sql(
            sql,
            binds,
            result.as_ref().copied().unwrap_or(0),
            started.elapsed(),
        );
        result
    }

    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
        self.inner.query(sql, params)
    }
}

#[cfg(test)]
mod apply_statement_reuse_tests {
    use super::*;

    #[tokio::test]
    async fn consumed_row_values_survive_statement_reuse_and_teardown() {
        let database = turso::Builder::new_local(":memory:").build().await.unwrap();
        let connection = database.connect().unwrap();
        let mut statement = connection
            .prepare_cached("SELECT ?1,?2,?3,?4,?5")
            .await
            .unwrap();
        let expected = vec![
            Value::Null,
            Value::Integer(i64::MIN),
            Value::Real(1.25),
            Value::Text("metadata \0 λ".repeat(4096)),
            Value::Blob([0, 255, 31, 128].repeat(16384)),
        ];
        let mut rows = statement.query(expected.clone()).await.unwrap();
        let held: Vec<_> = rows.next().await.unwrap().unwrap().into_values().collect();
        assert!(rows.next().await.unwrap().is_none());
        drop(rows);
        let replacement = vec![Value::Integer(7); 5];
        let mut rows = statement.query(replacement.clone()).await.unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .into_values()
                .collect::<Vec<_>>(),
            replacement
        );
        assert!(rows.next().await.unwrap().is_none());
        drop(rows);
        drop(statement);
        drop(connection);
        drop(database);
        assert_eq!(held, expected);
    }

    #[tokio::test]
    async fn reused_writes_rebind_nulls_and_release_state_before_rollback() {
        let database = turso::Builder::new_local(":memory:").build().await.unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute(
                "CREATE TABLE reuse_test(id INTEGER PRIMARY KEY, value TEXT UNIQUE, payload BLOB)",
                (),
            )
            .await
            .unwrap();
        run_reltx_blocking(move || {
            let plain = TursoRel(&connection);
            plain.execute("BEGIN", &[]).unwrap();
            {
                let cached = ApplyTursoRel::new(&connection);
                let insert = "INSERT INTO reuse_test VALUES(?1,?2,?3)";
                for id in 0..1024 {
                    assert_eq!(
                        cached
                            .execute_owned(
                                insert,
                                vec![
                                    RelValue::Integer(id),
                                    RelValue::Text(format!("value-{id}")),
                                    RelValue::Blob(vec![id as u8; (id as usize % 257) + 1]),
                                ],
                            )
                            .unwrap(),
                        1
                    );
                }
                // Rebinding the cached statement must not change previously
                // inserted text/blob buffers after their input vectors are gone.
                for id in [0_i64, 511, 1023] {
                    let rows = cached
                        .query(
                            "SELECT value,payload FROM reuse_test WHERE id=?1",
                            &[RelValue::Integer(id)],
                        )
                        .unwrap();
                    assert_eq!(rows[0].get::<String>(0).unwrap(), format!("value-{id}"));
                    assert_eq!(
                        rows[0].get::<Vec<u8>>(1).unwrap(),
                        vec![id as u8; (id as usize % 257) + 1]
                    );
                }
                let update = "UPDATE reuse_test SET value=?2 WHERE id=?1";
                for id in 0..1024 {
                    assert_eq!(
                        cached
                            .execute(update, &[RelValue::Integer(id), RelValue::Null])
                            .unwrap(),
                        1
                    );
                }
                // A uniqueness error must still abort the caller's transaction;
                // dropping the cache must release all statement state.
                assert!(
                    cached
                        .execute(
                            insert,
                            &[RelValue::Integer(0), RelValue::Null, RelValue::Null],
                        )
                        .is_err()
                );
            }
            plain.execute("ROLLBACK", &[]).unwrap();
            assert_eq!(
                plain.query("SELECT COUNT(*) FROM reuse_test", &[]).unwrap()[0]
                    .get::<i64>(0)
                    .unwrap(),
                0
            );
            plain.execute("BEGIN", &[]).unwrap();
            {
                let cached = ApplyTursoRel::new(&connection);
                cached
                    .execute(
                        "INSERT INTO reuse_test VALUES(?1,?2,NULL)",
                        &[RelValue::Integer(1), RelValue::Text("fresh".into())],
                    )
                    .unwrap();
            }
            plain.execute("COMMIT", &[]).unwrap();
            assert_eq!(
                plain.query("SELECT value FROM reuse_test", &[]).unwrap()[0]
                    .get::<String>(0)
                    .unwrap(),
                "fresh"
            );
        })
        .await;
    }
}

pub fn to_turso(value: &RelValue) -> Value {
    match value {
        RelValue::Null => Value::Null,
        RelValue::Integer(value) => Value::Integer(*value),
        RelValue::Real(value) => Value::Real(*value),
        RelValue::Text(value) => Value::Text(value.clone()),
        RelValue::Blob(value) => Value::Blob(value.clone()),
    }
}

fn into_turso(value: RelValue) -> Value {
    match value {
        RelValue::Null => Value::Null,
        RelValue::Integer(value) => Value::Integer(value),
        RelValue::Real(value) => Value::Real(value),
        RelValue::Text(value) => Value::Text(value),
        RelValue::Blob(value) => Value::Blob(value),
    }
}

pub fn from_turso(value: Value) -> RelValue {
    match value {
        Value::Null => RelValue::Null,
        Value::Integer(value) => RelValue::Integer(value),
        Value::Real(value) => RelValue::Real(value),
        Value::Text(value) => RelValue::Text(value),
        Value::Blob(value) => RelValue::Blob(value),
    }
}

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}

thread_local! {
    static LOCAL_RT: std::cell::OnceCell<tokio::runtime::Runtime> = const { std::cell::OnceCell::new() };
    static USE_LOCAL_RT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run RelTx work on a blocking thread with a thread-local current-thread runtime so each
/// statement is `block_on` locally (no object-log `block_in_place`, no per-statement channel hop).
pub async fn run_reltx_blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    if tokio::runtime::Handle::try_current().is_err() {
        return match std::thread::Builder::new()
            .name("turso-reltx-blocking".into())
            .spawn(move || run_with_local_runtime(work))
            .expect("turso RelTx blocking thread")
            .join()
        {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
    }

    tokio::task::spawn_blocking(move || run_with_local_runtime(work))
        .await
        .expect("turso RelTx blocking hop")
}

fn run_with_local_runtime<T>(work: impl FnOnce() -> T) -> T {
    struct ResetLocalRuntime;

    impl Drop for ResetLocalRuntime {
        fn drop(&mut self) {
            USE_LOCAL_RT.set(false);
        }
    }

    USE_LOCAL_RT.set(true);
    let _reset = ResetLocalRuntime;
    work()
}

fn block_on_local<T>(future: impl std::future::Future<Output = T>) -> T {
    LOCAL_RT.with(|slot| {
        let rt = slot.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("turso RelTx local runtime")
        });
        rt.block_on(future)
    })
}

/// Drive an owned native apply only from a blocking worker. This keeps native
/// Unix VFS calls made by async commit/checkpoint off the application's workers.
/// RelTx retains its existing separate hop, so its local block_on is never
/// nested inside this current-thread runtime.
pub(crate) fn block_on_owned_apply<T>(future: impl std::future::Future<Output = T>) -> T {
    block_on_local(future)
}

fn block_on_turso<T: Send + 'static>(
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    if USE_LOCAL_RT.get() {
        return block_on_local(future);
    }
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) | Err(_) => {
            // Current-thread tests cannot nest `block_on` or `block_in_place`. Drive Turso on a
            // process-wide worker so we do not build a runtime per statement.
            turso_reltx_worker().block(future)
        }
    }
}

type TursoRelTxJob = Box<dyn FnOnce(&tokio::runtime::Handle) + Send>;

struct TursoRelTxWorker {
    jobs: std::sync::mpsc::Sender<TursoRelTxJob>,
}

impl TursoRelTxWorker {
    fn block<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.jobs
            .send(Box::new(move |handle| {
                let _ = tx.send(handle.block_on(future));
            }))
            .expect("turso RelTx worker");
        rx.recv().expect("turso RelTx worker")
    }
}

fn turso_reltx_worker() -> &'static TursoRelTxWorker {
    static WORKER: std::sync::OnceLock<TursoRelTxWorker> = std::sync::OnceLock::new();
    WORKER.get_or_init(|| {
        let (jobs_tx, jobs_rx) = std::sync::mpsc::channel::<TursoRelTxJob>();
        std::thread::Builder::new()
            .name("turso-reltx".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .expect("turso RelTx runtime");
                let handle = runtime.handle().clone();
                while let Ok(job) = jobs_rx.recv() {
                    job(&handle);
                }
            })
            .expect("turso RelTx thread");
        TursoRelTxWorker { jobs: jobs_tx }
    })
}

impl TursoRel<'_> {
    fn execute_values(&self, sql: &str, params: Vec<Value>) -> EngineResult<usize> {
        if USE_LOCAL_RT.get() {
            return block_on_local(async {
                let mut stmt = self.0.prepare_cached(sql).await.map_err(storage)?;
                stmt.execute(params)
                    .await
                    .map(|changed| changed as usize)
                    .map_err(storage)
            });
        }
        let conn = self.0.clone();
        let sql = sql.to_string();
        block_on_turso(async move {
            let mut stmt = conn.prepare_cached(&sql).await.map_err(storage)?;
            stmt.execute(params)
                .await
                .map(|changed| changed as usize)
                .map_err(storage)
        })
    }
}

impl RelTx for TursoRel<'_> {
    fn prefer_point_updates(&self) -> bool {
        true
    }

    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let started = std::time::Instant::now();
        let result = self.execute_values(sql, params.iter().map(to_turso).collect());
        crate::projection::trace_sql(
            sql,
            params.len(),
            result.as_ref().copied().unwrap_or(0),
            started.elapsed(),
        );
        result
    }

    fn execute_owned(&self, sql: &str, params: Vec<RelValue>) -> EngineResult<usize> {
        let started = std::time::Instant::now();
        let binds = params.len();
        let result = self.execute_values(sql, params.into_iter().map(into_turso).collect());
        crate::projection::trace_sql(
            sql,
            binds,
            result.as_ref().copied().unwrap_or(0),
            started.elapsed(),
        );
        result
    }

    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
        let started = std::time::Instant::now();
        let binds = params.len();
        let params: Vec<Value> = params.iter().map(to_turso).collect();
        let result = if USE_LOCAL_RT.get() {
            block_on_local(async {
                let mut stmt = self.0.prepare_cached(sql).await.map_err(storage)?;
                let mut rows = stmt.query(params).await.map_err(storage)?;
                let mut collected = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    collected.push(RelRow(row.into_values().map(from_turso).collect()));
                }
                Ok(collected)
            })
        } else {
            let conn = self.0.clone();
            let owned_sql = sql.to_string();
            block_on_turso(async move {
                let mut stmt = conn.prepare_cached(&owned_sql).await.map_err(storage)?;
                let mut rows = stmt.query(params).await.map_err(storage)?;
                let mut collected = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    collected.push(RelRow(row.into_values().map(from_turso).collect()));
                }
                Ok(collected)
            })
        };
        crate::projection::trace_sql(
            sql,
            binds,
            result.as_ref().map_or(0, Vec::len),
            started.elapsed(),
        );
        result
    }
}

#[cfg(test)]
mod packed_authority_first_tests {
    use bytes::Bytes;
    use fireweed_conformance::{envelope, item, qdef, ts};
    use fireweed_core::{GroupKey, ItemId, ItemState, LeaseToken, WorkerId};
    use fireweed_engine::{
        AsyncProjectionStore, ClaimCommand, CommandEnvelope, CommandPosition, EngineError,
        FinalizeCommand, FinalizeKind, FinalizeOutcome, LeaseExpiredCommand, PayloadUpdate,
        PushCommand, QueueCommand, QueueKey, ScheduleUpdate, UpdateFieldsBatchCommand,
        UpdateFieldsCommand,
    };
    use fireweed_relational::AUTHORITY_FIRST_CLAIM_SHORT_MOVE;
    use turso::Value;

    use crate::TursoRelational;

    fn claim_envelope(
        ids: Vec<ItemId>,
        token: &str,
        expiry: i64,
        worker: &str,
        authority_first: bool,
    ) -> CommandEnvelope {
        let mut command = envelope(
            QueueCommand::Claim(
                ClaimCommand::new(
                    ids.clone(),
                    LeaseToken::new(token).unwrap(),
                    ts(expiry),
                    Some(WorkerId::new(worker).unwrap()),
                )
                .with_authority_first(),
            ),
            ids,
        );
        if !authority_first && let QueueCommand::Claim(claim) = &mut command.command {
            claim.authority_first = false;
        }
        command
    }

    fn complete_envelope(ids: Vec<ItemId>) -> CommandEnvelope {
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: ids
                    .iter()
                    .copied()
                    .map(|item_id| FinalizeOutcome::new(item_id, FinalizeKind::Complete))
                    .collect(),
            }),
            ids,
        )
    }

    fn fifo_item(id: &str, key: &str) -> fireweed_engine::PushItem {
        let mut item = item(id, key, 0);
        item.priority = None;
        item
    }

    fn expired_envelope(ids: Vec<ItemId>, now: i64) -> CommandEnvelope {
        let mut command = envelope(
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: ids.clone(),
            }),
            ids,
        );
        command.created_at = ts(now);
        command
    }

    async fn open_store() -> (TursoRelational, QueueKey) {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let store = TursoRelational::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition)
            .await
            .unwrap();
        (store, shard)
    }

    async fn apply(
        store: &TursoRelational,
        shard: &QueueKey,
        start: u64,
        commands: Vec<CommandEnvelope>,
    ) -> fireweed_engine::EngineResult<()> {
        let positions = (0..commands.len() as u64)
            .map(|offset| CommandPosition::new(shard.clone(), 0, start + offset))
            .collect();
        AsyncProjectionStore::apply_live(store, positions, commands).await
    }

    #[tokio::test]
    async fn purge_reclaims_payload_sidecars_across_cycles() {
        let (store, shard) = open_store().await;
        for cycle in 0..8 {
            let items: Vec<_> = (1..=32)
                .map(|i| {
                    let n = cycle * 32 + i;
                    let mut row = item(&n.to_string(), &format!("payload-{n}"), n);
                    row.payload = Some(Bytes::from(vec![n as u8; 8192]));
                    row
                })
                .collect();
            let ids = items.iter().map(|i| i.item_id).collect::<Vec<_>>();
            apply(
                &store,
                &shard,
                cycle as u64 * 2,
                vec![envelope(
                    QueueCommand::Push(PushCommand { items }),
                    ids.clone(),
                )],
            )
            .await
            .unwrap();
            let rows = store
                .query("SELECT count(*) FROM fireweed_item_payloads", vec![])
                .await
                .unwrap();
            assert_eq!(rows[0].values[0], Value::Integer(32));
            apply(
                &store,
                &shard,
                cycle as u64 * 2 + 1,
                vec![envelope(
                    QueueCommand::PurgeItems(fireweed_engine::PurgeItemsCommand {
                        item_ids: ids.clone(),
                        force: false,
                    }),
                    ids,
                )],
            )
            .await
            .unwrap();
            let rows = store
                .query("SELECT count(*) FROM fireweed_item_payloads", vec![])
                .await
                .unwrap();
            assert_eq!(
                rows[0].values[0],
                Value::Integer(0),
                "cycle {cycle} leaked bodies"
            );
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RowModel {
        state: String,
        worker: Option<String>,
        expiry: Option<i64>,
        token: Option<String>,
        bearer: Option<String>,
        version: i64,
        retry: i64,
    }

    async fn row_model(store: &TursoRelational, shard: &QueueKey, id: ItemId) -> RowModel {
        let tenant = Value::Text(shard.tenant_id.as_str().to_string());
        let queue = Value::Text(shard.queue_id.as_str().to_string());
        let item = Value::Text(id.to_string());
        let rows = store
            .query(
                "SELECT lifecycle_state, worker_id, lease_expires_at, item_version, retry_count \
                 FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![tenant.clone(), queue.clone(), item.clone()],
            )
            .await
            .unwrap();
        let values = &rows[0].values;
        let bearer = store
            .query(
                "SELECT lease_token FROM fireweed_lease_bearers \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                vec![tenant, queue, item],
            )
            .await
            .unwrap();
        let tokens = store.live_tokens.lock().await;
        RowModel {
            state: match &values[0] {
                Value::Text(state) => state.clone(),
                other => panic!("state {other:?}"),
            },
            worker: match &values[1] {
                Value::Null => None,
                Value::Text(worker) => Some(worker.clone()),
                other => panic!("worker {other:?}"),
            },
            expiry: match &values[2] {
                Value::Null => None,
                Value::Integer(expiry) => Some(*expiry),
                other => panic!("expiry {other:?}"),
            },
            token: tokens
                .get(&(shard.clone(), id))
                .map(|token| token.as_str().to_string()),
            bearer: bearer.first().map(|row| match &row.values[0] {
                Value::Text(token) => token.clone(),
                other => panic!("bearer {other:?}"),
            }),
            version: match &values[3] {
                Value::Integer(version) => *version,
                other => panic!("version {other:?}"),
            },
            retry: match &values[4] {
                Value::Integer(retry) => *retry,
                other => panic!("retry {other:?}"),
            },
        }
    }

    async fn group_summary(
        store: &TursoRelational,
        shard: &QueueKey,
        group: &GroupKey,
    ) -> (i64, Option<String>) {
        let rows = store
            .query(
                "SELECT eligible_item_count, rep_item_id FROM fireweed_group_summary \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                vec![
                    Value::Text(shard.tenant_id.as_str().to_string()),
                    Value::Text(shard.queue_id.as_str().to_string()),
                    Value::Text(group.as_str().to_string()),
                ],
            )
            .await
            .unwrap();
        let row = &rows[0].values;
        let count = match &row[0] {
            Value::Integer(count) => *count,
            other => panic!("count {other:?}"),
        };
        let rep = match &row[1] {
            Value::Null => None,
            Value::Text(rep) => Some(rep.clone()),
            other => panic!("rep {other:?}"),
        };
        (count, rep)
    }

    #[tokio::test]
    async fn packed_authority_first_claim_matches_solo_model_in_one_transaction() {
        let a = item("1", "k1", 1);
        let b = item("2", "k2", 2);
        let c = item("3", "k3", 3);
        let mut g1 = item("4", "g1", 1);
        let mut g2 = item("5", "g2", 2);
        let mut g3 = item("6", "g3", 3);
        let group = GroupKey::new("grp").unwrap();
        g1.group_key = Some(group.clone());
        g2.group_key = Some(group.clone());
        g3.group_key = Some(group.clone());
        let foreign = item("7", "k7", 7);
        let leftover = item("8", "k8", 8);
        let extra = item("9", "k9", 9);
        let legacy = item("10", "k10", 10);
        let all = vec![
            a.clone(),
            b.clone(),
            c.clone(),
            g1.clone(),
            g2.clone(),
            g3.clone(),
            foreign.clone(),
            leftover.clone(),
            extra.clone(),
            legacy.clone(),
        ];
        let ids: Vec<ItemId> = all.iter().map(|item| item.item_id).collect();

        let (packed, shard) = open_store().await;
        let (solo, solo_shard) = open_store().await;
        let push = envelope(
            QueueCommand::Push(PushCommand { items: all.clone() }),
            ids.clone(),
        );
        apply(&packed, &shard, 0, vec![push.clone()]).await.unwrap();
        apply(&solo, &solo_shard, 0, vec![push]).await.unwrap();

        let ordinary = vec![
            claim_envelope(vec![a.item_id], "tok-a", 100, "wa", true),
            claim_envelope(vec![b.item_id], "tok-b", 200, "wb", true),
            claim_envelope(vec![c.item_id], "tok-c", 300, "wc", true),
        ];
        apply(&packed, &shard, 1, ordinary.clone())
            .await
            .expect("packed ordinary claims");
        let packed_phase = packed
            .last_apply_phase_observation()
            .expect("one Immediate");
        assert!(packed_phase.begin_us > 0 || packed_phase.commit_us > 0);
        for (offset, command) in ordinary.into_iter().enumerate() {
            apply(&solo, &solo_shard, 1 + offset as u64, vec![command])
                .await
                .expect("solo claim");
        }
        for id in [a.item_id, b.item_id, c.item_id] {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "ordinary claim model {id}"
            );
        }
        let packed_claimed = AsyncProjectionStore::render_claimed(
            &packed,
            shard.clone(),
            vec![a.item_id, b.item_id, c.item_id],
        )
        .await
        .unwrap();
        let solo_claimed = AsyncProjectionStore::render_claimed(
            &solo,
            solo_shard.clone(),
            vec![a.item_id, b.item_id, c.item_id],
        )
        .await
        .unwrap();
        assert_eq!(packed_claimed.len(), 3);
        assert_eq!(
            packed_claimed
                .iter()
                .map(|item| (
                    item.item_id,
                    item.lease_token.clone(),
                    item.lease_expires_at
                ))
                .collect::<Vec<_>>(),
            solo_claimed
                .iter()
                .map(|item| (
                    item.item_id,
                    item.lease_token.clone(),
                    item.lease_expires_at
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            packed_claimed[0]
                .lease_token
                .as_ref()
                .map(|token| token.as_str()),
            Some("tok-a")
        );
        assert_eq!(
            packed_claimed[1]
                .lease_token
                .as_ref()
                .map(|token| token.as_str()),
            Some("tok-b")
        );
        assert_eq!(
            packed_claimed[2]
                .lease_token
                .as_ref()
                .map(|token| token.as_str()),
            Some("tok-c")
        );

        let fused = vec![
            claim_envelope(vec![g1.item_id], "tok-g1", 400, "wg1", true),
            complete_envelope(vec![g1.item_id]),
            claim_envelope(vec![g2.item_id], "tok-g2", 500, "wg2", true),
            complete_envelope(vec![g2.item_id]),
        ];
        apply(&packed, &shard, 4, fused.clone())
            .await
            .expect("packed fused claims");
        for (offset, command) in fused.into_iter().enumerate() {
            apply(&solo, &solo_shard, 4 + offset as u64, vec![command])
                .await
                .expect("solo fused neighbor");
        }
        for id in [g1.item_id, g2.item_id, g3.item_id] {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "fused grouped model {id}"
            );
        }
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), g1.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), g3.item_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        let packed_summary = group_summary(&packed, &shard, &group).await;
        let solo_summary = group_summary(&solo, &solo_shard, &group).await;
        assert_eq!(packed_summary, solo_summary);
        // Item Claim/Complete leave the Push-time head lagged until grouped Claim.
        assert_eq!(packed_summary, (3, Some(g1.item_id.to_string())));

        apply(
            &packed,
            &shard,
            8,
            vec![claim_envelope(
                vec![foreign.item_id],
                "foreign-token",
                600,
                "wf",
                true,
            )],
        )
        .await
        .unwrap();
        apply(
            &solo,
            &solo_shard,
            8,
            vec![claim_envelope(
                vec![foreign.item_id],
                "foreign-token",
                600,
                "wf",
                true,
            )],
        )
        .await
        .unwrap();
        let before_foreign = row_model(&packed, &shard, foreign.item_id).await;
        let before_left = row_model(&packed, &shard, leftover.item_id).await;
        let before_extra = row_model(&packed, &shard, extra.item_id).await;
        let poisoned = apply(
            &packed,
            &shard,
            9,
            vec![
                claim_envelope(
                    vec![foreign.item_id, leftover.item_id],
                    "attacker",
                    700,
                    "wa2",
                    true,
                ),
                claim_envelope(vec![extra.item_id], "tok-extra", 800, "we", true),
            ],
        )
        .await
        .expect_err("authority-first foreign token must poison");
        match poisoned {
            EngineError::Storage(message) => {
                assert!(
                    message.contains(AUTHORITY_FIRST_CLAIM_SHORT_MOVE),
                    "{message}"
                );
            }
            other => panic!("expected storage poison, got {other:?}"),
        }
        assert_eq!(
            row_model(&packed, &shard, foreign.item_id).await,
            before_foreign
        );
        assert_eq!(
            row_model(&packed, &shard, leftover.item_id).await,
            before_left
        );
        assert_eq!(
            row_model(&packed, &shard, extra.item_id).await,
            before_extra
        );
        assert_eq!(before_foreign.token.as_deref(), Some("foreign-token"));
        assert_eq!(before_foreign.bearer.as_deref(), Some("foreign-token"));

        apply(
            &packed,
            &shard,
            9,
            vec![claim_envelope(
                vec![legacy.item_id],
                "legacy-token",
                900,
                "wl",
                false,
            )],
        )
        .await
        .unwrap();
        apply(
            &packed,
            &shard,
            10,
            vec![claim_envelope(
                vec![legacy.item_id],
                "legacy-token",
                900,
                "wl",
                false,
            )],
        )
        .await
        .expect("legacy outbox claim replay must not poison");
        apply(
            &packed,
            &shard,
            11,
            vec![
                claim_envelope(vec![legacy.item_id], "legacy-token", 900, "wl", false),
                complete_envelope(vec![legacy.item_id]),
            ],
        )
        .await
        .expect("legacy fused already-this-token must not poison");
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), legacy.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
    }

    async fn seed_fifo_claimed(
        count: usize,
        seq: u64,
    ) -> (TursoRelational, QueueKey, Vec<ItemId>, u64) {
        let items: Vec<_> = (1..=count)
            .map(|index| fifo_item(&index.to_string(), &format!("fk{index}")))
            .collect();
        let ids: Vec<ItemId> = items.iter().map(|item| item.item_id).collect();
        let (store, shard) = open_store().await;
        let push = envelope(QueueCommand::Push(PushCommand { items }), ids.clone());
        apply(&store, &shard, seq, vec![push]).await.unwrap();
        let claims: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(offset, id)| {
                claim_envelope(
                    vec![*id],
                    &format!("tok-{offset}"),
                    50,
                    &format!("w{offset}"),
                    true,
                )
            })
            .collect();
        apply(&store, &shard, seq + 1, claims).await.unwrap();
        (store, shard, ids, seq + 1 + count as u64)
    }

    #[tokio::test]
    async fn packed_complete_is_non_rejecting_and_mixed_vector_matches_model() {
        let (three, three_shard, three_ids, three_next) = seed_fifo_claimed(3, 0).await;
        let completes: Vec<_> = three_ids
            .iter()
            .map(|id| complete_envelope(vec![*id]))
            .collect();
        apply(&three, &three_shard, three_next, completes)
            .await
            .expect("packed three completes");
        let three_shape = three
            .last_apply_statement_shape()
            .expect("three-complete statement shape");
        let three_phase = three
            .last_apply_phase_observation()
            .expect("three-complete Immediate");
        assert!(three_phase.begin_us > 0 || three_phase.commit_us > 0);

        let (eight, eight_shard, eight_ids, eight_next) = seed_fifo_claimed(8, 0).await;
        let completes: Vec<_> = eight_ids
            .iter()
            .map(|id| complete_envelope(vec![*id]))
            .collect();
        apply(&eight, &eight_shard, eight_next, completes)
            .await
            .expect("packed eight completes");
        let eight_shape = eight
            .last_apply_statement_shape()
            .expect("eight-complete statement shape");
        assert_eq!(
            three_shape.statement_count, eight_shape.statement_count,
            "coalesced Complete apply must not grow statements per envelope: three={three_shape:?} eight={eight_shape:?}"
        );
        assert!(
            three_shape.write_statement_count >= 2,
            "shape={three_shape:?}"
        );
        eprintln!("unfused complete three={three_shape:?} eight={eight_shape:?}");

        let a = fifo_item("1", "k1");
        let b = fifo_item("2", "k2");
        let c = fifo_item("3", "k3");
        let d = fifo_item("4", "k4");
        let e = fifo_item("5", "k5");
        let fifo_items = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone()];
        let fifo_ids: Vec<ItemId> = fifo_items.iter().map(|item| item.item_id).collect();
        let (packed, shard) = open_store().await;
        let (solo, solo_shard) = open_store().await;
        let push = envelope(
            QueueCommand::Push(PushCommand {
                items: fifo_items.clone(),
            }),
            fifo_ids.clone(),
        );
        apply(&packed, &shard, 0, vec![push.clone()]).await.unwrap();
        apply(&solo, &solo_shard, 0, vec![push]).await.unwrap();
        let claims = vec![
            claim_envelope(vec![a.item_id], "tok-a", 50, "wa", true),
            claim_envelope(vec![b.item_id], "tok-b", 50, "wb", true),
            claim_envelope(vec![c.item_id], "tok-c", 50, "wc", true),
            claim_envelope(vec![d.item_id], "tok-d", 50, "wd", true),
            claim_envelope(vec![e.item_id], "tok-e", 50, "we", true),
        ];
        apply(&packed, &shard, 1, claims.clone()).await.unwrap();
        for (offset, command) in claims.into_iter().enumerate() {
            apply(&solo, &solo_shard, 1 + offset as u64, vec![command])
                .await
                .unwrap();
        }

        let expire = expired_envelope(vec![b.item_id], 100);
        apply(&packed, &shard, 6, vec![expire.clone()])
            .await
            .unwrap();
        apply(&solo, &solo_shard, 6, vec![expire]).await.unwrap();
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), b.item_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );

        let hole = vec![
            complete_envelope(vec![a.item_id]),
            complete_envelope(vec![c.item_id]),
        ];
        apply(&packed, &shard, 7, hole.clone())
            .await
            .expect("expiry hole must not reject neighboring Completes");
        for (offset, command) in hole.into_iter().enumerate() {
            apply(&solo, &solo_shard, 7 + offset as u64, vec![command])
                .await
                .expect("solo hole neighbor");
        }
        for id in [a.item_id, b.item_id, c.item_id, d.item_id, e.item_id] {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "expiry-hole model {id}"
            );
        }
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), a.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), b.item_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), c.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), d.item_id)
                .await
                .unwrap(),
            Some(ItemState::Leased)
        );

        let expired_member = vec![
            complete_envelope(vec![b.item_id]),
            complete_envelope(vec![d.item_id]),
            complete_envelope(vec![e.item_id]),
        ];
        apply(&packed, &shard, 9, expired_member.clone())
            .await
            .expect("Complete of an expired member must not poison neighbors");
        for (offset, command) in expired_member.into_iter().enumerate() {
            apply(&solo, &solo_shard, 9 + offset as u64, vec![command])
                .await
                .expect("solo expired-member neighbor");
        }
        for id in [a.item_id, b.item_id, c.item_id, d.item_id, e.item_id] {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "expired-member model {id}"
            );
        }
        for id in [a.item_id, b.item_id, c.item_id, d.item_id, e.item_id] {
            assert_eq!(
                AsyncProjectionStore::item_state(&packed, shard.clone(), id)
                    .await
                    .unwrap(),
                Some(ItemState::Complete)
            );
        }

        let mut g1 = item("6", "g1", 1);
        let mut g2 = item("7", "g2", 2);
        let mut g3 = item("8", "g3", 3);
        let group = GroupKey::new("grp").unwrap();
        g1.group_key = Some(group.clone());
        g2.group_key = Some(group.clone());
        g3.group_key = Some(group.clone());
        let x = item("9", "kx", 9);
        let y = item("10", "ky", 10);
        let mixed_items = vec![g1.clone(), g2.clone(), g3.clone(), x.clone(), y.clone()];
        let mixed_ids: Vec<ItemId> = mixed_items.iter().map(|item| item.item_id).collect();
        let (packed, shard) = open_store().await;
        let (solo, solo_shard) = open_store().await;
        let push = envelope(
            QueueCommand::Push(PushCommand {
                items: mixed_items.clone(),
            }),
            mixed_ids.clone(),
        );
        apply(&packed, &shard, 0, vec![push.clone()]).await.unwrap();
        apply(&solo, &solo_shard, 0, vec![push]).await.unwrap();
        let preclaim = vec![
            claim_envelope(vec![x.item_id], "tok-x", 200, "wx", true),
            claim_envelope(vec![y.item_id], "tok-y", 200, "wy", true),
        ];
        apply(&packed, &shard, 1, preclaim.clone()).await.unwrap();
        for (offset, command) in preclaim.into_iter().enumerate() {
            apply(&solo, &solo_shard, 1 + offset as u64, vec![command])
                .await
                .unwrap();
        }

        let mixed = vec![
            claim_envelope(vec![g1.item_id], "tok-g1", 400, "wg1", true),
            complete_envelope(vec![g1.item_id]),
            claim_envelope(vec![g2.item_id], "tok-g2", 500, "wg2", true),
            complete_envelope(vec![g2.item_id]),
            complete_envelope(vec![x.item_id]),
            complete_envelope(vec![y.item_id]),
        ];
        apply(&packed, &shard, 3, mixed.clone())
            .await
            .expect("adjacent fusion plus unfused Completes");
        let mixed_shape = packed
            .last_apply_statement_shape()
            .expect("mixed-vector statement shape");
        let mixed_phase = packed
            .last_apply_phase_observation()
            .expect("mixed-vector Immediate");
        assert!(mixed_phase.begin_us > 0 || mixed_phase.commit_us > 0);
        assert!(
            mixed_shape.statement_count > 0 && mixed_shape.write_statement_count > 0,
            "shape={mixed_shape:?}"
        );
        eprintln!("adjacent fusion + unfused completes shape={mixed_shape:?}");
        for (offset, command) in mixed.into_iter().enumerate() {
            apply(&solo, &solo_shard, 3 + offset as u64, vec![command])
                .await
                .expect("solo mixed neighbor");
        }
        for id in [g1.item_id, g2.item_id, g3.item_id, x.item_id, y.item_id] {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "mixed vector model {id}"
            );
        }
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), g1.item_id)
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), g3.item_id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            group_summary(&packed, &shard, &group).await,
            group_summary(&solo, &solo_shard, &group).await
        );
        assert_eq!(
            group_summary(&packed, &shard, &group).await,
            (3, Some(g1.item_id.to_string()))
        );
    }

    fn update_fields_envelope(ids: Vec<ItemId>, payload: Bytes) -> CommandEnvelope {
        envelope(
            QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
                updates: ids
                    .iter()
                    .map(|item_id| UpdateFieldsCommand {
                        item_id: *item_id,
                        field_ops: Default::default(),
                        payload: PayloadUpdate::Set(Some(payload.clone())),
                        set_priority: ScheduleUpdate::Keep,
                        set_not_before: ScheduleUpdate::Keep,
                        set_entity_document: None,
                        set_fields: None,
                        set_metadata: Some(fireweed_core::Metadata::default()),
                        set_gate_keys: None,
                        api001_batch: true,
                        client_item_key: None,
                        expected_item_version: None,
                    })
                    .collect(),
            }),
            ids,
        )
    }

    #[tokio::test]
    async fn packed_update_fields_matches_solo_model_in_one_transaction() {
        let items: Vec<_> = (1..=6)
            .map(|index| item(&index.to_string(), &format!("uk{index}"), index))
            .collect();
        let ids: Vec<ItemId> = items.iter().map(|item| item.item_id).collect();
        let (packed, shard) = open_store().await;
        let (solo, solo_shard) = open_store().await;
        let push = envelope(
            QueueCommand::Push(PushCommand {
                items: items.clone(),
            }),
            ids.clone(),
        );
        apply(&packed, &shard, 0, vec![push.clone()]).await.unwrap();
        apply(&solo, &solo_shard, 0, vec![push]).await.unwrap();

        let first = update_fields_envelope(ids[0..3].to_vec(), Bytes::from("p-a"));
        let second = update_fields_envelope(ids[3..6].to_vec(), Bytes::from("p-b"));
        apply(&packed, &shard, 1, vec![first.clone(), second.clone()])
            .await
            .expect("packed update fields");
        let packed_phase = packed
            .last_apply_phase_observation()
            .expect("one Immediate");
        assert!(packed_phase.begin_us > 0 || packed_phase.commit_us > 0);
        apply(&solo, &solo_shard, 1, vec![first]).await.unwrap();
        apply(&solo, &solo_shard, 2, vec![second]).await.unwrap();

        for id in &ids {
            let packed_rows = packed
                .query(
                    "SELECT item_version,payload,metadata,lifecycle_state FROM fireweed_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    vec![
                        Value::Text(shard.tenant_id.as_str().to_string()),
                        Value::Text(shard.queue_id.as_str().to_string()),
                        Value::Text(id.to_string()),
                    ],
                )
                .await
                .unwrap();
            let solo_rows = solo
                .query(
                    "SELECT item_version,payload,metadata,lifecycle_state FROM fireweed_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    vec![
                        Value::Text(solo_shard.tenant_id.as_str().to_string()),
                        Value::Text(solo_shard.queue_id.as_str().to_string()),
                        Value::Text(id.to_string()),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(
                packed_rows[0].values, solo_rows[0].values,
                "update model {id}"
            );
            assert_eq!(
                row_model(&packed, &shard, *id).await,
                row_model(&solo, &solo_shard, *id).await,
                "lease model {id}"
            );
        }

        let claim = claim_envelope(vec![ids[0]], "tok-u0", 50, "wu", true);
        let complete = complete_envelope(vec![ids[0]]);
        apply(&packed, &shard, 3, vec![claim.clone(), complete.clone()])
            .await
            .expect("mixed claim then complete");
        apply(&solo, &solo_shard, 3, vec![claim]).await.unwrap();
        apply(&solo, &solo_shard, 4, vec![complete]).await.unwrap();
        assert_eq!(
            AsyncProjectionStore::item_state(&packed, shard.clone(), ids[0])
                .await
                .unwrap(),
            Some(ItemState::Complete)
        );
        assert_eq!(
            row_model(&packed, &shard, ids[0]).await,
            row_model(&solo, &solo_shard, ids[0]).await,
        );
    }

    #[tokio::test]
    async fn packed_claim_run_then_complete_run_fuses_like_adjacent_pairs() {
        let items: Vec<_> = (1..=3)
            .map(|index| fifo_item(&index.to_string(), &format!("cf{index}")))
            .collect();
        let ids: Vec<ItemId> = items.iter().map(|item| item.item_id).collect();
        let (packed, shard) = open_store().await;
        let (solo, solo_shard) = open_store().await;
        let push = envelope(
            QueueCommand::Push(PushCommand {
                items: items.clone(),
            }),
            ids.clone(),
        );
        apply(&packed, &shard, 0, vec![push.clone()]).await.unwrap();
        apply(&solo, &solo_shard, 0, vec![push]).await.unwrap();

        let claims: Vec<_> = ids
            .iter()
            .enumerate()
            .map(|(offset, id)| {
                claim_envelope(vec![*id], &format!("tok-cf{offset}"), 50, "w", true)
            })
            .collect();
        let completes: Vec<_> = ids.iter().map(|id| complete_envelope(vec![*id])).collect();
        let mut packed_vector = claims.clone();
        packed_vector.extend(completes.clone());
        apply(&packed, &shard, 1, packed_vector)
            .await
            .expect("claim run then complete run");
        for (seq, command) in (1u64..).zip(claims.into_iter().chain(completes)) {
            apply(&solo, &solo_shard, seq, vec![command])
                .await
                .expect("solo neighbor");
        }
        for id in ids {
            assert_eq!(
                row_model(&packed, &shard, id).await,
                row_model(&solo, &solo_shard, id).await,
                "fused run model {id}"
            );
            assert_eq!(
                AsyncProjectionStore::item_state(&packed, shard.clone(), id)
                    .await
                    .unwrap(),
                Some(ItemState::Complete)
            );
        }
    }
}
