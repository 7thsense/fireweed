//! Turso RelTx adapter — same apply/query surface as rusqlite, different engine.

use fireweed_engine::{EngineError, EngineResult};
use fireweed_relational::{RelRow, RelTx, RelValue};
use turso::{Connection, Value};

pub struct TursoRel<'a>(pub &'a Connection);

pub fn to_turso(value: &RelValue) -> Value {
    match value {
        RelValue::Null => Value::Null,
        RelValue::Integer(value) => Value::Integer(*value),
        RelValue::Real(value) => Value::Real(*value),
        RelValue::Text(value) => Value::Text(value.clone()),
        RelValue::Blob(value) => Value::Blob(value.clone()),
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

struct TursoRelTxWorker {
    jobs: std::sync::mpsc::Sender<Box<dyn FnOnce(&tokio::runtime::Handle) + Send>>,
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
        let (jobs_tx, jobs_rx) =
            std::sync::mpsc::channel::<Box<dyn FnOnce(&tokio::runtime::Handle) + Send>>();
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

impl RelTx for TursoRel<'_> {
    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let params: Vec<Value> = params.iter().map(to_turso).collect();
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

    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
        let params: Vec<Value> = params.iter().map(to_turso).collect();
        if USE_LOCAL_RT.get() {
            return block_on_local(async {
                let mut stmt = self.0.prepare_cached(sql).await.map_err(storage)?;
                let mut rows = stmt.query(params).await.map_err(storage)?;
                let width = rows.column_count();
                let mut collected = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    let mut values = Vec::with_capacity(width);
                    for index in 0..width {
                        values.push(from_turso(row.get_value(index).map_err(storage)?));
                    }
                    collected.push(RelRow(values));
                }
                Ok(collected)
            });
        }
        let conn = self.0.clone();
        let sql = sql.to_string();
        block_on_turso(async move {
            let mut stmt = conn.prepare_cached(&sql).await.map_err(storage)?;
            let mut rows = stmt.query(params).await.map_err(storage)?;
            let width = rows.column_count();
            let mut collected = Vec::new();
            while let Some(row) = rows.next().await.map_err(storage)? {
                let mut values = Vec::with_capacity(width);
                for index in 0..width {
                    values.push(from_turso(row.get_value(index).map_err(storage)?));
                }
                collected.push(RelRow(values));
            }
            Ok(collected)
        })
    }
}

#[cfg(test)]
mod packed_authority_first_tests {
    use fireweed_conformance::{envelope, item, qdef, ts};
    use fireweed_core::{GroupKey, ItemId, ItemState, LeaseToken, WorkerId};
    use fireweed_engine::{
        AsyncProjectionStore, ClaimCommand, CommandEnvelope, CommandPosition, EngineError,
        FinalizeCommand, FinalizeKind, FinalizeOutcome, PushCommand, QueueCommand, QueueKey,
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
        if !authority_first {
            if let QueueCommand::Claim(claim) = &mut command.command {
                claim.authority_first = false;
            }
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
        assert_eq!(packed_summary, (1, Some(g3.item_id.to_string())));

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
}
