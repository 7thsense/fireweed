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
    tokio::task::spawn_blocking(move || {
        USE_LOCAL_RT.set(true);
        let result = work();
        USE_LOCAL_RT.set(false);
        result
    })
    .await
    .expect("turso RelTx blocking hop")
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
