use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pqueue_core::{ItemId, LeaseToken};
use pqueue_engine::QueueKey;
use pqueue_relational::{OWNED_PROJECTION_TABLES, RELATIONAL_SCHEMA};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value, transaction::TransactionBehavior};

/// Default time spent retrying a locked database connection.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Journal selected for a local Turso projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// SQLite-compatible write-ahead logging, matching the existing projection durability profile.
    Wal,
    /// Turso's native multi-version concurrency mode. Opt-in until the complete projection is qualified.
    Mvcc,
}

impl JournalMode {
    fn pragma_value(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Mvcc => "MVCC",
        }
    }

    fn expected_readback(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Mvcc => "mvcc",
        }
    }
}

/// Configuration for an embedded Turso relational projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TursoConfig {
    path: PathBuf,
    busy_timeout: Duration,
    journal_mode: JournalMode,
}

impl TursoConfig {
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            journal_mode: JournalMode::Wal,
        }
    }

    pub fn in_memory() -> Self {
        Self::local(":memory:")
    }

    pub fn with_busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }

    pub fn with_journal_mode(mut self, mode: JournalMode) -> Self {
        self.journal_mode = mode;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }
}

/// Adapter error with an explicit schema/configuration boundary.
#[derive(Debug)]
pub enum TursoRelationalError {
    Database(turso::Error),
    InvalidPath(PathBuf),
    Configuration(String),
    Schema(String),
}

impl fmt::Display for TursoRelationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "Turso database error: {error}"),
            Self::InvalidPath(path) => {
                write!(formatter, "Turso database path is not UTF-8: {path:?}")
            }
            Self::Configuration(message) => {
                write!(formatter, "invalid Turso configuration: {message}")
            }
            Self::Schema(message) => {
                write!(formatter, "Turso relational schema mismatch: {message}")
            }
        }
    }
}

impl std::error::Error for TursoRelationalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<turso::Error> for TursoRelationalError {
    fn from(error: turso::Error) -> Self {
        Self::Database(error)
    }
}

pub type Result<T> = std::result::Result<T, TursoRelationalError>;

/// One owned SQL statement for an atomic relational batch.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationalStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl RelationalStatement {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

/// Driver-neutral ownership of a result row at the adapter boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedRow {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

/// Verified connection settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionSettings {
    pub journal_mode: String,
    pub synchronous: i64,
    pub busy_timeout_ms: i64,
}

/// Database objects observed after applying the shared relational schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReport {
    pub tables: Vec<String>,
    pub indexes: Vec<String>,
}

/// Async embedded Turso store with a single serialized write connection.
///
/// SQLite-family stores have one durable writer. The async mutex preserves that invariant without
/// blocking a Tokio worker or manufacturing a private runtime/thread boundary.
/// Mutation cancellation safety is composition-owned: production profile integration must dispatch
/// these directly-awaited futures through the engine's owned-task commit path before enabling this adapter.
pub struct TursoRelational {
    database: Database,
    pub(crate) writer: Arc<Mutex<Connection>>,
    pub(crate) live_tokens: Arc<Mutex<HashMap<(QueueKey, ItemId), LeaseToken>>>,
    config: TursoConfig,
}

impl TursoRelational {
    /// Open, configure, migrate, and verify an embedded database.
    pub async fn open(config: TursoConfig) -> Result<Self> {
        if config.busy_timeout.is_zero() {
            return Err(TursoRelationalError::Configuration(
                "busy timeout must be greater than zero".to_string(),
            ));
        }
        let path = config
            .path
            .to_str()
            .ok_or_else(|| TursoRelationalError::InvalidPath(config.path.clone()))?;
        let database = Builder::new_local(path).build().await?;
        let mut writer = database.connect()?;
        configure_connection(&writer, &config).await?;
        migrate_connection(&mut writer).await?;
        verify_connection_settings(&writer, &config).await?;
        verify_schema(&writer).await?;
        Ok(Self {
            database,
            writer: Arc::new(Mutex::new(writer)),
            live_tokens: Arc::new(Mutex::new(HashMap::new())),
            config,
        })
    }

    pub async fn in_memory() -> Result<Self> {
        Self::open(TursoConfig::in_memory()).await
    }

    pub fn config(&self) -> &TursoConfig {
        &self.config
    }

    /// Obtain a separately configured connection for future read-side fan-out.
    pub async fn connect(&self) -> Result<Connection> {
        let connection = self.database.connect()?;
        configure_connection(&connection, &self.config).await?;
        Ok(connection)
    }

    /// Reapply idempotent migrations and verify the resulting schema.
    pub async fn migrate(&self) -> Result<SchemaReport> {
        let mut connection = self.writer.lock().await;
        migrate_connection(&mut connection).await?;
        verify_schema(&connection).await
    }

    pub async fn connection_settings(&self) -> Result<ConnectionSettings> {
        let connection = self.writer.lock().await;
        read_connection_settings(&connection).await
    }

    pub async fn schema_report(&self) -> Result<SchemaReport> {
        let connection = self.writer.lock().await;
        verify_schema(&connection).await
    }

    /// Execute one autocommit statement on the serialized writer.
    pub async fn execute(&self, sql: impl AsRef<str>, params: Vec<Value>) -> Result<u64> {
        let connection = self.writer.lock().await;
        Ok(connection.execute(sql, params).await?)
    }

    /// Execute statements atomically in one immediate transaction.
    pub async fn execute_immediate(&self, statements: &[RelationalStatement]) -> Result<Vec<u64>> {
        let mut connection = self.writer.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let mut changed = Vec::with_capacity(statements.len());
        for statement in statements {
            match transaction
                .execute(&statement.sql, statement.params.clone())
                .await
            {
                Ok(count) => changed.push(count),
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error.into());
                }
            }
        }
        transaction.commit().await?;
        Ok(changed)
    }

    /// Run a query and detach all rows from the connection guard.
    pub async fn query(&self, sql: impl AsRef<str>, params: Vec<Value>) -> Result<Vec<OwnedRow>> {
        let connection = self.writer.lock().await;
        collect_rows(&connection, sql.as_ref(), params).await
    }
}

async fn configure_connection(connection: &Connection, config: &TursoConfig) -> Result<()> {
    // `journal_mode` produces a row. Turso's execute_batch rejects row-producing statements after applying
    // their side effect, so each pragma is deliberately driven through the row-aware API.
    connection
        .pragma_update("journal_mode", config.journal_mode.pragma_value())
        .await?;
    connection.pragma_update("synchronous", "NORMAL").await?;
    connection.busy_timeout(config.busy_timeout)?;
    Ok(())
}

async fn migrate_connection(connection: &mut Connection) -> Result<()> {
    connection.execute_batch(RELATIONAL_SCHEMA).await?;
    if let Err(error) = connection
        .execute(
            "ALTER TABLE queues ADD COLUMN pause_drain_intake INTEGER NOT NULL DEFAULT 0",
            (),
        )
        .await
        && !error
            .to_string()
            .to_ascii_lowercase()
            .contains("duplicate column")
    {
        return Err(error.into());
    }
    Ok(())
}

async fn verify_connection_settings(connection: &Connection, config: &TursoConfig) -> Result<()> {
    let settings = read_connection_settings(connection).await?;
    let expected_journal = config.journal_mode.expected_readback();
    if settings.journal_mode != expected_journal {
        return Err(TursoRelationalError::Configuration(format!(
            "journal_mode read back as {:?}, expected {:?}",
            settings.journal_mode, expected_journal
        )));
    }
    if settings.synchronous != 1 {
        return Err(TursoRelationalError::Configuration(format!(
            "synchronous read back as {}, expected NORMAL (1)",
            settings.synchronous
        )));
    }
    let expected_timeout = i64::try_from(config.busy_timeout.as_millis()).map_err(|_| {
        TursoRelationalError::Configuration("busy timeout exceeds i64 milliseconds".to_string())
    })?;
    if settings.busy_timeout_ms != expected_timeout {
        return Err(TursoRelationalError::Configuration(format!(
            "busy_timeout read back as {}, expected {expected_timeout}",
            settings.busy_timeout_ms
        )));
    }
    Ok(())
}

async fn read_connection_settings(connection: &Connection) -> Result<ConnectionSettings> {
    Ok(ConnectionSettings {
        journal_mode: scalar_text(connection, "PRAGMA journal_mode").await?,
        synchronous: scalar_i64(connection, "PRAGMA synchronous").await?,
        busy_timeout_ms: scalar_i64(connection, "PRAGMA busy_timeout").await?,
    })
}

async fn verify_schema(connection: &Connection) -> Result<SchemaReport> {
    let rows = collect_rows(
        connection,
        "SELECT type,name FROM sqlite_schema \
         WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' ORDER BY type,name",
        vec![],
    )
    .await?;
    let mut tables = Vec::new();
    let mut indexes = Vec::new();
    for row in rows {
        let [Value::Text(kind), Value::Text(name)] = row.values.as_slice() else {
            return Err(TursoRelationalError::Schema(
                "sqlite_schema returned a non-text type/name row".to_string(),
            ));
        };
        match kind.as_str() {
            "table" => tables.push(name.clone()),
            "index" => indexes.push(name.clone()),
            _ => {}
        }
    }
    let missing: Vec<&str> = OWNED_PROJECTION_TABLES
        .iter()
        .copied()
        .filter(|table| !tables.iter().any(|actual| actual == table))
        .collect();
    if !missing.is_empty() {
        return Err(TursoRelationalError::Schema(format!(
            "missing owned tables: {}",
            missing.join(", ")
        )));
    }
    Ok(SchemaReport { tables, indexes })
}

async fn collect_rows(
    connection: &Connection,
    sql: &str,
    params: Vec<Value>,
) -> Result<Vec<OwnedRow>> {
    let mut rows = connection.query(sql, params).await?;
    let columns = rows.column_names();
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await? {
        let mut values = Vec::with_capacity(row.column_count());
        for index in 0..row.column_count() {
            values.push(row.get_value(index)?);
        }
        collected.push(OwnedRow {
            columns: columns.clone(),
            values,
        });
    }
    Ok(collected)
}

async fn scalar_text(connection: &Connection, sql: &str) -> Result<String> {
    let rows = collect_rows(connection, sql, vec![]).await?;
    match rows.first().and_then(|row| row.values.first()) {
        Some(Value::Text(value)) => Ok(value.clone()),
        value => Err(TursoRelationalError::Schema(format!(
            "{sql} returned {value:?}, expected text"
        ))),
    }
}

async fn scalar_i64(connection: &Connection, sql: &str) -> Result<i64> {
    let rows = collect_rows(connection, sql, vec![]).await?;
    match rows.first().and_then(|row| row.values.first()) {
        Some(Value::Integer(value)) => Ok(*value),
        value => Err(TursoRelationalError::Schema(format!(
            "{sql} returned {value:?}, expected integer"
        ))),
    }
}
