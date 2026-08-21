use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use crate::tx::TursoRel;
use bytes::Bytes;
use fireweed_core::{ClientItemKey, GroupKey, ItemId, LeaseToken, QueueId, TenantId};
use fireweed_engine::{Claimed, ClaimedItem, EngineError, EngineResult, QueueKey};
use fireweed_relational::{
    ClaimOutboxRow, ClassSClaimRequest, ClassSClaimResult, OWNED_PROJECTION_TABLES,
    RELATIONAL_SCHEMA, class_s_claim, delete_claim_outbox, entity_from_json, fields_from_json,
    metadata_from_json, nanos_ts, parse_priority, select_claim_outbox,
};
use tokio::sync::{Mutex, oneshot};
use turso::{Builder, Connection, Database, Value, transaction::TransactionBehavior};

const LEASE_PACK_MAX: usize = 8;
const LEASE_PACK_LINGER: Duration = Duration::from_millis(20);

struct LeasePackWaiter {
    tenant_id: String,
    queue_id: String,
    now_nanos: i64,
    limit: i64,
    lease_token: LeaseToken,
    lease_expires_at: i64,
    outbox_id: String,
    worker_id: Option<String>,
    tx: oneshot::Sender<EngineResult<ClassSClaimResult>>,
}

#[derive(Default)]
struct LeasePackState {
    pending: Vec<LeasePackWaiter>,
    oldest: Option<std::time::Instant>,
}

/// Exact Turso release qualified by this adapter.
pub const TURSO_SUPPORTED_VERSION: &str = "0.7.0";
/// Public mode boundary. Remote, sync, embedded-replica, and MVCC modes are not qualified.
pub const TURSO_SUPPORTED_BOUNDARY: &str = "embedded_local_ordinary_wal";

/// Default time spent retrying a locked database connection.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Journal selected for a local Turso projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// SQLite-compatible write-ahead logging, matching the existing projection durability profile.
    Wal,
    /// Turso's native multi-version concurrency mode. Exposed only for typed fail-closed configuration;
    /// it is outside the qualified projection boundary.
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

/// Runtime-observed SQL shape for one API-001 BatchUpdate projection apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TursoBatchUpdateStatementShape {
    pub item_count: usize,
    pub statement_count: usize,
    pub max_bind_count: usize,
}

impl TursoBatchUpdateStatementShape {
    pub(crate) fn new(item_count: usize) -> Self {
        Self {
            item_count,
            statement_count: 0,
            max_bind_count: 0,
        }
    }

    pub(crate) fn record(&mut self, bind_count: usize) {
        self.statement_count += 1;
        self.max_bind_count = self.max_bind_count.max(bind_count);
    }
}

pub(crate) type ConsumerLeaseIndex = BTreeMap<(QueueKey, String, ItemId), ()>;

/// Async embedded Turso store with a single serialized write connection.
///
/// SQLite-family stores have one durable writer. The async mutex preserves that invariant without
/// blocking a Tokio worker or manufacturing a private runtime/thread boundary.
/// Mutation cancellation safety is composition-owned: production profile integration must dispatch
/// these directly-awaited futures through the engine's owned-task commit path before enabling this adapter.
pub struct TursoRelational {
    database: Database,
    pub(crate) writer: Arc<Mutex<Connection>>,
    pub(crate) reader: Arc<Mutex<Connection>>,
    pub(crate) live_tokens: Arc<Mutex<BTreeMap<(QueueKey, ItemId), LeaseToken>>>,
    pub(crate) live_tokens_by_consumer: Arc<Mutex<ConsumerLeaseIndex>>,
    pub(crate) last_batch_update_shape: Arc<StdMutex<Option<TursoBatchUpdateStatementShape>>>,
    pub(crate) claim_scan_hints: Arc<StdMutex<std::collections::HashMap<QueueKey, i64>>>,
    pub(crate) claim_scan_default_fifo: Arc<StdMutex<std::collections::HashMap<QueueKey, bool>>>,
    pub(crate) grouped_shards: Arc<StdMutex<std::collections::HashSet<QueueKey>>>,
    lease_pack: Arc<StdMutex<LeasePackState>>,
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
        if config.journal_mode != JournalMode::Wal {
            return Err(TursoRelationalError::Configuration(
                "only embedded/local Turso 0.7 ordinary WAL is qualified; MVCC is unsupported"
                    .to_string(),
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
        let reader = database.connect()?;
        configure_connection(&reader, &config).await?;
        // Plan-time SELECTs must not wait for the writer txn. query_only +
        // read_uncommitted keep ingest packing while apply is caught up.
        let _ = reader.pragma_update("query_only", "ON").await;
        let _ = reader.pragma_update("read_uncommitted", "ON").await;
        let grouped_shards = load_grouped_shards(&writer).await?;
        Ok(Self {
            database,
            writer: Arc::new(Mutex::new(writer)),
            reader: Arc::new(Mutex::new(reader)),
            live_tokens: Arc::new(Mutex::new(BTreeMap::new())),
            live_tokens_by_consumer: Arc::new(Mutex::new(BTreeMap::new())),
            last_batch_update_shape: Arc::new(StdMutex::new(None)),
            claim_scan_hints: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            claim_scan_default_fifo: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            grouped_shards: Arc::new(StdMutex::new(grouped_shards)),
            lease_pack: Arc::new(StdMutex::new(LeasePackState::default())),
            config,
        })
    }

    pub async fn in_memory() -> Result<Self> {
        Self::open(TursoConfig::in_memory()).await
    }

    pub fn config(&self) -> &TursoConfig {
        &self.config
    }

    /// Most recent API-001 BatchUpdate statement trace for structural qualification.
    pub fn last_batch_update_statement_shape(&self) -> Option<TursoBatchUpdateStatementShape> {
        *self
            .last_batch_update_shape
            .lock()
            .expect("Turso statement-shape mutex poisoned")
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
        let connection = self.reader.lock().await;
        collect_rows(&connection, sql.as_ref(), params).await
    }

    /// Class S claim: lease + outbox in one IMMEDIATE writer txn, then drop the writer.
    /// Concurrent waiters on the same connection group-commit into that one txn so
    /// eight inflight claims pay one fsync, not eight.
    pub async fn class_s_claim(
        &self,
        request: ClassSClaimRequest<'_>,
    ) -> EngineResult<ClassSClaimResult> {
        let (tx, rx) = oneshot::channel();
        let should_seal = {
            let mut state = self.lease_pack.lock().expect("lease pack");
            state.pending.push(LeasePackWaiter {
                tenant_id: request.tenant_id.to_string(),
                queue_id: request.queue_id.to_string(),
                now_nanos: request.now_nanos,
                limit: request.limit,
                lease_token: request.lease_token.clone(),
                lease_expires_at: request.lease_expires_at,
                outbox_id: request.outbox_id.to_string(),
                worker_id: request.worker_id.map(str::to_string),
                tx,
            });
            if state.oldest.is_none() {
                state.oldest = Some(std::time::Instant::now());
            }
            state.pending.len() >= LEASE_PACK_MAX
        };
        if should_seal {
            self.seal_lease_pack().await;
            return rx
                .await
                .map_err(|_| EngineError::Storage("lease packer waiter dropped".into()))?;
        }
        tokio::pin!(rx);
        tokio::select! {
            result = &mut rx => {
                return result.map_err(|_| {
                    EngineError::Storage("lease packer waiter dropped".into())
                })?;
            }
            _ = tokio::time::sleep(LEASE_PACK_LINGER) => {
                self.seal_lease_pack().await;
            }
        }
        rx.await
            .map_err(|_| EngineError::Storage("lease packer waiter dropped".into()))?
    }

    async fn seal_lease_pack(&self) {
        let pending = {
            let mut state = self.lease_pack.lock().expect("lease pack");
            if state.pending.is_empty() {
                return;
            }
            state.oldest = None;
            std::mem::take(&mut state.pending)
        };
        if pending.is_empty() {
            return;
        }
        let mut writer = self.writer.lock().await;
        let tx = match writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
        {
            Ok(tx) => tx,
            Err(error) => {
                let msg = error.to_string();
                for waiter in pending {
                    let _ = waiter.tx.send(Err(EngineError::Storage(msg.clone())));
                }
                return;
            }
        };
        let hop_txn = tx.clone();
        let work: Vec<_> = pending
            .iter()
            .map(|waiter| {
                (
                    waiter.tenant_id.clone(),
                    waiter.queue_id.clone(),
                    waiter.now_nanos,
                    waiter.limit,
                    waiter.lease_token.clone(),
                    waiter.lease_expires_at,
                    waiter.outbox_id.clone(),
                    waiter.worker_id.clone(),
                )
            })
            .collect();
        let results = crate::tx::run_reltx_blocking(move || {
            work.into_iter()
                .map(
                    |(
                        tenant_id,
                        queue_id,
                        now_nanos,
                        limit,
                        lease_token,
                        lease_expires_at,
                        outbox_id,
                        worker_id,
                    )| {
                        class_s_claim(
                            &TursoRel(&hop_txn),
                            &ClassSClaimRequest {
                                tenant_id: &tenant_id,
                                queue_id: &queue_id,
                                now_nanos,
                                limit,
                                lease_token: &lease_token,
                                lease_expires_at,
                                outbox_id: &outbox_id,
                                request_id: None,
                                request_fingerprint: None,
                                worker_id: worker_id.as_deref(),
                                claim_unit: "item",
                                cohort_id: None,
                            },
                        )
                    },
                )
                .collect::<Vec<_>>()
        })
        .await;
        let failed = results.iter().any(|result| result.is_err());
        if failed {
            let _ = tx.rollback().await;
            let msg = results
                .into_iter()
                .find_map(|result| result.err())
                .map(|error| error.to_string())
                .unwrap_or_else(|| "lease pack failed".into());
            for waiter in pending {
                let _ = waiter.tx.send(Err(EngineError::Storage(msg.clone())));
            }
            return;
        }
        if let Err(error) = tx.commit().await {
            let msg = error.to_string();
            for waiter in pending {
                let _ = waiter.tx.send(Err(EngineError::Storage(msg.clone())));
            }
            return;
        }
        for (waiter, result) in pending.into_iter().zip(results) {
            let _ = waiter.tx.send(result);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn class_s_claim_for_queue(
        &self,
        tenant_id: &str,
        queue_id: &str,
        now_nanos: i64,
        limit: i64,
        lease_token: &LeaseToken,
        lease_expires_at: i64,
        outbox_id: &str,
        worker_id: Option<&str>,
    ) -> EngineResult<ClassSClaimResult> {
        self.class_s_claim(ClassSClaimRequest {
            tenant_id,
            queue_id,
            now_nanos,
            limit,
            lease_token,
            lease_expires_at,
            outbox_id,
            request_id: None,
            request_fingerprint: None,
            worker_id,
            claim_unit: "item",
            cohort_id: None,
        })
        .await
    }

    pub async fn delete_claim_outbox_row(
        &self,
        tenant_id: &str,
        queue_id: &str,
        outbox_id: &str,
    ) -> EngineResult<()> {
        let mut writer = self.writer.lock().await;
        let tx = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(|e| EngineError::Storage(e.to_string()))?;
        let result = delete_claim_outbox(&TursoRel(&tx), tenant_id, queue_id, outbox_id);
        match result {
            Ok(()) => tx
                .commit()
                .await
                .map_err(|e| EngineError::Storage(e.to_string())),
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn pending_claim_outbox(
        &self,
        tenant_id: &str,
        queue_id: &str,
    ) -> EngineResult<Vec<ClaimOutboxRow>> {
        let writer = self.writer.lock().await;
        select_claim_outbox(&TursoRel(&writer), tenant_id, queue_id)
    }

    pub async fn remember_leases(&self, shard: &QueueKey, item_ids: &[ItemId], token: LeaseToken) {
        let mut tokens = self.live_tokens.lock().await;
        let mut by_consumer = self.live_tokens_by_consumer.lock().await;
        for item_id in item_ids {
            tokens.insert((shard.clone(), *item_id), token.clone());
            by_consumer.insert((shard.clone(), token.as_str().to_string(), *item_id), ());
        }
    }
}

#[cfg(test)]
mod class_s_tests {
    use std::collections::BTreeMap;

    use fireweed_core::{LeaseToken, MetadataValue, PriorityValue, TypedValue};

    use super::*;

    fn insert_pending(item_id: &str, created_seq: i64) -> String {
        format!(
            "INSERT INTO fireweed_items(\
             tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority_sort,\
             eligible_since,payload,fields,metadata,retry_count,item_version,\
             last_command_sequence,created_at,updated_at,fenced,superseded,max_attempts,created_seq) \
             VALUES('t','q','{item_id}','key-{item_id}','Pending',X'00',1,X'CAFE','{{}}','{{}}',0,1,1,1,1,0,0,3,{created_seq})"
        )
    }

    #[tokio::test]
    async fn class_s_sequential_claims_are_disjoint() {
        let store = TursoRelational::in_memory().await.expect("open");
        let ids: Vec<String> = (1..=5)
            .map(|seq| ItemId::mint(1, 0, seq as u32).to_string())
            .collect();
        for (seq, item_id) in ids.iter().enumerate() {
            store
                .execute(insert_pending(item_id, (seq as i64) + 1), vec![])
                .await
                .expect("insert");
        }
        let token_a = LeaseToken::new("token-a").expect("token");
        let token_b = LeaseToken::new("token-b").expect("token");
        let first = store
            .class_s_claim_for_queue("t", "q", 10, 2, &token_a, 1_000, "out-1", Some("w"))
            .await
            .expect("first");
        let second = store
            .class_s_claim_for_queue("t", "q", 10, 2, &token_b, 1_000, "out-2", Some("w"))
            .await
            .expect("second");
        let first_ids: Vec<_> = first.items.iter().map(|i| i.item_id.as_str()).collect();
        let second_ids: Vec<_> = second.items.iter().map(|i| i.item_id.as_str()).collect();
        assert_eq!(first_ids, [ids[0].as_str(), ids[1].as_str()]);
        assert_eq!(second_ids, [ids[2].as_str(), ids[3].as_str()]);
        assert!(first_ids.iter().all(|id| !second_ids.contains(id)));
        let claimed = claimed_from_class_s(&token_a, first).expect("map");
        assert_eq!(claimed.items.len(), 2);
        assert_eq!(claimed.items[0].payload.as_deref(), Some(&[0xCA, 0xFE][..]));
    }

    #[tokio::test]
    async fn class_s_claim_preserves_fields_metadata_and_entity() {
        let store = TursoRelational::in_memory().await.expect("open");
        let stored_id = ItemId::mint(1, 0, 1);
        let compact_id = ItemId::mint(1, 0, 2);
        let index_fields = BTreeMap::from([
            ("profile.rank".to_string(), TypedValue::Integer(42)),
            (
                "profile.region".to_string(),
                TypedValue::String("east".to_string()),
            ),
        ]);
        let encoded_index_fields =
            fireweed_engine::index_fields::encode_index_fields_blob(&index_fields)
                .expect("encode index fields")
                .expect("nonempty index fields");
        let insert = "INSERT INTO fireweed_items(\
             tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
             not_before,eligible_since,group_key,payload,fields,metadata,entity_document,index_fields,\
             retry_count,item_version,last_command_sequence,created_at,updated_at,fenced,superseded,\
             max_attempts,created_seq) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        for (id, entity, native_indexes, created_seq) in [
            (
                stored_id,
                Value::Text(r#"{"kind":"stored","rank":7}"#.to_string()),
                Value::Null,
                1_i64,
            ),
            (
                compact_id,
                Value::Null,
                Value::Blob(encoded_index_fields),
                2_i64,
            ),
        ] {
            store
                .execute(
                    insert,
                    vec![
                        Value::Text("t".to_string()),
                        Value::Text("q".to_string()),
                        Value::Text(id.to_string()),
                        Value::Text(format!("key-{id}")),
                        Value::Text("Pending".to_string()),
                        Value::Text(
                            serde_json::to_string(&PriorityValue::Int64(7)).expect("priority"),
                        ),
                        Value::Blob(vec![0, created_seq as u8]),
                        Value::Integer(7),
                        Value::Integer(1),
                        Value::Text("group-a".to_string()),
                        Value::Blob(vec![0xCA, 0xFE]),
                        Value::Text(r#"{"blob":[1,2,3]}"#.to_string()),
                        Value::Text(r#"{"attempt":7,"source":"class-s"}"#.to_string()),
                        entity,
                        native_indexes,
                        Value::Integer(2),
                        Value::Integer(4),
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(9),
                        Value::Integer(created_seq),
                    ],
                )
                .await
                .expect("insert complete serving row");
        }
        // Gate-state rows contain only currently blocked keys. This membership therefore describes
        // a satisfied gate and must be echoed even though the optimized SELECT omits its anti-join.
        store
            .execute(
                "INSERT INTO fireweed_item_gates(tenant_id,queue_id,item_id,gate_key) \
                 VALUES('t','q',?,'gate-satisfied')",
                vec![Value::Text(stored_id.to_string())],
            )
            .await
            .expect("insert satisfied gate membership");

        let token = LeaseToken::new("token-fidelity").expect("token");
        let relational = store
            .class_s_claim_for_queue("t", "q", 10, 2, &token, 1_000, "out-fidelity", Some("w"))
            .await
            .expect("class S claim");
        let claimed = claimed_from_class_s(&token, relational).expect("render public claim");
        assert_eq!(claimed.items.len(), 2);

        let stored = &claimed.items[0];
        assert_eq!(stored.item_id, stored_id);
        assert_eq!(stored.payload.as_deref(), Some(&[0xCA, 0xFE][..]));
        assert_eq!(
            stored.fields.get("blob").map(Bytes::as_ref),
            Some(&[1, 2, 3][..])
        );
        assert_eq!(
            stored.metadata.get("source"),
            Some(&MetadataValue::String("class-s".to_string()))
        );
        assert_eq!(
            stored.metadata.get("attempt"),
            Some(&MetadataValue::Integer(7))
        );
        assert_eq!(
            stored.entity,
            Some(serde_json::json!({"kind": "stored", "rank": 7}))
        );
        assert_eq!(stored.gate_keys, ["gate-satisfied"]);
        assert_eq!(stored.priority, Some(PriorityValue::Int64(7)));
        assert_eq!(
            stored.group_key.as_ref().map(GroupKey::as_str),
            Some("group-a")
        );
        assert_eq!(stored.not_before, Some(nanos_ts(7)));
        assert_eq!(stored.attempt_count, 3);
        assert_eq!(stored.max_attempts, 9);

        let compact = &claimed.items[1];
        assert_eq!(compact.item_id, compact_id);
        assert_eq!(
            compact.entity,
            Some(serde_json::json!({"profile": {"rank": 42, "region": "east"}}))
        );
        assert!(compact.gate_keys.is_empty());
        assert_eq!(compact.priority, stored.priority);
        assert_eq!(compact.group_key, stored.group_key);
        assert_eq!(compact.not_before, stored.not_before);
    }

    #[tokio::test]
    async fn reader_select_does_not_wait_on_held_immediate_writer() {
        use std::time::{Duration, Instant};
        use turso::transaction::TransactionBehavior;

        let store = TursoRelational::in_memory().await.expect("open");
        store
            .execute(insert_pending("1", 1), vec![])
            .await
            .expect("insert");
        let mut writer = store.writer.lock().await;
        let _txn = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .expect("begin immediate");
        let started = Instant::now();
        let reader = store.reader.lock().await;
        let query = async {
            let mut rows = reader
                .query(
                    "SELECT item_id FROM fireweed_items WHERE tenant_id='t' AND queue_id='q' LIMIT 1",
                    Vec::<turso::Value>::new(),
                )
                .await
                .expect("reader query");
            rows.next().await.expect("reader row")
        };
        let timed = tokio::time::timeout(Duration::from_millis(50), query).await;
        let waited = started.elapsed();
        assert!(
            timed.is_ok(),
            "reader waited {waited:?} on a held IMMEDIATE writer; produce will serialize on apply"
        );
    }
}

pub fn claimed_from_class_s(
    lease_token: &LeaseToken,
    result: ClassSClaimResult,
) -> EngineResult<Claimed> {
    Ok(Claimed {
        items: render_class_s_claimed_items(lease_token, result.items)?,
        cohort_lease_token: None,
        cohort_id: None,
    })
}

/// Render complete public Claim items from relational Class-S rows in one reusable bulk helper.
/// Native index fields stay inside the relational carrier and are used only to echo a compacted
/// entity document when the durable row omitted the equivalent JSON.
pub fn render_class_s_claimed_items(
    lease_token: &LeaseToken,
    relational_items: Vec<fireweed_relational::ClassSClaimedItem>,
) -> EngineResult<Vec<ClaimedItem>> {
    let mut items = Vec::with_capacity(relational_items.len());
    for item in relational_items {
        let index_fields =
            fireweed_engine::index_fields::decode_index_fields_blob(item.index_fields.as_deref())?;
        items.push(ClaimedItem {
            item_id: ItemId::new(&item.item_id).map_err(|e| EngineError::Storage(e.to_string()))?,
            client_item_key: ClientItemKey::new(item.client_item_key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            item_version: u64::try_from(item.item_version)
                .map_err(|_| EngineError::Storage("item_version".into()))?,
            priority: parse_priority(item.priority)?,
            group_key: item
                .group_key
                .map(GroupKey::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            not_before: item.not_before.map(nanos_ts),
            lease_token: Some(lease_token.clone()),
            lease_expires_at: nanos_ts(item.lease_expires_at),
            attempt_count: u32::try_from(item.retry_count)
                .map_err(|_| EngineError::Storage("retry_count".into()))?,
            max_attempts: u32::try_from(item.max_attempts)
                .map_err(|_| EngineError::Storage("max_attempts".into()))?,
            payload: item.payload.map(Bytes::from),
            fields: fields_from_json(item.fields_json)?,
            metadata: metadata_from_json(item.metadata_json)?,
            gate_keys: item.gate_keys,
            entity: fireweed_engine::index_fields::echo_entity_document(
                entity_from_json(item.entity_document)?,
                &index_fields,
            )?,
        });
    }
    Ok(items)
}

/// Validate one benchmark evidence record without accepting missing or zero-valued observations.
pub fn verify_local_wal_benchmark_evidence(evidence: &serde_json::Value) -> Result<()> {
    let object = evidence.as_object().ok_or_else(|| {
        TursoRelationalError::Configuration("benchmark evidence must be an object".to_string())
    })?;
    let text = |field: &str| {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                TursoRelationalError::Configuration(format!(
                    "benchmark evidence requires nonempty {field}"
                ))
            })
    };
    if text("turso_version")? != TURSO_SUPPORTED_VERSION {
        return Err(TursoRelationalError::Configuration(
            "benchmark Turso version is outside the qualified pin".to_string(),
        ));
    }
    if text("boundary")? != TURSO_SUPPORTED_BOUNDARY {
        return Err(TursoRelationalError::Configuration(
            "benchmark boundary is outside local ordinary WAL".to_string(),
        ));
    }
    let features = object
        .get("turso_features")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TursoRelationalError::Configuration(
                "benchmark evidence requires turso_features".to_string(),
            )
        })?;
    if features.len() != 1 || features[0].as_str() != Some("local") {
        return Err(TursoRelationalError::Configuration(
            "benchmark features must be exactly [local]".to_string(),
        ));
    }
    let batch_sizes = object
        .get("batch_sizes")
        .and_then(serde_json::Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(|| {
            TursoRelationalError::Configuration(
                "benchmark evidence requires batch_sizes".to_string(),
            )
        })?;
    if batch_sizes
        .iter()
        .any(|value| value.as_u64().is_none_or(|value| value == 0))
    {
        return Err(TursoRelationalError::Configuration(
            "benchmark batch sizes must be positive integers".to_string(),
        ));
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
        if object
            .get(field)
            .and_then(serde_json::Value::as_f64)
            .is_none_or(|value| !value.is_finite() || value <= 0.0)
        {
            return Err(TursoRelationalError::Configuration(format!(
                "benchmark evidence requires positive {field}"
            )));
        }
    }
    let p50 = object["p50_us"].as_f64().expect("validated p50");
    let p95 = object["p95_us"].as_f64().expect("validated p95");
    let p99 = object["p99_us"].as_f64().expect("validated p99");
    if !(p50 <= p95 && p95 <= p99) {
        return Err(TursoRelationalError::Configuration(
            "benchmark percentiles must be monotonic".to_string(),
        ));
    }
    let exclusions = object
        .get("excluded_time")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            TursoRelationalError::Configuration(
                "benchmark evidence requires excluded_time".to_string(),
            )
        })?;
    for field in ["cold_open", "fixture_generation"] {
        if exclusions.get(field).and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(TursoRelationalError::Configuration(format!(
                "benchmark evidence must exclude {field} time"
            )));
        }
    }
    let limits = object
        .get("regression_limits")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            TursoRelationalError::Configuration(
                "benchmark evidence requires regression_limits".to_string(),
            )
        })?;
    let min_ops = limits
        .get("min_operations_per_second_ratio")
        .and_then(serde_json::Value::as_f64);
    let max_p99 = limits
        .get("max_p99_ratio")
        .and_then(serde_json::Value::as_f64);
    if min_ops.is_none_or(|value| !(0.0..=1.0).contains(&value) || value == 0.0)
        || max_p99.is_none_or(|value| value < 1.0 || !value.is_finite())
    {
        return Err(TursoRelationalError::Configuration(
            "benchmark regression limits are missing or invalid".to_string(),
        ));
    }
    Ok(())
}

async fn configure_connection(connection: &Connection, config: &TursoConfig) -> Result<()> {
    // `journal_mode` produces a row. Turso's execute_batch rejects row-producing statements after applying
    // their side effect, so each pragma is deliberately driven through the row-aware API.
    connection
        .pragma_update("journal_mode", config.journal_mode.pragma_value())
        .await?;
    // Projection is derived and rebuildable from the log (ADR-016). Crash durability
    // lives on the log; OFF avoids a per-commit fsync storm on the serving store.
    connection.pragma_update("synchronous", "OFF").await?;
    // Negative cache_size is KiB. 128 MiB is a cache cap, not an O(N) working set.
    connection.pragma_update("cache_size", "-131072").await?;
    // Autocheckpoint during ingest fights object-log fsyncs on the same disk and
    // stalls the reader used for plan SELECTs (busy_timeout). The projection is
    // rebuildable; checkpoint on close/idle, not on every WAL fill.
    let _ = connection.pragma_update("wal_autocheckpoint", "0").await;
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
    if settings.synchronous != 0 {
        return Err(TursoRelationalError::Configuration(format!(
            "synchronous read back as {}, expected OFF (0)",
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

async fn load_grouped_shards(
    connection: &Connection,
) -> Result<std::collections::HashSet<QueueKey>> {
    let rows = collect_rows(
        connection,
        "SELECT DISTINCT tenant_id, queue_id FROM fireweed_items WHERE group_key IS NOT NULL",
        vec![],
    )
    .await?;
    let mut shards = std::collections::HashSet::new();
    for row in rows {
        let tenant = match row.values.first() {
            Some(Value::Text(value)) => value.clone(),
            other => {
                return Err(TursoRelationalError::Schema(format!(
                    "grouped shard tenant was {other:?}"
                )));
            }
        };
        let queue = match row.values.get(1) {
            Some(Value::Text(value)) => value.clone(),
            other => {
                return Err(TursoRelationalError::Schema(format!(
                    "grouped shard queue was {other:?}"
                )));
            }
        };
        shards.insert(QueueKey::new(
            TenantId::new(tenant).map_err(|error| {
                TursoRelationalError::Schema(format!("grouped shard tenant: {error}"))
            })?,
            QueueId::new(queue).map_err(|error| {
                TursoRelationalError::Schema(format!("grouped shard queue: {error}"))
            })?,
        ));
    }
    Ok(shards)
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
