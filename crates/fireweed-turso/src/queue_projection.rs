//! Native API-007 projection. Durable log records remain authority; all data,
//! receipts and the external applied cursor publish in one native transaction.
mod commit_prepare;
mod filter_sql;
mod group_authority;
mod insert_prepare;
mod insert_replay;
mod payload;
mod preparation;
mod prepare;
mod query_read;
mod receipt_metadata;
mod receipts;
mod replay;
mod selection;
mod snapshot;
mod state_authority;
mod state_read;
mod stats_group;
mod stats_read;
mod unique_authority;
mod work;
pub use commit_prepare::{AdmittedCommitReceipt, CommitPreparationInput, PreparedOrdinaryCommit};
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_claim::{ClaimContext, plan_claim},
    queue_claim_publication::{AdmittedClaimBinding, ClaimPublicationView},
    queue_fields::{
        CheckedFields, FieldCoverage, ResolvedFields, affected_index_fields, resolve_index_effects,
    },
    queue_index::ordered_key,
    queue_log_record::{OwnedQueueLogRecord, QueueLogFormat, QueueLogPayload},
    queue_planner::StoredWork,
    queue_publication::{BodyCommandRef, CommandRef, EffectRef, FieldsCommandRef, ValueRef},
    queue_schema_codec::AdmittedQueueDefinition,
    queue_storage_layout::{
        collection_definition, collection_number, decode, discovery_digest, discovery_prefix,
        encode, encode_lifecycle, field_number, index_number,
    },
};
use fireweed_queue_spool::ScratchWorkers;
pub use insert_prepare::PreparedInsert;
pub use insert_replay::{AdmittedInsertReplay, InsertReplayInput};
pub use prepare::PreparedClaim;
pub use query_read::{AdmittedQueryPage, QueryReadInput};
pub use receipts::{AdmittedCommitReceipts, ReceiptReadInput};
pub use replay::{AdmittedClaimReplay, ClaimReplayInput};
pub use selection::AdmittedClaimSelection;
pub use state_read::{AdmittedStateRecords, StateReadInput};
pub use stats_read::{AdmittedQueueStats, StatsReadInput};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::Mutex;
use turso::{
    Builder, Connection, Database, Value,
    transaction::{DropBehavior, TransactionBehavior},
};
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 projection: {error}"))
}
fn publication_error(error: fireweed_engine::queue_publication::PublicationError) -> QueueError {
    unknown(format!("{error:?}"))
}
async fn catch_apply_panic<F: std::future::Future>(future: F) -> QueueResult<F::Output> {
    let mut future = Box::pin(future);
    std::future::poll_fn(move |context| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().poll(context)
        })) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(_) => std::task::Poll::Ready(Err(unknown("projection apply panicked"))),
        }
    })
    .await
}
const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS q7_queues(id INTEGER PRIMARY KEY,address_hash BLOB NOT NULL,tenant BLOB NOT NULL,name BLOB NOT NULL,definition BLOB NOT NULL,cursor_secret BLOB NOT NULL,next_sequence BLOB NOT NULL,applied_next_offset INTEGER NOT NULL,last_record_epoch BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS q7_address ON q7_queues(address_hash);
CREATE TABLE IF NOT EXISTS q7_records(id INTEGER PRIMARY KEY,queue_id INTEGER NOT NULL,collection INTEGER NOT NULL,key_hash BLOB NOT NULL,key BLOB NOT NULL,version BLOB NOT NULL,active INTEGER NOT NULL,lifecycle INTEGER NOT NULL,priority INTEGER NOT NULL,available_at INTEGER NOT NULL,eligible_since INTEGER NOT NULL,attempts INTEGER NOT NULL,lease_until INTEGER,lease_hash BLOB,insertion_sequence BLOB NOT NULL,last_change_offset INTEGER NOT NULL,prior_highwater BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS q7_record_key ON q7_records(queue_id,collection,key_hash);
CREATE INDEX IF NOT EXISTS q7_claim_priority ON q7_records(queue_id,priority,insertion_sequence) WHERE active=1 AND collection=0;
CREATE INDEX IF NOT EXISTS q7_claim_due ON q7_records(queue_id,available_at,insertion_sequence) WHERE active=1 AND collection=0;
CREATE TABLE IF NOT EXISTS q7_bodies(record_id INTEGER PRIMARY KEY,value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS q7_fields(record_id INTEGER NOT NULL,queue_id INTEGER NOT NULL,collection INTEGER NOT NULL,field INTEGER NOT NULL,prefix BLOB NOT NULL,hash BLOB NOT NULL,full_key BLOB NOT NULL,value BLOB NOT NULL,PRIMARY KEY(record_id,field));
CREATE INDEX IF NOT EXISTS q7_field_equal ON q7_fields(queue_id,collection,field,hash,record_id);
CREATE INDEX IF NOT EXISTS q7_field_range ON q7_fields(queue_id,collection,field,prefix,record_id);
CREATE TABLE IF NOT EXISTS q7_additional(id INTEGER PRIMARY KEY,record_id INTEGER NOT NULL,queue_id INTEGER NOT NULL,collection INTEGER NOT NULL,name_hash BLOB NOT NULL,name BLOB NOT NULL,prefix BLOB NOT NULL,hash BLOB NOT NULL,full_key BLOB NOT NULL,value BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS q7_additional_name ON q7_additional(record_id,name_hash);
CREATE INDEX IF NOT EXISTS q7_additional_equal ON q7_additional(queue_id,collection,name_hash,hash,record_id);
CREATE INDEX IF NOT EXISTS q7_additional_range ON q7_additional(queue_id,collection,name_hash,prefix,record_id);
CREATE TABLE IF NOT EXISTS q7_indexes(record_id INTEGER NOT NULL,queue_id INTEGER NOT NULL,collection INTEGER NOT NULL,index_no INTEGER NOT NULL,prefix BLOB NOT NULL,hash BLOB NOT NULL,full_key BLOB NOT NULL,is_unique INTEGER NOT NULL,PRIMARY KEY(record_id,index_no));
CREATE INDEX IF NOT EXISTS q7_index_equal ON q7_indexes(queue_id,collection,index_no,hash,record_id);
CREATE INDEX IF NOT EXISTS q7_index_order ON q7_indexes(queue_id,collection,index_no,prefix,record_id);
CREATE TABLE IF NOT EXISTS q7_requests(id INTEGER PRIMARY KEY,queue_id INTEGER NOT NULL,domain INTEGER NOT NULL,identity_hash BLOB NOT NULL,identity BLOB NOT NULL,fingerprint_format INTEGER NOT NULL,fingerprint BLOB NOT NULL,outcome BLOB NOT NULL,expired INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS q7_request_identity ON q7_requests(queue_id,domain,identity_hash);
"#;
/// This lower-level projector is not a QueueStore or a consumer transaction API.
pub struct QueueProjection {
    writer: Arc<Mutex<Option<Connection>>>,
    #[cfg(test)]
    pause: Mutex<Option<Arc<ApplyPause>>>,
    #[cfg(test)]
    panic_after_command: std::sync::atomic::AtomicBool,
    _database: Database,
}
#[cfg(test)]
struct ApplyPause {
    after_commit: bool,
    fail_reply: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::watch::Sender<bool>,
}
#[cfg(test)]
impl QueueProjection {
    async fn pause_apply(&self, after_commit: bool) -> bool {
        let pause = self.pause.lock().await.clone();
        if let Some(pause) = pause
            && pause.after_commit == after_commit
        {
            let mut release = pause.release.subscribe();
            pause.entered.notify_one();
            release.wait_for(|ready| *ready).await.unwrap();
            return pause.fail_reply;
        }
        false
    }
}
pub struct AppliedQueueRecord {
    pub next_offset: u64,
    _charge: OwnedQueueBufferCharge,
}
/// One admitted native snapshot, bound to the original genesis payload authority.
/// This is projection progress, never durable log coverage or a live fence.
pub struct QueueProjectionCheckpoint {
    pub next_offset: u64,
    pub insertion_sequence: u64,
    pub last_record_epoch: u64,
    _charge: OwnedQueueBufferCharge,
}
struct CheckpointWorker {
    job: Arc<QueueReservation>,
    projection: Arc<QueueProjection>,
    genesis: OwnedQueueLogRecord,
    output_charge: Option<OwnedQueueBufferCharge>,
}
impl CheckpointWorker {
    fn execute(mut self) -> QueueResult<Option<QueueProjectionCheckpoint>> {
        crate::tx::block_on_owned_apply(async {
            let writer = self.projection.writer.lock().await;
            let connection = writer
                .as_ref()
                .ok_or_else(|| unknown("projection connection is poisoned"))?;
            let Some(snapshot) =
                snapshot::read_snapshot(connection, &self.genesis, &self.job).await?
            else {
                return Ok(None);
            };
            Ok(Some(QueueProjectionCheckpoint {
                next_offset: snapshot.next_offset,
                insertion_sequence: snapshot.insertion_sequence,
                last_record_epoch: snapshot.last_record_epoch,
                _charge: self.output_charge.take().expect("one checkpoint output"),
            }))
        })
    }
}
struct ApplyWorker {
    projection: Arc<QueueProjection>,
    record: OwnedQueueLogRecord,
    offset: i64,
    job: Arc<QueueReservation>,
    _header_charge: OwnedQueueBufferCharge,
    output_charge: Option<OwnedQueueBufferCharge>,
}
fn blob(bytes: &[u8]) -> Value {
    Value::Blob(bytes.to_vec())
}
fn number(value: u64) -> Value {
    blob(&value.to_be_bytes())
}
fn integer(value: &Value) -> QueueResult<i64> {
    if let Value::Integer(n) = value {
        Ok(*n)
    } else {
        Err(unknown("expected integer authority"))
    }
}
fn bytes(value: &Value) -> QueueResult<&[u8]> {
    if let Value::Blob(b) = value {
        Ok(b)
    } else {
        Err(unknown("expected blob authority"))
    }
}
fn unsigned(value: &Value) -> QueueResult<u64> {
    Ok(u64::from_be_bytes(
        bytes(value)?
            .try_into()
            .map_err(|_| unknown("invalid unsigned authority"))?,
    ))
}
async fn one(
    connection: &Connection,
    sql: &str,
    params: Vec<Value>,
    columns: usize,
) -> QueueResult<Option<Vec<Value>>> {
    let mut rows = connection.query(sql, params).await.map_err(unknown)?;
    let Some(row) = rows.next().await.map_err(unknown)? else {
        return Ok(None);
    };
    let values = (0..columns)
        .map(|n| row.get_value(n).map_err(unknown))
        .collect::<QueueResult<Vec<_>>>()?;
    if rows.next().await.map_err(unknown)?.is_some() {
        return Err(unknown("duplicate full identity authority"));
    }
    Ok(Some(values))
}
impl QueueProjection {
    pub async fn open(
        path: &Path,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Arc<Self>> {
        struct OpenWorker {
            path: String,
            _charge: OwnedQueueBufferCharge,
        }
        impl OpenWorker {
            fn execute(self) -> QueueResult<Arc<QueueProjection>> {
                crate::tx::block_on_owned_apply(async {
                    let database = Builder::new_local(&self.path)
                        .build()
                        .await
                        .map_err(unknown)?;
                    let connection = database.connect().map_err(unknown)?;
                    let journal = one(&connection, "PRAGMA journal_mode=WAL", vec![], 1)
                        .await?
                        .ok_or_else(|| unknown("journal mode has no readback"))?;
                    if !matches!(&journal[0], Value::Text(mode) if mode == "wal") {
                        return Err(unknown("projection requires ordinary WAL mode"));
                    }
                    connection
                        .execute("PRAGMA synchronous=FULL", ())
                        .await
                        .map_err(unknown)?;
                    connection
                        .execute("PRAGMA cache_size=-4096", ())
                        .await
                        .map_err(unknown)?;
                    let sync = one(&connection, "PRAGMA synchronous", vec![], 1)
                        .await?
                        .ok_or_else(|| unknown("synchronous mode has no readback"))?;
                    if integer(&sync[0])? != 2 {
                        return Err(unknown("projection requires full synchronous mode"));
                    }
                    let cache = one(&connection, "PRAGMA cache_size", vec![], 1)
                        .await?
                        .ok_or_else(|| unknown("cache size has no readback"))?;
                    if integer(&cache[0])? != -4096 {
                        return Err(unknown("projection cache bound was not installed"));
                    }
                    connection.execute_batch(DDL).await.map_err(unknown)?;
                    Ok(Arc::new(QueueProjection {
                        writer: Arc::new(Mutex::new(Some(connection))),
                        #[cfg(test)]
                        pause: Mutex::new(None),
                        #[cfg(test)]
                        panic_after_command: std::sync::atomic::AtomicBool::new(false),
                        _database: database,
                    }))
                })
            }
        }
        let text = path
            .to_str()
            .ok_or_else(|| QueueError::Invalid("projection path must be UTF-8".into()))?;
        let charge = job.owned_buffer(
            u64::try_from(text.len())
                .map_err(unknown)?
                .checked_mul(16)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let worker = OpenWorker {
            path: text.into(),
            _charge: charge,
        };
        workers.run(move || worker.execute()).await
    }
    /// Read a single native snapshot with exact full address, raw definition and
    /// cursor-secret binding. The caller must independently validate log coverage.
    pub async fn checkpoint(
        self: &Arc<Self>,
        genesis: OwnedQueueLogRecord,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<QueueProjectionCheckpoint>> {
        if !genesis.belongs_to(job) {
            return Err(unknown("checkpoint genesis belongs to another operation"));
        }
        let worker = CheckpointWorker {
            job: Arc::clone(job),
            projection: Arc::clone(self),
            genesis,
            output_charge: Some(job.owned_buffer(65536)?),
        };
        workers.run(move || worker.execute()).await
    }
    pub async fn apply(
        self: &Arc<Self>,
        record: OwnedQueueLogRecord,
        offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AppliedQueueRecord> {
        if !record.belongs_to(job) {
            return Err(unknown("record belongs to another operation"));
        }
        if record.view()?.format() != QueueLogFormat::ChecksummedV2 {
            return Err(unknown(
                "unchecked legacy record requires explicit migration",
            ));
        }
        let view = record.view()?;
        let address = view.address();
        let header_bytes = (address.tenant.len() as u64)
            .checked_add(address.queue.len() as u64)
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let header_charge = job.owned_buffer(header_bytes)?;
        let offset = i64::try_from(offset).map_err(unknown)?;
        let worker = ApplyWorker {
            projection: Arc::clone(self),
            record,
            offset,
            job: Arc::clone(job),
            _header_charge: header_charge,
            output_charge: Some(job.owned_buffer(4096)?),
        };
        workers.run(move || worker.execute()).await
    }
}
impl ApplyWorker {
    fn execute(mut self) -> QueueResult<AppliedQueueRecord> {
        crate::tx::block_on_owned_apply(self.execute_inner())
    }
    async fn execute_inner(&mut self) -> QueueResult<AppliedQueueRecord> {
        let projection = Arc::clone(&self.projection);
        owned_native_transaction(&projection, TransactionBehavior::Immediate, self).await?;
        #[cfg(test)]
        if self.projection.pause_apply(true).await {
            return Err(unknown("injected lost apply reply after commit"));
        }
        Ok(AppliedQueueRecord {
            next_offset: u64::try_from(self.offset)
                .map_err(unknown)?
                .checked_add(1)
                .ok_or(QueueError::TooLarge)?,
            _charge: self.output_charge.take().expect("one result charge"),
        })
    }
}
/// Internal only: physical operations share one proven begin/panic/rollback/poison
/// lifecycle. The owning ScratchWorker retains all input and admission until it
/// returns; no transaction handle is exposed to queue consumers.
trait NativeTransactionWork {
    type Output;
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output>;
    async fn before_commit(&mut self) {}
}
impl NativeTransactionWork for ApplyWorker {
    type Output = ();
    async fn run(&mut self, connection: &Connection) -> QueueResult<()> {
        apply_transaction(
            connection,
            &self.record,
            self.offset,
            &self.job,
            #[cfg(test)]
            self.projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::SeqCst),
        )
        .await
    }
    async fn before_commit(&mut self) {
        #[cfg(test)]
        self.projection.pause_apply(false).await;
    }
}
async fn owned_native_transaction<W: NativeTransactionWork>(
    projection: &Arc<QueueProjection>,
    behavior: TransactionBehavior,
    work: &mut W,
) -> QueueResult<W::Output> {
    let mut writer = projection.writer.lock().await;
    let connection = writer
        .as_mut()
        .ok_or_else(|| unknown("projection connection is poisoned"))?;
    let transaction = match catch_apply_panic(connection.transaction_with_behavior(behavior)).await
    {
        Ok(Ok(transaction)) => transaction,
        failure => {
            drop(failure);
            let _ = catch_apply_panic(connection.execute("ROLLBACK", ())).await;
            if !matches!(connection.is_autocommit(), Ok(true)) {
                *writer = None;
            }
            return Err(unknown("native transaction begin failed"));
        }
    };
    let applied = catch_apply_panic(async {
        let value = work.run(&transaction).await?;
        work.before_commit().await;
        Ok(value)
    })
    .await
    .and_then(|value| value);
    match applied {
        Ok(value) => {
            if let Err(error) = catch_apply_panic(transaction.commit())
                .await
                .and_then(|value| value.map_err(unknown))
            {
                // commit consumed the transaction. Force any SDK-deferred
                // rollback through a real awaited access before release.
                let _ = catch_apply_panic(connection.execute("ROLLBACK", ())).await;
                if !matches!(connection.is_autocommit(), Ok(true)) {
                    *writer = None;
                }
                return Err(error);
            }
            Ok(value)
        }
        Err(error) => {
            let mut transaction = transaction;
            let rolled_back = match transaction.is_autocommit() {
                Ok(true) => {
                    transaction.set_drop_behavior(DropBehavior::Ignore);
                    drop(transaction);
                    true
                }
                Ok(false) => {
                    matches!(catch_apply_panic(transaction.rollback()).await, Ok(Ok(())))
                }
                Err(_) => matches!(catch_apply_panic(transaction.rollback()).await, Ok(Ok(()))),
            };
            if !rolled_back {
                *writer = None;
            }
            Err(error)
        }
    }
}

async fn apply_transaction(
    connection: &Connection,
    record: &OwnedQueueLogRecord,
    offset: i64,
    job: &Arc<QueueReservation>,
    #[cfg(test)] panic_after_command: bool,
) -> QueueResult<()> {
    let view = record.view()?;
    let address = view.address();
    let address_hash = discovery_digest(&encode(&(address.tenant, address.queue))?);
    let params = || {
        vec![
            blob(&address_hash),
            blob(address.tenant.as_bytes()),
            blob(address.queue.as_bytes()),
        ]
    };
    let metadata=one(connection, "SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(definition)='blob' THEN length(definition) ELSE -1 END,CASE WHEN typeof(next_sequence)='blob' AND length(next_sequence)=8 THEN next_sequence ELSE NULL END,CASE WHEN typeof(applied_next_offset)='integer' THEN applied_next_offset ELSE NULL END,CASE WHEN typeof(last_record_epoch)='blob' AND length(last_record_epoch)=8 THEN last_record_epoch ELSE NULL END FROM q7_queues WHERE address_hash=?1 AND tenant=?2 AND name=?3", params(), 5).await?;
    match view.payload() {
        QueueLogPayload::Genesis(genesis) => {
            if offset != 0 || metadata.is_some() {
                return Err(unknown("genesis is not the first external record"));
            }
            let _definition = genesis.validate_definition(job)?;
            connection.execute("INSERT INTO q7_queues(address_hash,tenant,name,definition,cursor_secret,next_sequence,applied_next_offset,last_record_epoch) VALUES(?1,?2,?3,?4,?5,?6,1,?7)", vec![blob(&address_hash),blob(address.tenant.as_bytes()),blob(address.queue.as_bytes()),blob(genesis.definition_bytes()),blob(genesis.cursor_secret()),number(0),number(view.epoch())]).await.map_err(unknown)?;
        }
        QueueLogPayload::Publication(_) | QueueLogPayload::Claim(_) => {
            let (frame, claim) = match view.payload() {
                QueueLogPayload::Publication(frame) => (frame, None),
                QueueLogPayload::Claim(claim) => (claim.commands(), Some(claim)),
                _ => unreachable!(),
            };
            let binding = claim.map(|claim| claim.bind(job)).transpose()?;
            let metadata =
                metadata.ok_or_else(|| unknown("publication has no projected genesis"))?;
            let queue_id = integer(&metadata[0])?;
            let expected_offset = integer(&metadata[3])?;
            if expected_offset < 1 || expected_offset != offset {
                return Err(unknown("external applied cursor mismatch"));
            }
            if unsigned(&metadata[4])? == 0 || unsigned(&metadata[4])? > view.epoch() {
                return Err(unknown("historical record epoch regressed"));
            }
            let summary = frame.summary();
            if unsigned(&metadata[2])? != summary.initial_sequence {
                return Err(unknown("insertion sequence predecessor mismatch"));
            }
            let definition_bytes = u64::try_from(integer(&metadata[1])?).map_err(unknown)?;
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
            if summary.definition_pin != definition.pin().as_bytes() {
                return Err(unknown("publication definition pin mismatch"));
            }
            let context = FrameContext {
                offset,
                initial_sequence: summary.initial_sequence,
                job,
                claim: claim.zip(binding.as_ref()),
            };
            for command in frame.commands() {
                apply_command(
                    connection,
                    queue_id,
                    definition.schema(),
                    command.map_err(publication_error)?,
                    &context,
                )
                .await?;
                #[cfg(test)]
                if panic_after_command {
                    panic!("injected panic after physical command");
                }
            }
            // Native indexes are not individually constrained: atomic swaps are
            // validated against the completed transaction's exact full values.
            if one(connection, "SELECT a.record_id FROM q7_indexes a JOIN q7_indexes b ON a.queue_id=b.queue_id AND a.collection=b.collection AND a.index_no=b.index_no AND a.hash=b.hash AND a.full_key=b.full_key AND a.record_id<b.record_id WHERE a.queue_id=?1 AND a.is_unique=1 AND b.is_unique=1 LIMIT 1", vec![Value::Integer(queue_id)], 1).await?.is_some() { return Err(unknown("final projection unique ownership conflict")); }
            for receipt in frame.receipts() {
                let receipt = receipt.map_err(publication_error)?;
                let domain = match receipt.domain {
                    0 | 2 => i64::from(receipt.domain),
                    _ => return Err(unknown("unsupported receipt domain")),
                };
                let identity = discovery_digest(receipt.request_id.as_bytes());
                if one(connection, "SELECT id FROM q7_requests WHERE queue_id=?1 AND domain=?2 AND identity_hash=?3 AND identity=?4", vec![Value::Integer(queue_id),Value::Integer(domain),blob(&identity),blob(receipt.request_id.as_bytes())], 1).await?.is_some() { return Err(unknown("publication receipt is not fresh immutable authority")); }
                validate_receipt_versions(
                    connection,
                    queue_id,
                    definition.schema(),
                    receipt.outcome,
                    offset,
                )
                .await?;
                // These are fixed FWQP outcome bytes, explicitly not Postcard.
                connection.execute("INSERT INTO q7_requests(queue_id,domain,identity_hash,identity,fingerprint_format,fingerprint,outcome) VALUES(?1,?2,?3,?4,?5,?6,?7)", vec![Value::Integer(queue_id),Value::Integer(domain),blob(&identity),blob(receipt.request_id.as_bytes()),Value::Integer(i64::from(receipt.fingerprint_format)),blob(receipt.fingerprint),blob(receipt.encoded_outcome())]).await.map_err(unknown)?;
            }
            if let Some(claim) = claim {
                let identity = discovery_digest(claim.request_id().as_bytes());
                if one(connection,"SELECT id FROM q7_requests WHERE queue_id=?1 AND domain=1 AND identity_hash=?2 AND identity=?3",vec![Value::Integer(queue_id),blob(&identity),blob(claim.request_id().as_bytes())],1).await?.is_some() {return Err(unknown("claim receipt is not fresh immutable authority"));}
                connection.execute("INSERT INTO q7_requests(queue_id,domain,identity_hash,identity,fingerprint_format,fingerprint,outcome) VALUES(?1,1,?2,?3,?4,?5,?6)",vec![Value::Integer(queue_id),blob(&identity),blob(claim.request_id().as_bytes()),Value::Integer(i64::from(claim.fingerprint_format())),blob(claim.fingerprint()),blob(claim.receipt_bytes())]).await.map_err(unknown)?;
            }
            let next = offset.checked_add(1).ok_or(QueueError::TooLarge)?;
            if connection.execute("UPDATE q7_queues SET next_sequence=?2,applied_next_offset=?3,last_record_epoch=?4 WHERE id=?1 AND applied_next_offset=?5", vec![Value::Integer(queue_id),number(summary.final_sequence),Value::Integer(next),number(view.epoch()),Value::Integer(offset)]).await.map_err(unknown)?!=1 { return Err(unknown("external cursor publication lost authority")); }
        }
    }
    Ok(())
}
fn own_value(value: ValueRef<'_>) -> FieldValue {
    match value {
        ValueRef::Integer(n) => FieldValue::Integer(n),
        ValueRef::Float(n) => FieldValue::Float(n),
        ValueRef::Boolean(n) => FieldValue::Boolean(n),
        ValueRef::Text(n) => FieldValue::Text(n.into()),
        ValueRef::Bytes(n) => FieldValue::Bytes(n.to_vec()),
    }
}
struct OwnedFieldChange {
    change: ResolvedFields,
    _charge: OwnedQueueBufferCharge,
}
fn field_change(
    fields: FieldsCommandRef<'_>,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedFieldChange> {
    let count = match fields {
        FieldsCommandRef::Preserve => 0,
        FieldsCommandRef::Replace(f) | FieldsCommandRef::Edit(f) => f.len(),
    };
    let charge = job.owned_buffer(
        count
            .checked_mul(192)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let change = match fields {
        FieldsCommandRef::Preserve => ResolvedFields::Preserve,
        FieldsCommandRef::Replace(fields) => {
            let mut output = Fields::new();
            for entry in fields.iter() {
                let (name, value) = entry.map_err(publication_error)?;
                let value = own_value(value.ok_or_else(|| unknown("replacement field is absent"))?);
                definition.validate_value(name, &value)?;
                output.insert(name.into(), value);
            }
            ResolvedFields::Replace(output)
        }
        FieldsCommandRef::Edit(fields) => {
            let mut output = BTreeMap::new();
            for entry in fields.iter() {
                let (name, value) = entry.map_err(publication_error)?;
                field_number(definition, name)?;
                let value = value.map(own_value);
                if let Some(value) = &value {
                    definition.validate_value(name, value)?;
                }
                output.insert(name.into(), value);
            }
            ResolvedFields::Edit(output)
        }
    };
    Ok(OwnedFieldChange {
        change,
        _charge: charge,
    })
}
struct LoadedFields {
    fields: Fields,
    _charges: Vec<OwnedQueueBufferCharge>,
}
async fn load_index_fields(
    connection: &Connection,
    record_id: i64,
    definition: &CollectionDefinition,
    change: &ResolvedFields,
    job: &Arc<QueueReservation>,
) -> QueueResult<LoadedFields> {
    let names = affected_index_fields(change, definition);
    load_selected_fields(connection, record_id, definition, &names, job).await
}
async fn load_selected_fields(
    connection: &Connection,
    record_id: i64,
    definition: &CollectionDefinition,
    names: &std::collections::BTreeSet<String>,
    job: &Arc<QueueReservation>,
) -> QueueResult<LoadedFields> {
    let mut loaded = LoadedFields {
        fields: Fields::new(),
        _charges: vec![],
    };
    let mut total = 0;
    let mut response_limit = false;
    let mut body = bytes::Bytes::new();
    payload::load_payload(
        connection,
        record_id,
        definition,
        payload::PayloadSelection {
            fields: Some(names),
            include_body: false,
            max_bytes: u64::MAX,
        },
        &mut payload::NativePayloadBudget {
            job,
            charges: &mut loaded._charges,
            total: &mut total,
            response_limit: &mut response_limit,
        },
        &mut loaded.fields,
        &mut body,
    )
    .await?;
    Ok(loaded)
}

fn value_bytes(value: &FieldValue) -> u64 {
    match value {
        FieldValue::Integer(_) | FieldValue::Float(_) | FieldValue::Boolean(_) => 8,
        FieldValue::Text(n) => n.len() as u64,
        FieldValue::Bytes(n) => n.len() as u64,
    }
}
#[derive(Clone, Copy)]
struct PhysicalRecord {
    id: i64,
    queue_id: i64,
    collection: i32,
}
async fn write_field(
    connection: &Connection,
    location: PhysicalRecord,
    definition: &CollectionDefinition,
    name: &str,
    value: &FieldValue,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    let PhysicalRecord {
        id: record_id,
        queue_id,
        collection,
    } = location;
    let _scratch = job.owned_buffer(
        value_bytes(value)
            .checked_add(name.len() as u64)
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(8192))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let no = field_number(definition, name)?;
    let full = ordered_key(&[Some(value)])?;
    let prefix = discovery_prefix(&full);
    let digest = discovery_digest(&full);
    let encoded = encode(value)?;
    if no >= 0 {
        connection.execute("INSERT INTO q7_fields(record_id,queue_id,collection,field,prefix,hash,full_key,value) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", vec![Value::Integer(record_id),Value::Integer(queue_id),Value::Integer(i64::from(collection)),Value::Integer(i64::from(no)),blob(&prefix),blob(&digest),blob(&full),blob(&encoded)]).await.map_err(unknown)?;
    } else {
        connection.execute("INSERT INTO q7_additional(record_id,queue_id,collection,name_hash,name,prefix,hash,full_key,value) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)", vec![Value::Integer(record_id),Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&discovery_digest(name.as_bytes())),blob(name.as_bytes()),blob(&prefix),blob(&digest),blob(&full),blob(&encoded)]).await.map_err(unknown)?;
    }
    Ok(())
}
async fn delete_field(
    connection: &Connection,
    record_id: i64,
    definition: &CollectionDefinition,
    name: &str,
) -> QueueResult<()> {
    let no = field_number(definition, name)?;
    let count = if no >= 0 {
        connection
            .execute(
                "DELETE FROM q7_fields WHERE record_id=?1 AND field=?2",
                vec![Value::Integer(record_id), Value::Integer(i64::from(no))],
            )
            .await
            .map_err(unknown)?
    } else {
        connection
            .execute(
                "DELETE FROM q7_additional WHERE record_id=?1 AND name_hash=?2 AND name=?3",
                vec![
                    Value::Integer(record_id),
                    blob(&discovery_digest(name.as_bytes())),
                    blob(name.as_bytes()),
                ],
            )
            .await
            .map_err(unknown)?
    };
    if count > 1 {
        return Err(unknown("duplicate full field identity"));
    }
    Ok(())
}
async fn persist_fields(
    connection: &Connection,
    location: PhysicalRecord,
    definition: &CollectionDefinition,
    change: &ResolvedFields,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    let PhysicalRecord {
        id: record_id,
        queue_id,
        collection,
    } = location;
    match change {
        ResolvedFields::Preserve => {}
        ResolvedFields::Replace(fields) => {
            connection
                .execute(
                    "DELETE FROM q7_fields WHERE record_id=?1",
                    vec![Value::Integer(record_id)],
                )
                .await
                .map_err(unknown)?;
            connection
                .execute(
                    "DELETE FROM q7_additional WHERE record_id=?1",
                    vec![Value::Integer(record_id)],
                )
                .await
                .map_err(unknown)?;
            for (name, value) in fields {
                write_field(
                    connection,
                    PhysicalRecord {
                        id: record_id,
                        queue_id,
                        collection,
                    },
                    definition,
                    name,
                    value,
                    job,
                )
                .await?;
            }
        }
        ResolvedFields::Edit(fields) => {
            for (name, value) in fields {
                delete_field(connection, record_id, definition, name).await?;
                if let Some(value) = value {
                    write_field(
                        connection,
                        PhysicalRecord {
                            id: record_id,
                            queue_id,
                            collection,
                        },
                        definition,
                        name,
                        value,
                        job,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}
async fn write_body(
    connection: &Connection,
    record_id: i64,
    body: &[u8],
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    let _body = job.owned_buffer(
        (body.len() as u64)
            .checked_mul(8)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    connection.execute("INSERT INTO q7_bodies(record_id,value) VALUES(?1,?2) ON CONFLICT(record_id) DO UPDATE SET value=excluded.value",vec![Value::Integer(record_id),blob(body)]).await.map_err(unknown)?;
    Ok(())
}
struct FrameContext<'a> {
    offset: i64,
    initial_sequence: u64,
    job: &'a Arc<QueueReservation>,
    claim: Option<(&'a ClaimPublicationView<'a>, &'a AdmittedClaimBinding<'a>)>,
}
async fn apply_command(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    command: CommandRef<'_>,
    context: &FrameContext<'_>,
) -> QueueResult<()> {
    let FrameContext {
        offset,
        initial_sequence,
        job,
        ..
    } = *context;
    let collection = collection_number(schema, command.identity.collection)?;
    let definition = collection_definition(schema, collection)?;
    let key = command.identity.key.as_bytes();
    let key_hash = discovery_digest(key);
    let current=one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END,CASE WHEN typeof(insertion_sequence)='blob' AND length(insertion_sequence)=8 THEN insertion_sequence ELSE NULL END,CASE WHEN typeof(lifecycle)='integer' THEN lifecycle ELSE NULL END,CASE WHEN typeof(lease_until) IN ('null','integer') THEN lease_until ELSE 'invalid-type' END,CASE WHEN typeof(lease_hash)='null' THEN NULL WHEN typeof(lease_hash)='blob' AND length(lease_hash)=32 THEN lease_hash ELSE 'invalid-hash' END,CASE WHEN typeof(priority)='integer' THEN priority ELSE NULL END,CASE WHEN typeof(available_at)='integer' THEN available_at ELSE NULL END,CASE WHEN typeof(eligible_since)='integer' THEN eligible_since ELSE NULL END,CASE WHEN typeof(attempts)='integer' THEN attempts ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=?2 AND key_hash=?3 AND key=?4",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&key_hash),blob(key)],11).await?;
    let (current_version, highwater, old_sequence) = if let Some(row) = &current {
        let highwater = unsigned(&row[1])?;
        if highwater == 0 {
            return Err(unknown("zero retained version"));
        }
        let active = integer(&row[2])?;
        if active != 0 && active != 1 {
            return Err(unknown("invalid active authority"));
        }
        let lifecycle = fireweed_engine::queue_storage_layout::decode_lifecycle(
            i16::try_from(integer(&row[4])?).map_err(unknown)?,
        )?;
        let leased = lifecycle == Lifecycle::Leased;
        if leased != !matches!(row[5], Value::Null) || leased != !matches!(row[6], Value::Null) {
            return Err(unknown("raw stored lease authority mismatch"));
        }
        if leased {
            integer(&row[5])?;
        }
        if leased && bytes(&row[6])?.len() != 32 {
            return Err(unknown("invalid stored lease hash"));
        }
        let sequence = unsigned(&row[3])?;
        if active == 1 && collection == 0 && (sequence == 0 || sequence > initial_sequence) {
            return Err(unknown("invalid live predecessor insertion sequence"));
        }
        (
            if active == 1 { Some(highwater) } else { None },
            highwater,
            unsigned(&row[3])?,
        )
    } else {
        (None, 0, 0)
    };
    if current_version != command.expected_version || highwater != command.prior_highwater {
        return Err(unknown("record predecessor authority mismatch"));
    }
    if let Some((claim, binding)) = context.claim {
        let row = current
            .as_ref()
            .ok_or_else(|| unknown("claim predecessor disappeared"))?;
        let entry = binding
            .lease(command.identity.key)
            .ok_or_else(|| unknown("claim lease binding missing"))?;
        let work = StoredWork {
            record: WorkRecord {
                work: NewWork {
                    key: command.identity.key.into(),
                    priority: integer(&row[7])?,
                    available_at: integer(&row[8])?,
                    fields: Default::default(),
                    body: bytes::Bytes::new(),
                },
                version: highwater,
                insertion_sequence: old_sequence,
                lifecycle: fireweed_engine::queue_storage_layout::decode_lifecycle(
                    i16::try_from(integer(&row[4])?).map_err(unknown)?,
                )?,
                eligible_since: integer(&row[9])?,
                attempts: u32::try_from(integer(&row[10])?).map_err(unknown)?,
                lease_expires_at: if matches!(row[5], Value::Null) {
                    None
                } else {
                    Some(integer(&row[5])?)
                },
            },
            lease_hash: if matches!(row[6], Value::Null) {
                None
            } else {
                Some(bytes(&row[6])?.try_into().map_err(unknown)?)
            },
            field_coverage: FieldCoverage::Selected {
                names: Default::default(),
                prefixes: Default::default(),
            },
            body_loaded: false,
        };
        let planned = plan_claim(
            work,
            entry.token.to_vec(),
            ClaimContext {
                now: claim.claimed_at(),
                eligible_at: claim.eligible_at(),
                expires_at: claim.expires_at(),
                insertion_head: initial_sequence,
            },
        )?;
        let EffectRef::WorkPut { metadata, .. } = command.effect else {
            return Err(unknown("claim has non-work effect"));
        };
        let expected = &planned.work.record;
        if command.output_highwater != expected.version
            || metadata.priority != expected.work.priority
            || metadata.available_at != expected.work.available_at
            || metadata.eligible_since != expected.eligible_since
            || metadata.insertion_sequence != expected.insertion_sequence
            || metadata.attempts != expected.attempts
            || metadata.lifecycle != expected.lifecycle
            || metadata.lease_expires_at != expected.lease_expires_at
            || metadata.lease_hash.copied() != planned.work.lease_hash
        {
            return Err(unknown(
                "claim resolved metadata differs from physical predecessor",
            ));
        }
    }
    let record_id = if let Some(row) = &current {
        integer(&row[0])?
    } else {
        connection.execute("INSERT INTO q7_records(queue_id,collection,key_hash,key,version,active,lifecycle,priority,available_at,eligible_since,attempts,insertion_sequence,last_change_offset,prior_highwater) VALUES(?1,?2,?3,?4,?5,0,0,0,0,0,0,?6,?7,?8)",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&key_hash),blob(key),number(command.output_highwater),number(0),Value::Integer(offset),number(highwater)]).await.map_err(unknown)?;
        let row=one(connection,"SELECT id FROM q7_records WHERE queue_id=?1 AND collection=?2 AND key_hash=?3 AND key=?4",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&key_hash),blob(key)],1).await?.ok_or_else(||unknown("record insert lost full identity"))?;
        integer(&row[0])?
    };
    match command.effect {
        EffectRef::Delete => {
            for sql in [
                "DELETE FROM q7_fields WHERE record_id=?1",
                "DELETE FROM q7_additional WHERE record_id=?1",
                "DELETE FROM q7_indexes WHERE record_id=?1",
                "DELETE FROM q7_bodies WHERE record_id=?1",
            ] {
                connection
                    .execute(sql, vec![Value::Integer(record_id)])
                    .await
                    .map_err(unknown)?;
            }
            connection.execute("UPDATE q7_records SET version=?2,active=0,lifecycle=0,lease_until=NULL,lease_hash=NULL,last_change_offset=?3,prior_highwater=?4 WHERE id=?1",vec![Value::Integer(record_id),number(command.output_highwater),Value::Integer(offset),number(highwater)]).await.map_err(unknown)?;
        }
        EffectRef::WorkPut {
            metadata,
            fields,
            body,
        } => {
            if current_version.is_some()
                && metadata.insertion_sequence != old_sequence
                && metadata.insertion_sequence <= initial_sequence
            {
                return Err(unknown("changed insertion order is not freshly allocated"));
            }
            let owned = field_change(fields, definition, job)?;
            apply_fields_and_indexes(
                connection,
                PhysicalRecord {
                    id: record_id,
                    queue_id,
                    collection,
                },
                definition,
                &owned.change,
                current_version.is_some(),
                job,
            )
            .await?;
            if let BodyCommandRef::Replace(body) = body {
                write_body(connection, record_id, body, job).await?;
            }
            connection.execute("UPDATE q7_records SET version=?2,active=1,lifecycle=?3,priority=?4,available_at=?5,eligible_since=?6,attempts=?7,lease_until=?8,lease_hash=?9,insertion_sequence=?10,last_change_offset=?11,prior_highwater=?12 WHERE id=?1",vec![Value::Integer(record_id),number(command.output_highwater),Value::Integer(i64::from(encode_lifecycle(metadata.lifecycle))),Value::Integer(metadata.priority),Value::Integer(metadata.available_at),Value::Integer(metadata.eligible_since),Value::Integer(i64::from(metadata.attempts)),metadata.lease_expires_at.map_or(Value::Null,Value::Integer),metadata.lease_hash.map_or(Value::Null,|value|blob(value)),number(metadata.insertion_sequence),Value::Integer(offset),number(highwater)]).await.map_err(unknown)?;
        }
        EffectRef::StatePut { fields, value } => {
            let owned = field_change(FieldsCommandRef::Replace(fields), definition, job)?;
            apply_fields_and_indexes(
                connection,
                PhysicalRecord {
                    id: record_id,
                    queue_id,
                    collection,
                },
                definition,
                &owned.change,
                current_version.is_some(),
                job,
            )
            .await?;
            write_body(connection, record_id, value, job).await?;
            connection.execute("UPDATE q7_records SET version=?2,active=1,lifecycle=0,priority=0,available_at=0,eligible_since=0,attempts=0,lease_until=NULL,lease_hash=NULL,insertion_sequence=?3,last_change_offset=?4,prior_highwater=?5 WHERE id=?1",vec![Value::Integer(record_id),number(command.output_highwater),number(0),Value::Integer(offset),number(highwater)]).await.map_err(unknown)?;
        }
    }
    Ok(())
}
async fn apply_fields_and_indexes(
    connection: &Connection,
    location: PhysicalRecord,
    definition: &CollectionDefinition,
    change: &ResolvedFields,
    existed: bool,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    let PhysicalRecord {
        id: record_id,
        queue_id,
        collection,
    } = location;
    if matches!(change, ResolvedFields::Preserve) {
        return Ok(());
    }
    let loaded = if existed {
        load_index_fields(connection, record_id, definition, change, job).await?
    } else {
        LoadedFields {
            fields: Fields::new(),
            _charges: vec![],
        }
    };
    let names = affected_index_fields(change, definition);
    let coverage = if existed {
        FieldCoverage::Selected {
            names,
            prefixes: Default::default(),
        }
    } else {
        FieldCoverage::Complete
    };
    let checked = CheckedFields::new(&loaded.fields, &coverage)?;
    let bytes = definition
        .indexes
        .values()
        .try_fold(4096u64, |mut n, columns| {
            for name in columns {
                n = n
                    .checked_add(loaded.fields.get(name).map_or(0, value_bytes))
                    .and_then(|n| {
                        n.checked_add(match change {
                            ResolvedFields::Replace(fields) => {
                                fields.get(name).map_or(0, value_bytes)
                            }
                            ResolvedFields::Edit(edits) => edits.get(name).map_or_else(
                                || loaded.fields.get(name).map_or(0, value_bytes),
                                |value| value.as_ref().map_or(0, value_bytes),
                            ),
                            ResolvedFields::Preserve => 0,
                        })
                    })
                    .ok_or(QueueError::TooLarge)?;
            }
            n.checked_add(columns.len() as u64 * 192)
                .ok_or(QueueError::TooLarge)
        })?;
    let _indexes = job.owned_buffer(bytes.checked_mul(16).ok_or(QueueError::TooLarge)?)?;
    let effects = resolve_index_effects(&checked, change, definition)?;
    persist_fields(
        connection,
        PhysicalRecord {
            id: record_id,
            queue_id,
            collection,
        },
        definition,
        change,
        job,
    )
    .await?;
    if !existed {
        let ResolvedFields::Replace(fields) = change else {
            return Err(unknown("new record has partial fields"));
        };
        for (name, columns) in &definition.indexes {
            let key = ordered_key(
                &columns
                    .iter()
                    .map(|name| fields.get(name))
                    .collect::<Vec<_>>(),
            )?;
            let unique = definition.unique_indexes.contains(name)
                && columns.iter().all(|name| fields.contains_key(name));
            persist_index(
                connection,
                PhysicalRecord {
                    id: record_id,
                    queue_id,
                    collection,
                },
                index_number(definition, name)?,
                &key,
                unique,
            )
            .await?;
        }
    } else {
        for effect in effects {
            let key = effect
                .after
                .ok_or_else(|| unknown("edited query index has no physical after key"))?;
            persist_index(
                connection,
                PhysicalRecord {
                    id: record_id,
                    queue_id,
                    collection,
                },
                index_number(definition, &effect.index)?,
                &key,
                effect.after_unique,
            )
            .await?;
        }
    }
    Ok(())
}
async fn persist_index(
    connection: &Connection,
    location: PhysicalRecord,
    number: i32,
    key: &[u8],
    unique: bool,
) -> QueueResult<()> {
    let PhysicalRecord {
        id: record_id,
        queue_id,
        collection,
    } = location;
    connection.execute("INSERT INTO q7_indexes(record_id,queue_id,collection,index_no,prefix,hash,full_key,is_unique) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(record_id,index_no) DO UPDATE SET prefix=excluded.prefix,hash=excluded.hash,full_key=excluded.full_key,is_unique=excluded.is_unique",vec![Value::Integer(record_id),Value::Integer(queue_id),Value::Integer(i64::from(collection)),Value::Integer(i64::from(number)),blob(&discovery_prefix(key)),blob(&discovery_digest(key)),blob(key),Value::Integer(i64::from(unique))]).await.map_err(unknown)?;
    Ok(())
}
async fn validate_success_version(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    identity: fireweed_engine::queue_publication::IdentityRef<'_>,
    version: u64,
    offset: i64,
) -> QueueResult<()> {
    let collection = collection_number(schema, identity.collection)?;
    let row=one(connection,"SELECT CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END,CASE WHEN typeof(last_change_offset)='integer' THEN last_change_offset ELSE NULL END,CASE WHEN typeof(prior_highwater)='blob' AND length(prior_highwater)=8 THEN prior_highwater ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=?2 AND key_hash=?3 AND key=?4",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&discovery_digest(identity.key.as_bytes())),blob(identity.key.as_bytes())],4).await?.ok_or_else(||unknown("receipt success has no record authority"))?;
    let maximum = unsigned(&row[0])?;
    let minimum = if integer(&row[2])? == offset {
        unsigned(&row[3])?
    } else {
        if integer(&row[1])? != 1 {
            return Err(unknown("omitted no-op receipt has no live authority"));
        }
        maximum
    };
    if version == 0 || version < minimum || version > maximum {
        return Err(unknown("receipt version exceeds resolved authority"));
    }
    Ok(())
}
async fn validate_receipt_versions(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    outcome: fireweed_engine::queue_publication::ReceiptOutcomeRef<'_>,
    offset: i64,
) -> QueueResult<()> {
    use fireweed_engine::queue_publication::{
        CommitOutcomeRef, InsertOutcomeRef, ReceiptOutcomeRef,
    };
    match outcome {
        ReceiptOutcomeRef::Commit(CommitOutcomeRef::Committed(versions)) => {
            for version in versions.iter() {
                let version = version.map_err(publication_error)?;
                validate_success_version(
                    connection,
                    queue_id,
                    schema,
                    version.identity,
                    version.version,
                    offset,
                )
                .await?;
            }
        }
        ReceiptOutcomeRef::Commit(CommitOutcomeRef::AdministrativeCommitted {
            versions,
            administration,
        }) => {
            for version in versions.iter() {
                let version = version.map_err(publication_error)?;
                validate_success_version(
                    connection,
                    queue_id,
                    schema,
                    version.identity,
                    version.version,
                    offset,
                )
                .await?;
            }
            for item in administration.items.iter() {
                let item = item.map_err(publication_error)?;
                use fireweed_core::queue_administration::AdministrativeOutcome;
                if let AdministrativeOutcome::Changed { version }
                | AdministrativeOutcome::Deleted { version } = item.outcome
                {
                    validate_success_version(
                        connection,
                        queue_id,
                        schema,
                        fireweed_engine::queue_publication::IdentityRef {
                            collection: None,
                            key: item.key,
                        },
                        version,
                        offset,
                    )
                    .await?;
                    let authority = one(connection, "SELECT CASE WHEN typeof(active)='integer' THEN active ELSE NULL END,CASE WHEN typeof(last_change_offset)='integer' THEN last_change_offset ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=0 AND key_hash=?2 AND key=?3", vec![Value::Integer(queue_id),blob(&discovery_digest(item.key.as_bytes())),blob(item.key.as_bytes())],2).await?.ok_or_else(||unknown("administrative receipt missing authority"))?;
                    let changed = matches!(item.outcome, AdministrativeOutcome::Changed { .. });
                    if integer(&authority[1])? != offset
                        || integer(&authority[0])? != i64::from(changed)
                    {
                        return Err(unknown("administrative receipt effect mismatch"));
                    }
                }
            }
        }
        ReceiptOutcomeRef::Insert(outcomes) => {
            for outcome in outcomes.iter() {
                match outcome.map_err(publication_error)? {
                    InsertOutcomeRef::Inserted { key, version }
                    | InsertOutcomeRef::Replaced { key, version } => {
                        validate_success_version(
                            connection,
                            queue_id,
                            schema,
                            fireweed_engine::queue_publication::IdentityRef {
                                collection: None,
                                key,
                            },
                            version,
                            offset,
                        )
                        .await?
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_log_record::{encode_checked_genesis, encode_checked_publication_record},
        queue_planner::{
            RecordIdentity, RecordSnapshot, UniqueIdentity, combine_deltas, insert_unique_probes,
            plan_commit_group, plan_insert_vector, prospective_unique_probes,
        },
        queue_publication::{PublicationReceipt, encode_publication_pinned},
        queue_schema_codec::encode_definition,
    };
    pub(super) fn address() -> QueueAddress {
        QueueAddress {
            tenant: "tenant\0β".into(),
            queue: "work/λ".into(),
        }
    }
    pub(super) fn schema() -> QueueSchema {
        let work = CollectionDefinition {
            fields: [
                ("u".into(), FieldType::Integer),
                ("opaque".into(), FieldType::Bytes),
            ]
            .into(),
            indexes: [("u".into(), vec!["u".into()])].into(),
            unique_indexes: ["u".into()].into(),
            additional_field_prefixes: ["meta/".into()].into(),
        };
        let state = CollectionDefinition {
            fields: [("phase".into(), FieldType::Text)].into(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        };
        QueueSchema {
            work,
            state: [("workflow".into(), state)].into(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    pub(super) fn work(key: &str, u: i64, opaque: bool) -> NewWork {
        let mut fields = Fields::from([("u".into(), FieldValue::Integer(u))]);
        if opaque {
            fields.insert("opaque".into(), FieldValue::Bytes(vec![7; 1 << 20]));
        }
        NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields,
            body: if opaque {
                Bytes::from(vec![9; 1 << 20])
            } else {
                Bytes::new()
            },
        }
    }
    fn authorize(snapshot: &mut RecordSnapshot, group: &CommitGroup, schema: &QueueSchema) {
        snapshot
            .unique_probes_loaded
            .extend(prospective_unique_probes(group, snapshot, schema).unwrap());
        snapshot.unique_owners.clear();
        for (key, row) in &snapshot.work {
            if let Some(value) = row.record.work.fields.get("u") {
                snapshot.unique_owners.insert(
                    UniqueIdentity {
                        collection: None,
                        index: "u".into(),
                        full_sort_key: ordered_key(&[Some(value)]).unwrap(),
                    },
                    RecordIdentity {
                        collection: None,
                        key: key.clone(),
                    },
                );
            }
        }
    }
    pub(super) async fn inspect(
        projection: &Arc<QueueProjection>,
        sql: &str,
        params: Vec<Value>,
        columns: usize,
    ) -> Vec<Value> {
        let projection = projection.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            crate::tx::block_on_owned_apply(async {
                let writer = projection.writer.lock().await;
                one(writer.as_ref().unwrap(), &sql, params, columns)
                    .await
                    .unwrap()
                    .unwrap()
            })
        })
        .await
        .unwrap()
    }
    pub(super) async fn execute_for_test(
        projection: &Arc<QueueProjection>,
        sql: &str,
        params: Vec<Value>,
    ) {
        let projection = projection.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            crate::tx::block_on_owned_apply(async {
                let writer = projection.writer.lock().await;
                writer
                    .as_ref()
                    .unwrap()
                    .execute(&sql, params)
                    .await
                    .unwrap();
            })
        })
        .await
        .unwrap();
    }
    pub(super) async fn bootstrap(
        path: &Path,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> (Arc<QueueProjection>, AdmittedQueueDefinition) {
        let schema = schema();
        schema.validate().unwrap();
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), job).unwrap();
        let projection = QueueProjection::open(path, job, workers).await.unwrap();
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job).unwrap();
        assert_eq!(
            projection
                .apply(genesis, 0, job, workers)
                .await
                .unwrap()
                .next_offset,
            1
        );
        (projection, definition)
    }
    pub(super) fn publication(
        before: &RecordSnapshot,
        delta: &fireweed_engine::queue_planner::CommitDelta,
        receipts: &[PublicationReceipt<'_>],
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> OwnedQueueLogRecord {
        let packed = combine_deltas(std::slice::from_ref(delta), before.next_sequence).unwrap();
        let frame = encode_publication_pinned(
            &address(),
            definition.pin(),
            before,
            &packed,
            receipts,
            16 << 20,
            job,
        )
        .unwrap();
        encode_checked_publication_record(1, frame.as_bytes(), 16 << 20, job).unwrap()
    }
    pub(super) async fn seed(
        projection: &Arc<QueueProjection>,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
        opaque: bool,
    ) -> RecordSnapshot {
        let schema = schema();
        let mut snapshot = RecordSnapshot::default();
        let request = InsertRequest {
            queue: address(),
            request_id: "seed".into(),
            items: vec![work("a", 1, opaque), work("b", 2, false)],
            mode: InsertMode::InsertOnly,
        };
        snapshot.unique_probes_loaded =
            insert_unique_probes(&request.items, &snapshot, &schema).unwrap();
        let before = snapshot.clone();
        let (deltas, outcomes) =
            plan_insert_vector(&request, &mut snapshot, &schema, || Ok(10)).unwrap();
        let delta = combine_deltas(&deltas, before.next_sequence).unwrap();
        let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
        let record = publication(&before, &delta, &[receipt], definition, job);
        assert_eq!(
            projection
                .apply(record, 1, job, workers)
                .await
                .unwrap()
                .next_offset,
            2
        );
        snapshot
    }
    pub(super) fn group(id: &str) -> CommitGroup {
        CommitGroup {
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
            administration: None,
        }
    }
    #[tokio::test]
    async fn mixed_state_new_work_receipt_and_cursor_commit_and_reopen_together() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("projection.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let mut update = group("advance");
        update.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
                value: Bytes::from_static(b"exact-state"),
            },
        });
        update.work_inserts.push(work("next", 3, false));
        update.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(99),
                ..Default::default()
            }),
        });
        authorize(&mut snapshot, &update, &schema());
        let before = snapshot.clone();
        let delta = plan_commit_group(&update, &mut snapshot, 20, &schema()).unwrap();
        let queue = address();
        let receipt = PublicationReceipt::commit(&queue, &update, &delta.receipt).unwrap();
        let record = publication(&before, &delta, &[receipt], &definition, &job);
        assert_eq!(
            projection
                .apply(record, 2, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            3
        );
        let row=inspect(&projection,"SELECT applied_next_offset,next_sequence,(SELECT COUNT(*) FROM q7_records WHERE active=1),(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",vec![],4).await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        assert_eq!(unsigned(&row[1]).unwrap(), 3);
        assert_eq!(integer(&row[2]).unwrap(), 4);
        assert_eq!(integer(&row[3]).unwrap(), 2);
        let body=inspect(&projection,"SELECT length(b.value),substr(b.value,1,1) FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.key=?1",vec![blob(b"a")],2).await;
        assert_eq!(integer(&body[0]).unwrap(), 1 << 20);
        assert_eq!(bytes(&body[1]).unwrap(), [9]);
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let job = Arc::new(admission.reserve(0).unwrap());
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let row=inspect(&reopened,"SELECT applied_next_offset,next_sequence,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",vec![],3).await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        assert_eq!(unsigned(&row[1]).unwrap(), 3);
        assert_eq!(integer(&row[2]).unwrap(), 2);
        let state=inspect(&reopened,"SELECT b.value FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.collection=1 AND r.key=?1",vec![blob(b"run")],1).await;
        assert_eq!(bytes(&state[0]).unwrap(), b"exact-state");
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    pub(super) async fn apply_group_record(
        projection: &Arc<QueueProjection>,
        definition: &AdmittedQueueDefinition,
        snapshot: &mut RecordSnapshot,
        group: &CommitGroup,
        offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AppliedQueueRecord> {
        authorize(snapshot, group, &schema());
        let before = snapshot.clone();
        let delta = plan_commit_group(group, snapshot, 20, &schema()).unwrap();
        let queue = address();
        let receipt = PublicationReceipt::commit(&queue, group, &delta.receipt).unwrap();
        projection
            .apply(
                publication(&before, &delta, &[receipt], definition, job),
                offset,
                job,
                workers,
            )
            .await
    }
    pub(super) fn patch(
        key: &str,
        version: u64,
        priority: Option<i64>,
        field: Option<Option<i64>>,
    ) -> UnclaimedUpdate {
        UnclaimedUpdate {
            key: key.into(),
            expect: StateExpect::Version(version),
            change: UnclaimedChange::Patch(WorkPatch {
                priority,
                fields: field.map_or_else(BTreeMap::new, |value| {
                    [("u".into(), value.map(FieldValue::Integer))].into()
                }),
                ..Default::default()
            }),
        }
    }
    #[tokio::test]
    async fn late_predecessor_and_receipt_failure_roll_back_all_effects_and_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rollback.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let mut wrong = snapshot.clone();
        wrong.work.get_mut("b").unwrap().record.version = 2;
        wrong.versions.insert(
            RecordIdentity {
                collection: None,
                key: "b".into(),
            },
            2,
        );
        let mut late = group("bad-predecessor");
        late.work_updates = vec![patch("a", 1, Some(10), None), patch("b", 2, Some(20), None)];
        assert!(matches!(
            apply_group_record(
                &projection,
                &definition,
                &mut wrong,
                &late,
                2,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let unchanged=inspect(&projection,"SELECT version,priority,(SELECT applied_next_offset FROM q7_queues),(SELECT COUNT(*) FROM q7_requests) FROM q7_records WHERE key=?1",vec![blob(b"a")],4).await;
        assert_eq!(unsigned(&unchanged[0]).unwrap(), 1);
        assert_eq!(integer(&unchanged[1]).unwrap(), 0);
        assert_eq!(integer(&unchanged[2]).unwrap(), 2);
        assert_eq!(integer(&unchanged[3]).unwrap(), 1);
        let mut snapshot = snapshot;
        let mut first = group("same-id");
        first.work_updates.push(patch("a", 1, Some(1), None));
        assert_eq!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &first,
                2,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            3
        );
        let mut duplicate = group("same-id");
        duplicate.work_updates.push(patch("a", 2, Some(2), None));
        duplicate.work_inserts.push(work("d", 4, false));
        duplicate.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "new-state".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Fields::new(),
                value: Bytes::from_static(b"must-rollback"),
            },
        });
        assert!(matches!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &duplicate,
                3,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let row=inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_records WHERE active=1),(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",vec![],3).await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        assert_eq!(integer(&row[1]).unwrap(), 2);
        assert_eq!(integer(&row[2]).unwrap(), 2);
        let a = inspect(
            &projection,
            "SELECT version,priority FROM q7_records WHERE key=?1",
            vec![blob(b"a")],
            2,
        )
        .await;
        assert_eq!(unsigned(&a[0]).unwrap(), 2);
        assert_eq!(integer(&a[1]).unwrap(), 1);
        drop(projection);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn unique_swap_and_sparse_removal_preserve_query_keys() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("swap.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let mut swap = group("swap");
        swap.work_updates = vec![
            patch("a", 1, None, Some(Some(2))),
            patch("b", 1, None, Some(Some(1))),
        ];
        assert_eq!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &swap,
                2,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            3
        );
        let a=inspect(&projection,"SELECT i.full_key,i.is_unique FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE r.key=?1",vec![blob(b"a")],2).await;
        assert_eq!(
            bytes(&a[0]).unwrap(),
            ordered_key(&[Some(&FieldValue::Integer(2))]).unwrap()
        );
        assert_eq!(integer(&a[1]).unwrap(), 1);
        let mut remove = group("remove");
        remove.work_updates.push(patch("a", 2, None, Some(None)));
        assert_eq!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &remove,
                3,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            4
        );
        let a=inspect(&projection,"SELECT i.full_key,i.is_unique FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE r.key=?1",vec![blob(b"a")],2).await;
        assert_eq!(bytes(&a[0]).unwrap(), ordered_key(&[None]).unwrap());
        assert_eq!(integer(&a[1]).unwrap(), 0);
        drop(projection);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn metadata_only_preserve_fits_below_opaque_payload_and_offsets_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selective.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let raw_definition = definition.raw().to_vec();
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let small = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(small.reserve(0).unwrap());
        let definition = AdmittedQueueDefinition::from_raw(&raw_definition, &job).unwrap();
        let mut update = group("metadata-only");
        update.work_updates.push(patch("a", 1, Some(44), None));
        assert_eq!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &update,
                2,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            3
        );
        let body=inspect(&projection,"SELECT length(b.value),substr(b.value,1,1),r.priority FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.key=?1",vec![blob(b"a")],3).await;
        assert_eq!(integer(&body[0]).unwrap(), 1 << 20);
        assert_eq!(bytes(&body[1]).unwrap(), [9]);
        assert_eq!(integer(&body[2]).unwrap(), 44);
        let field=inspect(&projection,"SELECT length(f.value) FROM q7_fields f JOIN q7_records r ON r.id=f.record_id WHERE r.key=?1 AND f.field=?2",vec![blob(b"a"),Value::Integer(i64::from(field_number(&schema().work,"opaque").unwrap()))],1).await;
        assert!(integer(&field[0]).unwrap() >= 1 << 20);
        let before = snapshot.clone();
        let empty = combine_deltas(&[], before.next_sequence).unwrap();
        let replay = publication(&before, &empty, &[], &definition, &job);
        assert!(matches!(
            projection.apply(replay, 2, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let gap = publication(&before, &empty, &[], &definition, &job);
        assert!(matches!(
            projection.apply(gap, 4, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let row = inspect(
            &projection,
            "SELECT applied_next_offset FROM q7_queues",
            vec![],
            1,
        )
        .await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(small.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn full_unsigned_versions_use_exact_blob_order_and_cas() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsigned.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        snapshot.work.get_mut("a").unwrap().record.version = u64::MAX - 1;
        snapshot.versions.insert(
            RecordIdentity {
                collection: None,
                key: "a".into(),
            },
            u64::MAX - 1,
        );
        execute_for_test(
            &projection,
            "UPDATE q7_records SET version=?1 WHERE key=?2",
            vec![number(u64::MAX - 1), blob(b"a")],
        )
        .await;
        let mut update = group("maximum");
        update
            .work_updates
            .push(patch("a", u64::MAX - 1, Some(7), None));
        assert_eq!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &update,
                2,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            3
        );
        let row = inspect(
            &projection,
            "SELECT version,(version>?1) FROM q7_records WHERE key=?2",
            vec![number(u64::MAX - 1), blob(b"a")],
            2,
        )
        .await;
        assert_eq!(unsigned(&row[0]).unwrap(), u64::MAX);
        assert_eq!(integer(&row[1]).unwrap(), 1);
        drop(projection);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn caller_drop_during_transaction_retains_guards_until_physical_commit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancel.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let raw_definition = definition.raw().to_vec();
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notify = terminal.clone();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notify.notify_one())
                .unwrap(),
        );
        let definition = AdmittedQueueDefinition::from_raw(&raw_definition, &job).unwrap();
        let mut update = group("cancelled-caller");
        update.work_updates.push(patch("a", 1, Some(8), None));
        authorize(&mut snapshot, &update, &schema());
        let before = snapshot.clone();
        let delta = plan_commit_group(&update, &mut snapshot, 20, &schema()).unwrap();
        let queue = address();
        let receipt = PublicationReceipt::commit(&queue, &update, &delta.receipt).unwrap();
        let record = publication(&before, &delta, &[receipt], &definition, &job);
        drop(definition);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(pause.clone());
        let writer = projection.clone();
        let owned_workers = workers.clone();
        let caller =
            tokio::spawn(async move { writer.apply(record, 2, &job, &owned_workers).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let held = admission.usage().unwrap();
        assert_eq!(held.0, 1);
        assert!(held.1 > 65536);
        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("caller survived cancellation"),
        }
        assert_eq!(admission.usage().unwrap(), held);
        pause.release.send_replace(true);
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let row = inspect(
            &projection,
            "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
            vec![],
            2,
        )
        .await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        assert_eq!(integer(&row[1]).unwrap(), 2);
    }
    #[tokio::test]
    async fn ambiguous_commit_reopens_exact_receipt_and_cursor_without_replaying_old_offset() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ambiguous.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let pause = Arc::new(ApplyPause {
            after_commit: true,
            fail_reply: true,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(true).0,
        });
        *projection.pause.lock().await = Some(pause);
        let mut update = group("lost-reply");
        update.work_updates.push(patch("a", 1, Some(22), None));
        assert!(matches!(
            apply_group_record(
                &projection,
                &definition,
                &mut snapshot,
                &update,
                2,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let original=inspect(&projection,"SELECT fingerprint_format,fingerprint,outcome FROM q7_requests WHERE domain=2 AND identity=?1",vec![blob(b"lost-reply")],3).await;
        let raw_definition = definition.raw().to_vec();
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let job = Arc::new(admission.reserve(0).unwrap());
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let recovered=inspect(&reopened,"SELECT fingerprint_format,fingerprint,outcome FROM q7_requests WHERE domain=2 AND identity=?1",vec![blob(b"lost-reply")],3).await;
        assert_eq!(recovered, original);
        let row = inspect(
            &reopened,
            "SELECT applied_next_offset FROM q7_queues",
            vec![],
            1,
        )
        .await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        let definition = AdmittedQueueDefinition::from_raw(&raw_definition, &job).unwrap();
        let empty = combine_deltas(&[], snapshot.next_sequence).unwrap();
        assert!(matches!(
            reopened
                .apply(
                    publication(&snapshot, &empty, &[], &definition, &job),
                    2,
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn corrupt_fixed_authority_and_sequences_fail_before_effects() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("corrupt.db"), &job, &workers).await;
        let snapshot = seed(&projection, &definition, &job, &workers, false).await;
        for sql in [
            "UPDATE q7_records SET version=zeroblob(4194304) WHERE key=?1",
            "UPDATE q7_records SET lease_hash=zeroblob(4194304) WHERE key=?1",
            "UPDATE q7_records SET insertion_sequence=zeroblob(8) WHERE key=?1",
            "UPDATE q7_records SET insertion_sequence=x'FFFFFFFFFFFFFFFF' WHERE key=?1",
        ] {
            execute_for_test(&projection, sql, vec![blob(b"a")]).await;
            let mut working = snapshot.clone();
            let mut update = group("corrupt");
            update.work_updates.push(patch("a", 1, Some(9), None));
            assert!(matches!(
                apply_group_record(
                    &projection,
                    &definition,
                    &mut working,
                    &update,
                    2,
                    &job,
                    &workers
                )
                .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            let row = inspect(
                &projection,
                "SELECT applied_next_offset FROM q7_queues",
                vec![],
                1,
            )
            .await;
            assert_eq!(integer(&row[0]).unwrap(), 2);
            execute_for_test(&projection,"UPDATE q7_records SET version=?2,lease_hash=NULL,insertion_sequence=?2 WHERE key=?1",vec![blob(b"a"),number(1)]).await;
        }
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn panic_after_physical_write_rolls_back_before_reuse_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("panic.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let mut update = group("panic");
        update.work_updates = vec![patch("a", 1, Some(9), None), patch("b", 1, Some(8), None)];
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut working = snapshot.clone();
        assert!(matches!(
            apply_group_record(
                &projection,
                &definition,
                &mut working,
                &update,
                2,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        {
            let writer = projection.writer.lock().await;
            assert!(
                writer
                    .as_ref()
                    .is_none_or(|connection| matches!(connection.is_autocommit(), Ok(true)))
            );
        }
        drop(projection);
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let row = inspect(&reopened,"SELECT priority,(SELECT applied_next_offset FROM q7_queues) FROM q7_records WHERE key=?1",vec![blob(b"a")],2).await;
        assert_eq!(integer(&row[0]).unwrap(), 0);
        assert_eq!(integer(&row[1]).unwrap(), 2);
        let mut working = snapshot;
        assert_eq!(
            apply_group_record(
                &reopened,
                &definition,
                &mut working,
                &update,
                2,
                &job,
                &workers
            )
            .await
            .unwrap()
            .next_offset,
            3
        );
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn fwqp2_addressed_receipts_bind_effects_and_rejections_reopen() {
        use fireweed_core::queue_administration::*;
        use fireweed_engine::queue_publication::{
            AdministrativePublicationBuilder, AdministrativePublicationReceipt,
            AdministrativeReceiptOutcome, encode_administrative_work_fragment,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("v2.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let mut working = snapshot.clone();
        let mut update = group("admin");
        update.work_updates.push(patch("a", 1, Some(9), None));
        authorize(&mut working, &update, &schema());
        let delta = plan_commit_group(&update, &mut working, 20, &schema()).unwrap();
        let mut admin_before = snapshot.clone();
        admin_before.work.retain(|key, _| key == "a");
        admin_before
            .versions
            .retain(|identity, _| identity.key == "a");
        let fragment = encode_administrative_work_fragment(&admin_before, &delta, &job).unwrap();
        let mut empty_snapshot = snapshot.clone();
        let ordinary =
            plan_commit_group(&group("empty"), &mut empty_snapshot, 20, &schema()).unwrap();
        let intent = AdministrativeIntent {
            evaluated_at: 20,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: Some(1),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(9),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram
            .record(&AdministrativeOutcome::Changed { version: 2 })
            .unwrap();
        let result = AdministrativeResult {
            histogram,
            clauses: vec![],
            items: vec![AdministrativeItemResult {
                key: "a".into(),
                outcome: AdministrativeOutcome::Changed { version: 2 },
                before: None,
            }],
        };
        let queue = address();
        let admin_group = group("admin-v2");
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &[],
            administration: &result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &queue,
            &admin_group,
            &intent,
            1 << 20,
            &outcome,
            &job,
        )
        .unwrap();
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            definition.pin(),
            &snapshot,
            &ordinary,
            1,
            fragment.as_bytes().len() as u64,
            1 << 20,
            &job,
        )
        .unwrap();
        builder.append_administrative(fragment.as_bytes()).unwrap();
        let frame = builder.finish().unwrap();
        // Change only the addressed outcome key: the frame remains structurally valid.
        let view = fireweed_engine::queue_publication::view_resolved_publication(frame.as_bytes())
            .unwrap();
        let parsed = view.receipts().next().unwrap().unwrap();
        let fireweed_engine::queue_publication::ReceiptOutcomeRef::Commit(
            fireweed_engine::queue_publication::CommitOutcomeRef::AdministrativeCommitted {
                administration,
                ..
            },
        ) = parsed.outcome
        else {
            panic!("v2");
        };
        let expected_success = vec![
            Value::Integer(i64::from(parsed.fingerprint_format)),
            blob(parsed.fingerprint),
            blob(parsed.encoded_outcome()),
        ];
        let item = administration.items.iter().next().unwrap().unwrap();
        let position = item.key.as_ptr() as usize - frame.as_bytes().as_ptr() as usize;
        let mut bad = frame.as_bytes().to_vec();
        bad[position] = b'b';
        assert!(fireweed_engine::queue_publication::view_resolved_publication(&bad).is_ok());
        let record = encode_checked_publication_record(1, &bad, 1 << 20, &job).unwrap();
        assert!(matches!(
            projection.apply(record, 2, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let mut wrong_kind = frame.as_bytes().to_vec();
        assert_eq!(wrong_kind[position + item.key.len()], 0);
        wrong_kind[position + item.key.len()] = 1;
        let result_start = wrong_kind
            .windows(5)
            .position(|bytes| bytes == b"FWAR1")
            .unwrap();
        assert_eq!(wrong_kind[result_start + 21], 0);
        wrong_kind[result_start + 21] = 1;
        assert!(fireweed_engine::queue_publication::view_resolved_publication(&wrong_kind).is_ok());
        let record = encode_checked_publication_record(1, &wrong_kind, 1 << 20, &job).unwrap();
        assert!(matches!(
            projection.apply(record, 2, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let record = encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
        assert_eq!(
            projection
                .apply(record, 2, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            3
        );
        let rejected_group = group("admin-too-large");
        let rejected = AdministrativeReceiptOutcome::TooLarge;
        let receipt = AdministrativePublicationReceipt::new(
            &queue,
            &rejected_group,
            &intent,
            1 << 20,
            &rejected,
            &job,
        )
        .unwrap();
        let builder = AdministrativePublicationBuilder::new(
            &receipt,
            definition.pin(),
            &working,
            &ordinary,
            0,
            0,
            1 << 20,
            &job,
        )
        .unwrap();
        let frame = builder.finish().unwrap();
        let view = fireweed_engine::queue_publication::view_resolved_publication(frame.as_bytes())
            .unwrap();
        let parsed = view.receipts().next().unwrap().unwrap();
        let expected_rejected = vec![
            Value::Integer(i64::from(parsed.fingerprint_format)),
            blob(parsed.fingerprint),
            blob(parsed.encoded_outcome()),
        ];

        let record = encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
        assert_eq!(
            projection
                .apply(record, 3, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            4
        );
        drop(projection);
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let row = inspect(&reopened,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests),(SELECT priority FROM q7_records WHERE key=?1) FROM q7_queues",vec![blob(b"a")],3).await;
        assert_eq!(integer(&row[0]).unwrap(), 4);
        assert_eq!(integer(&row[1]).unwrap(), 3);
        assert_eq!(integer(&row[2]).unwrap(), 9);
        for (identity, expected) in [
            ("admin-v2", expected_success),
            ("admin-too-large", expected_rejected),
        ] {
            assert_eq!(inspect(&reopened,"SELECT fingerprint_format,fingerprint,outcome FROM q7_requests WHERE identity=?1",vec![blob(identity.as_bytes())],3).await,expected);
        }
        let expected_public = vec![
            ReceiptLookup::Found(CommitReceipt {
                request_id: "admin-v2".into(),
                outcome: GroupOutcome::AdministrativeCommitted {
                    versions: vec![],
                    administration: result.clone(),
                },
            }),
            ReceiptLookup::Found(CommitReceipt {
                request_id: "admin-too-large".into(),
                outcome: GroupOutcome::Rejected(Rejection::TooLarge),
            }),
        ];
        let read = reopened
            .read_commit_receipts(
                ReceiptReadInput {
                    genesis: encode_checked_genesis(
                        1,
                        &queue,
                        &definition,
                        &[31; 32],
                        1 << 20,
                        &job,
                    )
                    .unwrap(),
                    request: ReceiptReadRequest {
                        queue: queue.clone(),
                        request_ids: vec!["admin-v2".into(), "admin-too-large".into()],
                        max_bytes: encoded_len(&expected_public).unwrap(),
                    },
                    limits: QueueLimits::default(),
                    required_next_offset: 4,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(read.receipts(), expected_public);
        drop(read);
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn checkpoints_bind_exact_genesis_and_reject_corrupt_progress() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let path = directory.path().join("checkpoint.db");
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let make_genesis = |secret: &[u8; 32]| {
            encode_checked_genesis(1, &address(), &definition, secret, 16 << 20, &job).unwrap()
        };
        let checkpoint = projection
            .checkpoint(make_genesis(&[31; 32]), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                checkpoint.next_offset,
                checkpoint.insertion_sequence,
                checkpoint.last_record_epoch
            ),
            (1, 0, 1)
        );
        drop(checkpoint);
        seed(&projection, &definition, &job, &workers, false).await;
        let checkpoint = projection
            .checkpoint(make_genesis(&[31; 32]), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (checkpoint.next_offset, checkpoint.insertion_sequence),
            (2, 2)
        );
        drop(checkpoint);
        assert!(matches!(
            projection
                .checkpoint(make_genesis(&[32; 32]), &job, &workers)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        for sql in [
            "UPDATE q7_queues SET definition=zeroblob(4194304)",
            "UPDATE q7_queues SET cursor_secret=zeroblob(4194304)",
        ] {
            execute_for_test(&projection, sql, vec![]).await;
            assert!(matches!(
                projection
                    .checkpoint(make_genesis(&[31; 32]), &job, &workers)
                    .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            execute_for_test(
                &projection,
                "UPDATE q7_queues SET definition=?1,cursor_secret=?2",
                vec![blob(definition.raw()), blob(&[31; 32])],
            )
            .await;
        }
        for sql in [
            "UPDATE q7_queues SET next_sequence=zeroblob(4194304)",
            "UPDATE q7_queues SET applied_next_offset=0",
            "UPDATE q7_queues SET last_record_epoch=zeroblob(8)",
        ] {
            execute_for_test(&projection, sql, vec![]).await;
            assert!(matches!(
                projection
                    .checkpoint(make_genesis(&[31; 32]), &job, &workers)
                    .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            execute_for_test(
                &projection,
                "UPDATE q7_queues SET next_sequence=?1,applied_next_offset=2,last_record_epoch=?2",
                vec![number(2), number(1)],
            )
            .await;
        }
        drop(projection);
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        assert_eq!(
            reopened
                .checkpoint(make_genesis(&[31; 32]), &job, &workers)
                .await
                .unwrap()
                .unwrap()
                .next_offset,
            2
        );
        workers.close_and_drain().await.unwrap();
    }
    struct ClaimFixture<'a> {
        before: &'a RecordSnapshot,
        definition: &'a AdmittedQueueDefinition,
        job: &'a Arc<QueueReservation>,
        identity: &'a str,
        now: i64,
        token_seed: u8,
        selected: &'a [&'a str],
    }
    fn claim_fixture(
        input: ClaimFixture<'_>,
        bad_priority: bool,
    ) -> (OwnedQueueLogRecord, RecordSnapshot, Vec<u8>, [u8; 32]) {
        let request = ClaimRequest {
            queue: address(),
            request_id: input.identity.into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 1 << 20,
            fields: Some(Default::default()),
            include_body: false,
        };
        claim_fixture_with_request(input, bad_priority, request)
    }
    fn claim_fixture_with_request(
        input: ClaimFixture<'_>,
        bad_priority: bool,
        request: ClaimRequest,
    ) -> (OwnedQueueLogRecord, RecordSnapshot, Vec<u8>, [u8; 32]) {
        use fireweed_engine::{
            queue_claim_publication::{ClaimPublicationInput, encode_claim_publication},
            queue_claim_receipt::{ClaimLease, encode_claim_receipt},
            queue_log_record::encode_checked_claim_record,
            queue_planner::CommitDelta,
        };
        let before = input.before;
        let mut after = before.clone();
        let mut work = vec![];
        let mut versions = vec![];
        let mut work_fields = BTreeMap::new();
        let mut work_bodies = BTreeMap::new();
        let mut leases = vec![];
        for (ordinal, key) in input.selected.iter().enumerate() {
            let token = vec![input.token_seed + ordinal as u8; 32];
            let planned = plan_claim(
                before.work[*key].clone(),
                token.clone(),
                ClaimContext {
                    now: input.now,
                    eligible_at: 0,
                    expires_at: input.now + 1_000_000,
                    insertion_head: before.next_sequence,
                },
            )
            .unwrap();
            let mut stored = planned.work;
            if bad_priority && *key == "b" {
                stored.record.work.priority -= 1;
            }
            leases.push(ClaimLease {
                key: (*key).into(),
                token,
                version: stored.record.version,
            });
            versions.push(RecordVersion {
                collection: None,
                key: (*key).into(),
                version: stored.record.version,
            });
            after.work.insert((*key).into(), stored.clone());
            after.versions.insert(
                RecordIdentity {
                    collection: None,
                    key: (*key).into(),
                },
                stored.record.version,
            );
            work.push(stored);
            work_fields.insert((*key).into(), ResolvedFields::Preserve);
            work_bodies.insert(
                (*key).into(),
                fireweed_engine::queue_body::ResolvedBody::Preserve,
            );
        }
        let delta = CommitDelta {
            work,
            work_fields,
            work_bodies,
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: versions.clone(),
            next_sequence: before.next_sequence,
            receipt: CommitReceipt {
                request_id: "unused".into(),
                outcome: GroupOutcome::Committed { versions },
            },
        };
        let delta = combine_deltas(&[delta], before.next_sequence).unwrap();
        let commands = encode_publication_pinned(
            &request.queue,
            input.definition.pin(),
            before,
            &delta,
            &[],
            1 << 20,
            input.job,
        )
        .unwrap();
        let receipt = encode_claim_receipt(&leases, 1 << 20, input.job).unwrap();
        let expected = receipt.bytes().to_vec();
        let fingerprint = request.fingerprint().unwrap();
        let limits = QueueLimits::default();
        let publication = encode_claim_publication(
            ClaimPublicationInput {
                request: &request,
                definition: input.definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: input.now,
                limits: &limits,
            },
            1 << 20,
            input.job,
        )
        .unwrap();
        (
            encode_checked_claim_record(1, &publication, 1 << 20, input.job).unwrap(),
            after,
            expected,
            fingerprint,
        )
    }
    async fn read_claim_fixture(
        projection: &Arc<QueueProjection>,
        definition: &AdmittedQueueDefinition,
        request: ClaimRequest,
        now: i64,
        offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<AdmittedClaimReplay>> {
        projection
            .replay_claim(
                ClaimReplayInput {
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        definition,
                        &[31; 32],
                        16 << 20,
                        job,
                    )?,
                    request,
                    now,
                    limits: QueueLimits::default(),
                    required_next_offset: offset,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                job,
                workers,
            )
            .await
    }
    #[tokio::test]
    async fn current_claim_replay_renews_reopens_and_rejects_whole_partial_group_before_payload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live-replay.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (mut projection, definition) = bootstrap(&path, &job, &workers).await;
        let before = seed(&projection, &definition, &job, &workers, false).await;
        let request = ClaimRequest {
            queue: address(),
            request_id: "live".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: None,
            include_body: true,
        };
        let (record, mut claimed, _, _) = claim_fixture_with_request(
            ClaimFixture {
                before: &before,
                definition: &definition,
                job: &job,
                identity: "live",
                now: 10,
                token_seed: 11,
                selected: &["a", "b"],
            },
            false,
            request.clone(),
        );
        projection.apply(record, 2, &job, &workers).await.unwrap();
        let replay = read_claim_fixture(
            &projection,
            &definition,
            request.clone(),
            11,
            3,
            &job,
            &workers,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            replay
                .records()
                .iter()
                .map(|work| work.record.work.key.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            replay.records()[0].record.work.fields,
            before.work["a"].record.work.fields
        );
        assert_eq!(
            replay.records()[0].record.work.body,
            before.work["a"].record.work.body
        );
        let original = replay.records().to_vec();
        drop(replay);
        let mut missing = request.clone();
        missing.request_id = "absent".into();
        assert!(
            read_claim_fixture(&projection, &definition, missing, 11, 3, &job, &workers)
                .await
                .unwrap()
                .is_none()
        );
        let mut conflict = request.clone();
        conflict.order = ClaimOrder::PriorityDescending;
        let conflict_result =
            read_claim_fixture(&projection, &definition, conflict, 11, 3, &job, &workers).await;
        assert_eq!(conflict_result.err(), Some(QueueError::RequestConflict));
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                11,
                2,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        for (sql, restore) in [
            (
                "UPDATE q7_queues SET next_sequence=x'0000000000000003'",
                "UPDATE q7_queues SET next_sequence=x'0000000000000002'",
            ),
            (
                "UPDATE q7_queues SET last_record_epoch=x'0000000000000002'",
                "UPDATE q7_queues SET last_record_epoch=x'0000000000000001'",
            ),
        ] {
            execute_for_test(&projection, sql, vec![]).await;
            assert!(matches!(
                read_claim_fixture(
                    &projection,
                    &definition,
                    request.clone(),
                    11,
                    3,
                    &job,
                    &workers
                )
                .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            execute_for_test(&projection, restore, vec![]).await;
        }
        execute_for_test(&projection,"INSERT INTO q7_fields SELECT record_id,queue_id,collection,999,prefix,hash,full_key,value FROM q7_fields WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")]).await;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                11,
                3,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(&projection, "DELETE FROM q7_fields WHERE field=999", vec![]).await;
        execute_for_test(&projection,"UPDATE q7_fields SET value=?2 WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a"),blob(&encode(&FieldValue::Text("wrong-type".into())).unwrap())]).await;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                11,
                3,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(&projection,"UPDATE q7_fields SET value=?2 WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a"),blob(&encode(&FieldValue::Integer(1)).unwrap())]).await;
        let mut renewal = group("renew-live");
        renewal.claimed_updates = original
            .iter()
            .map(|work| ClaimedUpdate {
                key: work.record.work.key.clone(),
                lease: work.lease.clone(),
                expected_version: work.record.version,
                patch: WorkPatch::default(),
                disposition: Disposition::Retain {
                    renew_nanos: Some(2_000_000),
                },
            })
            .collect();
        apply_group_record(
            &projection,
            &definition,
            &mut claimed,
            &renewal,
            3,
            &job,
            &workers,
        )
        .await
        .unwrap();
        drop(projection);
        projection = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let renewed = read_claim_fixture(
            &projection,
            &definition,
            request.clone(),
            1_000_010,
            4,
            &job,
            &workers,
        )
        .await
        .unwrap()
        .unwrap();
        for (current, old) in renewed.records().iter().zip(&original) {
            assert_eq!(current.lease, old.lease);
            assert!(current.record.version > old.record.version);
            assert_eq!(current.record.lease_expires_at, Some(2_000_020));
        }
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                2_000_020,
                4,
                &job,
                &workers
            )
            .await,
            Err(QueueError::ClaimExpired)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET lease_hash=zeroblob(4194304) WHERE key=?1",
            vec![blob(b"a")],
        )
        .await;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                21,
                4,
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET lease_hash=?2 WHERE key=?1",
            vec![
                blob(b"a"),
                blob(&fireweed_engine::queue_planner::lease_hash(
                    &original[0].lease.token,
                )),
            ],
        )
        .await;
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET expired=1 WHERE domain=1 AND identity=?1",
            vec![blob(b"live")],
        )
        .await;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                request.clone(),
                21,
                4,
                &job,
                &workers
            )
            .await,
            Err(QueueError::RequestExpired)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET expired=0 WHERE domain=1 AND identity=?1",
            vec![blob(b"live")],
        )
        .await;
        let mut settlement = group("settle-one");
        let current = &renewed.records()[0];
        settlement.claimed_updates.push(ClaimedUpdate {
            key: current.record.work.key.clone(),
            lease: current.lease.clone(),
            expected_version: current.record.version,
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        });
        drop(renewed);
        apply_group_record(
            &projection,
            &definition,
            &mut claimed,
            &settlement,
            4,
            &job,
            &workers,
        )
        .await
        .unwrap();
        execute_for_test(&projection,"UPDATE q7_bodies SET value=zeroblob(4194304) WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"b")]).await;
        assert!(matches!(
            read_claim_fixture(&projection, &definition, request, 21, 5, &job, &workers).await,
            Err(QueueError::ClaimExpired)
        ));
        let autocommit = tokio::task::spawn_blocking({
            let projection = Arc::clone(&projection);
            move || {
                crate::tx::block_on_owned_apply(async {
                    projection
                        .writer
                        .lock()
                        .await
                        .as_ref()
                        .unwrap()
                        .is_autocommit()
                        .unwrap()
                })
            }
        })
        .await
        .unwrap();
        assert!(autocommit);
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn cancelled_started_claim_reader_retains_snapshot_and_result_until_terminal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancel-replay.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let before = seed(&projection, &definition, &job, &workers, false).await;
        let request = ClaimRequest {
            queue: address(),
            request_id: "cancel-reader".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: None,
            include_body: true,
        };
        let (record, _, _, _) = claim_fixture_with_request(
            ClaimFixture {
                before: &before,
                definition: &definition,
                job: &job,
                identity: "cancel-reader",
                now: 10,
                token_seed: 11,
                selected: &["a", "b"],
            },
            false,
            request.clone(),
        );
        projection.apply(record, 2, &job, &workers).await.unwrap();
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&terminal);
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notify.notify_one())
                .unwrap(),
        );
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap();
        drop(definition);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(Arc::clone(&pause));
        let reader = Arc::clone(&projection);
        let owned_workers = workers.clone();
        let caller = tokio::spawn(async move {
            reader
                .replay_claim(
                    ClaimReplayInput {
                        genesis,
                        request,
                        now: 11,
                        limits: QueueLimits::default(),
                        required_next_offset: 3,
                        required_insertion_sequence: 2,
                        required_last_record_epoch: 1,
                    },
                    &job,
                    &owned_workers,
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let held = admission.usage().unwrap();
        assert_eq!(held.0, 1);
        assert!(held.1 > 65536);
        caller.abort();
        assert!(caller.await.is_err());
        assert_eq!(admission.usage().unwrap(), held);
        pause.release.send_replace(true);
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let writer = Arc::clone(&projection);
        assert!(
            tokio::task::spawn_blocking(move || crate::tx::block_on_owned_apply(async {
                writer
                    .writer
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .is_autocommit()
                    .unwrap()
            }))
            .await
            .unwrap()
        );
        let row = inspect(
            &projection,
            "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
            vec![],
            2,
        )
        .await;
        assert_eq!(integer(&row[0]).unwrap(), 3);
        assert_eq!(integer(&row[1]).unwrap(), 2);
    }
    async fn select_fixture(
        projection: &Arc<QueueProjection>,
        definition: &AdmittedQueueDefinition,
        request: ClaimRequest,
        now: i64,
        offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedClaimSelection> {
        projection
            .select_claim_candidates(
                ClaimReplayInput {
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        definition,
                        &[31; 32],
                        16 << 20,
                        job,
                    )?,
                    request,
                    now,
                    limits: QueueLimits::default(),
                    required_next_offset: offset,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                job,
                workers,
            )
            .await
    }
    #[tokio::test]
    async fn native_candidate_filters_priority_typed_ranges_and_wide_sets_without_leasing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native-selection.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let mut priorities = group("selection-priorities");
        priorities.work_updates = vec![patch("a", 1, Some(-9), None), patch("b", 1, Some(8), None)];
        priorities.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            priority: Some(-9),
            fields: BTreeMap::from([
                ("meta/rank".into(), Some(FieldValue::Integer(i64::MAX))),
                ("meta/tag".into(), Some(FieldValue::Text("match".into()))),
                ("meta/range".into(), Some(FieldValue::Float(2.0))),
            ]),
            ..Default::default()
        });
        priorities.work_updates[1].change = UnclaimedChange::Patch(WorkPatch {
            priority: Some(8),
            fields: BTreeMap::from([("meta/rank".into(), Some(FieldValue::Float(2.0)))]),
            ..Default::default()
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &priorities,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let base = ClaimRequest {
            queue: address(),
            request_id: "fresh".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: Some(Default::default()),
            include_body: false,
        };
        let cases = vec![
            (Predicate::Equal(FieldValue::Integer(1)), vec!["a"]),
            (Predicate::In(vec![FieldValue::Integer(2)]), vec!["b"]),
            (
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Integer(1))),
                    upper: Some(Bound::Excluded(FieldValue::Integer(2))),
                },
                vec!["a"],
            ),
            (
                Predicate::Range {
                    lower: Some(Bound::Excluded(FieldValue::Integer(1))),
                    upper: Some(Bound::Included(FieldValue::Integer(2))),
                },
                vec!["b"],
            ),
        ];
        let mut invalid = base.clone();
        invalid
            .filter
            .fields
            .insert("u".into(), Predicate::In(vec![]));
        assert!(matches!(
            select_fixture(&projection, &definition, invalid, 11, 3, &job, &workers).await,
            Err(QueueError::Invalid(_))
        ));
        for (predicate, expected) in cases {
            let mut request = base.clone();
            request.filter.fields.insert("u".into(), predicate);
            let selected = select_fixture(&projection, &definition, request, 11, 3, &job, &workers)
                .await
                .unwrap();
            assert_eq!(
                selected
                    .keys()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        for (predicate, expected) in [
            (
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Integer(i64::MAX))),
                    upper: None,
                },
                "a",
            ),
            (
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Float(1.0))),
                    upper: Some(Bound::Included(FieldValue::Float(3.0))),
                },
                "b",
            ),
        ] {
            let mut request = base.clone();
            request.filter.fields.insert("meta/rank".into(), predicate);
            let selected = select_fixture(&projection, &definition, request, 11, 3, &job, &workers)
                .await
                .unwrap();
            assert_eq!(selected.keys(), &[expected.to_string()]);
        }
        let mut mixed = base.clone();
        mixed.filter.fields = BTreeMap::from([
            (
                "u".into(),
                Predicate::In(vec![
                    FieldValue::Integer(1),
                    FieldValue::Integer(1),
                    FieldValue::Integer(2),
                ]),
            ),
            (
                "opaque".into(),
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Bytes(vec![6]))),
                    upper: Some(Bound::Included(FieldValue::Bytes(vec![8]))),
                },
            ),
            (
                "meta/tag".into(),
                Predicate::Equal(FieldValue::Text("match".into())),
            ),
            (
                "meta/range".into(),
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Float(1.0))),
                    upper: Some(Bound::Included(FieldValue::Float(3.0))),
                },
            ),
        ]);
        assert_eq!(
            select_fixture(
                &projection,
                &definition,
                mixed.clone(),
                11,
                3,
                &job,
                &workers
            )
            .await
            .unwrap()
            .keys(),
            &["a".to_string()]
        );
        let explain_projection = Arc::clone(&projection);
        let explain_request = mixed.clone();
        let details = tokio::task::spawn_blocking(move || {
            crate::tx::block_on_owned_apply(async {
                let writer = explain_projection.writer.lock().await;
                let connection = writer.as_ref().unwrap();
                let queue_id = integer(
                    &one(connection, "SELECT id FROM q7_queues", vec![], 1)
                        .await
                        .unwrap()
                        .unwrap()[0],
                )
                .unwrap();
                let (sql, pool) =
                    selection::candidate_sql(&explain_request, &schema(), queue_id, 11).unwrap();
                let mut rows = connection
                    .query(
                        &format!("EXPLAIN QUERY PLAN {sql}"),
                        vec![Value::Blob(pool)],
                    )
                    .await
                    .unwrap();
                let mut details = Vec::new();
                while let Some(row) = rows.next().await.unwrap() {
                    match row.get_value(3).unwrap() {
                        Value::Text(text) => details.push(text),
                        _ => panic!("non-text query plan"),
                    }
                }
                details
            })
        })
        .await
        .unwrap();
        for index in [
            "q7_field_equal",
            "q7_field_range",
            "q7_additional_equal",
            "q7_additional_range",
        ] {
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("SEARCH f") && detail.contains(index)),
                "{index}: {details:?}"
            );
        }
        mixed.filter.fields.insert(
            "meta/range".into(),
            Predicate::Range {
                lower: Some(Bound::Included(FieldValue::Float(1.0))),
                upper: Some(Bound::Excluded(FieldValue::Float(2.0))),
            },
        );
        assert!(
            select_fixture(&projection, &definition, mixed, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys()
                .is_empty()
        );
        let mut descending = base.clone();
        descending.order = ClaimOrder::PriorityDescending;
        assert_eq!(
            select_fixture(&projection, &definition, descending, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys(),
            &["b".to_string(), "a".to_string()]
        );
        let mut disjoint = base.clone();
        disjoint.filter.keys = Some(["a".into()].into());
        disjoint
            .filter
            .fields
            .insert("u".into(), Predicate::Equal(FieldValue::Integer(2)));
        assert!(
            select_fixture(&projection, &definition, disjoint, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys()
                .is_empty()
        );
        let mut keys = base.clone();
        keys.filter.keys = Some(
            (0..10001)
                .map(|n| format!("missing-{n}"))
                .chain(["a".into()])
                .collect(),
        );
        assert_eq!(
            select_fixture(&projection, &definition, keys, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys(),
            &["a".to_string()]
        );
        let mut values = base.clone();
        values.filter.fields.insert(
            "u".into(),
            Predicate::In((1..=10001).map(FieldValue::Integer).collect()),
        );
        assert_eq!(
            select_fixture(&projection, &definition, values, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys(),
            &["a".to_string(), "b".to_string()]
        );
        // Force a discovery collision: complete values still decide membership.
        execute_for_test(
            &projection,
            "UPDATE q7_records SET key_hash=?2 WHERE key=?1",
            vec![blob(b"b"), blob(&discovery_digest(b"a"))],
        )
        .await;
        let mut keys = base.clone();
        keys.filter.keys = Some(["a".into()].into());
        assert_eq!(
            select_fixture(&projection, &definition, keys, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys(),
            &["a".to_string()]
        );
        execute_for_test(
            &projection,
            "UPDATE q7_records SET key_hash=?2 WHERE key=?1",
            vec![blob(b"b"), blob(&discovery_digest(b"b"))],
        )
        .await;
        let row=inspect(&projection,"SELECT (SELECT COUNT(*) FROM q7_records WHERE lifecycle=1),(SELECT COUNT(*) FROM q7_requests),applied_next_offset FROM q7_queues",vec![],3).await;
        assert_eq!(integer(&row[0]).unwrap(), 0);
        assert_eq!(integer(&row[1]).unwrap(), 2);
        assert_eq!(integer(&row[2]).unwrap(), 3);
        let (record, _, _, _) = claim_fixture(
            ClaimFixture {
                before: &snapshot,
                definition: &definition,
                job: &job,
                identity: "leased",
                now: 10,
                token_seed: 11,
                selected: &["a", "b"],
            },
            false,
        );
        projection.apply(record, 3, &job, &workers).await.unwrap();
        assert!(
            select_fixture(
                &projection,
                &definition,
                base.clone(),
                11,
                4,
                &job,
                &workers
            )
            .await
            .unwrap()
            .keys()
            .is_empty()
        );
        assert_eq!(
            select_fixture(&projection, &definition, base, 1_000_010, 4, &job, &workers)
                .await
                .unwrap()
                .keys(),
            &["a".to_string(), "b".to_string()]
        );
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn native_candidate_conjunction_handles_5001_additional_fields() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wide-conjunction.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let mut wide = group("wide-fields");
        let mut update = patch("a", 1, None, None);
        update.change = UnclaimedChange::Patch(WorkPatch {
            fields: (0..5001)
                .map(|n| (format!("meta/f{n}"), Some(FieldValue::Integer(n))))
                .collect(),
            ..Default::default()
        });
        wide.work_updates.push(update);
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &wide,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let mut request = ClaimRequest {
            queue: address(),
            request_id: "wide-selector".into(),
            filter: Filter {
                fields: (0..5001)
                    .map(|n| {
                        (
                            format!("meta/f{n}"),
                            Predicate::Equal(FieldValue::Integer(n)),
                        )
                    })
                    .collect(),
                ..Default::default()
            },
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: Some(Default::default()),
            include_body: false,
        };
        assert_eq!(
            select_fixture(
                &projection,
                &definition,
                request.clone(),
                11,
                3,
                &job,
                &workers
            )
            .await
            .unwrap()
            .keys(),
            &["a".to_string()]
        );
        request.filter.fields.insert(
            "meta/f5000".into(),
            Predicate::Equal(FieldValue::Integer(-1)),
        );
        assert!(
            select_fixture(&projection, &definition, request, 11, 3, &job, &workers)
                .await
                .unwrap()
                .keys()
                .is_empty()
        );
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn selective_and_empty_current_replay_omit_opaque_payload_under_small_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selective-replay.db");
        let initial = QueueAdmission::new(1, 64 << 20).unwrap();
        let initial_job = Arc::new(initial.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &initial_job, &workers).await;
        let mut before = seed(&projection, &definition, &initial_job, &workers, true).await;
        for work in before.work.values_mut() {
            work.record.work.fields.clear();
            work.record.work.body = Bytes::new();
            work.field_coverage = FieldCoverage::Selected {
                names: Default::default(),
                prefixes: Default::default(),
            };
            work.body_loaded = false;
        }
        drop(definition);
        drop(initial_job);
        assert_eq!(initial.usage().unwrap(), (0, 0));
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let request = ClaimRequest {
            queue: address(),
            request_id: "selective".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: Some(Default::default()),
            include_body: false,
        };
        let (record, claimed, _, _) = claim_fixture_with_request(
            ClaimFixture {
                before: &before,
                definition: &definition,
                job: &job,
                identity: "selective",
                now: 10,
                token_seed: 11,
                selected: &["a", "b"],
            },
            false,
            request.clone(),
        );
        projection.apply(record, 2, &job, &workers).await.unwrap();
        let replay = read_claim_fixture(&projection, &definition, request, 11, 3, &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.records().len(), 2);
        assert!(
            replay
                .records()
                .iter()
                .all(|work| work.record.work.fields.is_empty() && work.record.work.body.is_empty())
        );
        drop(replay);
        let mut empty_request = ClaimRequest {
            queue: address(),
            request_id: "empty-current".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 4096,
            fields: Some(Default::default()),
            include_body: false,
        };
        let (record, _, _, _) = claim_fixture_with_request(
            ClaimFixture {
                before: &claimed,
                definition: &definition,
                job: &job,
                identity: "empty-current",
                now: 10,
                token_seed: 11,
                selected: &[],
            },
            false,
            empty_request.clone(),
        );
        projection.apply(record, 3, &job, &workers).await.unwrap();
        drop(projection);
        let projection = QueueProjection::open(&path, &job, &workers).await.unwrap();
        for now in [11, i64::MAX] {
            let replay = read_claim_fixture(
                &projection,
                &definition,
                empty_request.clone(),
                now,
                4,
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
            assert!(replay.records().is_empty());
        }
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET expired=1 WHERE identity=?1",
            vec![blob(b"empty-current")],
        )
        .await;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                empty_request.clone(),
                11,
                4,
                &job,
                &workers
            )
            .await,
            Err(QueueError::RequestExpired)
        ));
        empty_request.order = ClaimOrder::PriorityDescending;
        assert!(matches!(
            read_claim_fixture(
                &projection,
                &definition,
                empty_request,
                11,
                4,
                &job,
                &workers
            )
            .await,
            Err(QueueError::RequestConflict)
        ));
        drop(projection);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn claims_empty_reclaim_and_late_failures_publish_one_atomic_receipt_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("claims.db");
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let mut priority = group("prioritize");
        priority.work_updates.push(patch("b", 1, Some(-1), None));
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &priority,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        // Claim only metadata; the authoritative opaque field/body remain stored.
        for work in snapshot.work.values_mut() {
            work.record.work.fields.clear();
            work.record.work.body = bytes::Bytes::new();
            work.field_coverage = FieldCoverage::Selected {
                names: Default::default(),
                prefixes: Default::default(),
            };
            work.body_loaded = false;
        }
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let (record, claimed, expected, fp) = claim_fixture(
            ClaimFixture {
                before: &snapshot,
                definition: &definition,
                job: &job,
                identity: "claim",
                now: 30,
                token_seed: 11,
                selected: &["b", "a"],
            },
            false,
        );
        assert_eq!(
            projection
                .apply(record, 3, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            4
        );
        let (empty, _, empty_expected, empty_fp) = claim_fixture(
            ClaimFixture {
                before: &claimed,
                definition: &definition,
                job: &job,
                identity: "empty",
                now: 31,
                token_seed: 31,
                selected: &[],
            },
            false,
        );
        assert_eq!(
            projection
                .apply(empty, 4, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            5
        );
        let (bad, _, _, _) = claim_fixture(
            ClaimFixture {
                before: &claimed,
                definition: &definition,
                job: &job,
                identity: "bad",
                now: 1_000_030,
                token_seed: 41,
                selected: &["b", "a"],
            },
            true,
        );
        assert!(matches!(
            projection.apply(bad, 5, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let mut stale = claimed.clone();
        stale.work.get_mut("b").unwrap().record.version += 1;
        stale.versions.insert(
            RecordIdentity {
                collection: None,
                key: "b".into(),
            },
            stale.work["b"].record.version,
        );
        let (bad, _, _, _) = claim_fixture(
            ClaimFixture {
                before: &stale,
                definition: &definition,
                job: &job,
                identity: "stale",
                now: 1_000_030,
                token_seed: 41,
                selected: &["b", "a"],
            },
            false,
        );
        assert!(matches!(
            projection.apply(bad, 5, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let unchanged=inspect(&projection,"SELECT version,attempts,(SELECT applied_next_offset FROM q7_queues) FROM q7_records WHERE key=?1",vec![blob(b"a")],3).await;
        assert_eq!(unsigned(&unchanged[0]).unwrap(), 2);
        assert_eq!(integer(&unchanged[1]).unwrap(), 1);
        assert_eq!(integer(&unchanged[2]).unwrap(), 5);
        let (record, reclaimed, _, _) = claim_fixture(
            ClaimFixture {
                before: &claimed,
                definition: &definition,
                job: &job,
                identity: "reclaim",
                now: 1_000_030,
                token_seed: 41,
                selected: &["b", "a"],
            },
            false,
        );
        assert_eq!(
            projection
                .apply(record, 5, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            6
        );
        let (duplicate, _, _, _) = claim_fixture(
            ClaimFixture {
                before: &reclaimed,
                definition: &definition,
                job: &job,
                identity: "reclaim",
                now: 2_000_030,
                token_seed: 61,
                selected: &["b", "a"],
            },
            false,
        );
        assert!(matches!(
            projection.apply(duplicate, 6, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(projection);
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let receipt=inspect(&reopened,"SELECT fingerprint_format,fingerprint,outcome FROM q7_requests WHERE domain=1 AND identity=?1",vec![blob(b"claim")],3).await;
        assert_eq!(integer(&receipt[0]).unwrap(), 3);
        assert_eq!(bytes(&receipt[1]).unwrap(), fp);
        assert_eq!(bytes(&receipt[2]).unwrap(), expected);
        assert_eq!(
            fireweed_engine::queue_claim_receipt::view_claim_receipt(bytes(&receipt[2]).unwrap())
                .unwrap()
                .entries()
                .map(|entry| entry.unwrap().key)
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        let empty_receipt=inspect(&reopened,"SELECT fingerprint_format,fingerprint,outcome FROM q7_requests WHERE domain=1 AND identity=?1",vec![blob(b"empty")],3).await;
        assert_eq!(integer(&empty_receipt[0]).unwrap(), 3);
        assert_eq!(bytes(&empty_receipt[1]).unwrap(), empty_fp);
        assert_eq!(bytes(&empty_receipt[2]).unwrap(), empty_expected);
        assert_eq!(
            empty_expected,
            [b"FWCR1".as_slice(), &0u64.to_le_bytes()].concat()
        );
        let state=inspect(&reopened,"SELECT version,attempts,lease_until,(SELECT applied_next_offset FROM q7_queues),(SELECT next_sequence FROM q7_queues) FROM q7_records WHERE key=?1",vec![blob(b"a")],5).await;
        assert_eq!(unsigned(&state[0]).unwrap(), 3);
        assert_eq!(integer(&state[1]).unwrap(), 2);
        assert_eq!(integer(&state[2]).unwrap(), 2_000_030);
        assert_eq!(integer(&state[3]).unwrap(), 6);
        assert_eq!(unsigned(&state[4]).unwrap(), 2);
        let opaque=inspect(&reopened,"SELECT length(b.value),(SELECT length(value) FROM q7_fields WHERE record_id=r.id AND field=0) FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.key=?1",vec![blob(b"a")],2).await;
        assert_eq!(integer(&opaque[0]).unwrap(), 1 << 20);
        assert_eq!(integer(&opaque[1]).unwrap(), (1 << 20) + 4);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
