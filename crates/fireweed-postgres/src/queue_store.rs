//! Native asynchronous PostgreSQL baseline for API-007. Complete operation jobs
//! retain admission and connection ownership after caller cancellation.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_memory::*;
use fireweed_engine::{
    Clock,
    queue_admission::{QueueAdmission, QueueBufferCharge, QueueReservation},
    queue_contract::{QueueFuture, QueueStore},
    queue_index::ordered_key,
    queue_planner::{
        CommitDelta, RecordIdentity, RecordSnapshot, StoredWork, UniqueIdentity,
        insert_unique_probes, lease_hash, plan_commit_group, required_unique_probes,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_postgres::{Client, GenericClient, Row, Transaction, types::ToSql};

const DDL: &str = r#"
CREATE SCHEMA IF NOT EXISTS fireweed_api007;
CREATE TABLE IF NOT EXISTS fireweed_api007.queues (
 id BIGSERIAL PRIMARY KEY, address_hash BYTEA NOT NULL, tenant BYTEA NOT NULL, name BYTEA NOT NULL,
 definition BYTEA NOT NULL, next_sequence NUMERIC(20,0) NOT NULL DEFAULT 0, cursor_secret BYTEA NOT NULL);
CREATE INDEX IF NOT EXISTS queue_address_probe ON fireweed_api007.queues(address_hash);
CREATE TABLE IF NOT EXISTS fireweed_api007.records (
 id BIGSERIAL PRIMARY KEY, queue_id BIGINT NOT NULL REFERENCES fireweed_api007.queues(id), collection INT NOT NULL,
 key_hash BYTEA NOT NULL, key BYTEA NOT NULL, version NUMERIC(20,0) NOT NULL, active BOOLEAN NOT NULL,
 lifecycle SMALLINT NOT NULL DEFAULT 0, priority BIGINT NOT NULL DEFAULT 0, available_at BIGINT NOT NULL DEFAULT 0,
 eligible_since BIGINT NOT NULL DEFAULT 0, attempts BIGINT NOT NULL DEFAULT 0, lease_until BIGINT, lease_hash BYTEA,
 insertion_sequence NUMERIC(20,0) NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS record_key_probe ON fireweed_api007.records(queue_id,collection,key_hash);
CREATE INDEX IF NOT EXISTS claim_priority ON fireweed_api007.records(queue_id,priority,insertion_sequence) WHERE active AND collection=0;
CREATE INDEX IF NOT EXISTS claim_due ON fireweed_api007.records(queue_id,available_at,insertion_sequence) WHERE active AND collection=0;
CREATE INDEX IF NOT EXISTS claim_age ON fireweed_api007.records(queue_id,eligible_since,insertion_sequence) WHERE active AND collection=0;
CREATE INDEX IF NOT EXISTS lease_expiry ON fireweed_api007.records(queue_id,lease_until) WHERE active AND lifecycle=1;
CREATE TABLE IF NOT EXISTS fireweed_api007.bodies(record_id BIGINT PRIMARY KEY REFERENCES fireweed_api007.records(id), value BYTEA NOT NULL);
CREATE TABLE IF NOT EXISTS fireweed_api007.fields (
 record_id BIGINT NOT NULL REFERENCES fireweed_api007.records(id), queue_id BIGINT NOT NULL, collection INT NOT NULL,
 field INT NOT NULL, prefix BYTEA NOT NULL, hash BYTEA NOT NULL, full_key BYTEA NOT NULL, value BYTEA NOT NULL,
 PRIMARY KEY(record_id,field));
CREATE INDEX IF NOT EXISTS field_equal_probe ON fireweed_api007.fields(queue_id,collection,field,hash,record_id);
CREATE INDEX IF NOT EXISTS field_range_probe ON fireweed_api007.fields(queue_id,collection,field,prefix,record_id);
CREATE TABLE IF NOT EXISTS fireweed_api007.indexes (
 record_id BIGINT NOT NULL REFERENCES fireweed_api007.records(id), queue_id BIGINT NOT NULL, collection INT NOT NULL,
 index_no INT NOT NULL, prefix BYTEA NOT NULL, hash BYTEA NOT NULL, full_key BYTEA NOT NULL, is_unique BOOLEAN NOT NULL,
 PRIMARY KEY(record_id,index_no));
CREATE INDEX IF NOT EXISTS index_equal_probe ON fireweed_api007.indexes(queue_id,collection,index_no,hash,record_id);
CREATE INDEX IF NOT EXISTS index_order_probe ON fireweed_api007.indexes(queue_id,collection,index_no,prefix,record_id);
CREATE TABLE IF NOT EXISTS fireweed_api007.requests (
 id BIGSERIAL PRIMARY KEY, queue_id BIGINT NOT NULL REFERENCES fireweed_api007.queues(id), domain SMALLINT NOT NULL,
 identity_hash BYTEA NOT NULL, identity BYTEA NOT NULL, fingerprint BYTEA NOT NULL, expired BOOLEAN NOT NULL DEFAULT false,
 outcome BYTEA NOT NULL);
CREATE INDEX IF NOT EXISTS request_identity_probe ON fireweed_api007.requests(queue_id,domain,identity_hash);
"#;
const RECORD_COLUMNS: &str = "r.id,r.collection,r.key,r.version::text,r.active,r.lifecycle,r.priority,r.available_at,r.eligible_since,r.attempts,r.lease_until,r.lease_hash,r.insertion_sequence::text";
fn storage(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
fn encode<T: Serialize>(value: &T) -> QueueResult<Vec<u8>> {
    postcard::to_allocvec(value).map_err(storage)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> QueueResult<T> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(storage)?;
    if !rest.is_empty() {
        return Err(storage("trailing persisted bytes"));
    }
    Ok(value)
}
fn hash(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}
fn prefix(bytes: &[u8]) -> Vec<u8> {
    bytes[..bytes.len().min(256)].to_vec()
}
fn version(raw: String) -> QueueResult<u64> {
    raw.parse().map_err(storage)
}
fn key(raw: Vec<u8>) -> QueueResult<String> {
    String::from_utf8(raw).map_err(storage)
}
fn life(value: Lifecycle) -> i16 {
    match value {
        Lifecycle::Pending => 0,
        Lifecycle::Leased => 1,
        Lifecycle::Completed => 2,
        Lifecycle::Failed => 3,
    }
}
fn lifecycle(value: i16) -> QueueResult<Lifecycle> {
    match value {
        0 => Ok(Lifecycle::Pending),
        1 => Ok(Lifecycle::Leased),
        2 => Ok(Lifecycle::Completed),
        3 => Ok(Lifecycle::Failed),
        _ => Err(storage("corrupt lifecycle")),
    }
}
fn collection(schema: &QueueSchema, name: Option<&str>) -> QueueResult<i32> {
    match name {
        None => Ok(0),
        Some(name) => schema
            .state
            .keys()
            .position(|key| key == name)
            .map(|n| n as i32 + 1)
            .ok_or_else(|| QueueError::Invalid("undeclared state collection".into())),
    }
}
fn definition(schema: &QueueSchema, col: i32) -> QueueResult<&CollectionDefinition> {
    if col == 0 {
        Ok(&schema.work)
    } else {
        schema
            .state
            .values()
            .nth((col - 1) as usize)
            .ok_or_else(|| storage("corrupt collection id"))
    }
}
fn field_no(def: &CollectionDefinition, name: &str) -> QueueResult<i32> {
    def.fields
        .keys()
        .position(|key| key == name)
        .map(|n| n as i32)
        .ok_or_else(|| QueueError::Invalid("undeclared field".into()))
}
fn index_no(def: &CollectionDefinition, name: &str) -> QueueResult<i32> {
    def.indexes
        .keys()
        .position(|key| key == name)
        .map(|n| n as i32)
        .ok_or_else(|| QueueError::Invalid("undeclared index".into()))
}

struct Inner {
    clients: Vec<Mutex<Option<Client>>>,
    drivers: Mutex<Vec<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>>,
    admission: Arc<QueueAdmission>,
    slots: Arc<Semaphore>,
    next: AtomicUsize,
    done: Notify,
    clock: Arc<dyn Clock>,
    limits: QueueLimits,
}
/// Uses real SQL transactions and indexed selection, not the old blocking
/// PostgreSQL adapter or an in-memory projection. No production activation yet.
#[derive(Clone)]
pub struct PostgresQueueStore {
    inner: Arc<Inner>,
}
impl PostgresQueueStore {
    pub async fn connect(
        url: &str,
        clock: Arc<dyn Clock>,
        limits: QueueLimits,
        pool_size: usize,
    ) -> QueueResult<Self> {
        limits.validate()?;
        if !(1..=32).contains(&pool_size) {
            return Err(QueueError::Invalid("pool size must be 1..32".into()));
        }
        let mut clients = vec![];
        let mut drivers = vec![];
        for connection_index in 0..pool_size {
            #[cfg(not(feature = "tls"))]
            let (mut client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
                .await
                .map_err(storage)?;
            #[cfg(feature = "tls")]
            let (mut client, connection) = {
                let tls = native_tls::TlsConnector::new().map_err(storage)?;
                tokio_postgres::connect(url, postgres_native_tls::MakeTlsConnector::new(tls))
                    .await
                    .map_err(storage)?
            };
            drivers.push(tokio::spawn(connection));
            // A success must survive process/server loss, regardless of URL/session
            // defaults. Cluster crash-safety settings are verified, never changed.
            client
                .batch_execute("SET synchronous_commit = 'on'")
                .await
                .map_err(storage)?;
            let settings = client
                .query_one(
                    "SELECT current_setting('fsync'),current_setting('full_page_writes')",
                    &[],
                )
                .await
                .map_err(storage)?;
            if settings.get::<_, String>(0) != "on" || settings.get::<_, String>(1) != "on" {
                return Err(QueueError::Invalid(
                    "PostgreSQL requires fsync and full_page_writes enabled".into(),
                ));
            }
            if connection_index == 0 {
                let tx = client.transaction().await.map_err(storage)?;
                tx.execute("SELECT pg_advisory_xact_lock(6073190500807001)", &[])
                    .await
                    .map_err(storage)?;
                // CREATE INDEX IF NOT EXISTS still acquires table locks. Fence
                // existing queue operations before touching any dependent table:
                // every operation reads queues first and retains that lock until
                // commit. Otherwise DDL's queues->records lock order can deadlock
                // a writer that updates records before its queue sequence row.
                let exists: bool = tx
                    .query_one(
                        "SELECT to_regclass('fireweed_api007.queues') IS NOT NULL",
                        &[],
                    )
                    .await
                    .map_err(storage)?
                    .get(0);
                if exists {
                    tx.batch_execute("LOCK TABLE fireweed_api007.queues IN ACCESS EXCLUSIVE MODE")
                        .await
                        .map_err(storage)?;
                }
                tx.batch_execute(DDL).await.map_err(storage)?;
                tx.commit().await.map_err(storage)?;
            }
            clients.push(Mutex::new(Some(client)));
        }
        let bytes = limits
            .max_request_bytes
            .checked_mul(8)
            .ok_or(QueueError::TooLarge)?;
        Ok(Self {
            inner: Arc::new(Inner {
                clients,
                drivers: Mutex::new(drivers),
                admission: QueueAdmission::new(64, bytes)?,
                slots: Arc::new(Semaphore::new(pool_size)),
                next: AtomicUsize::new(0),
                done: Notify::new(),
                clock,
                limits,
            }),
        })
    }
    async fn dispatch<T, F>(&self, bytes: u64, job: F) -> QueueResult<T>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut Client, QueueReservation) -> QueueFuture<'a, T> + Send + 'static,
    {
        let reservation = self.inner.admission.reserve(bytes)?;
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            let _completion = JobCompletion(Arc::clone(&inner));
            async move {
                let permit = Arc::clone(&inner.slots)
                    .acquire_owned()
                    .await
                    .map_err(storage)?;
                let start = inner.next.fetch_add(1, Ordering::Relaxed) % inner.clients.len();
                let mut selected = None;
                for offset in 0..inner.clients.len() {
                    if let Ok(guard) =
                        inner.clients[(start + offset) % inner.clients.len()].try_lock()
                    {
                        selected = Some(guard);
                        break;
                    }
                }
                let mut guard =
                    selected.ok_or_else(|| storage("pool permit without free connection"))?;
                let result = match guard.as_mut() {
                    Some(client) => job(client, reservation).await,
                    None => Err(storage("store closed")),
                };
                drop(guard);
                drop(permit);
                result
            }
            .await
        });
        task.await.map_err(storage)?
    }
}
struct JobCompletion(Arc<Inner>);
impl Drop for JobCompletion {
    fn drop(&mut self) {
        self.0.done.notify_waiters();
    }
}
struct QueueRow<'a> {
    _schema_charge: QueueBufferCharge<'a>,
    _schema_headers: QueueBufferCharge<'a>,
    id: i64,
    schema: QueueSchema,
    next_sequence: u64,
    secret: Vec<u8>,
}
async fn queue_row<'a>(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    reservation: &'a QueueReservation,
) -> QueueResult<QueueRow<'a>> {
    let address_hash = hash(&encode(address)?);
    let rows=tx.query("SELECT id,octet_length(definition),next_sequence::text,cursor_secret FROM fireweed_api007.queues WHERE address_hash=$1 AND tenant=$2 AND name=$3 FOR UPDATE",&[&address_hash,&address.tenant.as_bytes(),&address.queue.as_bytes()]).await.map_err(storage)?;
    if rows.len() != 1 {
        return Err(if rows.is_empty() {
            QueueError::QueueNotFound
        } else {
            storage("duplicate queue authority")
        });
    }
    let row = &rows[0];
    let id: i64 = row.get(0);
    let schema_bytes = u64::try_from(row.get::<_, i32>(1)).map_err(storage)?;
    let charge = reservation.buffer(
        schema_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let definition = tx
        .query_one(
            "SELECT definition FROM fireweed_api007.queues WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(storage)?
        .get::<_, Vec<u8>>(0);
    let headers = reservation.buffer(persisted_schema_headers(&definition)?)?;
    Ok(QueueRow {
        _schema_charge: charge,
        _schema_headers: headers,
        id,
        schema: decode(&definition)?,
        next_sequence: version(row.get(2))?,
        secret: row.get(3),
    })
}

async fn receipt<'a, T: DeserializeOwned + PersistedStructure>(
    tx: &Transaction<'_>,
    queue: i64,
    domain: i16,
    id: &str,
    fp: &[u8],
    reservation: &'a QueueReservation,
    charges: &mut Vec<QueueBufferCharge<'a>>,
) -> QueueResult<Option<T>> {
    let rows=tx.query("SELECT identity,fingerprint,expired,outcome FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=$2 AND identity_hash=$3 AND identity=$4",
        &[&queue,&domain,&hash(id.as_bytes()),&id.as_bytes()]).await.map_err(storage)?;
    if rows.len() > 1 {
        return Err(storage("duplicate receipt authority"));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if row.get::<_, Vec<u8>>(1) != fp {
        return Err(QueueError::RequestConflict);
    }
    if row.get::<_, bool>(2) {
        return Err(QueueError::RequestExpired);
    }
    let raw = row.get::<_, Vec<u8>>(3);
    charges.push(reservation.buffer(T::stored_headers(&raw)?)?);
    Ok(Some(decode(&raw)?))
}
async fn save_receipt<T: Serialize>(
    tx: &Transaction<'_>,
    queue: i64,
    domain: i16,
    id: &str,
    fp: &[u8],
    value: &T,
) -> QueueResult<()> {
    tx.execute("INSERT INTO fireweed_api007.requests(queue_id,domain,identity_hash,identity,fingerprint,outcome) VALUES($1,$2,$3,$4,$5,$6)",
        &[&queue,&domain,&hash(id.as_bytes()),&id.as_bytes(),&fp,&encode(value)?]).await.map_err(storage)?;
    Ok(())
}
async fn load_fields<C: GenericClient + Sync>(
    client: &C,
    ids: &[i64],
    schema: &QueueSchema,
    cols: &BTreeMap<i64, i32>,
    selected: Option<&BTreeSet<String>>,
) -> QueueResult<BTreeMap<i64, Fields>> {
    if selected.is_some_and(BTreeSet::is_empty) || ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let ordinals: Option<Vec<i32>> = selected.map(|wanted| {
        cols.values()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .flat_map(|col| {
                definition(schema, col)
                    .into_iter()
                    .flat_map(move |d| wanted.iter().filter_map(move |name| field_no(d, name).ok()))
            })
            .collect()
    });
    let rows=client.query("SELECT record_id,field,value FROM fireweed_api007.fields WHERE record_id=ANY($1) AND ($2::int[] IS NULL OR field=ANY($2))",&[&ids,&ordinals]).await.map_err(storage)?;
    let mut out: BTreeMap<i64, Fields> = BTreeMap::new();
    for row in rows {
        let id: i64 = row.get(0);
        let col = *cols
            .get(&id)
            .ok_or_else(|| storage("foreign field record"))?;
        let name = definition(schema, col)?
            .fields
            .keys()
            .nth(row.get::<_, i32>(1) as usize)
            .ok_or_else(|| storage("corrupt field ordinal"))?;
        if selected.is_none_or(|wanted| wanted.contains(name)) {
            out.entry(id)
                .or_default()
                .insert(name.clone(), decode(&row.get::<_, Vec<u8>>(2))?);
        }
    }
    Ok(out)
}
async fn load_bodies<C: GenericClient + Sync>(
    client: &C,
    ids: &[i64],
) -> QueueResult<BTreeMap<i64, Vec<u8>>> {
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = client
        .query(
            "SELECT record_id,value FROM fireweed_api007.bodies WHERE record_id=ANY($1)",
            &[&ids],
        )
        .await
        .map_err(storage)?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}
fn record(row: &Row, fields: Fields, body: Vec<u8>) -> QueueResult<StoredWork> {
    let hash_value: Option<Vec<u8>> = row.get(11);
    Ok(StoredWork {
        record: WorkRecord {
            work: NewWork {
                key: key(row.get(2))?,
                priority: row.get(6),
                available_at: row.get(7),
                fields,
                body: body.into(),
            },
            version: version(row.get(3))?,
            insertion_sequence: version(row.get(12))?,
            lifecycle: lifecycle(row.get(5))?,
            eligible_since: row.get(8),
            attempts: u32::try_from(row.get::<_, i64>(9)).map_err(storage)?,
            lease_expires_at: row.get(10),
        },
        lease_hash: hash_value
            .map(|b| b.try_into().map_err(|_| storage("corrupt lease hash")))
            .transpose()?,
        fields_loaded: true,
        body_loaded: true,
    })
}
struct Loaded<'a> {
    _charge: QueueBufferCharge<'a>,
    _probe_charges: Vec<QueueBufferCharge<'a>>,
    snapshot: RecordSnapshot,
    ids: BTreeMap<RecordIdentity, i64>,
}
async fn load_group<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    group: &CommitGroup,
    reservation: &'a QueueReservation,
) -> QueueResult<Loaded<'a>> {
    let mut identities = BTreeSet::new();
    for key in group
        .claimed_updates
        .iter()
        .map(|u| &u.key)
        .chain(group.work_inserts.iter().map(|u| &u.key))
        .chain(group.work_updates.iter().map(|u| &u.key))
    {
        identities.insert(RecordIdentity {
            collection: None,
            key: key.clone(),
        });
    }
    for u in &group.state_updates {
        identities.insert(RecordIdentity {
            collection: Some(u.collection.clone()),
            key: u.key.clone(),
        });
    }
    let mut rows = vec![];
    for col_name in identities
        .iter()
        .map(|id| id.collection.clone())
        .collect::<BTreeSet<_>>()
    {
        let col = collection(&queue.schema, col_name.as_deref())?;
        let keys: Vec<Vec<u8>> = identities
            .iter()
            .filter(|id| id.collection == col_name)
            .map(|id| id.key.as_bytes().to_vec())
            .collect();
        let hashes: Vec<Vec<u8>> = keys.iter().map(|key| hash(key)).collect();
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE queue_id=$1 AND collection=$2 AND key_hash=ANY($3) AND key=ANY($4)"
        );
        rows.extend(
            tx.query(&sql, &[&queue.id, &col, &hashes, &keys])
                .await
                .map_err(storage)?,
        );
    }
    let ids_vec: Vec<i64> = rows
        .iter()
        .filter(|r| r.get::<_, bool>(4))
        .map(|r| r.get(0))
        .collect();
    // Only administrative body comparisons require old opaque work bodies.
    let body_keys:BTreeSet<_>=group.work_updates.iter().filter(|u|matches!(&u.change,UnclaimedChange::Administrative {patch,..} if patch.body.is_some())).map(|u|u.key.as_bytes()).collect();
    let body_ids: Vec<i64> = rows
        .iter()
        .filter(|r| {
            r.get::<_, bool>(4)
                && r.get::<_, i32>(1) == 0
                && body_keys.contains(r.get::<_, Vec<u8>>(2).as_slice())
        })
        .map(|r| r.get(0))
        .collect();
    let body_size=u64::try_from(tx.query_one("SELECT COALESCE(sum(octet_length(value)),0)::bigint FROM fireweed_api007.bodies WHERE record_id=ANY($1)",&[&body_ids]).await.map_err(storage)?.get::<_,i64>(0)).map_err(storage)?;
    let field_sizes=tx.query("SELECT COALESCE(sum(octet_length(value)+octet_length(full_key)),0)::bigint FROM fireweed_api007.fields WHERE record_id=ANY($1)",&[&ids_vec]).await.map_err(storage)?;
    let mut names_and_headers = record_headers(rows.len(), 0)?;
    for col in rows
        .iter()
        .map(|row| row.get::<_, i32>(1))
        .collect::<BTreeSet<_>>()
    {
        let col_ids: Vec<i64> = rows
            .iter()
            .filter(|row| row.get::<_, i32>(1) == col && row.get::<_, bool>(4))
            .map(|row| row.get(0))
            .collect();
        let def = definition(&queue.schema, col)?;
        let names: Vec<i64> = def.fields.keys().map(|name| name.len() as i64).collect();
        let size=tx.query_one("SELECT COALESCE(sum(($2::bigint[])[field+1]+256),0)::bigint FROM fireweed_api007.fields WHERE record_id=ANY($1)",&[&col_ids,&names]).await.map_err(storage)?.get::<_,i64>(0);
        names_and_headers = names_and_headers
            .checked_add(u64::try_from(size).map_err(storage)?)
            .ok_or(QueueError::TooLarge)?;
        if col != 0 {
            let name = queue
                .schema
                .state
                .keys()
                .nth((col - 1) as usize)
                .ok_or_else(|| storage("corrupt collection"))?;
            names_and_headers = names_and_headers
                .checked_add(
                    (name.len() as u64)
                        .checked_mul(col_ids.len() as u64)
                        .ok_or(QueueError::TooLarge)?,
                )
                .ok_or(QueueError::TooLarge)?;
        }
    }
    let retained = body_size
        .checked_add(names_and_headers)
        .and_then(|n| n.checked_add(field_sizes[0].get::<_, i64>(0) as u64))
        .and_then(|n| n.checked_mul(4))
        .ok_or(QueueError::TooLarge)?;
    let charge = reservation.buffer(retained)?;
    let cols = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i32>(1)))
        .collect();
    let mut fields = load_fields(tx, &ids_vec, &queue.schema, &cols, None).await?;
    let mut bodies = load_bodies(tx, &body_ids).await?;
    let mut snapshot = RecordSnapshot {
        next_sequence: queue.next_sequence,
        ..RecordSnapshot::default()
    };
    let mut ids = BTreeMap::new();
    for row in rows {
        let id: i64 = row.get(0);
        let col: i32 = row.get(1);
        let record_key = key(row.get(2))?;
        let collection_name = if col == 0 {
            None
        } else {
            Some(
                queue
                    .schema
                    .state
                    .keys()
                    .nth((col - 1) as usize)
                    .ok_or_else(|| storage("corrupt collection"))?
                    .clone(),
            )
        };
        let identity = RecordIdentity {
            collection: collection_name.clone(),
            key: record_key.clone(),
        };
        if ids.insert(identity.clone(), id).is_some() {
            return Err(storage("duplicate record identity"));
        }
        let v = version(row.get(3))?;
        snapshot.versions.insert(identity, v);
        if !row.get::<_, bool>(4) {
            continue;
        }
        if let Some(collection) = collection_name {
            snapshot.state.insert(
                (collection.clone(), record_key.clone()),
                StateRecord {
                    collection,
                    key: record_key,
                    version: v,
                    fields: fields.remove(&id).unwrap_or_default(),
                    value: Default::default(),
                },
            );
        } else {
            let materialized = body_ids.contains(&id);
            let body = if materialized {
                bodies
                    .remove(&id)
                    .ok_or_else(|| storage("missing administrative body"))?
            } else {
                vec![]
            };
            let mut stored = record(&row, fields.remove(&id).unwrap_or_default(), body)?;
            stored.body_loaded = materialized;
            snapshot.work.insert(record_key, stored);
        }
    }
    Ok(Loaded {
        _charge: charge,
        _probe_charges: vec![],
        snapshot,
        ids,
    })
}
async fn load_unique_probes<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    loaded: &mut Loaded<'a>,
    group: &CommitGroup,
    now: i64,
    reservation: &'a QueueReservation,
) -> QueueResult<()> {
    // Charge tuple amplification before ordered-key generation or cloning.
    let charge = reservation.buffer(group_expansion_budget(
        group,
        &loaded.snapshot,
        &queue.schema,
    )?)?;
    loaded._probe_charges.push(charge);
    let probes: Vec<_> = required_unique_probes(group, &loaded.snapshot, now, &queue.schema)?
        .into_iter()
        .filter(|probe| !loaded.snapshot.unique_probes_loaded.contains(probe))
        .collect();
    load_unique_authority(tx, queue, loaded, probes, reservation).await
}
async fn load_unique_authority<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    loaded: &mut Loaded<'a>,
    probes: Vec<UniqueIdentity>,
    reservation: &'a QueueReservation,
) -> QueueResult<()> {
    if probes.is_empty() {
        return Ok(());
    }
    let mut columns = vec![];
    let mut numbers = vec![];
    let mut hashes = vec![];
    let mut full = vec![];
    for probe in &probes {
        let col = collection(&queue.schema, probe.collection.as_deref())?;
        columns.push(col);
        numbers.push(index_no(definition(&queue.schema, col)?, &probe.index)?);
        hashes.push(hash(&probe.full_sort_key));
        full.push(probe.full_sort_key.clone());
    }
    let rows=tx.query("SELECT p.ordinal,r.id,octet_length(r.key) FROM UNNEST($2::int[],$3::int[],$4::bytea[],$5::bytea[]) WITH ORDINALITY AS p(col,no,hash,full_key,ordinal) JOIN fireweed_api007.indexes i ON i.queue_id=$1 AND i.collection=p.col AND i.index_no=p.no AND i.hash=p.hash AND i.full_key=p.full_key AND i.is_unique JOIN fireweed_api007.records r ON r.id=i.record_id AND r.active",&[&queue.id,&columns,&numbers,&hashes,&full]).await.map_err(storage)?;
    let owner_bytes = rows.iter().try_fold(0u64, |n, row| {
        n.checked_add(u64::try_from(row.get::<_, i32>(2)).map_err(storage)?)
            .and_then(|n| n.checked_add(512))
            .ok_or(QueueError::TooLarge)
    })?;
    loaded
        ._probe_charges
        .push(reservation.buffer(owner_bytes.checked_mul(4).ok_or(QueueError::TooLarge)?)?);
    let owner_ids: Vec<i64> = rows.iter().map(|row| row.get(1)).collect();
    let owner_rows=tx.query("SELECT id,key FROM fireweed_api007.records WHERE id=ANY($1) AND queue_id=$2 AND active",&[&owner_ids,&queue.id]).await.map_err(storage)?;
    let owners: BTreeMap<i64, String> = owner_rows
        .iter()
        .map(|row| Ok((row.get(0), key(row.get(1))?)))
        .collect::<QueueResult<_>>()?;
    let mut seen = BTreeSet::new();
    for row in rows {
        let ordinal: usize = usize::try_from(row.get::<_, i64>(0)).map_err(storage)?;
        if !seen.insert(ordinal) {
            return Err(storage("duplicate unique tuple authority"));
        }
        let probe = probes
            .get(
                ordinal
                    .checked_sub(1)
                    .ok_or_else(|| storage("invalid probe ordinal"))?,
            )
            .ok_or_else(|| storage("invalid probe ordinal"))?;
        let owner = RecordIdentity {
            collection: probe.collection.clone(),
            key: owners
                .get(&row.get::<_, i64>(1))
                .ok_or_else(|| storage("missing unique owner"))?
                .clone(),
        };
        let prospective = match &owner.collection {
            None => loaded
                .snapshot
                .work
                .get(&owner.key)
                .map(|w| &w.record.work.fields),
            Some(col) => loaded
                .snapshot
                .state
                .get(&(col.clone(), owner.key.clone()))
                .map(|s| &s.fields),
        };
        let retained = if let Some(fields) = prospective {
            let col = collection(&queue.schema, probe.collection.as_deref())?;
            let def = definition(&queue.schema, col)?;
            let columns = &def.indexes[&probe.index];
            ordered_key(
                &columns
                    .iter()
                    .map(|key| fields.get(key))
                    .collect::<Vec<_>>(),
            )? == probe.full_sort_key
        } else {
            !loaded.ids.contains_key(&owner)
        };
        if retained {
            loaded.snapshot.unique_owners.insert(probe.clone(), owner);
        }
    }
    loaded.snapshot.unique_probes_loaded.extend(probes);
    Ok(())
}

async fn persist_delta(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    delta: &CommitDelta,
    before: &RecordSnapshot,
    loaded: &mut Loaded<'_>,
) -> QueueResult<()> {
    let work_by_key: BTreeMap<_, _> = delta
        .work
        .iter()
        .map(|work| (work.record.work.key.as_str(), work))
        .collect();
    let state_by_key: BTreeMap<_, _> = delta
        .state
        .iter()
        .map(|state| ((state.collection.as_str(), state.key.as_str()), state))
        .collect();
    let missing: Vec<_> = delta
        .versions
        .iter()
        .map(|v| RecordIdentity {
            collection: v.collection.clone(),
            key: v.key.clone(),
        })
        .filter(|id| !loaded.ids.contains_key(id))
        .collect();
    if !missing.is_empty() {
        let n = missing.len() as i32;
        let rows = tx
            .query(
                "SELECT nextval('fireweed_api007.records_id_seq') FROM generate_series(1,$1)",
                &[&n],
            )
            .await
            .map_err(storage)?;
        for (identity, row) in missing.into_iter().zip(rows) {
            loaded.ids.insert(identity, row.get(0));
        }
    }
    let mut ids = vec![];
    let mut cols = vec![];
    let mut keys = vec![];
    let mut hashes = vec![];
    let mut versions = vec![];
    let mut active = vec![];
    let mut states = vec![];
    let mut priorities = vec![];
    let mut available = vec![];
    let mut eligible = vec![];
    let mut attempts = vec![];
    let mut until: Vec<Option<i64>> = vec![];
    let mut tokens: Vec<Option<Vec<u8>>> = vec![];
    let mut sequences = vec![];
    let mut payload_ids = vec![];
    let mut payloads = vec![];
    let mut changed_fields: BTreeMap<i64, (i32, Fields)> = BTreeMap::new();
    for v in &delta.versions {
        let identity = RecordIdentity {
            collection: v.collection.clone(),
            key: v.key.clone(),
        };
        let id = loaded.ids[&identity];
        let col = collection(&queue.schema, v.collection.as_deref())?;
        ids.push(id);
        cols.push(col);
        keys.push(v.key.as_bytes().to_vec());
        hashes.push(hash(v.key.as_bytes()));
        versions.push(v.version.to_string());
        if let Some(work) = if col == 0 {
            work_by_key.get(v.key.as_str()).copied()
        } else {
            None
        } {
            active.push(true);
            states.push(life(work.record.lifecycle));
            priorities.push(work.record.work.priority);
            available.push(work.record.work.available_at);
            eligible.push(work.record.eligible_since);
            attempts.push(i64::from(work.record.attempts));
            until.push(work.record.lease_expires_at);
            tokens.push(work.lease_hash.map(|hash| hash.to_vec()));
            sequences.push(work.record.insertion_sequence.to_string());
            let old = before.work.get(&v.key);
            if work.body_loaded
                && old.is_none_or(|old| {
                    !old.body_loaded || old.record.work.body != work.record.work.body
                })
            {
                payload_ids.push(id);
                payloads.push(work.record.work.body.to_vec());
            }
            if old.is_none_or(|old| old.record.work.fields != work.record.work.fields) {
                changed_fields.insert(id, (col, work.record.work.fields.clone()));
            }
        } else if let Some(state) = v
            .collection
            .as_ref()
            .and_then(|col| state_by_key.get(&(col.as_str(), v.key.as_str())).copied())
        {
            active.push(true);
            states.push(0);
            priorities.push(0);
            available.push(0);
            eligible.push(0);
            attempts.push(0);
            until.push(None);
            tokens.push(None);
            sequences.push("0".into());
            let old = before
                .state
                .get(&(state.collection.clone(), state.key.clone()));
            if true {
                payload_ids.push(id);
                payloads.push(state.value.to_vec());
            }
            if old.is_none_or(|old| old.fields != state.fields) {
                changed_fields.insert(id, (col, state.fields.clone()));
            }
        } else {
            active.push(false);
            states.push(0);
            priorities.push(0);
            available.push(0);
            eligible.push(0);
            attempts.push(0);
            until.push(None);
            tokens.push(None);
            sequences.push("0".into());
            changed_fields.insert(id, (col, Fields::new()));
        }
    }
    if !ids.is_empty() {
        tx.execute("INSERT INTO fireweed_api007.records(id,queue_id,collection,key_hash,key,version,active,lifecycle,priority,available_at,eligible_since,attempts,lease_until,lease_hash,insertion_sequence) \
            SELECT id,$1,col,key_hash,key,version::numeric,active,state,priority,available,eligible,attempts,lease_until,lease_hash,sequence::numeric FROM \
            UNNEST($2::bigint[],$3::int[],$4::bytea[],$5::bytea[],$6::text[],$7::bool[],$8::smallint[],$9::bigint[],$10::bigint[],$11::bigint[],$12::bigint[],$13::bigint[],$14::bytea[],$15::text[]) \
            AS x(id,col,key_hash,key,version,active,state,priority,available,eligible,attempts,lease_until,lease_hash,sequence) \
            ON CONFLICT(id) DO UPDATE SET version=EXCLUDED.version,active=EXCLUDED.active,lifecycle=EXCLUDED.lifecycle,priority=EXCLUDED.priority,available_at=EXCLUDED.available_at,eligible_since=EXCLUDED.eligible_since,attempts=EXCLUDED.attempts,lease_until=EXCLUDED.lease_until,lease_hash=EXCLUDED.lease_hash,insertion_sequence=EXCLUDED.insertion_sequence",
            &[&queue.id,&ids,&cols,&hashes,&keys,&versions,&active,&states,&priorities,&available,&eligible,&attempts,&until,&tokens,&sequences]).await.map_err(storage)?;
    }
    if !payload_ids.is_empty() {
        tx.execute("INSERT INTO fireweed_api007.bodies(record_id,value) SELECT * FROM UNNEST($1::bigint[],$2::bytea[]) ON CONFLICT(record_id) DO UPDATE SET value=EXCLUDED.value",
            &[&payload_ids,&payloads]).await.map_err(storage)?;
    }
    let deleted_ids: Vec<_> = ids
        .iter()
        .zip(&active)
        .filter_map(|(id, active)| (!active).then_some(*id))
        .collect();
    if !deleted_ids.is_empty() {
        tx.execute(
            "DELETE FROM fireweed_api007.bodies WHERE record_id=ANY($1)",
            &[&deleted_ids],
        )
        .await
        .map_err(storage)?;
    }
    if !changed_fields.is_empty() {
        let changed: Vec<_> = changed_fields.keys().copied().collect();
        tx.execute(
            "DELETE FROM fireweed_api007.fields WHERE record_id=ANY($1)",
            &[&changed],
        )
        .await
        .map_err(storage)?;
        tx.execute(
            "DELETE FROM fireweed_api007.indexes WHERE record_id=ANY($1)",
            &[&changed],
        )
        .await
        .map_err(storage)?;
        let mut record_ids = vec![];
        let mut col_ids = vec![];
        let mut numbers = vec![];
        let mut prefixes = vec![];
        let mut value_hashes = vec![];
        let mut full = vec![];
        let mut values = vec![];
        let mut ix_records = vec![];
        let mut ix_cols = vec![];
        let mut ix_numbers = vec![];
        let mut ix_prefixes = vec![];
        let mut ix_hashes = vec![];
        let mut ix_full = vec![];
        let mut uniques = vec![];
        for (id, (col, fields)) in &changed_fields {
            if deleted_ids.contains(id) {
                continue;
            }
            let def = definition(&queue.schema, *col)?;
            for (name, value) in fields {
                let sort = ordered_key(&[Some(value)])?;
                record_ids.push(*id);
                col_ids.push(*col);
                numbers.push(field_no(def, name)?);
                prefixes.push(prefix(&sort));
                value_hashes.push(hash(&sort));
                full.push(sort);
                values.push(encode(value)?);
            }
            for (no, (name, columns)) in def.indexes.iter().enumerate() {
                let sort = ordered_key(
                    &columns
                        .iter()
                        .map(|key| fields.get(key))
                        .collect::<Vec<_>>(),
                )?;
                ix_records.push(*id);
                ix_cols.push(*col);
                ix_numbers.push(no as i32);
                ix_prefixes.push(prefix(&sort));
                ix_hashes.push(hash(&sort));
                ix_full.push(sort);
                uniques.push(def.unique_indexes.contains(name));
            }
        }
        if !record_ids.is_empty() {
            tx.execute("INSERT INTO fireweed_api007.fields(record_id,queue_id,collection,field,prefix,hash,full_key,value) SELECT rid,$1,col,no,prefix,hash,full_key,value FROM UNNEST($2::bigint[],$3::int[],$4::int[],$5::bytea[],$6::bytea[],$7::bytea[],$8::bytea[]) AS x(rid,col,no,prefix,hash,full_key,value)",
            &[&queue.id,&record_ids,&col_ids,&numbers,&prefixes,&value_hashes,&full,&values]).await.map_err(storage)?;
        }
        if !ix_records.is_empty() {
            tx.execute("INSERT INTO fireweed_api007.indexes(record_id,queue_id,collection,index_no,prefix,hash,full_key,is_unique) SELECT rid,$1,col,no,prefix,hash,full_key,is_unique FROM UNNEST($2::bigint[],$3::int[],$4::int[],$5::bytea[],$6::bytea[],$7::bytea[],$8::bool[]) AS x(rid,col,no,prefix,hash,full_key,is_unique)",
            &[&queue.id,&ix_records,&ix_cols,&ix_numbers,&ix_prefixes,&ix_hashes,&ix_full,&uniques]).await.map_err(storage)?;
        }
    }
    tx.execute(
        "UPDATE fireweed_api007.queues SET next_sequence=$2::text::numeric WHERE id=$1",
        &[&queue.id, &delta.next_sequence.to_string()],
    )
    .await
    .map_err(storage)?;
    Ok(())
}
type Parameters = Vec<Box<dyn ToSql + Sync + Send>>;
fn parameter<T: ToSql + Sync + Send + 'static>(params: &mut Parameters, value: T) -> String {
    params.push(Box::new(value));
    format!("${}", params.len())
}
fn refs(params: &Parameters) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|v| v.as_ref() as &(dyn ToSql + Sync))
        .collect()
}
fn filter_sql(
    filter: &Filter,
    def: &CollectionDefinition,
    params: &mut Parameters,
) -> QueueResult<String> {
    let mut sql = String::new();
    if let Some(keys) = &filter.keys {
        let raw: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let hashes: Vec<Vec<u8>> = raw.iter().map(|key| hash(key)).collect();
        let hp = parameter(params, hashes);
        let kp = parameter(params, raw);
        sql.push_str(&format!(" AND r.key_hash=ANY({hp}) AND r.key=ANY({kp})"));
    }
    for (field, predicate) in &filter.fields {
        let no = parameter(params, field_no(def, field)?);
        sql.push_str(&format!(" AND EXISTS(SELECT 1 FROM fireweed_api007.fields f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND f.field={no}"));
        match predicate {
            Predicate::Equal(value) => {
                let bytes = ordered_key(&[Some(value)])?;
                let hp = parameter(params, hash(&bytes));
                let vp = parameter(params, bytes);
                sql.push_str(&format!(" AND f.hash={hp} AND f.full_key={vp}"));
            }
            Predicate::In(values) => {
                let bytes: Vec<_> = values
                    .iter()
                    .map(|v| ordered_key(&[Some(v)]))
                    .collect::<QueueResult<_>>()?;
                let hashes: Vec<_> = bytes.iter().map(|b| hash(b)).collect();
                let hp = parameter(params, hashes);
                let vp = parameter(params, bytes);
                sql.push_str(&format!(" AND f.hash=ANY({hp}) AND f.full_key=ANY({vp})"));
            }
            Predicate::Range { lower, upper } => {
                for (bound, lower) in [(lower, true), (upper, false)] {
                    if let Some(bound) = bound {
                        let (value, inclusive) = match bound {
                            Bound::Included(v) => (v, true),
                            Bound::Excluded(v) => (v, false),
                        };
                        let bytes = ordered_key(&[Some(value)])?;
                        let pp = parameter(params, prefix(&bytes));
                        let vp = parameter(params, bytes);
                        let prefix_op = if lower { ">=" } else { "<=" };
                        let op = match (lower, inclusive) {
                            (true, true) => ">=",
                            (true, false) => ">",
                            (false, true) => "<=",
                            (false, false) => "<",
                        };
                        sql.push_str(&format!(
                            " AND f.prefix{prefix_op}{pp} AND f.full_key{op}{vp}"
                        ));
                    }
                }
            }
        }
        sql.push(')');
    }
    Ok(sql)
}
#[derive(Debug, Serialize, Deserialize)]
struct ClaimReplay {
    items: Vec<(i64, Vec<u8>)>,
}
async fn hydrate_work(
    tx: &Transaction<'_>,
    rows: &[Row],
    schema: &QueueSchema,
    selected: Option<&BTreeSet<String>>,
    include_body: bool,
    now: i64,
) -> QueueResult<Vec<StoredWork>> {
    let ids: Vec<_> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
    let cols = rows.iter().map(|r| (r.get::<_, i64>(0), 0)).collect();
    let mut fields = load_fields(tx, &ids, schema, &cols, selected).await?;
    let mut bodies = if include_body {
        load_bodies(tx, &ids).await?
    } else {
        BTreeMap::new()
    };
    rows.iter()
        .map(|r| {
            let id = r.get::<_, i64>(0);
            let mut work = record(
                r,
                fields.remove(&id).unwrap_or_default(),
                if include_body {
                    bodies
                        .remove(&id)
                        .ok_or_else(|| storage("missing work body"))?
                } else {
                    vec![]
                },
            )?;
            if work.record.lifecycle == Lifecycle::Leased
                && work
                    .record
                    .lease_expires_at
                    .is_some_and(|expiry| expiry <= now)
            {
                work.record.lifecycle = Lifecycle::Pending;
                work.record.lease_expires_at = None;
                work.lease_hash = None;
            }
            Ok(work)
        })
        .collect()
}
fn combine_deltas(deltas: &[CommitDelta], initial: u64) -> CommitDelta {
    let mut work: BTreeMap<String, Option<StoredWork>> = BTreeMap::new();
    let mut states: BTreeMap<(String, String), Option<StateRecord>> = BTreeMap::new();
    let mut versions = BTreeMap::new();
    let mut next = initial;
    for delta in deltas {
        for row in &delta.work {
            work.insert(row.record.work.key.clone(), Some(row.clone()));
        }
        for key in &delta.deleted_work {
            work.insert(key.clone(), None);
        }
        for row in &delta.state {
            states.insert((row.collection.clone(), row.key.clone()), Some(row.clone()));
        }
        for key in &delta.deleted_state {
            states.insert(key.clone(), None);
        }
        for v in &delta.versions {
            versions.insert(
                RecordIdentity {
                    collection: v.collection.clone(),
                    key: v.key.clone(),
                },
                v.clone(),
            );
        }
        next = delta.next_sequence;
    }
    let mut out = CommitDelta {
        work: vec![],
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: versions.into_values().collect(),
        next_sequence: next,
        receipt: CommitReceipt {
            request_id: "internal-packed-delta".into(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        },
    };
    for (key, value) in work {
        match value {
            Some(value) => out.work.push(value),
            None => out.deleted_work.push(key),
        }
    }
    for (key, value) in states {
        match value {
            Some(value) => out.state.push(value),
            None => out.deleted_state.push(key),
        }
    }
    out
}
fn load_envelope(groups: &[CommitGroup]) -> CommitGroup {
    let mut all = CommitGroup {
        request_id: "internal-read-set".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
    };
    for group in groups {
        all.claimed_updates.extend(group.claimed_updates.clone());
        all.state_updates.extend(group.state_updates.clone());
        all.work_inserts.extend(group.work_inserts.clone());
        all.work_updates.extend(group.work_updates.clone());
    }
    all
}
impl QueueStore for PostgresQueueStore {
    fn provision(&self, request: ProvisionRequest) -> QueueFuture<'_, ()> {
        Box::pin(async move {
            request.validate(&self.inner.limits)?;
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;
                let address_hash=hash(&encode(&request.queue)?);
                let lock=i64::from_be_bytes(address_hash[..8].try_into().map_err(storage)?);
                tx.execute("SELECT pg_advisory_xact_lock($1)",&[&lock]).await.map_err(storage)?;
                let rows=tx.query("SELECT definition FROM fireweed_api007.queues WHERE address_hash=$1 AND tenant=$2 AND name=$3",
                    &[&address_hash,&request.queue.tenant.as_bytes(),&request.queue.queue.as_bytes()]).await.map_err(storage)?;
                let encoded=encode(&request.schema)?;
                if let Some(row)=rows.first() {
                    if rows.len()!=1{return Err(storage("duplicate queue authority"));}
                    if row.get::<_,Vec<u8>>(0)!=encoded{return Err(QueueError::DefinitionConflict);}
                }else{
                    let token=fireweed_engine::generate_query_lease_token().map_err(storage)?;
                    let secret=hash(token.as_str().as_bytes());
                    tx.execute("INSERT INTO fireweed_api007.queues(address_hash,tenant,name,definition,cursor_secret) VALUES($1,$2,$3,$4,$5)",
                        &[&address_hash,&request.queue.tenant.as_bytes(),&request.queue.queue.as_bytes(),&encoded,&secret]).await.map_err(storage)?;
                }tx.commit().await.map_err(storage)?;Ok(())
            })).await
        })
    }
    fn commit(&self, request: CommitBatch) -> QueueFuture<'_, Vec<CommitReceipt>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge, move |client, reservation| {
                Box::pin(async move {
                    let tx = client.transaction().await.map_err(storage)?;
                    let queue = queue_row(&tx, &request.queue, &reservation).await?;
                    request.validate(&queue.schema, &limits)?;
                    let mut receipt_charges = vec![];
                    let mut out = vec![];
                    let mut deltas = vec![];
                    let mut known = vec![];
                    let mut fresh = vec![];
                    for group in &request.groups {
                        let fp = group.fingerprint(&request.queue)?;
                        let outcome = match receipt::<CommitReceipt>(
                            &tx,
                            queue.id,
                            2,
                            &group.request_id,
                            &fp,
                            &reservation,
                            &mut receipt_charges,
                        )
                        .await
                        {
                            Ok(value) => value,
                            Err(QueueError::RequestConflict) => Some(CommitReceipt {
                                request_id: group.request_id.clone(),
                                outcome: GroupOutcome::RequestConflict,
                            }),
                            Err(QueueError::RequestExpired) => Some(CommitReceipt {
                                request_id: group.request_id.clone(),
                                outcome: GroupOutcome::RequestExpired,
                            }),
                            Err(error) => return Err(error),
                        };
                        if outcome.is_none() {
                            fresh.push(group.clone());
                        }
                        known.push((fp, outcome));
                    }
                    // A retained receipt never depends on current addressed record materialization.
                    let read_set = load_envelope(&fresh);
                    let mut loaded = load_group(&tx, &queue, &read_set, &reservation).await?;
                    let before = loaded.snapshot.clone();
                    for (group, (fp, known)) in request.groups.iter().zip(known) {
                        if let Some(known) = known {
                            out.push(known);
                            continue;
                        }
                        let now = timestamp_nanos(clock.now())?;
                        load_unique_probes(&tx, &queue, &mut loaded, group, now, &reservation)
                            .await?;
                        let delta =
                            plan_commit_group(group, &mut loaded.snapshot, now, &queue.schema)?;
                        save_receipt(&tx, queue.id, 2, &group.request_id, &fp, &delta.receipt)
                            .await?;
                        out.push(delta.receipt.clone());
                        deltas.push(delta);
                    }
                    let combined = combine_deltas(&deltas, queue.next_sequence);
                    let _output_charge = reservation.buffer(
                        encoded_len(&combined)?
                            .checked_mul(4)
                            .ok_or(QueueError::TooLarge)?,
                    )?;
                    persist_delta(&tx, &queue, &combined, &before, &mut loaded).await?;
                    tx.commit().await.map_err(storage)?;
                    Ok(out)
                })
            })
            .await
        })
    }
    fn insert(&self, request: InsertRequest) -> QueueFuture<'_, Vec<InsertOutcome>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge, move |client, reservation| {
                Box::pin(async move {
                    let tx = client.transaction().await.map_err(storage)?;
                    let queue = queue_row(&tx, &request.queue, &reservation).await?;
                    request.validate(&queue.schema, &limits)?;
                    let fp = fingerprint("insert", &request)?;
                    let mut receipt_charges = vec![];
                    if let Some(found) = receipt(
                        &tx,
                        queue.id,
                        0,
                        &request.request_id,
                        &fp,
                        &reservation,
                        &mut receipt_charges,
                    )
                    .await?
                    {
                        return Ok(found);
                    }
                    let mut outcomes = vec![];
                    let mut deltas = vec![];
                    let read_set = CommitGroup {
                        request_id: "insert-read-set".into(),
                        claimed_updates: vec![],
                        state_updates: vec![],
                        work_inserts: request.items.clone(),
                        work_updates: vec![],
                    };
                    let mut loaded = load_group(&tx, &queue, &read_set, &reservation).await?;
                    let before = loaded.snapshot.clone();
                    // Reserve tuple/name amplification before materializing all
                    // prospective insert keys. SQL authority loads once per vector;
                    // the shared planner still decides each item in request order.
                    loaded
                        ._probe_charges
                        .push(reservation.buffer(group_expansion_budget(
                            &read_set,
                            &loaded.snapshot,
                            &queue.schema,
                        )?)?);
                    let probes = insert_unique_probes(&request.items, &queue.schema)?
                        .into_iter()
                        .collect();
                    load_unique_authority(&tx, &queue, &mut loaded, probes, &reservation).await?;
                    for item in &request.items {
                        let mut group = CommitGroup {
                            request_id: request.request_id.clone(),
                            claimed_updates: vec![],
                            state_updates: vec![],
                            work_inserts: vec![item.clone()],
                            work_updates: vec![],
                        };
                        let old = loaded.snapshot.work.get(&item.key).cloned();
                        if request.mode == InsertMode::Upsert
                            && let Some(old) = &old
                        {
                            group.work_inserts.clear();
                            group.work_updates.push(UnclaimedUpdate {
                                key: item.key.clone(),
                                expect: StateExpect::Version(old.record.version),
                                change: UnclaimedChange::Replace(item.clone()),
                            });
                        }
                        let now = timestamp_nanos(clock.now())?;
                        let delta =
                            plan_commit_group(&group, &mut loaded.snapshot, now, &queue.schema)?;
                        deltas.push(delta.clone());
                        outcomes.push(match delta.receipt.outcome {
                            GroupOutcome::Committed { versions } => {
                                let version = versions
                                    .first()
                                    .ok_or_else(|| storage("missing insert version"))?
                                    .version;
                                if old.is_some() {
                                    InsertOutcome::Replaced {
                                        key: item.key.clone(),
                                        version,
                                    }
                                } else {
                                    InsertOutcome::Inserted {
                                        key: item.key.clone(),
                                        version,
                                    }
                                }
                            }
                            GroupOutcome::Rejected(reason) => InsertOutcome::Rejected {
                                key: item.key.clone(),
                                reason,
                            },
                            _ => return Err(storage("invalid planned insert outcome")),
                        });
                    }
                    let combined = combine_deltas(&deltas, queue.next_sequence);
                    let _charge = reservation.buffer(
                        encoded_len(&combined)?
                            .checked_mul(4)
                            .ok_or(QueueError::TooLarge)?,
                    )?;
                    persist_delta(&tx, &queue, &combined, &before, &mut loaded).await?;
                    save_receipt(&tx, queue.id, 0, &request.request_id, &fp, &outcomes).await?;
                    tx.commit().await.map_err(storage)?;
                    Ok(outcomes)
                })
            })
            .await
        })
    }
    fn claim(&self, request: ClaimRequest) -> QueueFuture<'_, Vec<ClaimedWork>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(request.max_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;request.validate(&queue.schema,&limits)?;
                let fp=request.fingerprint()?;let now=timestamp_nanos(clock.now())?;
                let mut receipt_charges=vec![];
                if let Some(replay)=receipt::<ClaimReplay>(&tx,queue.id,1,&request.request_id,&fp,&_owned_job,&mut receipt_charges).await? {
                    if replay.items.is_empty(){return Ok(vec![]);}
                    let ids:Vec<_>=replay.items.iter().map(|(id,_)|*id).collect();
                    let sql=format!("SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE r.id=ANY($1) AND r.queue_id=$2 AND r.active");
                    let rows=tx.query(&sql,&[&ids,&queue.id]).await.map_err(storage)?;
                    if rows.len()!=ids.len(){return Err(QueueError::ClaimExpired);}
                    let metadata:BTreeMap<i64,&Row>=rows.iter().map(|row|(row.get(0),row)).collect();
                    for (id,token) in &replay.items {let row=metadata.get(id).ok_or(QueueError::ClaimExpired)?;
                        if row.get::<_,i16>(5)!=1 || row.get::<_,Option<i64>>(10).is_none_or(|expiry|expiry<=now) || row.get::<_,Option<Vec<u8>>>(11).as_deref()!=Some(lease_hash(token).as_slice()) {return Err(QueueError::ClaimExpired);}}
                    let selected_fields=selected_field_numbers(&queue.schema.work,request.fields.as_ref())?;
                    payload_budget(&tx,&ids,selected_fields.as_deref(),request.include_body,request.max_bytes,&queue.schema.work).await?;
                    let _headers=_owned_job.buffer(materialized_headers(&tx,&rows.iter().map(|row|row.get::<_,i64>(0)).collect::<Vec<_>>(),selected_fields.as_deref()).await?)?;
                let stored=hydrate_work(&tx,&rows,&queue.schema,request.fields.as_ref(),request.include_body,now).await?;
                    let map:BTreeMap<_,_>=rows.iter().map(|row|row.get::<_,i64>(0)).zip(stored).collect();let mut out=vec![];
                    for (id,token) in replay.items {
                        let work=map.get(&id).ok_or(QueueError::ClaimExpired)?;
                        if work.record.lifecycle!=Lifecycle::Leased||work.record.lease_expires_at.is_none_or(|expiry|expiry<=now)||work.lease_hash!=Some(lease_hash(&token)){return Err(QueueError::ClaimExpired);}
                        out.push(ClaimedWork {record:work.record.clone(),lease:LeaseCredential {token}});
                    }
                    if encoded_len(&out)?>request.max_bytes{return Err(QueueError::TooLarge);}return Ok(out);
                }
                let mut params:Parameters=vec![Box::new(queue.id),Box::new(request.eligible_at)];
                let filter=filter_sql(&request.filter,&queue.schema.work,&mut params)?;
                let nowp=parameter(&mut params,now);let boundp=parameter(&mut params,(queue.schema.progress_bound_nanos/4*3).to_string());
                let limitp=parameter(&mut params,i64::from(request.max_records));
                let order=match request.order {ClaimOrder::PriorityAscending=>"r.priority ASC",ClaimOrder::PriorityDescending=>"r.priority DESC",ClaimOrder::ScheduledAscending=>"r.available_at ASC"};
                let sql=format!("SELECT r.id,octet_length(r.key) FROM fireweed_api007.records r WHERE r.queue_id=$1 AND r.collection=0 AND r.active AND (r.lifecycle=0 OR (r.lifecycle=1 AND r.lease_until<={nowp}::bigint)) AND r.available_at<=$2{filter} ORDER BY CASE WHEN r.eligible_since::numeric+{boundp}::text::numeric<={nowp}::bigint::numeric THEN r.eligible_since::numeric ELSE 999999999999999999999999::numeric END,{order},r.insertion_sequence LIMIT {limitp} FOR UPDATE OF r SKIP LOCKED");
                let candidates=tx.query(&sql,&refs(&params)).await.map_err(storage)?;
                let candidate_ids:Vec<i64>=candidates.iter().map(|row|row.get(0)).collect();
                let selected_fields=selected_field_numbers(&queue.schema.work,request.fields.as_ref())?;
                let sizes=payload_sizes(&tx,&candidate_ids,selected_fields.as_deref(),request.include_body,&queue.schema.work).await?;
                let mut total=0u64;let mut fitting=vec![];
                for row in &candidates {let id:i64=row.get(0);let bytes=sizes.get(&id).copied().unwrap_or(0).checked_add(u64::try_from(row.get::<_,i32>(1)).map_err(storage)?).ok_or(QueueError::TooLarge)?;
                    if total.checked_add(bytes).is_none_or(|n|n>request.max_bytes){break;}total+=bytes;fitting.push(id);}
                if !candidates.is_empty()&&fitting.is_empty(){return Err(QueueError::TooLarge);}
                let sql=format!("SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE r.id=ANY($1) AND r.queue_id=$2");
                let rows=tx.query(&sql,&[&fitting,&queue.id]).await.map_err(storage)?;
                let mut by_id:BTreeMap<i64,Row>=rows.into_iter().map(|row|(row.get(0),row)).collect();
                let rows:Vec<Row>=fitting.iter().map(|id|by_id.remove(id).ok_or_else(||storage("missing selected record"))).collect::<QueueResult<_>>()?;
                let _headers=_owned_job.buffer(materialized_headers(&tx,&rows.iter().map(|row|row.get::<_,i64>(0)).collect::<Vec<_>>(),selected_fields.as_deref()).await?)?;
                let stored=hydrate_work(&tx,&rows,&queue.schema,request.fields.as_ref(),request.include_body,now).await?;
                let expiry=now.checked_add(i64::try_from(request.lease_nanos).map_err(storage)?).ok_or_else(||QueueError::Invalid("lease expiry overflow".into()))?;
                let mut out=vec![];let mut replay=ClaimReplay {items:vec![]};let mut ids=vec![];let mut token_hashes=vec![];let mut versions=vec![];let mut attempts=vec![];
                for (row,mut work) in rows.iter().zip(stored) {
                    let token=fireweed_engine::generate_query_lease_token().map_err(storage)?.as_str().as_bytes().to_vec();
                    work.record.version=work.record.version.checked_add(1).ok_or_else(||storage("version exhausted"))?;
                    work.record.attempts=work.record.attempts.checked_add(1).ok_or_else(||storage("attempts exhausted"))?;
                    work.record.lifecycle=Lifecycle::Leased;work.record.lease_expires_at=Some(expiry);
                    let claimed=ClaimedWork {record:work.record,lease:LeaseCredential {token:token.clone()}};
                    out.push(claimed);
                    if encoded_len(&out)?>request.max_bytes {
                        out.pop();if out.is_empty(){return Err(QueueError::TooLarge);}break;
                    }
                    let id:i64=row.get(0);ids.push(id);token_hashes.push(lease_hash(&token).to_vec());versions.push(out.last().ok_or_else(||storage("missing claim"))?.record.version.to_string());attempts.push(i64::from(out.last().ok_or_else(||storage("missing claim"))?.record.attempts));replay.items.push((id,token));
                }
                if !ids.is_empty(){tx.execute("UPDATE fireweed_api007.records r SET lifecycle=1,lease_until=$1,lease_hash=x.hash,version=x.version::numeric,attempts=x.attempts FROM UNNEST($2::bigint[],$3::bytea[],$4::text[],$5::bigint[]) AS x(id,hash,version,attempts) WHERE r.id=x.id AND r.queue_id=$6",&[&expiry,&ids,&token_hashes,&versions,&attempts,&queue.id]).await.map_err(storage)?;}
                save_receipt(&tx,queue.id,1,&request.request_id,&fp,&replay).await?;tx.commit().await.map_err(storage)?;Ok(out)
            })).await
        })
    }
    fn read_state(&self, request: StateReadRequest) -> QueueFuture<'_, Vec<Option<StateRecord>>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(request.max_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;request.validate(&queue.schema,&limits)?;
                let col=collection(&queue.schema,Some(&request.collection))?;
                let keys:Vec<Vec<u8>>=request.keys.iter().map(|key|key.as_bytes().to_vec()).collect();let hashes:Vec<_>=keys.iter().map(|key|hash(key)).collect();
                let rows=tx.query("SELECT id,key,version::text FROM fireweed_api007.records WHERE queue_id=$1 AND collection=$2 AND active AND key_hash=ANY($3) AND key=ANY($4)",&[&queue.id,&col,&hashes,&keys]).await.map_err(storage)?;
                let ids:Vec<i64>=rows.iter().map(|r|r.get(0)).collect();
                payload_budget(&tx,&ids,None,true,request.max_bytes,definition(&queue.schema,col)?).await?;
                let _headers=_owned_job.buffer(materialized_headers(&tx,&ids,None).await?)?;
                let collection_size=(ids.len() as u64).checked_mul(request.collection.len() as u64).ok_or(QueueError::TooLarge)?;
                if collection_size>request.max_bytes{return Err(QueueError::TooLarge);}
                let cols=ids.iter().map(|id|(*id,col)).collect();let mut fields=load_fields(&tx,&ids,&queue.schema,&cols,None).await?;let mut bodies=load_bodies(&tx,&ids).await?;
                let mut found=BTreeMap::new();
                for row in rows {let id:i64=row.get(0);let key=key(row.get(1))?;let value=bodies.remove(&id).ok_or_else(||storage("missing state body"))?;
                    let state=StateRecord {collection:request.collection.clone(),key:key.clone(),version:version(row.get(2))?,fields:fields.remove(&id).unwrap_or_default(),value:value.into()};
                    if found.insert(key,state).is_some(){return Err(storage("duplicate state identity"));}
                }
                let borrowed:Vec<_>=request.keys.iter().map(|key|found.get(key)).collect();
                if encoded_len(&borrowed)?>request.max_bytes{return Err(QueueError::TooLarge);}
                let _result_headers=_owned_job.buffer(record_headers(request.keys.len(),0)?.checked_add(borrowed.iter().filter_map(|record|*record).try_fold(0u64,|n,record|n.checked_add((record.fields.len() as u64).checked_mul(256).ok_or(QueueError::TooLarge)?).ok_or(QueueError::TooLarge))?).ok_or(QueueError::TooLarge)?)?;
                let out:Vec<_>=borrowed.into_iter().map(|record|record.cloned()).collect();
                if encoded_len(&out)?>request.max_bytes{return Err(QueueError::TooLarge);}Ok(out)
            })).await
        })
    }
    fn read_commit(&self, request: ReceiptReadRequest) -> QueueFuture<'_, Vec<ReceiptLookup>> {
        Box::pin(async move {
            request.validate(&self.inner.limits)?;
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(request.max_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;
                let ids:Vec<Vec<u8>>=request.request_ids.iter().map(|s|s.as_bytes().to_vec()).collect();let hashes:Vec<_>=ids.iter().map(|s|hash(s)).collect();
                let size=tx.query_one("SELECT COALESCE(sum(octet_length(outcome)),0)::bigint FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=2 AND identity_hash=ANY($2) AND identity=ANY($3) AND NOT expired",&[&queue.id,&hashes,&ids]).await.map_err(storage)?.get::<_,i64>(0);
                if size as u64>request.max_bytes{return Err(QueueError::TooLarge);}
                let rows=tx.query("SELECT identity,expired,CASE WHEN expired THEN NULL ELSE outcome END FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=2 AND identity_hash=ANY($2) AND identity=ANY($3)",&[&queue.id,&hashes,&ids]).await.map_err(storage)?;
                let mut receipt_charges=vec![];
                let mut found=BTreeMap::new();for row in rows {let id=key(row.get(0))?;let outcome=if row.get::<_,bool>(1){ReceiptLookup::Expired}else{let raw=row.get::<_,Vec<u8>>(2);receipt_charges.push(_owned_job.buffer(CommitReceipt::stored_headers(&raw)?)?);ReceiptLookup::Found(decode(&raw)?)};
                    if found.insert(id,outcome).is_some(){return Err(storage("duplicate receipt identity"));}}
                let absent=ReceiptLookup::Absent;let borrowed:Vec<_>=request.request_ids.iter().map(|id|found.get(id).unwrap_or(&absent)).collect();
                if encoded_len(&borrowed)?>request.max_bytes{return Err(QueueError::TooLarge);}
                let headers=borrowed.iter().try_fold(0u64,|n,lookup|{let bytes=match lookup {ReceiptLookup::Found(receipt)=>receipt_headers(receipt)?,_=>256};n.checked_add(bytes).ok_or(QueueError::TooLarge)})?;
                let _result_headers=_owned_job.buffer(headers)?;
                let out:Vec<_>=borrowed.into_iter().cloned().collect();
                if encoded_len(&out)?>request.max_bytes{return Err(QueueError::TooLarge);}Ok(out)
            })).await
        })
    }
    fn shutdown(&self) -> QueueFuture<'_, ()> {
        Box::pin(async move {
            self.inner.admission.close()?;
            loop {
                let notified = self.inner.done.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.inner.admission.usage()?.0 == 0 {
                    break;
                }
                notified.await;
            }
            for client in &self.inner.clients {
                client.lock().await.take();
            }
            let drivers = std::mem::take(&mut *self.inner.drivers.lock().await);
            for driver in drivers {
                driver.await.map_err(storage)?.map_err(storage)?;
            }
            Ok(())
        })
    }
    fn stats(&self, request: StatsRequest) -> QueueFuture<'_, QueueStats> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;request.validate(&queue.schema,&limits)?;let now=timestamp_nanos(clock.now())?;
                let mut params:Parameters=vec![Box::new(queue.id),Box::new(now),Box::new(request.eligible_at)];
                let filter=filter_sql(&request.filter,&queue.schema.work,&mut params)?;
                let effective="CASE WHEN r.lifecycle=1 AND r.lease_until<=$2 THEN 0 ELSE r.lifecycle END";
                let lifecycle_filter=if let Some(state)=request.filter.lifecycle {let p=parameter(&mut params,life(state));format!(" AND ({effective})={p}")}else{String::new()};
                let aggregate=format!("count(*) FILTER(WHERE ({effective})=0),count(*) FILTER(WHERE ({effective})=1),count(*) FILTER(WHERE ({effective})=2),count(*) FILTER(WHERE ({effective})=3),min(r.eligible_since) FILTER(WHERE ({effective})=0 AND r.available_at<=$3)");
                let base=format!("r.queue_id=$1 AND r.collection=0 AND r.active{filter}{lifecycle_filter}");
                let total=tx.query_one(&format!("SELECT {aggregate} FROM fireweed_api007.records r WHERE {base}"),&refs(&params)).await.map_err(storage)?;
                let totals=counts(&total,0)?;let mut groups=vec![];
                if !request.group_by.is_empty(){
                    let mut joins=String::new();let mut columns=vec![];
                    for (n,name) in request.group_by.iter().enumerate(){let p=parameter(&mut params,field_no(&queue.schema.work,name)?);
                        joins.push_str(&format!(" LEFT JOIN fireweed_api007.fields g{n} ON g{n}.record_id=r.id AND g{n}.field={p}"));columns.push(format!("g{n}.full_key"));}
                    let joined=columns.join(",");let ordering=columns.iter().map(|col|format!("{col} ASC NULLS FIRST")).collect::<Vec<_>>().join(",");let lp=parameter(&mut params,i64::from(limits.max_records)+1);
                    let sql=format!("SELECT min(r.id),{aggregate} FROM fireweed_api007.records r{joins} WHERE {base} GROUP BY {joined} ORDER BY {ordering} LIMIT {lp}");
                    let rows=tx.query(&sql,&refs(&params)).await.map_err(storage)?;
                    if rows.len()>limits.max_records as usize{return Err(QueueError::TooLarge);}
                    let ids:Vec<i64>=rows.iter().map(|row|row.get(0)).collect();
                    // Load only bounded representative grouping values, never queue bodies.
                    let nos:Vec<i32>=request.group_by.iter().map(|name|field_no(&queue.schema.work,name)).collect::<QueueResult<_>>()?;
                    let bytes=tx.query_one("SELECT COALESCE(sum(octet_length(value)),0)::bigint FROM fireweed_api007.fields WHERE record_id=ANY($1) AND field=ANY($2)",&[&ids,&nos]).await.map_err(storage)?.get::<_,i64>(0);
                    if u64::try_from(bytes).map_err(storage)?>limits.max_response_bytes{return Err(QueueError::TooLarge);}
                    let _headers=_owned_job.buffer(record_headers(ids.len(),nos.len())?)?;
                    let values=tx.query("SELECT record_id,field,value FROM fireweed_api007.fields WHERE record_id=ANY($1) AND field=ANY($2)",&[&ids,&nos]).await.map_err(storage)?;
                    let mut fields:BTreeMap<(i64,i32),FieldValue>=BTreeMap::new();for row in values {fields.insert((row.get(0),row.get(1)),decode(&row.get::<_,Vec<u8>>(2))?);}
                    for row in rows {let id:i64=row.get(0);let values=nos.iter().map(|no|fields.get(&(id,*no)).cloned()).collect();groups.push(StatsGroup {values,counts:counts(&row,1)?});}

                }
                let result=QueueStats {totals,groups};if encoded_len(&result)?>limits.max_response_bytes{return Err(QueueError::TooLarge);}Ok(result)
            })).await
        })
    }
    fn query(&self, request: QueryRequest) -> QueueFuture<'_, QueryPage> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(request.max_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;request.validate(&queue.schema,&limits)?;let now=timestamp_nanos(clock.now())?;
                let name=match &request.source {RecordSource::Work=>None,RecordSource::State(name)=>Some(name.as_str())};let col=collection(&queue.schema,name)?;let def=definition(&queue.schema,col)?;
                let scope=request.cursor_fingerprint(&queue.schema)?;
                let after=if let Some(raw)=&request.cursor {Some(read_cursor(raw,&queue.secret,scope)?)}else{None};
                let mut params:Parameters=vec![Box::new(queue.id),Box::new(col)];
                let mut filter=filter_sql(&request.filter,def,&mut params)?;
                if let Some(state)=request.filter.lifecycle {let p=parameter(&mut params,life(state));let np=parameter(&mut params,now);filter.push_str(&format!(" AND (CASE WHEN r.lifecycle=1 AND r.lease_until<={np} THEN 0 ELSE r.lifecycle END)={p}"));}
                let (join,sort)=if request.index=="$key"{(String::new(),"r.key")}else{let p=parameter(&mut params,index_no(def,&request.index)?);(format!(" JOIN fireweed_api007.indexes i ON i.record_id=r.id AND i.index_no={p}"),"i.full_key")};
                if let Some(after)=&after {let sp=parameter(&mut params,after.sort.clone());let kp=parameter(&mut params,after.key.clone());let op=if request.descending{"<"}else{">"};filter.push_str(&format!(" AND ({sort},r.key){op}({sp},{kp})"));}
                let direction=if request.descending{"DESC"}else{"ASC"};let lp=parameter(&mut params,i64::from(request.max_records)+1);
                // Candidate selection returns sizes, not unbounded keys/index values.
                let sql=format!("SELECT r.id,octet_length(r.key),octet_length({sort}) FROM fireweed_api007.records r{join} WHERE r.queue_id=$1 AND r.collection=$2 AND r.active{filter} ORDER BY {sort} {direction},r.key {direction} LIMIT {lp}");
                let mut candidates=tx.query(&sql,&refs(&params)).await.map_err(storage)?;let full=candidates.len()>request.max_records as usize;candidates.truncate(request.max_records as usize);
                let ids:Vec<i64>=candidates.iter().map(|r|r.get(0)).collect();let sizes=payload_sizes(&tx,&ids,None,true,def).await?;let mut raw_bytes=0u64;let mut fitting=vec![];
                for row in &candidates {let id:i64=row.get(0);let n=sizes.get(&id).copied().unwrap_or(0).checked_add(u64::try_from(row.get::<_,i32>(1)).map_err(storage)?).and_then(|n|n.checked_add(name.map_or(0,|name|name.len() as u64))).ok_or(QueueError::TooLarge)?;
                    if raw_bytes.checked_add(n).is_none_or(|n|n>request.max_bytes){break;}raw_bytes+=n;fitting.push(id);}
                if !candidates.is_empty()&&fitting.is_empty(){return Err(QueueError::TooLarge);}let mut more=full||fitting.len()<candidates.len();
                let hydration_join=if request.index=="$key" {String::new()}else{" JOIN fireweed_api007.indexes i ON i.record_id=r.id AND i.index_no=$3".into()};
                let sql=format!("SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE r.id=ANY($1) AND r.queue_id=$2 AND r.active");
                let hydration_params:Parameters=vec![Box::new(fitting.clone()),Box::new(queue.id)];
                let rows=tx.query(&sql,&refs(&hydration_params)).await.map_err(storage)?;let mut by_id:BTreeMap<i64,Row>=rows.into_iter().map(|row|(row.get(0),row)).collect();
                let rows:Vec<Row>=fitting.iter().map(|id|by_id.remove(id).ok_or_else(||storage("missing selected query record"))).collect::<QueueResult<_>>()?;
                let _headers=_owned_job.buffer(materialized_headers(&tx,&fitting,None).await?)?;
                if let Some(name)=name && (name.len() as u64).checked_mul(rows.len() as u64).is_none_or(|n|n>request.max_bytes){return Err(QueueError::TooLarge);}
                let records:Vec<QueryRecord>=if col==0 {hydrate_work(&tx,&rows,&queue.schema,None,true,now).await?.into_iter().map(|work|QueryRecord::Work(work.record)).collect()}
                    else {let ids:Vec<i64>=rows.iter().map(|r|r.get(0)).collect();let cols=ids.iter().map(|id|(*id,col)).collect();let mut fields=load_fields(&tx,&ids,&queue.schema,&cols,None).await?;let mut bodies=load_bodies(&tx,&ids).await?;
                        rows.iter().map(|row|{let id:i64=row.get(0);Ok(QueryRecord::State(StateRecord {collection:name.ok_or_else(||storage("missing state name"))?.into(),key:key(row.get(2))?,version:version(row.get(3))?,fields:fields.remove(&id).unwrap_or_default(),value:bodies.remove(&id).ok_or_else(||storage("missing state body"))?.into()}))}).collect::<QueueResult<_>>()?};
                let mut page=QueryPage {records,cursor:None};
                loop {
                    page.cursor=if more&&!page.records.is_empty(){
                        let position=page.records.len()-1;let candidate=&candidates[position];
                        let cursor_raw=u64::try_from(candidate.get::<_,i32>(1)).map_err(storage)?.checked_add(u64::try_from(candidate.get::<_,i32>(2)).map_err(storage)?).ok_or(QueueError::TooLarge)?;
                        if cursor_raw>request.max_bytes {page.records.pop();if page.records.is_empty(){return Err(QueueError::TooLarge);}continue;}
                        let mut cursor_params:Parameters=vec![Box::new(candidate.get::<_,i64>(0)),Box::new(queue.id)];if request.index!="$key"{cursor_params.push(Box::new(index_no(def,&request.index)?));}
                        let sql=format!("SELECT {sort},r.key FROM fireweed_api007.records r{hydration_join} WHERE r.id=$1 AND r.queue_id=$2");
                        let row=tx.query_one(&sql,&refs(&cursor_params)).await.map_err(storage)?;
                        Some(make_cursor(Cursor {scope,sort:row.get(0),key:row.get(1)},&queue.secret)?)
                    }else{None};
                    if encoded_len(&page)?<=request.max_bytes{break;}page.records.pop();more=true;
                    if page.records.is_empty(){return Err(QueueError::TooLarge);}
                }Ok(page)
            })).await
        })
    }
}
fn counts(row: &Row, offset: usize) -> QueueResult<Counts> {
    Ok(Counts {
        pending: u64::try_from(row.get::<_, i64>(offset)).map_err(storage)?,
        leased: u64::try_from(row.get::<_, i64>(offset + 1)).map_err(storage)?,
        completed: u64::try_from(row.get::<_, i64>(offset + 2)).map_err(storage)?,
        failed: u64::try_from(row.get::<_, i64>(offset + 3)).map_err(storage)?,
        oldest_eligible_since: row.get(offset + 4),
    })
}
async fn payload_sizes(
    tx: &Transaction<'_>,
    ids: &[i64],
    fields: Option<&[i32]>,
    body: bool,
    def: &CollectionDefinition,
) -> QueueResult<BTreeMap<i64, u64>> {
    let selected = fields.map(|fields| fields.to_vec());
    let names: Vec<i64> = def.fields.keys().map(|name| name.len() as i64).collect();
    let rows=tx.query("SELECT r.id,(COALESCE((SELECT sum(octet_length(f.value)+($4::bigint[])[f.field+1]) FROM fireweed_api007.fields f WHERE f.record_id=r.id AND ($2::int[] IS NULL OR f.field=ANY($2))),0)+CASE WHEN $3 THEN COALESCE((SELECT octet_length(b.value) FROM fireweed_api007.bodies b WHERE b.record_id=r.id),0) ELSE 0 END)::bigint FROM fireweed_api007.records r WHERE r.id=ANY($1)",&[&ids,&selected,&body,&names]).await.map_err(storage)?;
    rows.iter()
        .map(|row| {
            Ok((
                row.get(0),
                u64::try_from(row.get::<_, i64>(1)).map_err(storage)?,
            ))
        })
        .collect()
}
async fn payload_budget(
    tx: &Transaction<'_>,
    ids: &[i64],
    fields: Option<&[i32]>,
    body: bool,
    budget: u64,
    def: &CollectionDefinition,
) -> QueueResult<()> {
    let total = payload_sizes(tx, ids, fields, body, def)
        .await?
        .values()
        .try_fold(0u64, |n, size| {
            n.checked_add(*size).ok_or(QueueError::TooLarge)
        })?;
    if total > budget {
        Err(QueueError::TooLarge)
    } else {
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Cursor {
    scope: [u8; 32],
    sort: Vec<u8>,
    key: Vec<u8>,
}
fn mac(secret: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 64];
    if secret.len() > 64 {
        key[..32].copy_from_slice(&Sha256::digest(secret));
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }
    let mut inner = Sha256::new();
    inner.update(key.map(|b| b ^ 0x36));
    inner.update(message);
    let digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(key.map(|b| b ^ 0x5c));
    outer.update(digest);
    outer.finalize().into()
}
fn make_cursor(cursor: Cursor, secret: &[u8]) -> QueueResult<Vec<u8>> {
    let bytes = encode(&(1u8, cursor))?;
    encode(&(mac(secret, &bytes), bytes))
}
fn read_cursor(raw: &[u8], secret: &[u8], scope: [u8; 32]) -> QueueResult<Cursor> {
    let invalid = || QueueError::Invalid("invalid or mismatched query cursor".into());
    let (tag, bytes): ([u8; 32], Vec<u8>) = decode(raw).map_err(|_| invalid())?;
    let expected = mac(secret, &bytes);
    if tag.iter().zip(expected).fold(0u8, |d, (a, b)| d | (a ^ b)) != 0 {
        return Err(invalid());
    }
    let (version, cursor): (u8, Cursor) = decode(&bytes).map_err(|_| invalid())?;
    if version != 1 || cursor.scope != scope {
        return Err(invalid());
    }
    Ok(cursor)
}

fn selected_field_numbers(
    def: &CollectionDefinition,
    selected: Option<&BTreeSet<String>>,
) -> QueueResult<Option<Vec<i32>>> {
    selected
        .map(|keys| keys.iter().map(|key| field_no(def, key)).collect())
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_hmac_matches_standard_vector_and_rejects_scope_tampering() {
        let tag = mac(&[0x0b; 20], b"Hi There");
        let hex: String = tag.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        let cursor = make_cursor(
            Cursor {
                scope: [1; 32],
                sort: vec![0, 255],
                key: b"key".to_vec(),
            },
            b"secret",
        )
        .unwrap();
        assert!(read_cursor(&cursor, b"secret", [1; 32]).is_ok());
        assert!(matches!(
            read_cursor(&cursor, b"secret", [2; 32]),
            Err(QueueError::Invalid(_))
        ));
        let mut corrupt = cursor;
        corrupt[0] ^= 1;
        assert!(matches!(
            read_cursor(&corrupt, b"secret", [1; 32]),
            Err(QueueError::Invalid(_))
        ));
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn admitted_caller_cancellation_drains_and_reopens_complete_group() {
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let store = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 1)
            .await
            .unwrap();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let queue = QueueAddress {
            tenant: "api007-cancellation".into(),
            queue: nonce.to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: fireweed_conformance::queue_contract::schema(),
            })
            .await
            .unwrap();
        let (mut blocker, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let held = blocker.transaction().await.unwrap();
        let fixture_admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let fixture_job = fixture_admission.reserve(0).unwrap();
        queue_row(&held, &queue, &fixture_job).await.unwrap();
        let group = CommitGroup {
            request_id: "owned-commit".into(),
            claimed_updates: vec![],
            work_updates: vec![],
            state_updates: vec![StateUpdate {
                collection: "instances".into(),
                key: "instance".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Fields::new(),
                    value: vec![9].into(),
                },
            }],
            work_inserts: vec![NewWork {
                key: "next".into(),
                priority: 0,
                available_at: 0,
                fields: Fields::new(),
                body: vec![8].into(),
            }],
        };
        let owned = store.clone();
        let address = queue.clone();
        let caller = tokio::spawn(async move {
            owned
                .commit(CommitBatch {
                    queue: address,
                    groups: vec![group],
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while store.inner.admission.usage().unwrap().0 != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(store.inner.admission.usage().unwrap().0, 1);
        held.commit().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), store.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.inner.admission.usage().unwrap(), (0, 0));
        drop(blocker);
        driver.await.unwrap().unwrap();
        let reopened = PostgresQueueStore::connect(&url, clock, QueueLimits::default(), 1)
            .await
            .unwrap();
        let receipt = reopened
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec!["owned-commit".into()],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        assert!(matches!(
            &receipt[0],
            ReceiptLookup::Found(CommitReceipt {
                outcome: GroupOutcome::Committed { .. },
                ..
            })
        ));
        let state = reopened
            .read_state(StateReadRequest {
                queue: queue.clone(),
                collection: "instances".into(),
                keys: vec!["instance".into()],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[9]);
        let work = reopened
            .query(QueryRequest {
                queue,
                source: RecordSource::Work,
                index: "$key".into(),
                filter: Filter::default(),
                descending: false,
                cursor: None,
                max_records: 10,
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        assert_eq!(work.records.len(), 1);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn reopening_schema_fences_writers_before_dependent_tables() {
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let store = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 1)
            .await
            .unwrap();
        let (mut client, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(driver);
        let tx = client.transaction().await.unwrap();
        tx.batch_execute("LOCK TABLE fireweed_api007.queues IN ACCESS SHARE MODE; LOCK TABLE fireweed_api007.records IN ROW EXCLUSIVE MODE").await.unwrap();
        let opener = tokio::spawn(async move {
            PostgresQueueStore::connect(&url, clock, QueueLimits::default(), 1).await
        });
        // Wait until the opener has reached its schema lock barrier, without
        // guessing an interleaving from a wall-clock sleep.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiting: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='fireweed_api007.queues'::regclass AND NOT granted)", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tx.batch_execute("LOCK TABLE fireweed_api007.queues IN ROW EXCLUSIVE MODE"),
        )
        .await
        .unwrap()
        .unwrap();
        tx.commit().await.unwrap();
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), opener)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        reopened.shutdown().await.unwrap();
        store.shutdown().await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn invalid_envelope_precedes_saturated_admission() {
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let limits = QueueLimits {
            max_records: 4096,
            max_groups: 64,
            max_group_bytes: 8192,
            max_request_bytes: 16384,
            max_response_bytes: 8192,
            max_body_bytes: 1024,
            max_state_bytes: 2048,
        };
        let store = PostgresQueueStore::connect(&url, clock, limits, 1)
            .await
            .unwrap();
        let charges: Vec<_> = (0..64)
            .map(|_| store.inner.admission.reserve(0).unwrap())
            .collect();
        let mut request = QueryRequest {
            queue: QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            },
            source: RecordSource::Work,
            index: "$key".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 0,
        };
        assert!(matches!(
            store.query(request.clone()).await,
            Err(QueueError::Invalid(_))
        ));
        request.max_bytes = 1024;
        request.cursor = Some(vec![0; 16385]);
        assert_eq!(
            store.query(request.clone()).await,
            Err(QueueError::TooLarge)
        );
        request.cursor = None;
        assert_eq!(store.query(request).await, Err(QueueError::Busy));
        drop(charges);
        store.shutdown().await.unwrap();
    }
}

async fn materialized_headers(
    tx: &Transaction<'_>,
    ids: &[i64],
    selected: Option<&[i32]>,
) -> QueueResult<u64> {
    let selected = selected.map(|nos| nos.to_vec());
    let fields=u64::try_from(tx.query_one("SELECT count(*) FROM fireweed_api007.fields WHERE record_id=ANY($1) AND ($2::int[] IS NULL OR field=ANY($2))",&[&ids,&selected]).await.map_err(storage)?.get::<_,i64>(0)).map_err(storage)?;
    fields
        .checked_mul(256)
        .and_then(|n| n.checked_add((ids.len() as u64).checked_mul(1024)?))
        .ok_or(QueueError::TooLarge)
}

impl PersistedStructure for ClaimReplay {
    fn stored_headers(raw: &[u8]) -> QueueResult<u64> {
        Ok(decode::<MeasuredClaimReplay>(raw)?.items.headers)
    }
}

#[derive(Deserialize)]
struct MeasuredClaimPair(i64, MeasuredBytes);
impl MeasuredValue for MeasuredClaimPair {
    fn headers(&self) -> QueueResult<u64> {
        let _ = (self.0, &self.1);
        Ok(256)
    }
}
#[derive(Deserialize)]
struct MeasuredClaimReplay {
    items: MeasuredList<MeasuredClaimPair>,
}
