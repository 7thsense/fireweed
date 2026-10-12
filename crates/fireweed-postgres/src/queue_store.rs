//! Native asynchronous PostgreSQL baseline for API-007. Complete operation jobs
//! retain admission and connection ownership after caller cancellation.
use fireweed_core::queue_contract::*;
use fireweed_engine::queue_body::ResolvedBody;
use fireweed_engine::queue_claim::{
    ClaimContext, ClaimOwnership, ClaimResponseBudget, checked_lease_hash, claim_expiry, plan_claim,
};
#[cfg(test)]
use fireweed_engine::queue_cursor::cursor_mac as mac;
use fireweed_engine::queue_cursor::{QueryCursor as Cursor, make_cursor, read_cursor};
use fireweed_engine::queue_fields::{CheckedFields, FieldEdits, IndexEffect, ResolvedFields};
use fireweed_engine::queue_memory::*;
use fireweed_engine::queue_schema_codec::{
    DefinitionPin, decode_definition, definition_headers, encode_definition,
};
use fireweed_engine::queue_storage_layout::{
    collection_definition as definition, collection_number as collection, decode,
    decode_lifecycle as lifecycle, discovery_digest, discovery_prefix as prefix, encode,
    encode_lifecycle as life, field_number as field_no, index_number as index_no,
};
use fireweed_engine::{
    Clock,
    queue_admission::{QueueAdmission, QueueBufferCharge, QueueReservation},
    queue_contract::{QueueFuture, QueueStore},
    queue_index::{ordered_key, scalar_type_prefix},
    queue_planner::{
        CommitDelta, RecordIdentity, RecordSnapshot, StoredWork, UniqueIdentity, combine_deltas,
        insert_unique_probes, lease_hash, plan_commit_group, plan_insert_vector,
        prospective_unique_probes, required_unique_probes, required_work_fields,
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
CREATE TABLE IF NOT EXISTS fireweed_api007.additional_fields (
 id BIGSERIAL PRIMARY KEY, record_id BIGINT NOT NULL REFERENCES fireweed_api007.records(id),
 queue_id BIGINT NOT NULL, collection INT NOT NULL, name_hash BYTEA NOT NULL, name BYTEA NOT NULL,
 prefix BYTEA NOT NULL, hash BYTEA NOT NULL, full_key BYTEA NOT NULL, value BYTEA NOT NULL);
CREATE INDEX IF NOT EXISTS additional_name_probe ON fireweed_api007.additional_fields(record_id,name_hash);
CREATE INDEX IF NOT EXISTS additional_equal_probe ON fireweed_api007.additional_fields(queue_id,collection,name_hash,hash,record_id);
CREATE INDEX IF NOT EXISTS additional_range_probe ON fireweed_api007.additional_fields(queue_id,collection,name_hash,prefix,record_id);
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
const RECORD_COLUMNS: &str = "r.id,r.collection,r.key,r.version::text,r.active,r.lifecycle,r.priority,r.available_at,r.eligible_since,r.attempts,r.lease_until,CASE WHEN r.lease_hash IS NULL OR octet_length(r.lease_hash)=32 THEN r.lease_hash ELSE ''::bytea END,r.insertion_sequence::text";
fn storage(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
#[cfg(test)]
thread_local! {
    // Only discovery hashes collide. Semantic fingerprints, lease hashes and
    // cursor HMAC retain their normal algorithms. Current-thread fixture jobs
    // run on this same test thread; other tests have independent hook state.
    static COLLIDING_PROBES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
fn probe_hash(bytes: &[u8]) -> Vec<u8> {
    #[cfg(test)]
    if COLLIDING_PROBES.get() {
        return vec![0; 32];
    }
    discovery_digest(bytes).to_vec()
}
fn version(raw: String) -> QueueResult<u64> {
    raw.parse().map_err(storage)
}
fn key(raw: Vec<u8>) -> QueueResult<String> {
    String::from_utf8(raw).map_err(storage)
}

struct Inner {
    clients: Vec<Mutex<Option<Client>>>,
    drivers: Mutex<Vec<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>>,
    admission: Arc<QueueAdmission>,
    slots: Arc<Semaphore>,
    next: AtomicUsize,
    done: Arc<Notify>,
    clock: Arc<dyn Clock>,
    limits: QueueLimits,
    scratch_workers: fireweed_queue_spool::ScratchWorkers,
    scratch_quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
}
/// Uses real SQL transactions and indexed selection, not the old blocking
/// PostgreSQL adapter or an in-memory projection. The concrete consumer facade
/// constructs this mandatory native baseline; S3/Turso qualification is separate.
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
        Self::connect_with_runtime(
            url,
            clock,
            limits,
            pool_size,
            fireweed_core::queue_runtime::QueueRuntimeOptions::default(),
        )
        .await
    }
    pub async fn connect_with_runtime(
        url: &str,
        clock: Arc<dyn Clock>,
        limits: QueueLimits,
        pool_size: usize,
        options: fireweed_core::queue_runtime::QueueRuntimeOptions,
    ) -> QueueResult<Self> {
        let runtime = fireweed_engine::queue_runtime::ValidatedQueueRuntime::with_options(
            &limits, options, pool_size,
        )?;
        if !(1..=32).contains(&pool_size) {
            return Err(QueueError::Invalid("pool size must be 1..32".into()));
        }
        if url.trim().is_empty() {
            return Err(QueueError::Invalid("PostgreSQL URL is required".into()));
        }
        let config: tokio_postgres::Config = url
            .parse()
            .map_err(|_| QueueError::Invalid("invalid PostgreSQL URL".into()))?;
        // Every fallible resource/configuration check precedes TLS, DNS, socket
        // creation and schema DDL. No late arithmetic can fail after publication.
        let admission = QueueAdmission::new(runtime.operations(), runtime.retained_bytes())?;
        let scratch_quota = fireweed_queue_spool::ScratchDiskQuota::new(runtime.scratch_bytes())?;
        let scratch_workers = fireweed_queue_spool::ScratchWorkers::new(
            runtime.scratch_workers(),
            runtime.operations(),
        )?;
        let mut clients = vec![];
        let mut drivers = vec![];
        for connection_index in 0..pool_size {
            #[cfg(not(feature = "tls"))]
            let (mut client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|_| storage("PostgreSQL connection setup failed"))?;
            #[cfg(feature = "tls")]
            let (mut client, connection) = {
                let tls = native_tls::TlsConnector::new()
                    .map_err(|_| storage("PostgreSQL connection setup failed"))?;
                config
                    .connect(postgres_native_tls::MakeTlsConnector::new(tls))
                    .await
                    .map_err(|_| storage("PostgreSQL connection setup failed"))?
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
        Ok(Self {
            inner: Arc::new(Inner {
                clients,
                drivers: Mutex::new(drivers),
                admission,
                slots: Arc::new(Semaphore::new(pool_size)),
                next: AtomicUsize::new(0),
                done: Arc::new(Notify::new()),
                clock,
                limits,
                scratch_workers,
                scratch_quota,
            }),
        })
    }
    async fn dispatch<T, F>(&self, bytes: u64, job: F) -> QueueResult<T>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut Client, QueueReservation) -> QueueFuture<'a, T> + Send + 'static,
    {
        let done = Arc::clone(&self.inner.done);
        let reservation = self
            .inner
            .admission
            .reserve(bytes)?
            .with_terminal_callback(move || done.notify_waiters())?;
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            let permit = Arc::clone(&inner.slots)
                .acquire_owned()
                .await
                .map_err(storage)?;
            let start = inner.next.fetch_add(1, Ordering::Relaxed) % inner.clients.len();
            let mut selected = None;
            for offset in 0..inner.clients.len() {
                if let Ok(guard) = inner.clients[(start + offset) % inner.clients.len()].try_lock()
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
        });
        task.await.map_err(storage)?
    }
}
struct QueueRow<'a> {
    id: i64,
    schema: QueueSchema,
    definition_pin: DefinitionPin,
    next_sequence: u64,
    secret: Vec<u8>,
    // Retain schema admission until all owned payloads have dropped.
    _schema_charge: QueueBufferCharge<'a>,
    _schema_headers: QueueBufferCharge<'a>,
}
async fn queue_row<'a>(
    tx: &Transaction<'_>,
    address: &QueueAddress,
    reservation: &'a QueueReservation,
) -> QueueResult<QueueRow<'a>> {
    let address_hash = probe_hash(&encode(address)?);
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
    let headers = reservation.buffer(definition_headers(&definition)?)?;
    Ok(QueueRow {
        _schema_charge: charge,
        _schema_headers: headers,
        id,
        schema: decode_definition(&definition)?,
        definition_pin: DefinitionPin::from_persisted_bytes(&definition),
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
    // The queue fence prevents receipt changes between metadata admission and fetch.
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&queue, &domain, &probe_hash(id.as_bytes()), &id.as_bytes()];
    let rows = tx.query("SELECT fingerprint,expired,octet_length(outcome)::bigint FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=$2 AND identity_hash=$3 AND identity=$4", params).await.map_err(storage)?;
    if rows.len() > 1 {
        return Err(storage("duplicate receipt authority"));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if row.get::<_, Vec<u8>>(0) != fp {
        return Err(QueueError::RequestConflict);
    }
    if row.get::<_, bool>(1) {
        return Err(QueueError::RequestExpired);
    }
    let size = u64::try_from(row.get::<_, i64>(2)).map_err(storage)?;
    charges.push(reservation.buffer(size.checked_mul(4).ok_or(QueueError::TooLarge)?)?);
    let row = tx.query_one("SELECT outcome FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=$2 AND identity_hash=$3 AND identity=$4", params).await.map_err(storage)?;
    let raw = row.get::<_, Vec<u8>>(0);
    if u64::try_from(raw.len()).map_err(storage)? != size {
        return Err(storage("receipt length changed under queue fence"));
    }
    charges.push(reservation.buffer(T::stored_headers(&raw)?)?);
    let value: T = decode(&raw)?;
    value.validate_persisted()?;
    Ok(Some(value))
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
        &[&queue,&domain,&probe_hash(id.as_bytes()),&id.as_bytes(),&fp,&encode(value)?]).await.map_err(storage)?;
    Ok(())
}
fn selected_ordinals(
    ids: &[i64],
    schema: &QueueSchema,
    cols: &BTreeMap<i64, i32>,
    selected: Option<&BTreeSet<String>>,
) -> QueueResult<Option<Vec<i32>>> {
    let Some(wanted) = selected else {
        return Ok(None);
    };
    let partitions = ids
        .iter()
        .map(|id| {
            cols.get(id)
                .copied()
                .ok_or_else(|| storage("missing field partition authority"))
        })
        .collect::<QueueResult<BTreeSet<_>>>()?;
    let mut ordinals = BTreeSet::new();
    for col in partitions {
        let def = definition(schema, col)?;
        ordinals.extend(wanted.iter().filter_map(|name| field_no(def, name).ok()));
    }
    Ok(Some(ordinals.into_iter().collect()))
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
    let ordinals = selected_ordinals(ids, schema, cols, selected)?;
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
    let names = selected.map(|names| {
        names
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect::<Vec<_>>()
    });
    let hashes = names.as_ref().map(|names| {
        names
            .iter()
            .map(|name| probe_hash(name))
            .collect::<Vec<_>>()
    });
    let extra_rows=client.query("SELECT record_id,collection,name,value FROM fireweed_api007.additional_fields WHERE record_id=ANY($1) AND ($2::bytea[] IS NULL OR (name_hash=ANY($2) AND name=ANY($3)))",&[&ids,&hashes,&names]).await.map_err(storage)?;
    for row in extra_rows {
        let id: i64 = row.get(0);
        let col = *cols
            .get(&id)
            .ok_or_else(|| storage("foreign additional field record"))?;
        if row.get::<_, i32>(1) != col {
            return Err(storage("additional field collection mismatch"));
        }
        let name = key(row.get(2))?;
        let def = definition(schema, col)?;
        if def.fields.contains_key(&name) || !def.admits_field(&name) {
            return Err(storage("invalid additional field namespace"));
        }
        let value: FieldValue = decode(&row.get::<_, Vec<u8>>(3))?;
        if matches!(&value,FieldValue::Float(n) if !n.is_finite()) {
            return Err(storage("invalid additional field value"));
        }
        if out.entry(id).or_default().insert(name, value).is_some() {
            return Err(storage("duplicate additional field authority"));
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
    let stored = StoredWork {
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
        field_coverage: fireweed_engine::queue_fields::FieldCoverage::Complete,
        body_loaded: true,
    };
    fireweed_engine::queue_planner::validate_raw_work_lease(&stored)?;
    Ok(stored)
}
struct Loaded<'a> {
    // Drop owned payloads before their admission guards on every error path.
    snapshot: RecordSnapshot,
    ids: BTreeMap<RecordIdentity, i64>,
    _charge: QueueBufferCharge<'a>,
    _selection_charge: Option<QueueBufferCharge<'a>>,
    _probe_charges: Vec<QueueBufferCharge<'a>>,
}
/// Transfer addressed authority to owned scratch work without copying any payload
/// or releasing/reacquiring its admitted bytes. It remains input-bounded.
struct OwnedLoaded {
    snapshot: RecordSnapshot,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "physical IDs retained for forthcoming resolved application"
        )
    )]
    ids: BTreeMap<RecordIdentity, i64>,
    _charge: fireweed_engine::queue_admission::OwnedQueueBufferCharge,
}
impl Loaded<'_> {
    fn into_owned(self, reservation: &Arc<QueueReservation>) -> QueueResult<OwnedLoaded> {
        if !self._charge.belongs_to(reservation)
            || self
                ._selection_charge
                .as_ref()
                .is_some_and(|c| !c.belongs_to(reservation))
            || self
                ._probe_charges
                .iter()
                .any(|c| !c.belongs_to(reservation))
        {
            return Err(storage("cannot transfer loaded authority to another job"));
        }
        // Same-job charges are disjoint portions of the admitted u64 total;
        // their sum cannot overflow. Transfer preserves payload-before-guard Drop.
        let Self {
            snapshot,
            ids,
            mut _charge,
            _selection_charge,
            _probe_charges,
        } = self;
        if let Some(charge) = _selection_charge {
            _charge = _charge.merge(charge)?;
        }
        for charge in _probe_charges {
            _charge = _charge.merge(charge)?;
        }
        Ok(OwnedLoaded {
            snapshot,
            ids,
            _charge: _charge.into_owned(Arc::clone(reservation))?,
        })
    }
}
async fn load_group<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    group: &CommitGroup,
    reservation: &'a QueueReservation,
) -> QueueResult<Loaded<'a>> {
    let wanted = required_work_fields(group, &queue.schema)?;
    load_group_with_fields(tx, queue, group, reservation, Some(&wanted)).await
}
async fn load_group_with_fields<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    group: &CommitGroup,
    reservation: &'a QueueReservation,
    selected_work: Option<&BTreeSet<String>>,
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
        let hashes: Vec<Vec<u8>> = keys.iter().map(|key| probe_hash(key)).collect();
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE queue_id=$1 AND collection=$2 AND key_hash=ANY($3) AND key=ANY($4)"
        );
        rows.extend(
            tx.query(&sql, &[&queue.id, &col, &hashes, &keys])
                .await
                .map_err(storage)?,
        );
    }
    // Reserve certificate multiplicity AND SQL parameter scratch before cloning
    // any requested names. Absent names still occupy every selected certificate.
    let selection_charge = if let Some(names) = selected_work {
        let name_bytes = names.iter().try_fold(64u64, |n, name| {
            n.checked_add(name.len() as u64)
                .and_then(|n| n.checked_add(256))
                .ok_or(QueueError::TooLarge)
        })?;
        let count = rows
            .iter()
            .filter(|row| row.get::<_, i32>(1) == 0 && row.get::<_, bool>(4))
            .count() as u64;
        let retained = name_bytes.checked_mul(count).ok_or(QueueError::TooLarge)?;
        let scratch = name_bytes.checked_add(4096).ok_or(QueueError::TooLarge)?;
        Some(
            reservation.buffer(
                retained
                    .checked_add(scratch)
                    .and_then(|n| n.checked_mul(4))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        )
    } else {
        None
    };
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
    let cols: BTreeMap<i64, i32> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    let mut names_and_headers = record_headers(rows.len(), 0)?;
    let mut partitions = Vec::new();
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
        let selected = if col == 0 { selected_work } else { None };
        let ordinals: Option<Vec<i32>> = selected.map(|names| {
            names
                .iter()
                .filter_map(|name| field_no(def, name).ok())
                .collect()
        });
        let names: Vec<i64> = def.fields.keys().map(|name| name.len() as i64).collect();
        let size = tx.query_one("SELECT COALESCE(sum(octet_length(value)+octet_length(full_key)+COALESCE(($2::bigint[])[field+1],0)+256),0)::bigint FROM fireweed_api007.fields WHERE record_id=ANY($1) AND ($3::int[] IS NULL OR field=ANY($3))", &[&col_ids, &names, &ordinals]).await.map_err(storage)?.get::<_,i64>(0);
        let extra_names: Option<Vec<Vec<u8>>> =
            selected.map(|names| names.iter().map(|name| name.as_bytes().to_vec()).collect());
        let extra_hashes: Option<Vec<Vec<u8>>> = extra_names
            .as_ref()
            .map(|names| names.iter().map(|name| probe_hash(name)).collect());
        let extra_size = tx.query_one("SELECT COALESCE(sum(octet_length(name)+octet_length(value)+octet_length(full_key)+256),0)::bigint FROM fireweed_api007.additional_fields WHERE record_id=ANY($1) AND ($2::bytea[] IS NULL OR (name_hash=ANY($2) AND name=ANY($3)))", &[&col_ids, &extra_hashes, &extra_names]).await.map_err(storage)?.get::<_,i64>(0);
        names_and_headers = names_and_headers
            .checked_add(u64::try_from(size).map_err(storage)?)
            .and_then(|n| n.checked_add(u64::try_from(extra_size).ok()?))
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
        partitions.push((col, col_ids));
    }
    let retained = body_size
        .checked_add(names_and_headers)
        .and_then(|n| n.checked_mul(4))
        .ok_or(QueueError::TooLarge)?;
    let charge = reservation.buffer(retained)?;
    let mut fields = BTreeMap::new();
    for (col, ids) in partitions {
        fields.extend(
            load_fields(
                tx,
                &ids,
                &queue.schema,
                &cols,
                if col == 0 { selected_work } else { None },
            )
            .await?,
        );
    }
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
            if let Some(names) = selected_work {
                stored.field_coverage = fireweed_engine::queue_fields::FieldCoverage::Selected {
                    names: names.clone(),
                    prefixes: BTreeSet::new(),
                };
            }
            snapshot.work.insert(record_key, stored);
        }
    }
    Ok(Loaded {
        _charge: charge,
        _selection_charge: selection_charge,
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
async fn load_prospective_unique_probes<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    loaded: &mut Loaded<'a>,
    group: &CommitGroup,
    reservation: &'a QueueReservation,
) -> QueueResult<()> {
    let charge = reservation.buffer(group_expansion_budget(
        group,
        &loaded.snapshot,
        &queue.schema,
    )?)?;
    loaded._probe_charges.push(charge);
    let probes = prospective_unique_probes(group, &loaded.snapshot, &queue.schema)?
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
    let mut fresh = BTreeSet::new();
    if probes.iter().any(|probe| {
        loaded.snapshot.unique_probes_loaded.contains(probe)
            || loaded.snapshot.unique_owners.contains_key(probe)
            || !fresh.insert(probe)
    }) {
        return Err(storage(
            "unique authority load requires fresh distinct tuples",
        ));
    }
    // All accepted releases/claims require retained probe coverage. Therefore a
    // fresh tuple cannot have changed in a prior accepted speculative group.
    let mut columns = vec![];
    let mut numbers = vec![];
    let mut hashes = vec![];
    let mut full = vec![];
    for probe in &probes {
        let col = collection(&queue.schema, probe.collection.as_deref())?;
        columns.push(col);
        numbers.push(index_no(definition(&queue.schema, col)?, &probe.index)?);
        hashes.push(probe_hash(&probe.full_sort_key));
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
        loaded.snapshot.unique_owners.insert(probe.clone(), owner);
    }
    loaded.snapshot.unique_probes_loaded.extend(probes);
    Ok(())
}

/// Resolve only accepted changed identities after planning, including deletes and
/// retained tombstones. A selected row's transient hydration ID must not become
/// an unbounded retained scan list. The caller keeps the same queue fence through
/// this read and publication. This function never allocates database sequences.
async fn load_delta_ids<'a>(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    delta: &CommitDelta,
    before: &RecordSnapshot,
    reservation: &'a QueueReservation,
) -> QueueResult<Loaded<'a>> {
    let bytes = delta.versions.iter().try_fold(4096u64, |bytes, version| {
        let identity = (version.key.len() as u64)
            .checked_add(
                version
                    .collection
                    .as_ref()
                    .map_or(0, |name| name.len() as u64),
            )
            .and_then(|bytes| bytes.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| storage("changed identity allocation overflow"))?;
        bytes
            .checked_add(identity)
            .ok_or_else(|| storage("changed identity allocation overflow"))
    })?;
    let charge = reservation.buffer(bytes)?;
    let mut ids = BTreeMap::new();
    let mut distinct = BTreeSet::new();
    for chunk in delta.versions.chunks(512) {
        let mut columns = Vec::with_capacity(chunk.len());
        let mut hashes = Vec::with_capacity(chunk.len());
        let mut keys = Vec::with_capacity(chunk.len());
        for version in chunk {
            let identity = RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            };
            if !distinct.insert(identity) {
                return Err(storage("duplicate changed identity"));
            }
            columns.push(collection(&queue.schema, version.collection.as_deref())?);
            hashes.push(probe_hash(version.key.as_bytes()));
            keys.push(version.key.as_bytes().to_vec());
        }
        let rows = tx.query(
            "SELECT p.ordinal,r.id,r.version::text,r.active FROM UNNEST($2::int[],$3::bytea[],$4::bytea[]) WITH ORDINALITY AS p(col,hash,key,ordinal) LEFT JOIN fireweed_api007.records r ON r.queue_id=$1 AND r.collection=p.col AND r.key_hash=p.hash AND r.key=p.key ORDER BY p.ordinal",
            &[&queue.id, &columns, &hashes, &keys],
        ).await.map_err(storage)?;
        if rows.len() != chunk.len() {
            return Err(storage("changed identity authority cardinality mismatch"));
        }
        for (index, row) in rows.into_iter().enumerate() {
            if row.get::<_, i64>(0) != index as i64 + 1 {
                return Err(storage("changed identity authority ordinal mismatch"));
            }
            let version = &chunk[index];
            let identity = RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            };
            let expected = before.versions.get(&identity).copied().unwrap_or(0);
            let present_version = match &identity.collection {
                None => before.work.get(&identity.key).map(|row| row.record.version),
                Some(collection) => before
                    .state
                    .get(&(collection.clone(), identity.key.clone()))
                    .map(|row| row.version),
            };
            if present_version.is_some_and(|version| version == 0 || version != expected) {
                return Err(storage("changed identity live authority mismatch"));
            }
            let id: Option<i64> = row.get(1);
            let stored: Option<String> = row.get(2);
            let active: Option<bool> = row.get(3);
            match (id, stored, active) {
                (Some(id), Some(stored), Some(active))
                    if expected != 0
                        && stored.parse::<u64>().map_err(storage)? == expected
                        && active == present_version.is_some() =>
                {
                    ids.insert(identity, id);
                }
                (None, None, None) if expected == 0 && present_version.is_none() => {}
                _ => return Err(storage("changed identity predecessor mismatch")),
            }
        }
    }
    Ok(Loaded {
        snapshot: RecordSnapshot::default(),
        ids,
        _charge: charge,
        _selection_charge: None,
        _probe_charges: Vec::new(),
    })
}

async fn persist_delta(
    tx: &Transaction<'_>,
    queue: &QueueRow<'_>,
    delta: &CommitDelta,
    before: &RecordSnapshot,
    loaded: &mut Loaded<'_>,
) -> QueueResult<()> {
    fireweed_engine::queue_planner::validate_resolved_writes(delta)?;
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
    let mut exact_edits: BTreeMap<i64, &FieldEdits> = BTreeMap::new();
    let mut exact_indexes: BTreeMap<i64, Vec<IndexEffect>> = BTreeMap::new();
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
        hashes.push(probe_hash(v.key.as_bytes()));
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
            // Read coverage permits a physical no-op optimization, but never
            // creates write intent. Explicit empty replacements remain writes.
            if let ResolvedBody::Replace(body) = &delta.work_bodies[&v.key]
                && old.is_none_or(|old| !old.body_loaded || old.record.work.body != *body)
            {
                payload_ids.push(id);
                payloads.push(body.to_vec());
            }
            match &delta.work_fields[&v.key] {
                ResolvedFields::Preserve => {}
                ResolvedFields::Replace(fields) => {
                    changed_fields.insert(id, (col, fields.clone()));
                }
                ResolvedFields::Edit(edits) => {
                    let old = old.ok_or_else(|| storage("partial field command cannot insert"))?;
                    let view = CheckedFields::new(&old.record.work.fields, &old.field_coverage)?;
                    exact_indexes.insert(
                        id,
                        fireweed_engine::queue_fields::resolve_index_effects(
                            &view,
                            &delta.work_fields[&v.key],
                            &queue.schema.work,
                        )?,
                    );
                    exact_edits.insert(id, edits);
                    changed_fields.insert(
                        id,
                        (
                            col,
                            edits
                                .iter()
                                .filter_map(|(name, value)| {
                                    value.as_ref().map(|v| (name.clone(), v.clone()))
                                })
                                .collect(),
                        ),
                    );
                }
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
        let changed: Vec<_> = changed_fields
            .keys()
            .filter(|id| !exact_edits.contains_key(id))
            .copied()
            .collect();
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
        tx.execute(
            "DELETE FROM fireweed_api007.additional_fields WHERE record_id=ANY($1)",
            &[&changed],
        )
        .await
        .map_err(storage)?;
        let mut edited_records = vec![];
        let mut edited_numbers = vec![];
        let mut edited_extra_records = vec![];
        let mut edited_names = vec![];
        let mut edited_name_hashes = vec![];
        let mut edited_index_records = vec![];
        let mut edited_index_numbers = vec![];
        for (id, edits) in &exact_edits {
            let (col, _) = &changed_fields[id];
            let def = definition(&queue.schema, *col)?;
            for name in edits.keys() {
                let no = field_no(def, name)?;
                if no >= 0 {
                    edited_records.push(*id);
                    edited_numbers.push(no);
                } else {
                    edited_extra_records.push(*id);
                    edited_names.push(name.as_bytes().to_vec());
                    edited_name_hashes.push(probe_hash(name.as_bytes()));
                }
            }
            for effect in &exact_indexes[id] {
                edited_index_records.push(*id);
                edited_index_numbers.push(index_no(def, &effect.index)?);
            }
        }
        if !edited_records.is_empty() {
            tx.execute("DELETE FROM fireweed_api007.fields f USING UNNEST($1::bigint[],$2::int[]) AS x(id,no) WHERE f.record_id=x.id AND f.field=x.no", &[&edited_records,&edited_numbers]).await.map_err(storage)?;
        }
        if !edited_extra_records.is_empty() {
            tx.execute("DELETE FROM fireweed_api007.additional_fields f USING UNNEST($1::bigint[],$2::bytea[],$3::bytea[]) AS x(id,hash,name) WHERE f.record_id=x.id AND f.name_hash=x.hash AND f.name=x.name", &[&edited_extra_records,&edited_name_hashes,&edited_names]).await.map_err(storage)?;
        }
        if !edited_index_records.is_empty() {
            tx.execute("DELETE FROM fireweed_api007.indexes i USING UNNEST($1::bigint[],$2::int[]) AS x(id,no) WHERE i.record_id=x.id AND i.index_no=x.no", &[&edited_index_records,&edited_index_numbers]).await.map_err(storage)?;
        }
        let mut extra_records = vec![];
        let mut extra_cols = vec![];
        let mut extra_names = vec![];
        let mut extra_name_hashes = vec![];
        let mut extra_prefixes = vec![];
        let mut extra_hashes = vec![];
        let mut extra_full = vec![];
        let mut extra_values = vec![];
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
                let no = field_no(def, name)?;
                if no >= 0 {
                    record_ids.push(*id);
                    col_ids.push(*col);
                    numbers.push(no);
                    prefixes.push(prefix(&sort));
                    value_hashes.push(probe_hash(&sort));
                    full.push(sort);
                    values.push(encode(value)?);
                } else {
                    extra_records.push(*id);
                    extra_cols.push(*col);
                    extra_names.push(name.as_bytes().to_vec());
                    extra_name_hashes.push(probe_hash(name.as_bytes()));
                    extra_prefixes.push(prefix(&sort));
                    extra_hashes.push(probe_hash(&sort));
                    extra_full.push(sort);
                    extra_values.push(encode(value)?);
                }
            }
            if let Some(effects) = exact_indexes.get(id) {
                for effect in effects {
                    let sort = effect
                        .after
                        .as_ref()
                        .ok_or_else(|| storage("existing edited index lacks physical after key"))?;
                    ix_records.push(*id);
                    ix_cols.push(*col);
                    ix_numbers.push(index_no(def, &effect.index)?);
                    ix_prefixes.push(prefix(sort));
                    ix_hashes.push(probe_hash(sort));
                    ix_full.push(sort.clone());
                    uniques.push(effect.after_unique);
                }
            } else {
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
                    ix_hashes.push(probe_hash(&sort));
                    ix_full.push(sort);
                    uniques.push(
                        def.unique_indexes.contains(name)
                            && columns.iter().all(|name| fields.contains_key(name)),
                    );
                }
            }
        }
        if !record_ids.is_empty() {
            tx.execute("INSERT INTO fireweed_api007.fields(record_id,queue_id,collection,field,prefix,hash,full_key,value) SELECT rid,$1,col,no,prefix,hash,full_key,value FROM UNNEST($2::bigint[],$3::int[],$4::int[],$5::bytea[],$6::bytea[],$7::bytea[],$8::bytea[]) AS x(rid,col,no,prefix,hash,full_key,value)",
            &[&queue.id,&record_ids,&col_ids,&numbers,&prefixes,&value_hashes,&full,&values]).await.map_err(storage)?;
        }
        if !extra_records.is_empty() {
            tx.execute("INSERT INTO fireweed_api007.additional_fields(record_id,queue_id,collection,name_hash,name,prefix,hash,full_key,value) SELECT rid,$1,col,name_hash,name,prefix,hash,full_key,value FROM UNNEST($2::bigint[],$3::int[],$4::bytea[],$5::bytea[],$6::bytea[],$7::bytea[],$8::bytea[],$9::bytea[]) AS x(rid,col,name_hash,name,prefix,hash,full_key,value)",&[&queue.id,&extra_records,&extra_cols,&extra_name_hashes,&extra_names,&extra_prefixes,&extra_hashes,&extra_full,&extra_values]).await.map_err(storage)?;
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
#[cfg(test)]
#[derive(Clone)]
struct GroupBoundaryTest {
    queue_id: i64,
    expected: Vec<Vec<(String, u64, bool, u64)>>,
    visits: usize,
    receipt_offset: usize,
    fail_at: Option<usize>,
}
#[cfg(test)]
thread_local! {
    static GROUP_BOUNDARY_TEST: std::cell::RefCell<Option<GroupBoundaryTest>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
async fn check_group_boundary(tx: &Transaction<'_>, queue_id: i64) -> QueueResult<()> {
    let step = GROUP_BOUNDARY_TEST.with(|slot| {
        let mut slot = slot.borrow_mut();
        let probe = slot.as_mut()?;
        if probe.queue_id != queue_id {
            return None;
        }
        let visit = probe.visits;
        probe.visits += 1;
        Some((
            probe.expected.get(visit).cloned(),
            probe.fail_at == Some(visit),
            visit + 1 + probe.receipt_offset,
        ))
    });
    let Some((expected, fail, receipt_count)) = step else {
        return Ok(());
    };
    let rows = tx.query("SELECT key,version::text,active,insertion_sequence::text FROM fireweed_api007.records WHERE queue_id=$1 AND collection=0 ORDER BY key", &[&queue_id]).await.map_err(storage)?;
    let observed: Vec<_> = rows
        .iter()
        .map(|row| {
            Ok((
                String::from_utf8(row.get(0)).map_err(storage)?,
                version(row.get(1))?,
                row.get(2),
                version(row.get(3))?,
            ))
        })
        .collect::<QueueResult<_>>()?;
    if expected.as_ref() != Some(&observed) {
        return Err(storage(format!(
            "per-group SQL visibility mismatch: expected {expected:?}, observed {observed:?}"
        )));
    }
    let count: i64 = tx
        .query_one(
            "SELECT COUNT(*) FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=2",
            &[&queue_id],
        )
        .await
        .map_err(storage)?
        .get(0);
    if count != receipt_count as i64 {
        return Err(storage("per-group receipt visibility mismatch"));
    }
    if fail {
        tx.query_one("SELECT 1 / 0", &[]).await.map_err(storage)?;
    }
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
// Covers predicates, five repeated aggregate expressions, joined/final SQL,
// construction temporaries and bounded numeric parameter containers. Full name
// bytes are bound once and charged by the owned request budget separately.
fn bucket_sql_scratch(request: &StatsRequest) -> QueueResult<u64> {
    request.buckets.as_ref().map_or(Ok(0), |dimension| {
        (dimension.buckets.len() as u64)
            .checked_add(1)
            .and_then(|n| n.checked_mul(16_384))
            .and_then(|n| n.checked_add((request.filter.fields.len() as u64).checked_mul(4096)?))
            .ok_or(QueueError::TooLarge)
    })
}
// Shared typed field SQL vocabulary; administrative selectors bind one flat
// byte-array pool instead of allocating a protocol parameter per leaf.
trait FieldSqlParameters {
    fn bytes(&mut self, value: Vec<u8>) -> String;
    fn byte_list(&mut self, values: Vec<Vec<u8>>) -> String;
    fn integer(&mut self, value: i32) -> String;
}
impl FieldSqlParameters for Parameters {
    fn bytes(&mut self, value: Vec<u8>) -> String {
        parameter(self, value)
    }
    fn byte_list(&mut self, values: Vec<Vec<u8>>) -> String {
        parameter(self, values)
    }
    fn integer(&mut self, value: i32) -> String {
        parameter(self, value)
    }
}
mod administration;
fn field_identity_sql(
    field: &str,
    def: &CollectionDefinition,
    params: &mut impl FieldSqlParameters,
) -> QueueResult<(&'static str, String)> {
    let no = field_no(def, field)?;
    let result = if no >= 0 {
        let no = params.integer(no);
        ("fireweed_api007.fields", format!("f.field={no}"))
    } else {
        let hash = params.bytes(probe_hash(field.as_bytes()));
        let name = params.bytes(field.as_bytes().to_vec());
        (
            "fireweed_api007.additional_fields",
            format!("f.name_hash={hash} AND f.name={name}"),
        )
    };
    Ok(result)
}
fn field_predicate_sql(
    predicate: &Predicate,
    params: &mut impl FieldSqlParameters,
) -> QueueResult<String> {
    let mut sql = String::new();
    match predicate {
        Predicate::Equal(value) => {
            let bytes = ordered_key(&[Some(value)])?;
            let hp = params.bytes(probe_hash(&bytes));
            let vp = params.bytes(bytes);
            sql.push_str(&format!(" AND f.hash={hp} AND f.full_key={vp}"));
        }
        Predicate::In(values) => {
            let bytes: Vec<_> = values
                .iter()
                .map(|v| ordered_key(&[Some(v)]))
                .collect::<QueueResult<_>>()?;
            let hashes: Vec<_> = bytes.iter().map(|b| probe_hash(b)).collect();
            let hp = params.byte_list(hashes);
            let vp = params.byte_list(bytes);
            sql.push_str(&format!(" AND f.hash=ANY({hp}) AND f.full_key=ANY({vp})"));
        }
        Predicate::Range { lower, upper } => {
            let first = lower
                .as_ref()
                .or(upper.as_ref())
                .ok_or_else(|| QueueError::Invalid("empty range".into()))?;
            let value = match first {
                Bound::Included(v) | Bound::Excluded(v) => v,
            };
            let floor = scalar_type_prefix(value.field_type());
            let mut ceiling = floor;
            ceiling[2] += 1;
            let fp = params.bytes(floor.to_vec());
            let cp = params.bytes(ceiling.to_vec());
            sql.push_str(&format!(" AND f.prefix>={fp} AND f.prefix<{cp}"));
            for (bound, lower) in [(lower, true), (upper, false)] {
                if let Some(bound) = bound {
                    let (value, inclusive) = match bound {
                        Bound::Included(v) => (v, true),
                        Bound::Excluded(v) => (v, false),
                    };
                    let bytes = ordered_key(&[Some(value)])?;
                    let pp = params.bytes(prefix(&bytes));
                    let vp = params.bytes(bytes);
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
    Ok(sql)
}
fn filter_sql(
    filter: &Filter,
    def: &CollectionDefinition,
    params: &mut Parameters,
) -> QueueResult<String> {
    let mut sql = String::new();
    if let Some(keys) = &filter.keys {
        let raw: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let hashes: Vec<Vec<u8>> = raw.iter().map(|key| probe_hash(key)).collect();
        let hp = parameter(params, hashes);
        let kp = parameter(params, raw);
        sql.push_str(&format!(" AND r.key_hash=ANY({hp}) AND r.key=ANY({kp})"));
    }
    for (field, predicate) in &filter.fields {
        let (table, identity) = field_identity_sql(field, def, params)?;
        sql.push_str(&format!(" AND EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND {identity}"));
        sql.push_str(&field_predicate_sql(predicate, params)?);
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
    effective_at: Option<i64>,
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
            work.body_loaded = include_body;
            if selected.is_some() {
                // Present values prove themselves. Claim metadata does not need
                // absent-name certificates, so omitted/missing fields stay unknown
                // without copying an uncharged requested-name set per row.
                work.field_coverage = fireweed_engine::queue_fields::FieldCoverage::Selected {
                    names: Default::default(),
                    prefixes: Default::default(),
                };
            }
            if let Some(now) = effective_at
                && work.record.lifecycle == Lifecycle::Leased
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
                let address_hash=probe_hash(&encode(&request.queue)?);
                let lock=i64::from_be_bytes(address_hash[..8].try_into().map_err(storage)?);
                tx.execute("SELECT pg_advisory_xact_lock($1)",&[&lock]).await.map_err(storage)?;
                let rows=tx.query("SELECT id,octet_length(definition) FROM fireweed_api007.queues WHERE address_hash=$1 AND tenant=$2 AND name=$3",
                    &[&address_hash,&request.queue.tenant.as_bytes(),&request.queue.queue.as_bytes()]).await.map_err(storage)?;
                let encoded=encode_definition(&request.schema)?;
                if let Some(row)=rows.first() {
                    if rows.len()!=1{return Err(storage("duplicate queue authority"));}
                    let bytes=u64::try_from(row.get::<_,i32>(1)).map_err(storage)?;
                    let _schema_bytes=_owned_job.buffer(bytes.checked_mul(4).ok_or(QueueError::TooLarge)?)?;
                    let raw:Vec<u8>=tx.query_one("SELECT definition FROM fireweed_api007.queues WHERE id=$1",&[&row.get::<_,i64>(0)]).await.map_err(storage)?.get(0);
                    let _schema_headers=_owned_job.buffer(definition_headers(&raw)?)?;
                    if decode_definition(&raw)?!=request.schema{return Err(QueueError::DefinitionConflict);}
                }else{
                    let token=fireweed_engine::generate_query_lease_token().map_err(storage)?;
                    let secret=Sha256::digest(token.as_str().as_bytes()).to_vec();
                    tx.execute("INSERT INTO fireweed_api007.queues(address_hash,tenant,name,definition,cursor_secret) VALUES($1,$2,$3,$4,$5)",
                        &[&address_hash,&request.queue.tenant.as_bytes(),&request.queue.queue.as_bytes(),&encoded,&secret]).await.map_err(storage)?;
                }tx.commit().await.map_err(storage)?;Ok(())
            })).await
        })
    }
    fn commit(&self, request: CommitBatch) -> QueueFuture<'_, Vec<CommitReceipt>> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            administrative_envelope_preflight(&request, &self.inner.limits)?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let workers = self.inner.scratch_workers.clone();
            let quota = Arc::clone(&self.inner.scratch_quota);
            let incoming_headers = incoming_headers(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes))
                .and_then(|n| n.checked_add(incoming_headers))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge, move |client, reservation| {
                Box::pin(async move {
                    let reservation = Arc::new(reservation);
                    let tx = client.transaction().await.map_err(storage)?;
                    let mut queue = queue_row(&tx, &request.queue, &reservation).await?;
                    request.validate(&queue.schema, &limits)?;
                    let mut receipt_charges = vec![];
                    let mut fresh_receipt_charges = vec![];
                    let mut out = vec![];
                    let mut known = vec![];
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
                        known.push((fp, outcome));
                    }
                    // Replay is resolved before any current-record materialization.
                    // Each fresh group reads the previous groups' physical writes
                    // inside this same fenced transaction. No public transaction
                    // or partial acknowledgement is introduced.
                    for (group, (fp, known)) in request.groups.iter().zip(known) {
                        if let Some(known) = known {
                            out.push(known);
                            continue;
                        }
                        if group.administration.is_some() {
                            let admitted = administration::run_administrative_group(
                                &tx,
                                &request.queue,
                                &queue,
                                group,
                                &limits,
                                clock.as_ref(),
                                &reservation,
                                &workers,
                                Arc::clone(&quota),
                                true,
                            )
                            .await?;
                            let (receipt, next_sequence, charge) = admitted.into_parts();
                            fresh_receipt_charges.push(charge);
                            save_receipt(&tx, queue.id, 2, &group.request_id, &fp, &receipt)
                                .await?;
                            #[cfg(test)]
                            check_group_boundary(&tx, queue.id).await?;
                            if let Some(next_sequence) = next_sequence {
                                queue.next_sequence = next_sequence;
                            }
                            out.push(receipt);
                            continue;
                        }
                        let mut loaded = load_group(&tx, &queue, group, &reservation).await?;
                        let before = loaded.snapshot.clone();
                        let now = timestamp_nanos(clock.now())?;
                        load_unique_probes(&tx, &queue, &mut loaded, group, now, &reservation)
                            .await?;
                        let delta =
                            plan_commit_group(group, &mut loaded.snapshot, now, &queue.schema)?;
                        let _output_charge = reservation.buffer(
                            encoded_len(&delta)?
                                .checked_mul(4)
                                .ok_or(QueueError::TooLarge)?,
                        )?;
                        persist_delta(&tx, &queue, &delta, &before, &mut loaded).await?;
                        save_receipt(&tx, queue.id, 2, &group.request_id, &fp, &delta.receipt)
                            .await?;
                        #[cfg(test)]
                        check_group_boundary(&tx, queue.id).await?;
                        queue.next_sequence = delta.next_sequence;
                        out.push(delta.receipt);
                    }
                    if encoded_len(&out)? > limits.max_response_bytes {
                        return Err(storage(
                            "complete commit response exceeds admitted envelope",
                        ));
                    }
                    tx.commit().await.map_err(storage)?;
                    Ok(out)
                })
            })
            .await
        })
    }
    fn preview(&self, request: PreviewRequest) -> QueueFuture<'_, PreviewResult> {
        Box::pin(async move {
            request.preflight(&self.inner.limits)?;
            administrative_groups_preflight(
                &request.queue,
                std::slice::from_ref(&request.group),
                &self.inner.limits,
            )?;
            let limits = self.inner.limits;
            let clock = Arc::clone(&self.inner.clock);
            let workers = self.inner.scratch_workers.clone();
            let quota = Arc::clone(&self.inner.scratch_quota);
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes))
                .and_then(|n| n.checked_add(incoming_headers(&request).ok()?))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge, move |client, reservation| {
                Box::pin(async move {
                    let reservation = Arc::new(reservation);
                    let tx = client.transaction().await.map_err(storage)?;
                    let queue = queue_row(&tx, &request.queue, &reservation).await?;
                    request.validate(&queue.schema, &limits)?;
                    // Preview never consults or writes execution identity receipts.
                    let mut administrative_charge = None;
                    let outcome = if request.group.administration.is_some() {
                        let admitted = administration::run_administrative_group(
                            &tx,
                            &request.queue,
                            &queue,
                            &request.group,
                            &limits,
                            clock.as_ref(),
                            &reservation,
                            &workers,
                            quota,
                            false,
                        )
                        .await?;
                        let (receipt, _, charge) = admitted.into_parts();
                        administrative_charge = Some(charge);
                        receipt.outcome
                    } else {
                        let mut loaded =
                            load_group(&tx, &queue, &request.group, &reservation).await?;
                        let now = timestamp_nanos(clock.now())?;
                        load_unique_probes(
                            &tx,
                            &queue,
                            &mut loaded,
                            &request.group,
                            now,
                            &reservation,
                        )
                        .await?;
                        plan_commit_group(&request.group, &mut loaded.snapshot, now, &queue.schema)?
                            .receipt
                            .outcome
                    };
                    let result = PreviewResult {
                        outcome: match outcome {
                            GroupOutcome::Committed { versions } => {
                                PreviewOutcome::Committed { versions }
                            }
                            GroupOutcome::AdministrativeCommitted {
                                versions,
                                administration,
                            } => PreviewOutcome::AdministrativeCommitted {
                                versions,
                                administration,
                            },
                            GroupOutcome::Rejected(reason) => PreviewOutcome::Rejected(reason),
                            GroupOutcome::RequestConflict | GroupOutcome::RequestExpired => {
                                return Err(storage("preview returned execution receipt status"));
                            }
                        },
                    };
                    if encoded_len(&result)? > limits.max_response_bytes {
                        return Err(storage("preview response exceeds admitted budget"));
                    }
                    // No persist_delta or nextval was called, even speculatively.
                    tx.rollback().await.map_err(storage)?;
                    drop(administrative_charge);
                    Ok(result)
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
                    let read_set = CommitGroup {
                        administration: None,
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
                    let probes =
                        insert_unique_probes(&request.items, &loaded.snapshot, &queue.schema)?
                            .into_iter()
                            .collect();
                    load_unique_authority(&tx, &queue, &mut loaded, probes, &reservation).await?;
                    let (deltas, outcomes) =
                        plan_insert_vector(&request, &mut loaded.snapshot, &queue.schema, || {
                            timestamp_nanos(clock.now())
                        })?;
                    let combined = combine_deltas(&deltas, queue.next_sequence)?;
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
                        let raw_hash:Option<Vec<u8>>=row.get(11);
                        ClaimOwnership {lifecycle:lifecycle(row.get(5))?,expires_at:row.get(10),token_hash:checked_lease_hash(raw_hash.as_deref())?}.validate(token,now)?;}
                    let selected_fields=selected_field_numbers(&queue.schema.work,request.fields.as_ref())?;
                    payload_budget(&tx,&ids,selected_fields.as_ref(),request.include_body,request.max_bytes,&queue.schema.work).await?;
                    let _headers=_owned_job.buffer(materialized_headers(&tx,&rows.iter().map(|row|row.get::<_,i64>(0)).collect::<Vec<_>>(),selected_fields.as_ref()).await?)?;
                let stored=hydrate_work(&tx,&rows,&queue.schema,request.fields.as_ref(),request.include_body,None).await?;
                    let map:BTreeMap<_,_>=rows.iter().map(|row|row.get::<_,i64>(0)).zip(stored).collect();let mut out=vec![];
                    for (id,token) in replay.items {
                        let work=map.get(&id).ok_or(QueueError::ClaimExpired)?;
                        ClaimOwnership {lifecycle:work.record.lifecycle,expires_at:work.record.lease_expires_at,token_hash:work.lease_hash}.validate(&token,now)?;
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
                let sizes=payload_sizes(&tx,&candidate_ids,selected_fields.as_ref(),request.include_body,&queue.schema.work).await?;
                let mut total=0u64;let mut fitting=vec![];
                for row in &candidates {let id:i64=row.get(0);let bytes=sizes.get(&id).copied().unwrap_or(0).checked_add(u64::try_from(row.get::<_,i32>(1)).map_err(storage)?).ok_or(QueueError::TooLarge)?;
                    if total.checked_add(bytes).is_none_or(|n|n>request.max_bytes){break;}total+=bytes;fitting.push(id);}
                if !candidates.is_empty()&&fitting.is_empty(){return Err(QueueError::TooLarge);}
                let sql=format!("SELECT {RECORD_COLUMNS} FROM fireweed_api007.records r WHERE r.id=ANY($1) AND r.queue_id=$2");
                let rows=tx.query(&sql,&[&fitting,&queue.id]).await.map_err(storage)?;
                let mut by_id:BTreeMap<i64,Row>=rows.into_iter().map(|row|(row.get(0),row)).collect();
                let rows:Vec<Row>=fitting.iter().map(|id|by_id.remove(id).ok_or_else(||storage("missing selected record"))).collect::<QueueResult<_>>()?;
                let _headers=_owned_job.buffer(materialized_headers(&tx,&rows.iter().map(|row|row.get::<_,i64>(0)).collect::<Vec<_>>(),selected_fields.as_ref()).await?)?;
                let stored=hydrate_work(&tx,&rows,&queue.schema,request.fields.as_ref(),request.include_body,None).await?;
                let expiry=claim_expiry(now,request.lease_nanos)?;
                let mut out=vec![];let mut response_budget=ClaimResponseBudget::new(request.max_bytes)?;let mut replay=ClaimReplay {items:vec![]};let mut ids=vec![];let mut token_hashes=vec![];let mut versions=vec![];let mut attempts=vec![];
                for (row,work) in rows.iter().zip(stored) {
                    let token=fireweed_engine::generate_query_lease_token().map_err(storage)?.as_str().as_bytes().to_vec();
                    let planned=plan_claim(work,token,ClaimContext {now,eligible_at:request.eligible_at,expires_at:expiry,insertion_head:queue.next_sequence})?;
                    let token=planned.lease.token.clone();
                    let claimed=planned.into_claimed();
                    if !response_budget.try_include(&claimed)? {
                        if out.is_empty(){return Err(QueueError::TooLarge);}break;
                    }
                    out.push(claimed);
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
                let keys:Vec<Vec<u8>>=request.keys.iter().map(|key|key.as_bytes().to_vec()).collect();let hashes:Vec<_>=keys.iter().map(|key|probe_hash(key)).collect();
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
                let ids:Vec<Vec<u8>>=request.request_ids.iter().map(|s|s.as_bytes().to_vec()).collect();let hashes:Vec<_>=ids.iter().map(|s|probe_hash(s)).collect();
                let size=tx.query_one("SELECT COALESCE(sum(octet_length(outcome)),0)::bigint FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=2 AND identity_hash=ANY($2) AND identity=ANY($3) AND NOT expired",&[&queue.id,&hashes,&ids]).await.map_err(storage)?.get::<_,i64>(0);
                if size as u64>request.max_bytes{return Err(QueueError::TooLarge);}
                let rows=tx.query("SELECT identity,expired,CASE WHEN expired THEN NULL ELSE outcome END FROM fireweed_api007.requests WHERE queue_id=$1 AND domain=2 AND identity_hash=ANY($2) AND identity=ANY($3)",&[&queue.id,&hashes,&ids]).await.map_err(storage)?;
                let mut receipt_charges=vec![];
                let mut found=BTreeMap::new();for row in rows {let id=key(row.get(0))?;let outcome=if row.get::<_,bool>(1){ReceiptLookup::Expired}else{let raw=row.get::<_,Vec<u8>>(2);receipt_charges.push(_owned_job.buffer(CommitReceipt::stored_headers(&raw)?)?);ReceiptLookup::Found({ let receipt: CommitReceipt = decode(&raw)?; receipt.validate_persisted()?; receipt })};
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
            self.inner.scratch_workers.close_and_drain().await?;
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
            let sql_scratch = bucket_sql_scratch(&request)?;
            let charge = encoded_len(&request)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(limits.max_response_bytes.checked_mul(4)?))
                .and_then(|n| n.checked_add(incoming_headers))
                .and_then(|n| n.checked_add(sql_scratch))
                .ok_or(QueueError::TooLarge)?;
            self.dispatch(charge,move |client,reservation|Box::pin(async move {
                let _owned_job=reservation;
                let tx=client.transaction().await.map_err(storage)?;let queue=queue_row(&tx,&request.queue,&_owned_job).await?;request.validate(&queue.schema,&limits)?;let now=timestamp_nanos(clock.now())?;
                let mut params:Parameters=vec![Box::new(queue.id),Box::new(now),Box::new(request.eligible_at)];
                let filter=filter_sql(&request.filter,&queue.schema.work,&mut params)?;
                let effective="CASE WHEN r.lifecycle=1 AND r.lease_until<=$2 THEN 0 ELSE r.lifecycle END";
                let lifecycle_filter=if let Some(state)=request.filter.lifecycle {let p=parameter(&mut params,life(state));format!(" AND ({effective})={p}::smallint")}else{String::new()};
                let aggregate=format!("count(*) FILTER(WHERE ({effective})=0),count(*) FILTER(WHERE ({effective})=1),count(*) FILTER(WHERE ({effective})=2),count(*) FILTER(WHERE ({effective})=3),min(r.eligible_since) FILTER(WHERE ({effective})=0 AND r.available_at<=$3)");
                let base=format!("r.queue_id=$1 AND r.collection=0 AND r.active{filter}{lifecycle_filter}");
                let total=tx.query_one(&format!("SELECT {aggregate} FROM fireweed_api007.records r WHERE {base}"),&refs(&params)).await.map_err(storage)?;
                let totals=counts(&total,0)?;let mut groups=vec![];
                if !request.group_by.is_empty(){
                    let mut joins=String::new();let mut columns=vec![];
                    for (n,name) in request.group_by.iter().enumerate(){
                        let no=field_no(&queue.schema.work,name)?;
                        if no>=0 {
                            let p=parameter(&mut params,no);
                            joins.push_str(&format!(" LEFT JOIN fireweed_api007.fields g{n} ON g{n}.record_id=r.id AND g{n}.field={p}"));
                        } else {
                            let hp=parameter(&mut params,probe_hash(name.as_bytes()));
                            let np=parameter(&mut params,name.as_bytes().to_vec());
                            joins.push_str(&format!(" LEFT JOIN fireweed_api007.additional_fields g{n} ON g{n}.record_id=r.id AND g{n}.name_hash={hp} AND g{n}.name={np}"));
                        }
                        columns.push(format!("g{n}.full_key"));
                    }
                    let joined=columns.join(",");let ordering=columns.iter().map(|col|format!("{col} ASC NULLS FIRST")).collect::<Vec<_>>().join(",");let lp=parameter(&mut params,i64::from(limits.max_records)+1);
                    let sql=format!("SELECT min(r.id),{aggregate} FROM fireweed_api007.records r{joins} WHERE {base} GROUP BY {joined} ORDER BY {ordering} LIMIT {lp}");
                    let rows=tx.query(&sql,&refs(&params)).await.map_err(storage)?;
                    if rows.len()>limits.max_records as usize{return Err(QueueError::TooLarge);}
                    let ids:Vec<i64>=rows.iter().map(|row|row.get(0)).collect();
                    // Load only bounded representative grouping values, never queue bodies.
                    let nos:Vec<i32>=request.group_by.iter().map(|name|field_no(&queue.schema.work,name)).collect::<QueueResult<_>>()?;
                    let names:Vec<Vec<u8>>=request.group_by.iter().map(|name|name.as_bytes().to_vec()).collect();
                    let hashes:Vec<Vec<u8>>=names.iter().map(|name|probe_hash(name)).collect();
                    let values_sql="SELECT f.record_id,p.ordinal,f.value FROM UNNEST($2::int[],$3::bytea[],$4::bytea[]) WITH ORDINALITY AS p(no,hash,name,ordinal) JOIN fireweed_api007.fields f ON p.no>=0 AND f.field=p.no AND f.record_id=ANY($1) UNION ALL SELECT f.record_id,p.ordinal,f.value FROM UNNEST($2::int[],$3::bytea[],$4::bytea[]) WITH ORDINALITY AS p(no,hash,name,ordinal) JOIN fireweed_api007.additional_fields f ON p.no=-1 AND f.name_hash=p.hash AND f.name=p.name AND f.record_id=ANY($1)";
                    let bytes=tx.query_one(&format!("SELECT COALESCE(sum(octet_length(value)),0)::bigint FROM ({values_sql}) v"),&[&ids,&nos,&hashes,&names]).await.map_err(storage)?.get::<_,i64>(0);
                    if u64::try_from(bytes).map_err(storage)?>limits.max_response_bytes{return Err(QueueError::TooLarge);}
                    let _headers=_owned_job.buffer(record_headers(ids.len(),nos.len())?)?;
                    let values=tx.query(values_sql,&[&ids,&nos,&hashes,&names]).await.map_err(storage)?;
                    let mut fields:BTreeMap<(i64,i64),FieldValue>=BTreeMap::new();
                    for row in values {
                        if fields.insert((row.get(0),row.get(1)),decode(&row.get::<_,Vec<u8>>(2))?).is_some() {return Err(storage("duplicate grouping value authority"));}
                    }
                    for row in rows {let id:i64=row.get(0);let values=(1..=nos.len() as i64).map(|no|fields.get(&(id,no)).cloned()).collect();groups.push(StatsGroup {values,counts:counts(&row,1)?});}


                }
                drop(params);
                let mut buckets = Vec::new();
                if let Some(dimension) = &request.buckets {
                    let mut params: Parameters = vec![Box::new(queue.id), Box::new(now), Box::new(request.eligible_at)];
                    let filter = filter_sql(&request.filter, &queue.schema.work, &mut params)?;
                    let lifecycle_filter = if let Some(state) = request.filter.lifecycle {
                        let p = parameter(&mut params, life(state)); format!(" AND ({effective})={p}::smallint")
                    } else { String::new() };
                    let base = format!("r.queue_id=$1 AND r.collection=0 AND r.active{filter}{lifecycle_filter}");
                    // Bind full field identity once, even for long names and 50 bins.
                    let (table, identity) = field_identity_sql(&dimension.field, &queue.schema.work, &mut params)?;
                    let mut predicates = Vec::new();
                    for bucket in &dimension.buckets {
                        let predicate = Predicate::Range {
                            lower: Some(Bound::Included(bucket.lower_inclusive.clone())),
                            upper: Some(Bound::Excluded(bucket.upper_exclusive.clone())),
                        };
                        let range = field_predicate_sql(&predicate, &mut params)?;
                        predicates.push(format!("EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND {identity}{range})"));
                    }
                    let missing = format!("NOT EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=r.collection AND {identity})");
                    predicates.push(missing);
                    let columns = predicates.iter().map(|p| aggregate.replace("WHERE ", &format!("WHERE ({p}) AND "))).collect::<Vec<_>>().join(",");
                    let row = tx.query_one(&format!("SELECT {columns} FROM fireweed_api007.records r WHERE {base}"), &refs(&params)).await.map_err(storage)?;
                    for (i, label) in dimension.buckets.iter().map(|b| &b.label).chain(std::iter::once(&dimension.missing_label)).enumerate() {
                        buckets.push(BucketCounts { label: label.clone(), counts: counts(&row, i * 5)? });
                    }
                }
                let result=QueueStats {totals,groups,buckets};if encoded_len(&result)?>limits.max_response_bytes{return Err(QueueError::TooLarge);}Ok(result)
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
                if let Some(state)=request.filter.lifecycle {let p=parameter(&mut params,life(state));let np=parameter(&mut params,now);filter.push_str(&format!(" AND (CASE WHEN r.lifecycle=1 AND r.lease_until<={np} THEN 0 ELSE r.lifecycle END)={p}::smallint"));}
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
                let records:Vec<QueryRecord>=if col==0 {hydrate_work(&tx,&rows,&queue.schema,None,true,Some(now)).await?.into_iter().map(|work|QueryRecord::Work(work.record)).collect()}
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
    fields: Option<&FieldSelection>,
    body: bool,
    def: &CollectionDefinition,
) -> QueueResult<BTreeMap<i64, u64>> {
    let selected = fields.map(|fields| fields.declared.clone());
    let names: Vec<i64> = def.fields.keys().map(|name| name.len() as i64).collect();
    let rows=tx.query("SELECT r.id,(COALESCE((SELECT sum(octet_length(f.value)+($4::bigint[])[f.field+1]) FROM fireweed_api007.fields f WHERE f.record_id=r.id AND ($2::int[] IS NULL OR f.field=ANY($2))),0)+CASE WHEN $3 THEN COALESCE((SELECT octet_length(b.value) FROM fireweed_api007.bodies b WHERE b.record_id=r.id),0) ELSE 0 END)::bigint FROM fireweed_api007.records r WHERE r.id=ANY($1)",&[&ids,&selected,&body,&names]).await.map_err(storage)?;
    let hashes = fields.map(|fields| fields.hashes.clone());
    let additional = fields.map(|fields| fields.additional.clone());
    let extra=tx.query("SELECT record_id,COALESCE(sum(octet_length(name)+octet_length(value)),0)::bigint FROM fireweed_api007.additional_fields WHERE record_id=ANY($1) AND ($2::bytea[] IS NULL OR (name_hash=ANY($2) AND name=ANY($3))) GROUP BY record_id",&[&ids,&hashes,&additional]).await.map_err(storage)?;
    let extra: BTreeMap<i64, u64> = extra
        .iter()
        .map(|row| {
            Ok((
                row.get(0),
                u64::try_from(row.get::<_, i64>(1)).map_err(storage)?,
            ))
        })
        .collect::<QueueResult<_>>()?;
    rows.iter()
        .map(|row| {
            let id: i64 = row.get(0);
            Ok((
                id,
                u64::try_from(row.get::<_, i64>(1))
                    .map_err(storage)?
                    .checked_add(extra.get(&id).copied().unwrap_or(0))
                    .ok_or(QueueError::TooLarge)?,
            ))
        })
        .collect()
}
async fn payload_budget(
    tx: &Transaction<'_>,
    ids: &[i64],
    fields: Option<&FieldSelection>,
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

struct FieldSelection {
    declared: Vec<i32>,
    additional: Vec<Vec<u8>>,
    hashes: Vec<Vec<u8>>,
}
fn selected_field_numbers(
    def: &CollectionDefinition,
    selected: Option<&BTreeSet<String>>,
) -> QueueResult<Option<FieldSelection>> {
    selected
        .map(|keys| {
            let mut out = FieldSelection {
                declared: vec![],
                additional: vec![],
                hashes: vec![],
            };
            for key in keys {
                let no = field_no(def, key)?;
                if no >= 0 {
                    out.declared.push(no);
                } else {
                    out.hashes.push(probe_hash(key.as_bytes()));
                    out.additional.push(key.as_bytes().to_vec());
                }
            }
            Ok(out)
        })
        .transpose()
}

// These fixtures share one schema: opening a store intentionally takes a
// global DDL fence. Isolate fixtures, not the concurrent operations they test.
#[cfg(test)]
async fn physical_fixture() -> tokio::sync::OwnedMutexGuard<()> {
    static FIXTURE: std::sync::OnceLock<Arc<Mutex<()>>> = std::sync::OnceLock::new();
    FIXTURE
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
        .lock_owned()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn replay_rejects_oversized_raw_lease_hash_before_payload_hydration() {
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let address = QueueAddress {
            tenant: "lease-corruption".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: address.clone(),
                schema: fireweed_conformance::queue_contract::schema(),
            })
            .await
            .unwrap();
        store
            .insert(InsertRequest {
                queue: address.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items: vec![NewWork {
                    key: "a".into(),
                    priority: 0,
                    available_at: 0,
                    fields: BTreeMap::from([
                        ("kind".into(), FieldValue::Text("task".into())),
                        ("target".into(), FieldValue::Text("a".into())),
                    ]),
                    body: Default::default(),
                }],
            })
            .await
            .unwrap();
        let request = ClaimRequest {
            queue: address.clone(),
            request_id: "claim".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000_000,
            max_records: 1,
            max_bytes: 4096,
            fields: Some(Default::default()),
            include_body: false,
        };
        assert_eq!(store.claim(request.clone()).await.unwrap().len(), 1);
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        client.execute("UPDATE fireweed_api007.records r SET lease_hash=decode(repeat('ab',4194304),'hex') FROM fireweed_api007.queues q WHERE r.queue_id=q.id AND q.tenant=$1 AND q.name=$2", &[&address.tenant.as_bytes(), &address.queue.as_bytes()]).await.unwrap();
        assert!(matches!(
            store.claim(request).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        store.shutdown().await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    }
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
    async fn public_preview_never_allocates_ids_and_late_admin_failure_rolls_back_receipts() {
        use fireweed_core::queue_administration::*;
        let _fixture = physical_fixture().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                GROUP_BOUNDARY_TEST.with(|slot| *slot.borrow_mut() = None);
            }
        }
        let _reset = Reset;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let address = QueueAddress {
            tenant: "public-admin-atomic".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        store
            .provision(ProvisionRequest {
                queue: address.clone(),
                schema: fireweed_conformance::queue_contract::schema(),
            })
            .await
            .unwrap();
        store
            .insert(InsertRequest {
                queue: address.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items: ["a", "b"]
                    .into_iter()
                    .map(|key| NewWork {
                        key: key.into(),
                        priority: 0,
                        available_at: 0,
                        fields: BTreeMap::from([
                            ("kind".into(), FieldValue::Text("task".into())),
                            ("target".into(), FieldValue::Text(key.into())),
                        ]),
                        body: Default::default(),
                    })
                    .collect(),
            })
            .await
            .unwrap();
        let make = |id: &str, key: &str, version, priority| CommitGroup {
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
            administration: Some(AdministrativeCommit {
                max_result_bytes: 4096,
                intent: AdministrativeIntent {
                    evaluated_at: 0,
                    before_fields: Some(BTreeSet::from(["kind".into()])),
                    operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                        key: key.into(),
                        expected_version: Some(version),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(priority),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: Default::default(),
                        },
                    }]),
                },
            }),
        };
        let mut transition = make("preview-then-execute", "a", 1, 17);
        transition.state_updates.push(StateUpdate {
            collection: "instances".into(),
            key: "workflow".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Default::default(),
                value: vec![7, 8].into(),
            },
        });
        transition.work_inserts.push(NewWork {
            key: "next".into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([("target".into(), FieldValue::Text("next".into()))]),
            body: Default::default(),
        });
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let sequence = || async {
            let row = client
                .query_one(
                    "SELECT last_value,is_called FROM fireweed_api007.records_id_seq",
                    &[],
                )
                .await
                .unwrap();
            (row.get::<_, i64>(0), row.get::<_, bool>(1))
        };
        let before = sequence().await;
        let preview = store
            .preview(PreviewRequest {
                queue: address.clone(),
                group: transition.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(
            preview.outcome,
            PreviewOutcome::AdministrativeCommitted { .. }
        ));
        assert_eq!(sequence().await, before);
        assert_eq!(
            store
                .read_commit(ReceiptReadRequest {
                    queue: address.clone(),
                    request_ids: vec![transition.request_id.clone()],
                    max_bytes: 1 << 20
                })
                .await
                .unwrap(),
            vec![ReceiptLookup::Absent]
        );
        let committed = store
            .commit(CommitBatch {
                queue: address.clone(),
                groups: vec![transition.clone()],
            })
            .await
            .unwrap();
        let after = sequence().await;
        assert!(after.0 > before.0);
        let preview = store
            .preview(PreviewRequest {
                queue: address.clone(),
                group: transition,
            })
            .await
            .unwrap();
        assert_eq!(
            preview.outcome,
            PreviewOutcome::Rejected(Rejection::AlreadyExists)
        );
        assert_eq!(sequence().await, after);
        assert_eq!(
            store
                .read_commit(ReceiptReadRequest {
                    queue: address.clone(),
                    request_ids: vec!["preview-then-execute".into()],
                    max_bytes: 1 << 20
                })
                .await
                .unwrap(),
            vec![ReceiptLookup::Found(committed[0].clone())]
        );
        let first = make("late-first", "a", 2, 33);
        let mut last = make("late-last", "b", 1, 44);
        last.state_updates.push(StateUpdate {
            collection: "instances".into(),
            key: "late".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Default::default(),
                value: vec![9].into(),
            },
        });
        let queue_id: i64 = client
            .query_one(
                "SELECT id FROM fireweed_api007.queues WHERE tenant=$1 AND name=$2",
                &[&address.tenant.as_bytes(), &address.queue.as_bytes()],
            )
            .await
            .unwrap()
            .get(0);
        // A stored before image is independent of the tiny replay request.
        // Even malformed payload bytes must be rejected by admission before decode.
        let tx = client.transaction().await.unwrap();
        let id = "large-before-replay";
        let fp = [17u8; 32];
        tx.execute("INSERT INTO fireweed_api007.requests(queue_id,domain,identity_hash,identity,fingerprint,outcome) VALUES($1,2,$2,$3,$4,$5)",
            &[&queue_id, &probe_hash(id.as_bytes()), &id.as_bytes(), &fp.as_slice(), &vec![255u8; 1 << 20]]).await.unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let _fence = queue_row(&tx, &address, &job).await.unwrap();
        let before_usage = admission.usage().unwrap();
        let mut charges = vec![];
        assert!(matches!(
            receipt::<CommitReceipt>(&tx, queue_id, 2, id, &fp, &job, &mut charges).await,
            Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted")
        ));
        assert_eq!(admission.usage().unwrap(), before_usage);
        assert!(charges.is_empty());
        drop(charges);
        tx.rollback().await.unwrap();
        GROUP_BOUNDARY_TEST.with(|slot| {
            *slot.borrow_mut() = Some(GroupBoundaryTest {
                queue_id,
                visits: 0,
                receipt_offset: 1,
                fail_at: Some(1),
                expected: vec![
                    vec![
                        ("a".into(), 3, true, 1),
                        ("b".into(), 1, true, 2),
                        ("next".into(), 1, true, 3),
                    ],
                    vec![
                        ("a".into(), 3, true, 1),
                        ("b".into(), 2, true, 2),
                        ("next".into(), 1, true, 3),
                    ],
                ],
            })
        });
        assert!(matches!(
            store
                .commit(CommitBatch {
                    queue: address.clone(),
                    groups: vec![first, last]
                })
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(
            GROUP_BOUNDARY_TEST.with(|slot| slot.borrow().as_ref().unwrap().visits),
            2
        );
        GROUP_BOUNDARY_TEST.with(|slot| *slot.borrow_mut() = None);
        assert_eq!(
            store
                .read_commit(ReceiptReadRequest {
                    queue: address.clone(),
                    request_ids: vec!["late-first".into(), "late-last".into()],
                    max_bytes: 1 << 20
                })
                .await
                .unwrap(),
            vec![ReceiptLookup::Absent, ReceiptLookup::Absent]
        );
        let state = store
            .read_state(StateReadRequest {
                queue: address.clone(),
                collection: "instances".into(),
                keys: vec!["late".into()],
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        assert!(state[0].is_none());
        let rows = client.query("SELECT r.key,r.version::text,r.priority FROM fireweed_api007.records r JOIN fireweed_api007.queues q ON q.id=r.queue_id WHERE q.tenant=$1 AND q.name=$2 AND r.key=ANY($3::bytea[]) ORDER BY r.key", &[&address.tenant.as_bytes(), &address.queue.as_bytes(), &vec![b"a".to_vec(),b"b".to_vec()]]).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.get::<_, Vec<u8>>(0),
                    row.get::<_, String>(1),
                    row.get::<_, i64>(2)
                ))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), "2".into(), 17),
                (b"b".to_vec(), "1".into(), 0)
            ]
        );
        store.shutdown().await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn shutdown_waits_for_final_worker_reservation_after_dispatch_root_returns() {
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let (ready, held) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        store
            .dispatch(128, move |_client, reservation| {
                Box::pin(async move {
                    let reservation = Arc::new(reservation);
                    let resource = reservation.owned_buffer(4096)?;
                    let worker = tokio::task::spawn_blocking(move || {
                        let _resource = resource;
                        ready.send(()).unwrap();
                        wait.recv().unwrap();
                    });
                    // Model root-owner termination while an already-owned worker finishes.
                    drop(worker);
                    Ok(())
                })
            })
            .await
            .unwrap();
        held.await.unwrap();
        assert_eq!(store.inner.admission.usage().unwrap(), (1, 128 + 4096));
        let shutdown = store.shutdown();
        tokio::pin!(shutdown);
        assert!(matches!(
            futures::poll!(shutdown.as_mut()),
            std::task::Poll::Pending
        ));
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.inner.admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn selective_loader_and_writer_avoid_unrelated_payload() {
        staged_selective_fixture().await;
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn selective_writer_disambiguates_colliding_names_and_values() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COLLIDING_PROBES.set(false);
            }
        }
        COLLIDING_PROBES.set(true);
        let _reset = Reset;
        staged_selective_fixture().await;
    }
    async fn staged_selective_fixture() {
        let _fixture = physical_fixture().await;
        use fireweed_engine::{queue_fields::CheckedFields, queue_planner::required_work_fields};
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let queue = QueueAddress {
            tenant: "api007-staged-selective".into(),
            queue: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string(),
        };
        let mut schema = fireweed_conformance::queue_contract::schema();
        schema.work.fields = BTreeMap::from([
            ("gate".into(), FieldType::Boolean),
            ("target".into(), FieldType::Text),
            ("unrelated".into(), FieldType::Bytes),
        ]);
        schema.work.indexes.clear();
        schema.work.unique_indexes.clear();
        schema
            .work
            .additional_field_prefixes
            .insert("/payload/".into());
        schema.state = BTreeMap::from([(
            "state".into(),
            CollectionDefinition {
                fields: BTreeMap::from([
                    ("aaa".into(), FieldType::Text),
                    ("bbb".into(), FieldType::Text),
                    ("gate".into(), FieldType::Boolean),
                ]),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::new(),
            },
        )]);
        schema
            .work
            .indexes
            .insert("by_gate".into(), vec!["gate".into()]);
        schema
            .work
            .indexes
            .insert("by_target".into(), vec!["target".into()]);
        schema.work.unique_indexes.insert("by_target".into());
        store
            .provision(ProvisionRequest {
                queue: queue.clone(),
                schema: schema.clone(),
            })
            .await
            .unwrap();
        let item = NewWork {
            key: "a".into(),
            priority: 0,
            available_at: 0,
            body: vec![9; 1 << 20].into(),
            fields: BTreeMap::from([
                ("gate".into(), FieldValue::Boolean(false)),
                ("target".into(), FieldValue::Text("owned".into())),
                ("/payload/blob".into(), FieldValue::Bytes(vec![7; 1 << 20])),
                ("unrelated".into(), FieldValue::Bytes(vec![8; 1 << 20])),
                ("/payload/control\0α".into(), FieldValue::Integer(10)),
                ("/payload/control\0β".into(), FieldValue::Integer(20)),
            ]),
        };
        let mut other = item.clone();
        other.key = "b".into();
        other
            .fields
            .insert("target".into(), FieldValue::Text("other".into()));
        store
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: "seed".into(),
                mode: InsertMode::InsertOnly,
                items: vec![
                    item,
                    other,
                    NewWork {
                        key: "c".into(),
                        priority: 0,
                        available_at: 0,
                        fields: Fields::new(),
                        body: Default::default(),
                    },
                ],
            })
            .await
            .unwrap();
        let state_update = StateUpdate {
            collection: "state".into(),
            key: "s".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: BTreeMap::from([
                    ("aaa".into(), FieldValue::Text("a".into())),
                    ("bbb".into(), FieldValue::Text("b".into())),
                    ("gate".into(), FieldValue::Boolean(false)),
                ]),
                value: Default::default(),
            },
        };
        store
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![CommitGroup {
                    administration: None,
                    request_id: "seed-state".into(),
                    claimed_updates: vec![],
                    work_updates: vec![],
                    work_inserts: vec![],
                    state_updates: vec![state_update.clone()],
                }],
            })
            .await
            .unwrap();
        let mut state_update = state_update;
        state_update.expect = StateExpect::Version(1);
        let group = CommitGroup {
            administration: None,
            request_id: "staged-control".into(),
            claimed_updates: vec![],
            state_updates: vec![state_update],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("gate".into(), Some(FieldValue::Boolean(true)))]),
                    ..WorkPatch::default()
                }),
            }],
        };
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let tx = client.transaction().await.unwrap();
        let admission = QueueAdmission::new(1, 256 << 10).unwrap();
        let job = admission.reserve(0).unwrap();
        let row = queue_row(&tx, &queue, &job).await.unwrap();
        assert!(matches!(
            load_group_with_fields(&tx, &row, &group, &job, None).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let wanted = required_work_fields(&group, &schema).unwrap();
        assert_eq!(wanted, BTreeSet::from(["gate".into()]));
        let mut selected = load_group(&tx, &row, &group, &job).await.unwrap();
        assert_eq!(selected.snapshot.work["a"].record.work.fields.len(), 1);
        assert_eq!(
            selected.snapshot.state[&("state".into(), "s".into())]
                .fields
                .len(),
            3
        );
        let work_id = selected.ids[&RecordIdentity {
            collection: None,
            key: "a".into(),
        }];
        let state_id = selected.ids[&RecordIdentity {
            collection: Some("state".into()),
            key: "s".into(),
        }];
        assert_eq!(
            selected_ordinals(
                &[work_id],
                &schema,
                &BTreeMap::from([(work_id, 0), (state_id, 1)]),
                Some(&wanted)
            )
            .unwrap(),
            Some(vec![0])
        );
        let work = &selected.snapshot.work["a"];
        let view = CheckedFields::new(&work.record.work.fields, &work.field_coverage).unwrap();
        assert_eq!(view.get("gate").unwrap(), Some(&FieldValue::Boolean(false)));
        assert!(view.get("target").is_err());
        assert!(view.get("/payload/blob").is_err());
        let owned_tuple = UniqueIdentity {
            collection: None,
            index: "by_target".into(),
            full_sort_key: ordered_key(&[Some(&FieldValue::Text("owned".into()))]).unwrap(),
        };
        load_unique_authority(&tx, &row, &mut selected, vec![owned_tuple.clone()], &job)
            .await
            .unwrap();
        assert_eq!(selected.snapshot.unique_owners[&owned_tuple].key, "a");
        let owners = selected.snapshot.unique_owners.clone();
        let probes = selected.snapshot.unique_probes_loaded.clone();
        assert!(
            load_unique_authority(&tx, &row, &mut selected, vec![owned_tuple], &job)
                .await
                .is_err()
        );
        let missing = UniqueIdentity {
            collection: None,
            index: "by_target".into(),
            full_sort_key: ordered_key(&[Some(&FieldValue::Text("absent".into()))]).unwrap(),
        };
        assert!(
            load_unique_authority(
                &tx,
                &row,
                &mut selected,
                vec![missing.clone(), missing],
                &job
            )
            .await
            .is_err()
        );
        assert_eq!(selected.snapshot.unique_owners, owners);
        assert_eq!(selected.snapshot.unique_probes_loaded, probes);
        let before = selected.snapshot.clone();
        let planning_charge = job
            .buffer(group_expansion_budget(&group, &before, &schema).unwrap())
            .unwrap();
        let delta = plan_commit_group(&group, &mut selected.snapshot, 0, &schema).unwrap();
        let output_charge = job.buffer(encoded_len(&delta).unwrap() * 4).unwrap();
        tx.batch_execute("SAVEPOINT selected_entry").await.unwrap();
        persist_delta(&tx, &row, &delta, &before, &mut selected)
            .await
            .unwrap();
        assert_eq!(
            tx.query_one(
                "SELECT version::text FROM fireweed_api007.records WHERE id=$1",
                &[&work_id]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "2"
        );
        tx.batch_execute("ROLLBACK TO SAVEPOINT selected_entry")
            .await
            .unwrap();
        drop(output_charge);
        drop(planning_charge);
        assert_eq!(
            tx.query_one(
                "SELECT version::text FROM fireweed_api007.records WHERE queue_id=$1 AND key=$2",
                &[&row.id, &b"a".as_slice()]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "1"
        );
        assert!(admission.usage().unwrap().1 < 256 << 10);
        drop(selected);
        let mut two_rows = group.clone();
        let mut other = two_rows.work_updates[0].clone();
        other.key = "b".into();
        two_rows.work_updates.push(other);
        let absent = BTreeSet::from([format!("/payload/{}", "x\0λ".repeat(20_000))]);
        let before_usage = admission.usage().unwrap();
        assert!(matches!(
            load_group_with_fields(&tx, &row, &two_rows, &job, Some(&absent)).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap(), before_usage);
        let mut writer_group = group.clone();
        writer_group.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            fields: BTreeMap::from([
                ("gate".into(), Some(FieldValue::Boolean(true))),
                ("/payload/control\0α".into(), Some(FieldValue::Integer(1))),
            ]),
            ..WorkPatch::default()
        });
        writer_group.work_updates.push(UnclaimedUpdate {
            key: "b".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([
                    ("target".into(), Some(FieldValue::Text("new-owner".into()))),
                    ("/payload/control\0β".into(), Some(FieldValue::Integer(2))),
                ]),
                ..WorkPatch::default()
            }),
        });
        let update_target = |id: &str, key: &str, version, target: &str| CommitGroup {
            administration: None,
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: key.into(),
                expect: StateExpect::Version(version),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([(
                        "target".into(),
                        Some(FieldValue::Text(target.into())),
                    )]),
                    ..WorkPatch::default()
                }),
            }],
        };
        let rejected_rekey = update_target("rejected-rekey", "a", 3, "new-owner");
        let free_and_create = CommitGroup {
            administration: None,
            request_id: "free-create".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "b".into(),
                expect: StateExpect::Version(2),
                change: UnclaimedChange::Delete,
            }],
            work_inserts: vec![NewWork {
                key: "d".into(),
                priority: 0,
                available_at: 0,
                body: Default::default(),
                fields: BTreeMap::from([("target".into(), FieldValue::Text("new-owner".into()))]),
            }],
        };
        let rekey_created = update_target("rekey-created", "d", 1, "final-owner");
        let reuse = update_target("reuse", "a", 3, "new-owner");
        let writer_readset = fireweed_engine::queue_planner::load_envelope(&[
            writer_group.clone(),
            rejected_rekey.clone(),
            free_and_create.clone(),
            rekey_created.clone(),
            reuse.clone(),
        ]);
        let writer_wanted = required_work_fields(&writer_readset, &schema).unwrap();
        let writer_admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let writer_job = writer_admission.reserve(0).unwrap();
        let writer_row = queue_row(&tx, &queue, &writer_job).await.unwrap();
        let mut writer = load_group(&tx, &writer_row, &writer_readset, &writer_job)
            .await
            .unwrap();
        assert!(writer.snapshot.work.values().all(|work| matches!(&work.field_coverage, fireweed_engine::queue_fields::FieldCoverage::Selected { names, .. } if names == &writer_wanted)));
        let writer_before = writer.snapshot.clone();
        let unchanged = tx.query("SELECT record_id,field,ctid::text FROM fireweed_api007.fields WHERE record_id=ANY($1) ORDER BY record_id,field", &[&vec![work_id, writer.ids[&RecordIdentity { collection: None, key: "b".into() }]]]).await.unwrap().into_iter().map(|r| (r.get::<_,i64>(0), r.get::<_,i32>(1), r.get::<_,String>(2))).collect::<Vec<_>>();
        let unchanged_indexes = tx.query("SELECT record_id,index_no,ctid::text FROM fireweed_api007.indexes WHERE queue_id=$1 AND collection=0 ORDER BY record_id,index_no", &[&row.id]).await.unwrap().into_iter().map(|r| (r.get::<_,i64>(0), r.get::<_,i32>(1), r.get::<_,String>(2))).collect::<Vec<_>>();
        let unchanged_extras = tx.query("SELECT id,record_id,name,ctid::text FROM fireweed_api007.additional_fields WHERE queue_id=$1 ORDER BY id", &[&row.id]).await.unwrap().into_iter().map(|r| (r.get::<_,i64>(0), r.get::<_,i64>(1), r.get::<_,Vec<u8>>(2), r.get::<_,String>(3))).collect::<Vec<_>>();
        let unchanged_bodies = tx
            .query(
                "SELECT record_id,ctid::text FROM fireweed_api007.bodies WHERE record_id=ANY($1)",
                &[&vec![
                    work_id,
                    writer.ids[&RecordIdentity {
                        collection: None,
                        key: "b".into(),
                    }],
                ]],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>();
        let noop = CommitGroup {
            administration: None,
            request_id: "noop".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Administrative {
                    patch: WorkPatch {
                        fields: BTreeMap::from([("gate".into(), Some(FieldValue::Boolean(false)))]),
                        ..WorkPatch::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            }],
        };
        load_unique_probes(&tx, &writer_row, &mut writer, &noop, 0, &writer_job)
            .await
            .unwrap();
        let noop_before = writer.snapshot.clone();
        let noop_delta = plan_commit_group(&noop, &mut writer.snapshot, 0, &schema).unwrap();
        assert_eq!(noop_delta.work[0].record.version, 1);
        assert_eq!(noop_delta.work_fields["a"], ResolvedFields::Preserve);
        let noop_output = writer_job
            .buffer(encoded_len(&noop_delta).unwrap() * 4)
            .unwrap();
        persist_delta(&tx, &writer_row, &noop_delta, &noop_before, &mut writer)
            .await
            .unwrap();
        for (id, field, ctid) in &unchanged {
            assert_eq!(
                &tx.query_one(
                    "SELECT ctid::text FROM fireweed_api007.fields WHERE record_id=$1 AND field=$2",
                    &[id, field]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                ctid
            );
        }
        for (id, index, ctid) in &unchanged_indexes {
            assert_eq!(&tx.query_one("SELECT ctid::text FROM fireweed_api007.indexes WHERE record_id=$1 AND index_no=$2", &[id,index]).await.unwrap().get::<_,String>(0), ctid);
        }
        drop(noop_output);
        load_unique_probes(&tx, &writer_row, &mut writer, &writer_group, 0, &writer_job)
            .await
            .unwrap();
        let delta = plan_commit_group(&writer_group, &mut writer.snapshot, 0, &schema).unwrap();
        let output_charge = writer_job.buffer(encoded_len(&delta).unwrap() * 4).unwrap();
        persist_delta(&tx, &writer_row, &delta, &writer_before, &mut writer)
            .await
            .unwrap();
        let b_id = writer.ids[&RecordIdentity {
            collection: None,
            key: "b".into(),
        }];
        for (id, field, ctid) in unchanged {
            if (id == work_id && field == 0) || (id == b_id && field == 1) {
                continue;
            }
            assert_eq!(
                tx.query_one(
                    "SELECT ctid::text FROM fireweed_api007.fields WHERE record_id=$1 AND field=$2",
                    &[&id, &field]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                ctid
            );
        }
        for (id, index, ctid) in unchanged_indexes {
            if (id == work_id && index == 0) || (id == b_id && index == 1) {
                continue;
            }
            assert_eq!(tx.query_one("SELECT ctid::text FROM fireweed_api007.indexes WHERE record_id=$1 AND index_no=$2", &[&id,&index]).await.unwrap().get::<_,String>(0), ctid);
        }
        for (id, record, name, ctid) in unchanged_extras {
            if (record == work_id && name == "/payload/control\0α".as_bytes())
                || (record == b_id && name == "/payload/control\0β".as_bytes())
            {
                continue;
            }
            assert_eq!(
                tx.query_one(
                    "SELECT ctid::text FROM fireweed_api007.additional_fields WHERE id=$1",
                    &[&id]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                ctid
            );
        }
        assert_eq!(
            decode::<FieldValue>(
                &tx.query_one(
                    "SELECT value FROM fireweed_api007.fields WHERE record_id=$1 AND field=0",
                    &[&work_id]
                )
                .await
                .unwrap()
                .get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            FieldValue::Boolean(true)
        );
        assert_eq!(
            decode::<FieldValue>(
                &tx.query_one(
                    "SELECT value FROM fireweed_api007.fields WHERE record_id=$1 AND field=1",
                    &[&b_id]
                )
                .await
                .unwrap()
                .get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            FieldValue::Text("new-owner".into())
        );
        for (id, ctid) in unchanged_bodies {
            assert_eq!(
                tx.query_one(
                    "SELECT ctid::text FROM fireweed_api007.bodies WHERE record_id=$1",
                    &[&id]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                ctid
            );
        }
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&b_id, &"/payload/control\0α".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(10));
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&work_id, &"/payload/control\0β".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(20));
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&work_id, &"/payload/control\0α".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(1));
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&b_id, &"/payload/control\0β".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(2));
        assert_eq!(
            tx.query_one(
                "SELECT full_key FROM fireweed_api007.indexes WHERE record_id=$1 AND index_no=0",
                &[&work_id]
            )
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
            ordered_key(&[Some(&FieldValue::Boolean(true))]).unwrap()
        );
        assert_eq!(
            tx.query_one(
                "SELECT full_key FROM fireweed_api007.indexes WHERE record_id=$1 AND index_no=1",
                &[&b_id]
            )
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
            ordered_key(&[Some(&FieldValue::Text("new-owner".into()))]).unwrap()
        );
        drop(output_charge);
        let remove_only = CommitGroup {
            administration: None,
            request_id: "remove-only".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(2),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("/payload/control\0α".into(), None)]),
                    ..WorkPatch::default()
                }),
            }],
        };
        let remove_before = writer.snapshot.clone();
        load_unique_probes(&tx, &writer_row, &mut writer, &remove_only, 0, &writer_job)
            .await
            .unwrap();
        let remove_delta =
            plan_commit_group(&remove_only, &mut writer.snapshot, 0, &schema).unwrap();
        assert!(
            matches!(&remove_delta.work_fields["a"], ResolvedFields::Edit(edits) if edits.len() == 1 && edits.values().all(Option::is_none))
        );
        let remove_output = writer_job
            .buffer(encoded_len(&remove_delta).unwrap() * 4)
            .unwrap();
        persist_delta(&tx, &writer_row, &remove_delta, &remove_before, &mut writer)
            .await
            .unwrap();
        assert_eq!(tx.query_one("SELECT count(*) FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&work_id, &"/payload/control\0α".as_bytes()]).await.unwrap().get::<_,i64>(0), 0);
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&work_id, &"/payload/control\0β".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(20));
        assert_eq!(decode::<FieldValue>(&tx.query_one("SELECT value FROM fireweed_api007.additional_fields WHERE record_id=$1 AND name=$2", &[&b_id, &"/payload/control\0α".as_bytes()]).await.unwrap().get::<_,Vec<u8>>(0)).unwrap(), FieldValue::Integer(10));
        drop(remove_output);
        let physical_before = writer.snapshot.clone();
        let mut deltas = vec![];
        for (index, operation) in [rejected_rekey, free_and_create, rekey_created, reuse]
            .iter()
            .enumerate()
        {
            load_unique_probes(&tx, &writer_row, &mut writer, operation, 0, &writer_job)
                .await
                .unwrap();
            let owners = writer.snapshot.unique_owners.clone();
            let planned = plan_commit_group(operation, &mut writer.snapshot, 0, &schema).unwrap();
            if index == 0 {
                assert_eq!(
                    planned.receipt.outcome,
                    GroupOutcome::Rejected(Rejection::UniqueConflict)
                );
                assert_eq!(writer.snapshot.unique_owners, owners);
            } else {
                assert!(matches!(
                    planned.receipt.outcome,
                    GroupOutcome::Committed { .. }
                ));
            }
            deltas.push(planned);
        }
        let packed = combine_deltas(&deltas, physical_before.next_sequence).unwrap();
        let packed_charge = writer_job
            .buffer(encoded_len(&packed).unwrap() * 4)
            .unwrap();
        persist_delta(&tx, &writer_row, &packed, &physical_before, &mut writer)
            .await
            .unwrap();
        assert!(
            !tx.query_one(
                "SELECT active FROM fireweed_api007.records WHERE id=$1",
                &[&b_id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
        let d_id = writer.ids[&RecordIdentity {
            collection: None,
            key: "d".into(),
        }];
        assert_eq!(
            tx.query_one(
                "SELECT count(*) FROM fireweed_api007.indexes WHERE record_id=$1",
                &[&d_id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            2
        );
        assert_eq!(
            tx.query_one(
                "SELECT full_key FROM fireweed_api007.indexes WHERE record_id=$1 AND index_no=0",
                &[&d_id]
            )
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
            ordered_key(&[None]).unwrap()
        );
        assert_eq!(
            decode::<FieldValue>(
                &tx.query_one(
                    "SELECT value FROM fireweed_api007.fields WHERE record_id=$1 AND field=1",
                    &[&d_id]
                )
                .await
                .unwrap()
                .get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            FieldValue::Text("final-owner".into())
        );
        assert_eq!(
            decode::<FieldValue>(
                &tx.query_one(
                    "SELECT value FROM fireweed_api007.fields WHERE record_id=$1 AND field=1",
                    &[&work_id]
                )
                .await
                .unwrap()
                .get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            FieldValue::Text("new-owner".into())
        );
        assert!(writer_admission.usage().unwrap().1 < 4 << 20);
        drop(packed_charge);
        // Existing nonempty opaque bytes must become explicitly empty, then a
        // metadata-only patch must preserve the stored empty body's identity.
        assert_eq!(
            tx.query_one(
                "SELECT octet_length(value) FROM fireweed_api007.bodies WHERE record_id=$1",
                &[&work_id]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
            1 << 20
        );
        let mut body_group = CommitGroup {
            administration: None,
            request_id: "empty-body".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(writer.snapshot.work["a"].record.version),
                change: UnclaimedChange::Patch(WorkPatch {
                    body: Some(Default::default()),
                    ..WorkPatch::default()
                }),
            }],
        };
        let body_before = writer.snapshot.clone();
        load_unique_probes(&tx, &writer_row, &mut writer, &body_group, 0, &writer_job)
            .await
            .unwrap();
        let empty_delta = plan_commit_group(&body_group, &mut writer.snapshot, 0, &schema).unwrap();
        assert!(
            matches!(&empty_delta.work_bodies["a"], ResolvedBody::Replace(bytes) if bytes.is_empty())
        );
        let empty_charge = writer_job
            .buffer(encoded_len(&empty_delta).unwrap() * 4)
            .unwrap();
        persist_delta(&tx, &writer_row, &empty_delta, &body_before, &mut writer)
            .await
            .unwrap();
        let body_row = tx
            .query_one(
                "SELECT value,ctid::text FROM fireweed_api007.bodies WHERE record_id=$1",
                &[&work_id],
            )
            .await
            .unwrap();
        assert!(body_row.get::<_, Vec<u8>>(0).is_empty());
        let empty_ctid = body_row.get::<_, String>(1);
        drop(empty_charge);
        body_group.request_id = "empty-body-preserved".into();
        body_group.work_updates[0].expect =
            StateExpect::Version(writer.snapshot.work["a"].record.version);
        body_group.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            priority: Some(77),
            ..WorkPatch::default()
        });
        let metadata_before = writer.snapshot.clone();
        load_unique_probes(&tx, &writer_row, &mut writer, &body_group, 0, &writer_job)
            .await
            .unwrap();
        let metadata_delta =
            plan_commit_group(&body_group, &mut writer.snapshot, 0, &schema).unwrap();
        assert_eq!(metadata_delta.work_bodies["a"], ResolvedBody::Preserve);
        let metadata_charge = writer_job
            .buffer(encoded_len(&metadata_delta).unwrap() * 4)
            .unwrap();
        persist_delta(
            &tx,
            &writer_row,
            &metadata_delta,
            &metadata_before,
            &mut writer,
        )
        .await
        .unwrap();
        assert_eq!(
            tx.query_one(
                "SELECT ctid::text FROM fireweed_api007.bodies WHERE record_id=$1",
                &[&work_id]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            empty_ctid
        );
        drop(metadata_charge);
        drop(writer);
        drop(writer_row);
        // Rollback-scoped corruption: invalid ordinal must not hide payload bytes
        // from complete production-reader preflight.
        let corrupt_id: i64 = tx
            .query_one(
                "SELECT id FROM fireweed_api007.records WHERE queue_id=$1 AND key=$2",
                &[&row.id, &b"c".as_slice()],
            )
            .await
            .unwrap()
            .get(0);
        tx.execute("INSERT INTO fireweed_api007.fields(record_id,queue_id,collection,field,prefix,hash,full_key,value) VALUES($1,$2,0,999,$3,$3,$3,$4)", &[&corrupt_id, &row.id, &vec![0u8], &vec![0u8; 1 << 20]]).await.unwrap();
        let mut corrupt_group = group.clone();
        corrupt_group.state_updates.clear();
        corrupt_group.work_updates[0].key = "c".into();
        corrupt_group.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            priority: Some(1),
            ..WorkPatch::default()
        });
        let error = load_group_with_fields(&tx, &row, &corrupt_group, &job, None)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains("admitted buffer capacity"))
        );
        assert_eq!(admission.usage().unwrap(), before_usage);
        drop(row);
        tx.rollback().await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
        store.shutdown().await.unwrap();
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn fresh_groups_read_prior_sql_writes_and_storage_failure_rolls_back() {
        let _fixture = physical_fixture().await;
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                GROUP_BOUNDARY_TEST.with(|slot| *slot.borrow_mut() = None);
            }
        }
        let _reset = Reset;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let store = PostgresQueueStore::connect(
            &url,
            Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
            QueueLimits::default(),
            1,
        )
        .await
        .unwrap();
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let item = |key: &str, target: &str| NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([("target".into(), FieldValue::Text(target.into()))]),
            body: Default::default(),
        };
        let group = |id: &str| CommitGroup {
            administration: None,
            request_id: id.into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        };
        let state = |expect: StateExpect, value: u8| StateUpdate {
            collection: "instances".into(),
            key: "state".into(),
            expect,
            change: StateChange::Put {
                fields: Fields::new(),
                value: vec![value].into(),
            },
        };
        let mut first = group("first");
        first.work_inserts.push(item("x", "alpha"));
        first.state_updates.push(state(StateExpect::Absent, 1));
        let mut stale = group("stale");
        stale
            .state_updates
            .push(state(StateExpect::Version(999), 99));
        stale.work_inserts.push(item("never", "never"));
        let mut rekey = group("rekey");
        rekey.work_updates.push(UnclaimedUpdate {
            key: "x".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("target".into(), Some(FieldValue::Text("beta".into())))]),
                ..Default::default()
            }),
        });
        rekey.work_inserts.push(item("y", "alpha"));
        rekey.state_updates.push(state(StateExpect::Version(1), 2));
        let mut delete = group("delete");
        delete.work_updates.push(UnclaimedUpdate {
            key: "x".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Delete,
        });
        delete.state_updates.push(state(StateExpect::Version(2), 3));
        let mut recreate = group("recreate");
        recreate.work_inserts.push(item("x", "gamma"));
        let groups = vec![first, stale, rekey, delete, recreate];
        let expected = vec![
            vec![("x".into(), 1, true, 1)],
            vec![("x".into(), 1, true, 1)],
            vec![("x".into(), 2, true, 1), ("y".into(), 1, true, 2)],
            vec![("x".into(), 3, false, 0), ("y".into(), 1, true, 2)],
            vec![("x".into(), 4, true, 3), ("y".into(), 1, true, 2)],
        ];
        for fail in [true, false] {
            let queue = QueueAddress {
                tenant: "api007-group-sql".into(),
                queue: format!("{nonce}-{fail}"),
            };
            let mut schema = fireweed_conformance::queue_contract::schema();
            schema.work.fields.insert("target".into(), FieldType::Text);
            schema
                .work
                .indexes
                .insert("target".into(), vec!["target".into()]);
            schema.work.unique_indexes.insert("target".into());
            store
                .provision(ProvisionRequest {
                    queue: queue.clone(),
                    schema,
                })
                .await
                .unwrap();
            let queue_id: i64 = client
                .query_one(
                    "SELECT id FROM fireweed_api007.queues WHERE tenant=$1 AND name=$2",
                    &[&queue.tenant.as_bytes(), &queue.queue.as_bytes()],
                )
                .await
                .unwrap()
                .get(0);
            GROUP_BOUNDARY_TEST.with(|slot| {
                *slot.borrow_mut() = Some(GroupBoundaryTest {
                    queue_id,
                    expected: expected.clone(),
                    receipt_offset: 0,
                    visits: 0,
                    fail_at: fail.then_some(2),
                })
            });
            let request = CommitBatch {
                queue: queue.clone(),
                groups: groups.clone(),
            };
            let outcome = store.commit(request.clone()).await;
            if fail {
                assert!(matches!(outcome, Err(QueueError::OutcomeUnknown(_))));
                assert_eq!(
                    GROUP_BOUNDARY_TEST.with(|slot| slot.borrow().as_ref().unwrap().visits),
                    3
                );
                let rows = client.query_one("SELECT (SELECT COUNT(*) FROM fireweed_api007.records WHERE queue_id=$1), (SELECT COUNT(*) FROM fireweed_api007.requests WHERE queue_id=$1), next_sequence::text FROM fireweed_api007.queues WHERE id=$1", &[&queue_id]).await.unwrap();
                assert_eq!(rows.get::<_, i64>(0), 0);
                assert_eq!(rows.get::<_, i64>(1), 0);
                assert_eq!(rows.get::<_, String>(2), "0");
                assert!(
                    store
                        .read_commit(ReceiptReadRequest {
                            queue: queue.clone(),
                            request_ids: groups.iter().map(|g| g.request_id.clone()).collect(),
                            max_bytes: 1 << 20
                        })
                        .await
                        .unwrap()
                        .iter()
                        .all(|r| *r == ReceiptLookup::Absent)
                );
                GROUP_BOUNDARY_TEST.with(|slot| {
                    let mut slot = slot.borrow_mut();
                    let probe = slot.as_mut().unwrap();
                    probe.fail_at = None;
                    probe.visits = 0;
                });
            }
            let receipts = if fail {
                store.commit(request.clone()).await.unwrap()
            } else {
                outcome.unwrap()
            };
            assert_eq!(
                GROUP_BOUNDARY_TEST.with(|slot| slot.borrow().as_ref().unwrap().visits),
                5
            );
            assert_eq!(
                receipts[1].outcome,
                GroupOutcome::Rejected(Rejection::VersionConflict)
            );
            for index in [0, 2, 3, 4] {
                assert!(matches!(
                    receipts[index].outcome,
                    GroupOutcome::Committed { .. }
                ));
            }
            let mut reverse = request.clone();
            reverse.groups.reverse();
            let replay = store.commit(reverse).await.unwrap();
            assert_eq!(replay, receipts.into_iter().rev().collect::<Vec<_>>());
            assert_eq!(
                GROUP_BOUNDARY_TEST.with(|slot| slot.borrow().as_ref().unwrap().visits),
                5
            );
            let states = store
                .read_state(StateReadRequest {
                    queue: queue.clone(),
                    collection: "instances".into(),
                    keys: vec!["state".into()],
                    max_bytes: 1 << 20,
                })
                .await
                .unwrap();
            assert_eq!(states[0].as_ref().unwrap().version, 3);
            assert_eq!(states[0].as_ref().unwrap().value.as_ref(), &[3]);
            GROUP_BOUNDARY_TEST.with(|slot| *slot.borrow_mut() = None);
            let mut final_group = group("after-replay");
            final_group.work_inserts.push(item("z", "delta"));
            store
                .commit(CommitBatch {
                    queue,
                    groups: vec![final_group],
                })
                .await
                .unwrap();
            let sequence: String = client.query_one("SELECT insertion_sequence::text FROM fireweed_api007.records WHERE queue_id=$1 AND key=$2", &[&queue_id, &b"z".as_slice()]).await.unwrap().get(0);
            assert_eq!(sequence, "4");
        }
        store.shutdown().await.unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn admitted_caller_cancellation_drains_and_reopens_complete_group() {
        let _fixture = physical_fixture().await;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let store = PostgresQueueStore::connect_with_runtime(
            &url,
            clock.clone(),
            QueueLimits::default(),
            1,
            fireweed_core::queue_runtime::QueueRuntimeOptions {
                operations: 1,
                scratch_workers: Some(1),
                ..Default::default()
            },
        )
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
            administration: None,
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
        assert_eq!(
            store
                .clone()
                .read_commit(ReceiptReadRequest {
                    queue: queue.clone(),
                    request_ids: vec!["owned-commit".into()],
                    max_bytes: 1024,
                })
                .await,
            Err(QueueError::Busy)
        );

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

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn full_values_disambiguate_all_discovery_hash_collisions() {
        let _fixture = physical_fixture().await;
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                COLLIDING_PROBES.set(false);
            }
        }
        COLLIDING_PROBES.set(true);
        let _restore = Restore;
        let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let store = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 2)
            .await
            .unwrap();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let q = |tenant: &str, name: &str| QueueAddress {
            tenant: format!("hash-collision-{nonce}-{tenant}"),
            queue: name.into(),
        };
        fireweed_conformance::queue_contract::numeric_bucket_stats(
            &store,
            &clock,
            q("left", "buckets"),
        )
        .await;
        let left = q("left", "same-name");
        let right = q("right", "same-name");
        let another = q("left", "other-name");
        // Same logical keys/request identities/index values live in distinct
        // tenant/name authorities despite every physical probe sharing one hash.
        fireweed_conformance::queue_contract::run(&store, left).await;
        fireweed_conformance::queue_contract::run(&store, right).await;
        fireweed_conformance::queue_contract::run(&store, another).await;
        fireweed_conformance::queue_contract::ordered_insert_uniqueness(
            &store,
            q("left", "uniqueness"),
        )
        .await;
        fireweed_conformance::queue_contract::predicate_and_order_oracles(
            &store,
            q("left", "predicates"),
        )
        .await;
        fireweed_conformance::queue_contract::ordered_long_values_and_cursor_scope(
            &store,
            q("left", "long-values"),
        )
        .await;
        fireweed_conformance::queue_contract::indexed_additional_attributes(
            &store,
            q("left", "additional-attributes"),
        )
        .await;
        store.shutdown().await.unwrap();
        // Reopening cannot consult a process identity cache for disambiguation.
        let reopened = PostgresQueueStore::connect(&url, clock, QueueLimits::default(), 1)
            .await
            .unwrap();
        for address in [
            q("left", "same-name"),
            q("right", "same-name"),
            q("left", "other-name"),
        ] {
            let state = reopened
                .read_state(StateReadRequest {
                    queue: address,
                    collection: "instances".into(),
                    keys: vec!["instance".into()],
                    max_bytes: 1 << 20,
                })
                .await
                .unwrap();
            assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
        }
        reopened.shutdown().await.unwrap();
    }
    #[tokio::test]
    #[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
    async fn reopening_schema_fences_writers_before_dependent_tables() {
        let _fixture = physical_fixture().await;
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
        let _fixture = physical_fixture().await;
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
        let mut stats = StatsRequest {
            queue: QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            },
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 0,
            buckets: Some(NumericBucketDimension {
                field: "score".into(),
                missing_label: "missing".into(),
                buckets: (0..51)
                    .map(|i| NumericBucket {
                        label: "b".into(),
                        lower_inclusive: FieldValue::Integer(i),
                        upper_exclusive: FieldValue::Integer(i + 1),
                    })
                    .collect(),
            }),
        };
        assert!(matches!(
            store.stats(stats.clone()).await,
            Err(QueueError::Invalid(_))
        ));
        stats.buckets.as_mut().unwrap().buckets.pop();
        assert_eq!(store.stats(stats).await, Err(QueueError::Busy));
        drop(charges);
        let byte_charge = store.inner.admission.reserve(65_536).unwrap();
        // An otherwise-valid 50-bin request fits its wire/result allowance but
        // its SQL scratch cannot fit the remaining 64 KiB. Reject before lookup.
        let stats = StatsRequest {
            queue: QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            },
            filter: Filter::default(),
            group_by: vec![],
            eligible_at: 0,
            buckets: Some(NumericBucketDimension {
                field: "score".into(),
                missing_label: "missing".into(),
                buckets: (0..50)
                    .map(|i| NumericBucket {
                        label: "b".into(),
                        lower_inclusive: FieldValue::Integer(i),
                        upper_exclusive: FieldValue::Integer(i + 1),
                    })
                    .collect(),
            }),
        };
        assert_eq!(store.stats(stats).await, Err(QueueError::Busy));
        drop(byte_charge);
        store.shutdown().await.unwrap();
    }
}

async fn materialized_headers(
    tx: &Transaction<'_>,
    ids: &[i64],
    selected: Option<&FieldSelection>,
) -> QueueResult<u64> {
    let declared = selected.map(|nos| nos.declared.clone());
    let additional = selected.map(|nos| nos.additional.clone());
    let hashes = selected.map(|nos| nos.hashes.clone());
    let fields=u64::try_from(tx.query_one("SELECT count(*) FROM fireweed_api007.fields WHERE record_id=ANY($1) AND ($2::int[] IS NULL OR field=ANY($2))",&[&ids,&declared]).await.map_err(storage)?.get::<_,i64>(0)).map_err(storage)?;
    let extras=u64::try_from(tx.query_one("SELECT count(*) FROM fireweed_api007.additional_fields WHERE record_id=ANY($1) AND ($2::bytea[] IS NULL OR (name_hash=ANY($2) AND name=ANY($3)))",&[&ids,&hashes,&additional]).await.map_err(storage)?.get::<_,i64>(0)).map_err(storage)?;
    fields
        .checked_add(extras)
        .ok_or(QueueError::TooLarge)?
        .checked_mul(256)
        .and_then(|n| n.checked_add((ids.len() as u64).checked_mul(1024)?))
        .ok_or(QueueError::TooLarge)
}

impl PersistedStructure for ClaimReplay {
    fn validate_persisted(&self) -> QueueResult<()> {
        if self
            .items
            .iter()
            .any(|(id, token)| *id <= 0 || token.is_empty())
        {
            return Err(storage("invalid persisted claim replay"));
        }
        Ok(())
    }
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
