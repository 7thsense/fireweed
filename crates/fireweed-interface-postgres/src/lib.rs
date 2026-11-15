//! PostgreSQL implementation of the Fireweed data-plane interface
//! (API-006), first increment.
//!
//! Design of this increment:
//!
//! - Records live in PostgreSQL tables (`fw_spaces`, `fw_queue_meta`,
//!   `fw_items`, `fw_rows`, `fw_row_hwm`, `fw_requests`), one row per item,
//!   table row and request record.
//! - Every write request is one transaction. It locks the space's row
//!   (`SELECT … FOR UPDATE`), so writes to one space are serialized across
//!   every process and instance (`MultiProcess`). Request time is
//!   non-decreasing per space because it is stored on that row.
//! - The transaction loads the objects the request references, runs the
//!   shared request logic of `fireweed-interface-engine` (the same code and
//!   reference evaluator as the in-memory implementation), and writes back
//!   the records that changed. Request records are written in the same
//!   transaction, so a rejected group's record is atomic with its outcome.
//! - Reads run in a `REPEATABLE READ` read-only transaction.
//!
//! Not yet done, and required before production use: loading only the
//! records a request touches (today a request loads whole queues and
//! tables), pushing access terms and claims into indexed SQL (`FOR UPDATE
//! SKIP LOCKED`), the `SERIALIZABLE` group-commit protocol of API-006
//! §PostgreSQL in place of the per-space lock, database time as the
//! production authority, and `Replicated` durability. Event logs, blobs,
//! cohorts, group batching and queue history are reported unsupported
//! pending the owner's rulings.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_interface::*;
use fireweed_interface_engine::handlers::{self, Profile};
use fireweed_interface_engine::{Item, KeyVal, Queue, Record, Row, Space, St, Table};
use postgres::{Client, NoTls, Transaction};

const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS fw_spaces (
  tenant text NOT NULL, space text NOT NULL, schema text NOT NULL,
  next_item_id bigint NOT NULL, next_token bigint NOT NULL, last_time bigint NOT NULL,
  PRIMARY KEY (tenant, space));
CREATE TABLE IF NOT EXISTS fw_queue_meta (
  tenant text NOT NULL, space text NOT NULL, queue text NOT NULL, data text NOT NULL,
  PRIMARY KEY (tenant, space, queue));
CREATE TABLE IF NOT EXISTS fw_items (
  tenant text NOT NULL, space text NOT NULL, queue text NOT NULL, item_id bigint NOT NULL,
  state text NOT NULL, data text NOT NULL,
  PRIMARY KEY (tenant, space, queue, item_id));
CREATE TABLE IF NOT EXISTS fw_rows (
  tenant text NOT NULL, space text NOT NULL, tbl text NOT NULL, key text COLLATE "C" NOT NULL,
  data text NOT NULL, PRIMARY KEY (tenant, space, tbl, key));
CREATE TABLE IF NOT EXISTS fw_row_hwm (
  tenant text NOT NULL, space text NOT NULL, tbl text NOT NULL, key text COLLATE "C" NOT NULL,
  version bigint NOT NULL, at bigint NOT NULL, PRIMARY KEY (tenant, space, tbl, key));
CREATE TABLE IF NOT EXISTS fw_requests (
  tenant text NOT NULL, space text NOT NULL, request_id text COLLATE "C" NOT NULL,
  recorded_at bigint NOT NULL, data text NOT NULL,
  PRIMARY KEY (tenant, space, request_id));
"#;

const PROFILE: Profile = Profile {
    durability: DurabilityLevel::Local,
    max_durability: DurabilityLevel::Local,
};

pub struct PostgresDataPlane {
    clock: Arc<dyn Clock>,
    limits: ImplementationLimits,
    client: Mutex<Client>,
    /// For isolated test instances: the schema to drop, and how to connect.
    cleanup: Option<(String, String)>,
}

fn unavailable(e: postgres::Error) -> DataPlaneError {
    DataPlaneError::Unavailable(e.to_string())
}

fn corrupt(e: impl std::fmt::Display) -> DataPlaneError {
    DataPlaneError::Internal(format!("stored record does not decode: {e}"))
}

fn ser<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("records serialize")
}

/// Queue state other than items, as stored in `fw_queue_meta`.
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct QueueMeta {
    paused: bool,
    drain_intake: bool,
    blocked: BTreeMap<String, u64>,
    gate_epoch: u64,
}

fn meta_of(q: &Queue) -> QueueMeta {
    QueueMeta {
        paused: q.paused,
        drain_intake: q.drain_intake,
        blocked: q.blocked.clone(),
        gate_epoch: q.gate_epoch,
    }
}

fn state_name(s: St) -> &'static str {
    match s {
        St::Pending => "pending",
        St::Leased => "leased",
        St::Complete => "complete",
        St::Failed => "failed",
        St::Tombstone => "tombstone",
        St::Superseded => "superseded",
    }
}

/// How much of each object to load.
#[derive(Clone, Copy, PartialEq)]
enum Load {
    Full,
    MetaOnly,
}

impl PostgresDataPlane {
    /// Connects and creates the tables if needed.
    pub fn connect(
        url: &str,
        clock: Arc<dyn Clock>,
        limits: ImplementationLimits,
    ) -> DpResult<Self> {
        let mut client = Client::connect(url, NoTls).map_err(unavailable)?;
        client.batch_execute(DDL).map_err(unavailable)?;
        Ok(Self {
            clock,
            limits,
            client: Mutex::new(client),
            cleanup: None,
        })
    }

    /// Connects with `search_path` set to an existing PostgreSQL schema.
    pub fn connect_in_schema(
        url: &str,
        schema: &str,
        clock: Arc<dyn Clock>,
        limits: ImplementationLimits,
    ) -> DpResult<Self> {
        if !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(DataPlaneError::InvalidRequest("schema name".into()));
        }
        let mut cfg = postgres::Config::from_str(url).map_err(unavailable)?;
        cfg.options(&format!("-c search_path={schema}"));
        let mut client = cfg.connect(NoTls).map_err(unavailable)?;
        client.batch_execute(DDL).map_err(unavailable)?;
        Ok(Self {
            clock,
            limits,
            client: Mutex::new(client),
            cleanup: None,
        })
    }

    /// Connects in a fresh, private PostgreSQL schema that is dropped when
    /// the instance is dropped. For tests.
    pub fn isolated(
        url: &str,
        clock: Arc<dyn Clock>,
        limits: ImplementationLimits,
    ) -> DpResult<Self> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let schema = format!("fw_test_{}_{n}", std::process::id());
        let mut admin = Client::connect(url, NoTls).map_err(unavailable)?;
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .map_err(unavailable)?;
        let mut cfg = postgres::Config::from_str(url).map_err(unavailable)?;
        cfg.options(&format!("-c search_path={schema}"));
        let mut client = cfg.connect(NoTls).map_err(unavailable)?;
        client.batch_execute(DDL).map_err(unavailable)?;
        Ok(Self {
            clock,
            limits,
            client: Mutex::new(client),
            cleanup: Some((schema, url.to_owned())),
        })
    }

    fn ready<T: Send + 'static>(r: DpResult<T>) -> DpFuture<'static, T> {
        Box::pin(std::future::ready(r))
    }

    fn now(&self) -> i64 {
        self.clock.now().0
    }

    /// Loads the space row and the named objects. `lock` takes the space
    /// row lock that serializes writers.
    fn load(
        tx: &mut Transaction<'_>,
        key: &SpaceKey,
        objects: &BTreeSet<ObjectName>,
        load: Load,
        request: Option<&RequestId>,
        lock: bool,
    ) -> DpResult<Space> {
        let sql = if lock {
            "SELECT schema, next_item_id, next_token, last_time FROM fw_spaces WHERE tenant=$1 AND space=$2 FOR UPDATE"
        } else {
            "SELECT schema, next_item_id, next_token, last_time FROM fw_spaces WHERE tenant=$1 AND space=$2"
        };
        let (t, s) = (key.tenant_id.as_str(), key.space_id.as_str());
        let row = tx
            .query_opt(sql, &[&t, &s])
            .map_err(unavailable)?
            .ok_or(DataPlaneError::SpaceNotFound)?;
        let schema: SpaceSchema = serde_json::from_str(row.get::<_, &str>(0)).map_err(corrupt)?;
        let mut space = Space {
            schema: schema.clone(),
            queues: BTreeMap::new(),
            tables: BTreeMap::new(),
            records: BTreeMap::new(),
            next_item_id: row.get::<_, i64>(1) as u64,
            next_token: row.get::<_, i64>(2) as u64,
            last_time: row.get(3),
        };
        for name in objects {
            if let Some(decl) = schema.queues.iter().find(|q| &q.name == name) {
                let mut q = handlers::empty_queue(decl);
                if let Some(m) = tx
                    .query_opt(
                        "SELECT data FROM fw_queue_meta WHERE tenant=$1 AND space=$2 AND queue=$3",
                        &[&t, &s, &name.as_str()],
                    )
                    .map_err(unavailable)?
                {
                    let meta: QueueMeta =
                        serde_json::from_str(m.get::<_, &str>(0)).map_err(corrupt)?;
                    q.paused = meta.paused;
                    q.drain_intake = meta.drain_intake;
                    q.blocked = meta.blocked;
                    q.gate_epoch = meta.gate_epoch;
                }
                if load == Load::Full {
                    for r in tx
                        .query(
                            "SELECT data FROM fw_items WHERE tenant=$1 AND space=$2 AND queue=$3 ORDER BY item_id",
                            &[&t, &s, &name.as_str()],
                        )
                        .map_err(unavailable)?
                    {
                        let it: Item = serde_json::from_str(r.get::<_, &str>(0)).map_err(corrupt)?;
                        q.items.insert(it.id, it);
                    }
                    handlers::rebuild_keys(&mut q);
                }
                space.queues.insert(name.clone(), q);
            } else if let Some(decl) = schema.tables.iter().find(|tb| &tb.name == name) {
                let mut tb = handlers::empty_table(decl);
                if load == Load::Full {
                    for r in tx
                        .query(
                            "SELECT key, data FROM fw_rows WHERE tenant=$1 AND space=$2 AND tbl=$3",
                            &[&t, &s, &name.as_str()],
                        )
                        .map_err(unavailable)?
                    {
                        let k: Vec<KeyVal> =
                            serde_json::from_str(r.get::<_, &str>(0)).map_err(corrupt)?;
                        let row: Row =
                            serde_json::from_str(r.get::<_, &str>(1)).map_err(corrupt)?;
                        tb.rows.insert(k, row);
                    }
                    for r in tx
                        .query(
                            "SELECT key, version, at FROM fw_row_hwm WHERE tenant=$1 AND space=$2 AND tbl=$3",
                            &[&t, &s, &name.as_str()],
                        )
                        .map_err(unavailable)?
                    {
                        let k: Vec<KeyVal> = serde_json::from_str(r.get::<_, &str>(0)).map_err(corrupt)?;
                        tb.hwm.insert(k, (r.get::<_, i64>(1) as u64, r.get(2)));
                    }
                }
                space.tables.insert(name.clone(), tb);
            }
        }
        if let Some(rid) = request
            && let Some(r) = tx
                .query_opt(
                    "SELECT data FROM fw_requests WHERE tenant=$1 AND space=$2 AND request_id=$3",
                    &[&t, &s, &rid.as_str()],
                )
                .map_err(unavailable)?
        {
            let rec: Record = serde_json::from_str(r.get::<_, &str>(0)).map_err(corrupt)?;
            space.records.insert(rid.clone(), rec);
        }
        Ok(space)
    }

    /// Writes back every loaded record that changed.
    fn store(
        tx: &mut Transaction<'_>,
        key: &SpaceKey,
        before: &Space,
        after: &Space,
        t_now: i64,
    ) -> DpResult<()> {
        let (t, s) = (key.tenant_id.as_str(), key.space_id.as_str());
        tx.execute(
            "UPDATE fw_spaces SET schema=$3, next_item_id=$4, next_token=$5, last_time=$6 WHERE tenant=$1 AND space=$2",
            &[&t, &s, &ser(&after.schema), &(after.next_item_id as i64), &(after.next_token as i64), &after.last_time],
        )
        .map_err(unavailable)?;
        for (name, q) in &after.queues {
            let old = before.queues.get(name);
            if old.is_none_or(|o| meta_of(o) != meta_of(q)) {
                tx.execute(
                    "INSERT INTO fw_queue_meta (tenant, space, queue, data) VALUES ($1,$2,$3,$4)
                     ON CONFLICT (tenant, space, queue) DO UPDATE SET data = EXCLUDED.data",
                    &[&t, &s, &name.as_str(), &ser(&meta_of(q))],
                )
                .map_err(unavailable)?;
            }
            store_items(tx, (t, s, name.as_str()), old, q)?;
        }
        for (name, tb) in &after.tables {
            store_rows(tx, (t, s, name.as_str()), before.tables.get(name), tb)?;
        }
        for (rid, rec) in &after.records {
            if before.records.get(rid) != Some(rec) {
                tx.execute(
                    "INSERT INTO fw_requests (tenant, space, request_id, recorded_at, data) VALUES ($1,$2,$3,$4,$5)
                     ON CONFLICT (tenant, space, request_id) DO UPDATE SET recorded_at = EXCLUDED.recorded_at, data = EXCLUDED.data",
                    &[&t, &s, &rid.as_str(), &rec.recorded_at, &ser(rec)],
                )
                .map_err(unavailable)?;
            }
        }
        // Request-record retention (time-driven).
        let cutoff = t_now - after.schema.request_id_retention_ms as i64;
        tx.execute(
            "DELETE FROM fw_requests WHERE tenant=$1 AND space=$2 AND recorded_at <= $3",
            &[&t, &s, &cutoff],
        )
        .map_err(unavailable)?;
        Ok(())
    }

    /// Runs a write request in one transaction under the space lock.
    fn write<T>(
        &self,
        key: &SpaceKey,
        objects: BTreeSet<ObjectName>,
        request: Option<&RequestId>,
        f: impl FnOnce(&mut Space, i64) -> DpResult<T>,
    ) -> DpResult<T> {
        let mut client = self.client.lock().expect("lock");
        let mut tx = client.transaction().map_err(unavailable)?;
        let mut space = Self::load(&mut tx, key, &objects, Load::Full, request, true)?;
        let before = space.clone();
        let t = handlers::begin(&mut space, self.now());
        let out = f(&mut space, t)?;
        Self::store(&mut tx, key, &before, &space, t)?;
        tx.commit().map_err(unavailable)?;
        Ok(out)
    }

    /// Runs a read in a `REPEATABLE READ` read-only snapshot.
    fn read_tx<T>(
        &self,
        key: &SpaceKey,
        objects: impl FnOnce(&SpaceSchema) -> BTreeSet<ObjectName>,
        request: Option<&RequestId>,
        f: impl FnOnce(&Space, i64) -> DpResult<T>,
    ) -> DpResult<T> {
        let mut client = self.client.lock().expect("lock");
        let mut tx = client
            .build_transaction()
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(unavailable)?;
        let schema_only = Self::load(&mut tx, key, &BTreeSet::new(), Load::Full, None, false)?;
        let objs = objects(&schema_only.schema);
        let mut space = Self::load(&mut tx, key, &objs, Load::Full, request, false)?;
        let t = handlers::begin(&mut space, self.now());
        let out = f(&space, t)?;
        tx.commit().map_err(unavailable)?;
        Ok(out)
    }
}

type Obj<'a> = (&'a str, &'a str, &'a str);

fn store_items(
    tx: &mut Transaction<'_>,
    (t, s, q): Obj<'_>,
    old: Option<&Queue>,
    new: &Queue,
) -> DpResult<()> {
    for (id, it) in &new.items {
        if old.and_then(|o| o.items.get(id)) != Some(it) {
            tx.execute(
                "INSERT INTO fw_items (tenant, space, queue, item_id, state, data) VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (tenant, space, queue, item_id) DO UPDATE SET state = EXCLUDED.state, data = EXCLUDED.data",
                &[&t, &s, &q, &(*id as i64), &state_name(it.state), &ser(it)],
            )
            .map_err(unavailable)?;
        }
    }
    if let Some(o) = old {
        for id in o.items.keys().filter(|id| !new.items.contains_key(id)) {
            tx.execute(
                "DELETE FROM fw_items WHERE tenant=$1 AND space=$2 AND queue=$3 AND item_id=$4",
                &[&t, &s, &q, &(*id as i64)],
            )
            .map_err(unavailable)?;
        }
    }
    Ok(())
}

fn store_rows(
    tx: &mut Transaction<'_>,
    (t, s, tbl): Obj<'_>,
    old: Option<&Table>,
    new: &Table,
) -> DpResult<()> {
    for (k, row) in &new.rows {
        if old.and_then(|o| o.rows.get(k)) != Some(row) {
            tx.execute(
                "INSERT INTO fw_rows (tenant, space, tbl, key, data) VALUES ($1,$2,$3,$4,$5)
                 ON CONFLICT (tenant, space, tbl, key) DO UPDATE SET data = EXCLUDED.data",
                &[&t, &s, &tbl, &ser(k), &ser(row)],
            )
            .map_err(unavailable)?;
        }
    }
    for (k, (v, at)) in &new.hwm {
        if old.and_then(|o| o.hwm.get(k)) != Some(&(*v, *at)) {
            tx.execute(
                "INSERT INTO fw_row_hwm (tenant, space, tbl, key, version, at) VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (tenant, space, tbl, key) DO UPDATE SET version = EXCLUDED.version, at = EXCLUDED.at",
                &[&t, &s, &tbl, &ser(k), &(*v as i64), at],
            )
            .map_err(unavailable)?;
        }
    }
    if let Some(o) = old {
        for k in o.rows.keys().filter(|k| !new.rows.contains_key(*k)) {
            tx.execute(
                "DELETE FROM fw_rows WHERE tenant=$1 AND space=$2 AND tbl=$3 AND key=$4",
                &[&t, &s, &tbl, &ser(k)],
            )
            .map_err(unavailable)?;
        }
        for k in o.hwm.keys().filter(|k| !new.hwm.contains_key(*k)) {
            tx.execute(
                "DELETE FROM fw_row_hwm WHERE tenant=$1 AND space=$2 AND tbl=$3 AND key=$4",
                &[&t, &s, &tbl, &ser(k)],
            )
            .map_err(unavailable)?;
        }
    }
    Ok(())
}

impl Drop for PostgresDataPlane {
    fn drop(&mut self) {
        if let Some((schema, url)) = self.cleanup.take()
            && let Ok(mut admin) = Client::connect(&url, NoTls)
        {
            let _ = admin.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        }
    }
}

fn one<T>(x: T) -> BTreeSet<T>
where
    T: Ord,
{
    [x].into_iter().collect()
}

impl DataPlane for PostgresDataPlane {
    fn guarantees(&self) -> Guarantees {
        Guarantees {
            durability: DurabilityLevel::Local,
            new_work_visibility: Visibility::Immediate,
            sharing: Sharing::MultiProcess,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            max_durability: DurabilityLevel::Local,
            shapes: [ShapeKind::Queue, ShapeKind::Table].into_iter().collect(),
            queue_features: [
                QueueFeature::DynamicGates,
                QueueFeature::MetadataBlockers,
                QueueFeature::Recurrence,
            ]
            .into_iter()
            .collect(),
            queue_history: false,
            priority_kinds: [PriorityKind::Int64, PriorityKind::Timestamp]
                .into_iter()
                .collect(),
            ordering_modes: [OrderingMode::Strict].into_iter().collect(),
            limits: self.limits,
        }
    }

    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse> {
        Self::ready((|| {
            let caps = self.capabilities();
            let mut client = self.client.lock().expect("lock");
            let mut tx = client.transaction().map_err(unavailable)?;
            let (t, s) = (r.space.tenant_id.as_str(), r.space.space_id.as_str());
            let exists = tx
                .query_opt(
                    "SELECT 1 FROM fw_spaces WHERE tenant=$1 AND space=$2 FOR UPDATE",
                    &[&t, &s],
                )
                .map_err(unavailable)?
                .is_some();
            if !exists {
                let (resp, new) = handlers::apply_schema(None, &r, &caps, &PROFILE, self.now())?;
                let space = new.expect("new space");
                tx.execute(
                    "INSERT INTO fw_spaces (tenant, space, schema, next_item_id, next_token, last_time) VALUES ($1,$2,$3,0,0,$4)",
                    &[&t, &s, &ser(&space.schema), &space.last_time],
                )
                .map_err(unavailable)?;
                let empty = Space {
                    queues: BTreeMap::new(),
                    tables: BTreeMap::new(),
                    ..space.clone()
                };
                Self::store(&mut tx, &r.space, &empty, &space, space.last_time)?;
                tx.commit().map_err(unavailable)?;
                return Ok(resp);
            }
            let names: BTreeSet<ObjectName> = {
                let probe = Self::load(
                    &mut tx,
                    &r.space,
                    &BTreeSet::new(),
                    Load::MetaOnly,
                    None,
                    true,
                )?;
                probe
                    .schema
                    .queues
                    .iter()
                    .map(|q| q.name.clone())
                    .chain(probe.schema.tables.iter().map(|t| t.name.clone()))
                    .collect()
            };
            let mut space = Self::load(&mut tx, &r.space, &names, Load::MetaOnly, None, true)?;
            let before = space.clone();
            let (resp, _) =
                handlers::apply_schema(Some(&mut space), &r, &caps, &PROFILE, self.now())?;
            Self::store(&mut tx, &r.space, &before, &space, space.last_time)?;
            tx.commit().map_err(unavailable)?;
            Ok(resp)
        })())
    }

    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>> {
        Self::ready(
            match self.read_tx(
                &space,
                |_| BTreeSet::new(),
                None,
                |s, _| Ok(handlers::schema_state(s)),
            ) {
                Ok(s) => Ok(Some(s)),
                Err(DataPlaneError::SpaceNotFound) => Ok(None),
                Err(e) => Err(e),
            },
        )
    }

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse> {
        Self::ready(self.write(
            &r.header.space,
            one(r.queue.clone()),
            Some(&r.header.request_id),
            |sp, t| handlers::enqueue(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse> {
        Self::ready(self.write(
            &r.header.space,
            one(r.queue.clone()),
            Some(&r.header.request_id),
            |sp, t| handlers::claim(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse> {
        Self::ready(self.write(
            &r.header.space,
            one(r.queue.clone()),
            Some(&r.header.request_id),
            |sp, t| handlers::claim_by_item_ids(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse> {
        Self::ready(self.write(
            &r.header.space,
            one(r.queue.clone()),
            Some(&r.header.request_id),
            |sp, t| handlers::renew(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse> {
        Self::ready(self.write(
            &r.header.space,
            one(r.queue.clone()),
            Some(&r.header.request_id),
            |sp, t| handlers::finalize(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse> {
        Self::ready(self.write(
            &r.header.space,
            handlers::commit_objects(&r),
            Some(&r.header.request_id),
            |sp, t| handlers::commit(sp, t, &self.limits, &PROFILE, &r),
        ))
    }

    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>> {
        Self::ready(self.read_tx(
            &r.space,
            |_| BTreeSet::new(),
            Some(&r.request_id),
            |sp, _| Ok(handlers::get_request(sp, &r)),
        ))
    }

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse> {
        Self::ready(self.read_tx(
            &r.space,
            |_| handlers::get_objects(&r),
            None,
            |sp, t| handlers::get(sp, t, &r),
        ))
    }

    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse> {
        Self::ready(self.read_tx(
            &r.space,
            |s| handlers::read_objects(s, &r),
            None,
            |sp, t| handlers::read(sp, t, &self.limits, &r),
        ))
    }

    fn read_log(&self, _r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::EventLog,
        ))))
    }

    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics> {
        Self::ready(self.read_tx(
            &r.space,
            |_| one(r.queue.clone()),
            None,
            |sp, t| handlers::metrics(sp, t, &r),
        ))
    }

    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse> {
        Self::ready(self.read_tx(
            &r.space,
            |s| {
                s.queues
                    .iter()
                    .map(|q| q.name.clone())
                    .filter(|n| r.queues.as_ref().is_none_or(|a| a.contains(n)))
                    .collect()
            },
            None,
            |sp, t| Ok(handlers::discover(sp, t, &r)),
        ))
    }

    fn put_blob(&self, _r: PutBlobRequest) -> DpFuture<'_, PutBlobResponse> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::Blob,
        ))))
    }

    fn get_blob(&self, _r: GetBlobRequest) -> DpFuture<'_, Bytes> {
        Self::ready(Err(DataPlaneError::FeatureUnsupported(Unsupported::Shape(
            ShapeKind::Blob,
        ))))
    }
}
