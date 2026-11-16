//! PostgreSQL implementation of the Fireweed data-plane interface
//! (API-006), first increment.
//!
//! Design of this increment:
//!
//! - Records live in PostgreSQL tables (`fw_spaces`, `fw_queue_meta`,
//!   `fw_items`, `fw_rows`, `fw_row_hwm`, `fw_requests`), one row per item,
//!   table row and request record.
//! - Each instance owns one connection on a worker thread. Requests run
//!   there one at a time, and the returned futures wait for the worker
//!   without blocking the caller's executor, so the blocking `postgres`
//!   client never runs inside an async runtime. The worker reopens the
//!   connection when it has closed.
//! - Every write request is one transaction. It locks the space's row
//!   (`SELECT … FOR UPDATE`), so writes to one space are serialized across
//!   every process and instance (`MultiProcess`). Request time is
//!   non-decreasing per space because it is stored on that row.
//! - The transaction loads the objects the request references, runs the
//!   shared request logic of `fireweed-interface-engine` (the same code and
//!   reference evaluator as the in-memory implementation), and writes back
//!   the records that changed. Request records are written in the same
//!   transaction, so a rejected group's record is atomic with its outcome.
//! - Reads run in a `READ COMMITTED` transaction that also locks the space
//!   row `FOR UPDATE`: no write to the space commits while the read runs,
//!   and the read stores its request time, so no later request gets an
//!   earlier one (API-006 §Time rule 3). Reads and writes to one space are
//!   therefore serialized.
//!
//! Not yet done, and required before production use: loading only the
//! records a request touches (today a request loads whole queues and
//! tables), pushing access terms and claims into indexed SQL (`FOR UPDATE
//! SKIP LOCKED`), the `SERIALIZABLE` group-commit protocol of API-006
//! §PostgreSQL in place of the per-space lock, database time as the
//! production authority, and a connection pool in place of the one
//! connection per instance. Cohorts and group batching are reported
//! unsupported.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

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
CREATE TABLE IF NOT EXISTS fw_table_seq (
  tenant text NOT NULL, space text NOT NULL, tbl text NOT NULL, last bigint NOT NULL,
  PRIMARY KEY (tenant, space, tbl));
"#;

/// PostgreSQL is assumed fully replicated (owner, 2026-11-15): a committed
/// transaction is `Durable`, and keeping it so is the operator's job. A
/// `FireAndForget` request commits with `synchronous_commit = off`. A
/// `Tracked` request is acknowledged at `Durable`, the next level offered.
const PROFILE: Profile = Profile {
    offered: &[DurabilityLevel::FireAndForget, DurabilityLevel::Durable],
    default_durability: DurabilityLevel::Durable,
};

pub struct PostgresDataPlane {
    clock: Arc<dyn Clock>,
    limits: ImplementationLimits,
    /// Requests for the connection worker; `None` once dropped.
    jobs: Option<mpsc::Sender<Job>>,
    worker: Option<JoinHandle<()>>,
    /// How this instance connected: the URL and the schema, if any.
    location: (String, Option<String>),
}

/// Work for the connection worker.
type Job = Box<dyn FnOnce(&mut Conn) + Send>;

/// The worker's connection, reopened when it has closed.
struct Conn {
    url: String,
    schema: Option<String>,
    client: Option<Client>,
}

impl Conn {
    fn client(&mut self) -> DpResult<&mut Client> {
        if self.client.as_ref().is_none_or(|c| c.is_closed()) {
            self.client = None;
            self.client = Some(open_client(&self.url, self.schema.as_deref())?);
        }
        Ok(self.client.as_mut().expect("connection just opened"))
    }
}

/// Connects, with `search_path` set to `schema` if given, and creates the
/// tables if needed.
fn open_client(url: &str, schema: Option<&str>) -> DpResult<Client> {
    let mut cfg = postgres::Config::from_str(url).map_err(unavailable)?;
    if let Some(s) = schema {
        cfg.options(&format!("-c search_path={s}"));
    }
    let mut client = cfg.connect(NoTls).map_err(unavailable)?;
    client.batch_execute(DDL).map_err(unavailable)?;
    Ok(client)
}

fn unavailable(e: postgres::Error) -> DataPlaneError {
    match e.as_db_error() {
        Some(db) => DataPlaneError::Unavailable(format!("{}: {}", db.code().code(), db.message())),
        None => DataPlaneError::Unavailable(e.to_string()),
    }
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

impl PostgresDataPlane {
    /// Connects and creates the tables if needed.
    pub fn connect(
        url: &str,
        clock: Arc<dyn Clock>,
        limits: ImplementationLimits,
    ) -> DpResult<Self> {
        Self::start(url, None, false, clock, limits)
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
        Self::start(url, Some(schema.to_owned()), false, clock, limits)
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
        Self::start(url, Some(schema), true, clock, limits)
    }

    /// Another instance on the same database and schema, with its own
    /// connection and clock. It does not drop an isolated schema.
    pub fn peer(&self, clock: Arc<dyn Clock>) -> DpResult<Self> {
        let (url, schema) = &self.location;
        Self::start(url, schema.clone(), false, clock, self.limits)
    }

    /// Starts the connection worker and waits for its first connection. An
    /// `isolated` worker creates its schema first and drops it when it stops.
    fn start(
        url: &str,
        schema: Option<String>,
        isolated: bool,
        clock: Arc<dyn Clock>,
        limits: ImplementationLimits,
    ) -> DpResult<Self> {
        let (jobs, inbox) = mpsc::channel::<Job>();
        let (ready, started) = mpsc::channel::<DpResult<()>>();
        let mut conn = Conn {
            url: url.to_owned(),
            schema: schema.clone(),
            client: None,
        };
        let cleanup = schema.clone().filter(|_| isolated);
        let worker = std::thread::Builder::new()
            .name("fireweed-postgres".into())
            .spawn(move || {
                let init = (|| {
                    if let Some(s) = &cleanup {
                        Client::connect(&conn.url, NoTls)
                            .and_then(|mut admin| {
                                admin.batch_execute(&format!("CREATE SCHEMA {s}"))
                            })
                            .map_err(unavailable)?;
                    }
                    conn.client().map(|_| ())
                })();
                let ok = init.is_ok();
                let _ = ready.send(init);
                if ok {
                    for job in inbox {
                        // A panicking request drops its reply, which the
                        // caller sees as an error; the connection is reset.
                        if catch_unwind(AssertUnwindSafe(|| job(&mut conn))).is_err() {
                            conn.client = None;
                        }
                    }
                }
                drop(conn.client.take());
                if let Some(s) = cleanup
                    && let Ok(mut admin) = Client::connect(&conn.url, NoTls)
                {
                    let _ = admin.batch_execute(&format!("DROP SCHEMA IF EXISTS {s} CASCADE"));
                }
            })
            .map_err(|e| DataPlaneError::Unavailable(format!("connection worker: {e}")))?;
        started.recv().unwrap_or_else(|_| {
            Err(DataPlaneError::Unavailable(
                "connection worker stopped".into(),
            ))
        })?;
        Ok(Self {
            clock,
            limits,
            jobs: Some(jobs),
            worker: Some(worker),
            location: (url.to_owned(), schema),
        })
    }

    /// Runs `f` on the connection worker; the future waits for its answer.
    fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Client) -> DpResult<T> + Send + 'static,
    ) -> DpFuture<'static, T> {
        let (reply, answer) = futures::channel::oneshot::channel();
        let job: Job = Box::new(move |conn: &mut Conn| {
            let _ = reply.send(conn.client().and_then(f));
        });
        if let Some(jobs) = &self.jobs {
            let _ = jobs.send(job);
        }
        Box::pin(async move {
            answer.await.unwrap_or_else(|_| {
                Err(DataPlaneError::Unavailable(
                    "the connection worker dropped the request".into(),
                ))
            })
        })
    }

    /// Loads the space row and the objects `objects` names from its schema.
    /// The space row is locked `FOR UPDATE`, which serializes requests to the
    /// space. `request` also loads that request's record.
    fn load(
        tx: &mut Transaction<'_>,
        key: &SpaceKey,
        objects: impl FnOnce(&SpaceSchema) -> BTreeSet<ObjectName>,
        request: Option<&RequestId>,
    ) -> DpResult<Space> {
        let (t, s) = (key.tenant_id.as_str(), key.space_id.as_str());
        let row = tx
            .query_opt(
                "SELECT schema, next_item_id, next_token, last_time FROM fw_spaces WHERE tenant=$1 AND space=$2 FOR UPDATE",
                &[&t, &s],
            )
            .map_err(unavailable)?
            .ok_or(DataPlaneError::SpaceNotFound)?;
        let schema: SpaceSchema = serde_json::from_str(row.get::<_, &str>(0)).map_err(corrupt)?;
        let objects = objects(&schema);
        let mut space = Space {
            schema: schema.clone(),
            queues: BTreeMap::new(),
            tables: BTreeMap::new(),
            records: BTreeMap::new(),
            next_item_id: row.get::<_, i64>(1) as u64,
            next_token: row.get::<_, i64>(2) as u64,
            last_time: row.get(3),
        };
        for name in &objects {
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
                space.queues.insert(name.clone(), q);
            } else if let Some(decl) = schema.tables.iter().find(|tb| &tb.name == name) {
                let mut tb = handlers::empty_table(decl);
                if let Some(r) = tx
                    .query_opt(
                        "SELECT last FROM fw_table_seq WHERE tenant=$1 AND space=$2 AND tbl=$3",
                        &[&t, &s, &name.as_str()],
                    )
                    .map_err(unavailable)?
                {
                    tb.next_seq = r.get::<_, i64>(0) as u64;
                }
                for r in tx
                    .query(
                        "SELECT key, data FROM fw_rows WHERE tenant=$1 AND space=$2 AND tbl=$3",
                        &[&t, &s, &name.as_str()],
                    )
                    .map_err(unavailable)?
                {
                    let k: Vec<KeyVal> =
                        serde_json::from_str(r.get::<_, &str>(0)).map_err(corrupt)?;
                    let row: Row = serde_json::from_str(r.get::<_, &str>(1)).map_err(corrupt)?;
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
        if before.schema != after.schema {
            tx.execute(
                "UPDATE fw_spaces SET schema=$3 WHERE tenant=$1 AND space=$2",
                &[&t, &s, &ser(&after.schema)],
            )
            .map_err(unavailable)?;
        }
        tx.execute(
            "UPDATE fw_spaces SET next_item_id=$3, next_token=$4, last_time=$5 WHERE tenant=$1 AND space=$2",
            &[&t, &s, &(after.next_item_id as i64), &(after.next_token as i64), &after.last_time],
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
    fn write<T: Send + 'static>(
        &self,
        header: &WriteHeader,
        objects: BTreeSet<ObjectName>,
        f: impl FnOnce(&mut Space, i64) -> DpResult<T> + Send + 'static,
    ) -> DpFuture<'static, T> {
        let durability = header.min_durability;
        if let Err(e) = handlers::check_durability(&PROFILE, durability) {
            return Self::ready(Err(e));
        }
        let (key, request) = (header.space.clone(), header.request_id.clone());
        let clock = self.clock.clone();
        self.run(move |client| {
            let mut tx = client.transaction().map_err(unavailable)?;
            if PROFILE.level(durability) == DurabilityLevel::FireAndForget {
                tx.batch_execute("SET LOCAL synchronous_commit = off")
                    .map_err(unavailable)?;
            }
            let mut space = Self::load(&mut tx, &key, |_| objects, Some(&request))?;
            let before = space.clone();
            let t = handlers::begin(&mut space, clock.now().0);
            let out = f(&mut space, t)?;
            Self::store(&mut tx, &key, &before, &space, t)?;
            tx.commit().map_err(unavailable)?;
            Ok(out)
        })
    }

    /// Runs a read under the space lock (see the module documentation) and
    /// stores its request time. Due transitions are applied to the loaded
    /// copy only; the read writes nothing else.
    fn read_tx<T: Send + 'static>(
        &self,
        key: &SpaceKey,
        objects: impl FnOnce(&SpaceSchema) -> BTreeSet<ObjectName> + Send + 'static,
        request: Option<RequestId>,
        f: impl FnOnce(&Space, i64) -> DpResult<T> + Send + 'static,
    ) -> DpFuture<'static, T> {
        let key = key.clone();
        let clock = self.clock.clone();
        self.run(move |client| {
            let mut tx = client.transaction().map_err(unavailable)?;
            let mut space = Self::load(&mut tx, &key, objects, request.as_ref())?;
            let stored = space.last_time;
            let t = handlers::begin(&mut space, clock.now().0);
            let out = f(&space, t)?;
            if t > stored {
                tx.execute(
                    "UPDATE fw_spaces SET last_time=$3 WHERE tenant=$1 AND space=$2",
                    &[&key.tenant_id.as_str(), &key.space_id.as_str(), &t],
                )
                .map_err(unavailable)?;
            }
            tx.commit().map_err(unavailable)?;
            Ok(out)
        })
    }

    fn ready<T: Send + 'static>(r: DpResult<T>) -> DpFuture<'static, T> {
        Box::pin(std::future::ready(r))
    }
}

/// Every queue and table the schema declares.
fn all_objects(s: &SpaceSchema) -> BTreeSet<ObjectName> {
    s.queues
        .iter()
        .map(|q| q.name.clone())
        .chain(s.tables.iter().map(|t| t.name.clone()))
        .collect()
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
    if old.is_none_or(|o| o.next_seq != new.next_seq) {
        tx.execute(
            "INSERT INTO fw_table_seq (tenant, space, tbl, last) VALUES ($1,$2,$3,$4)
             ON CONFLICT (tenant, space, tbl) DO UPDATE SET last = EXCLUDED.last",
            &[&t, &s, &tbl, &(new.next_seq as i64)],
        )
        .map_err(unavailable)?;
    }
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
    /// Stops the connection worker after its queued requests; an isolated
    /// instance's worker then drops its schema.
    fn drop(&mut self) {
        drop(self.jobs.take());
        if let Some(w) = self.worker.take() {
            let _ = w.join();
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
            durability: DurabilityLevel::FireAndForget,
            new_work_visibility: Visibility::Immediate,
            sharing: Sharing::MultiProcess,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            max_durability: DurabilityLevel::Durable,
            shapes: [ShapeKind::Queue, ShapeKind::Table].into_iter().collect(),
            queue_features: [
                QueueFeature::DynamicGates,
                QueueFeature::MetadataBlockers,
                QueueFeature::Recurrence,
            ]
            .into_iter()
            .collect(),
            priority_kinds: [PriorityKind::Int64, PriorityKind::Timestamp]
                .into_iter()
                .collect(),
            ordering_modes: [OrderingMode::Strict].into_iter().collect(),
            limits: self.limits,
        }
    }

    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse> {
        let caps = self.capabilities();
        let clock = self.clock.clone();
        self.run(move |client| {
            let now = clock.now().0;
            let mut tx = client.transaction().map_err(unavailable)?;
            let (t, s) = (r.space.tenant_id.as_str(), r.space.space_id.as_str());
            let exists = tx
                .query_opt(
                    "SELECT 1 FROM fw_spaces WHERE tenant=$1 AND space=$2",
                    &[&t, &s],
                )
                .map_err(unavailable)?
                .is_some();
            if !exists {
                let (resp, new) = handlers::apply_schema(None, &r, &caps, &PROFILE, now)?;
                let space = new.expect("new space");
                // If another process creates the space first, this insert
                // waits for it and does nothing, and the request is applied
                // to that space below.
                let created = tx
                    .execute(
                        "INSERT INTO fw_spaces (tenant, space, schema, next_item_id, next_token, last_time) VALUES ($1,$2,$3,0,0,$4)
                         ON CONFLICT (tenant, space) DO NOTHING",
                        &[&t, &s, &ser(&space.schema), &space.last_time],
                    )
                    .map_err(unavailable)?
                    == 1;
                if created {
                    let empty = Space {
                        queues: BTreeMap::new(),
                        tables: BTreeMap::new(),
                        ..space.clone()
                    };
                    Self::store(&mut tx, &r.space, &empty, &space, space.last_time)?;
                    tx.commit().map_err(unavailable)?;
                    return Ok(resp);
                }
            }
            // A schema change loads every object fully, so the handler can
            // apply expirations due under the old policy before installing
            // the new one; request records are expired here under the old
            // retention, at the space's request time, for the same reason.
            let mut space = Self::load(&mut tx, &r.space, all_objects, None)?;
            let cutoff =
                now.max(space.last_time) - space.schema.request_id_retention_ms as i64;
            tx.execute(
                "DELETE FROM fw_requests WHERE tenant=$1 AND space=$2 AND recorded_at <= $3",
                &[&t, &s, &cutoff],
            )
            .map_err(unavailable)?;
            let before = space.clone();
            let (resp, _) = handlers::apply_schema(Some(&mut space), &r, &caps, &PROFILE, now)?;
            Self::store(&mut tx, &r.space, &before, &space, space.last_time)?;
            tx.commit().map_err(unavailable)?;
            Ok(resp)
        })
    }

    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>> {
        let read = self.read_tx(
            &space,
            |_| BTreeSet::new(),
            None,
            |s, _| Ok(handlers::schema_state(s)),
        );
        Box::pin(async move {
            match read.await {
                Ok(s) => Ok(Some(s)),
                Err(DataPlaneError::SpaceNotFound) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse> {
        let l = self.limits;
        self.write(&r.header.clone(), one(r.queue.clone()), move |sp, t| {
            handlers::enqueue(sp, t, &l, &PROFILE, &r)
        })
    }

    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse> {
        let l = self.limits;
        self.write(&r.header.clone(), one(r.queue.clone()), move |sp, t| {
            handlers::claim(sp, t, &l, &PROFILE, &r)
        })
    }

    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse> {
        let l = self.limits;
        self.write(&r.header.clone(), one(r.queue.clone()), move |sp, t| {
            handlers::claim_by_item_ids(sp, t, &l, &PROFILE, &r)
        })
    }

    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse> {
        let l = self.limits;
        self.write(&r.header.clone(), one(r.queue.clone()), move |sp, t| {
            handlers::renew(sp, t, &l, &PROFILE, &r)
        })
    }

    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse> {
        let l = self.limits;
        self.write(&r.header.clone(), one(r.queue.clone()), move |sp, t| {
            handlers::finalize(sp, t, &l, &PROFILE, &r)
        })
    }

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse> {
        let l = self.limits;
        self.write(
            &r.header.clone(),
            handlers::commit_objects(&r),
            move |sp, t| handlers::commit(sp, t, &l, &PROFILE, &r),
        )
    }

    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>> {
        let request = Some(r.request_id.clone());
        self.read_tx(
            &r.space.clone(),
            |_| BTreeSet::new(),
            request,
            move |sp, _| Ok(handlers::get_request(sp, &r)),
        )
    }

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse> {
        let l = self.limits;
        let objects = handlers::get_objects(&r);
        self.read_tx(
            &r.space.clone(),
            move |_| objects,
            None,
            move |sp, t| handlers::get(sp, t, &l, &r),
        )
    }

    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse> {
        let l = self.limits;
        let (key, r2) = (r.space.clone(), r.clone());
        self.read_tx(
            &key,
            move |s| handlers::read_objects(s, &r2),
            None,
            move |sp, t| handlers::read(sp, t, &l, &r),
        )
    }

    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics> {
        let objects = one(r.queue.clone());
        self.read_tx(
            &r.space.clone(),
            move |_| objects,
            None,
            move |sp, t| handlers::metrics(sp, t, &r),
        )
    }

    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse> {
        let queues = r.queues.clone();
        self.read_tx(
            &r.space.clone(),
            move |s| {
                s.queues
                    .iter()
                    .map(|q| q.name.clone())
                    .filter(|n| queues.as_ref().is_none_or(|a| a.contains(n)))
                    .collect()
            },
            None,
            move |sp, t| Ok(handlers::discover(sp, t, &r)),
        )
    }
}
