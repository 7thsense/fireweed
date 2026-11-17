//! PostgreSQL executor for Fireweed's data-plane DSL (API-008).
//!
//! [`apply_schema`] creates a space's tables and indexes from its schema.
//! [`execute`] runs a program as one transaction of plain SQL statements
//! over those tables. [`sweep`] writes back time-driven state and deletes
//! what retention has expired.
//!
//! These are functions over a connection. They keep nothing in the process
//! between calls: each call reads the space's schema from the database,
//! checks the program against it, and runs it. Queries are sent unprepared
//! with text parameters (`query_typed`), so each costs one round trip.

#![forbid(unsafe_code)]

mod ddl;
mod lower;
mod run;

use fireweed_dsl::check;
use fireweed_dsl::{Applied, Durability, Error, Program, Response, Result, Schema, Status};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Row, Transaction};

pub use ddl::CATALOG;
use lower::Q;

/// How many times a program retries a deadlock or serialization failure.
const RETRIES: usize = 5;

pub(crate) fn db(e: tokio_postgres::Error) -> Error {
    match e.as_db_error() {
        // Data exceptions: a value didn't convert (a bad timestamp, an
        // overflow, a body path that isn't a number).
        Some(d) if d.code().code().starts_with("22") => Error::Invalid(d.message().to_owned()),
        Some(d) => Error::Unavailable(format!("{}: {}", d.code().code(), d.message())),
        None => Error::Unavailable(e.to_string()),
    }
}

fn retryable(e: &tokio_postgres::Error) -> bool {
    e.code()
        .is_some_and(|c| matches!(c.code(), "40001" | "40P01"))
}

/// Runs `q` unprepared, every parameter typed `text`.
pub(crate) async fn rows(
    tx: &Transaction<'_>,
    q: &Q,
) -> std::result::Result<Vec<Row>, tokio_postgres::Error> {
    let params: Vec<(&(dyn ToSql + Sync), Type)> = q
        .params
        .iter()
        .map(|p| (p as &(dyn ToSql + Sync), Type::TEXT))
        .collect();
    tx.query_typed(&q.sql, &params).await
}

/// A space as stored in the catalog.
pub(crate) struct Space {
    pub id: i64,
    pub schema_name: String,
    pub schema: std::sync::Arc<Schema>,
}

async fn load(c: &Client, tenant: &str, space: &str) -> Result<Option<(i64, Schema)>> {
    let row = c
        .query_typed_opt(
            "SELECT id, schema_doc::text FROM fireweed.spaces WHERE tenant = $1 AND space = $2",
            &[(&tenant, Type::TEXT), (&space, Type::TEXT)],
        )
        .await;
    let row = match row {
        Ok(r) => r,
        // The catalog doesn't exist yet: no space does.
        Err(e)
            if e.code()
                .is_some_and(|c| c.code() == "42P01" || c.code() == "3F000") =>
        {
            return Ok(None);
        }
        Err(e) => return Err(db(e)),
    };
    match row {
        None => Ok(None),
        Some(r) => {
            let doc: String = r.get(1);
            let schema = serde_json::from_str(&doc)
                .map_err(|e| Error::Unavailable(format!("stored schema: {e}")))?;
            Ok(Some((r.get(0), schema)))
        }
    }
}

/// The stored schema of a space, if it exists.
pub async fn get_schema(c: &Client, tenant: &str, space: &str) -> Result<Option<Schema>> {
    Ok(load(c, tenant, space).await?.map(|(_, s)| s))
}

/// Creates a space, or changes its schema (API-008 §Changing a schema).
/// `expected` is the version the caller expects to replace: `None` for a
/// new space.
pub async fn apply_schema(
    c: &mut Client,
    tenant: &str,
    space: &str,
    schema: &Schema,
    expected: Option<i64>,
) -> Result<Applied> {
    check::schema(schema)?;
    let tx = c.transaction().await.map_err(db)?;
    // One catalog change at a time, across every process.
    tx.batch_execute("SELECT pg_advisory_xact_lock(7342651001)")
        .await
        .map_err(db)?;
    tx.batch_execute(CATALOG).await.map_err(db)?;
    // ALTER TABLE waits for an exclusive lock, and claims would queue behind
    // that wait: bound it.
    tx.batch_execute("SET LOCAL lock_timeout = '5s'")
        .await
        .map_err(db)?;
    let doc = serde_json::to_string(schema).expect("schemas serialize");
    let version = schema.version.to_string();
    let row = tx
        .query_typed_opt(
            "SELECT id, schema_doc::text FROM fireweed.spaces WHERE tenant = $1 AND space = $2 FOR UPDATE",
            &[(&tenant, Type::TEXT), (&space, Type::TEXT)],
        )
        .await
        .map_err(db)?;
    let applied = match row {
        None => {
            if expected.is_some() {
                return Err(Error::SchemaConflict { current: None });
            }
            let id: i64 = tx
                .query_typed_one(
                    "INSERT INTO fireweed.spaces (tenant, space, version, schema_doc) VALUES ($1, $2, $3::bigint, $4::jsonb) RETURNING id",
                    &[(&tenant, Type::TEXT), (&space, Type::TEXT), (&version, Type::TEXT), (&doc, Type::TEXT)],
                )
                .await
                .map_err(db)?
                .get(0);
            tx.batch_execute(&ddl::create(&ddl::schema_name(id), schema))
                .await
                .map_err(db)?;
            Applied {
                version: schema.version,
                changed: true,
            }
        }
        Some(r) => {
            let id: i64 = r.get(0);
            let old: Schema = serde_json::from_str(r.get::<_, &str>(1))
                .map_err(|e| Error::Unavailable(format!("stored schema: {e}")))?;
            if &old == schema {
                return Ok(Applied {
                    version: old.version,
                    changed: false,
                });
            }
            if expected != Some(old.version) {
                return Err(Error::SchemaConflict {
                    current: Some(old.version),
                });
            }
            check::change(&old, schema)?;
            tx.batch_execute(&ddl::change(&ddl::schema_name(id), &old, schema))
                .await
                .map_err(db)?;
            let id = id.to_string();
            tx.execute_typed(
                "UPDATE fireweed.spaces SET version = $2::bigint, schema_doc = $3::jsonb WHERE id = $1::bigint",
                &[(&id, Type::TEXT), (&version, Type::TEXT), (&doc, Type::TEXT)],
            )
            .await
            .map_err(db)?;
            Applied {
                version: schema.version,
                changed: true,
            }
        }
    };
    tx.commit().await.map_err(db)?;
    Ok(applied)
}

/// Runs a program as one transaction (API-008 §Transactions).
pub async fn execute(c: &mut Client, tenant: &str, space: &str, p: &Program) -> Result<Response> {
    let fingerprint = check::fingerprint(p)?;
    let durability = durability(p);
    for _ in 0..RETRIES {
        let tx = c.transaction().await.map_err(db)?;
        match program(
            &tx,
            tenant,
            space,
            p,
            &fingerprint,
            &setup(durability),
            durability,
        )
        .await
        {
            Ok(r) => match tx.commit().await {
                Ok(()) => return Ok(r),
                Err(e) if retryable(&e) => continue,
                Err(e) => return Err(db(e)),
            },
            Err(Attempt::Retry) => continue,
            Err(Attempt::Failed(e)) => return Err(e),
        }
    }
    Err(Error::Contention)
}

/// Runs independent programs in one space as one transaction with one
/// commit (API-008 §Batches). Each program runs under its own savepoint, so
/// one that fails or rejects leaves the others alone; each gets its own
/// result, in order. A program that hits a deadlock is run again.
pub async fn execute_batch(
    c: &mut Client,
    tenant: &str,
    space: &str,
    programs: &[Program],
) -> Result<Vec<Result<Response>>> {
    if programs.len() > check::MAX_BATCH {
        return Err(Error::Invalid(format!(
            "a batch has at most {} programs",
            check::MAX_BATCH
        )));
    }
    let mut out: Vec<Option<Result<Response>>> = programs
        .iter()
        .map(|p| check::fingerprint(p).err().map(Err))
        .collect();
    for _ in 0..RETRIES {
        let pending: Vec<usize> = (0..programs.len()).filter(|&i| out[i].is_none()).collect();
        if pending.is_empty() {
            break;
        }
        // One commit for all, so it waits for the WAL flush unless every
        // program is fire-and-forget.
        let durability = if pending
            .iter()
            .all(|&i| durability(&programs[i]) == Durability::FireAndForget)
        {
            Durability::FireAndForget
        } else {
            Durability::Durable
        };
        let tx = c.transaction().await.map_err(db)?;
        let mut setup = setup(durability);
        let mut done = vec![];
        for &i in &pending {
            let p = &programs[i];
            let fp = check::fingerprint(p)?;
            let r = program(&tx, tenant, space, p, &fp, &setup, durability).await;
            setup.clear();
            let undo = if r.is_ok() {
                "RELEASE SAVEPOINT open"
            } else {
                "ROLLBACK TO SAVEPOINT open; RELEASE SAVEPOINT open"
            };
            tx.batch_execute(undo).await.map_err(db)?;
            match r {
                Ok(resp) => done.push((i, Ok(resp))),
                Err(Attempt::Failed(e)) => done.push((i, Err(e))),
                Err(Attempt::Retry) => {}
            }
        }
        match tx.commit().await {
            Ok(()) => done.into_iter().for_each(|(i, r)| out[i] = Some(r)),
            Err(e) if retryable(&e) => {}
            Err(e) => return Err(db(e)),
        }
    }
    Ok(out
        .into_iter()
        .map(|r| r.unwrap_or(Err(Error::Contention)))
        .collect())
}

fn durability(p: &Program) -> Durability {
    match p.durability {
        Some(Durability::FireAndForget) => Durability::FireAndForget,
        _ => Durability::Durable,
    }
}

/// A transaction's settings. The timeouts come first, so even a wait on a
/// duplicate request's record is bounded. The checker admits only
/// statements with an index path (API-008 §Index rule), so the planner is
/// told to take it rather than read a whole table it judges small.
fn setup(durability: Durability) -> String {
    let mut s = String::from(
        "SET LOCAL TimeZone = 'UTC'; SET LOCAL statement_timeout = '5s'; \
         SET LOCAL enable_seqscan = off;",
    );
    if durability == Durability::FireAndForget {
        s.push_str(" SET LOCAL synchronous_commit = off;");
    }
    s
}

pub(crate) enum Attempt {
    Retry,
    Failed(Error),
}

impl From<tokio_postgres::Error> for Attempt {
    fn from(e: tokio_postgres::Error) -> Self {
        if retryable(&e) {
            Attempt::Retry
        } else {
            Attempt::Failed(db(e))
        }
    }
}

impl From<Error> for Attempt {
    fn from(e: Error) -> Self {
        Attempt::Failed(e)
    }
}

/// Runs one program inside `tx`, under savepoint `open`, which the caller
/// releases or rolls back to. The caller commits. Round trips: one
/// pipelined flight of `setup`, the savepoint, `fireweed.open_space` (the
/// space's schema and its request record) and a second savepoint; one per
/// statement; the response.
async fn program(
    tx: &Transaction<'_>,
    tenant: &str,
    space: &str,
    p: &Program,
    fingerprint: &str,
    setup: &str,
    durability: Durability,
) -> std::result::Result<Response, Attempt> {
    let rid = p.request_id.clone();
    let issued = p.issued_at.clone();
    let fp = fingerprint.to_owned();
    let open = "SELECT space_id, schema_doc, fingerprint, response, fresh, expired \
                FROM fireweed.open_space($1, $2, $3, $4, $5::timestamptz)";
    let open_params: [(&(dyn ToSql + Sync), Type); 5] = [
        (&tenant, Type::TEXT),
        (&space, Type::TEXT),
        (&rid, Type::TEXT),
        (&fp, Type::TEXT),
        (&issued, Type::TEXT),
    ];
    let first = format!("{setup} SAVEPOINT open;");
    let (_, row, _) = match futures::try_join!(
        tx.batch_execute(&first),
        tx.query_typed_one(open, &open_params),
        tx.batch_execute("SAVEPOINT program"),
    ) {
        Ok(x) => x,
        // No catalog yet: no space exists.
        Err(e)
            if e.code()
                .is_some_and(|c| matches!(c.code(), "42883" | "3F000" | "42P01")) =>
        {
            return Err(Attempt::Failed(Error::SpaceNotFound));
        }
        Err(e) => return Err(e.into()),
    };
    let Some(id) = row.get::<_, Option<i64>>(0) else {
        return Err(Attempt::Failed(Error::SpaceNotFound));
    };
    if row.get::<_, bool>(5) {
        return Err(Attempt::Failed(Error::RequestExpired));
    }
    let schema: Schema = serde_json::from_str(row.get::<_, &str>(1))
        .map_err(|e| Attempt::Failed(Error::Unavailable(format!("stored schema: {e}"))))?;
    let sp = Space {
        id,
        schema_name: ddl::schema_name(id),
        schema: schema.into(),
    };
    let r = check::resolve(&sp.schema, p)?;
    if !row.get::<_, bool>(4) {
        if row.get::<_, Option<&str>>(2) != Some(fingerprint) {
            return Err(Attempt::Failed(Error::RequestIdConflict));
        }
        let mut resp: Response =
            serde_json::from_str(row.get::<_, Option<&str>>(3).unwrap_or("null")).map_err(|e| {
                Attempt::Failed(Error::Unavailable(format!("stored response: {e}")))
            })?;
        run::rehydrate_claims(tx, &sp, &r, &mut resp).await?;
        resp.replayed = true;
        return Ok(resp);
    }
    let mut results = vec![];
    let mut rejected = None;
    for (i, st) in r.statements.iter().enumerate() {
        match run::statement(tx, &sp, st).await? {
            run::Outcome::Done(v) => results.push(v),
            run::Outcome::Rejected(reason) => {
                rejected = Some(fireweed_dsl::Rejection {
                    statement: i,
                    reason,
                });
                break;
            }
        }
    }
    let is_rejected = rejected.is_some();
    let resp = Response {
        request_id: p.request_id.clone(),
        status: if is_rejected {
            Status::Rejected
        } else {
            Status::Committed
        },
        rejected,
        results: if is_rejected { vec![] } else { results },
        durability,
        replayed: false,
    };
    // A rejection undoes the statements but keeps the request record.
    if is_rejected {
        tx.batch_execute("ROLLBACK TO SAVEPOINT program").await?;
    }
    let Some(rid) = &p.request_id else {
        return Ok(resp);
    };
    // Claims are recorded as ids and tokens; a replay re-reads the items.
    let recorded = run::compact_claims(&r, &resp);
    let mut q = Q::default();
    let (id, doc) = (
        q.bind(Some(rid.clone())),
        q.bind(Some(
            serde_json::to_string(&recorded).expect("responses serialize"),
        )),
    );
    q.sql = format!(
        "UPDATE \"{}\".fw_requests SET response = {doc}::jsonb WHERE request_id = {id}",
        sp.schema_name
    );
    rows(tx, &q).await?;
    Ok(resp)
}

/// What one sweep pass changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Leased items whose leases expired, written back as pending or failed.
    pub expired_leases: u64,
    /// Finished items, expired table rows and request records deleted.
    pub deleted: u64,
}

/// Writes back time-driven state and deletes what retention has expired,
/// at most `batch` rows per object (API-008 §Time). Rows other
/// transactions hold are skipped.
pub async fn sweep(c: &mut Client, tenant: &str, space: &str, batch: u32) -> Result<Swept> {
    let (id, schema) = load(c, tenant, space).await?.ok_or(Error::SpaceNotFound)?;
    let sp = Space {
        id,
        schema_name: ddl::schema_name(id),
        schema: schema.into(),
    };
    let tx = c.transaction().await.map_err(db)?;
    let out = run::sweep(&tx, &sp, batch as i64).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    Ok(out)
}
