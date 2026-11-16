//! PostgreSQL executor for Fireweed's data-plane DSL (API-006).
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

mod ddl;
mod lower;
mod run;

use fireweed_dsl::check;
use fireweed_dsl::{Applied, Durability, Error, Program, Response, Result, Schema, Status};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Row, Transaction};

pub use ddl::CATALOG;
use lower::{NOW, Q};

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
    pub schema_name: String,
    pub schema: Schema,
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

/// Creates a space, or changes its schema (API-006 §Changing a schema).
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

/// Runs a program as one transaction (API-006 §Transactions).
pub async fn execute(c: &mut Client, tenant: &str, space: &str, p: &Program) -> Result<Response> {
    let (id, schema) = load(c, tenant, space).await?.ok_or(Error::SpaceNotFound)?;
    let resolved = check::resolve(&schema, p)?;
    let sp = Space {
        schema_name: ddl::schema_name(id),
        schema,
    };
    for _ in 0..RETRIES {
        match attempt(c, &sp, p, &resolved).await {
            Err(Attempt::Retry) => continue,
            Err(Attempt::Failed(e)) => return Err(e),
            Ok(r) => return Ok(r),
        }
    }
    Err(Error::Contention)
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

/// One try at a program. Round trips: BEGIN, the request record (with an
/// id), settings and savepoint, one per statement, the response, COMMIT.
async fn attempt(
    c: &mut Client,
    sp: &Space,
    p: &Program,
    r: &check::Resolved,
) -> std::result::Result<Response, Attempt> {
    let tx = c.transaction().await?;
    let durability = match p.durability {
        Some(Durability::FireAndForget) => Durability::FireAndForget,
        _ => Durability::Durable,
    };
    let requests = format!("\"{}\".fw_requests", sp.schema_name);
    if let Some(rid) = &p.request_id {
        // Record the request, or take over a record past retention. A live
        // record (or a concurrent first run, once it commits) means replay.
        let mut q = Q::default();
        let (id, fp) = (
            q.bind(Some(rid.clone())),
            q.bind(Some(r.fingerprint.clone())),
        );
        let retention = q.duration(sp.schema.request_retention.0);
        q.sql = format!(
            "INSERT INTO {requests} AS t (request_id, fingerprint, response, recorded_at) VALUES ({id}, {fp}, NULL, {NOW})
             ON CONFLICT (request_id) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, response = NULL, recorded_at = EXCLUDED.recorded_at
             WHERE t.recorded_at <= {NOW} - {retention}
             RETURNING 1"
        );
        if rows(&tx, &q).await?.is_empty() {
            return replay(tx, &requests, rid, &r.fingerprint).await;
        }
    }
    let mut setup = String::from("SET LOCAL TimeZone = 'UTC'; SET LOCAL statement_timeout = '5s';");
    if durability == Durability::FireAndForget {
        setup.push_str(" SET LOCAL synchronous_commit = off;");
    }
    if p.request_id.is_some() {
        setup.push_str(" SAVEPOINT program;");
    }
    tx.batch_execute(&setup).await?;
    let mut results = vec![];
    let mut rejected = None;
    for (i, st) in r.statements.iter().enumerate() {
        match run::statement(&tx, sp, st).await? {
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
    let Some(rid) = &p.request_id else {
        // No record to keep: a rejection just rolls back.
        if resp.rejected.is_some() {
            tx.rollback().await?;
        } else {
            tx.commit().await?;
        }
        return Ok(resp);
    };
    if resp.rejected.is_some() {
        tx.batch_execute("ROLLBACK TO SAVEPOINT program").await?;
    }
    let mut q = Q::default();
    let (id, doc) = (
        q.bind(Some(rid.clone())),
        q.bind(Some(
            serde_json::to_string(&resp).expect("responses serialize"),
        )),
    );
    q.sql = format!("UPDATE {requests} SET response = {doc}::jsonb WHERE request_id = {id}");
    rows(&tx, &q).await?;
    tx.commit().await?;
    Ok(resp)
}

async fn replay(
    tx: Transaction<'_>,
    requests: &str,
    rid: &str,
    fingerprint: &str,
) -> std::result::Result<Response, Attempt> {
    let row = tx
        .query_typed_one(
            &format!("SELECT fingerprint, response::text FROM {requests} WHERE request_id = $1"),
            &[(&rid, Type::TEXT)],
        )
        .await?;
    let stored: &str = row.get(0);
    if stored != fingerprint {
        return Err(Attempt::Failed(Error::RequestIdConflict));
    }
    let doc: Option<&str> = row.get(1);
    let mut resp: Response = serde_json::from_str(doc.unwrap_or("null"))
        .map_err(|e| Attempt::Failed(Error::Unavailable(format!("stored response: {e}"))))?;
    resp.replayed = true;
    tx.rollback().await?;
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
/// at most `batch` rows per object (API-006 §Time). Rows other
/// transactions hold are skipped.
pub async fn sweep(c: &mut Client, tenant: &str, space: &str, batch: u32) -> Result<Swept> {
    let (id, schema) = load(c, tenant, space).await?.ok_or(Error::SpaceNotFound)?;
    let sp = Space {
        schema_name: ddl::schema_name(id),
        schema,
    };
    let tx = c.transaction().await.map_err(db)?;
    let out = run::sweep(&tx, &sp, batch as i64).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    Ok(out)
}
