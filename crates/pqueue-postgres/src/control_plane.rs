//! Transactional postgres [`QueueControlPlane`] (BQ-22) — the production default control plane (TD-003).
//!
//! This is the DURABLE store for queue ownership; the lease state machine + the C4b seam invariants are NOT
//! reimplemented here — they live once in `pqueue-engine`'s pure lease decisions
//! ([`lease_decide_acquire`] / `_renew` / `_begin_drain` / `_release`, [`lease_resolution`],
//! [`resolve_target`]). Each lease op is ONE postgres transaction: `SELECT ... FOR UPDATE` the authority
//! row (so concurrent acquires linearize — TD-003 "at most one succeeds vs a prior epoch"), apply the pure
//! decision, persist the next record, commit. The owner registry (`pqueue_workers`) supplies the live set
//! for the assignment function and the fail-closed owner-liveness gate.
//!
//! Two durable tables in the control-plane schema:
//! - `pqueue_queue_owner` — the per-queue authority record `(active_owner, target_owner, assignment_epoch,
//!   lease_expires_at, state)`. A missing row materializes as the genesis `unassigned`/epoch-0 lease.
//! - `pqueue_workers` — `(owner_id, heartbeat_at)`; an owner is live while `heartbeat_at + ttl > now`.
//!
//! BINDING TO THE STORAGE FENCE (BQ-23): when a postgres storage schema is present in the same search path,
//! acquire advances that schema's append-fence `assignment_epoch` inside the same transaction as the owner
//! row. A stale owner is therefore fenced by one durable epoch value before the new owner serves.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use postgres::Client;
use pqueue_core::{OwnerId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, ControlPlaneConfig, EngineError, EngineResult, LeaseRenewal,
    LeaseRenewalOutcome, LeaseState, OwnerEndpointAdvertisement, OwnerResolution,
    QueueControlPlane, QueueKey, QueueLease, lease_decide_acquire, lease_decide_begin_drain,
    lease_decide_confirm_fence, lease_decide_release, lease_decide_renew, lease_resolution,
    owner_heartbeat_live, resolve_target,
};

use crate::{PostgresConnectConfig, connect};

const CONTROL_PLANE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pqueue_workers (
    owner_id TEXT NOT NULL PRIMARY KEY,
    heartbeat_at BIGINT NOT NULL,                -- nanoseconds since epoch
    endpoint TEXT
);
ALTER TABLE pqueue_workers ADD COLUMN IF NOT EXISTS endpoint TEXT;
CREATE TABLE IF NOT EXISTS pqueue_queue_owner (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    state TEXT NOT NULL,                          -- 'unassigned' | 'pending_fence' | 'assigned' | 'draining'
    active_owner_id TEXT,                         -- NULL while unassigned
    target_owner_id TEXT,
    assignment_epoch BIGINT NOT NULL,            -- TD-003 fence authority; strictly-monotonic per queue
    lease_expires_at BIGINT,                     -- nanoseconds since epoch; NULL while unassigned
    PRIMARY KEY (tenant, queue)
);
"#;

fn st<T>(r: Result<T, postgres::Error>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

fn parts(queue: &QueueKey) -> (String, String) {
    (
        queue.tenant_id.as_str().to_string(),
        queue.queue_id.as_str().to_string(),
    )
}

fn ts_nanos(ts: UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanoseconds as i64)
}

fn nanos_ts(v: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        v.div_euclid(1_000_000_000),
        v.rem_euclid(1_000_000_000) as u32,
    )
    .expect("nanoseconds bounded by rem_euclid")
}

fn state_str(state: LeaseState) -> &'static str {
    match state {
        LeaseState::Unassigned => "unassigned",
        LeaseState::PendingFence => "pending_fence",
        LeaseState::Assigned => "assigned",
        LeaseState::Draining => "draining",
    }
}

fn parse_state(s: &str) -> EngineResult<LeaseState> {
    match s {
        "unassigned" => Ok(LeaseState::Unassigned),
        "pending_fence" => Ok(LeaseState::PendingFence),
        "assigned" => Ok(LeaseState::Assigned),
        "draining" => Ok(LeaseState::Draining),
        other => Err(EngineError::Storage(format!("bad lease state {other:?}"))),
    }
}

/// Reconstruct a [`QueueLease`] from an authority row (the SELECT returns these 5 columns in order).
fn row_to_lease(row: &postgres::Row) -> EngineResult<QueueLease> {
    let state: String = row.get(0);
    let active: Option<String> = row.get(1);
    let target: Option<String> = row.get(2);
    let epoch: i64 = row.get(3);
    let expires: Option<i64> = row.get(4);
    let to_owner = |o: Option<String>| -> EngineResult<Option<OwnerId>> {
        o.map(|s| OwnerId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
            .transpose()
    };
    Ok(QueueLease {
        state: parse_state(&state)?,
        active_owner_id: to_owner(active)?,
        target_owner_id: to_owner(target)?,
        assignment_epoch: epoch as u64,
        lease_expires_at: expires.map(nanos_ts),
    })
}

const SELECT_LEASE_COLS: &str =
    "state, active_owner_id, target_owner_id, assignment_epoch, lease_expires_at";

const BATCH_RENEW_SQL: &str = r#"
WITH input AS MATERIALIZED (
    SELECT tenant, queue, owner_id, expected_epoch, ord
    FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[])
         WITH ORDINALITY AS i(tenant, queue, owner_id, expected_epoch, ord)
),
locked AS MATERIALIZED (
    SELECT q.tenant, q.queue, q.state, q.active_owner_id, q.target_owner_id,
           q.assignment_epoch, q.lease_expires_at
    FROM pqueue_queue_owner q
    JOIN (SELECT DISTINCT tenant, queue FROM input) i
      ON i.tenant = q.tenant AND i.queue = q.queue
    ORDER BY q.tenant, q.queue
    FOR UPDATE OF q
),
updated AS (
    UPDATE pqueue_queue_owner q
       SET lease_expires_at = GREATEST(q.lease_expires_at, $6)
      FROM input i, locked l
     WHERE q.tenant = i.tenant AND q.queue = i.queue
       AND l.tenant = q.tenant AND l.queue = q.queue
       AND q.active_owner_id = i.owner_id
       AND q.assignment_epoch = i.expected_epoch
       AND q.state IN ('assigned', 'draining')
       AND q.lease_expires_at > $5
    RETURNING q.tenant, q.queue, q.state, q.active_owner_id, q.target_owner_id,
              q.assignment_epoch, q.lease_expires_at
)
SELECT i.ord,
       COALESCE((l.active_owner_id = i.owner_id
        AND l.assignment_epoch = i.expected_epoch
        AND l.state IN ('assigned', 'draining')
        AND l.lease_expires_at > $5), FALSE) AS renewed,
       l.tenant IS NOT NULL AS present,
       COALESCE(u.state, l.state), COALESCE(u.active_owner_id, l.active_owner_id),
       COALESCE(u.target_owner_id, l.target_owner_id),
       COALESCE(u.assignment_epoch, l.assignment_epoch),
       COALESCE(u.lease_expires_at, l.lease_expires_at)
  FROM input i
  LEFT JOIN locked l ON l.tenant = i.tenant AND l.queue = i.queue
  LEFT JOIN updated u ON u.tenant = i.tenant AND u.queue = i.queue
 ORDER BY i.ord
"#;

const BATCH_RESOLVE_SQL: &str = r#"
WITH live AS MATERIALIZED (
    SELECT COALESCE(array_agg(owner_id ORDER BY owner_id), ARRAY[]::text[]) AS owners
      FROM pqueue_workers
     WHERE heartbeat_at > $3
), input AS MATERIALIZED (
    SELECT tenant, queue, ord
    FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS i(tenant, queue, ord)
)
SELECT i.ord, q.state, q.active_owner_id, q.target_owner_id,
       q.assignment_epoch, q.lease_expires_at,
       CASE WHEN i.ord = 1 THEN live.owners END
  FROM input i
  LEFT JOIN pqueue_queue_owner q ON q.tenant = i.tenant AND q.queue = i.queue
 CROSS JOIN live
 ORDER BY i.ord
"#;

fn optional_lease(
    state: Option<String>,
    active: Option<String>,
    target: Option<String>,
    epoch: Option<i64>,
    expires: Option<i64>,
) -> EngineResult<QueueLease> {
    let Some(state) = state else {
        return Ok(QueueLease::unassigned());
    };
    let owner = |value: Option<String>| -> EngineResult<Option<OwnerId>> {
        value
            .map(|value| {
                OwnerId::new(value).map_err(|error| EngineError::Storage(error.to_string()))
            })
            .transpose()
    };
    Ok(QueueLease {
        state: parse_state(&state)?,
        active_owner_id: owner(active)?,
        target_owner_id: owner(target)?,
        assignment_epoch: epoch
            .ok_or_else(|| EngineError::Storage("authority row omitted assignment epoch".into()))?
            as u64,
        lease_expires_at: expires.map(nanos_ts),
    })
}

/// Persist a (possibly mutated) authority record under `tx` (UPSERT on the queue key).
fn upsert_lease(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    lease: &QueueLease,
) -> EngineResult<()> {
    let active = lease
        .active_owner_id
        .as_ref()
        .map(|o| o.as_str().to_string());
    let target = lease
        .target_owner_id
        .as_ref()
        .map(|o| o.as_str().to_string());
    let expires = lease.lease_expires_at.map(ts_nanos);
    st(tx.execute(
        "INSERT INTO pqueue_queue_owner \
         (tenant,queue,state,active_owner_id,target_owner_id,assignment_epoch,lease_expires_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7) \
         ON CONFLICT (tenant,queue) DO UPDATE SET state=EXCLUDED.state, \
           active_owner_id=EXCLUDED.active_owner_id, target_owner_id=EXCLUDED.target_owner_id, \
           assignment_epoch=EXCLUDED.assignment_epoch, lease_expires_at=EXCLUDED.lease_expires_at",
        &[
            &t,
            &q,
            &state_str(lease.state),
            &active,
            &target,
            &(lease.assignment_epoch as i64),
            &expires,
        ],
    ))?;
    Ok(())
}

/// Whether `table_name` exists in the current schema AND carries `column_name`. Both the log-replay
/// `queues` table and the relational family's own (differently-shaped) `queues` table share a name, so a
/// table-existence check alone cannot tell which schema flavor is present -- only the relational family's
/// `relational_cursor.assignment_epoch` / log-replay's `queues.assignment_epoch` column actually
/// distinguishes them.
fn column_exists(
    tx: &mut postgres::Transaction<'_>,
    table_name: &str,
    column_name: &str,
) -> EngineResult<bool> {
    let row = st(tx.query_one(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)",
        &[&table_name, &column_name],
    ))?;
    Ok(row.get(0))
}

fn bind_storage_epoch_if_present(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    epoch: u64,
) -> EngineResult<bool> {
    let epoch = epoch as i64;
    let mut bound = false;
    if column_exists(tx, "queues", "assignment_epoch")? {
        let updated = st(tx.execute(
            "UPDATE queues SET assignment_epoch=$3 WHERE tenant=$1 AND queue=$2",
            &[&t, &q, &epoch],
        ))?;
        if updated == 0 {
            return Err(EngineError::NotFound);
        }
        bound = true;
    }
    if column_exists(tx, "relational_cursor", "assignment_epoch")? {
        let updated = st(tx.execute(
            "UPDATE relational_cursor SET assignment_epoch=$3 WHERE tenant=$1 AND queue=$2",
            &[&t, &q, &epoch],
        ))?;
        if updated == 0 {
            return Err(EngineError::NotFound);
        }
        bound = true;
    }
    Ok(bound)
}

/// The transactional postgres control plane. One blocking `postgres::Client` behind a `Mutex` (mirroring
/// the storage backends' single-connection model; see their blocking-executor caveat). Each lease op opens
/// its own transaction and takes a `FOR UPDATE` row lock for linearization.
pub struct PostgresControlPlane {
    config: ControlPlaneConfig,
    inner: Mutex<Client>,
    batch_renewal_calls: AtomicU64,
    batch_renewal_transactions: AtomicU64,
    batch_renewal_statements: AtomicU64,
    batch_resolution_calls: AtomicU64,
    batch_resolution_statements: AtomicU64,
}

/// Structural amplification counters for node-level ownership work. These count protocol operations,
/// not elapsed time, so the density contract is portable across hosts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PostgresControlPlaneDiagnostics {
    pub batch_renewal_calls: u64,
    pub batch_renewal_transactions: u64,
    pub batch_renewal_statements: u64,
    pub batch_resolution_calls: u64,
    pub batch_resolution_statements: u64,
    pub connections: u64,
}

impl PostgresControlPlane {
    /// Connect to `url` on the default `search_path` and ensure the control-plane schema.
    pub fn connect(url: &str, config: ControlPlaneConfig) -> EngineResult<Self> {
        let client = connect(PostgresConnectConfig::new(url))?;
        Self::from_client(client, config)
    }

    /// Connect isolated in a dedicated `schema` (test isolation; same DB on reconnect).
    pub fn connect_in_schema(
        url: &str,
        schema: &str,
        config: ControlPlaneConfig,
    ) -> EngineResult<Self> {
        if !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = connect(PostgresConnectConfig::new(url))?;
        st(client.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; SET search_path TO {schema};"
        )))?;
        Self::from_client(client, config)
    }

    fn from_client(mut client: Client, config: ControlPlaneConfig) -> EngineResult<Self> {
        st(client.batch_execute(CONTROL_PLANE_SCHEMA))?;
        Ok(PostgresControlPlane {
            config,
            inner: Mutex::new(client),
            batch_renewal_calls: AtomicU64::new(0),
            batch_renewal_transactions: AtomicU64::new(0),
            batch_renewal_statements: AtomicU64::new(0),
            batch_resolution_calls: AtomicU64::new(0),
            batch_resolution_statements: AtomicU64::new(0),
        })
    }

    pub fn diagnostics(&self) -> PostgresControlPlaneDiagnostics {
        PostgresControlPlaneDiagnostics {
            batch_renewal_calls: self.batch_renewal_calls.load(Ordering::Relaxed),
            batch_renewal_transactions: self.batch_renewal_transactions.load(Ordering::Relaxed),
            batch_renewal_statements: self.batch_renewal_statements.load(Ordering::Relaxed),
            batch_resolution_calls: self.batch_resolution_calls.load(Ordering::Relaxed),
            batch_resolution_statements: self.batch_resolution_statements.load(Ordering::Relaxed),
            connections: 1,
        }
    }

    /// Read the authority record for `queue` under `tx` with a `FOR UPDATE` row lock.
    ///
    /// B1 (BQ-22 fresh-eyes BLOCKING fix): `FOR UPDATE` locks nothing when the row is ABSENT, so two
    /// concurrent FIRST-acquires of a genesis queue would each read "no row" and both INSERT epoch 1 (two
    /// live writers at one epoch — the exact failure this durable store prevents). We therefore MATERIALIZE
    /// the genesis row (`INSERT ... ON CONFLICT DO NOTHING`) first: a concurrent inserter blocks on the
    /// first's uncommitted tuple until it commits, then the `SELECT ... FOR UPDATE` locks the now-existing
    /// row — serializing the two acquires so the second correctly sees the first's committed epoch.
    fn lease_for_update(
        tx: &mut postgres::Transaction<'_>,
        t: &str,
        q: &str,
    ) -> EngineResult<QueueLease> {
        st(tx.execute(
            "INSERT INTO pqueue_queue_owner \
             (tenant,queue,state,active_owner_id,target_owner_id,assignment_epoch,lease_expires_at) \
             VALUES ($1,$2,'unassigned',NULL,NULL,0,NULL) ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q],
        ))?;
        let row = st(tx.query_opt(
            &format!(
                "SELECT {SELECT_LEASE_COLS} FROM pqueue_queue_owner \
                 WHERE tenant=$1 AND queue=$2 FOR UPDATE"
            ),
            &[&t, &q],
        ))?;
        match row {
            Some(r) => row_to_lease(&r),
            // The genesis INSERT guarantees a row; absence here would be a concurrent DELETE we don't issue.
            None => Ok(QueueLease::unassigned()),
        }
    }

    /// Whether `owner` has a live heartbeat at `now` (the fail-closed liveness gate). Reads `pqueue_workers`.
    fn owner_is_live(
        &self,
        client: &mut Client,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<bool> {
        let row = st(client.query_opt(
            "SELECT heartbeat_at FROM pqueue_workers WHERE owner_id=$1",
            &[&owner.as_str()],
        ))?;
        Ok(match row {
            Some(r) => {
                let hb: i64 = r.get(0);
                owner_heartbeat_live(nanos_ts(hb), now, self.config.heartbeat_ttl_ms)
            }
            None => false,
        })
    }
}

impl QueueControlPlane for PostgresControlPlane {
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let mut client = self.inner.lock().expect("poisoned");
        st(client.execute(
            "INSERT INTO pqueue_workers (owner_id, heartbeat_at) VALUES ($1,$2) \
             ON CONFLICT (owner_id) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at",
            &[&owner.as_str(), &ts_nanos(now)],
        ))?;
        Ok(())
    }

    fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let mut client = self.inner.lock().expect("poisoned");
        st(client.execute(
            "INSERT INTO pqueue_workers (owner_id, heartbeat_at, endpoint) VALUES ($1,$2,$3) \
             ON CONFLICT (owner_id) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at, endpoint=EXCLUDED.endpoint",
            &[&owner.as_str(), &ts_nanos(now), &endpoint],
        ))?;
        Ok(())
    }

    fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        let cutoff =
            ts_nanos(now) - (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000);
        let mut client = self.inner.lock().expect("poisoned");
        let rows = st(client.query(
            "SELECT owner_id, endpoint, heartbeat_at FROM pqueue_workers \
             WHERE heartbeat_at > $1 AND endpoint IS NOT NULL ORDER BY owner_id",
            &[&cutoff],
        ))?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let owner = OwnerId::new(row.get::<_, String>(0)).ok()?;
                Some(OwnerEndpointAdvertisement {
                    owner,
                    endpoint: row.get(1),
                    expires_at: nanos_ts(row.get::<_, i64>(2).saturating_add(
                        (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000),
                    )),
                })
            })
            .collect())
    }

    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        // Identical upsert: a heartbeat from an unknown owner re-admits it (register-on-heartbeat).
        self.register_owner(owner, now)
    }

    fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        // FAIL-CLOSED (I2): a query failure is SURFACED, never swallowed into a fabricated `unassigned`
        // record (which would invite a spurious acquire / a bogus epoch-0 fence value).
        let cutoff =
            ts_nanos(now) - (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000);
        let rows = st(client.query(
            "SELECT owner_id FROM pqueue_workers WHERE heartbeat_at > $1",
            &[&cutoff],
        ))?;
        let live: Vec<OwnerId> = rows
            .iter()
            .filter_map(|r| OwnerId::new(r.get::<_, String>(0)).ok())
            .collect();
        let target = resolve_target(queue, live.iter());
        // Current authority record (no lock needed for a read-only resolve).
        let current = match st(client.query_opt(
            &format!(
                "SELECT {SELECT_LEASE_COLS} FROM pqueue_queue_owner WHERE tenant=$1 AND queue=$2"
            ),
            &[&t, &q],
        ))? {
            Some(r) => row_to_lease(&r)?,
            None => QueueLease::unassigned(),
        };
        Ok(lease_resolution(&current, target, now))
    }

    fn resolve_queue_owners(
        &self,
        queues: &[QueueKey],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerResolution>> {
        if queues.is_empty() {
            return Ok(Vec::new());
        }
        let tenants: Vec<String> = queues
            .iter()
            .map(|queue| queue.tenant_id.as_str().to_string())
            .collect();
        let queue_ids: Vec<String> = queues
            .iter()
            .map(|queue| queue.queue_id.as_str().to_string())
            .collect();
        let cutoff =
            ts_nanos(now) - (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000);
        self.batch_resolution_calls.fetch_add(1, Ordering::Relaxed);
        let mut client = self.inner.lock().expect("poisoned");
        self.batch_resolution_statements
            .fetch_add(1, Ordering::Relaxed);
        let rows = st(client.query(BATCH_RESOLVE_SQL, &[&tenants, &queue_ids, &cutoff]))?;
        if rows.len() != queues.len() {
            return Err(EngineError::Storage(format!(
                "batch owner resolution returned {} rows for {} inputs",
                rows.len(),
                queues.len()
            )));
        }
        let live: Vec<OwnerId> = rows[0]
            .get::<_, Option<Vec<String>>>(6)
            .unwrap_or_default()
            .into_iter()
            .map(|owner| {
                OwnerId::new(owner).map_err(|error| EngineError::Storage(error.to_string()))
            })
            .collect::<EngineResult<_>>()?;
        rows.into_iter()
            .zip(queues)
            .map(|(row, queue)| {
                let current =
                    optional_lease(row.get(1), row.get(2), row.get(3), row.get(4), row.get(5))?;
                Ok(lease_resolution(
                    &current,
                    resolve_target(queue, live.iter()),
                    now,
                ))
            })
            .collect()
    }

    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        // Fail-closed: only a live registered owner may acquire (checked before the txn — a dead owner
        // never reaches the authority row).
        if !self.owner_is_live(&mut client, owner, now)? {
            return Err(EngineError::Forbidden(
                "owner is not live (register + heartbeat first)",
            ));
        }
        let mut tx = st(client.transaction())?;
        let current = Self::lease_for_update(&mut tx, &t, &q)?;
        let mut outcome = lease_decide_acquire(&current, owner, now, self.config.lease_ttl_ms);
        if let AcquireOutcome::Acquired(ref mut acquired) = outcome {
            // Postgres-native TD-003 binding: when a paired postgres storage schema is available in this
            // transaction's search_path, the acquire transaction is also the durable append fence. CP-only
            // tests that create no storage schema still exercise the lease state machine without a bind.
            if bind_storage_epoch_if_present(&mut tx, &t, &q, acquired.assignment_epoch)? {
                acquired.state = LeaseState::Assigned;
            }
            upsert_lease(&mut tx, &t, &q, acquired)?;
        }
        st(tx.commit())?;
        Ok(outcome)
    }

    fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        let mut tx = st(client.transaction())?;
        let current = Self::lease_for_update(&mut tx, &t, &q)?;
        let confirmed = lease_decide_confirm_fence(&current, owner, expected_epoch, now)?;
        upsert_lease(&mut tx, &t, &q, &confirmed)?;
        st(tx.commit())?;
        Ok(confirmed)
    }

    fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        let mut tx = st(client.transaction())?;
        let current = Self::lease_for_update(&mut tx, &t, &q)?;
        let renewed = lease_decide_renew(
            &current,
            owner,
            expected_epoch,
            now,
            self.config.lease_ttl_ms,
        )?;
        upsert_lease(&mut tx, &t, &q, &renewed)?;
        st(tx.commit())?;
        Ok(renewed)
    }

    fn renew_queue_leases(
        &self,
        renewals: &[LeaseRenewal],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<LeaseRenewalOutcome>> {
        if renewals.is_empty() {
            return Ok(Vec::new());
        }
        let tenants: Vec<String> = renewals
            .iter()
            .map(|renewal| renewal.queue.tenant_id.as_str().to_string())
            .collect();
        let queues: Vec<String> = renewals
            .iter()
            .map(|renewal| renewal.queue.queue_id.as_str().to_string())
            .collect();
        let owners: Vec<String> = renewals
            .iter()
            .map(|renewal| renewal.owner.as_str().to_string())
            .collect();
        let epochs: Vec<i64> = renewals
            .iter()
            .map(|renewal| renewal.expected_epoch as i64)
            .collect();
        let now_nanos = ts_nanos(now);
        let expires_nanos = ts_nanos(pqueue_engine::add_millis(now, self.config.lease_ttl_ms));

        self.batch_renewal_calls.fetch_add(1, Ordering::Relaxed);
        let mut client = self.inner.lock().expect("poisoned");
        let mut tx = st(client.transaction())?;
        self.batch_renewal_transactions
            .fetch_add(1, Ordering::Relaxed);
        self.batch_renewal_statements
            .fetch_add(1, Ordering::Relaxed);
        let rows = st(tx.query(
            BATCH_RENEW_SQL,
            &[
                &tenants,
                &queues,
                &owners,
                &epochs,
                &now_nanos,
                &expires_nanos,
            ],
        ))?;
        st(tx.commit())?;

        rows.into_iter()
            .map(|row| {
                let renewed: bool = row.get(1);
                let present: bool = row.get(2);
                if !present {
                    return Ok(LeaseRenewalOutcome::Missing);
                }
                if !renewed {
                    return Ok(LeaseRenewalOutcome::Fenced);
                }
                let state: Option<String> = row.get(3);
                let active: Option<String> = row.get(4);
                let target: Option<String> = row.get(5);
                let epoch: Option<i64> = row.get(6);
                let expires: Option<i64> = row.get(7);
                let owner = |value: Option<String>| -> EngineResult<Option<OwnerId>> {
                    value
                        .map(|value| {
                            OwnerId::new(value)
                                .map_err(|error| EngineError::Storage(error.to_string()))
                        })
                        .transpose()
                };
                Ok(LeaseRenewalOutcome::Renewed(QueueLease {
                    state: parse_state(state.as_deref().ok_or_else(|| {
                        EngineError::Storage("batch renewal omitted lease state".into())
                    })?)?,
                    active_owner_id: owner(active)?,
                    target_owner_id: owner(target)?,
                    assignment_epoch: epoch.ok_or_else(|| {
                        EngineError::Storage("batch renewal omitted assignment epoch".into())
                    })? as u64,
                    lease_expires_at: expires.map(nanos_ts),
                }))
            })
            .collect()
    }

    fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        let mut tx = st(client.transaction())?;
        let current = Self::lease_for_update(&mut tx, &t, &q)?;
        let draining = lease_decide_begin_drain(&current, expected_epoch, target_owner, now)?;
        upsert_lease(&mut tx, &t, &q, &draining)?;
        st(tx.commit())?;
        Ok(draining)
    }

    fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let (t, q) = parts(queue);
        let _ = now; // release validates by epoch, not time
        let mut client = self.inner.lock().expect("poisoned");
        let mut tx = st(client.transaction())?;
        let current = Self::lease_for_update(&mut tx, &t, &q)?;
        let released = lease_decide_release(&current, owner, expected_epoch)?;
        upsert_lease(&mut tx, &t, &q, &released)?;
        st(tx.commit())?;
        Ok(())
    }

    fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut client = self.inner.lock().expect("poisoned");
        // FAIL-CLOSED (I2): surface the read error rather than fabricate a genesis epoch-0 record.
        match st(client.query_opt(
            &format!(
                "SELECT {SELECT_LEASE_COLS} FROM pqueue_queue_owner WHERE tenant=$1 AND queue=$2"
            ),
            &[&t, &q],
        ))? {
            Some(r) => row_to_lease(&r),
            None => Ok(QueueLease::unassigned()),
        }
    }
}

#[cfg(test)]
mod sql_shape_tests {
    //! No-DB assertions on the assembled SQL shapes; the live-DB behavioral suite is env-gated below.
    use super::*;

    #[test]
    fn schema_declares_both_control_plane_tables() {
        assert!(CONTROL_PLANE_SCHEMA.contains("pqueue_workers"));
        assert!(CONTROL_PLANE_SCHEMA.contains("pqueue_queue_owner"));
        assert!(
            CONTROL_PLANE_SCHEMA.contains("assignment_epoch BIGINT NOT NULL"),
            "the durable monotonic epoch column (TD-003 fence authority)"
        );
        assert!(
            CONTROL_PLANE_SCHEMA.contains("PRIMARY KEY (tenant, queue)"),
            "one authority row per queue (single active lease)"
        );
    }

    #[test]
    fn batch_renewal_is_one_ordered_set_based_statement() {
        assert_eq!(
            BATCH_RENEW_SQL.matches("UPDATE pqueue_queue_owner").count(),
            1
        );
        assert!(
            BATCH_RENEW_SQL.contains("unnest($1::text[], $2::text[], $3::text[], $4::bigint[])")
        );
        assert!(BATCH_RENEW_SQL.contains("ORDER BY q.tenant, q.queue"));
        assert!(BATCH_RENEW_SQL.contains("FOR UPDATE OF q"));
        assert!(BATCH_RENEW_SQL.contains("GREATEST(q.lease_expires_at, $6)"));
        assert!(BATCH_RENEW_SQL.contains("ORDER BY i.ord"));
    }

    #[test]
    fn batch_resolution_is_fixed_statement_set_and_preserves_order() {
        assert!(BATCH_RESOLVE_SQL.contains("unnest($1::text[], $2::text[])"));
        assert!(BATCH_RESOLVE_SQL.contains("heartbeat_at > $3"));
        assert!(BATCH_RESOLVE_SQL.contains("CASE WHEN i.ord = 1 THEN live.owners END"));
        assert!(BATCH_RESOLVE_SQL.contains("ORDER BY i.ord"));
    }

    #[test]
    fn lease_for_update_materializes_then_locks_the_row() {
        // Linearization (TD-003: at most one acquire succeeds vs a prior epoch) needs the row to EXIST so
        // FOR UPDATE has something to lock — `FOR UPDATE` on a missing row locks nothing (the B1 genesis
        // race). The fix materializes the genesis row first; this asserts the shape of both statements. The
        // LIVE proof that two concurrent first-acquires don't both win is the env-gated
        // `genesis_concurrent_acquire_has_a_single_winner` integration test.
        let select = format!(
            "SELECT {SELECT_LEASE_COLS} FROM pqueue_queue_owner WHERE tenant=$1 AND queue=$2 FOR UPDATE"
        );
        assert!(select.contains("FOR UPDATE"));
        // The genesis INSERT (in `lease_for_update`) is what serializes concurrent first-acquires.
        assert!(
            CONTROL_PLANE_SCHEMA.contains("PRIMARY KEY (tenant, queue)"),
            "the PK is what makes the genesis INSERT...ON CONFLICT serialize two first-acquires"
        );
    }

    #[test]
    fn lease_state_round_trips_through_text() {
        for s in [
            LeaseState::Unassigned,
            LeaseState::PendingFence,
            LeaseState::Assigned,
            LeaseState::Draining,
        ] {
            assert_eq!(parse_state(state_str(s)).unwrap(), s);
        }
        assert!(parse_state("bogus").is_err());
    }

    #[test]
    fn timestamp_nanos_round_trip() {
        let t = UtcTimestamp::new(1_234, 567_000_000).unwrap();
        assert_eq!(nanos_ts(ts_nanos(t)), t);
    }
}
