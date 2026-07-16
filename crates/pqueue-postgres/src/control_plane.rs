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

use postgres::Client;
use pqueue_core::{OwnerId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, ControlPlaneConfig, EngineError, EngineResult, LeaseState,
    OwnerEndpointAdvertisement, OwnerResolution, QueueControlPlane, QueueKey, QueueLease,
    lease_decide_acquire, lease_decide_begin_drain, lease_decide_confirm_fence,
    lease_decide_release, lease_decide_renew, lease_resolution, owner_heartbeat_live,
    resolve_target,
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
        })
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
