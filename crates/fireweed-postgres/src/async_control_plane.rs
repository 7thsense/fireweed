//! Native async PostgreSQL ownership; network and transaction calls are awaited.
use crate::control_plane::{
    BATCH_RENEW_SQL, BATCH_RESOLVE_SQL, CONTROL_PLANE_SCHEMA, PostgresControlPlaneDiagnostics,
    SELECT_LEASE_COLS, nanos_ts, optional_lease, parse_state, parts, row_to_lease, st, state_str,
    ts_nanos,
};
use async_trait::async_trait;
use fireweed_core::{OwnerId, UtcTimestamp};
use fireweed_engine::{
    AcquireOutcome, AsyncQueueControlPlane, ControlPlaneConfig, EngineError, EngineResult,
    LeaseRenewal, LeaseRenewalOutcome, OwnerEndpointAdvertisement, OwnerResolution, QueueKey,
    QueueLease, lease_decide_acquire, lease_decide_begin_drain, lease_decide_confirm_fence,
    lease_decide_release, lease_decide_renew, lease_resolution, owner_heartbeat_live,
    resolve_target,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
#[cfg(not(feature = "tls"))]
use tokio_postgres::NoTls;
use tokio_postgres::{Client, Transaction};

/// Async worker registry and queue lease authority in one application database.
/// The native S3 log supplies storage fencing; PostgreSQL publishes serving only
/// after the async ownership sequence confirms that fencing and hydration.
pub struct AsyncPostgresControlPlane {
    config: ControlPlaneConfig,
    inner: Mutex<Option<Client>>,
    driver: Mutex<Option<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>>,
    batch_renewal_calls: AtomicU64,
    batch_renewal_transactions: AtomicU64,
    batch_renewal_statements: AtomicU64,
    batch_resolution_calls: AtomicU64,
    batch_resolution_statements: AtomicU64,
}

impl AsyncPostgresControlPlane {
    /// Connect and initialize an isolated application-owned control-plane schema.
    ///
    /// # Errors
    /// Returns invalid configuration, transport, or schema setup failures.
    pub async fn connect_in_schema(
        url: &str,
        schema: &str,
        config: ControlPlaneConfig,
    ) -> EngineResult<Self> {
        if schema.is_empty()
            || schema.len() > 63
            || !schema
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(EngineError::Invalid(
                "control-plane schema must be 1..63 ASCII letters, digits or underscores",
            ));
        }
        #[cfg(not(feature = "tls"))]
        let (mut client, connection) = st(tokio_postgres::connect(url, NoTls).await)?;
        #[cfg(feature = "tls")]
        let (mut client, connection) = {
            let tls = native_tls::TlsConnector::new()
                .map_err(|error| EngineError::Storage(error.to_string()))?;
            st(tokio_postgres::connect(url, postgres_native_tls::MakeTlsConnector::new(tls)).await)?
        };
        let driver = tokio::spawn(connection);
        let tx = st(client.transaction().await)?;
        st(tx
            .query(
                "SELECT pg_advisory_xact_lock(hashtext(current_database()),hashtext($1))",
                &[&schema],
            )
            .await)?;
        st(tx
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS \"{schema}\"; SET search_path TO \"{schema}\";"
            ))
            .await)?;
        st(tx.batch_execute(CONTROL_PLANE_SCHEMA).await)?;
        st(tx.commit().await)?;
        Ok(Self {
            config,
            inner: Mutex::new(Some(client)),
            driver: Mutex::new(Some(driver)),
            batch_renewal_calls: AtomicU64::new(0),
            batch_renewal_transactions: AtomicU64::new(0),
            batch_renewal_statements: AtomicU64::new(0),
            batch_resolution_calls: AtomicU64::new(0),
            batch_resolution_statements: AtomicU64::new(0),
        })
    }
    /// Close the connection and await its driver without blocking a runtime worker.
    pub async fn shutdown(&self) -> EngineResult<()> {
        drop(self.inner.lock().await.take());
        let mut driver_guard = self.driver.lock().await;
        if let Some(driver) = driver_guard.take() {
            st(driver.await.map_err(|error| {
                EngineError::Storage(format!("control-plane driver task failed: {error}"))
            })?)?;
        }
        Ok(())
    }

    /// Structural counters; reading them performs no I/O.
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
    async fn lease_for_update(tx: &Transaction<'_>, t: &str, q: &str) -> EngineResult<QueueLease> {
        st(tx.execute(
            "INSERT INTO fireweed_queue_owner \
             (tenant,queue,state,active_owner_id,target_owner_id,assignment_epoch,lease_expires_at) \
             VALUES ($1,$2,'unassigned',NULL,NULL,0,NULL) ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q],
        ).await)?;
        let row = st(tx
            .query_opt(
                &format!(
                    "SELECT {SELECT_LEASE_COLS} FROM fireweed_queue_owner \
                 WHERE tenant=$1 AND queue=$2 FOR UPDATE"
                ),
                &[&t, &q],
            )
            .await)?;
        match row {
            Some(r) => row_to_lease(&r),
            // The genesis INSERT guarantees a row; absence here would be a concurrent DELETE we don't issue.
            None => Err(EngineError::Storage(
                "lease row missing after genesis insert".into(),
            )),
        }
    }
    async fn owner_is_live(
        &self,
        client: &mut Client,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<bool> {
        let row = st(client
            .query_opt(
                "SELECT heartbeat_at FROM fireweed_workers WHERE owner_id=$1",
                &[&owner.as_str()],
            )
            .await)?;
        Ok(match row {
            Some(r) => {
                let hb: i64 = r.get(0);
                owner_heartbeat_live(nanos_ts(hb), now, self.config.heartbeat_ttl_ms)
            }
            None => false,
        })
    }
}

async fn upsert_lease(
    tx: &Transaction<'_>,
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
    st(tx
        .execute(
            "INSERT INTO fireweed_queue_owner \
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
        )
        .await)?;
    Ok(())
}

#[async_trait]
impl AsyncQueueControlPlane for AsyncPostgresControlPlane {
    async fn unregister_owner(&self, owner: &OwnerId) -> EngineResult<()> {
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        st(client
            .execute(
                "DELETE FROM fireweed_workers WHERE owner_id=$1",
                &[&owner.as_str()],
            )
            .await)?;
        Ok(())
    }

    async fn shutdown(&self) -> EngineResult<()> {
        Self::shutdown(self).await
    }

    async fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        st(client
            .execute(
                "INSERT INTO fireweed_workers (owner_id, heartbeat_at) VALUES ($1,$2) \
             ON CONFLICT (owner_id) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at",
                &[&owner.as_str(), &ts_nanos(now)],
            )
            .await)?;
        Ok(())
    }
    async fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        st(client.execute(
            "INSERT INTO fireweed_workers (owner_id, heartbeat_at, endpoint) VALUES ($1,$2,$3) \
             ON CONFLICT (owner_id) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at, endpoint=EXCLUDED.endpoint",
            &[&owner.as_str(), &ts_nanos(now), &endpoint],
        ).await)?;
        Ok(())
    }
    async fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        let cutoff =
            ts_nanos(now) - (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000);
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        let rows = st(client
            .query(
                "SELECT owner_id, endpoint, heartbeat_at FROM fireweed_workers \
             WHERE heartbeat_at > $1 AND endpoint IS NOT NULL ORDER BY owner_id",
                &[&cutoff],
            )
            .await)?;
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
    async fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        // Identical upsert: a heartbeat from an unknown owner re-admits it (register-on-heartbeat).
        self.register_owner(owner, now).await
    }
    async fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        let (t, q) = parts(queue);
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        // FAIL-CLOSED (I2): a query failure is SURFACED, never swallowed into a fabricated `unassigned`
        // record (which would invite a spurious acquire / a bogus epoch-0 fence value).
        let cutoff =
            ts_nanos(now) - (self.config.heartbeat_ttl_ms as i64).saturating_mul(1_000_000);
        let rows = st(client
            .query(
                "SELECT owner_id FROM fireweed_workers WHERE heartbeat_at > $1",
                &[&cutoff],
            )
            .await)?;
        let live: Vec<OwnerId> = rows
            .iter()
            .filter_map(|r| OwnerId::new(r.get::<_, String>(0)).ok())
            .collect();
        let target = resolve_target(queue, live.iter());
        // Current authority record (no lock needed for a read-only resolve).
        let current = match st(client.query_opt(
            &format!(
                "SELECT {SELECT_LEASE_COLS} FROM fireweed_queue_owner WHERE tenant=$1 AND queue=$2"
            ),
            &[&t, &q],
        ).await)? {
            Some(r) => row_to_lease(&r)?,
            None => QueueLease::unassigned(),
        };
        Ok(lease_resolution(&current, target, now))
    }
    async fn resolve_queue_owners(
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
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        self.batch_resolution_statements
            .fetch_add(1, Ordering::Relaxed);
        let rows = st(client
            .query(BATCH_RESOLVE_SQL, &[&tenants, &queue_ids, &cutoff])
            .await)?;
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
    async fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let (t, q) = parts(queue);
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        // Fail-closed: only a live registered owner may acquire (checked before the txn — a dead owner
        // never reaches the authority row).
        if !self.owner_is_live(client, owner, now).await? {
            return Err(EngineError::Forbidden(
                "owner is not live (register + heartbeat first)",
            ));
        }
        let tx = st(client.transaction().await)?;
        let current = Self::lease_for_update(&tx, &t, &q).await?;
        let outcome = lease_decide_acquire(&current, owner, now, self.config.lease_ttl_ms);
        if let AcquireOutcome::Acquired(ref acquired) = outcome {
            upsert_lease(&tx, &t, &q, acquired).await?;
        }
        st(tx.commit().await)?;
        Ok(outcome)
    }
    async fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        let tx = st(client.transaction().await)?;
        let current = Self::lease_for_update(&tx, &t, &q).await?;
        let confirmed = lease_decide_confirm_fence(&current, owner, expected_epoch, now)?;
        upsert_lease(&tx, &t, &q, &confirmed).await?;
        st(tx.commit().await)?;
        Ok(confirmed)
    }
    async fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        let tx = st(client.transaction().await)?;
        let current = Self::lease_for_update(&tx, &t, &q).await?;
        let renewed = lease_decide_renew(
            &current,
            owner,
            expected_epoch,
            now,
            self.config.lease_ttl_ms,
        )?;
        upsert_lease(&tx, &t, &q, &renewed).await?;
        st(tx.commit().await)?;
        Ok(renewed)
    }
    async fn renew_queue_leases(
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
        let expires_nanos = ts_nanos(fireweed_engine::add_millis(now, self.config.lease_ttl_ms));

        self.batch_renewal_calls.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        let tx = st(client.transaction().await)?;
        self.batch_renewal_transactions
            .fetch_add(1, Ordering::Relaxed);
        self.batch_renewal_statements
            .fetch_add(1, Ordering::Relaxed);
        let rows = st(tx
            .query(
                BATCH_RENEW_SQL,
                &[
                    &tenants,
                    &queues,
                    &owners,
                    &epochs,
                    &now_nanos,
                    &expires_nanos,
                ],
            )
            .await)?;
        st(tx.commit().await)?;

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
    async fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        let tx = st(client.transaction().await)?;
        let current = Self::lease_for_update(&tx, &t, &q).await?;
        let draining = lease_decide_begin_drain(&current, expected_epoch, target_owner, now)?;
        upsert_lease(&tx, &t, &q, &draining).await?;
        st(tx.commit().await)?;
        Ok(draining)
    }
    async fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let (t, q) = parts(queue);
        let _ = now; // release validates by epoch, not time
        let mut guard = self.inner.lock().await;
        let client = guard.as_mut().ok_or(EngineError::Unavailable)?;
        let tx = st(client.transaction().await)?;
        let current = Self::lease_for_update(&tx, &t, &q).await?;
        let released = lease_decide_release(&current, owner, expected_epoch)?;
        upsert_lease(&tx, &t, &q, &released).await?;
        st(tx.commit().await)?;
        Ok(())
    }
    async fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        let (t, q) = parts(queue);
        let guard = self.inner.lock().await;
        let client = guard.as_ref().ok_or(EngineError::Unavailable)?;
        // FAIL-CLOSED (I2): surface the read error rather than fabricate a genesis epoch-0 record.
        match st(client.query_opt(
            &format!(
                "SELECT {SELECT_LEASE_COLS} FROM fireweed_queue_owner WHERE tenant=$1 AND queue=$2"
            ),
            &[&t, &q],
        ).await)? {
            Some(r) => row_to_lease(&r),
            None => Ok(QueueLease::unassigned()),
        }
    }
}
