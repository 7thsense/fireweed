#![forbid(unsafe_code)]
//! # pqueue-server
//!
//! The **composition root**: the single place that selects a concrete backend (memory / sqlite /
//! objectlog) and wires it to the two faces of pqueue. It binds the RESP front ([`pqueue_resp::serve`])
//! and runs a **background [`ReclaimDriver`] task** that periodically `tick`s the engine so expired
//! leases are reclaimed on a *quiet* queue with no client traffic — closing the orphan-on-quiet-queue
//! gap (TD-007 §3) that the client-triggered `XAUTOCLAIM` alone leaves open.
//!
//! Hexagonal: this is the ONLY crate that names concrete adapters; everything else depends only inward.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pqueue_core::{OwnerId, QueueDefinition, QueueId, TenantId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, AuthContext, Clock, EngineError, EngineResult, InMemoryControlPlane,
    LeaseState, OwnedSession, QueueControlPlane, QueueKey,
};
use pqueue_memory::MemoryBackend;
use pqueue_objectlog::ObjectLogBackend;
use pqueue_resp::{
    RespBackend, RespHooks, RouteDecision, SystemClock, route, serve_with_shutdown,
    serve_with_shutdown_and_hooks,
};
use pqueue_sqlite::SqliteBackend;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod object_log_sqlite;
pub use object_log_sqlite::{
    DEFAULT_RECOVERY_MAX_TAIL, ObjectLogSqliteBackend, SegmentedObjectLogInMemoryBackend,
    SegmentedObjectLogSqliteBackend,
};
pub use pqueue_objectlog::segmented::SegmentConfig;

/// The single optional env-var populator for [`Config`] (`Config::from_env`) plus its [`ConfigError`]. Pure
/// over a caller-supplied env map; the only process-env read lives in the `pqueue-service` bin's `main`.
#[cfg(feature = "env-config")]
mod env_config;
#[cfg(feature = "env-config")]
pub use env_config::ConfigError;

#[cfg(feature = "postgres")]
mod postgres_native;
#[cfg(feature = "postgres")]
pub use postgres_native::PostgresNativeBackend;

/// Which durable backend the server runs over.
pub enum Backend {
    /// In-memory reference backend (atomic class; non-durable).
    Memory,
    /// Sqlite durable log at `path` (atomic class).
    Sqlite(PathBuf),
    /// Object-log durable store rooted at `path` (eventual-apply class).
    ObjectLog(PathBuf),
    /// Local object-log authority plus SQLite materialized projection (per-command file object log).
    ObjectLogSqlite {
        object_root: PathBuf,
        projection_path: PathBuf,
    },
    /// Segmented group-commit object log (`SegmentedObjectLog<LocalFsBlobStore>`) plus SQLite projection.
    /// The high-throughput `object_log_sqlite_projection` variant: concurrent pushes co-buffer into one
    /// sealed segment (one durable object + one manifest-CAS + one batched SQLite apply) instead of paying
    /// a per-command object write + per-command SQLite transaction.
    SegmentedObjectLogSqlite {
        object_root: PathBuf,
        projection_path: PathBuf,
        config: SegmentConfig,
    },
    /// Segmented group-commit object log (`SegmentedObjectLog<LocalFsBlobStore>`) plus an IN-MEMORY
    /// `ProjectionData` projection (Fix B). Same durable authority + ack-after-seal coordination as
    /// `SegmentedObjectLogSqlite`, but the per-segment projection write is a cheap in-memory `apply_command`
    /// rather than a SQLite transaction; the projection is rebuilt by `read_all` replay on open. The fast
    /// durable single-node path (`PQUEUE_OBJECT_LOG_MODE=segmented` + `PQUEUE_PROJECTION_BACKEND=inmemory`).
    SegmentedObjectLogInMemory {
        object_root: PathBuf,
        config: SegmentConfig,
    },
    /// SYNC postgres durable-log adapter (atomic class), driven through the blocking-safe
    /// [`PostgresNativeBackend`] wrapper so no sync postgres client call runs on a Tokio worker thread.
    /// `url` is a libpq/postgres connection string (URL or `key=value` DSN, with a native password); with the
    /// `tls` feature an `sslmode=require|prefer` url connects over native-tls (Lakebase / cloud postgres).
    /// `credentials` optionally injects a Databricks service-principal/PAT credential at connect (the
    /// user/password is set from the provider instead of the DSN). Requires the `postgres` cargo feature.
    #[cfg(feature = "postgres")]
    PostgresNative {
        url: String,
        credentials: Option<pqueue_postgres::CredentialProvider>,
    },
}

/// Resolve the postgres `Backend` from the runtime environment, using the env names the Helm Lakebase
/// profile renders. The DSN secret is `PQUEUE_POSTGRES_LOG_DATABASE_URL` (the chart's log-backend Secret
/// ref); `PQUEUE_PG_URL` is the local/dev fallback, and the documented default is the last resort. A
/// Databricks service-principal/PAT credential provider is attached when `DATABRICKS_HOST` is present.
///
/// No plaintext fallback: if the DSN demands `sslmode=require` but this binary was built WITHOUT the `tls`
/// feature, this fails at config time rather than letting the runtime silently downgrade to `NoTls`.
///
/// This is a pure function over an env map (no live DB, no process env) so the composition-root config
/// layer is unit-testable.
#[cfg(feature = "postgres")]
pub fn resolve_postgres_backend(
    env: &std::collections::BTreeMap<String, String>,
) -> Result<Backend, String> {
    let nonempty = |key: &str| env.get(key).filter(|s| !s.is_empty()).cloned();
    let url = nonempty("PQUEUE_POSTGRES_LOG_DATABASE_URL")
        .or_else(|| nonempty("PQUEUE_PG_URL"))
        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());

    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
    let ssl_mode = pqueue_postgres::PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
    #[cfg(not(feature = "tls"))]
    if matches!(ssl_mode, pqueue_postgres::PostgresSslMode::Require) {
        return Err(
            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
             `--features postgres,tls` (no plaintext downgrade)"
                .to_string(),
        );
    }
    let _ = ssl_mode;

    // Databricks service-principal / PAT credential injection: present iff DATABRICKS_HOST is set. The
    // provider supersedes any DSN password (and sets the postgres user for service-principal OAuth).
    let credentials = if nonempty("DATABRICKS_HOST").is_some() {
        let config = pqueue_postgres::DatabricksCredentialConfig::from_env_map(env.clone())
            .map_err(|e| format!("invalid Databricks credential configuration: {e}"))?;
        let provider = pqueue_postgres::DatabricksCredentialProvider::from_config(config)
            .map_err(|e| format!("could not build Databricks credential provider: {e}"))?;
        Some(pqueue_postgres::CredentialProvider::Databricks(provider))
    } else {
        None
    };

    Ok(Backend::PostgresNative { url, credentials })
}

/// The single authoritative, fully-typed runtime configuration for a pqueue server. Every knob the server
/// needs lives here as a typed field; there is exactly ONE optional env populator (`Config::from_env`, in
/// the `pqueue-service` bin) that maps the documented `PQUEUE_*`/`DATABRICKS_*` env names onto these fields.
/// A pure-library embedder constructs this struct directly and never touches the process environment — the
/// library reads no env vars at all.
pub struct Config {
    pub backend: Backend,
    /// This instance's node id, packed into the disambiguation byte of every minted `ItemId` (ADR-009) so
    /// distinct replicas over a shared store never mint a colliding id. It is a *configured* value: the
    /// deployment is responsible for handing each replica a distinct one (e.g. the Helm chart maps a
    /// StatefulSet ordinal or pod identity into it) — the application stays infrastructure-agnostic. Build
    /// it from a configured string via [`resolve_node_id`]. `0` is the single-instance default.
    pub node_id: u8,
    /// Listen address, e.g. `"127.0.0.1:6380"` (use `":0"` for an ephemeral port in tests).
    pub listen: String,
    /// How often the background reclaim task ticks the engine.
    pub reclaim_interval: Duration,
    /// Queues to provision at startup. The RESP front has no create-queue command, so a server started
    /// with no queues here (and no out-of-band creation) would reject every request with `no such
    /// queue` — provision them up front.
    pub queues: Vec<QueueDefinition>,
    /// Recovery-window budget (max object-log tail commands) before a segmented/object-log+SQLite reopen
    /// logs a recovery-window warning. Applied to the constructed backend by [`start`]. The env populator
    /// sources it from `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS`; default [`DEFAULT_RECOVERY_MAX_TAIL`].
    pub recovery_max_tail: u64,
    /// Opt-in group-commit telemetry for the segmented+SQLite backend (the typed form of `PQUEUE_DEBUG_SEGMENTS`).
    pub debug_segments: bool,
    /// Tokio worker-thread cap (the typed form of `PQUEUE_WORKER_THREADS`). `None` = one worker per core.
    /// Consumed by the bin when building the runtime, not by [`start`].
    pub worker_threads: Option<usize>,
}

impl Config {
    /// Construct a config with the in-code defaults for the env-only knobs (recovery budget, debug-segments,
    /// worker-threads). The composition root / embedder supplies the core fields; the optional knobs default
    /// to their library defaults. Keeps call sites that don't care about the env knobs concise.
    pub fn new(
        backend: Backend,
        node_id: u8,
        listen: String,
        reclaim_interval: Duration,
        queues: Vec<QueueDefinition>,
    ) -> Self {
        Self {
            backend,
            node_id,
            listen,
            reclaim_interval,
            queues,
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            debug_segments: false,
            worker_threads: None,
        }
    }
}

pub struct OwnershipRuntime<B, CP> {
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    endpoint: String,
    owner_endpoints: Mutex<std::collections::HashMap<OwnerId, String>>,
    managed_queues: Mutex<std::collections::HashSet<QueueKey>>,
    sessions: Mutex<std::collections::HashMap<QueueKey, OwnedSession>>,
    /// Per-queue gate serializing COLD-START acquisition. `acquire_queue_lease` is non-idempotent (it bumps
    /// the epoch on every call), so two concurrent first-writes to an unowned queue would each acquire,
    /// double-bumping the epoch and fencing the laggard. This gate (taken only on the unowned path, never on
    /// the hot already-owned path) lets the first acquirer win and the rest reuse its session.
    acquire_gates: Mutex<std::collections::HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
}

impl<B, CP> OwnershipRuntime<B, CP>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    pub fn new(backend: Arc<B>, control_plane: Arc<CP>, owner: OwnerId, endpoint: String) -> Self {
        let mut endpoints = std::collections::HashMap::new();
        endpoints.insert(owner.clone(), endpoint.clone());
        Self {
            backend,
            control_plane,
            owner,
            endpoint,
            owner_endpoints: Mutex::new(endpoints),
            managed_queues: Mutex::new(std::collections::HashSet::new()),
            sessions: Mutex::new(std::collections::HashMap::new()),
            acquire_gates: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn owner(&self) -> &OwnerId {
        &self.owner
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn set_owner_endpoint(&self, owner: OwnerId, endpoint: impl Into<String>) {
        self.owner_endpoints
            .lock()
            .expect("poisoned")
            .insert(owner, endpoint.into());
    }

    pub fn watch_queue(&self, queue: QueueKey) {
        self.managed_queues.lock().expect("poisoned").insert(queue);
    }

    pub fn register_owner(&self, now: UtcTimestamp) -> EngineResult<()> {
        self.control_plane.register_owner(&self.owner, now)
    }

    pub async fn acquire_queue(&self, queue: &QueueKey, now: UtcTimestamp) -> EngineResult<()> {
        match self
            .cp_acquire(queue.clone(), self.owner.clone(), now)
            .await?
        {
            AcquireOutcome::Acquired(lease) => {
                let current_epoch = self.backend.current_epoch(queue).await?;
                let fence_epoch = if current_epoch == lease.assignment_epoch {
                    current_epoch
                } else if current_epoch < lease.assignment_epoch {
                    self.backend.acquire_epoch(queue).await?
                } else {
                    return Err(EngineError::EpochFenced);
                };
                let session = OwnedSession {
                    owner: self.owner.clone(),
                    queue: queue.clone(),
                    lease_epoch: lease.assignment_epoch,
                    fence_epoch,
                };
                self.sessions
                    .lock()
                    .expect("poisoned")
                    .insert(queue.clone(), session);
                Ok(())
            }
            AcquireOutcome::Rejected(_) => Err(EngineError::Unavailable),
        }
    }

    pub async fn renew_sessions(&self, now: UtcTimestamp) -> EngineResult<()> {
        self.cp_heartbeat(self.owner.clone(), now).await?;
        let mut queues: std::collections::BTreeSet<QueueKey> = self
            .managed_queues
            .lock()
            .expect("poisoned")
            .iter()
            .cloned()
            .collect();
        queues.extend(self.sessions.lock().expect("poisoned").keys().cloned());
        for queue in queues {
            let resolution = self.cp_resolve(queue.clone(), now).await?;
            if resolution.active_owner.as_ref() == Some(&self.owner)
                && resolution
                    .target_owner
                    .as_ref()
                    .is_some_and(|target| target != &self.owner)
                && resolution.state == LeaseState::Assigned
                && let Some(active_epoch) = resolution.assignment_epoch
            {
                let _ = self
                    .cp_begin_drain(
                        queue.clone(),
                        active_epoch,
                        resolution.target_owner.as_ref().expect("checked").clone(),
                        now,
                    )
                    .await?;
            }
            let session = self.sessions.lock().expect("poisoned").get(&queue).cloned();
            match (resolution.state, resolution.active_owner.as_ref(), session) {
                (LeaseState::Assigned, Some(owner), Some(session)) if owner == &self.owner => {
                    match self
                        .cp_renew(queue.clone(), self.owner.clone(), session.lease_epoch, now)
                        .await
                    {
                        Ok(_) => {}
                        Err(EngineError::EpochFenced) => {
                            self.sessions.lock().expect("poisoned").remove(&queue);
                        }
                        Err(e) => return Err(e),
                    }
                }
                (LeaseState::Draining, Some(owner), Some(session)) if owner == &self.owner => {
                    let metrics = self.backend.metrics(&queue).await?;
                    if metrics.leased == 0 {
                        self.cp_release(
                            queue.clone(),
                            self.owner.clone(),
                            session.lease_epoch,
                            now,
                        )
                        .await?;
                        self.sessions.lock().expect("poisoned").remove(&queue);
                    } else {
                        match self
                            .cp_renew(queue.clone(), self.owner.clone(), session.lease_epoch, now)
                            .await
                        {
                            Ok(_) => {}
                            Err(EngineError::EpochFenced) => {
                                self.sessions.lock().expect("poisoned").remove(&queue);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                (LeaseState::Unassigned, None, _)
                    if resolution.target_owner.as_ref() == Some(&self.owner) =>
                {
                    match self.acquire_queue(&queue, now).await {
                        Ok(()) | Err(EngineError::Unavailable) => {}
                        Err(e) => return Err(e),
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn ensure_epoch(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<Option<u64>> {
        self.cp_register(self.owner.clone(), now).await?;
        let mut resolution = self.cp_resolve(queue.clone(), now).await?;
        if resolution.active_owner.as_ref() == Some(&self.owner)
            && resolution
                .target_owner
                .as_ref()
                .is_some_and(|target| target != &self.owner)
            && resolution.state == LeaseState::Assigned
            && let (Some(epoch), Some(target)) = (
                resolution.assignment_epoch,
                resolution.target_owner.as_ref(),
            )
        {
            self.cp_begin_drain(queue.clone(), epoch, target.clone(), now)
                .await?;
            resolution = self.cp_resolve(queue.clone(), now).await?;
        }
        match (resolution.state, resolution.active_owner.as_ref()) {
            (LeaseState::Assigned, Some(owner)) | (LeaseState::Draining, Some(owner))
                if owner == &self.owner =>
            {
                if resolution.state == LeaseState::Draining && is_new_claim {
                    return Err(EngineError::Unavailable);
                }
                let epoch = resolution
                    .assignment_epoch
                    .ok_or(EngineError::Unavailable)?;
                // Hot path: already own this queue → no gate, just (re)validate the cached session.
                self.establish_owned_session(queue, epoch).await
            }
            (_, Some(_)) => Err(EngineError::Unavailable),
            (LeaseState::Unassigned, None)
                if resolution.target_owner.as_ref() == Some(&self.owner) =>
            {
                // Cold start: serialize acquisition per queue (the non-idempotent epoch bump must happen at
                // most once across concurrent first-writes).
                let gate = self.acquire_gate_for(queue);
                let _g = gate.lock().await;
                // Re-resolve under the gate: a peer first-write may have acquired while we waited.
                let resolution = self.cp_resolve(queue.clone(), now).await?;
                match (resolution.state, resolution.active_owner.as_ref()) {
                    (LeaseState::Assigned, Some(owner)) | (LeaseState::Draining, Some(owner))
                        if owner == &self.owner =>
                    {
                        if resolution.state == LeaseState::Draining && is_new_claim {
                            return Err(EngineError::Unavailable);
                        }
                        let epoch = resolution
                            .assignment_epoch
                            .ok_or(EngineError::Unavailable)?;
                        self.establish_owned_session(queue, epoch).await
                    }
                    (_, Some(_)) => Err(EngineError::Unavailable),
                    (LeaseState::Unassigned, None)
                        if resolution.target_owner.as_ref() == Some(&self.owner) =>
                    {
                        self.acquire_queue(queue, now).await?;
                        let sessions = self.sessions.lock().expect("poisoned");
                        Ok(sessions.get(queue).map(|session| session.fence_epoch))
                    }
                    _ => Err(EngineError::Unavailable),
                }
            }
            _ => Err(EngineError::Unavailable),
        }
    }

    /// (Re)establish the cached fence session for a queue this owner holds at `epoch`, returning the fence
    /// epoch passed to write commands. Reuses a still-valid cached session; otherwise reads the backend's
    /// authoritative epoch and caches it (fencing if the backend epoch no longer matches the lease).
    async fn establish_owned_session(
        &self,
        queue: &QueueKey,
        epoch: u64,
    ) -> EngineResult<Option<u64>> {
        let existing = {
            let sessions = self.sessions.lock().expect("poisoned");
            sessions
                .get(queue)
                .and_then(|session| (session.lease_epoch == epoch).then_some(session.fence_epoch))
        };
        if existing.is_some() {
            return Ok(existing);
        }
        let fence_epoch = self.backend.current_epoch(queue).await?;
        if fence_epoch != epoch {
            return Err(EngineError::EpochFenced);
        }
        let session = OwnedSession {
            owner: self.owner.clone(),
            queue: queue.clone(),
            lease_epoch: epoch,
            fence_epoch,
        };
        self.sessions
            .lock()
            .expect("poisoned")
            .insert(queue.clone(), session);
        Ok(Some(fence_epoch))
    }

    fn acquire_gate_for(&self, queue: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        self.acquire_gates
            .lock()
            .expect("poisoned")
            .entry(queue.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn cp_register(&self, owner: OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.register_owner(&owner, now)).await
    }

    async fn cp_heartbeat(&self, owner: OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.heartbeat(&owner, now)).await
    }

    async fn cp_resolve(
        &self,
        queue: QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::OwnerResolution> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.resolve_queue_owner(&queue, now)).await
    }

    async fn cp_acquire(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.acquire_queue_lease(&queue, &owner, now)).await
    }

    async fn cp_renew(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::QueueLease> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.renew_queue_lease(&queue, &owner, expected_epoch, now))
            .await
    }

    async fn cp_begin_drain(
        &self,
        queue: QueueKey,
        expected_epoch: u64,
        target_owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::QueueLease> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.begin_drain(&queue, expected_epoch, &target_owner, now))
            .await
    }

    async fn cp_release(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.release_queue_lease(&queue, &owner, expected_epoch, now))
            .await
    }
}

async fn blocking_control_plane<T, F>(f: F) -> EngineResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> EngineResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| EngineError::Storage(format!("control-plane task failed: {e}")))?
}

impl<B, CP> RespHooks for OwnershipRuntime<B, CP>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    async fn route_command(
        &self,
        _name: &str,
        _args: &[Vec<u8>],
        routing_key: &[u8],
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<RouteDecision> {
        let Ok(queue) = parse_resp_queue_key(routing_key) else {
            return Ok(RouteDecision::Serve);
        };
        let auth = AuthContext::new("resp", [queue.tenant_id.as_str()]);
        if auth.authorize_tenant(queue.tenant_id.as_str()).is_err() {
            return Ok(RouteDecision::NoPerm);
        }
        self.cp_register(self.owner.clone(), now).await?;
        let resolution = self.cp_resolve(queue.clone(), now).await?;
        if resolution.state == LeaseState::Unassigned
            && resolution.target_owner.as_ref() == Some(&self.owner)
        {
            self.acquire_queue(&queue, now).await?;
            return Ok(RouteDecision::Serve);
        }
        let endpoints = self.owner_endpoints.lock().expect("poisoned").clone();
        Ok(route(
            &self.owner,
            &queue,
            routing_key,
            &auth,
            &resolution,
            |owner| endpoints.get(owner).cloned(),
            is_new_claim,
        ))
    }

    async fn expected_epoch_for_write(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<Option<u64>> {
        self.ensure_epoch(shard, now, is_new_claim).await
    }
}

fn parse_resp_queue_key(key: &[u8]) -> EngineResult<QueueKey> {
    let s = std::str::from_utf8(key).map_err(|_| EngineError::Invalid("non-utf8 key"))?;
    let (tenant, queue) = match s.split_once(':') {
        Some((t, q)) => (t, q),
        None => ("default", s),
    };
    Ok(QueueKey::new(
        TenantId::new(tenant).map_err(|_| EngineError::Invalid("bad tenant"))?,
        QueueId::new(queue).map_err(|_| EngineError::Invalid("bad queue"))?,
    ))
}

/// Observable counters for the background reclaim loop (so a swallowed tick error is countable, not
/// silent, and the reclaim work is surfaced for ops).
#[derive(Default)]
struct ReclaimCounters {
    ticks: AtomicU64,
    errors: AtomicU64,
    leases_reclaimed: AtomicU64,
    cohorts_expired: AtomicU64,
}

#[derive(Default)]
struct OwnershipCounters {
    ticks: AtomicU64,
    errors: AtomicU64,
}

/// A point-in-time snapshot of the reclaim loop's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    pub ticks: u64,
    pub errors: u64,
    pub leases_reclaimed: u64,
    pub cohorts_expired: u64,
}

/// A point-in-time snapshot of the ownership loop's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnershipStats {
    pub ticks: u64,
    pub errors: u64,
}

/// A running server: the bound address + the two background tasks (RESP accept loop + reclaim ticker).
/// The task handles are `Option` so [`Server::shutdown_and_drain`] can take ownership to await the serve
/// task; [`Drop`] aborts whatever remains.
pub struct Server {
    addr: SocketAddr,
    serve_task: Option<JoinHandle<()>>,
    reclaim_task: Option<JoinHandle<()>>,
    ownership_task: Option<JoinHandle<()>>,
    /// Signals the RESP serve loop to stop accepting and drain in-flight connection handlers.
    cancel: CancellationToken,
    reclaim: Arc<ReclaimCounters>,
    ownership: Arc<OwnershipCounters>,
}

impl Server {
    /// The actually-bound listen address (resolves `:0` to the OS-assigned port).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Liveness probe: neither background task has panicked/aborted/finished. NOTE: this is task
    /// liveness, not deep readiness — it does not prove the listener accepts or that reclaim ticks
    /// succeed. Pair with [`Server::reclaim_stats`] to detect a tick that is erroring every cycle.
    pub fn is_running(&self) -> bool {
        self.serve_task.as_ref().is_some_and(|t| !t.is_finished())
            && self.reclaim_task.as_ref().is_some_and(|t| !t.is_finished())
            && self
                .ownership_task
                .as_ref()
                .is_none_or(|t| !t.is_finished())
    }

    /// A snapshot of the background reclaim loop's counters (ticks run, tick errors, leases reclaimed,
    /// cohorts expired).
    pub fn reclaim_stats(&self) -> ReclaimStats {
        ReclaimStats {
            ticks: self.reclaim.ticks.load(Ordering::Relaxed),
            errors: self.reclaim.errors.load(Ordering::Relaxed),
            leases_reclaimed: self.reclaim.leases_reclaimed.load(Ordering::Relaxed),
            cohorts_expired: self.reclaim.cohorts_expired.load(Ordering::Relaxed),
        }
    }

    /// A snapshot of the ownership loop's counters (renew ticks and renewal/resolve errors).
    pub fn ownership_stats(&self) -> OwnershipStats {
        OwnershipStats {
            ticks: self.ownership.ticks.load(Ordering::Relaxed),
            errors: self.ownership.errors.load(Ordering::Relaxed),
        }
    }

    /// Stop serving and stop the reclaim ticker, synchronously. Signals the drain token (so the serve
    /// loop stops accepting) and then **aborts** both background tasks immediately — it does NOT wait for
    /// in-flight connection handlers to drain. Being sync, it is safe to call from [`Drop`] and from the
    /// existing non-async call sites. For a bounded graceful drain, use [`Server::shutdown_and_drain`].
    pub fn shutdown(&self) {
        self.cancel.cancel();
        if let Some(t) = &self.serve_task {
            t.abort();
        }
        if let Some(t) = &self.reclaim_task {
            t.abort();
        }
        if let Some(t) = &self.ownership_task {
            t.abort();
        }
    }

    /// Gracefully stop: signal the serve loop to stop accepting and **drain** in-flight connection
    /// handlers (each finishes its current command, then exits), awaiting them up to `timeout`. Past the
    /// bound the serve task is aborted; because the serve loop owns the handlers in a `JoinSet`, aborting
    /// it drops the set and hard-aborts any handler still running — so the bound is real, not best-effort.
    /// The reclaim ticker is aborted (it holds no client work). Consumes the server.
    pub async fn shutdown_and_drain(mut self, timeout: Duration) {
        self.cancel.cancel();
        if let Some(mut serve) = self.serve_task.take()
            && tokio::time::timeout(timeout, &mut serve).await.is_err()
        {
            serve.abort();
        }
        if let Some(reclaim) = self.reclaim_task.take() {
            reclaim.abort();
        }
        if let Some(ownership) = self.ownership_task.take() {
            ownership.abort();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn io_err(e: std::io::Error) -> EngineError {
    EngineError::Storage(e.to_string())
}

/// Resolve a *configured* node-identity string into the 8-bit `node_id` packed into every minted `ItemId`.
/// A plain integer already in `0..=255` is used verbatim (the clean operator-assigned case); anything else
/// — an out-of-range number, a hostname, or a pod name/UID the deployment wired in — is hashed into a `u8`.
/// This keeps the application infrastructure-agnostic: the deployment decides what identity to pass, and
/// this only guarantees it lands in range. (NOTE: the hash path lives in a 256-value space, so for very
/// large fleets prefer configuring distinct small integers directly; `node_id` is defense-in-depth anyway.)
pub fn resolve_node_id(configured: &str) -> u8 {
    match configured.trim().parse::<u8>() {
        Ok(n) => n,
        Err(_) => {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            configured.trim().hash(&mut h);
            (h.finish() & 0xFF) as u8
        }
    }
}

/// Construct the configured backend + a `SystemClock`, provision the config's queues, then run the
/// server. After this returns the server is ready to serve requests against the provisioned queues.
pub async fn start(config: Config) -> EngineResult<Server> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let node_id = config.node_id;
    match config.backend {
        Backend::Memory => {
            let backend = Arc::new(MemoryBackend::new().with_node_id(node_id));
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::Sqlite(path) => {
            let p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
            let backend = Arc::new(SqliteBackend::open(p)?.with_node_id(node_id));
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::ObjectLog(path) => {
            let backend = Arc::new(ObjectLogBackend::open(path)?.with_node_id(node_id));
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::ObjectLogSqlite {
            object_root,
            projection_path,
        } => {
            let p = projection_path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
            let backend = Arc::new(
                ObjectLogSqliteBackend::open(object_root, p)?
                    .with_node_id(node_id)
                    .with_recovery_max_tail(config.recovery_max_tail),
            );
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::SegmentedObjectLogSqlite {
            object_root,
            projection_path,
            config: segment_config,
        } => {
            let p = projection_path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
            let backend = Arc::new(
                SegmentedObjectLogSqliteBackend::open(object_root, p, segment_config)?
                    .with_node_id(node_id)
                    .with_recovery_max_tail(config.recovery_max_tail)
                    .with_debug_segments(config.debug_segments),
            );
            // The flusher seals latency-due segments so a buffer below `target_bytes` still acks promptly.
            backend.spawn_flusher();
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::SegmentedObjectLogInMemory {
            object_root,
            config: segment_config,
        } => {
            let backend = Arc::new(
                SegmentedObjectLogInMemoryBackend::open(object_root, segment_config)?
                    .with_node_id(node_id),
            );
            backend.spawn_flusher();
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        Backend::PostgresNative { url, credentials } => {
            // The sync postgres `connect` (client handshake + log replay) MUST run off the reactor: the
            // postgres client drives its own internal runtime per call, so connecting on a Tokio worker
            // would panic ("cannot start a runtime from within a runtime"). Connect inside `spawn_blocking`,
            // then drive the backend only through the blocking-safe wrapper.
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                pqueue_postgres::PostgresBackend::connect_with_config(connect_config)
                    .map(|b| b.with_node_id(node_id))
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres connect task join failed: {e}"))
            })??;
            let backend = Arc::new(PostgresNativeBackend::new(backend));
            // Mirror every other backend's ownership wiring: an in-memory control plane coordinates lease
            // ownership (single-node default); the postgres `queues.assignment_epoch` is the durable fence.
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{node_id}"))
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            start_with_ownership(
                backend,
                cp,
                owner,
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
    }
}

/// Run the server over an already-constructed backend + clock (the generic core; tests inject a
/// controllable clock and keep a handle to the backend). `queues` are created before serving.
pub async fn start_with<B: RespBackend>(
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server> {
    // Provision queues up front (idempotent create), so the wire surface — which has no create-queue
    // command — has something to serve. A definition conflict surfaces as a structured error.
    for def in queues {
        backend.create_queue(def.clone()).await?;
    }
    let listener = TcpListener::bind(listen).await.map_err(io_err)?;
    let addr = listener.local_addr().map_err(io_err)?;
    let reclaim = Arc::new(ReclaimCounters::default());
    let ownership = Arc::new(OwnershipCounters::default());
    let cancel = CancellationToken::new();
    let serve_task = tokio::spawn(serve_with_shutdown(
        listener,
        backend.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = tokio::spawn(reclaim_loop(
        backend,
        clock,
        reclaim_interval,
        reclaim.clone(),
    ));
    Ok(Server {
        addr,
        serve_task: Some(serve_task),
        reclaim_task: Some(reclaim_task),
        ownership_task: None,
        cancel,
        reclaim,
        ownership,
    })
}

pub async fn start_with_ownership<B, CP>(
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    for def in queues {
        backend.create_queue(def.clone()).await?;
    }
    let listener = TcpListener::bind(listen).await.map_err(io_err)?;
    let addr = listener.local_addr().map_err(io_err)?;
    let endpoint = {
        let ip = addr.ip();
        let host = if ip.is_unspecified() {
            "127.0.0.1".to_string()
        } else {
            ip.to_string()
        };
        format!("{host}:{}", addr.port())
    };
    let hooks = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        control_plane,
        owner,
        endpoint,
    ));
    let now = clock.now();
    hooks.register_owner(now)?;
    for def in queues {
        let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        hooks.watch_queue(queue.clone());
        match hooks.acquire_queue(&queue, now).await {
            Ok(()) | Err(EngineError::Unavailable) => {}
            Err(e) => return Err(e),
        }
    }

    let reclaim = Arc::new(ReclaimCounters::default());
    let ownership = Arc::new(OwnershipCounters::default());
    let cancel = CancellationToken::new();
    let serve_task = tokio::spawn(serve_with_shutdown_and_hooks(
        listener,
        backend.clone(),
        hooks.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = tokio::spawn(reclaim_loop(
        backend,
        clock.clone(),
        reclaim_interval,
        reclaim.clone(),
    ));
    let ownership_task = tokio::spawn(ownership_loop(
        hooks,
        clock,
        reclaim_interval,
        ownership.clone(),
    ));
    Ok(Server {
        addr,
        serve_task: Some(serve_task),
        reclaim_task: Some(reclaim_task),
        ownership_task: Some(ownership_task),
        cancel,
        reclaim,
        ownership,
    })
}

/// The background reclaim driver: every `interval`, `tick(now)` so expired leases are reclaimed without
/// any client traffic (TD-007 §3). Best-effort + idempotent (the engine's `tick` makes no transitions at
/// the same/earlier `now`). A tick error is COUNTED (not silently dropped) so a persistently-failing
/// reclaim is observable via [`Server::reclaim_stats`] rather than hiding behind a green liveness probe.
async fn reclaim_loop<B: RespBackend>(
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    counters: Arc<ReclaimCounters>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        counters.ticks.fetch_add(1, Ordering::Relaxed);
        match backend.tick(clock.now()).await {
            Ok(report) => {
                if report.leases_reclaimed > 0 {
                    counters
                        .leases_reclaimed
                        .fetch_add(report.leases_reclaimed, Ordering::Relaxed);
                }
                if report.cohorts_expired > 0 {
                    counters
                        .cohorts_expired
                        .fetch_add(report.cohorts_expired, Ordering::Relaxed);
                }
            }
            Err(_) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn ownership_loop<B, CP>(
    hooks: Arc<OwnershipRuntime<B, CP>>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    counters: Arc<OwnershipCounters>,
) where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        counters.ticks.fetch_add(1, Ordering::Relaxed);
        if hooks.renew_sessions(clock.now()).await.is_err() {
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
