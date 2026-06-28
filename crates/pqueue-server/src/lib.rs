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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pqueue_core::QueueDefinition;
use pqueue_engine::{Clock, EngineError, EngineResult};
use pqueue_memory::MemoryBackend;
use pqueue_objectlog::ObjectLogBackend;
use pqueue_resp::{RespBackend, SystemClock, serve_with_shutdown};
use pqueue_sqlite::SqliteBackend;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Which durable backend the server runs over.
pub enum Backend {
    /// In-memory reference backend (atomic class; non-durable).
    Memory,
    /// Sqlite durable log at `path` (atomic class).
    Sqlite(PathBuf),
    /// Object-log durable store rooted at `path` (eventual-apply class).
    ObjectLog(PathBuf),
}

/// Server configuration.
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
}

/// Observable counters for the background reclaim loop (so a swallowed tick error is countable, not
/// silent, and the reclaim work is surfaced for ops).
#[derive(Default)]
struct ReclaimCounters {
    ticks: AtomicU64,
    errors: AtomicU64,
    leases_reclaimed: AtomicU64,
}

/// A point-in-time snapshot of the reclaim loop's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    pub ticks: u64,
    pub errors: u64,
    pub leases_reclaimed: u64,
}

/// A running server: the bound address + the two background tasks (RESP accept loop + reclaim ticker).
/// The task handles are `Option` so [`Server::shutdown_and_drain`] can take ownership to await the serve
/// task; [`Drop`] aborts whatever remains.
pub struct Server {
    addr: SocketAddr,
    serve_task: Option<JoinHandle<()>>,
    reclaim_task: Option<JoinHandle<()>>,
    /// Signals the RESP serve loop to stop accepting and drain in-flight connection handlers.
    cancel: CancellationToken,
    reclaim: Arc<ReclaimCounters>,
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
    }

    /// A snapshot of the background reclaim loop's counters (ticks run, tick errors, leases reclaimed).
    pub fn reclaim_stats(&self) -> ReclaimStats {
        ReclaimStats {
            ticks: self.reclaim.ticks.load(Ordering::Relaxed),
            errors: self.reclaim.errors.load(Ordering::Relaxed),
            leases_reclaimed: self.reclaim.leases_reclaimed.load(Ordering::Relaxed),
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
            start_with(
                Arc::new(MemoryBackend::new().with_node_id(node_id)),
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
            start_with(
                Arc::new(SqliteBackend::open(p)?.with_node_id(node_id)),
                clock,
                &config.listen,
                config.reclaim_interval,
                &config.queues,
            )
            .await
        }
        Backend::ObjectLog(path) => {
            start_with(
                Arc::new(ObjectLogBackend::open(path)?.with_node_id(node_id)),
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
        cancel,
        reclaim,
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
            }
            Err(_) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
