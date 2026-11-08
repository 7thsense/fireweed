//! `Queue::s3_turso` (production) and `Queue::ephemeral_disk` (development and
//! tests): option validation, resource derivation and assembly of the native
//! pipeline. Everything fallible without I/O is checked before the first
//! connection or file is made.
use super::{Queue, QueueStore};
use crate::{
    Clock, ConfigSecret, OwnerId, SystemClock,
    queue_recovery_pipeline::{
        NativeQueueStore, QueueOwnership, QueuePipelineOptions, QueueRecoveryPipeline,
        SystemLeaseClock,
    },
};
use fireweed_core::queue_contract::*;
use fireweed_core::queue_runtime::QueueRuntimeOptions;
use fireweed_engine::{
    AsyncQueueControlPlane, ControlPlaneConfig, InMemoryAsyncControlPlane, InMemoryControlPlane,
    queue_contract::QueueFuture, queue_runtime::ValidatedQueueRuntime,
};
use fireweed_objectlog::queue_native_log::{
    QueueNativeLog, QueueNativeLogOptions, QueueNativeS3Connection, QueueNativeS3TransportOptions,
};
use fireweed_postgres::AsyncPostgresControlPlane;
use fireweed_queue_spool::{ScratchDiskQuota, ScratchRoot, ScratchWorkers};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// Scratch workers when `QueueRuntimeOptions::scratch_workers` is None; the same
/// default as `PostgresOptions::new`'s pool size.
const DEFAULT_SCRATCH_WORKERS: usize = 2;
/// The version-metadata bound handed to the native log, as in the reference runner.
const NATIVE_VERSION_BYTES: usize = 4096;
/// Lease timing of an ephemeral handle. Its control plane lives in this process
/// and nothing else competes for its queues, so no other process's clock can skew
/// against it and the skew allowance is zero (a queue is served at once). The
/// lease must still outlast the slowest single operation, since renewal happens
/// between batches: a loaded debug build takes seconds on large requests.
const EPHEMERAL_LEASE_TTL_MS: u64 = 60_000;
const EPHEMERAL_HEARTBEAT_TTL_MS: u64 = 20_000;

/// Production storage: S3 log, local Turso cache, PostgreSQL control plane.
#[derive(Clone)]
pub struct S3TursoOptions {
    pub log: S3LogOptions,
    pub cache: TursoCacheOptions,
    pub control_plane: PostgresControlPlaneOptions,
    pub limits: QueueLimits,
    pub runtime: QueueRuntimeOptions,
}

#[derive(Debug, Clone)]
pub struct S3LogOptions {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Key prefix isolating this deployment's objects.
    pub namespace: String,
    pub access_key_id: ConfigSecret,
    pub secret_access_key: ConfigSecret,
    pub allow_insecure_http: bool,
}

#[derive(Debug, Clone)]
pub struct TursoCacheOptions {
    /// Absolute path of a directory used by this handle alone. Each queue the
    /// handle owns has its own disposable projection database in it, created
    /// when the handle takes the queue's lease and deleted when the handle
    /// gives the lease up or shuts down. Files left by an earlier process are
    /// deleted, never read. The directory is created if it does not exist.
    pub directory: PathBuf,
}

#[derive(Clone)]
pub struct PostgresControlPlaneOptions {
    pub url: ConfigSecret,
    pub schema: String,
    pub instance_id: OwnerId,
    pub heartbeat_ttl_ms: u64,
    pub lease_ttl_ms: u64,
    /// None uses SystemClock. Independent of the `clock` passed to the constructor.
    pub ownership_clock: Option<Arc<dyn Clock>>,
}

impl fmt::Debug for PostgresControlPlaneOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresControlPlaneOptions")
            .field("url", &self.url)
            .field("schema", &self.schema)
            .field("instance_id", &self.instance_id)
            .field("heartbeat_ttl_ms", &self.heartbeat_ttl_ms)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("separate_ownership_clock", &self.ownership_clock.is_some())
            .finish()
    }
}

impl fmt::Debug for S3TursoOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3TursoOptions")
            .field("log", &self.log)
            .field("cache", &self.cache)
            .field("control_plane", &self.control_plane)
            .field("limits", &self.limits)
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl S3TursoOptions {
    /// Default limits and runtime, matching `PostgresOptions::new`.
    pub fn new(
        log: S3LogOptions,
        cache: TursoCacheOptions,
        control_plane: PostgresControlPlaneOptions,
    ) -> Self {
        Self {
            log,
            cache,
            control_plane,
            limits: QueueLimits::default(),
            runtime: QueueRuntimeOptions::default(),
        }
    }
}

fn invalid(message: &str) -> QueueError {
    QueueError::Invalid(message.into())
}
fn unavailable(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("S3/Turso: {message}"))
}

/// Everything derived from the options without I/O.
struct Resolved {
    runtime: ValidatedQueueRuntime,
    scratch_path: PathBuf,
    pipeline: QueuePipelineOptions,
    control: ControlPlaneConfig,
}

/// Capacities from `QueueRuntimeOptions`, resolved as `PostgresQueueStore` does:
/// `ValidatedQueueRuntime::with_options` with the default worker count, and a
/// scratch root that defaults to the OS temporary directory once, here.
fn resolve_runtime(
    limits: &QueueLimits,
    runtime: &QueueRuntimeOptions,
) -> QueueResult<(ValidatedQueueRuntime, PathBuf, QueuePipelineOptions)> {
    let validated =
        ValidatedQueueRuntime::with_options(limits, runtime.clone(), DEFAULT_SCRATCH_WORKERS)?;
    let operations = validated.operations();
    let scratch_path = validated.clone().resolve_scratch_root(std::env::temp_dir)?;
    // A record is at most one request.
    let pipeline = QueuePipelineOptions {
        operations,
        hard_record_bytes: limits.max_request_bytes,
    };
    pipeline.validate()?;
    Ok((validated, scratch_path, pipeline))
}

fn validate_endpoint(endpoint: &str, allow_insecure_http: bool) -> QueueResult<()> {
    let rest = if let Some(rest) = endpoint.strip_prefix("https://") {
        rest
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        if !allow_insecure_http {
            return Err(invalid(
                "S3 endpoint uses http; set allow_insecure_http to permit it",
            ));
        }
        rest
    } else {
        return Err(invalid("S3 endpoint must start with https:// or http://"));
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty()
        || authority.contains('@')
        || endpoint
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        || rest.contains(['?', '#'])
    {
        return Err(invalid(
            "S3 endpoint needs a host and no credentials, query or fragment",
        ));
    }
    Ok(())
}

fn validate_schema(schema: &str) -> QueueResult<()> {
    // The rule `AsyncPostgresControlPlane::connect_in_schema` applies.
    if schema.is_empty()
        || schema.len() > 63
        || !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(invalid(
            "control-plane schema must be 1..63 ASCII letters, digits or underscores",
        ));
    }
    Ok(())
}

fn non_empty(value: &str, what: &str) -> QueueResult<()> {
    if value.trim().is_empty() {
        return Err(QueueError::Invalid(format!("{what} is required")));
    }
    Ok(())
}

impl S3TursoOptions {
    fn validate(&self) -> QueueResult<Resolved> {
        let log = &self.log;
        non_empty(&log.endpoint, "S3 endpoint")?;
        non_empty(&log.region, "S3 region")?;
        non_empty(&log.bucket, "S3 bucket")?;
        non_empty(&log.namespace, "S3 namespace")?;
        non_empty(&log.access_key_id.0.0, "S3 access key id")?;
        non_empty(&log.secret_access_key.0.0, "S3 secret access key")?;
        validate_endpoint(&log.endpoint, log.allow_insecure_http)?;
        if !self.cache.directory.is_absolute() {
            return Err(invalid("cache directory must be an absolute path"));
        }
        let control = &self.control_plane;
        if control.heartbeat_ttl_ms == 0
            || control.lease_ttl_ms == 0
            || control.lease_ttl_ms < control.heartbeat_ttl_ms
        {
            return Err(invalid(
                "TTLs must be positive and the lease TTL at least the heartbeat TTL",
            ));
        }
        validate_schema(&control.schema)?;
        non_empty(&control.url.0.0, "PostgreSQL URL")?;
        control
            .url
            .0
            .0
            .parse::<tokio_postgres::Config>()
            .map_err(|_| invalid("invalid PostgreSQL URL"))?;
        self.limits.validate()?;
        let (runtime, scratch_path, pipeline) = resolve_runtime(&self.limits, &self.runtime)?;
        // Pure; fails on a limits/runtime combination the native log cannot hold.
        native_options(&self.limits, &runtime)?;
        Ok(Resolved {
            runtime,
            scratch_path,
            pipeline,
            control: ControlPlaneConfig {
                heartbeat_ttl_ms: control.heartbeat_ttl_ms,
                lease_ttl_ms: control.lease_ttl_ms,
            },
        })
    }
}

fn native_options(
    limits: &QueueLimits,
    runtime: &ValidatedQueueRuntime,
) -> QueueResult<QueueNativeLogOptions> {
    QueueNativeLogOptions::for_queue_limits(
        limits,
        runtime.operations(),
        runtime.scratch_workers(),
        NATIVE_VERSION_BYTES,
        None,
    )
}

/// A store whose shutdown also closes what it was assembled with: the control-plane
/// connection and, for an ephemeral handle, its directory. Field order is drop order.
struct ManagedStore {
    store: NativeQueueStore,
    control: Option<Arc<AsyncPostgresControlPlane>>,
    directory: Option<EphemeralDirectory>,
}

/// A directory of this handle alone, removed on shutdown or drop.
struct EphemeralDirectory(PathBuf);
impl EphemeralDirectory {
    fn create(parent: &Path) -> QueueResult<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = parent.join(format!(
            "fireweed-ephemeral-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&path)
            .map_err(|_| unavailable("ephemeral directory creation failed"))?;
        // Canonical, so the later absolute-path checks do not depend on symlinks.
        let path = std::fs::canonicalize(&path)
            .map_err(|_| unavailable("ephemeral directory resolution failed"))?;
        Ok(Self(path))
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn remove(&self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl Drop for EphemeralDirectory {
    fn drop(&mut self) {
        self.remove();
    }
}

macro_rules! delegate {
    ($($name:ident($request:ty)->$result:ty;)+) => {$(
        fn $name(&self, request: $request) -> QueueFuture<'_, $result> {
            self.store.$name(request)
        }
    )+};
}
impl QueueStore for ManagedStore {
    delegate! {
        provision(ProvisionRequest)->();
        insert(InsertRequest)->Vec<InsertOutcome>;
        claim(ClaimRequest)->Vec<ClaimedWork>;
        commit(CommitBatch)->Vec<CommitReceipt>;
        preview(PreviewRequest)->PreviewResult;
        stats(StatsRequest)->QueueStats;
        read_state(StateReadRequest)->Vec<Option<StateRecord>>;
        query(QueryRequest)->QueryPage;
        read_commit(ReceiptReadRequest)->Vec<ReceiptLookup>;
    }
    fn shutdown(&self) -> QueueFuture<'_, ()> {
        Box::pin(async move {
            // The pipeline releases the leases and its cache lock before this returns.
            let stored = self.store.shutdown().await;
            if let Some(control) = &self.control {
                let _ = control.shutdown().await;
            }
            if let Some(directory) = &self.directory {
                directory.remove();
            }
            stored
        })
    }
}

/// Workers, scratch, quota, pipeline and store over an opened log and ownership.
/// On failure the workers are drained before the error is returned.
#[allow(clippy::too_many_arguments)]
async fn assemble(
    log: QueueNativeLog,
    cache_directory: PathBuf,
    scratch_path: PathBuf,
    runtime: &ValidatedQueueRuntime,
    limits: QueueLimits,
    pipeline_options: QueuePipelineOptions,
    ownership: QueueOwnership,
    clock: Arc<dyn Clock>,
) -> QueueResult<NativeQueueStore> {
    let workers = ScratchWorkers::new(runtime.scratch_workers(), runtime.operations())?;
    let built = async {
        let scratch = ScratchRoot::open(scratch_path, &workers).await?;
        let quota = ScratchDiskQuota::new(runtime.scratch_bytes())?;
        let pipeline = QueueRecoveryPipeline::new(
            Arc::new(log),
            cache_directory,
            workers.clone(),
            pipeline_options,
            scratch,
            quota,
            ownership,
        )?;
        NativeQueueStore::new(pipeline, limits, runtime.retained_bytes(), clock)
    }
    .await;
    match built {
        Ok(store) => Ok(store),
        Err(error) => {
            let _ = workers.close_and_drain().await;
            Err(error)
        }
    }
}

impl Queue {
    /// Open the production stack: objects in S3, a disposable local Turso
    /// projection per owned queue, and queue leases in PostgreSQL.
    ///
    /// Every option is validated before any file or network access; invalid
    /// options give `QueueError::Invalid`. Infrastructure failures give a
    /// sanitized `QueueError::OutcomeUnknown` that carries no credentials.
    /// `clock` controls logical eligibility and lease semantics; ownership timing
    /// uses `PostgresControlPlaneOptions::ownership_clock`. Ownership renewal runs
    /// inside the handle. `shutdown` stops it and releases the leases.
    pub async fn s3_turso(options: S3TursoOptions, clock: Arc<dyn Clock>) -> QueueResult<Self> {
        let resolved = options.validate()?;
        let S3TursoOptions {
            log,
            cache,
            control_plane,
            limits,
            runtime: _,
        } = options;
        let native = native_options(&limits, &resolved.runtime)?;
        let native_log = QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: &log.endpoint,
                region: &log.region,
                bucket: &log.bucket,
                access_key: &log.access_key_id.0.0,
                secret_key: &log.secret_access_key.0.0,
            },
            log.namespace.clone(),
            native,
            QueueNativeS3TransportOptions::default(),
        )?;
        let control = Arc::new(
            AsyncPostgresControlPlane::connect_in_schema(
                &control_plane.url.0.0,
                &control_plane.schema,
                resolved.control,
            )
            .await
            .map_err(|_| unavailable("PostgreSQL control plane setup failed"))?,
        );
        let ownership = QueueOwnership::new(
            control.clone() as Arc<dyn AsyncQueueControlPlane>,
            control_plane.instance_id,
            control_plane
                .ownership_clock
                .unwrap_or_else(|| Arc::new(SystemClock)),
            Arc::new(SystemLeaseClock),
            Duration::from_millis(control_plane.lease_ttl_ms),
        );
        let store = match ownership {
            Ok(ownership) => {
                assemble(
                    native_log,
                    cache.directory,
                    resolved.scratch_path,
                    &resolved.runtime,
                    limits,
                    resolved.pipeline,
                    ownership,
                    clock,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match store {
            Ok(store) => Ok(Self {
                store: Arc::new(ManagedStore {
                    store,
                    control: Some(control),
                    directory: None,
                }),
            }),
            Err(error) => {
                let _ = control.shutdown().await;
                Err(error)
            }
        }
    }

    /// A disposable on-disk store for development and tests: a local object log,
    /// projection files and scratch in a fresh temporary directory, and an
    /// in-process control plane. Nothing is shared between handles and nothing
    /// survives the last clone's `shutdown` or drop, which remove the directory.
    /// Production uses `s3_turso`.
    pub async fn ephemeral_disk(clock: Arc<dyn Clock>) -> QueueResult<Self> {
        Self::ephemeral_disk_in(&std::env::temp_dir(), clock).await
    }

    /// `ephemeral_disk` with the parent of its temporary directory chosen.
    async fn ephemeral_disk_in(parent: &Path, clock: Arc<dyn Clock>) -> QueueResult<Self> {
        let limits = QueueLimits::default();
        let (runtime, _, pipeline) = resolve_runtime(&limits, &QueueRuntimeOptions::default())?;
        let directory = EphemeralDirectory::create(parent)?;
        let native = native_options(&limits, &runtime)?;
        let log = QueueNativeLog::local(
            &directory.path().join("log"),
            "ephemeral".to_owned(),
            native,
        )?;
        let plane = Arc::new(InMemoryAsyncControlPlane(Arc::new(
            InMemoryControlPlane::new(ControlPlaneConfig {
                heartbeat_ttl_ms: EPHEMERAL_HEARTBEAT_TTL_MS,
                lease_ttl_ms: EPHEMERAL_LEASE_TTL_MS,
            }),
        )));
        let ownership = QueueOwnership::new(
            plane,
            OwnerId::new("ephemeral").map_err(|e| QueueError::Invalid(e.to_string()))?,
            Arc::new(SystemClock),
            Arc::new(SystemLeaseClock),
            Duration::from_millis(EPHEMERAL_LEASE_TTL_MS),
        )?
        .with_skew(Duration::ZERO);
        let store = assemble(
            log,
            directory.path().join("projections"),
            directory.path().join("scratch"),
            &runtime,
            limits,
            pipeline,
            ownership,
            clock,
        )
        .await?;
        Ok(Self {
            store: Arc::new(ManagedStore {
                store,
                control: None,
                directory: Some(directory),
            }),
        })
    }
}

#[cfg(test)]
mod tests;
