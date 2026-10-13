//! Internal-port reference smoke. This runner does not establish TP-005 acceptance.
use fireweed_conformance::queue_benchmark::{Workload, run_owned};
use fireweed_core::{queue_contract::*, queue_runtime::QueueRuntimeOptions};
use fireweed_engine::{Clock, queue_contract::QueueStore, queue_runtime::ValidatedQueueRuntime};
use futures::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
struct S3 {
    endpoint: String,
    region: String,
    bucket: String,
    access: String,
    secret: String,
}
struct Config {
    backend: String,
    root: PathBuf,
    artifact: PathBuf,
    workload: Workload,
    pg: Option<String>,
    s3: Option<S3>,
    runtime: QueueRuntimeOptions,
    prefix: String,
    profile: String,
    flush: fireweed_objectlog::FlushConfig,
}
fn limits() -> QueueLimits {
    QueueLimits {
        max_response_bytes: 4 << 20,
        ..QueueLimits::default()
    }
}
fn required(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("required environment setting: {name}"))
}
fn config() -> Result<Config, String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(8..=9).contains(&args.len()) {
        return Err("usage: fireweed-api007-reference <postgres|local|s3> <fresh-absolute-root> <fresh-absolute-artifact> <records> <batch> <seed> <body-bytes> <state-bytes> [default|zero-linger]".into());
    }
    let profile = args.get(8).map(String::as_str).unwrap_or("default");
    let mut flush = fireweed_objectlog::FlushConfig::default();
    match profile {
        "default" => {}
        "zero-linger" => flush.linger = std::time::Duration::ZERO,
        _ => return Err("profile must be default or zero-linger".into()),
    }
    let root = PathBuf::from(&args[1]);
    let artifact = PathBuf::from(&args[2]);
    if !root.is_absolute() || root.exists() || !artifact.is_absolute() || artifact.exists() {
        return Err("root and artifact must be fresh absolute paths".into());
    }
    for path in [&root, &artifact] {
        if path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        }) || path.file_name().is_none()
            || !path.parent().is_some_and(Path::is_dir)
        {
            return Err("paths require an existing parent and no dot components".into());
        }
    }
    let resolved = |path: &Path| -> Result<PathBuf, String> {
        Ok(path
            .parent()
            .unwrap()
            .canonicalize()
            .map_err(|_| "path parent resolution failed")?
            .join(path.file_name().unwrap()))
    };
    if resolved(&root)? == resolved(&artifact)? {
        return Err("root and artifact destinations must differ".into());
    }
    let number = |index: usize| {
        args[index]
            .parse::<u64>()
            .map_err(|_| "invalid unsigned workload argument".to_string())
    };
    let records = u32::try_from(number(3)?).map_err(|_| "records exceed u32")?;
    let batch = u32::try_from(number(4)?).map_err(|_| "batch exceeds u32")?;
    let body_bytes = usize::try_from(number(6)?).map_err(|_| "body size exceeds usize")?;
    let state_bytes = usize::try_from(number(7)?).map_err(|_| "state size exceeds usize")?;
    if records == 0 || !(1..=64).contains(&batch) || body_bytes > 1 << 20 || state_bytes > 1 << 20 {
        return Err(
            "reference workload requires records > 0, batch 1..64, body/state <= 1 MiB".into(),
        );
    }
    let (pg, s3) = match args[0].as_str() {
        "postgres" => {
            let url = required("FIREWEED_API007_PG_TEST_URL")?;
            url.parse::<postgres::Config>()
                .map_err(|_| "invalid PostgreSQL configuration")?;
            (Some(url), None)
        }
        "local" => (None, None),
        "s3" => {
            let endpoint = required("FIREWEED_S3_TEST_ENDPOINT")?;
            let parsed = url::Url::parse(&endpoint).map_err(|_| "invalid S3 endpoint")?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(
                    "S3 endpoint must be HTTP(S) without credentials, query or fragment".into(),
                );
            }
            (
                None,
                Some(S3 {
                    endpoint,
                    region: required("FIREWEED_S3_TEST_REGION")?,
                    bucket: required("FIREWEED_S3_TEST_BUCKET")?,
                    access: required("FIREWEED_S3_TEST_ACCESS_KEY")?,
                    secret: required("FIREWEED_S3_TEST_SECRET_KEY")?,
                }),
            )
        }
        _ => return Err("backend must be postgres, local or s3".into()),
    };
    let runtime = QueueRuntimeOptions {
        operations: 8,
        retained_bytes: Some(512 << 20),
        scratch_bytes: Some(64 << 20),
        scratch_workers: Some(2),
        scratch_root: Some(root.join("scratch")),
    };
    ValidatedQueueRuntime::with_options(&limits(), runtime.clone(), 2)
        .map_err(|_| "invalid runtime limits")?;
    Ok(Config {
        backend: args[0].clone(),
        root,
        artifact,
        workload: Workload {
            records,
            batch,
            body_bytes,
            state_bytes,
            seed: number(5)?,
            available_at: 0,
        },
        pg,
        s3,
        runtime,
        profile: profile.to_owned(),
        flush,
        prefix: format!(
            "api007-reference-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "clock precedes epoch")?
                .as_nanos()
        ),
    })
}
fn sha256(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn executable_sha256() -> Result<String, String> {
    use std::io::Read;
    let mut executable =
        std::fs::File::open("/proc/self/exe").map_err(|_| "executable read failed")?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = executable
            .read(&mut buffer)
            .map_err(|_| "executable hash read failed")?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
fn git(args: &[&str]) -> Result<Vec<u8>, String> {
    let result = std::process::Command::new("git")
        .args(args)
        .current_dir(repo())
        .output()
        .map_err(|_| "Git spawn failed")?;
    if !result.status.success() {
        return Err("Git command failed".into());
    }
    Ok(result.stdout)
}
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn provenance() -> Result<Value, String> {
    let mut hashes = serde_json::Map::new();
    for (path, compiled) in [
        (
            "Cargo.lock",
            include_bytes!("../../../../Cargo.lock").as_slice(),
        ),
        (
            "crates/fireweed-bench/Cargo.lock",
            include_bytes!("../../Cargo.lock").as_slice(),
        ),
        (
            "crates/fireweed-bench/Cargo.toml",
            include_bytes!("../../Cargo.toml").as_slice(),
        ),
        (
            "crates/fireweed-conformance/Cargo.toml",
            include_bytes!("../../../fireweed-conformance/Cargo.toml").as_slice(),
        ),
        (
            "crates/fireweed-conformance/src/queue_benchmark.rs",
            include_bytes!("../../../fireweed-conformance/src/queue_benchmark.rs").as_slice(),
        ),
        (
            "crates/fireweed-bench/src/bin/fireweed-api007-reference.rs",
            include_bytes!("fireweed-api007-reference.rs").as_slice(),
        ),
    ] {
        let current = std::fs::read(repo().join(path)).map_err(|_| "source read failed")?;
        if current != compiled {
            return Err("source differs from compiled reference runner; rebuild required".into());
        }
        hashes.insert(path.into(), json!(sha256(current)));
    }
    let head =
        String::from_utf8(git(&["rev-parse", "HEAD"])?).map_err(|_| "invalid Git HEAD bytes")?;
    Ok(
        json!({"head":head.trim(), "dirty_status_sha256":sha256(git(&["status", "--porcelain"])?), "dirty_patch_sha256":sha256(git(&["diff", "HEAD", "--binary"])?), "source_hashes":hashes,"executable_sha256":executable_sha256()?}),
    )
}
type Constructed = (
    Box<dyn QueueStore>,
    Option<Arc<fireweed::queue_recovery_pipeline::RecoveryCounters>>,
);
async fn construct(c: &Config) -> QueueResult<Constructed> {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock);
    if let Some(pg) = &c.pg {
        return Ok((
            Box::new(
                fireweed_postgres::PostgresQueueStore::connect_with_runtime(
                    pg,
                    clock,
                    limits(),
                    2,
                    c.runtime.clone(),
                )
                .await?,
            ),
            None,
        ));
    }
    use fireweed::queue_recovery_pipeline::{
        NativeQueueStore, QueuePipelineOptions, QueueRecoveryPipeline,
    };
    use fireweed_engine::queue_admission::QueueAdmission;
    use fireweed_objectlog::{NativeLogOptions, ObjectLogEngineStore};
    use fireweed_queue_spool::{ScratchDiskQuota, ScratchRoot, ScratchWorkers};
    let workers = ScratchWorkers::new(2, c.runtime.operations)?;
    let result = std::panic::AssertUnwindSafe(async {
        let scratch = ScratchRoot::open(c.root.join("scratch"), &workers).await?;
        let native = NativeLogOptions {
            provision_operations: c.runtime.operations,
            ..NativeLogOptions::default()
        };
        let store = if let Some(s3) = &c.s3 {
            ObjectLogEngineStore::open_s3_with_prefixes_and_native_options(
                &s3.endpoint,
                &s3.region,
                &s3.bucket,
                &s3.access,
                &s3.secret,
                format!("{}/data/", c.prefix),
                format!("{}/meta/", c.prefix),
                c.flush,
                native,
            )
            .await
        } else {
            ObjectLogEngineStore::open_local_with_native_options(
                c.root.join("log"),
                c.flush,
                native,
            )
            .await
        }
        .map_err(|_| QueueError::OutcomeUnknown("native constructor failed".into()))?;
        let admission = QueueAdmission::new(1, c.runtime.retained_bytes.unwrap())?;
        let job = Arc::new(admission.reserve(0)?);
        let projection = fireweed_turso::queue_projection::QueueProjection::open(
            &c.root.join("projection.db"),
            &job,
            &workers,
        )
        .await?;
        drop(job);
        let quota = ScratchDiskQuota::new(c.runtime.scratch_bytes.unwrap())?;
        let pipeline = QueueRecoveryPipeline::new(
            Arc::new(store),
            projection,
            workers.clone(),
            QueuePipelineOptions {
                operations: c.runtime.operations,
                metadata_bytes: 65536,
                hard_record_bytes: 256 << 20,
            },
            scratch,
            quota,
        )?;
        let store =
            NativeQueueStore::new(pipeline, limits(), c.runtime.retained_bytes.unwrap(), clock)?;
        let diagnostics = store.recovery_diagnostics();
        Ok((Box::new(store) as Box<dyn QueueStore>, Some(diagnostics)))
    })
    .catch_unwind()
    .await;
    match result {
        Ok(Ok(store)) => Ok(store),
        failed => {
            workers.close_and_drain().await?;
            match failed {
                Ok(Err(error)) => Err(error),
                Err(_) => Err(QueueError::OutcomeUnknown(
                    "native constructor panicked after worker cleanup".into(),
                )),
                _ => unreachable!(),
            }
        }
    }
}
fn client_peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next()?.parse().ok())
        })
}
fn save(path: &Path, report: &Value) -> Result<(), String> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| "artifact creation failed")?;
    file.write_all(
        serde_json::to_string_pretty(report)
            .map_err(|_| "artifact encoding failed")?
            .as_bytes(),
    )
    .map_err(|_| "artifact write failed")?;
    file.sync_all()
        .map_err(|_| "artifact sync failed".to_string())
}
async fn execute(c: Config, source: Value) -> Result<bool, String> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "clock precedes epoch")?
        .as_nanos();
    let queue = QueueAddress {
        tenant: "api007-reference".into(),
        queue: format!("{}-{nonce}-{}", c.backend, std::process::id()),
    };
    let construction = Instant::now();
    let result = std::panic::AssertUnwindSafe(async {
        std::fs::create_dir(&c.root)
            .map_err(|_| QueueError::Invalid("fresh root reservation failed".into()))?;
        construct(&c).await
    })
    .catch_unwind()
    .await;
    let construction_ns = construction.elapsed().as_nanos();
    let (workload, mut passed, recovery) = match result {
        Ok(Ok((store, diagnostics))) => {
            let before = diagnostics.as_ref().map(|counter| counter.snapshot());
            let outcome = run_owned(&*store, queue, c.workload.clone(), None).await;
            let recovery = diagnostics.zip(before).map(|(counter, before)| {
                let after = counter.snapshot();
                let delta = if outcome.report["shutdown"] == "completed" {
                    after.checked_delta(before)
                } else { None };
                json!({"interval":"after construction; warmup, provision, insertion, lifecycle, verification, owned shutdown",
                       "scope":"admitted framed records, not physical provider reads; elapsed is sum of recovery-task durations including waits and may exceed wall time",
                       "snapshot_after_successful_drain":outcome.report["shutdown"] == "completed",
                       "overflow":after.overflow,
                       "delta":delta.map(|d| json!({"attempts":d.attempts,"succeeded":d.succeeded,"failed":d.failed,"abandoned":d.abandoned,"admitted_raw_records":d.admitted_raw_records,"admitted_raw_bytes":d.admitted_raw_bytes,"summed_recovery_task_elapsed_ns":d.summed_recovery_task_elapsed_ns}))})
            });
            (outcome.report, outcome.passed, recovery)
        }
        Ok(Err(_)) | Err(_) => (
            json!({"correctness":"failed", "phase":"construction", "failure":"backend constructor failed; details suppressed to avoid credential disclosure"}),
            false,
            None,
        ),
    };
    let source_matches = provenance().is_ok_and(|current| current == source);
    passed &= source_matches;
    let mut report = json!({"qualification":"internal-port reference; histogram only; not TP-005 or public-facade acceptance", "backend":c.backend,"native_flush_profile":c.profile,"profile_effect":if c.pg.is_some() {"no PostgreSQL behavior change"} else {"configured native linger only; budget, durability and recovery waits remain"}, "source":source,"compiled_source_scope":"listed benchmark/common sources and manifests/locks; remaining backend source is identified by HEAD and dirty patch", "source_matches_after_drain":source_matches, "command":std::env::args().collect::<Vec<_>>(), "build_profile":if cfg!(debug_assertions) {"debug"} else {"release"}, "clock":"fixed0", "postgres_pool_size":c.pg.as_ref().map(|_|2), "operation_slots":8, "scratch_workers":2, "retained_bytes":512u64<<20, "scratch_bytes":64u64<<20, "queue_limits":limits(),"native_settings":if c.pg.is_none() { Some(json!({"provision_operations":8,"provision_retry_delay_ms":125,"metadata_bytes":65536,"hard_record_bytes":256u64<<20,"flush":format!("{:?}",c.flush),"turso_cache_kib":4096,"turso_cache_scope":"SDK soft cache setting; hard RAM qualification pending"})) } else { None }, "construction_ns":construction_ns,"namespace_prefix":c.prefix,"local_root":c.root,"s3_endpoint":c.s3.as_ref().map(|s3| &s3.endpoint),"s3_bucket":c.s3.as_ref().map(|s3| &s3.bucket),"s3_region":c.s3.as_ref().map(|s3| &s3.region),"postgres_config_source":c.pg.as_ref().map(|_| "FIREWEED_API007_PG_TEST_URL (credentials omitted)"),"host":std::fs::read_to_string("/proc/sys/kernel/hostname").ok().map(|v| v.trim().to_owned()), "client_peak_rss_kib":client_peak_rss_kib(), "service_memory":"unmeasured; client RSS is not backend-total RAM", "remote_storage_and_cost":"unmeasured", "workload":workload, "passed":passed});
    report["native_recovery"] = recovery.unwrap_or(Value::Null);
    if !source_matches {
        report["workload"]["correctness"] = json!("failed_provenance");
        report["workload"]["settled_per_second"] = Value::Null;
    }
    let saved = save(&c.artifact, &report);
    if saved.is_err() {
        report["passed"] = json!(false);
        report["artifact_write"] = json!("failed; stdout evidence retained");
    } else {
        report["artifact_write"] = json!("completed");
    }
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|_| "report encoding failed")?
    );
    saved?;
    Ok(passed)
}
fn main() {
    let result = (|| {
        let c = config()?;
        let source = provenance()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| "Tokio construction failed")?;
        runtime.block_on(execute(c, source))
    })();
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(message) => {
            eprintln!("reference runner failed: {message}");
            std::process::exit(2);
        }
    }
}
