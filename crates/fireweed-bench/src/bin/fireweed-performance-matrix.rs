use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fireweed::{
    ConfigSecret, Fireweed, ObjectLogRuntimeConfig, ObjectLogStorage, PostgresMode,
    PostgresRuntimeConfig, ProjectionConfig, RecoveryAction, RecoveryPolicy, ResponseBarrier,
    SegmentConfig, open_memory, open_objectlog, open_objectlog_postgres, open_objectlog_sqlite,
    open_postgres_runtime, open_sqlite, open_sqlite_relational,
};
use fireweed_bench::performance_matrix::{
    ProjectionCatchupEvidence, RepetitionSpec, run_preflight, run_repetition,
};
use fireweed_bench::performance_matrix_analysis::build_comparisons;
use fireweed_bench::performance_matrix_checkpoint::{
    LifecycleFragment, MatrixCheckpoint, read_checkpoint, read_fragment, read_lifecycle_fragment,
    write_checkpoint, write_fragment, write_lifecycle_fragment,
};
use fireweed_bench::performance_matrix_evidence::{
    CellEvidence, CleanupEvidence, MatrixEvidence, SCHEMA_VERSION, ServiceTopology, ShapeEvidence,
    build_schedule, build_summaries, canonical_bytes, digest_hex, verify_file, write_evidence,
};
use fireweed_bench::performance_matrix_lifecycle::{
    reopen_verify_and_drain, run_projection_maintenance, seed_recovery_population,
};
use fireweed_bench::performance_matrix_provenance::collect as collect_provenance;
use fireweed_bench::performance_matrix_services::{
    AuthorizedCleanup, ObjectStoreService, PostgresService, RunOwnership, SchemaKind,
    SecretRedactor, ServiceLocks, cleanup_owned, derived_plain_schema, object_store_preflight_rtts,
};
use fireweed_bench::{Grouping, PriorityDist, Shape, SystemClock, bench_qdef, qkey};
use postgres::{Client, NoTls};

const CELLS_FULL: &[&str] = &[
    "memory",
    "sqlite-log",
    "sqlite-relational",
    "postgres-log",
    "postgres-relational",
    "objectlog-local-memory-legacy",
    "objectlog-local-sqlite-strict",
    "objectlog-local-sqlite-async",
    "objectlog-local-postgres-strict",
    "objectlog-s3-sqlite-strict",
    "objectlog-s3-sqlite-async",
    "objectlog-s3-postgres-strict",
];
static CANCELLED: AtomicBool = AtomicBool::new(false);

fn check_cancelled() -> Result<(), String> {
    if CANCELLED.load(Ordering::Acquire) {
        Err("performance matrix interrupted; resume from the verified checkpoint".into())
    } else {
        Ok(())
    }
}

fn wait_for_fragment(mut child: Child) -> Result<ExitStatus, String> {
    loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            return Ok(status);
        }
        if CANCELLED.load(Ordering::Acquire) {
            let pid = child.id().to_string();
            let _ = Command::new("kill").arg("-TERM").arg(&pid).status();
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if child
                    .try_wait()
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    return Err(
                        "performance matrix interrupted; active fragment terminated for cleanup"
                            .into(),
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(
                "performance matrix interrupted; active fragment killed after cleanup grace period"
                    .into(),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
const CELLS_SMOKE: &[&str] = &[
    "memory",
    "sqlite-log",
    "sqlite-relational",
    "objectlog-local-memory-legacy",
    "objectlog-local-sqlite-strict",
    "objectlog-local-sqlite-async",
];
const RECOVERY_CELLS: &[&str] = &[
    "sqlite-log",
    "sqlite-relational",
    "postgres-log",
    "postgres-relational",
    "objectlog-local-memory-legacy",
    "objectlog-local-sqlite-strict",
    "objectlog-local-sqlite-async",
    "objectlog-local-postgres-strict",
    "objectlog-s3-sqlite-strict",
    "objectlog-s3-sqlite-async",
    "objectlog-s3-postgres-strict",
];
const MAINTENANCE_CELLS: &[&str] = &[
    "objectlog-local-sqlite-strict",
    "objectlog-local-sqlite-async",
    "objectlog-local-postgres-strict",
    "objectlog-s3-sqlite-strict",
    "objectlog-s3-sqlite-async",
    "objectlog-s3-postgres-strict",
];

struct Config {
    tier: String,
    output: Option<PathBuf>,
    resume: bool,
    fragment: Option<FragmentArgs>,
    work_root: PathBuf,
    postgres: Option<PostgresService>,
    s3: Option<ObjectStoreService>,
}

struct FragmentArgs {
    phase: String,
    run_id: String,
    source_commit: String,
    cell: String,
    shape: String,
    repetition: usize,
    output: PathBuf,
}

#[derive(serde::Serialize)]
struct FailureJournal<'a> {
    run_id: &'a str,
    git_commit: &'a str,
    tier: &'a str,
    failures: &'a [String],
}

fn write_failure_journal(
    checkpoint_path: &Path,
    run_id: &str,
    git_commit: &str,
    tier: &str,
    failures: &[String],
    redactor: &SecretRedactor,
) -> Result<(), String> {
    let path = checkpoint_path.with_extension("failures.json");
    let bytes = serde_json::to_vec_pretty(&FailureJournal {
        run_id,
        git_commit,
        tier,
        failures,
    })
    .map_err(|error| error.to_string())?;
    redactor.validate_serialized_evidence(&bytes)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("failures.tmp");
    std::fs::write(&temporary, &bytes).map_err(|error| error.to_string())?;
    std::fs::rename(temporary, path).map_err(|error| error.to_string())
}

enum CleanupRecipe {
    Local,
    LocalAndPostgres(SchemaKind),
    S3,
    S3AndPostgres(SchemaKind),
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

fn shape_specs(tier: &str) -> Vec<(Shape, u64, usize)> {
    let all = vec![
        (
            Shape {
                name: "minimal",
                payload_bytes: 0,
                n_fields: 0,
                field_bytes: 0,
                grouping: Grouping::Ungrouped,
                priority: PriorityDist::Sequential,
            },
            12_800,
            128,
        ),
        (
            Shape {
                name: "record-1k",
                payload_bytes: 1024,
                n_fields: 16,
                field_bytes: 64,
                grouping: Grouping::Ungrouped,
                priority: PriorityDist::Uniform,
            },
            12_800,
            128,
        ),
        (
            Shape {
                name: "group-keyed-256",
                payload_bytes: 256,
                n_fields: 4,
                field_bytes: 32,
                grouping: Grouping::Grouped(64),
                priority: PriorityDist::Uniform,
            },
            12_800,
            128,
        ),
        (
            Shape {
                name: "large-16k",
                payload_bytes: 16 * 1024,
                n_fields: 0,
                field_bytes: 0,
                grouping: Grouping::Ungrouped,
                priority: PriorityDist::Uniform,
            },
            1_600,
            16,
        ),
    ];
    if tier == "smoke" {
        vec![(all[0].0, 512, 64)]
    } else {
        all
    }
}

fn matrix_qdef(queue: &str, shape: &Shape) -> fireweed::QueueDefinition {
    let mut definition = bench_qdef("bench", queue, shape);
    definition.max_push_batch_size = 1_000_000;
    definition.max_claim_batch_size = 1_000_000;
    definition.emit_change_records = false;
    definition
}

fn barrier_class(cell: &str) -> &'static str {
    match cell {
        "memory" => "volatile-visible",
        "sqlite-log" | "sqlite-relational" => "local-durable-visible",
        "postgres-log" | "postgres-relational" => "postgres-durable-visible",
        "objectlog-local-memory-legacy" => "legacy-objectlog-visible",
        value if value.ends_with("sqlite-async") => "objectlog-hot-visible",
        _ => "objectlog-projection-visible",
    }
}

fn object_config(
    cfg: &Config,
    cell: &str,
    root: &Path,
    namespace: &str,
) -> Result<(ObjectLogRuntimeConfig, CleanupRecipe), String> {
    let s3_cell = cell.starts_with("objectlog-s3-");
    let postgres_projection = cell.ends_with("postgres-strict");
    let object_log = if s3_cell {
        let s3 = cfg.s3.as_ref().ok_or("S3 configuration missing")?;
        ObjectLogStorage::S3Compatible {
            endpoint: s3.endpoint.clone(),
            bucket: s3.bucket.clone(),
            region: s3.region.clone(),
            access_key_id: ConfigSecret::new(&s3.access),
            secret_access_key: ConfigSecret::new(&s3.secret),
            allow_insecure_http: s3.endpoint.starts_with("http://"),
        }
    } else {
        ObjectLogStorage::Local {
            root: root.join("log"),
        }
    };
    let projection = if postgres_projection {
        ProjectionConfig::Postgres {
            url: ConfigSecret::new(
                cfg.postgres
                    .as_ref()
                    .ok_or("PostgreSQL configuration missing")?
                    .url
                    .as_str(),
            ),
        }
    } else {
        ProjectionConfig::Sqlite {
            path: root.join("projection.sqlite"),
        }
    };
    let cleanup = match (s3_cell, postgres_projection) {
        (true, true) => CleanupRecipe::S3AndPostgres(SchemaKind::ObjectLog),
        (true, false) => CleanupRecipe::S3,
        (false, true) => CleanupRecipe::LocalAndPostgres(SchemaKind::ObjectLog),
        (false, false) => CleanupRecipe::Local,
    };
    Ok((
        ObjectLogRuntimeConfig {
            object_log,
            projection,
            response_barrier: if cell.ends_with("sqlite-async") {
                ResponseBarrier::AsyncProjection
            } else {
                ResponseBarrier::Strict
            },
            segments: SegmentConfig::new(262_144, 20).map_err(|error| error.to_string())?,
            namespace: namespace.into(),
            recovery: RecoveryPolicy {
                incompatible_projection: RecoveryAction::RebuildProjection,
                verify_checksums: true,
                max_tail_commands: 1_000_000,
            },
        },
        cleanup,
    ))
}

fn construct(
    cfg: &Config,
    cell: &str,
    root: &Path,
    namespace: &str,
) -> Result<(Fireweed, CleanupRecipe), String> {
    std::fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let clock = || Arc::new(SystemClock);
    match cell {
        "memory" => Ok((open_memory(clock()), CleanupRecipe::Local)),
        "sqlite-log" => Ok((
            open_sqlite(root.join("log.sqlite").to_str().unwrap(), clock())
                .map_err(|e| e.to_string())?,
            CleanupRecipe::Local,
        )),
        "sqlite-relational" => Ok((
            open_sqlite_relational(root.join("relational.sqlite").to_str().unwrap(), clock())
                .map_err(|e| e.to_string())?,
            CleanupRecipe::Local,
        )),
        "objectlog-local-memory-legacy" => Ok((
            open_objectlog(root.join("legacy"), clock()).map_err(|e| e.to_string())?,
            CleanupRecipe::Local,
        )),
        "postgres-log" | "postgres-relational" => {
            let schema = derived_plain_schema(namespace);
            let fw = open_postgres_runtime(
                PostgresRuntimeConfig {
                    url: ConfigSecret::new(
                        cfg.postgres
                            .as_ref()
                            .ok_or("PostgreSQL configuration missing")?
                            .url
                            .as_str(),
                    ),
                    schema: Some(schema.clone()),
                    mode: if cell == "postgres-log" {
                        PostgresMode::LogReplay
                    } else {
                        PostgresMode::Relational
                    },
                    node_id: None,
                    coordination: None,
                },
                clock(),
            )
            .map_err(|e| e.to_string())?;
            Ok((fw, CleanupRecipe::LocalAndPostgres(SchemaKind::Plain)))
        }
        _ => {
            let (runtime, cleanup) = object_config(cfg, cell, root, namespace)?;
            let fw = if cell.ends_with("postgres-strict") {
                open_objectlog_postgres(runtime, clock())
            } else {
                open_objectlog_sqlite(runtime, clock())
            }
            .map_err(|e| e.to_string())?;
            Ok((fw, cleanup))
        }
    }
}

fn authorize_cleanup(
    ownership: &RunOwnership,
    namespace: &str,
    root: &Path,
    recipe: CleanupRecipe,
) -> Result<AuthorizedCleanup, String> {
    let (schema, object_store) = match recipe {
        CleanupRecipe::Local => (None, false),
        CleanupRecipe::LocalAndPostgres(kind) => (Some(kind), false),
        CleanupRecipe::S3 => (None, true),
        CleanupRecipe::S3AndPostgres(kind) => (Some(kind), true),
    };
    ownership.authorize(namespace, Some(root), schema, object_store)
}

fn parse_config() -> Result<Config, String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("verify") {
        let path = args.get(1).ok_or("verify requires an evidence path")?;
        let evidence = verify_file(Path::new(path))?;
        println!(
            "verified {} {} rows",
            evidence.run_id,
            evidence.repetitions.len()
        );
        std::process::exit(0);
    }
    let mut tier = "smoke".to_owned();
    let mut output = None;
    let mut resume = false;
    let mut fragment_mode = false;
    let mut fragment_phase = None;
    let mut fragment_run_id = None;
    let mut fragment_source_commit = None;
    let mut fragment_cell = None;
    let mut fragment_shape = None;
    let mut fragment_repetition = None;
    let mut fragment_output = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--tier" => {
                tier = args
                    .get(index + 1)
                    .ok_or("--tier requires smoke or full")?
                    .clone();
                index += 2;
            }
            "--output" => {
                output = Some(PathBuf::from(
                    args.get(index + 1).ok_or("--output requires path")?,
                ));
                index += 2;
            }
            "--resume" => {
                resume = true;
                index += 1;
            }
            "--fragment" => {
                fragment_mode = true;
                index += 1;
            }
            "--phase" => {
                fragment_phase = Some(
                    args.get(index + 1)
                        .ok_or("--phase requires common, recovery, or maintenance")?
                        .clone(),
                );
                index += 2;
            }
            "--run-id" => {
                fragment_run_id = Some(
                    args.get(index + 1)
                        .ok_or("--run-id requires a value")?
                        .clone(),
                );
                index += 2;
            }
            "--source-commit" => {
                fragment_source_commit = Some(
                    args.get(index + 1)
                        .ok_or("--source-commit requires a value")?
                        .clone(),
                );
                index += 2;
            }
            "--cell" => {
                fragment_cell = Some(
                    args.get(index + 1)
                        .ok_or("--cell requires a value")?
                        .clone(),
                );
                index += 2;
            }
            "--shape" => {
                fragment_shape = Some(
                    args.get(index + 1)
                        .ok_or("--shape requires a value")?
                        .clone(),
                );
                index += 2;
            }
            "--round" => {
                fragment_repetition = Some(
                    args.get(index + 1)
                        .ok_or("--round requires a value")?
                        .parse::<usize>()
                        .map_err(|_| "--round requires a non-negative integer")?,
                );
                index += 2;
            }
            "--fragment-output" => {
                fragment_output = Some(PathBuf::from(
                    args.get(index + 1)
                        .ok_or("--fragment-output requires a path")?,
                ));
                index += 2;
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if tier != "smoke" && tier != "full" {
        return Err("tier must be smoke or full".into());
    }
    let postgres = std::env::var("FIREWEED_PERF_POSTGRES_URL")
        .ok()
        .map(|url| PostgresService { url });
    let s3 = match (
        std::env::var("FIREWEED_PERF_S3_ENDPOINT").ok(),
        std::env::var("FIREWEED_PERF_S3_BUCKET").ok(),
        std::env::var("FIREWEED_PERF_S3_REGION").ok(),
        std::env::var("FIREWEED_PERF_S3_ACCESS_KEY").ok(),
        std::env::var("FIREWEED_PERF_S3_SECRET_KEY").ok(),
    ) {
        (Some(endpoint), Some(bucket), Some(region), Some(access), Some(secret)) => {
            Some(ObjectStoreService {
                endpoint,
                bucket,
                region,
                access,
                secret,
            })
        }
        _ => None,
    };
    if tier == "full" && (postgres.is_none() || s3.is_none()) {
        return Err("full tier requires PostgreSQL and S3 configuration".into());
    }
    if tier == "full" {
        if ["CI", "GITHUB_ACTIONS", "GITLAB_CI", "BUILDKITE", "TF_BUILD"]
            .iter()
            .any(|name| std::env::var_os(name).is_some())
        {
            return Err("authoritative full performance runs are forbidden in CI".into());
        }
        let url = &postgres.as_ref().unwrap().url;
        let database = url
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .split('?')
            .next()
            .unwrap_or_default();
        let acknowledged = std::env::var("FIREWEED_PERF_POSTGRES_DATABASE_ACK")
            .map_err(|_| "full tier requires FIREWEED_PERF_POSTGRES_DATABASE_ACK")?;
        if acknowledged != database
            || !database.starts_with("fireweed")
            || matches!(database, "postgres" | "template0" | "template1")
        {
            return Err("PostgreSQL database acknowledgement is unsafe or does not match".into());
        }
        let s3_cfg = s3.as_ref().unwrap();
        if std::env::var("FIREWEED_PERF_S3_BUCKET_ACK").ok().as_deref()
            != Some(s3_cfg.bucket.as_str())
        {
            return Err("S3 bucket acknowledgement is missing or does not match".into());
        }
    }
    let fragment = if fragment_mode {
        if resume || output.is_some() {
            return Err("fragment mode does not accept --resume or --output".into());
        }
        let phase = fragment_phase.unwrap_or_else(|| "common".into());
        if !matches!(phase.as_str(), "common" | "recovery" | "maintenance") {
            return Err("fragment phase must be common, recovery, or maintenance".into());
        }
        if phase != "common" && tier != "full" {
            return Err("lifecycle fragments are full-tier only".into());
        }
        Some(FragmentArgs {
            phase,
            run_id: fragment_run_id.ok_or("fragment mode requires --run-id")?,
            source_commit: fragment_source_commit
                .ok_or("fragment mode requires --source-commit")?,
            cell: fragment_cell.ok_or("fragment mode requires --cell")?,
            shape: fragment_shape.ok_or("fragment mode requires --shape")?,
            repetition: fragment_repetition.ok_or("fragment mode requires --round")?,
            output: fragment_output.ok_or("fragment mode requires --fragment-output")?,
        })
    } else {
        if fragment_run_id.is_some()
            || fragment_phase.is_some()
            || fragment_source_commit.is_some()
            || fragment_cell.is_some()
            || fragment_shape.is_some()
            || fragment_repetition.is_some()
            || fragment_output.is_some()
        {
            return Err("fragment-only arguments require --fragment".into());
        }
        None
    };
    Ok(Config {
        tier,
        output,
        resume,
        fragment,
        work_root: std::env::temp_dir().join("fireweed-performance-matrix"),
        postgres,
        s3,
    })
}

fn cells_for_tier(tier: &str) -> &'static [&'static str] {
    if tier == "full" {
        CELLS_FULL
    } else {
        CELLS_SMOKE
    }
}

fn repetitions_for_tier(tier: &str) -> usize {
    if tier == "full" { 5 } else { 1 }
}

fn cleanup_recipe(cell: &str) -> CleanupRecipe {
    match cell {
        "postgres-log" | "postgres-relational" => {
            CleanupRecipe::LocalAndPostgres(SchemaKind::Plain)
        }
        value if value.starts_with("objectlog-s3-") && value.ends_with("postgres-strict") => {
            CleanupRecipe::S3AndPostgres(SchemaKind::ObjectLog)
        }
        value if value.starts_with("objectlog-s3-") => CleanupRecipe::S3,
        value if value.ends_with("postgres-strict") => {
            CleanupRecipe::LocalAndPostgres(SchemaKind::ObjectLog)
        }
        _ => CleanupRecipe::Local,
    }
}

fn safe_postgres_identity(url: &str) -> &str {
    let without_scheme = url.split_once("://").map_or(url, |(_, value)| value);
    let without_userinfo = without_scheme
        .rsplit_once('@')
        .map_or(without_scheme, |(_, value)| value);
    without_userinfo.split('?').next().unwrap_or_default()
}

fn safe_endpoint(endpoint: &str) -> String {
    let without_query = endpoint.split('?').next().unwrap_or(endpoint);
    if let Some((scheme, rest)) = without_query.split_once("://") {
        let host = rest.rsplit_once('@').map_or(rest, |(_, value)| value);
        format!("{scheme}://{host}")
    } else {
        without_query.to_owned()
    }
}

fn collect_service_topology(cfg: &Config) -> Result<ServiceTopology, String> {
    let (postgres_server, postgres_durability) = if let Some(postgres) = &cfg.postgres {
        let mut client = Client::connect(&postgres.url, NoTls)
            .map_err(|error| format!("PostgreSQL provenance connection: {error}"))?;
        let server: String = client
            .query_one("SHOW server_version", &[])
            .map_err(|error| format!("PostgreSQL version provenance: {error}"))?
            .get(0);
        let fsync: String = client
            .query_one("SHOW fsync", &[])
            .map_err(|error| format!("PostgreSQL fsync provenance: {error}"))?
            .get(0);
        let synchronous_commit: String = client
            .query_one("SHOW synchronous_commit", &[])
            .map_err(|error| format!("PostgreSQL commit provenance: {error}"))?
            .get(0);
        let full_page_writes: String = client
            .query_one("SHOW full_page_writes", &[])
            .map_err(|error| format!("PostgreSQL page-write provenance: {error}"))?
            .get(0);
        (
            Some(server),
            Some(format!(
                "fsync={fsync};synchronous_commit={synchronous_commit};full_page_writes={full_page_writes}"
            )),
        )
    } else {
        (None, None)
    };
    let safe_s3 = cfg.s3.as_ref().map(|s3| safe_endpoint(&s3.endpoint));
    Ok(ServiceTopology {
        postgres_configured: cfg.postgres.is_some(),
        postgres_server,
        postgres_durability,
        object_store_configured: cfg.s3.is_some(),
        object_store_scheme: safe_s3
            .as_deref()
            .and_then(|endpoint| endpoint.split_once("://").map(|(scheme, _)| scheme.into())),
        object_store_endpoint_sha256: safe_s3.as_deref().map(|value| digest_hex(value.as_bytes())),
        object_store_bucket_sha256: cfg.s3.as_ref().map(|s3| digest_hex(s3.bucket.as_bytes())),
        object_store_region: cfg.s3.as_ref().map(|s3| s3.region.clone()),
        object_store_provider: cfg.s3.as_ref().map(|_| "Garage".into()),
        object_store_preflight_rtt_ns: cfg
            .s3
            .as_ref()
            .map(object_store_preflight_rtts)
            .transpose()?
            .unwrap_or_default(),
    })
}

fn resolved_config(cfg: &Config, cells: &[&str], shapes: &[(Shape, u64, usize)]) -> Vec<u8> {
    let mut value = format!(
        "matrix=v1\nseed=104352798002925\ntier={}\nrepetitions={}\nsegments=262144,20\nrecovery=rebuild,true,1000000\n",
        cfg.tier,
        repetitions_for_tier(&cfg.tier)
    );
    for cell in cells {
        value.push_str(&format!("cell={cell},{}\n", barrier_class(cell)));
    }
    for (shape, items, batch) in shapes {
        value.push_str(&format!(
            "shape={},{},{},{},{items},{batch}\n",
            shape.name, shape.payload_bytes, shape.n_fields, shape.field_bytes
        ));
    }
    if let Some(postgres) = &cfg.postgres {
        value.push_str(&format!(
            "postgres={}\n",
            safe_postgres_identity(&postgres.url)
        ));
    }
    if let Some(s3) = &cfg.s3 {
        value.push_str(&format!(
            "object_store={},{},{}\n",
            safe_endpoint(&s3.endpoint),
            s3.bucket,
            s3.region
        ));
    }
    value.push_str(
        "schedule=warmup-stable;measured-round-shape-rotated-reversed;recovery;maintenance\n",
    );
    value.into_bytes()
}

fn output_directory_or_file(output: Option<&Path>, commit: &str, run_id: &str) -> PathBuf {
    match output {
        Some(path)
            if path
                .extension()
                .is_some_and(|extension| extension == "json") =>
        {
            path.to_owned()
        }
        Some(path) => path.join(format!("{}-{run_id}.json", &commit[..commit.len().min(12)])),
        None => PathBuf::from("docs/perf/matrix-results")
            .join(format!("{}-{run_id}.json", &commit[..commit.len().min(12)])),
    }
}

fn checkpoint_path(commit: &str, tier: &str) -> PathBuf {
    PathBuf::from("target/perf-matrix-checkpoints").join(format!(
        "{}-{tier}.checkpoint.json",
        &commit[..commit.len().min(12)]
    ))
}

fn write_safe_checkpoint(
    path: &Path,
    checkpoint: &MatrixCheckpoint,
    redactor: &SecretRedactor,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(checkpoint).map_err(|error| error.to_string())?;
    redactor.validate_serialized_evidence(&bytes)?;
    write_checkpoint(path, checkpoint)
}

fn fragment_coordinates(cfg: &Config, args: &FragmentArgs) -> Result<(Shape, u64, usize), String> {
    if command_output("git", &["rev-parse", "HEAD"]) != args.source_commit {
        return Err("fragment source commit differs from coordinator".into());
    }
    let allowed_cells = match args.phase.as_str() {
        "common" => cells_for_tier(&cfg.tier),
        "recovery" => RECOVERY_CELLS,
        "maintenance" => MAINTENANCE_CELLS,
        _ => return Err("unsupported fragment phase".into()),
    };
    if !allowed_cells.contains(&args.cell.as_str()) {
        return Err("fragment cell is not in the selected tier".into());
    }
    let repetitions = if args.phase == "common" {
        repetitions_for_tier(&cfg.tier)
    } else {
        3
    };
    if args.repetition >= repetitions {
        return Err("fragment repetition is outside the selected tier".into());
    }
    let found = shape_specs(&cfg.tier)
        .into_iter()
        .find(|(shape, _, _)| shape.name == args.shape)
        .ok_or_else(|| "fragment shape is not in the selected tier".to_owned())?;
    if args.phase == "recovery" && !matches!(found.0.name, "minimal" | "record-1k") {
        return Err("recovery fragment shape must be minimal or record-1k".into());
    }
    if args.phase == "maintenance" && found.0.name != "record-1k" {
        return Err("maintenance fragment shape must be record-1k".into());
    }
    Ok(found)
}

fn wait_for_projection(
    fireweed: &Fireweed,
    cell: &str,
) -> Result<Option<ProjectionCatchupEvidence>, String> {
    let Some(control) = fireweed.projection_control() else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(60);
    let mut poll_count = 0;
    loop {
        poll_count += 1;
        match futures::executor::block_on(control.verify()) {
            Ok(value)
                if value.compatible
                    && value.projection_sequence == value.authoritative_sequence =>
            {
                return Ok(cell
                    .ends_with("sqlite-async")
                    .then(|| ProjectionCatchupEvidence {
                        duration_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        poll_count,
                        compatible: value.compatible,
                        projection_sequence: value.projection_sequence,
                        authoritative_sequence: value.authoritative_sequence,
                    }));
            }
            Ok(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Ok(_) => return Err("projection did not catch up within 60s".into()),
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_fragment(
    cfg: &Config,
    run_id: &str,
    commit: &str,
    cell: &str,
    shape: &Shape,
    items: u64,
    batch: usize,
    repetition: usize,
) -> Result<fireweed_bench::performance_matrix::RepetitionResult, String> {
    let namespace = format!(
        "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
        &commit[..commit.len().min(12)],
        run_id,
        cell,
        shape.name,
        repetition
    );
    let root = cfg
        .work_root
        .join(run_id)
        .join(cell)
        .join(shape.name)
        .join(format!("r{repetition:02}"));
    let ownership = RunOwnership::new(&cfg.work_root, run_id)?;
    std::fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    let owned = authorize_cleanup(&ownership, &namespace, &root, cleanup_recipe(cell))?;
    match construct(cfg, cell, &root, &namespace) {
        Ok((fw, _)) => {
            let preflight_definition = matrix_qdef("preflight", shape);
            let preflight = futures::executor::block_on(run_preflight(
                &fw,
                preflight_definition,
                qkey("preflight"),
                shape,
                &format!("{cell}-{repetition}"),
            ));
            let definition = matrix_qdef("matrix", shape);
            let outcome = preflight.and_then(|_| {
                futures::executor::block_on(run_repetition(
                    &fw,
                    definition,
                    qkey("matrix"),
                    shape,
                    RepetitionSpec {
                        cell,
                        repetition,
                        items,
                        batch,
                    },
                ))
            });
            let verification = wait_for_projection(&fw, cell);
            drop(fw);
            let cleaned = cleanup_owned(owned, cfg.postgres.as_ref(), cfg.s3.as_ref());
            let mut row = outcome?;
            row.projection_catchup = verification?;
            cleaned?;
            Ok(row)
        }
        Err(error) => {
            let cleanup = cleanup_owned(owned, cfg.postgres.as_ref(), cfg.s3.as_ref());
            cleanup?;
            Err(error)
        }
    }
}

fn execute_lifecycle_fragment(
    cfg: &Config,
    args: &FragmentArgs,
    shape: &Shape,
    items: u64,
    batch: usize,
) -> Result<LifecycleFragment, String> {
    let coordinate_shape = format!("{}-{}", args.phase, shape.name);
    let namespace = format!(
        "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
        &args.source_commit[..args.source_commit.len().min(12)],
        args.run_id,
        args.cell,
        coordinate_shape,
        args.repetition
    );
    let root = cfg
        .work_root
        .join(&args.run_id)
        .join(&args.cell)
        .join(&coordinate_shape)
        .join(format!("r{:02}", args.repetition));
    let ownership = RunOwnership::new(&cfg.work_root, &args.run_id)?;
    std::fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    let owned = authorize_cleanup(&ownership, &namespace, &root, cleanup_recipe(&args.cell))?;
    let (fireweed, _) = match construct(cfg, &args.cell, &root, &namespace) {
        Ok(value) => value,
        Err(error) => {
            cleanup_owned(owned, cfg.postgres.as_ref(), cfg.s3.as_ref())?;
            return Err(error);
        }
    };
    let result = if args.phase == "recovery" {
        let definition = matrix_qdef("recovery", shape);
        let population = futures::executor::block_on(seed_recovery_population(
            &fireweed,
            definition,
            &qkey("recovery"),
            shape,
            &format!("tp005-recovery-{}-{}", args.cell, args.repetition),
            items,
            batch,
        ));
        let prepared = match population {
            Ok(population) => wait_for_projection(&fireweed, &args.cell).map(|_| population),
            Err(error) => Err(error),
        };
        drop(fireweed);
        match prepared {
            Ok(population) => futures::executor::block_on(reopen_verify_and_drain(
                &args.cell,
                args.repetition,
                &qkey("recovery"),
                population,
                || construct(cfg, &args.cell, &root, &namespace).map(|(fireweed, _)| fireweed),
            ))
            .map(LifecycleFragment::Recovery),
            Err(error) => Err(error),
        }
    } else {
        let definition = matrix_qdef("maintenance", shape);
        let result = futures::executor::block_on(run_projection_maintenance(
            &fireweed,
            definition,
            &qkey("maintenance"),
            shape,
            &args.cell,
            args.repetition,
            items,
            batch,
        ))
        .map(LifecycleFragment::Maintenance);
        drop(fireweed);
        result
    };
    let cleaned = cleanup_owned(owned, cfg.postgres.as_ref(), cfg.s3.as_ref());
    let result = result?;
    cleaned?;
    Ok(result)
}

fn run_fragment_mode(cfg: &Config, args: &FragmentArgs) -> Result<(), String> {
    let redactor = SecretRedactor::new(cfg.postgres.as_ref(), cfg.s3.as_ref());
    let result = (|| {
        let (shape, items, batch) = fragment_coordinates(cfg, args)?;
        if args.phase == "common" {
            let row = execute_fragment(
                cfg,
                &args.run_id,
                &args.source_commit,
                &args.cell,
                &shape,
                items,
                batch,
                args.repetition,
            )?;
            write_fragment(&args.output, &row)
        } else {
            let fragment = execute_lifecycle_fragment(cfg, args, &shape, items, batch)?;
            write_lifecycle_fragment(&args.output, &fragment)
        }
    })();
    result.map_err(|error: String| redactor.redact(error))
}

fn clean_fragment_state(
    cfg: &Config,
    ownership: &RunOwnership,
    run_id: &str,
    commit: &str,
    cell: &str,
    shape: &str,
    repetition: usize,
) -> Result<(), String> {
    let namespace = format!(
        "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
        &commit[..commit.len().min(12)],
        run_id,
        cell,
        shape,
        repetition
    );
    let root = cfg
        .work_root
        .join(run_id)
        .join(cell)
        .join(shape)
        .join(format!("r{repetition:02}"));
    std::fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    let owned = authorize_cleanup(ownership, &namespace, &root, cleanup_recipe(cell))?;
    cleanup_owned(owned, cfg.postgres.as_ref(), cfg.s3.as_ref())
}

fn launch_fragment(
    cfg: &Config,
    run_id: &str,
    commit: &str,
    cell: &str,
    shape: &str,
    repetition: usize,
    output: &Path,
) -> Result<fireweed_bench::performance_matrix::RepetitionResult, String> {
    if output.exists() {
        std::fs::remove_file(output).map_err(|error| error.to_string())?;
    }
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let child = Command::new("timeout")
        .arg("--signal=TERM")
        .arg("--kill-after=10s")
        .arg("15m")
        .arg(executable)
        .arg("--tier")
        .arg(&cfg.tier)
        .arg("--fragment")
        .arg("--run-id")
        .arg(run_id)
        .arg("--source-commit")
        .arg(commit)
        .arg("--cell")
        .arg(cell)
        .arg("--shape")
        .arg(shape)
        .arg("--round")
        .arg(repetition.to_string())
        .arg("--fragment-output")
        .arg(output)
        .spawn()
        .map_err(|error| format!("launch 15-minute fragment boundary: {error}"))?;
    let status = wait_for_fragment(child)?;
    if !status.success() {
        return Err(if status.code() == Some(124) {
            "fragment exceeded the 15-minute timeout".into()
        } else {
            format!("fragment subprocess exited with {status}")
        });
    }
    let row = read_fragment(output)?;
    if row.cell != cell || row.shape != shape || row.repetition != repetition {
        return Err("fragment result coordinates do not match the request".into());
    }
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
fn launch_lifecycle_fragment(
    cfg: &Config,
    run_id: &str,
    commit: &str,
    phase: &str,
    cell: &str,
    shape: &str,
    repetition: usize,
    output: &Path,
) -> Result<LifecycleFragment, String> {
    if output.exists() {
        std::fs::remove_file(output).map_err(|error| error.to_string())?;
    }
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let child = Command::new("timeout")
        .arg("--signal=TERM")
        .arg("--kill-after=10s")
        .arg("15m")
        .arg(executable)
        .arg("--tier")
        .arg(&cfg.tier)
        .arg("--fragment")
        .arg("--phase")
        .arg(phase)
        .arg("--run-id")
        .arg(run_id)
        .arg("--source-commit")
        .arg(commit)
        .arg("--cell")
        .arg(cell)
        .arg("--shape")
        .arg(shape)
        .arg("--round")
        .arg(repetition.to_string())
        .arg("--fragment-output")
        .arg(output)
        .spawn()
        .map_err(|error| format!("launch 15-minute lifecycle fragment boundary: {error}"))?;
    let status = wait_for_fragment(child)?;
    if !status.success() {
        return Err(if status.code() == Some(124) {
            "lifecycle fragment exceeded the 15-minute timeout".into()
        } else {
            format!("lifecycle fragment subprocess exited with {status}")
        });
    }
    let result = read_lifecycle_fragment(output)?;
    let matches = match &result {
        LifecycleFragment::Recovery(value) => {
            phase == "recovery"
                && value.cell == cell
                && value.population.shape == shape
                && value.repetition == repetition
        }
        LifecycleFragment::Maintenance(value) => {
            phase == "maintenance"
                && value.cell == cell
                && value.population.shape == shape
                && value.repetition == repetition
        }
    };
    if !matches {
        return Err("lifecycle fragment result coordinates do not match the request".into());
    }
    Ok(result)
}

fn run(cfg: Config) -> Result<PathBuf, String> {
    let started = now_ms();
    let commit = command_output("git", &["rev-parse", "HEAD"]);
    let branch = command_output("git", &["branch", "--show-current"]);
    let cells = cells_for_tier(&cfg.tier);
    let shapes = shape_specs(&cfg.tier);
    let repetitions = repetitions_for_tier(&cfg.tier);
    let resolved = resolved_config(&cfg, cells, &shapes);
    let checkpoint_file = checkpoint_path(&commit, &cfg.tier);
    let mut checkpoint = if cfg.resume {
        let value = read_checkpoint(&checkpoint_file).map_err(|error| {
            format!(
                "cannot resume checkpoint {}: {error}",
                checkpoint_file.display()
            )
        })?;
        value.validate_binding(&commit, &cfg.tier, &resolved)?;
        value
    } else {
        MatrixCheckpoint::new(
            commit.clone(),
            cfg.tier.clone(),
            format!("{}-{}", started, std::process::id()),
            &resolved,
        )
    };
    let run_id = checkpoint.run_id.clone();
    let redactor = SecretRedactor::new(cfg.postgres.as_ref(), cfg.s3.as_ref());
    let _locks = if cfg.tier == "full" {
        ServiceLocks::acquire(cfg.postgres.as_ref(), cfg.s3.as_ref(), &run_id, &commit)
            .map_err(|error| redactor.redact(error))?
    } else {
        ServiceLocks::acquire(None, None, &run_id, &commit)?
    };
    let ownership = RunOwnership::new(&cfg.work_root, &run_id)?;
    write_safe_checkpoint(&checkpoint_file, &checkpoint, &redactor)?;
    let fragment_root = cfg.work_root.join(&run_id).join("fragment-results");
    let mut failures = Vec::new();
    let warmup_run_id = format!("{run_id}-warmup");
    let warmup_ownership = RunOwnership::new(&cfg.work_root, &warmup_run_id)?;
    for (shape, _, _) in &shapes {
        for cell in cells {
            check_cancelled()?;
            if !(0..repetitions).any(|round| !checkpoint.contains(cell, shape.name, round)) {
                continue;
            }
            if let Err(error) = clean_fragment_state(
                &cfg,
                &warmup_ownership,
                &warmup_run_id,
                &commit,
                cell,
                shape.name,
                0,
            ) {
                failures.push(format!(
                    "{cell}/{} warmup pre-clean: {}",
                    shape.name,
                    redactor.redact(error)
                ));
                write_failure_journal(
                    &checkpoint_file,
                    &run_id,
                    &commit,
                    &cfg.tier,
                    &failures,
                    &redactor,
                )?;
                continue;
            }
            let warmup_output =
                fragment_root.join(format!("warmup-{cell}-{}-r00.json", shape.name));
            if let Err(error) = launch_fragment(
                &cfg,
                &warmup_run_id,
                &commit,
                cell,
                shape.name,
                0,
                &warmup_output,
            ) {
                let cleanup_error = clean_fragment_state(
                    &cfg,
                    &warmup_ownership,
                    &warmup_run_id,
                    &commit,
                    cell,
                    shape.name,
                    0,
                )
                .err()
                .map(|value| format!("; cleanup: {value}"))
                .unwrap_or_default();
                failures.push(format!(
                    "{cell}/{} warmup: {}",
                    shape.name,
                    redactor.redact(format!("{error}{cleanup_error}"))
                ));
                write_failure_journal(
                    &checkpoint_file,
                    &run_id,
                    &commit,
                    &cfg.tier,
                    &failures,
                    &redactor,
                )?;
            } else {
                println!("warmed {cell} {}", shape.name);
            }
        }
    }
    for round in 0..repetitions {
        for (shape_index, (shape, _, _)) in shapes.iter().enumerate() {
            let mut scheduled_cells = cells.to_vec();
            let cell_count = scheduled_cells.len();
            scheduled_cells.rotate_left((round + shape_index) % cell_count);
            if round % 2 == 1 {
                scheduled_cells.reverse();
            }
            for cell in scheduled_cells {
                check_cancelled()?;
                if checkpoint.contains(cell, shape.name, round) {
                    println!("resumed {cell} {} r{round}", shape.name);
                    continue;
                }
                if let Err(error) = clean_fragment_state(
                    &cfg, &ownership, &run_id, &commit, cell, shape.name, round,
                ) {
                    failures.push(format!(
                        "{cell}/{} r{round} pre-clean: {}",
                        shape.name,
                        redactor.redact(error)
                    ));
                    write_failure_journal(
                        &checkpoint_file,
                        &run_id,
                        &commit,
                        &cfg.tier,
                        &failures,
                        &redactor,
                    )?;
                    continue;
                }
                let fragment_output =
                    fragment_root.join(format!("{cell}-{}-r{round:02}.json", shape.name));
                match launch_fragment(
                    &cfg,
                    &run_id,
                    &commit,
                    cell,
                    shape.name,
                    round,
                    &fragment_output,
                ) {
                    Ok(row) => {
                        checkpoint.append(row)?;
                        write_safe_checkpoint(&checkpoint_file, &checkpoint, &redactor)?;
                        println!("passed {cell} {} r{round}", shape.name);
                    }
                    Err(error) => {
                        let cleanup_error = clean_fragment_state(
                            &cfg, &ownership, &run_id, &commit, cell, shape.name, round,
                        )
                        .err()
                        .map(|value| format!("; cleanup: {value}"))
                        .unwrap_or_default();
                        let error = redactor.redact(format!("{error}{cleanup_error}"));
                        failures.push(format!("{cell}/{} r{round}: {error}", shape.name));
                        write_failure_journal(
                            &checkpoint_file,
                            &run_id,
                            &commit,
                            &cfg.tier,
                            &failures,
                            &redactor,
                        )?;
                    }
                }
            }
        }
    }
    if cfg.tier == "full" {
        for (shape, _, _) in shapes
            .iter()
            .filter(|(shape, _, _)| matches!(shape.name, "minimal" | "record-1k"))
        {
            for cell in RECOVERY_CELLS {
                for round in 0..3 {
                    check_cancelled()?;
                    if checkpoint.contains_recovery(cell, shape.name, round) {
                        println!("resumed recovery {cell} {} r{round}", shape.name);
                        continue;
                    }
                    let coordinate_shape = format!("recovery-{}", shape.name);
                    if let Err(error) = clean_fragment_state(
                        &cfg,
                        &ownership,
                        &run_id,
                        &commit,
                        cell,
                        &coordinate_shape,
                        round,
                    ) {
                        failures.push(format!(
                            "recovery/{cell}/{} r{round} pre-clean: {}",
                            shape.name,
                            redactor.redact(error)
                        ));
                        write_failure_journal(
                            &checkpoint_file,
                            &run_id,
                            &commit,
                            &cfg.tier,
                            &failures,
                            &redactor,
                        )?;
                        continue;
                    }
                    let fragment_output = fragment_root
                        .join(format!("recovery-{cell}-{}-r{round:02}.json", shape.name));
                    match launch_lifecycle_fragment(
                        &cfg,
                        &run_id,
                        &commit,
                        "recovery",
                        cell,
                        shape.name,
                        round,
                        &fragment_output,
                    ) {
                        Ok(result) => {
                            checkpoint.append_lifecycle(result)?;
                            write_safe_checkpoint(&checkpoint_file, &checkpoint, &redactor)?;
                            println!("passed recovery {cell} {} r{round}", shape.name);
                        }
                        Err(error) => {
                            let cleanup_error = clean_fragment_state(
                                &cfg,
                                &ownership,
                                &run_id,
                                &commit,
                                cell,
                                &coordinate_shape,
                                round,
                            )
                            .err()
                            .map(|value| format!("; cleanup: {value}"))
                            .unwrap_or_default();
                            let error = redactor.redact(format!("{error}{cleanup_error}"));
                            failures
                                .push(format!("recovery/{cell}/{} r{round}: {error}", shape.name));
                            write_failure_journal(
                                &checkpoint_file,
                                &run_id,
                                &commit,
                                &cfg.tier,
                                &failures,
                                &redactor,
                            )?;
                        }
                    }
                }
            }
        }
        let maintenance_shape = shapes
            .iter()
            .find(|(shape, _, _)| shape.name == "record-1k")
            .map(|(shape, _, _)| shape)
            .expect("full tier includes record-1k");
        for cell in MAINTENANCE_CELLS {
            for round in 0..3 {
                check_cancelled()?;
                if checkpoint.contains_maintenance(cell, round) {
                    println!("resumed maintenance {cell} r{round}");
                    continue;
                }
                let coordinate_shape = "maintenance-record-1k";
                if let Err(error) = clean_fragment_state(
                    &cfg,
                    &ownership,
                    &run_id,
                    &commit,
                    cell,
                    coordinate_shape,
                    round,
                ) {
                    failures.push(format!(
                        "maintenance/{cell} r{round} pre-clean: {}",
                        redactor.redact(error)
                    ));
                    write_failure_journal(
                        &checkpoint_file,
                        &run_id,
                        &commit,
                        &cfg.tier,
                        &failures,
                        &redactor,
                    )?;
                    continue;
                }
                let fragment_output =
                    fragment_root.join(format!("maintenance-{cell}-r{round:02}.json"));
                match launch_lifecycle_fragment(
                    &cfg,
                    &run_id,
                    &commit,
                    "maintenance",
                    cell,
                    maintenance_shape.name,
                    round,
                    &fragment_output,
                ) {
                    Ok(result) => {
                        checkpoint.append_lifecycle(result)?;
                        write_safe_checkpoint(&checkpoint_file, &checkpoint, &redactor)?;
                        println!("passed maintenance {cell} r{round}");
                    }
                    Err(error) => {
                        let cleanup_error = clean_fragment_state(
                            &cfg,
                            &ownership,
                            &run_id,
                            &commit,
                            cell,
                            coordinate_shape,
                            round,
                        )
                        .err()
                        .map(|value| format!("; cleanup: {value}"))
                        .unwrap_or_default();
                        let error = redactor.redact(format!("{error}{cleanup_error}"));
                        failures.push(format!("maintenance/{cell} r{round}: {error}"));
                        write_failure_journal(
                            &checkpoint_file,
                            &run_id,
                            &commit,
                            &cfg.tier,
                            &failures,
                            &redactor,
                        )?;
                    }
                }
            }
        }
    }
    if !failures.is_empty() {
        return Err(format!(
            "matrix failed; resume from {} (failure journal {})",
            checkpoint_file.display(),
            checkpoint_file.with_extension("failures.json").display()
        ));
    }
    let schedule = build_schedule(&cfg.tier)?;
    let rows = schedule
        .iter()
        .filter(|entry| entry.phase == "common")
        .map(|entry| {
            checkpoint
                .rows
                .iter()
                .find(|row| {
                    row.cell == entry.cell
                        && row.shape == entry.shape
                        && row.repetition == entry.repetition
                })
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "checkpoint is missing scheduled row {}/{}/r{}",
                        entry.cell, entry.shape, entry.repetition
                    )
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let provenance = collect_provenance();
    let service_topology =
        collect_service_topology(&cfg).map_err(|error| redactor.redact(error))?;
    let source_clean =
        command_output("git", &["status", "--porcelain", "--untracked-files=no"]).is_empty();
    let submodule_status = command_output("git", &["submodule", "status", "--recursive"]);
    let command = std::env::args().skip(1).collect::<Vec<_>>();
    let shape_evidence = shapes
        .iter()
        .map(|(shape, items, batch)| ShapeEvidence {
            id: shape.name.into(),
            items: *items,
            batch: *batch,
        })
        .collect();
    let cell_evidence = cells
        .iter()
        .map(|cell| CellEvidence {
            id: (*cell).into(),
            barrier_class: barrier_class(cell).into(),
            status: if failures.iter().any(|failure| failure.starts_with(cell)) {
                "failed".into()
            } else {
                "passed".into()
            },
        })
        .collect();
    let summaries = build_summaries(&rows);
    let comparisons = build_comparisons(&rows, &summaries);
    let mut cleanup = rows
        .iter()
        .map(|row| CleanupEvidence {
            phase: "common".into(),
            cell: row.cell.clone(),
            shape: row.shape.clone(),
            repetition: row.repetition,
            logical_namespace: format!(
                "fireweed-perf/v1/{}/{}/{}/{}/r{:02}",
                &commit[..commit.len().min(12)],
                run_id,
                row.cell,
                row.shape,
                row.repetition
            ),
            status: "passed".into(),
        })
        .collect::<Vec<_>>();
    cleanup.extend(checkpoint.recovery.iter().map(|result| CleanupEvidence {
        phase: "recovery".into(),
        cell: result.cell.clone(),
        shape: result.population.shape.clone(),
        repetition: result.repetition,
        logical_namespace: format!(
            "fireweed-perf/v1/{}/{}/{}/recovery-{}/r{:02}",
            &commit[..commit.len().min(12)],
            run_id,
            result.cell,
            result.population.shape,
            result.repetition
        ),
        status: "passed".into(),
    }));
    cleanup.extend(checkpoint.maintenance.iter().map(|result| CleanupEvidence {
        phase: "maintenance".into(),
        cell: result.cell.clone(),
        shape: result.population.shape.clone(),
        repetition: result.repetition,
        logical_namespace: format!(
            "fireweed-perf/v1/{}/{}/{}/maintenance-{}/r{:02}",
            &commit[..commit.len().min(12)],
            run_id,
            result.cell,
            result.population.shape,
            result.repetition
        ),
        status: "passed".into(),
    }));
    let evidence = MatrixEvidence {
        schema_version: SCHEMA_VERSION.into(),
        run_id: run_id.clone(),
        tier: cfg.tier.clone(),
        status: if failures.is_empty() {
            "passed".into()
        } else {
            "failed".into()
        },
        command,
        seed: 0x5eed_f17e_0eed_u64,
        resolved_config_sha256: digest_hex(&resolved),
        schedule,
        unsupported_cells: vec![
            "objectlog-s3-memory".into(),
            "objectlog-*-postgres-async".into(),
        ],
        git_commit: commit.clone(),
        git_branch: branch,
        host_fingerprint_sha256: provenance.host_fingerprint_sha256.clone(),
        provenance,
        source_clean,
        submodule_status,
        enabled_features: "fireweed/default+postgres;profile=release".into(),
        rustflags_sha256: digest_hex(std::env::var("RUSTFLAGS").unwrap_or_default().as_bytes()),
        service_topology,
        started_unix_ms: started,
        finished_unix_ms: now_ms(),
        shapes: shape_evidence,
        cells: cell_evidence,
        summaries,
        comparisons,
        recovery: checkpoint.recovery.clone(),
        maintenance: checkpoint.maintenance.clone(),
        cleanup,
        repetitions: rows,
        failures,
    };
    let path = output_directory_or_file(cfg.output.as_deref(), &commit, &run_id);
    redactor.validate_serialized_evidence(&canonical_bytes(&evidence)?)?;
    write_evidence(&path, &evidence)?;
    verify_file(&path)?;
    if evidence.status != "passed" {
        return Err(format!("matrix failed; evidence at {}", path.display()));
    }
    Ok(path)
}

fn main() {
    let fragment_process = std::env::args().any(|argument| argument == "--fragment");
    if !fragment_process
        && let Err(error) = ctrlc::set_handler(|| CANCELLED.store(true, Ordering::Release))
    {
        eprintln!("performance matrix failed to install signal handler: {error}");
        std::process::exit(1);
    }
    let result = parse_config().and_then(|mut cfg| {
        if let Some(fragment) = cfg.fragment.take() {
            run_fragment_mode(&cfg, &fragment)?;
            Ok(None)
        } else {
            run(cfg).map(Some)
        }
    });
    match result {
        Ok(Some(path)) => println!("performance matrix passed: {}", path.display()),
        Ok(None) => {}
        Err(error) => {
            eprintln!("performance matrix failed: {error}");
            std::process::exit(1);
        }
    }
}
