//! TP-005 million-cycle-v1 gate: insert/modify/read+verify fixed work per matrix cell.
//!
//! Usage:
//!   fireweed-million-cycle --tier probe            # 2k items, local 9 cells
//!   fireweed-million-cycle --tier production       # 1M items, requires services for full 20
//!   fireweed-million-cycle --cell memory--memory --tier probe

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    ConfigSecret, Fireweed, LogConfig, ObjectLogAuthority, PostgresMode, ProjectionStoreConfig,
    RecoveryAction, RecoveryPolicy, ResponseBarrier, SegmentConfig, StorageConfig, open,
    open_async,
};
use fireweed_bench::performance_matrix_cells::{FULL_CELL_IDS, SMOKE_CELL_IDS, parse_cell};
use fireweed_bench::performance_matrix_million_cycle::{
    WorkSizes, run_million_cycle_with,
};
use fireweed_bench::performance_matrix_services::{
    ObjectStoreService, PostgresService, derived_plain_schema,
};
use fireweed_bench::{SystemClock, bench_qdef, qkey};

struct Args {
    tier: String,
    cell: Option<String>,
    output: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut tier = "probe".to_owned();
    let mut cell = None;
    let mut output = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--tier" => {
                tier = args
                    .get(i + 1)
                    .ok_or("--tier requires probe or production")?
                    .clone();
                i += 2;
            }
            "--cell" => {
                cell = Some(args.get(i + 1).ok_or("--cell requires id")?.clone());
                i += 2;
            }
            "--output" => {
                output = Some(PathBuf::from(
                    args.get(i + 1).ok_or("--output requires path")?,
                ));
                i += 2;
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if !matches!(tier.as_str(), "probe" | "production") {
        return Err("--tier must be probe or production".into());
    }
    Ok(Args { tier, cell, output })
}

fn sizes(tier: &str) -> WorkSizes {
    if tier == "production" {
        WorkSizes::production()
    } else {
        WorkSizes::probe()
    }
}

fn cells(tier: &str, only: Option<&str>) -> Result<Vec<&'static str>, String> {
    let base: &[&str] = if tier == "production" {
        FULL_CELL_IDS
    } else {
        SMOKE_CELL_IDS
    };
    if let Some(c) = only {
        parse_cell(c)?;
        let found = base
            .iter()
            .copied()
            .find(|id| *id == c)
            .ok_or_else(|| format!("cell {c} not in {tier} register"))?;
        return Ok(vec![found]);
    }
    Ok(base.to_vec())
}

fn postgres_service() -> Option<PostgresService> {
    std::env::var("FIREWEED_PERF_POSTGRES_URL")
        .ok()
        .map(|url| PostgresService { url })
}

fn s3_service() -> Option<ObjectStoreService> {
    let endpoint = std::env::var("FIREWEED_PERF_S3_ENDPOINT").ok()?;
    let bucket = std::env::var("FIREWEED_PERF_S3_BUCKET").ok()?;
    let region = std::env::var("FIREWEED_PERF_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("FIREWEED_PERF_S3_ACCESS_KEY").ok()?;
    let secret = std::env::var("FIREWEED_PERF_S3_SECRET_KEY").ok()?;
    Some(ObjectStoreService {
        endpoint,
        bucket,
        region,
        access,
        secret,
    })
}

fn build_config(
    cell: &str,
    root: &Path,
    namespace: &str,
    postgres: Option<&PostgresService>,
    s3: Option<&ObjectStoreService>,
) -> Result<StorageConfig, String> {
    let (log_axis, proj_axis) = parse_cell(cell)?;
    let log = match log_axis {
        "memory" => LogConfig::Memory,
        "sqlite" => LogConfig::Sqlite {
            path: root.join("log.sqlite"),
        },
        "postgres" => {
            let url = postgres
                .map(|p| p.url.as_str())
                .ok_or("PostgreSQL required for this cell")?;
            LogConfig::Postgres {
                url: ConfigSecret::new(url),
                schema: Some(derived_plain_schema(namespace)),
                mode: if proj_axis == "postgres" {
                    PostgresMode::Relational
                } else {
                    PostgresMode::LogReplay
                },
                node_id: None,
                coordination: None,
            }
        }
        "filesystem" => LogConfig::Filesystem {
            root: root.join("log"),
        },
        "s3" => {
            let s3 = s3.ok_or("S3 required for this cell")?;
            LogConfig::S3 {
                endpoint: s3.endpoint.clone(),
                bucket: s3.bucket.clone(),
                region: s3.region.clone(),
                access_key_id: ConfigSecret::new(&s3.access),
                secret_access_key: ConfigSecret::new(&s3.secret),
                allow_insecure_http: s3.endpoint.starts_with("http://"),
            }
        }
        other => return Err(format!("unknown log {other}")),
    };
    let projection = match proj_axis {
        "memory" => ProjectionStoreConfig::Memory,
        "sqlite" => ProjectionStoreConfig::Sqlite {
            path: root.join("projection.sqlite"),
        },
        "turso" => ProjectionStoreConfig::Turso {
            path: root.join("projection.turso"),
        },
        "postgres" => {
            let url = postgres
                .map(|p| p.url.as_str())
                .ok_or("PostgreSQL required for this cell")?;
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(url),
            }
        }
        other => return Err(format!("unknown projection {other}")),
    };
    let authority = matches!(log_axis, "filesystem" | "s3")
        .then_some(ObjectLogAuthority::NativeConditionalWrite);
    Ok(StorageConfig {
        log,
        projection,
        control_plane: None,
        authority,
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(262_144, 20).map_err(|e| e.to_string())?,
        namespace: namespace.into(),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    })
}

fn construct(
    cell: &str,
    root: &Path,
    namespace: &str,
    postgres: Option<&PostgresService>,
    s3: Option<&ObjectStoreService>,
) -> Result<Fireweed, String> {
    std::fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let config = build_config(cell, root, namespace, postgres, s3)?;
    let clock = Arc::new(SystemClock);
    let (log, proj) = parse_cell(cell)?;
    if matches!(log, "postgres") || matches!(proj, "postgres") {
        std::thread::scope(|scope| {
            scope
                .spawn(|| open(config, clock).map_err(|e| e.to_string()))
                .join()
                .map_err(|_| "construct thread panicked".to_owned())?
        })
    } else if matches!(log, "filesystem" | "s3") || matches!(proj, "turso") {
        fireweed_objectlog::block_on_objectlog_future(async move {
            open_async(config, clock).await.map_err(|e| e.to_string())
        })
    } else {
        open(config, clock).map_err(|e| e.to_string())
    }
}

fn drive_cell<F, T>(cell: &str, fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match parse_cell(cell) {
        Ok((log, _)) if matches!(log, "filesystem" | "s3") => {
            fireweed_objectlog::block_on_objectlog_future(fut)
        }
        Ok((log, "turso")) if !matches!(log, "postgres") => {
            fireweed_objectlog::block_on_objectlog_future(fut)
        }
        _ => futures::executor::block_on(fut),
    }
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("million-cycle failed: {e}");
            std::process::exit(2);
        }
    };
    let sizes = sizes(&args.tier);
    let cells = match cells(&args.tier, args.cell.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("million-cycle failed: {e}");
            std::process::exit(2);
        }
    };
    let postgres = postgres_service();
    let s3 = s3_service();
    if args.tier == "production"
        && cells.iter().any(|c| {
            parse_cell(c)
                .map(|(l, p)| matches!(l, "postgres" | "s3") || matches!(p, "postgres"))
                .unwrap_or(false)
        })
        && (postgres.is_none() || s3.is_none())
    {
        eprintln!("production full register needs FIREWEED_PERF_POSTGRES_URL and FIREWEED_PERF_S3_*");
        std::process::exit(2);
    }

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let work_root = std::env::temp_dir().join(format!("fireweed-million-cycle-{run_id}"));
    let mut results = Vec::new();
    let mut failures = Vec::new();

    for cell in &cells {
        let root = work_root.join(cell);
        let namespace = format!("mc/{run_id}/{cell}");
        let fireweed = match construct(cell, &root, &namespace, postgres.as_ref(), s3.as_ref()) {
            Ok(fw) => fw,
            Err(e) => {
                failures.push(format!("{cell}: construct: {e}"));
                eprintln!("FAIL {cell}: construct: {e}");
                continue;
            }
        };
        let mut def = bench_qdef("bench", "million-cycle", &fireweed_bench::all_shapes()[0]);
        def.max_push_batch_size = 1_000;
        def.max_claim_batch_size = 1_000;
        let queue = qkey("million-cycle");
        let result = drive_cell(
            cell,
            run_million_cycle_with(&fireweed, def, queue.clone(), cell, sizes),
        );
        match result {
            Ok(mut r) => {
                // Class A: reopen same namespace and require live items still present.
                // Class B (memory log): no durable log claim; reopen_ok marks the boundary check only.
                let is_class_b = parse_cell(cell).map(|(l, _)| l == "memory").unwrap_or(false);
                drop(fireweed);
                if is_class_b {
                    r.reopen_ok = true;
                } else {
                    match construct(cell, &root, &namespace, postgres.as_ref(), s3.as_ref()) {
                        Ok(reopened) => {
                            let keys: Vec<_> = (0..sizes.insert_items.min(100))
                                .map(|i| {
                                    fireweed::ClientItemKey::new(format!("mc-{i:09}"))
                                        .expect("key")
                                })
                                .collect();
                            r.reopen_ok = drive_cell(cell, async {
                                reopened
                                    .live_items(&queue, keys)
                                    .await
                                    .map(|v| v.iter().filter(|x| x.is_some()).count() > 0)
                                    .unwrap_or(false)
                            });
                            if !r.reopen_ok {
                                failures.push(format!("{cell}: reopen lost live items"));
                            }
                        }
                        Err(e) => {
                            failures.push(format!("{cell}: reopen: {e}"));
                            r.reopen_ok = false;
                        }
                    }
                }
                println!(
                    "PASS {cell}: insert={:.1}s modify={:.1}s read={:.1}s reopen={}",
                    r.insert_ns as f64 / 1e9,
                    r.modify_ns as f64 / 1e9,
                    r.read_verify_ns as f64 / 1e9,
                    r.reopen_ok
                );
                results.push(r);
            }
            Err(e) => {
                failures.push(format!("{cell}: {e}"));
                eprintln!("FAIL {cell}: {e}");
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    let report = serde_json::json!({
        "tier": args.tier,
        "sizes": {
            "insert": sizes.insert_items,
            "modify": sizes.modify_items,
            "batch": sizes.batch,
            "warmup": sizes.warmup_items,
        },
        "results": results,
        "failures": failures,
        "status": if failures.is_empty() { "passed" } else { "failed" },
    });
    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap_or_default())
        {
            eprintln!("write output: {e}");
        } else {
            println!("wrote {}", path.display());
        }
    }
    let _ = std::fs::remove_dir_all(&work_root);
    if !failures.is_empty() {
        eprintln!("million-cycle failed: {} failure(s)", failures.len());
        std::process::exit(1);
    }
    println!("million-cycle passed: {} cell(s)", results.len());
}
