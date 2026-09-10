use fireweed_workload::{Config, Profile, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    let mut root = None;
    let mut primitives = false;
    let mut retention = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--recycle" => config.recycle = true,
            "--cycles" => config.cycles = args.next().ok_or("missing cycles")?.parse()?,
            "--items" => config.items = args.next().ok_or("missing items")?.parse()?,
            "--batch" => config.batch = args.next().ok_or("missing batch")?.parse()?,
            "--shards" => config.shards = args.next().ok_or("missing shards")?.parse()?,
            "--load-workers" => config.load_workers = args.next().ok_or("missing load workers")?.parse()?,
            "--workers" => config.workers = args.next().ok_or("missing workers")?.parse()?,
            "--payload-bytes" => {
                config.payload_bytes = args.next().ok_or("missing payload bytes")?.parse()?
            }
            "--deadline-seconds" => {
                config.deadline =
                    Duration::from_secs(args.next().ok_or("missing deadline")?.parse()?)
            }
            "--memory" => config.memory = true,
            "--no-faults" => config.faults = false,
            "--projection-root" => {
                config.projection_root = Some(std::path::PathBuf::from(
                    args.next().ok_or("missing projection root")?,
                ))
            }
            "--root" => root = Some(std::path::PathBuf::from(args.next().ok_or("missing root")?)),
            "--profile" => {
                config.profile = match args.next().as_deref() {
                    Some("retention") => {
                        retention = true;
                        Profile::Bulk
                    }
                    Some("primitives") => {
                        primitives = true;
                        Profile::Bulk
                    }
                    Some("bulk") => Profile::Bulk,
                    Some("mutable") => Profile::Mutable,
                    Some("snorri") => Profile::Snorri,
                    _ => {
                        return Err(
                            "profile must be primitives, retention, bulk, mutable, or snorri"
                                .into(),
                        );
                    }
                }
            }
            "--help" => {
                println!(
                    "fireweed-workload [--profile primitives|retention|bulk|mutable|snorri] [--items N] [--recycle --cycles N] [--batch 1..1000] [--shards N] [--workers N] [--load-workers N] [--payload-bytes N] [--deadline-seconds N] [--memory] [--no-faults] [--root NEW_DIRECTORY] [--projection-root NEW_DIRECTORY]"
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    let temporary = tempfile::tempdir()?;
    let root = root.as_deref().unwrap_or(temporary.path());
    if root.exists() && std::fs::read_dir(root)?.next().is_some() {
        return Err("root must be empty; refusing to overwrite data".into());
    }
    if let Some(path) = &config.projection_root {
        if config.memory {
            return Err("projection-root requires the durable-log Turso cell".into());
        }
        if path.exists() && std::fs::read_dir(path)?.next().is_some() {
            return Err("projection root must be empty; refusing to overwrite data".into());
        }
    }
    let report = if retention {
        fireweed_workload::retention::run(config, root).await?
    } else if primitives {
        fireweed_workload::primitives::run(config, root).await?
    } else {
        fireweed_workload::run(config, root).await?
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
