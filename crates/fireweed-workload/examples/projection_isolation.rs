//! Diagnostic client of the existing high-level campaign API. The caller owns
//! empty, distinct shard directories, which may be symlinked onto several mounts.
//! Both disk and RAM comparisons use this same executable and campaign code.
use fireweed_workload::{Config, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn validate_layout(log_root: &Path, projection_root: &Path, shards: usize) -> Result<()> {
    if shards == 0 || !log_root.is_dir() || std::fs::read_dir(log_root)?.next().is_some() {
        return Err("log root must be an existing empty directory; shards must be positive".into());
    }
    if std::fs::read_dir(projection_root)?.count() != shards {
        return Err("projection root must contain exactly the declared shard directories".into());
    }
    let log_root = log_root.canonicalize()?;
    let mut destinations = HashSet::new();
    for shard in 0..shards {
        let path = projection_root.join(format!("shard-{shard}"));
        let target = path.canonicalize()?;
        if !target.is_dir()
            || target == log_root
            || target.starts_with(&log_root)
            || std::fs::read_dir(&target)?.next().is_some()
            || !destinations.insert(target)
        {
            return Err(
                "each projection shard must be distinct, empty, and outside the log root".into(),
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config {
        items: 1_000_000,
        cycles: 8,
        recycle: true,
        batch: 1000,
        shards: 64,
        workers: 2,
        load_workers: 2,
        purge_batch: Some(8000),
        campaign_metadata_only: true,
        campaign_timestamp_priority: true,
        deadline: Duration::from_secs(900),
        ..Config::default()
    };
    let mut root = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--root" => root = Some(PathBuf::from(value)),
            "--projection-root" => config.projection_root = Some(PathBuf::from(value)),
            // Smaller correctness smokes remain diagnostic and cannot qualify.
            "--items" => config.items = value.parse()?,
            "--cycles" => config.cycles = value.parse()?,
            "--shards" => config.shards = value.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    let root = root.ok_or("--root is required")?;
    let projection_root = config
        .projection_root
        .as_deref()
        .ok_or("--projection-root is required")?;
    if config.items == 0 || config.cycles == 0 {
        return Err("items and cycles must be positive".into());
    }
    validate_layout(&root, projection_root, config.shards)?;
    let mut report = fireweed_workload::campaign::run(config, &root).await?;
    report["diagnostic_fixture"] = "projection_isolation/high_level_api/v1".into();
    report["not_workflow_qualification"] = true.into();
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn layout_rejects_shared_targets_existing_data_and_unexpected_entries() {
        let log = tempfile::tempdir().unwrap();
        let routing = tempfile::tempdir().unwrap();
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(first.path(), routing.path().join("shard-0")).unwrap();
        std::os::unix::fs::symlink(second.path(), routing.path().join("shard-1")).unwrap();
        validate_layout(log.path(), routing.path(), 2).unwrap();
        std::fs::write(second.path().join("existing.db"), b"preserve").unwrap();
        assert!(validate_layout(log.path(), routing.path(), 2).is_err());
        assert_eq!(
            std::fs::read(second.path().join("existing.db")).unwrap(),
            b"preserve"
        );
        std::fs::remove_file(second.path().join("existing.db")).unwrap();
        std::fs::remove_file(routing.path().join("shard-1")).unwrap();
        std::os::unix::fs::symlink(first.path(), routing.path().join("shard-1")).unwrap();
        assert!(validate_layout(log.path(), routing.path(), 2).is_err());
        std::fs::write(routing.path().join("unexpected"), b"preserve").unwrap();
        assert!(validate_layout(log.path(), routing.path(), 2).is_err());
    }
}
