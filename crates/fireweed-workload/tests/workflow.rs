use fireweed_workload::{Config, Profile};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_workflows_reach_exact_outcomes() {
    for memory in [true, false] {
        for profile in [Profile::Bulk, Profile::Mutable, Profile::Snorri] {
            let root = tempfile::tempdir().unwrap();
            let config = Config {
                memory,
                profile,
                workers: 2,
                deadline: std::time::Duration::from_secs(30),
                ..Default::default()
            };
            let report = fireweed_workload::run(config, root.path())
                .await
                .unwrap_or_else(|error| panic!("{memory:?}/{profile:?}: {error}"));
            assert_eq!(report["shards"][0]["delivered"], 116);
            assert_eq!(report["shards"][0]["failed"], 4);
            assert_eq!(report["shards"][0]["retries"], 7);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_primitives_settle_and_purge() {
    for memory in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            memory,
            items: 240,
            shards: 2,
            deadline: std::time::Duration::from_secs(30),
            ..Default::default()
        };
        fireweed_workload::primitives::run(config, root.path())
            .await
            .unwrap();
    }
}

// Cross several selector pages while asynchronous projection removes earlier
// claimed rows. OFFSET pagination used to silently skip lower-priority rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_backlog_drains_in_priority_order() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        items: 10_000,
        batch: 100,
        deadline: std::time::Duration::from_secs(300),
        ..Default::default()
    };
    fireweed_workload::primitives::run(config, root.path())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_store_capacity_profile_reuses_keys() {
    for memory in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            memory,
            items: 120,
            cycles: 3,
            deadline: std::time::Duration::from_secs(45),
            ..Default::default()
        };
        fireweed_workload::retention::run(config, root.path())
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_rows_recycle_across_shards_with_exact_fault_outcomes() {
    for memory in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            memory,
            items: 240,
            shards: 2,
            workers: 2,
            recycle: true,
            cycles: 4,
            deadline: std::time::Duration::from_secs(60),
            ..Default::default()
        };
        let report = fireweed_workload::run(config, root.path()).await.unwrap();
        assert_eq!(report["includes_purge"], true);
        for shard in report["shards"].as_array().unwrap() {
            assert_eq!(shard["cycles"].as_array().unwrap().len(), 4);
            for cycle in shard["cycles"].as_array().unwrap() {
                assert_eq!(cycle["items"], 120);
                assert_eq!(cycle["delivered"], 116);
                assert_eq!(cycle["failed"], 4);
                assert_eq!(cycle["pending"], 0);
                assert_eq!(cycle["leased"], 0);
            }
        }
    }
}

// Full-sized claim/mutation batches must deliver every result to its caller,
// including when one worker drives another worker's generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_batches_recycle_original_rows_without_orphaned_leases() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        items: 12_500,
        batch: 1000,
        purge_batch: Some(8000),
        workers: 4,
        recycle: true,
        cycles: 3,
        // This guards completion and lease accounting, not host throughput.
        // Unoptimized projection execution needs a separate bounded watchdog.
        deadline: std::time::Duration::from_secs(if cfg!(debug_assertions) { 600 } else { 120 }),
        ..Default::default()
    };
    let report = fireweed_workload::run(config, root.path()).await.unwrap();
    for cycle in report["shards"][0]["cycles"].as_array().unwrap() {
        assert_eq!(cycle["pending"], 0);
        assert_eq!(cycle["leased"], 0);
        assert_eq!(
            cycle["delivered"].as_u64().unwrap() + cycle["failed"].as_u64().unwrap(),
            12_500
        );
    }
}

// Concurrent producers use the same public API as sequential loading. Every
// original recipient must survive overlapping loads, claims, retries and purge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_loaders_recycle_every_original_recipient() {
    for memory in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            memory,
            items: 240,
            shards: 2,
            batch: 10,
            workers: 2,
            load_workers: 4,
            recycle: true,
            cycles: 3,
            deadline: std::time::Duration::from_secs(60),
            ..Default::default()
        };
        let report = fireweed_workload::run(config, root.path()).await.unwrap();
        assert_eq!(report["load_workers_per_shard"], 4);
        for shard in report["shards"].as_array().unwrap() {
            for cycle in shard["cycles"].as_array().unwrap() {
                assert_eq!(cycle["delivered"], 116);
                assert_eq!(cycle["failed"], 4);
                assert_eq!(cycle["pending"], 0);
                assert_eq!(cycle["leased"], 0);
            }
        }
    }
}
