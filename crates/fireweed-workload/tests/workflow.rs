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
