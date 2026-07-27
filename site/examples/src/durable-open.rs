// Provenance: crates/fireweed/tests/concrete_fireweed.rs::role_named_object_log_configuration_validates
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn role_named_object_log_configuration_validates() {
    let config = ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: "object-log".into(),
        },
        authority: ObjectLogAuthority::NativeConditionalWrite,
        projection: ProjectionConfig::Sqlite {
            path: "projection.sqlite".into(),
        },
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(1024, 5).unwrap(),
        namespace: "downstream".to_string(),
        recovery: RecoveryPolicy::default(),
    };
    config.validate().unwrap();
}

// Provenance: crates/fireweed-bench/tests/e2e_shapes_tests.rs::lifecycle_over_shapes_sqlite_log
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn lifecycle_over_shapes_sqlite_log() {
    for shape in all_shapes() {
        let path = tmp(&format!("sqlite-{}", shape.name));
        let _ = std::fs::remove_file(&path);
        let fireweed = open_sqlite(path.to_str().unwrap(), Arc::new(SystemClock)).expect("open sqlite");
        run_one("sqlite", &fireweed, &shape, true);
        let _ = std::fs::remove_file(&path);
    }
}

// Provenance: crates/fireweed-bench/tests/e2e_shapes_tests.rs::lifecycle_over_shapes_sqlite_relational
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn lifecycle_over_shapes_sqlite_relational() {
    for shape in all_shapes() {
        let fireweed =
            open_sqlite_relational(":memory:", Arc::new(SystemClock)).expect("sqlite relational");
        run_one("sqlite_relational", &fireweed, &shape, true);
    }
}

// Provenance: crates/fireweed-bench/tests/e2e_shapes_tests.rs::lifecycle_over_shapes_objectlog
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn lifecycle_over_shapes_objectlog() {
    for shape in all_shapes() {
        let dir = tmp(&format!("objectlog-{}", shape.name));
        let _ = std::fs::remove_dir_all(&dir);
        let fireweed = open_objectlog(&dir, Arc::new(SystemClock)).expect("open objectlog");
        // Eventual-apply class: update_fields is refused, so the lifecycle skips it.
        run_one("objectlog", &fireweed, &shape, false);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
