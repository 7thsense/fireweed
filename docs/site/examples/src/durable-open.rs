// Provenance: crates/fireweed-bench/tests/e2e_shapes_tests.rs::lifecycle_over_shapes_turso
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn lifecycle_over_shapes_turso() {
    for shape in all_shapes() {
        let root = tmp(&format!("turso-{}", shape.name));
        let fireweed = open_log_turso(&root, Arc::new(SystemClock)).expect("open Turso");
        run_one("turso", &fireweed, &shape, true);
        drop(fireweed);
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}

// Provenance: crates/fireweed-bench/tests/e2e_shapes_tests.rs::lifecycle_over_shapes_objectlog
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn lifecycle_over_shapes_objectlog() {
    for shape in all_shapes() {
        let dir = tmp(&format!("objectlog-{}", shape.name));
        let _ = std::fs::remove_dir_all(&dir);
        let fireweed = open_objectlog(&dir, Arc::new(SystemClock)).expect("open objectlog");
        // Exercise the current public batch-update contract on the original rows.
        run_one("objectlog", &fireweed, &shape, true);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
