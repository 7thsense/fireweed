// Provenance: crates/fireweed/tests/active_scope_routing.rs::bounded_selection_is_stable_dispersed_and_preserves_source_order
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn bounded_selection_is_stable_dispersed_and_preserves_source_order() {
    let q = queue("tenant", "queue");
    let source = vec![
        scope("queue", Some("g0"), 10),
        scope("queue", Some("g1"), 9),
        scope("queue", Some("g2"), 8),
        scope("queue", Some("outside"), 7),
    ];
    let prefix = OldestFirstScopePrefix::attest(stamped(&q, source.clone())).unwrap();
    let mut selected = BTreeSet::new();
    for worker in 0..256 {
        let routing_key = format!("worker-{worker}");
        let first =
            select_active_scope_from_prefix(&prefix, &q, routing_key.as_bytes(), 3, 10_000, 0, 0)
                .unwrap();
        let repeat =
            select_active_scope_from_prefix(&prefix, &q, routing_key.as_bytes(), 3, 10_000, 0, 0)
                .unwrap();
        assert_eq!(first, repeat);
        assert!(first.index < 3, "selection escaped its leading window");
        selected.insert(first.index);
    }
    assert!(
        selected.len() > 1,
        "routing keys should disperse ordinary work"
    );
    assert_eq!(prefix.scopes(), source.as_slice());

    let oversized =
        select_active_scope_from_prefix(&prefix, &q, b"oversized-window", usize::MAX, 10_000, 0, 0)
            .unwrap();
    assert!(oversized.index < source.len());
}
