use pqueue_service::scaffold;

#[test]
fn flaky_harness_scaffold_is_repeatable() {
    let expected = [
        scaffold::client_name(),
        scaffold::core_name(),
        scaffold::postgres_core_name(),
        scaffold::storage_name(),
    ];

    for _ in 0..16 {
        assert_eq!(
            [
                scaffold::client_name(),
                scaffold::core_name(),
                scaffold::postgres_core_name(),
                scaffold::storage_name(),
            ],
            expected
        );
    }
}
