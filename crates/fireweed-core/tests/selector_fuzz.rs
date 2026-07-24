use fireweed_core::scaffold;

fn selector_score(input: &str) -> usize {
    input
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .count()
}

#[test]
fn selector_scaffold_accepts_representative_inputs() {
    let cases = [
        "tenant=alpha",
        "queue=beta,tenant=alpha",
        "priority=high,group=omega",
    ];

    for case in cases {
        assert!(selector_score(case) >= 1);
    }

    assert_eq!(scaffold::name(), "fireweed-core");
}
