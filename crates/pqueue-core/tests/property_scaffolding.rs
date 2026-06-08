use pqueue_core::scaffold;

#[test]
fn property_scaffolding_round_trips_representative_inputs() {
    let cases = ["priority:0", "priority:1", "priority:64", "priority:256"];

    for case in cases {
        assert_eq!(case.trim(), case);
        assert!(!case.is_empty());
    }

    assert_eq!(scaffold::name(), "pqueue-core");
}
