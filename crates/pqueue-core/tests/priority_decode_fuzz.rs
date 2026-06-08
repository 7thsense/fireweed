use pqueue_core::scaffold;

fn parse_priority(input: &str) -> Result<u64, std::num::ParseIntError> {
    input.parse::<u64>()
}

#[test]
fn priority_decode_scaffold_accepts_representative_inputs() {
    let cases = ["0", "1", "64", "256", "18446744073709551615"];

    for case in cases {
        assert!(parse_priority(case).is_ok());
    }

    assert_eq!(scaffold::name(), "pqueue-core");
}
