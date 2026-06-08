use pqueue_core::scaffold;

fn decode_command(input: &[u8]) -> usize {
    input
        .iter()
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .count()
}

#[test]
fn command_decode_scaffold_accepts_representative_inputs() {
    let cases = [
        b"push".as_slice(),
        b"claim".as_slice(),
        b"finalize".as_slice(),
        b"redrive".as_slice(),
    ];

    for case in cases {
        assert!(decode_command(case) > 0);
    }

    assert_eq!(scaffold::name(), "pqueue-core");
}
