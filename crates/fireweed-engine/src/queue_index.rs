//! Order-preserving v1 index keys, separate from request fingerprint encoding.
//! Full values remain authoritative. Storage may use bounded prefixes/hash probes
//! to find candidates, but must compare complete keys and never truncate semantics.
use fireweed_core::queue_contract::{FieldType, FieldValue, QueueError, QueueResult};

fn escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}
/// Inclusive lower prefix for exactly one present scalar type. The next tag is
/// its exclusive upper prefix, allowing mixed typed property-bag range probes.
pub fn scalar_type_prefix(value: FieldType) -> [u8; 3] {
    [
        1,
        1,
        match value {
            FieldType::Integer => 0,
            FieldType::Float => 1,
            FieldType::Boolean => 2,
            FieldType::Text => 3,
            FieldType::Bytes => 4,
        },
    ]
}
/// Missing values precede present values. A queue declares each column's type.
/// Length prefixes are intentionally absent, preserving lexical tuple order.
pub fn ordered_key(values: &[Option<&FieldValue>]) -> QueueResult<Vec<u8>> {
    let mut out = vec![1]; // codec version, identical for all compared keys
    for value in values {
        let Some(value) = value else {
            out.push(0);
            continue;
        };
        out.push(1);
        match value {
            FieldValue::Integer(n) => {
                out.push(0);
                out.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes());
            }
            FieldValue::Float(n) => {
                if !n.is_finite() {
                    return Err(QueueError::Invalid("noncanonical index float".into()));
                }
                out.push(1);
                let bits = if *n == 0.0 {
                    0.0f64.to_bits()
                } else {
                    n.to_bits()
                };
                let ordered = if bits >> 63 == 1 {
                    !bits
                } else {
                    bits ^ (1 << 63)
                };
                out.extend_from_slice(&ordered.to_be_bytes());
            }
            FieldValue::Boolean(n) => {
                out.push(2);
                out.push(u8::from(*n));
            }
            FieldValue::Text(s) => {
                out.push(3);
                escaped(&mut out, s.as_bytes());
            }
            FieldValue::Bytes(b) => {
                out.push(4);
                escaped(&mut out, b);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::compare_optional_fields;
    #[test]
    fn scalar_keys_match_shared_order_for_all_edge_shapes() {
        let domains = vec![
            vec![
                FieldValue::Integer(i64::MIN),
                FieldValue::Integer(-1),
                FieldValue::Integer(0),
                FieldValue::Integer(i64::MAX),
            ],
            vec![
                FieldValue::Float(-100.0),
                FieldValue::Float(-0.1),
                FieldValue::Float(-0.0),
                FieldValue::Float(0.0),
                FieldValue::Float(0.1),
                FieldValue::Float(100.0),
            ],
            vec![FieldValue::Boolean(false), FieldValue::Boolean(true)],
            vec![
                FieldValue::Text("".into()),
                FieldValue::Text("\0".into()),
                FieldValue::Text("a".into()),
                FieldValue::Text("a\0".into()),
                FieldValue::Text("aa".into()),
                FieldValue::Text("z".into()),
                FieldValue::Text("é".into()),
                FieldValue::Text("🦊".into()),
            ],
            vec![
                FieldValue::Bytes(vec![]),
                FieldValue::Bytes(vec![0]),
                FieldValue::Bytes(vec![0, 0]),
                FieldValue::Bytes(vec![0, 255]),
                FieldValue::Bytes(vec![255]),
            ],
        ];
        for domain in domains {
            let values: Vec<_> = std::iter::once(None)
                .chain(domain.iter().map(Some))
                .collect();
            for a in &values {
                for b in &values {
                    assert_eq!(
                        ordered_key(&[*a])
                            .unwrap()
                            .cmp(&ordered_key(&[*b]).unwrap()),
                        compare_optional_fields(*a, *b).unwrap()
                    );
                }
            }
        }
    }
    #[test]
    fn variable_length_composites_are_not_length_prefixed() {
        let a = FieldValue::Text("z".into());
        let b = FieldValue::Text("aa".into());
        let second = FieldValue::Integer(1);
        assert!(
            ordered_key(&[Some(&a), Some(&second)]).unwrap()
                > ordered_key(&[Some(&b), Some(&second)]).unwrap()
        );
        let a = FieldValue::Text("a".into());
        let b = FieldValue::Text("a\0".into());
        assert!(
            ordered_key(&[Some(&a), Some(&second)]).unwrap()
                < ordered_key(&[Some(&b), None]).unwrap()
        );
    }
    #[test]
    fn oversized_strings_and_keys_are_preserved_exactly() {
        let a = FieldValue::Text("x".repeat(1025));
        let b = FieldValue::Text(format!("{}y", "x".repeat(1025)));
        let encoded = ordered_key(&[Some(&a)]).unwrap();
        assert!(encoded.len() > 1024);
        assert!(encoded < ordered_key(&[Some(&b)]).unwrap());
    }
    #[test]
    fn signed_zero_has_one_ordered_unique_key() {
        assert_eq!(
            ordered_key(&[Some(&FieldValue::Float(-0.0))]).unwrap(),
            ordered_key(&[Some(&FieldValue::Float(0.0))]).unwrap()
        );
    }
}
