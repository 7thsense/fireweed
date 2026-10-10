//! Exact JSON equality bytes for indexed consumer attributes. This is not a
//! JSON range-ordering codec; storage treats the result as an opaque Bytes value.
//! Object order is insignificant, array order is significant, and numeric
//! representation follows the actual serde_json::Value equality model.
use crate::queue_contract::{QueueError, QueueResult};
use serde_json::{Number, Value};
use std::fmt::Write as _;

pub const JSON_EQUALITY_MAGIC: &[u8; 4] = b"FWJ1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JsonNumberEquality {
    NumericVariants,
    LiteralRepresentations,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JsonEqualityEncoding {
    pub version: u8,
    pub numbers: JsonNumberEquality,
}
/// Pin this complete identity in the consuming namespace/schema. Check it before
/// indexed reads as well as writes; per-value headers alone cannot detect drift.
pub fn current_json_equality_encoding() -> JsonEqualityEncoding {
    JsonEqualityEncoding {
        version: 1,
        numbers: if Number::from_f64(0.0).unwrap() == Number::from_f64(-0.0).unwrap() {
            JsonNumberEquality::NumericVariants
        } else {
            JsonNumberEquality::LiteralRepresentations
        },
    }
}

/// RFC6901 escapes a literal top-level key. Namespace and key stay distinct;
/// neither JSON pointer parsing nor Unicode normalization changes their identity.
pub fn json_attribute_name(namespace: &str, key: &str) -> String {
    format!("{namespace}/{}", key.replace('~', "~0").replace('/', "~1"))
}
struct Writer {
    bytes: Vec<u8>,
    limit: u64,
    pending: u64,
}
impl Writer {
    fn append(&mut self, bytes: &[u8]) -> QueueResult<()> {
        if (self.bytes.len() as u64)
            .checked_add(bytes.len() as u64)
            .and_then(|n| n.checked_add(self.pending))
            .is_none_or(|n| n > self.limit)
        {
            return Err(QueueError::TooLarge);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn reserve_minimum(&mut self, bytes: u64) -> QueueResult<()> {
        let pending = self
            .pending
            .checked_add(bytes)
            .ok_or(QueueError::TooLarge)?;
        if (self.bytes.len() as u64)
            .checked_add(pending)
            .is_none_or(|n| n > self.limit)
        {
            return Err(QueueError::TooLarge);
        }
        self.pending = pending;
        Ok(())
    }
    fn consume_minimum(&mut self, bytes: u64) {
        self.pending = self
            .pending
            .checked_sub(bytes)
            .expect("equality traversal reservation");
    }
    fn object_key(&mut self, key: &str) -> QueueResult<()> {
        self.consume_minimum(key_minimum(key)?);
        self.string(key)
    }
    fn literal_number(&mut self, number: &Number) -> QueueResult<()> {
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        write!(&mut count, "{number}").map_err(|_| QueueError::TooLarge)?;
        self.length(count.0)?;
        write!(self, "{number}").map_err(|_| QueueError::TooLarge)
    }
    fn tag(&mut self, tag: u8) -> QueueResult<()> {
        self.append(&[tag])
    }
    fn length(&mut self, mut length: usize) -> QueueResult<()> {
        loop {
            let byte = (length & 127) as u8;
            length >>= 7;
            self.tag(byte | if length == 0 { 0 } else { 128 })?;
            if length == 0 {
                return Ok(());
            }
        }
    }
    fn string(&mut self, value: &str) -> QueueResult<()> {
        self.length(value.len())?;
        self.append(value.as_bytes())
    }
}
impl std::fmt::Write for Writer {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.append(text.as_bytes()).map_err(|_| std::fmt::Error)
    }
}
fn key_minimum(key: &str) -> QueueResult<u64> {
    let mut length = key.len();
    let mut prefix = 1u64;
    while length >= 128 {
        length >>= 7;
        prefix += 1;
    }
    (key.len() as u64)
        .checked_add(prefix)
        .ok_or(QueueError::TooLarge)
}
enum Frame<'a> {
    Value(&'a Value),
    Array {
        values: &'a [Value],
        next: usize,
    },
    Object {
        entries: Vec<(&'a str, &'a Value)>,
        next: usize,
    },
}
/// Byte-limited, iterative encoding. Only references to object members are
/// sorted; no JSON tree/body/string is cloned. The limit bounds encoded output;
/// scratch is O(max_bytes) with a container/reference multiplier, not capped to
/// max_bytes allocator bytes. Traversal does not recurse on the call stack.
/// The consumer calls this before submitting a Bytes attribute; it is not a
/// backend interpretation of an opaque queue body.
pub fn encode_json_equality(value: &Value, max_bytes: u64) -> QueueResult<Vec<u8>> {
    encode_json_equality_for(value, current_json_equality_encoding(), max_bytes)
}
/// Namespace consumers use their persisted encoding pin. A drift rejection
/// requires explicit rebuild/migration; it must not become a fresh equality key.
pub fn encode_json_equality_for(
    value: &Value,
    encoding: JsonEqualityEncoding,
    max_bytes: u64,
) -> QueueResult<Vec<u8>> {
    if encoding != current_json_equality_encoding() {
        return Err(QueueError::Invalid(
            "JSON equality encoding drift; explicit migration required".into(),
        ));
    }
    let literal_numbers = encoding.numbers == JsonNumberEquality::LiteralRepresentations;
    let mut writer = Writer {
        bytes: Vec::new(),
        limit: max_bytes,
        pending: 0,
    };
    writer.append(JSON_EQUALITY_MAGIC)?;
    writer.tag(u8::from(literal_numbers))?;
    writer.reserve_minimum(1)?;
    let mut stack = vec![Frame::Value(value)];
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Value(value) => {
                writer.consume_minimum(1);
                match value {
                    Value::Null => writer.tag(0)?,
                    Value::Bool(value) => {
                        writer.tag(1)?;
                        writer.tag(u8::from(*value))?;
                    }
                    Value::Number(number) => {
                        if literal_numbers {
                            writer.tag(8)?;
                            writer.literal_number(number)?;
                        } else if number.is_u64() {
                            writer.tag(2)?;
                            writer.append(&number.as_u64().unwrap().to_be_bytes())?;
                        } else if number.is_i64() {
                            writer.tag(3)?;
                            writer.append(&number.as_i64().unwrap().to_be_bytes())?;
                        } else {
                            let value = number.as_f64().ok_or_else(|| {
                                QueueError::Invalid("unsupported JSON number equality model".into())
                            })?;
                            if !value.is_finite() {
                                return Err(QueueError::Invalid("nonfinite JSON number".into()));
                            }
                            writer.tag(4)?;
                            writer.append(
                                &(if value == 0.0 { 0 } else { value.to_bits() }).to_be_bytes(),
                            )?;
                        }
                    }
                    Value::String(value) => {
                        writer.tag(5)?;
                        writer.string(value)?;
                    }
                    Value::Array(values) => {
                        writer.tag(6)?;
                        writer.length(values.len())?;
                        // Reservations include all unvisited siblings/ancestors,
                        // preventing nested wide containers multiplying scratch.
                        writer.reserve_minimum(values.len() as u64)?;
                        if let Some(first) = values.first() {
                            stack.push(Frame::Array { values, next: 1 });
                            stack.push(Frame::Value(first));
                        }
                    }
                    Value::Object(values) => {
                        writer.tag(7)?;
                        writer.length(values.len())?;
                        let minimum = values.keys().try_fold(0u64, |n, key| {
                            n.checked_add(key_minimum(key)?)
                                .and_then(|n| n.checked_add(1))
                                .ok_or(QueueError::TooLarge)
                        })?;
                        writer.reserve_minimum(minimum)?;
                        let mut entries: Vec<_> = values
                            .iter()
                            .map(|(key, value)| (key.as_str(), value))
                            .collect();
                        entries.sort_unstable_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
                        if let Some((key, value)) = entries.first().copied() {
                            writer.object_key(key)?;
                            stack.push(Frame::Object { entries, next: 1 });
                            stack.push(Frame::Value(value));
                        }
                    }
                }
            }
            Frame::Array { values, next } => {
                if let Some(value) = values.get(next) {
                    stack.push(Frame::Array {
                        values,
                        next: next + 1,
                    });
                    stack.push(Frame::Value(value));
                }
            }
            Frame::Object { entries, next } => {
                if let Some((key, value)) = entries.get(next).copied() {
                    writer.object_key(key)?;
                    stack.push(Frame::Object {
                        entries,
                        next: next + 1,
                    });
                    stack.push(Frame::Value(value));
                }
            }
        }
    }
    debug_assert_eq!(writer.pending, 0);
    Ok(writer.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equality_matches_json_values_across_numeric_and_nested_cases() {
        let mut left = serde_json::Map::new();
        left.insert("z".into(), serde_json::json!([null,1,{"nested":true}]));
        left.insert("a".into(), serde_json::json!("\0λ/~"));
        let mut right = serde_json::Map::new();
        right.insert("a".into(), serde_json::json!("\0λ/~"));
        right.insert("z".into(), serde_json::json!([null,1,{"nested":true}]));
        let mut values = vec![
            Value::Null,
            serde_json::json!(false),
            serde_json::json!(true),
            serde_json::json!("null"),
            serde_json::json!(""),
            serde_json::json!(0),
            serde_json::json!(0u64),
            serde_json::json!(0.0),
            serde_json::json!(-0.0),
            serde_json::json!(1i64),
            serde_json::json!(1u64),
            serde_json::json!(1.0),
            serde_json::json!(i64::MIN),
            serde_json::json!(i64::MAX),
            serde_json::json!(u64::MAX),
            serde_json::json!(f64::MIN),
            serde_json::json!(f64::MAX),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!([1, 2]),
            serde_json::json!([2, 1]),
            serde_json::json!({"present":null}),
            Value::Object(left),
            Value::Object(right),
        ];
        for text in [
            "-0",
            "0",
            "0.0",
            "-0.0",
            "1",
            "1.0",
            "1.00",
            "1e0",
            "18446744073709551615",
            "18446744073709551616",
            "-9223372036854775808",
            "9007199254740993",
            "9007199254740992.0",
        ] {
            values.push(serde_json::from_str(text).unwrap());
        }
        for a in &values {
            for b in &values {
                assert_eq!(
                    encode_json_equality(a, 1 << 20).unwrap()
                        == encode_json_equality(b, 1 << 20).unwrap(),
                    a == b,
                    "{a:?} versus {b:?}"
                );
            }
        }
    }
    #[test]
    fn golden_wire_and_exact_byte_limit() {
        let value = serde_json::json!({"a":null,"b":[true,"x"]});
        let bytes = encode_json_equality(&value, 1024).unwrap();
        let model_tag = match current_json_equality_encoding().numbers {
            JsonNumberEquality::NumericVariants => 0,
            JsonNumberEquality::LiteralRepresentations => 1,
        };
        assert_eq!(&bytes[..5], &[b'F', b'W', b'J', b'1', model_tag]);
        let mut numeric = vec![b'F', b'W', b'J', b'1', model_tag];
        if model_tag == 0 {
            numeric.push(4);
            numeric.extend_from_slice(&1.0f64.to_bits().to_be_bytes());
        } else {
            numeric.extend_from_slice(&[8, 3, b'1', b'.', b'0']);
        }
        assert_eq!(
            encode_json_equality(&serde_json::json!(1.0), 1024).unwrap(),
            numeric
        );
        assert_eq!(
            &bytes[5..],
            &[7, 2, 1, b'a', 0, 1, b'b', 6, 2, 1, 1, 5, 1, b'x']
        );
        assert_eq!(
            encode_json_equality(&value, bytes.len() as u64).unwrap(),
            bytes
        );
        assert_eq!(
            encode_json_equality(&value, bytes.len() as u64 - 1),
            Err(QueueError::TooLarge)
        );
        assert_eq!(
            encode_json_equality(&Value::Null, 0),
            Err(QueueError::TooLarge)
        );
    }
    #[test]
    fn literal_key_namespace_identity_is_not_pointer_interpretation() {
        assert_eq!(
            json_attribute_name("/payload", "route/~tier"),
            "/payload/route~1~0tier"
        );
        assert_ne!(
            json_attribute_name("/payload", "a/b"),
            json_attribute_name("/payload/a", "b")
        );
        assert_ne!(
            json_attribute_name("/payload", "~1"),
            json_attribute_name("/payload", "/")
        );
        assert_eq!(json_attribute_name("/payload", ""), "/payload/");
        assert_eq!(json_attribute_name("/payload", "\0λ"), "/payload/\0λ");
    }
    #[test]
    fn deep_values_use_iterative_traversal_without_tree_cloning() {
        let mut value = Value::Null;
        for _ in 0..2048 {
            value = Value::Array(vec![value]);
        }
        assert_eq!(
            encode_json_equality(&value, 1 << 20).unwrap().len(),
            5 + 4096 + 1
        );
        // Drop the caller's deliberately deep tree iteratively too.
        while let Value::Array(mut entries) = value {
            value = entries.pop().unwrap();
        }
    }
    #[test]
    fn namespace_pin_rejects_encoding_and_numeric_model_drift() {
        let pin = current_json_equality_encoding();
        let value = serde_json::json!(1.0);
        assert_eq!(
            encode_json_equality_for(&value, pin, 1024).unwrap(),
            encode_json_equality(&value, 1024).unwrap()
        );
        let mut wrong = pin;
        wrong.version += 1;
        assert!(matches!(
            encode_json_equality_for(&value, wrong, 1024),
            Err(QueueError::Invalid(_))
        ));
        wrong = pin;
        wrong.numbers = match pin.numbers {
            JsonNumberEquality::NumericVariants => JsonNumberEquality::LiteralRepresentations,
            JsonNumberEquality::LiteralRepresentations => JsonNumberEquality::NumericVariants,
        };
        assert!(matches!(
            encode_json_equality_for(&value, wrong, 1024),
            Err(QueueError::Invalid(_))
        ));
    }
    #[test]
    fn nested_wide_objects_reserve_all_unvisited_siblings() {
        let mut value = serde_json::json!([null, true, "leaf"]);
        for _ in 0..8 {
            let mut entries = serde_json::Map::new();
            entries.insert("a-nested".into(), value);
            for n in 0..64 {
                entries.insert(format!("z-sibling-{n:02}"), Value::Null);
            }
            value = Value::Object(entries);
        }
        let encoded = encode_json_equality(&value, 1 << 20).unwrap();
        assert_eq!(
            encode_json_equality(&value, encoded.len() as u64).unwrap(),
            encoded
        );
        assert_eq!(
            encode_json_equality(&value, encoded.len() as u64 - 1),
            Err(QueueError::TooLarge)
        );
        assert_eq!(encode_json_equality(&value, 32), Err(QueueError::TooLarge));
    }
}
