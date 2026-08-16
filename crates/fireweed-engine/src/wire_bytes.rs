//! Compact wire encoding for opaque byte fields in durable command envelopes.
//!
//! Historical JSON encoding used serde's default for `Vec<u8>` / `Bytes`: a JSON
//! **array of decimal integers** (`[115,110,...]`). Measured expansion is ~4.28×
//! raw size and dominated durable log volume (fireweed-659490cc).
//!
//! **Current write format:** standard Base64 strings (~1.33× raw; ≤1.4× gate).
//!
//! **Read compatibility:** deserializers accept either:
//! 1. Base64 strings (current), or
//! 2. JSON sequences of `u8` (legacy integer-array encoding).
//!
//! Existing logs therefore still replay. New appends use only the Base64 form.
//! Binary formats that call `serialize_bytes` / `visit_bytes` keep working.

use std::collections::BTreeMap;
use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};

/// Encode raw bytes as a standard Base64 string for JSON (and as raw bytes for
/// binary serializers).
fn serialize_raw_bytes<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        serializer.serialize_str(&B64.encode(bytes))
    } else {
        serializer.serialize_bytes(bytes)
    }
}

/// Decode Base64 string **or** legacy JSON integer array into raw bytes.
///
/// Binary codecs (postcard) call `deserialize_bytes` — never `deserialize_any`
/// (postcard rejects `deserialize_any`).
fn deserialize_raw_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("raw bytes, base64 string, or integer byte array")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            B64.decode(v)
                .map_err(|e| E::custom(format!("invalid base64 byte field: {e}")))
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(b) = seq.next_element::<u8>()? {
                out.push(b);
            }
            Ok(out)
        }
    }

    if deserializer.is_human_readable() {
        deserializer.deserialize_any(BytesVisitor)
    } else {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

/// `Vec<u8>` field encoding.
pub mod vec_u8 {
    use super::*;

    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serialize_raw_bytes(value, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserialize_raw_bytes(deserializer)
    }
}

/// `bytes::Bytes` field encoding.
pub mod bytes_val {
    use super::*;

    pub fn serialize<S: Serializer>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_raw_bytes(value.as_ref(), serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
        deserialize_raw_bytes(deserializer).map(Bytes::from)
    }
}

/// Wrapper so binary `serialize_some` emits raw bytes (postcard Option discriminant).
struct BytesSer<'a>(&'a [u8]);
impl Serialize for BytesSer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_raw_bytes(self.0, serializer)
    }
}

/// `Option<Bytes>` field encoding (`None` → JSON null / absent via Option).
pub mod option_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<Bytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some(b) => {
                if serializer.is_human_readable() {
                    // JSON: field is null or a base64 string (no nested Option tag).
                    serialize_raw_bytes(b.as_ref(), serializer)
                } else {
                    serializer.serialize_some(&BytesSer(b.as_ref()))
                }
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Bytes>, D::Error> {
        struct OptVisitor;

        impl<'de> Visitor<'de> for OptVisitor {
            type Value = Option<Bytes>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("null, raw bytes, base64 string, or integer byte array")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
                deserialize_raw_bytes(d).map(|v| Some(Bytes::from(v)))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                deserialize_raw_bytes(de::value::StrDeserializer::new(v))
                    .map(|b| Some(Bytes::from(b)))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                self.visit_str(&v)
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(Some(Bytes::copy_from_slice(v)))
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(Some(Bytes::from(v)))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
                // Legacy: Option was encoded as the array itself when Some.
                deserialize_raw_bytes(de::value::SeqAccessDeserializer::new(seq))
                    .map(|b| Some(Bytes::from(b)))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(OptVisitor)
        } else {
            deserializer.deserialize_option(OptVisitor)
        }
    }
}

/// `BTreeMap<String, Bytes>` — each value is Base64 (or legacy int array).
pub mod btreemap_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &BTreeMap<String, Bytes>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let human = serializer.is_human_readable();
        let mut map = serializer.serialize_map(Some(value.len()))?;
        for (k, v) in value {
            if human {
                map.serialize_entry(k, &B64.encode(v.as_ref()))?;
            } else {
                // Native binary: raw bytes. Base64 here would round-trip as the
                // encoded TEXT (postcard str/bytes framing is identical), silently
                // corrupting every field value.
                map.serialize_entry(k, &BytesSer(v.as_ref()))?;
            }
        }
        map.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, Bytes>, D::Error> {
        struct MapVisitor;

        impl<'de> Visitor<'de> for MapVisitor {
            type Value = BTreeMap<String, Bytes>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("map of string to base64 or integer byte arrays")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut out = BTreeMap::new();
                while let Some(key) = access.next_key::<String>()? {
                    let val = access.next_value_seed(BytesSeed)?;
                    out.insert(key, val);
                }
                Ok(out)
            }
        }

        deserializer.deserialize_map(MapVisitor)
    }

    struct BytesSeed;

    impl<'de> de::DeserializeSeed<'de> for BytesSeed {
        type Value = Bytes;

        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_raw_bytes(d).map(Bytes::from)
        }
    }
}

/// `BTreeMap<String, Option<Bytes>>` for FAC-1 field_ops (`None` removes the key).
pub mod btreemap_option_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &BTreeMap<String, Option<Bytes>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let human = serializer.is_human_readable();
        let mut map = serializer.serialize_map(Some(value.len()))?;
        for (k, v) in value {
            match (human, v) {
                (true, None) => map.serialize_entry(k, &serde_json::Value::Null)?,
                (true, Some(b)) => map.serialize_entry(k, &B64.encode(b.as_ref()))?,
                // Native binary: real Option + raw bytes (see btreemap_bytes).
                (false, None) => map.serialize_entry(k, &None::<BytesSer>)?,
                (false, Some(b)) => map.serialize_entry(k, &Some(BytesSer(b.as_ref())))?,
            }
        }
        map.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, Option<Bytes>>, D::Error> {
        struct MapVisitor;

        impl<'de> Visitor<'de> for MapVisitor {
            type Value = BTreeMap<String, Option<Bytes>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("map of string to null | base64 | integer byte array")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut out = BTreeMap::new();
                while let Some(key) = access.next_key::<String>()? {
                    let val = access.next_value_seed(OptBytesSeed)?;
                    out.insert(key, val);
                }
                Ok(out)
            }
        }

        deserializer.deserialize_map(MapVisitor)
    }

    struct OptBytesSeed;

    impl<'de> de::DeserializeSeed<'de> for OptBytesSeed {
        type Value = Option<Bytes>;

        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            option_bytes::deserialize(d)
        }
    }
}

/// `Option<BTreeMap<String, Bytes>>` (e.g. UpdateFieldsCommand::set_fields).
pub mod option_btreemap_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<BTreeMap<String, Bytes>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some(map) => {
                if serializer.is_human_readable() {
                    btreemap_bytes::serialize(map, serializer)
                } else {
                    // postcard needs the Option discriminant before the map body.
                    struct MapSer<'a>(&'a BTreeMap<String, Bytes>);
                    impl Serialize for MapSer<'_> {
                        fn serialize<S2: Serializer>(&self, s: S2) -> Result<S2::Ok, S2::Error> {
                            btreemap_bytes::serialize(self.0, s)
                        }
                    }
                    serializer.serialize_some(&MapSer(map))
                }
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<BTreeMap<String, Bytes>>, D::Error> {
        struct OptVisitor;

        impl<'de> Visitor<'de> for OptVisitor {
            type Value = Option<BTreeMap<String, Bytes>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("null or map of string to base64/int-array bytes")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
                btreemap_bytes::deserialize(d).map(Some)
            }

            fn visit_map<A: MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
                // Present map without Option wrapper (legacy).
                btreemap_bytes::deserialize(de::value::MapAccessDeserializer::new(access)).map(Some)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(OptVisitor)
        } else {
            deserializer.deserialize_option(OptVisitor)
        }
    }
}

/// `Vec<Vec<u8>>` (e.g. side_record_keys).
pub mod vec_vec_u8 {
    use super::*;

    pub fn serialize<S: Serializer>(value: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let human = serializer.is_human_readable();
        let mut seq = serializer.serialize_seq(Some(value.len()))?;
        for v in value {
            if human {
                seq.serialize_element(&B64.encode(v))?;
            } else {
                // Native binary: raw bytes (see btreemap_bytes).
                seq.serialize_element(&BytesSer(v))?;
            }
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        struct Outer;

        impl<'de> Visitor<'de> for Outer {
            type Value = Vec<Vec<u8>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("array of base64 strings or integer byte arrays")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(item) = seq.next_element_seed(OneVecSeed)? {
                    out.push(item);
                }
                Ok(out)
            }
        }

        deserializer.deserialize_seq(Outer)
    }

    struct OneVecSeed;

    impl<'de> de::DeserializeSeed<'de> for OneVecSeed {
        type Value = Vec<u8>;

        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            deserialize_raw_bytes(d)
        }
    }
}

/// `Option<(Vec<u8>, u64)>` for instance fence pairs.
pub mod option_instance {
    use super::*;

    #[derive(Deserialize)]
    struct WireOwned {
        #[serde(deserialize_with = "deserialize_raw_bytes")]
        key: Vec<u8>,
        fence: u64,
    }

    /// Legacy tuple form: `[key_bytes_or_b64, fence]` or object form.
    pub fn serialize<S: Serializer>(
        value: &Option<(Vec<u8>, u64)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some((key, fence)) => {
                if serializer.is_human_readable() {
                    // Preserve historical tuple shape `[key, fence]` for structure,
                    // with key as base64 string.
                    use serde::ser::SerializeTuple;
                    let mut t = serializer.serialize_tuple(2)?;
                    t.serialize_element(&B64.encode(key))?;
                    t.serialize_element(fence)?;
                    t.end()
                } else {
                    struct PairSer<'a>(&'a [u8], u64);
                    impl Serialize for PairSer<'_> {
                        fn serialize<S2: Serializer>(&self, s: S2) -> Result<S2::Ok, S2::Error> {
                            use serde::ser::SerializeTuple;
                            let mut t = s.serialize_tuple(2)?;
                            t.serialize_element(&BytesSer(self.0))?;
                            t.serialize_element(&self.1)?;
                            t.end()
                        }
                    }
                    serializer.serialize_some(&PairSer(key.as_slice(), *fence))
                }
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<(Vec<u8>, u64)>, D::Error> {
        struct V {
            human: bool,
        }

        impl<'de> Visitor<'de> for V {
            type Value = Option<(Vec<u8>, u64)>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("null, [key, fence] tuple, or {key, fence} object")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
                #[derive(Deserialize)]
                struct WireTuple(
                    #[serde(deserialize_with = "deserialize_raw_bytes")] Vec<u8>,
                    u64,
                );
                if !self.human {
                    // Native binary is always the tuple layout; untagged fallback would
                    // buffer through `deserialize_any`, which postcard rejects.
                    let WireTuple(k, f) = WireTuple::deserialize(d)?;
                    return Ok(Some((k, f)));
                }
                // Prefer tuple; fall back to object.
                #[derive(Deserialize)]
                #[serde(untagged)]
                enum Forms {
                    Tuple(WireTuple),
                    Object(WireOwned),
                }
                match Forms::deserialize(d)? {
                    Forms::Tuple(WireTuple(k, f)) => Ok(Some((k, f))),
                    Forms::Object(WireOwned { key, fence }) => Ok(Some((key, fence))),
                }
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let key = seq
                    .next_element_seed(KeySeed)?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let fence = seq
                    .next_element::<u64>()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                // Consume optional trailing nothing.
                Ok(Some((key, fence)))
            }
        }

        struct KeySeed;
        impl<'de> de::DeserializeSeed<'de> for KeySeed {
            type Value = Vec<u8>;
            fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
                deserialize_raw_bytes(d)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(V { human: true })
        } else {
            deserializer.deserialize_option(V { human: false })
        }
    }
}

/// Maximum allowed encoded_size / raw_size for a pure byte field under the new format.
/// Applies to payloads large enough that Base64 padding is amortized (see
/// [`MIN_EXPANSION_GATE_RAW_LEN`]). Short keys (a few bytes) pay fixed padding
/// overhead; the log-volume win is dominated by multi-KB side-record payloads.
pub const MAX_ENCODED_EXPANSION: f64 = 1.4;

/// Raw lengths at or above this size must stay ≤ [`MAX_ENCODED_EXPANSION`].
pub const MIN_EXPANSION_GATE_RAW_LEN: usize = 24;

/// Expansion of Base64 encoding relative to raw byte length (no JSON quotes).
pub fn base64_expansion(raw_len: usize) -> f64 {
    if raw_len == 0 {
        return 1.0;
    }
    let encoded = B64.encode(vec![0u8; raw_len]).len();
    encoded as f64 / raw_len as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Sample {
        #[serde(with = "vec_u8")]
        key: Vec<u8>,
        #[serde(with = "bytes_val")]
        payload: Bytes,
        #[serde(with = "option_bytes")]
        optional: Option<Bytes>,
    }

    #[test]
    fn new_encoding_is_base64_and_round_trips() {
        let s = Sample {
            key: b"state/run-1".to_vec(),
            payload: Bytes::from_static(b"opaque-state-payload-bytes"),
            optional: Some(Bytes::from_static(b"x")),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            json.contains(r#""key":"c3RhdGUvcnVuLTE=""#),
            "key should be base64, got {json}"
        );
        assert!(
            !json.contains("[115,116,97"),
            "must not emit integer arrays: {json}"
        );
        let back: Sample = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn legacy_integer_array_still_deserializes() {
        let legacy = r#"{
            "key": [115,116,97,116,101,47,114,117,110,45,49],
            "payload": [111,112,97,113,117,101],
            "optional": [122]
        }"#;
        let s: Sample = serde_json::from_str(legacy).unwrap();
        assert_eq!(s.key, b"state/run-1");
        assert_eq!(&s.payload[..], b"opaque");
        assert_eq!(s.optional.as_deref(), Some(&b"z"[..]));
    }

    #[test]
    fn expansion_stays_under_gate() {
        for raw in [MIN_EXPANSION_GATE_RAW_LEN, 32, 48, 64, 256, 3149, 65536] {
            let exp = base64_expansion(raw);
            assert!(
                exp <= MAX_ENCODED_EXPANSION,
                "raw={raw} expansion={exp} exceeds {MAX_ENCODED_EXPANSION}"
            );
        }
        // Tiny values still encode; padding dominates but absolute waste is bytes not MB.
        assert!(base64_expansion(1) >= 1.0);
    }

    #[test]
    fn side_record_envelope_field_expansion_gate() {
        // Mirror snorri's 3,149-byte side-record payload example from the bead.
        let raw = vec![0xABu8; 3149];
        let encoded = B64.encode(&raw);
        let expansion = encoded.len() as f64 / raw.len() as f64;
        assert!(
            expansion <= MAX_ENCODED_EXPANSION,
            "3149-byte payload expands {expansion}x"
        );
        // JSON string adds two quotes; still well under the old 4.28x integer array.
        let json_len = encoded.len() + 2;
        let json_expansion = json_len as f64 / raw.len() as f64;
        assert!(
            json_expansion < 1.4,
            "json-quoted base64 expands {json_expansion}x"
        );
    }
}
