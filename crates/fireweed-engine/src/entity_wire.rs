//! Wire encoding for typed entity documents (ADR-011).
//!
//! Human-readable (JSON log legacy): natural nested JSON `Value`.
//! Native binary (postcard / FWC1): the document is stored as an **opaque JSON
//! blob** (`serialize_bytes` of `serde_json::to_vec`). That matches the product
//! rule — payload and entity content are consumer-owned blobs; core queue
//! primitives stay native. Projection re-parses the blob when it needs typed
//! index keys (axon_esf native encodings), never a second outer JSON envelope.

use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Wrapper so `serialize_some` can emit raw bytes on the native path.
struct JsonBlob(Vec<u8>);

impl Serialize for JsonBlob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

/// `Option<serde_json::Value>` as natural JSON or a raw JSON blob.
pub mod option_entity {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<serde_json::Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            None => serializer.serialize_none(),
            Some(doc) => {
                if serializer.is_human_readable() {
                    serializer.serialize_some(doc)
                } else {
                    let bytes = serde_json::to_vec(doc).map_err(serde::ser::Error::custom)?;
                    serializer.serialize_some(&JsonBlob(bytes))
                }
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<serde_json::Value>, D::Error> {
        struct OptVisitor {
            human: bool,
        }

        impl<'de> Visitor<'de> for OptVisitor {
            type Value = Option<serde_json::Value>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("null, JSON value, or JSON document bytes")
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
                if self.human {
                    Ok(Some(serde_json::Value::deserialize(d)?))
                } else {
                    // Native: length-prefixed byte blob of JSON.
                    struct BytesVisitor;
                    impl<'de> Visitor<'de> for BytesVisitor {
                        type Value = Vec<u8>;
                        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                            f.write_str("JSON document bytes")
                        }
                        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                            Ok(v.to_vec())
                        }
                        fn visit_byte_buf<E: de::Error>(
                            self,
                            v: Vec<u8>,
                        ) -> Result<Self::Value, E> {
                            Ok(v)
                        }
                        fn visit_seq<A: de::SeqAccess<'de>>(
                            self,
                            mut seq: A,
                        ) -> Result<Self::Value, A::Error> {
                            // postcard sometimes delivers bytes as seq of u8 depending on path
                            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                            while let Some(b) = seq.next_element::<u8>()? {
                                out.push(b);
                            }
                            Ok(out)
                        }
                    }
                    let bytes = d.deserialize_byte_buf(BytesVisitor)?;
                    let doc = serde_json::from_slice(&bytes).map_err(de::Error::custom)?;
                    Ok(Some(doc))
                }
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                // Human-readable Option-less object form (legacy bare field).
                let value =
                    serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(Some(value))
            }
        }

        let human = deserializer.is_human_readable();
        if human {
            deserializer.deserialize_any(OptVisitor { human })
        } else {
            deserializer.deserialize_option(OptVisitor { human })
        }
    }
}
