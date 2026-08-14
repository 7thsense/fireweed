//! Client-controllable typed index values (ADR-011) as **native** durable fields.
//!
//! # Model
//!
//! | Surface | Who controls | Representation |
//! |---|---|---|
//! | `create_queue.typed_indexes` | client (queue schema) | field path + [`IndexType`] |
//! | `push.index_fields` | client (per item) | [`TypedValue`] map, no JSON |
//! | `push.payload` | client | opaque byte blob |
//!
//! The durable log stores `index_fields` as postcard-native `TypedValue`s
//! (`String` / `Integer` / `Float` / `Bool` / `DateTime`). Unique SQL keys are
//! framed from those values directly ([`typed_index_key`]) — never by building
//! a JSON entity and asking axon to re-parse it.
//!
//! Admission may still accept a JSON entity for ergonomics; call
//! [`materialize_index_fields`] **once** at the admission boundary, then only
//! the native map is durable.

use std::collections::BTreeMap;

use fireweed_core::{
    IndexDeclaration, IndexType, QueueDefinition, QueueIndex, TypedValue, UtcTimestamp,
};
use serde_json::Value;

use crate::error::{EngineError, EngineResult};

/// Collect every field path declared by the queue's typed indexes (order not significant).
pub fn declared_index_field_paths(indexes: &[QueueIndex]) -> Vec<(String, IndexType)> {
    let mut out = Vec::new();
    for qi in indexes {
        match &qi.declaration {
            IndexDeclaration::Single(def) => {
                out.push((def.field.clone(), def.index_type.clone()));
            }
            IndexDeclaration::Compound(def) => {
                for f in &def.fields {
                    out.push((f.field.clone(), f.index_type.clone()));
                }
            }
        }
    }
    out
}

/// Coerce a JSON scalar (from an admission-time entity) into a native [`TypedValue`].
pub fn json_to_typed_value(value: &Value, index_type: &IndexType) -> EngineResult<TypedValue> {
    match index_type {
        IndexType::String => match value {
            Value::String(s) => Ok(TypedValue::String(s.clone())),
            _ => Err(EngineError::Invalid(
                "typed index field requires string value",
            )),
        },
        IndexType::Integer => match value {
            Value::Number(n) => n
                .as_i64()
                .map(TypedValue::Integer)
                .ok_or(EngineError::Invalid(
                    "typed index field requires integer value",
                )),
            _ => Err(EngineError::Invalid(
                "typed index field requires integer value",
            )),
        },
        IndexType::Float => match value {
            Value::Number(n) => n
                .as_f64()
                .map(TypedValue::Float)
                .ok_or(EngineError::Invalid(
                    "typed index field requires float value",
                )),
            _ => Err(EngineError::Invalid(
                "typed index field requires float value",
            )),
        },
        IndexType::Boolean => match value {
            Value::Bool(b) => Ok(TypedValue::Bool(*b)),
            _ => Err(EngineError::Invalid(
                "typed index field requires boolean value",
            )),
        },
        IndexType::Datetime => match value {
            Value::String(s) => {
                // ISO-ish or decimal seconds — keep admission permissive; axon encode is strict.
                if let Ok(secs) = s.parse::<i64>() {
                    return Ok(TypedValue::DateTime(UtcTimestamp::new(secs, 0).map_err(
                        |_| EngineError::Invalid("typed index datetime out of range"),
                    )?));
                }
                let nanos = axon_esf::coerce_datetime_nanos(value).map_err(|_| {
                    EngineError::Invalid(
                        "typed index datetime string must be RFC 3339 or epoch seconds",
                    )
                })?;
                Ok(TypedValue::DateTime(
                    UtcTimestamp::new(
                        nanos.div_euclid(1_000_000_000),
                        u32::try_from(nanos.rem_euclid(1_000_000_000)).expect("rem < 1e9"),
                    )
                    .map_err(|_| EngineError::Invalid("typed index datetime out of range"))?,
                ))
            }
            Value::Number(n) => {
                let secs = n.as_i64().ok_or(EngineError::Invalid(
                    "typed index datetime requires integer seconds",
                ))?;
                Ok(TypedValue::DateTime(UtcTimestamp::new(secs, 0).map_err(
                    |_| EngineError::Invalid("typed index datetime out of range"),
                )?))
            }
            _ => Err(EngineError::Invalid(
                "typed index field requires datetime value",
            )),
        },
    }
}

/// Walk a dotted path on a JSON object (`a.b.c`).
fn extract_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = record;
    for part in path.split('.') {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

/// Pull native index field values out of an admission-time entity document, constrained
/// to the queue's declared typed indexes. Sparse fields (missing/null) are omitted.
pub fn extract_index_fields_from_entity(
    indexes: &[QueueIndex],
    entity: &Value,
) -> EngineResult<BTreeMap<String, TypedValue>> {
    let mut out = BTreeMap::new();
    for (path, index_type) in declared_index_field_paths(indexes) {
        if out.contains_key(&path) {
            continue; // same path may appear in multiple indexes
        }
        let Some(raw) = extract_path(entity, &path) else {
            continue;
        };
        if raw.is_null() {
            continue;
        }
        out.insert(path, json_to_typed_value(raw, &index_type)?);
    }
    Ok(out)
}

/// Admission materialization: prefer explicit native `index_fields`; else project from entity.
///
/// Call once at the push admission boundary (where the queue definition is known). The durable
/// command carries only the returned map (+ payload blob); it does not need the entity for indexes.
pub fn materialize_index_fields(
    definition: &QueueDefinition,
    explicit: BTreeMap<String, TypedValue>,
    entity: Option<&Value>,
) -> EngineResult<BTreeMap<String, TypedValue>> {
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    if definition.typed_indexes.is_empty() {
        return Ok(BTreeMap::new());
    }
    match entity {
        Some(doc) => extract_index_fields_from_entity(&definition.typed_indexes, doc),
        None => Ok(BTreeMap::new()),
    }
}

/// Order-preserving bytes for one native [`TypedValue`], matching axon_esf's unframed
/// `encode_index_value` (sign-flipped i64, total-order f64, UTF-8, bool 0/1, datetime nanos).
pub fn encode_typed_value(value: &TypedValue, expected: &IndexType) -> EngineResult<Vec<u8>> {
    match (expected, value) {
        (IndexType::String, TypedValue::String(s)) => Ok(s.as_bytes().to_vec()),
        (IndexType::Integer, TypedValue::Integer(v)) => Ok(encode_i64(*v)),
        (IndexType::Float, TypedValue::Float(v)) => Ok(encode_f64(*v)),
        (IndexType::Boolean, TypedValue::Bool(b)) => Ok(vec![u8::from(*b)]),
        (IndexType::Datetime, TypedValue::DateTime(ts)) => {
            let nanos = ts
                .seconds
                .checked_mul(1_000_000_000)
                .and_then(|n| n.checked_add(i64::from(ts.nanoseconds)))
                .ok_or(EngineError::Invalid("typed index datetime out of range"))?;
            Ok(encode_i64(nanos))
        }
        _ => Err(EngineError::Invalid(
            "typed index field value does not match declared IndexType",
        )),
    }
}

fn encode_i64(v: i64) -> Vec<u8> {
    let ordered = (v as u64) ^ (1u64 << 63);
    ordered.to_be_bytes().to_vec()
}

fn encode_f64(v: f64) -> Vec<u8> {
    let bits = v.to_bits();
    let ordered = if bits & (1u64 << 63) == 0 {
        bits | (1u64 << 63)
    } else {
        !bits
    };
    ordered.to_be_bytes().to_vec()
}

fn frame_value(encoded: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(encoded.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(encoded);
}

/// Framed axon-compatible bytes for one [`TypedValue`] (`u32` BE length + [`encode_typed_value`]).
pub fn framed_typed_value(value: &TypedValue, expected: &IndexType) -> EngineResult<Vec<u8>> {
    let encoded = encode_typed_value(value, expected)?;
    let mut out = Vec::with_capacity(4 + encoded.len());
    frame_value(&encoded, &mut out);
    Ok(out)
}

/// Framed axon-compatible key for one declared index from a native field map.
///
/// `None` when any required field is missing (sparse). Bytes match
/// `IndexDef::index_key` / `CompoundIndexDef::index_key` on the equivalent JSON
/// record so existing unique SQL rows stay valid.
pub fn typed_index_key(
    declaration: &IndexDeclaration,
    fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<Option<Vec<u8>>> {
    match declaration {
        IndexDeclaration::Single(def) => {
            let Some(value) = fields.get(&def.field) else {
                return Ok(None);
            };
            Ok(Some(framed_typed_value(value, &def.index_type)?))
        }
        IndexDeclaration::Compound(def) => {
            let mut out = Vec::new();
            for field in &def.fields {
                let Some(value) = fields.get(&field.field) else {
                    return Ok(None);
                };
                out.extend(framed_typed_value(value, &field.index_type)?);
            }
            Ok(Some(out))
        }
    }
}

/// `(index_name, framed_key)` for every declared index that is fully present in `fields`.
pub fn typed_index_keys(
    indexes: &[QueueIndex],
    fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::with_capacity(indexes.len());
    for qi in indexes {
        if let Some(key) = typed_index_key(&qi.declaration, fields)? {
            out.push((qi.name.clone(), key));
        }
    }
    Ok(out)
}

/// Apply/insert keys: native `index_fields` first, else extract from an admission entity
/// and encode natively. Never calls axon `index_key` on a JSON record.
pub fn typed_index_keys_for_item(
    indexes: &[QueueIndex],
    index_fields: &BTreeMap<String, TypedValue>,
    entity: Option<&Value>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    if indexes.is_empty() {
        return Ok(Vec::new());
    }
    if !index_fields.is_empty() {
        return typed_index_keys(indexes, index_fields);
    }
    let Some(doc) = entity else {
        return Ok(Vec::new());
    };
    let extracted = extract_index_fields_from_entity(indexes, doc)?;
    typed_index_keys(indexes, &extracted)
}

/// Decode a claim-by-query lookup slice (UTF-8 string / JSON number / JSON bool)
/// into a native [`TypedValue`].
pub fn lookup_bytes_to_typed(index_type: &IndexType, bytes: &[u8]) -> EngineResult<TypedValue> {
    match index_type {
        IndexType::String => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(TypedValue::String(s.to_owned()))
        }
        IndexType::Integer => {
            let n: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number"))?;
            json_to_typed_value(&n, index_type)
        }
        IndexType::Float => {
            let n: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number"))?;
            json_to_typed_value(&n, index_type)
        }
        IndexType::Boolean => {
            let n: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON boolean"))?;
            json_to_typed_value(&n, index_type)
        }
        IndexType::Datetime => {
            if let Ok(Value::Number(num)) = serde_json::from_slice::<Value>(bytes) {
                let nanos = num.as_i64().ok_or(EngineError::Invalid(
                    "lookup datetime number is not an i64 epoch-nanos",
                ))?;
                return Ok(TypedValue::DateTime(
                    UtcTimestamp::new(
                        nanos.div_euclid(1_000_000_000),
                        u32::try_from(nanos.rem_euclid(1_000_000_000)).expect("rem < 1e9"),
                    )
                    .map_err(|_| EngineError::Invalid("typed index datetime out of range"))?,
                ));
            }
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            json_to_typed_value(&Value::String(s.to_owned()), index_type)
        }
    }
}

/// Framed lookup key from caller-supplied raw value slices (API-004 / claim-by-query).
pub fn typed_lookup_key(
    declaration: &IndexDeclaration,
    key_values: &[Vec<u8>],
) -> EngineResult<Vec<u8>> {
    let mut fields = BTreeMap::new();
    match declaration {
        IndexDeclaration::Single(def) => {
            let value = lookup_bytes_to_typed(&def.index_type, &key_values[0])?;
            fields.insert(def.field.clone(), value);
        }
        IndexDeclaration::Compound(def) => {
            for (field, bytes) in def.fields.iter().zip(key_values.iter()) {
                fields.insert(
                    field.field.clone(),
                    lookup_bytes_to_typed(&field.index_type, bytes)?,
                );
            }
        }
    }
    typed_index_key(declaration, &fields)?.ok_or(EngineError::Storage("missing lookup key".into()))
}

/// Serving-time entity echo: stored document, else synthesize from native index fields.
///
/// Compact-log admission may omit `entity_document` when `index_fields` already carry the
/// declared typed keys. Claim/query must still return the same JSON object on every cell.
pub fn echo_entity_document(
    entity: Option<Value>,
    index_fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<Option<Value>> {
    if entity.is_some() {
        return Ok(entity);
    }
    if index_fields.is_empty() {
        return Ok(None);
    }
    index_fields_as_entity(index_fields).map(Some)
}

/// Build a synthetic JSON object from native index fields (claim/query entity echo).
///
/// Index **keys** are encoded from [`TypedValue`] directly ([`typed_index_key`]); this adapter is
/// only for the serving-time document shape, not for keying.
pub fn index_fields_as_entity(fields: &BTreeMap<String, TypedValue>) -> EngineResult<Value> {
    let mut map = serde_json::Map::new();
    for (path, value) in fields {
        insert_dotted(&mut map, path, typed_value_to_json(value)?)?;
    }
    Ok(Value::Object(map))
}

/// Compact durable blob for a native index-field map (sqlite projection item row).
pub fn encode_index_fields_blob(
    fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<Option<Vec<u8>>> {
    if fields.is_empty() {
        return Ok(None);
    }
    postcard::to_allocvec(fields)
        .map(Some)
        .map_err(|e| EngineError::Storage(format!("index_fields encode: {e}")))
}

pub fn decode_index_fields_blob(blob: Option<&[u8]>) -> EngineResult<BTreeMap<String, TypedValue>> {
    match blob {
        None | Some([]) => Ok(BTreeMap::new()),
        Some(bytes) => postcard::from_bytes(bytes)
            .map_err(|e| EngineError::Storage(format!("index_fields decode: {e}"))),
    }
}

/// True when an admitted item's entity document is fully represented by its materialized
/// index fields, i.e. dropping it from the durable row loses nothing.
///
/// The claim path echoes the entity document to consumers (contract since v0.31.0), and
/// consumers may treat it as the authoritative item representation — so admission must
/// only drop it when the synthesized index-field document is byte-equivalent.
pub fn entity_fully_indexed(
    definition: &fireweed_core::QueueDefinition,
    index_fields: &BTreeMap<String, TypedValue>,
    entity: Option<&Value>,
) -> bool {
    if definition.typed_indexes.is_empty()
        || index_fields.is_empty()
        || definition.entity_schema.is_some()
    {
        return false;
    }
    let Some(doc) = entity else {
        return false;
    };
    match index_fields_as_entity(index_fields) {
        Ok(synth) => *doc == synth,
        Err(_) => false,
    }
}

pub fn typed_value_to_json(value: &TypedValue) -> EngineResult<Value> {
    Ok(match value {
        TypedValue::String(v) => Value::String(v.clone()),
        TypedValue::Integer(v) => Value::Number((*v).into()),
        TypedValue::Float(v) => Value::Number(
            serde_json::Number::from_f64(*v)
                .ok_or(EngineError::Invalid("typed index float is not finite"))?,
        ),
        TypedValue::Bool(v) => Value::Bool(*v),
        // Axon's canonical numeric-datetime unit is epoch NANOS (`coerce_datetime_nanos`);
        // emitting seconds here would shift every datetime key by 1e9.
        TypedValue::DateTime(v) => {
            let nanos = v
                .seconds
                .checked_mul(1_000_000_000)
                .and_then(|n| n.checked_add(i64::from(v.nanoseconds)))
                .ok_or(EngineError::Invalid("typed index datetime out of range"))?;
            Value::Number(nanos.into())
        }
    })
}

fn insert_dotted(
    root: &mut serde_json::Map<String, Value>,
    path: &str,
    value: Value,
) -> EngineResult<()> {
    let mut parts = path.split('.').peekable();
    let mut cur = root;
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            cur.insert(part.to_string(), value);
            return Ok(());
        }
        let entry = cur
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        let Value::Object(map) = entry else {
            return Err(EngineError::Invalid(
                "typed index path collides with non-object parent",
            ));
        };
        cur = map;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axon_esf::{IndexDeclaration, IndexDef};
    use fireweed_core::QueueIndex;
    use serde_json::json;

    fn idx(name: &str, field: &str, ty: IndexType) -> QueueIndex {
        QueueIndex {
            name: name.into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: field.into(),
                index_type: ty,
                unique: false,
            }),
        }
    }

    #[test]
    fn extract_native_ints_and_strings() {
        let indexes = vec![
            idx("by_status", "status", IndexType::String),
            idx("by_rank", "rank", IndexType::Integer),
        ];
        let entity = json!({"status": "open", "rank": 7, "extra": "ignored"});
        let fields = extract_index_fields_from_entity(&indexes, &entity).unwrap();
        assert_eq!(
            fields.get("status"),
            Some(&TypedValue::String("open".into()))
        );
        assert_eq!(fields.get("rank"), Some(&TypedValue::Integer(7)));
        assert!(!fields.contains_key("extra"));
    }

    #[test]
    fn materialize_prefers_explicit_native_over_entity() {
        let mut def = {
            // Minimal valid definition shell; only typed_indexes matter here.
            use fireweed_core::*;
            QueueDefinition {
                tenant_id: TenantId::new("t").unwrap(),
                queue_id: QueueId::new("q").unwrap(),
                priority_model: PriorityModel {
                    kind: PriorityModelKind::Int64,
                    direction: PriorityDirection::Ascending,
                    tie_breaker: PriorityTieBreaker::CreatedSequence,
                },
                ordering_mode: OrderingMode::Strict,
                max_rank_error: 0,
                progress_bound_ms: 60_000,
                eligibility_policy: EligibilityPolicy::default(),
                cohort_policy: None,
                recurrence: RecurrencePolicy::default(),
                request_id_retention_ms: 60_000,
                client_item_key_retention_ms: 60_000,
                terminal_retention_ms: 60_000,
                max_lease_duration_ms: 60_000,
                retry_policy: RetryPolicy { max_attempts: 3 },
                max_push_batch_size: 10_000,
                max_claim_batch_size: 10_000,
                max_eligible_group_size: None,
                secondary_indexes: vec![],
                entity_schema: None,
                typed_indexes: vec![idx("by_s", "s", IndexType::String)],
                emit_change_records: false,
            }
        };
        let _ = &mut def;
        let explicit = BTreeMap::from([("s".into(), TypedValue::String("native".into()))]);
        let entity = json!({"s": "from-entity"});
        let got = materialize_index_fields(&def, explicit, Some(&entity)).unwrap();
        assert_eq!(got.get("s"), Some(&TypedValue::String("native".into())));
    }

    #[test]
    fn index_fields_as_entity_round_trips_for_axon() {
        let fields = BTreeMap::from([
            ("f0".into(), TypedValue::String("k-1".into())),
            ("rank".into(), TypedValue::Integer(3)),
        ]);
        let entity = index_fields_as_entity(&fields).unwrap();
        assert_eq!(entity["f0"], json!("k-1"));
        assert_eq!(entity["rank"], json!(3));
        assert_eq!(
            echo_entity_document(None, &fields).unwrap(),
            Some(entity.clone())
        );
        assert_eq!(
            echo_entity_document(Some(json!({"kept": true})), &fields).unwrap(),
            Some(json!({"kept": true}))
        );
    }

    #[test]
    fn native_typed_key_matches_axon_framed_entity_key() {
        let def = IndexDef {
            field: "f0".into(),
            index_type: IndexType::String,
            unique: true,
        };
        let fields = BTreeMap::from([("f0".into(), TypedValue::String("k-1".into()))]);
        let native = typed_index_key(&IndexDeclaration::Single(def.clone()), &fields)
            .unwrap()
            .unwrap();
        let entity = index_fields_as_entity(&fields).unwrap();
        let axon = def.index_key(&entity).unwrap().unwrap();
        assert_eq!(native, axon);

        let idef = IndexDef {
            field: "n".into(),
            index_type: IndexType::Integer,
            unique: false,
        };
        let ints = BTreeMap::from([("n".into(), TypedValue::Integer(-3))]);
        assert_eq!(
            typed_index_key(&IndexDeclaration::Single(idef.clone()), &ints)
                .unwrap()
                .unwrap(),
            idef.index_key(&index_fields_as_entity(&ints).unwrap())
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn framed_typed_value_matches_axon_compound_component() {
        let value = TypedValue::Integer(-3);
        let native = framed_typed_value(&value, &IndexType::Integer).unwrap();
        let json = typed_value_to_json(&value).unwrap();
        let axon = axon_esf::encode_compound_index_key(&[(&json, &IndexType::Integer)])
            .unwrap()
            .unwrap();
        assert_eq!(native, axon);
    }
}
