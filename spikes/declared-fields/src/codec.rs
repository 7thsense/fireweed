//! Field-value encodings shared by the four candidate layouts:
//!
//!  - JSON tagged-array text (layout A, "the draft today" per SPIKE-003's
//!    model (a)), carrying every declared field kind including Bytes
//!    (base64), since A never spills anything off the hot row;
//!  - a compact postcard binary map, declared `u16` field ids (layouts
//!    B/C/D's spill-table field-id column and B/C's hot/cold blob), holding
//!    only Int/Str fields -- large (Bytes) fields never enter this map, they
//!    are always spilled per SPIKE-005's recommendation.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::gen::FieldValue;

// --------------------------------------------------------------- JSON (A)

pub fn json_tag(v: &FieldValue) -> serde_json::Value {
    match v {
        FieldValue::Int(i) => serde_json::json!(["i", i]),
        FieldValue::Str(s) => serde_json::json!(["s", s]),
        FieldValue::Bytes(b) => {
            serde_json::json!(["b", base64::engine::general_purpose::STANDARD.encode(b)])
        }
    }
}

pub fn json_untag(v: &serde_json::Value) -> FieldValue {
    let arr = v.as_array().expect("tagged array");
    let tag = arr[0].as_str().expect("tag");
    match tag {
        "i" => FieldValue::Int(arr[1].as_i64().expect("int")),
        "s" => FieldValue::Str(arr[1].as_str().expect("str").to_string()),
        "b" => FieldValue::Bytes(
            base64::engine::general_purpose::STANDARD
                .decode(arr[1].as_str().expect("b64"))
                .expect("valid base64"),
        ),
        other => panic!("unknown tag {other}"),
    }
}

/// Encode the full 100-field record (A never spills) into one JSON object,
/// keyed `f<NNN>` (matches the draft's tagged-array encoding shape,
/// kernel-contract-draft.md:757).
pub fn json_encode_full(fields: &[(usize, FieldValue)]) -> String {
    let mut obj = serde_json::Map::with_capacity(fields.len());
    for (idx, v) in fields {
        obj.insert(format!("f{idx:03}"), json_tag(v));
    }
    serde_json::Value::Object(obj).to_string()
}

pub fn json_decode_full(text: &str) -> BTreeMap<usize, FieldValue> {
    let obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(text).expect("valid JSON object");
    obj.iter()
        .map(|(k, v)| {
            let idx: usize = k[1..].parse().expect("f<NNN> key");
            (idx, json_untag(v))
        })
        .collect()
}

/// `json_patch`-shaped merge-patch document for a sparse set of fields.
pub fn json_encode_patch(fields: &[(usize, FieldValue)]) -> String {
    json_encode_full(fields)
}

// ------------------------------------------------------------ postcard map

/// The postcard-mapped scalar: only Int/Str ever enter the hot/cold blob --
/// large (Bytes) fields are always spilled (SPIKE-005 Recommendation 2, ~4
/// KiB threshold; here every large field is 16-48 KiB so it is always
/// spilled, never a boundary case).
#[derive(Clone, Serialize, Deserialize)]
pub enum PValue {
    Int(i64),
    Str(String),
}

impl From<&FieldValue> for PValue {
    fn from(v: &FieldValue) -> Self {
        match v {
            FieldValue::Int(i) => PValue::Int(*i),
            FieldValue::Str(s) => PValue::Str(s.clone()),
            FieldValue::Bytes(_) => panic!("large fields never enter the postcard map"),
        }
    }
}
impl From<&PValue> for FieldValue {
    fn from(v: &PValue) -> Self {
        match v {
            PValue::Int(i) => FieldValue::Int(*i),
            PValue::Str(s) => FieldValue::Str(s.clone()),
        }
    }
}

pub type FieldMap = BTreeMap<u16, PValue>;

/// Small+medium-only map (fields 0..94): the hot/cold blob for layouts
/// B/C, and the value-generation source for D's typed columns.
pub fn small_medium_map(fields: &[(usize, FieldValue)]) -> FieldMap {
    fields
        .iter()
        .filter(|(idx, _)| crate::gen::is_small(*idx) || crate::gen::is_medium(*idx))
        .map(|(idx, v)| (*idx as u16, PValue::from(v)))
        .collect()
}

pub fn encode_map(map: &FieldMap) -> Vec<u8> {
    postcard::to_allocvec(map).expect("postcard encode")
}

pub fn decode_map(bytes: &[u8]) -> FieldMap {
    postcard::from_bytes(bytes).expect("postcard decode")
}
