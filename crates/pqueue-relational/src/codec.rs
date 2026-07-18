use std::cmp::Ordering;
use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_core::{
    FilterOp, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken, Metadata, OrderField,
    PriorityModel, PriorityValue, QueryFilter, QueueIndex, RangeScanRow, SortDirection, TypedValue,
    UtcTimestamp, priority_sort,
};
use pqueue_engine::{EngineError, EngineResult};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

#[derive(serde::Deserialize)]
struct ClaimByQueryReplayItemIds {
    item_ids: Vec<ItemId>,
}

/// Decode the item-id portion of a retained claim-by-query response without coupling a driver to
/// the response's JSON representation.
pub fn claim_by_query_replay_item_ids(raw: &str) -> EngineResult<Vec<ItemId>> {
    serde_json::from_str::<ClaimByQueryReplayItemIds>(raw)
        .map(|replay| replay.item_ids)
        .map_err(|error| EngineError::Storage(error.to_string()))
}

pub fn fields_to_json(fields: &BTreeMap<String, Bytes>) -> EngineResult<String> {
    let raw: BTreeMap<&str, Vec<u8>> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.to_vec()))
        .collect();
    to_json(&raw)
}

pub fn fields_from_json(raw: String) -> EngineResult<BTreeMap<String, Bytes>> {
    let decoded: BTreeMap<String, Vec<u8>> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(decoded
        .into_iter()
        .map(|(k, v)| (k, Bytes::from(v)))
        .collect())
}

pub fn metadata_to_json(metadata: &Metadata) -> EngineResult<String> {
    to_json(&metadata.clone().into_inner())
}

pub fn metadata_from_json(raw: String) -> EngineResult<Metadata> {
    let entries = serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Metadata::from_entries(entries))
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn parse_utc_timestamp(value: &str) -> EngineResult<UtcTimestamp> {
    let Some(value) = value
        .strip_suffix('Z')
        .or_else(|| value.strip_suffix("+00:00"))
    else {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    };
    let (date, time) = value.split_once('T').ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let month: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let day: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    if date_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }

    let mut time_parts = time.split(':');
    let hour: i64 = time_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let minute: i64 =
        time_parts
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or(EngineError::Invalid(
                "typed index value is not a valid datetime",
            ))?;
    let sec_part = time_parts.next().ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;
    if time_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }
    let (second, nanos) = match sec_part.split_once('.') {
        Some((whole, frac)) => {
            let second: i64 = whole
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            if frac.is_empty() || frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
                return Err(EngineError::Invalid(
                    "typed index value is not a valid datetime",
                ));
            }
            let mut digits = frac.to_string();
            while digits.len() < 9 {
                digits.push('0');
            }
            let nanos: u32 = digits
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            (second, nanos)
        }
        None => (
            sec_part
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?,
            0,
        ),
    };
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    UtcTimestamp::new(seconds, nanos)
        .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))
}

pub fn typed_value_for_json(
    value: &JsonValue,
    index_type: &IndexType,
) -> EngineResult<Option<TypedValue>> {
    if value.is_null() {
        return Ok(None);
    }
    let typed = match index_type {
        IndexType::String => value
            .as_str()
            .map(|s| TypedValue::String(s.to_string()))
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Integer => {
            value
                .as_i64()
                .map(TypedValue::Integer)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        IndexType::Float => value
            .as_f64()
            .map(TypedValue::Float)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Boolean => value
            .as_bool()
            .map(TypedValue::Bool)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Datetime => match value {
            JsonValue::String(s) => TypedValue::DateTime(parse_utc_timestamp(s)?),
            JsonValue::Number(n) => {
                let seconds = n.as_i64().ok_or({
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?;
                TypedValue::DateTime(UtcTimestamp::new(seconds, 0).map_err(|_| {
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?)
            }
            _ => {
                return Err(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ));
            }
        },
    };
    Ok(Some(typed))
}

pub fn typed_value_from_filter_value(
    value: &TypedValue,
    index_type: &IndexType,
) -> EngineResult<TypedValue> {
    match (value, index_type) {
        (TypedValue::String(v), IndexType::String) => Ok(TypedValue::String(v.clone())),
        (TypedValue::Integer(v), IndexType::Integer) => Ok(TypedValue::Integer(*v)),
        (TypedValue::Float(v), IndexType::Float) => Ok(TypedValue::Float(*v)),
        (TypedValue::Bool(v), IndexType::Boolean) => Ok(TypedValue::Bool(*v)),
        (TypedValue::DateTime(v), IndexType::Datetime) => Ok(TypedValue::DateTime(*v)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

pub fn typed_value_matches_query(value: &TypedValue, filter: &TypedValue) -> bool {
    match (value, filter) {
        (TypedValue::String(a), TypedValue::String(b)) => a == b,
        (TypedValue::Integer(a), TypedValue::Integer(b)) => a == b,
        (TypedValue::Float(a), TypedValue::Float(b)) => a == b,
        (TypedValue::Bool(a), TypedValue::Bool(b)) => a == b,
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => a == b,
        _ => false,
    }
}

pub fn typed_value_compare(a: &TypedValue, b: &TypedValue) -> EngineResult<Ordering> {
    match (a, b) {
        (TypedValue::String(a), TypedValue::String(b)) => Ok(a.cmp(b)),
        (TypedValue::Integer(a), TypedValue::Integer(b)) => Ok(a.cmp(b)),
        (TypedValue::Float(a), TypedValue::Float(b)) => a.partial_cmp(b).ok_or(
            EngineError::Invalid("typed index value comparison is undefined"),
        ),
        (TypedValue::Bool(a), TypedValue::Bool(b)) => Ok(a.cmp(b)),
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => Ok(a.cmp(b)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

pub fn merge_entity_document(
    entity: Option<&JsonValue>,
    set_fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<JsonValue> {
    let mut object = match entity {
        Some(JsonValue::Object(map)) => map.clone(),
        Some(_) => {
            return Err(EngineError::Invalid("typed index entity is not an object"));
        }
        None => serde_json::Map::new(),
    };
    for (field, value) in set_fields {
        object.insert(
            field.clone(),
            match value {
                TypedValue::String(v) => JsonValue::String(v.clone()),
                TypedValue::Integer(v) => JsonValue::Number((*v).into()),
                TypedValue::Float(v) => {
                    JsonValue::Number(serde_json::Number::from_f64(*v).ok_or({
                        EngineError::Invalid("typed index value is not valid for declared type")
                    })?)
                }
                TypedValue::Bool(v) => JsonValue::Bool(*v),
                TypedValue::DateTime(v) => JsonValue::Number(v.seconds.into()),
            },
        );
    }
    Ok(JsonValue::Object(object))
}

pub fn typed_index_row_from_entity(
    spec: &QueueIndex,
    item_id: ItemId,
    entity: &JsonValue,
) -> EngineResult<Option<RangeScanRow>> {
    let mut fields = BTreeMap::new();
    match &spec.declaration {
        IndexDeclaration::Single(def) => {
            let Some(value) = typed_value_for_json(
                entity.get(&def.field).unwrap_or(&JsonValue::Null),
                &def.index_type,
            )?
            else {
                return Ok(None);
            };
            fields.insert(def.field.clone(), value);
        }
        IndexDeclaration::Compound(def) => {
            for field in &def.fields {
                let Some(value) = typed_value_for_json(
                    entity.get(&field.field).unwrap_or(&JsonValue::Null),
                    &field.index_type,
                )?
                else {
                    return Ok(None);
                };
                fields.insert(field.field.clone(), value);
            }
        }
    }
    Ok(Some(RangeScanRow { item_id, fields }))
}

pub fn typed_index_row_matches(
    spec: &QueueIndex,
    filters: &[QueryFilter],
    row: &RangeScanRow,
) -> EngineResult<bool> {
    let fields: Vec<(&str, &IndexType)> = match &spec.declaration {
        IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
        IndexDeclaration::Compound(def) => def
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    };
    let mut filter_map: BTreeMap<&str, &QueryFilter> = BTreeMap::new();
    for filter in filters {
        filter_map.insert(filter.field.as_str(), filter);
    }
    let mut prefix_len = 0usize;
    for (field_name, index_type) in &fields {
        let Some(filter) = filter_map.get(field_name).copied() else {
            break;
        };
        let typed = typed_value_from_filter_value(&filter.value, index_type)?;
        let Some(value) = row.fields.get(*field_name) else {
            return Ok(false);
        };
        if filter.op != FilterOp::Eq || !typed_value_matches_query(value, &typed) {
            break;
        }
        prefix_len += 1;
    }
    for filter in filters {
        let Some((idx, (_, index_type))) = fields
            .iter()
            .enumerate()
            .find(|(_, (field_name, _))| *field_name == filter.field.as_str())
        else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        if idx < prefix_len {
            continue;
        }
        let Some(value) = row.fields.get(filter.field.as_str()) else {
            return Ok(false);
        };
        let typed = typed_value_from_filter_value(&filter.value, index_type)?;
        let ord = typed_value_compare(value, &typed)?;
        let ok = match filter.op {
            FilterOp::Eq => ord.is_eq(),
            FilterOp::Gte => ord.is_ge(),
            FilterOp::Gt => ord.is_gt(),
            FilterOp::Lte => ord.is_le(),
            FilterOp::Lt => ord.is_lt(),
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn compare_rows(
    lhs: &RangeScanRow,
    rhs: &RangeScanRow,
    order_by: &[OrderField],
) -> EngineResult<Ordering> {
    for field in order_by {
        let left = lhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let right = rhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let ord = typed_value_compare(left, right)?;
        let ord = match field.direction {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        };
        if !ord.is_eq() {
            return Ok(ord);
        }
    }
    Ok(lhs.item_id.cmp(&rhs.item_id))
}

pub fn ts_nanos(ts: UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanoseconds as i64)
}

pub fn ts_nanos_opt(ts: Option<UtcTimestamp>) -> Option<i64> {
    ts.map(ts_nanos)
}

pub fn nanos_ts(v: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        v.div_euclid(1_000_000_000),
        v.rem_euclid(1_000_000_000) as u32,
    )
    .expect("nanoseconds bounded by rem_euclid")
}

pub fn state_str(s: ItemState) -> &'static str {
    match s {
        ItemState::Pending => "Pending",
        ItemState::Leased => "Leased",
        ItemState::Complete => "Complete",
        ItemState::Failed => "Failed",
    }
}

pub fn parse_state(s: &str) -> EngineResult<ItemState> {
    match s {
        "Pending" => Ok(ItemState::Pending),
        "Leased" => Ok(ItemState::Leased),
        "Complete" => Ok(ItemState::Complete),
        "Failed" => Ok(ItemState::Failed),
        other => Err(EngineError::Storage(format!(
            "unknown lifecycle_state {other}"
        ))),
    }
}

/// Tagged priority-sort encoding, byte-identical to the in-memory `elig_key` (priced items tag 0 then
/// the model's `priority_sort` bytes; unpriced tag 1) — so `ORDER BY priority_sort` matches the
/// in-memory eligibility order exactly.
pub fn elig_sort(priority: &Option<PriorityValue>, model: &PriorityModel) -> Vec<u8> {
    match priority {
        Some(p) => {
            let mut v = vec![0u8];
            v.extend(priority_sort(p, model));
            v
        }
        None => vec![1u8],
    }
}

pub fn lease_hash(token: &LeaseToken) -> Vec<u8> {
    Sha256::digest(token.as_str().as_bytes()).to_vec()
}

pub fn parse_priority(raw: Option<String>) -> EngineResult<Option<PriorityValue>> {
    raw.map(|s| serde_json::from_str(&s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
}
