//! Native declared-index queries. SQL bounds candidate rows before materialization; reads never
//! reconstruct a whole queue. A non-prefix predicate may scan the addressed covering index because
//! the durable schema stores compound keys, not a separate index for every component.

use std::collections::{BTreeMap, BTreeSet};

use fireweed_core::{
    AggregateGroup, BoundedMutationRequest, BoundedMutationResponse, BucketCount,
    ClaimByItemIdClass, ClientItemKey, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse,
    FilterOp, GroupedAggregateRequest, GroupedAggregateResponse, IndexDeclaration, IndexType,
    ItemId, MetricsByQueryRequest, MutationOutcome, OrderField, QueryCursor, QueryFilter,
    QueueDefinition, QueueIndex, RangeScanRequest, RangeScanResponse, RangeScanRow, RequestId,
    SortDirection, TimeBucket, TypedValue, UtcTimestamp,
};
use fireweed_engine::index_fields::{
    decode_index_fields_blob, encode_typed_value, extract_index_fields_from_entity,
};
use fireweed_engine::{
    ActiveScope, BoundedMutationPlan, DiscoveryGranularity, EngineError, EngineResult, IndexHit,
    QueueKey, QueueMetrics,
};
use fireweed_projection::{ProjectionData, ProjectionImage};
use fireweed_relational::{index_is_unique, nanos_ts, ts_nanos, typed_lookup_canonical_key};
use turso::{Connection, Value, transaction::TransactionBehavior};

use crate::TursoRelational;

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;

const MAX_PAGE: usize = 1_000;
// Every caller binds the addressed tenant and queue as ?1/?2 and aliases its item as i.
// Pin both correlated seeks: a queue-prefix scan here repeats for every candidate row.
const UNBLOCKED: &str = "NOT EXISTS (SELECT 1 FROM fireweed_item_gates ig \
    INDEXED BY sqlite_autoindex_fireweed_item_gates_1 \
    CROSS JOIN fireweed_gate_state gs INDEXED BY sqlite_autoindex_fireweed_gate_state_1 \
    ON gs.tenant_id=?1 AND gs.queue_id=?2 AND gs.gate_key=ig.gate_key \
    WHERE ig.tenant_id=?1 AND ig.queue_id=?2 AND ig.item_id=i.item_id)";

fn storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
}

fn text(value: &Value) -> EngineResult<&str> {
    match value {
        Value::Text(value) => Ok(value),
        _ => Err(storage("expected text")),
    }
}

fn integer(value: &Value) -> EngineResult<i64> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => Err(storage("expected integer")),
    }
}

fn blob(value: &Value) -> EngineResult<&[u8]> {
    match value {
        Value::Blob(value) => Ok(value),
        _ => Err(storage("expected blob")),
    }
}

async fn rows_on(
    connection: &Connection,
    sql: &str,
    params: Vec<Value>,
) -> EngineResult<Vec<Vec<Value>>> {
    let mut rows = connection.query(sql, params).await.map_err(storage)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage)? {
        result.push(
            (0..row.column_count())
                .map(|column| row.get_value(column).map_err(storage))
                .collect::<EngineResult<_>>()?,
        );
    }
    Ok(result)
}

fn queue_params(shard: &QueueKey) -> Vec<Value> {
    vec![
        shard.tenant_id.as_str().to_owned().into(),
        shard.queue_id.as_str().to_owned().into(),
    ]
}

async fn definition_on(
    connection: &Connection,
    shard: &QueueKey,
) -> EngineResult<(QueueDefinition, bool)> {
    let rows = rows_on(
        connection,
        "SELECT definition,paused FROM queues WHERE tenant=?1 AND queue=?2",
        queue_params(shard),
    )
    .await?;
    let row = rows.first().ok_or(EngineError::NotFound)?;
    Ok((
        serde_json::from_str(text(&row[0])?).map_err(storage)?,
        integer(&row[1])? != 0,
    ))
}

fn index_spec<'a>(
    definition: &'a QueueDefinition,
    name: Option<&str>,
) -> EngineResult<&'a QueueIndex> {
    match name {
        Some(name) => definition
            .typed_indexes
            .iter()
            .find(|index| index.name == name),
        None => definition.typed_indexes.first(),
    }
    .ok_or(EngineError::Invalid("unknown secondary index"))
}

fn index_fields(spec: &QueueIndex) -> Vec<(&str, &IndexType)> {
    match &spec.declaration {
        IndexDeclaration::Single(field) => vec![(&field.field, &field.index_type)],
        IndexDeclaration::Compound(compound) => compound
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    }
}

fn position(spec: &QueueIndex, field: &str) -> EngineResult<usize> {
    index_fields(spec)
        .iter()
        .position(|(name, _)| *name == field)
        .ok_or(EngineError::Invalid("unindexed-field"))
}

fn encode(value: &TypedValue, kind: &IndexType) -> EngineResult<Vec<u8>> {
    if matches!(value, TypedValue::Float(value) if !value.is_finite()) {
        return Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        ));
    }
    encode_typed_value(value, kind)
}

fn successor(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    for at in (0..prefix.len()).rev() {
        if prefix[at] != 255 {
            prefix[at] += 1;
            prefix.truncate(at + 1);
            return Some(prefix);
        }
    }
    None
}

fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut result = (bytes.len() as u32).to_be_bytes().to_vec();
    result.extend_from_slice(bytes);
    result
}

/// Decode a frame length from eight already-decoded hex digits. Keeping the digits in a
/// preceding streaming stage bounds expression depth in Turso's recursive SQL translator.
fn component_length_sql(kind: &IndexType) -> String {
    match kind {
        IndexType::Boolean => "1".into(),
        IndexType::Integer | IndexType::Float | IndexType::Datetime => "8".into(),
        IndexType::String => {
            let mut terms = (1..=8)
                .map(|digit| format!("n{digit}*{}", 16_u64.pow(8 - digit)))
                .collect::<Vec<_>>();
            // Eight terms form three binary levels. A left-associated sum adds
            // seven levels to Turso's recursive expression translator instead.
            while terms.len() > 1 {
                terms = terms
                    .chunks_exact(2)
                    .map(|pair| format!("({}+{})", pair[0], pair[1]))
                    .collect();
            }
            terms.pop().expect("eight framing nibbles")
        }
    }
}

#[derive(Clone)]
struct IndexedSql {
    ctes: String,
    from: String,
    predicates: Vec<String>,
    params: Vec<Value>,
    bounds: String,
}

impl IndexedSql {
    fn new(shard: &QueueKey, spec: &QueueIndex, filters: &[QueryFilter]) -> EngineResult<Self> {
        let fields = index_fields(spec);
        let mut params = queue_params(shard);
        params.push(spec.name.clone().into());
        let mut bounds = String::new();
        let mut prefix = Vec::new();
        let mut prefix_lengths = Vec::new();
        let mut equality_fields = 0;
        for (name, kind) in &fields {
            let equalities = filters
                .iter()
                .filter(|filter| filter.field == *name && filter.op == FilterOp::Eq)
                .collect::<Vec<_>>();
            if equalities.len() != 1 {
                break;
            }
            let encoded = encode(&equalities[0].value, kind)?;
            prefix_lengths.push(encoded.len());
            prefix.extend(frame(&encoded));
            equality_fields += 1;
        }
        if !prefix.is_empty() {
            params.push(prefix.clone().into());
            bounds.push_str(&format!(" AND index_key>=?{}", params.len()));
            if let Some(end) = successor(prefix.clone()) {
                params.push(end.into());
                bounds.push_str(&format!(" AND index_key<?{}", params.len()));
            }
        }
        // The next fixed-width component preserves typed ordering in the framed key, so its range
        // can seek the covering index too. String framing includes a variable length and cannot use
        // that optimization; its decoded component predicate remains authoritative below.
        if let Some((name, kind)) = fields.get(equality_fields)
            && !matches!(kind, IndexType::String)
        {
            for filter in filters
                .iter()
                .filter(|filter| filter.field == *name && filter.op != FilterOp::Eq)
            {
                let mut endpoint = prefix.clone();
                endpoint.extend(frame(&encode(&filter.value, kind)?));
                let (operator, endpoint) = match filter.op {
                    FilterOp::Gte => (">=", Some(endpoint)),
                    FilterOp::Gt => (">=", successor(endpoint)),
                    FilterOp::Lt => ("<", Some(endpoint)),
                    FilterOp::Lte => ("<", successor(endpoint)),
                    FilterOp::Eq => unreachable!("equality excluded above"),
                };
                if let Some(endpoint) = endpoint {
                    params.push(endpoint.into());
                    bounds.push_str(&format!(" AND index_key{operator}?{}", params.len()));
                }
            }
        }
        let mut stages = vec![format!(
            "k0 AS (SELECT item_id,index_key,1 AS pos FROM fireweed_item_index \
             INDEXED BY fireweed_item_index_key_numeric_asc_idx \
             WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3{bounds})"
        )];
        for (number, (_, kind)) in fields.iter().enumerate() {
            // The covering-index prefix bound already fixes these framed bytes.
            // Reuse their known length instead of decoding the same prefix in SQL.
            let length = prefix_lengths
                .get(number)
                .map_or_else(|| component_length_sql(kind), usize::to_string);
            let previous = (0..number).map(|at| format!(",c{at}")).collect::<String>();
            let (source, length) = if matches!(kind, IndexType::String)
                && prefix_lengths.get(number).is_none()
            {
                // Ordinary single-reference CTEs stream through coroutines. Separate
                // nibble decoding and arithmetic so neither the parser nor translator
                // recursively expands an eight-term decoder inside substr/position.
                let nibbles = (1..=8)
                    .map(|digit| format!(
                        ",instr('0123456789ABCDEF',substr(hex(substr(index_key,pos,4)),{digit},1))-1 AS n{digit}"
                    ))
                    .collect::<String>();
                stages.push(format!(
                    "k{number}_digits AS (SELECT item_id,index_key{previous},pos{nibbles} FROM k{number})"
                ));
                stages.push(format!(
                    "k{number}_length AS (SELECT item_id,index_key{previous},pos,{length} AS component_length FROM k{number}_digits)"
                ));
                (format!("k{number}_length"), "component_length".to_owned())
            } else {
                (format!("k{number}"), length)
            };
            stages.push(format!("k{} AS (SELECT item_id,index_key{previous},substr(index_key,pos+4,({length})) AS c{number},pos+4+({length}) AS pos FROM {source})", number + 1));
        }
        let from = format!(
            "k{} k CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
            ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=k.item_id",
            fields.len()
        );
        let mut predicates = vec!["i.superseded=0".to_owned()];
        for filter in filters {
            let at = position(spec, &filter.field)?;
            params.push(encode(&filter.value, fields[at].1)?.into());
            let operator = match filter.op {
                FilterOp::Eq => "=",
                FilterOp::Gt => ">",
                FilterOp::Gte => ">=",
                FilterOp::Lt => "<",
                FilterOp::Lte => "<=",
            };
            predicates.push(format!("k.c{at}{operator}?{}", params.len()));
        }
        Ok(Self {
            ctes: format!("WITH {}", stages.join(",")),
            from,
            predicates,
            params,
            bounds,
        })
    }

    /// Canonical fixed-width components have the same byte order as their typed values. When
    /// equality filters fix every other component, use that physical order directly and let LIMIT
    /// stop the covering-index walk. Variable-length string ordering retains the decoded fallback.
    fn native_order(
        &mut self,
        spec: &QueueIndex,
        filters: &[QueryFilter],
        order_by: &[OrderField],
    ) -> EngineResult<Option<SortDirection>> {
        let fields = index_fields(spec);
        let equalities = fields
            .iter()
            .map(|(name, _)| {
                filters
                    .iter()
                    .find(|filter| filter.field == *name && filter.op == FilterOp::Eq)
            })
            .collect::<Vec<_>>();
        let remaining = fields
            .iter()
            .zip(&equalities)
            .filter_map(|((name, _), eq)| eq.is_none().then_some(*name))
            .collect::<Vec<_>>();
        let requested = order_by
            .iter()
            .filter(|order| {
                !filters
                    .iter()
                    .any(|filter| filter.field == order.field && filter.op == FilterOp::Eq)
            })
            .map(|order| order.field.as_str())
            .collect::<Vec<_>>();
        if remaining != requested
            || fields
                .iter()
                .zip(&equalities)
                .any(|((_, kind), eq)| matches!(kind, IndexType::String) && eq.is_none())
        {
            return Ok(None);
        }
        let Some(order) = order_by.first() else {
            return Ok(None);
        };
        let mut expressions = Vec::new();
        let mut offset = 1;
        for (at, (_, kind)) in fields.iter().enumerate() {
            let length = match kind {
                IndexType::Boolean => 1,
                IndexType::Integer | IndexType::Float | IndexType::Datetime => 8,
                IndexType::String => {
                    let filter = equalities[at]
                        .expect("variable-length components are equality-constrained");
                    let length = encode(&filter.value, kind)?.len();
                    let bind = self.bind((length as u32).to_be_bytes().to_vec());
                    self.predicates
                        .push(format!("substr(k.index_key,{offset},4)={bind}"));
                    length
                }
            };
            expressions.push(format!("substr(k.index_key,{}, {length})", offset + 4));
            offset += 4 + length;
        }
        for at in (0..fields.len()).rev() {
            for predicate in &mut self.predicates {
                *predicate = predicate.replace(&format!("k.c{at}"), &expressions[at]);
            }
        }
        let physical = if order.direction == SortDirection::Descending {
            "desc"
        } else {
            "asc"
        };
        self.ctes.clear();
        self.from = format!(
            "fireweed_item_index k INDEXED BY fireweed_item_index_key_numeric_{physical}_idx \
            CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
            ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=k.item_id"
        );
        self.predicates.push(format!(
            "k.tenant_id=?1 AND k.queue_id=?2 AND k.index_name=?3{}",
            self.bounds.replace("index_key", "k.index_key")
        ));
        Ok(Some(order.direction))
    }

    fn select(&self, columns: &str, suffix: &str) -> String {
        format!(
            "{} SELECT {columns} FROM {} WHERE {} {suffix}",
            self.ctes,
            self.from,
            self.predicates.join(" AND ")
        )
    }

    fn bind(&mut self, value: impl Into<Value>) -> String {
        self.params.push(value.into());
        format!("?{}", self.params.len())
    }
}

fn native_order_sql(direction: SortDirection) -> &'static str {
    match direction {
        SortDirection::Ascending => "k.index_key ASC,length(k.item_id) ASC,k.item_id ASC",
        SortDirection::Descending => "k.index_key DESC,length(k.item_id) ASC,k.item_id ASC",
    }
}

/// Continue a native keyset in three disjoint seeks: remaining same-width IDs at the anchor key,
/// larger IDs at that key, then subsequent keys. Each branch has an index-compatible ORDER BY and
/// its own remaining LIMIT; a huge equal-key population never forces a tie-group sort or rescan.
async fn native_cursor_rows(
    connection: &Connection,
    query: IndexedSql,
    direction: SortDirection,
    anchor_key: Vec<u8>,
    anchor_id: ItemId,
    limit: usize,
) -> EngineResult<Vec<Vec<Value>>> {
    let mut result = Vec::with_capacity(limit);
    for part in 0..3 {
        if result.len() == limit {
            break;
        }
        let (sql, params) = native_cursor_statement(
            &query,
            direction,
            &anchor_key,
            anchor_id,
            part,
            limit - result.len(),
        );
        result.extend(rows_on(connection, &sql, params).await?);
    }
    Ok(result)
}

fn native_cursor_statement(
    query: &IndexedSql,
    direction: SortDirection,
    anchor_key: &[u8],
    anchor_id: ItemId,
    part: usize,
    limit: usize,
) -> (String, Vec<Value>) {
    let mut query = query.clone();
    let key = query.bind(anchor_key.to_vec());
    let predicate = match part {
        0 => {
            let length = query.bind(anchor_id.to_string().len() as i64);
            let id = query.bind(anchor_id.to_string());
            format!("k.index_key={key} AND length(k.item_id)={length} AND k.item_id>{id}")
        }
        1 => {
            let length = query.bind(anchor_id.to_string().len() as i64);
            format!("k.index_key={key} AND length(k.item_id)>{length}")
        }
        _ => format!(
            "k.index_key{}{key}",
            if direction == SortDirection::Descending {
                "<"
            } else {
                ">"
            }
        ),
    };
    query.predicates.push(predicate);
    let remaining = query.bind(limit as i64);
    let sql = query.select(
        "i.item_id,k.index_key",
        &format!("ORDER BY {} LIMIT {remaining}", native_order_sql(direction)),
    );
    (sql, query.params)
}

fn decode_component(bytes: &[u8], kind: &IndexType) -> EngineResult<TypedValue> {
    let bad = || storage("invalid canonical index key");
    Ok(match kind {
        IndexType::String => {
            TypedValue::String(std::str::from_utf8(bytes).map_err(|_| bad())?.to_owned())
        }
        IndexType::Boolean => match bytes {
            [0] => TypedValue::Bool(false),
            [1] => TypedValue::Bool(true),
            _ => return Err(bad()),
        },
        IndexType::Integer | IndexType::Datetime | IndexType::Float => {
            let encoded = u64::from_be_bytes(bytes.try_into().map_err(|_| bad())?);
            match kind {
                IndexType::Integer => TypedValue::Integer((encoded ^ (1 << 63)) as i64),
                IndexType::Datetime => TypedValue::DateTime(nanos_ts((encoded ^ (1 << 63)) as i64)),
                _ => TypedValue::Float(f64::from_bits(if encoded & (1 << 63) != 0 {
                    encoded & !(1 << 63)
                } else {
                    !encoded
                })),
            }
        }
    })
}

fn decode_fields(spec: &QueueIndex, key: &[u8]) -> EngineResult<BTreeMap<String, TypedValue>> {
    let mut offset = 0;
    let mut fields = BTreeMap::new();
    for (name, kind) in index_fields(spec) {
        let size = key
            .get(offset..offset + 4)
            .ok_or_else(|| storage("invalid canonical index key"))?;
        let length = u32::from_be_bytes(size.try_into().map_err(storage)?) as usize;
        offset += 4;
        let bytes = key
            .get(offset..offset + length)
            .ok_or_else(|| storage("invalid canonical index key"))?;
        fields.insert(name.to_owned(), decode_component(bytes, kind)?);
        offset += length;
    }
    if offset != key.len() {
        return Err(storage("invalid canonical index key"));
    }
    Ok(fields)
}

fn orders(spec: &QueueIndex, order_by: &[OrderField]) -> EngineResult<String> {
    let first = order_by
        .first()
        .ok_or(EngineError::Invalid("range-scan order_by required"))?;
    if order_by
        .iter()
        .any(|order| order.direction != first.direction)
    {
        return Err(EngineError::Invalid(
            "mixed order directions are unsupported",
        ));
    }
    let direction = if first.direction == SortDirection::Descending {
        "DESC"
    } else {
        "ASC"
    };
    let mut terms = order_by
        .iter()
        .map(|order| position(spec, &order.field).map(|at| format!("k.c{at} {direction}")))
        .collect::<EngineResult<Vec<_>>>()?;
    // Item ids are decimal u64 strings. Length plus text is exact beyond SQLite's signed i64 range.
    terms.extend(["length(i.item_id) ASC".into(), "i.item_id ASC".into()]);
    Ok(terms.join(","))
}

struct CursorState {
    item_id: ItemId,
    values: Vec<TypedValue>,
}

fn parse_cursor(
    request: &RangeScanRequest,
    spec: &QueueIndex,
) -> EngineResult<Option<CursorState>> {
    let Some(cursor) = &request.cursor else {
        return Ok(None);
    };
    let invalid = || EngineError::Invalid("cursor-invalidated");
    let value: serde_json::Value = serde_json::from_str(&cursor.0).map_err(|_| invalid())?;
    if value["index"] != spec.name
        || value["filters"] != serde_json::to_value(&request.filters).map_err(storage)?
        || value["order_by"] != serde_json::to_value(&request.order_by).map_err(storage)?
    {
        return Err(invalid());
    }
    let item_id = serde_json::from_value(value["anchor_item_id"].clone()).map_err(|_| invalid())?;
    let values: Vec<TypedValue> =
        serde_json::from_value(value["anchor_values"].clone()).map_err(|_| invalid())?;
    if values.len() != request.order_by.len() {
        return Err(invalid());
    }
    Ok(Some(CursorState { item_id, values }))
}

async fn range_scan_on(
    connection: &Connection,
    shard: &QueueKey,
    definition: &QueueDefinition,
    request: RangeScanRequest,
) -> EngineResult<RangeScanResponse> {
    request
        .validate(MAX_PAGE as u32)
        .map_err(|_| EngineError::Invalid("invalid page size"))?;
    let spec = index_spec(definition, request.index.as_deref())?;
    let mut order = orders(spec, &request.order_by)?;
    let mut query = IndexedSql::new(shard, spec, &request.filters)?;
    let native_direction = query.native_order(spec, &request.filters, &request.order_by)?;
    if let Some(direction) = native_direction {
        order = native_order_sql(direction).into();
    }
    let mut native_cursor = None;
    if let Some(cursor) = parse_cursor(&request, spec)? {
        let mut anchor_params = queue_params(shard);
        anchor_params.extend([spec.name.clone().into(), cursor.item_id.to_string().into()]);
        let anchor = rows_on(
            connection,
            "SELECT idx.index_key FROM fireweed_item_index idx \
            CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
            ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id AND i.item_id=idx.item_id \
            WHERE idx.tenant_id=?1 AND idx.queue_id=?2 AND idx.index_name=?3 AND idx.item_id=?4 \
            AND i.superseded=0",
            anchor_params,
        )
        .await?;
        let anchor = anchor
            .first()
            .ok_or(EngineError::Invalid("cursor-invalidated"))?;
        let fields = decode_fields(spec, blob(&anchor[0])?)?;
        if !matches_fields(spec, &fields, &request.filters)? {
            return Err(EngineError::Invalid("cursor-invalidated"));
        }
        let current = request
            .order_by
            .iter()
            .map(|order| {
                fields
                    .get(&order.field)
                    .cloned()
                    .ok_or(EngineError::Invalid("cursor-invalidated"))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        if current != cursor.values {
            return Err(EngineError::Invalid("cursor-invalidated"));
        }
        if native_direction.is_some() {
            native_cursor = Some((blob(&anchor[0])?.to_vec(), cursor.item_id));
        } else {
            let anchor_bind = query.bind(cursor.item_id.to_string());
            let declared = index_fields(spec);
            let mut equal = Vec::new();
            let mut after = Vec::new();
            for (order, value) in request.order_by.iter().zip(&cursor.values) {
                let at = position(spec, &order.field)?;
                let bind = query.bind(encode(value, declared[at].1)?);
                let op = if order.direction == SortDirection::Descending {
                    "<"
                } else {
                    ">"
                };
                let term = format!("k.c{at}{op}{bind}");
                after.push(if equal.is_empty() {
                    term
                } else {
                    format!("({} AND {term})", equal.join(" AND "))
                });
                equal.push(format!("k.c{at}={bind}"));
            }
            after.push(format!("({} AND (length(i.item_id)>length({anchor_bind}) OR (length(i.item_id)=length({anchor_bind}) AND i.item_id>{anchor_bind})))", equal.join(" AND ")));
            query.predicates.push(format!("({})", after.join(" OR ")));
        }
    }
    let data = if let Some((key, id)) = native_cursor {
        native_cursor_rows(
            connection,
            query,
            native_direction.expect("native cursor has native order"),
            key,
            id,
            request.page_size as usize + 1,
        )
        .await?
    } else {
        let limit = query.bind(i64::from(request.page_size) + 1);
        let sql = query.select(
            "i.item_id,k.index_key",
            &format!("ORDER BY {order} LIMIT {limit}"),
        );
        rows_on(connection, &sql, query.params).await?
    };
    let has_more = data.len() > request.page_size as usize;
    let rows = data
        .into_iter()
        .take(request.page_size as usize)
        .map(|row| {
            Ok(RangeScanRow {
                item_id: ItemId::new(text(&row[0])?).map_err(storage)?,
                fields: decode_fields(spec, blob(&row[1])?)?,
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let next_cursor = if has_more {
        let anchor = rows
            .last()
            .ok_or_else(|| storage("empty query cursor page"))?;
        Some(QueryCursor(serde_json::to_string(&serde_json::json!({
            "index": spec.name, "filters": request.filters, "order_by": request.order_by,
            "anchor_item_id": anchor.item_id,
            "anchor_values": request.order_by.iter().map(|order| &anchor.fields[&order.field]).collect::<Vec<_>>(),
            "anchor_index_key": null,
        })).map_err(storage)?))
    } else {
        None
    };
    Ok(RangeScanResponse { rows, next_cursor })
}

impl TursoRelational {
    /// Validate an upsert before log append while the composition owns its queue mutation fence.
    /// Replacement uniqueness excludes only the predecessor; every other row retains its keys.
    pub async fn server_validate_upsert(
        &self,
        shard: &QueueKey,
        definition: &QueueDefinition,
        item: &fireweed_engine::PushItem,
        replaced: Option<ItemId>,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        if shard.tenant_id != definition.tenant_id || shard.queue_id != definition.queue_id {
            return Err(EngineError::Invalid("upsert definition queue mismatch"));
        }
        let spec = fireweed_engine::PushSpec {
            client_item_key: Some(item.client_item_key.clone()),
            priority: item.priority.clone(),
            not_before: item.not_before,
            group_key: item.group_key.clone(),
            payload: item.payload.clone(),
            fields: item.fields.clone(),
            metadata: item.metadata.clone(),
            cohort_size: item.cohort_size,
            gate_keys: item.gate_keys.clone(),
            index_fields: item.index_fields.clone(),
            entity: item.entity_document.clone(),
        };
        fireweed_engine::validate_push_shape(definition, std::slice::from_ref(&spec))?;
        let schema = definition
            .entity_schema
            .as_ref()
            .and_then(|descriptor| descriptor.entity_schema.as_ref())
            .map(fireweed_engine::compile_entity_schema)
            .transpose()?;
        fireweed_engine::validate_entity(schema.as_ref(), item.entity_document.as_ref())?;
        if fireweed_engine::AsyncProjectionStore::pause_blocks_intake(self, shard.clone()).await? {
            return Err(EngineError::Paused { drain_intake: true });
        }
        let Some(replaced) = replaced else {
            return fireweed_engine::AsyncProjectionStore::validate_push(
                self,
                shard.clone(),
                vec![item.clone()],
                now,
            )
            .await;
        };
        let mut image = self.mutation_queue_image(shard).await?;
        let predecessor = image
            .items
            .iter()
            .find(|record| record.item_id == replaced)
            .ok_or(EngineError::NotFound)?;
        if predecessor.superseded {
            return Err(EngineError::Superseded);
        }
        if predecessor.state.is_terminal() {
            return Err(EngineError::Terminal);
        }
        if predecessor.state != fireweed_core::ItemState::Pending {
            return Err(EngineError::Invalid("collision with claimed item"));
        }
        if predecessor.fenced {
            return Err(EngineError::StaleLease);
        }
        if predecessor.client_item_key != item.client_item_key
            || image
                .items
                .iter()
                .any(|record| record.item_id == item.item_id)
        {
            return Err(EngineError::Conflict);
        }
        image.items.retain(|record| record.item_id != replaced);
        if let (Some(max), Some(group)) =
            (definition.max_eligible_group_size, item.group_key.as_ref())
        {
            let occupied = image
                .items
                .iter()
                .filter(|record| {
                    !record.superseded
                        && record.group_key.as_ref() == Some(group)
                        && matches!(
                            record.state,
                            fireweed_core::ItemState::Pending | fireweed_core::ItemState::Leased
                        )
                })
                .count() as u64;
            if occupied >= max {
                return Err(EngineError::Conflict);
            }
        }
        if let Some(group) = item.group_key.as_ref()
            && item.cohort_size.is_some()
            && image.items.iter().any(|record| {
                !record.superseded
                    && record.group_key.as_ref() == Some(group)
                    && !record.state.is_terminal()
                    && record.cohort_size != item.cohort_size
            })
        {
            return Err(EngineError::Conflict);
        }
        // The push validator consumes native index_fields directly. Removing the predecessor from
        // this scratch image gives replace semantics without translating opaque or native keys.
        let projection = ProjectionData::from_image(definition, image)?;
        projection.index_validate_push(std::slice::from_ref(item))
    }

    /// Count terminal records awaiting change-record emission from their persisted terminal
    /// positions. The caller supplies the emission cursor; this read never consults the log tail.
    pub async fn server_terminal_emission_metrics_at(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&fireweed_engine::CommandPosition>,
    ) -> EngineResult<fireweed_engine::TerminalEmissionMetrics> {
        if !emit_change_records {
            let metrics = self.server_metrics(shard).await?;
            return Ok(fireweed_engine::TerminalEmissionMetrics {
                resident_terminal_count: metrics.resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            });
        }
        let mut params = queue_params(shard);
        let behind = if let Some(cursor) = emission_cursor {
            if &cursor.queue != shard {
                return Err(EngineError::Invalid("emission cursor queue mismatch"));
            }
            params.extend([
                i64::try_from(cursor.backend_epoch).map_err(storage)?.into(),
                i64::try_from(cursor.sequence).map_err(storage)?.into(),
            ]);
            "terminal_command_epoch IS NOT NULL AND (terminal_command_epoch>?3 \
                OR (terminal_command_epoch=?3 AND last_command_sequence>?4))"
        } else {
            "terminal_command_epoch IS NOT NULL"
        };
        let connection = self.reader.lock().await;
        let rows = rows_on(
            &connection,
            &format!(
                "SELECT COUNT(*),SUM(CASE WHEN {behind} THEN 1 ELSE 0 END),\
             MIN(CASE WHEN {behind} THEN terminal_at ELSE NULL END) \
             FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND lifecycle_state IN ('Complete','Failed') AND superseded=0"
            ),
            params,
        )
        .await?;
        let row = rows
            .first()
            .ok_or_else(|| storage("terminal aggregate returned no row"))?;
        let count = |value: &Value| {
            if matches!(value, Value::Null) {
                Ok(0)
            } else {
                u64::try_from(integer(value)?).map_err(storage)
            }
        };
        let age = if matches!(row[2], Value::Null) {
            0
        } else {
            // Match the shared evaluator's floor-to-milliseconds before subtracting, including
            // negative timestamps and clock skew. Subtracting nanoseconds before flooring differs.
            let now_ms = i128::from(now.seconds) * 1_000 + i128::from(now.nanoseconds / 1_000_000);
            let terminal_ms = i128::from(integer(&row[2])?.div_euclid(1_000_000));
            now_ms
                .saturating_sub(terminal_ms)
                .clamp(0, i128::from(u64::MAX)) as u64
        };
        Ok(fireweed_engine::TerminalEmissionMetrics {
            resident_terminal_count: count(&row[0])?,
            emission_lag_commands: count(&row[1])?,
            emission_oldest_unemitted_age_ms: age,
        })
    }

    pub async fn server_range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        let result = range_scan_on(&transaction, shard, &definition, request).await?;
        transaction.commit().await.map_err(storage)?;
        Ok(result)
    }

    pub async fn server_index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.index_lookup_native(shard, index, key, false).await
    }

    pub async fn server_index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        Ok(self
            .index_lookup_native(shard, index, key, true)
            .await?
            .into_iter()
            .next())
    }

    async fn index_lookup_native(
        &self,
        shard: &QueueKey,
        name: &str,
        key: &[Vec<u8>],
        unique: bool,
    ) -> EngineResult<Vec<IndexHit>> {
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        let canonical = if let Some(spec) = definition
            .secondary_indexes
            .iter()
            .find(|spec| spec.name == name)
        {
            if unique && !spec.unique {
                return Err(EngineError::Invalid("secondary index is not unique"));
            }
            if key.len() != spec.fields.len() {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            key.iter()
                .flat_map(|value| frame(value))
                .collect::<Vec<_>>()
        } else {
            let spec = index_spec(&definition, Some(name))?;
            if unique && !index_is_unique(spec) {
                return Err(EngineError::Invalid("secondary index is not unique"));
            }
            if key.len() != index_fields(spec).len() {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            typed_lookup_canonical_key(spec, key)?
        };
        let mut params = queue_params(shard);
        params.extend([name.to_owned().into(), canonical.into()]);
        let suffix = if unique {
            "LIMIT 1"
        } else {
            "ORDER BY length(idx.item_id),idx.item_id"
        };
        let data = rows_on(&transaction, &format!("SELECT i.item_id,i.client_item_key,i.item_version \
            FROM fireweed_item_index idx INDEXED BY fireweed_item_index_key_numeric_asc_idx \
            CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 \
            ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id AND i.item_id=idx.item_id \
            WHERE idx.tenant_id=?1 AND idx.queue_id=?2 AND idx.index_name=?3 AND idx.index_key=?4 AND i.superseded=0 {suffix}"), params).await?;
        let result = data
            .into_iter()
            .map(|row| {
                Ok(IndexHit {
                    item_id: ItemId::new(text(&row[0])?).map_err(storage)?,
                    client_item_key: ClientItemKey::new(text(&row[1])?).map_err(storage)?,
                    item_version: u64::try_from(integer(&row[2])?).map_err(storage)?,
                })
            })
            .collect::<EngineResult<_>>()?;
        transaction.commit().await.map_err(storage)?;
        Ok(result)
    }

    pub async fn server_discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ActiveScope>> {
        let connection = self.reader.lock().await;
        let mut params = queue_params(shard);
        params.push(ts_nanos(now).into());
        let rows = rows_on(&connection, &format!("SELECT i.group_key,MIN(i.eligible_since),COUNT(*) \
            FROM fireweed_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.lifecycle_state='Pending' \
            AND i.superseded=0 AND i.eligible_since IS NOT NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
            AND {UNBLOCKED} GROUP BY i.group_key ORDER BY MIN(i.eligible_since),(i.group_key IS NOT NULL),i.group_key"), params).await?;
        let scopes = rows
            .into_iter()
            .map(|row| {
                Ok(ActiveScope {
                    queue_id: shard.queue_id.as_str().to_owned(),
                    group_key: match &row[0] {
                        Value::Null => None,
                        value => Some(text(value)?.to_owned()),
                    },
                    oldest_eligible_age_ms: ts_nanos(now).saturating_sub(integer(&row[1])?).max(0)
                        as u64
                        / 1_000_000,
                    eligible_count: Some(u64::try_from(integer(&row[2])?).map_err(storage)?),
                    progress_bound_risk_count: None,
                })
            })
            .collect::<EngineResult<_>>()?;
        Ok(fireweed_engine::project_scopes(scopes, granularity))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Mirrors the projection claim-selection port"
    )]
    pub async fn server_select_claim_by_query(
        &self,
        shard: &QueueKey,
        index: Option<&str>,
        filters: &[QueryFilter],
        order_by: &OrderField,
        max_items: usize,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ItemId>> {
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, paused) = definition_on(&transaction, shard).await?;
        let spec = index_spec(&definition, index)?;
        let mut order = orders(spec, std::slice::from_ref(order_by))?;
        let mut query = IndexedSql::new(shard, spec, filters)?;
        if let Some(direction) =
            query.native_order(spec, filters, std::slice::from_ref(order_by))?
        {
            order = native_order_sql(direction).into();
        }
        if paused || max_items == 0 {
            return Ok(Vec::new());
        }
        let now_bind = query.bind(ts_nanos(now));
        query.predicates.extend([
            "i.lifecycle_state='Pending'".into(),
            "i.fenced=0".into(),
            "i.cohort_size IS NULL".into(),
            "i.retry_count<i.max_attempts".into(),
            format!("(i.not_before IS NULL OR i.not_before<={now_bind})"),
            UNBLOCKED.into(),
        ]);
        let limit = query.bind(
            i64::try_from(max_items)
                .map_err(|_| EngineError::Invalid("invalid claim_by_query max_items"))?,
        );
        let sql = query.select("i.item_id", &format!("ORDER BY {order} LIMIT {limit}"));
        let rows = rows_on(&transaction, &sql, query.params).await?;
        let result = rows
            .into_iter()
            .map(|row| ItemId::new(text(&row[0])?).map_err(storage))
            .collect::<EngineResult<_>>()?;
        transaction.commit().await.map_err(storage)?;
        Ok(result)
    }

    pub async fn server_classify_claim_by_item_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<(ItemId, ClaimByItemIdClass)>> {
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (_, paused) = definition_on(&transaction, shard).await?;
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let mut params = queue_params(shard);
            params.push(id.to_string().into());
            let rows = rows_on(&transaction, &format!("SELECT lifecycle_state,superseded,fenced,not_before,cohort_size,retry_count,max_attempts,{UNBLOCKED} \
                FROM fireweed_items i WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3"), params).await?;
            let class = if let Some(row) = rows.first() {
                let state = text(&row[0])?;
                if integer(&row[1])? != 0 {
                    ClaimByItemIdClass::NotFound
                } else if matches!(state, "Complete" | "Failed") {
                    ClaimByItemIdClass::Terminal
                } else if state == "Leased" {
                    ClaimByItemIdClass::Leased
                } else if paused
                    || state != "Pending"
                    || integer(&row[2])? != 0
                    || !matches!(row[4], Value::Null)
                    || integer(&row[5])? >= integer(&row[6])?
                    || (!matches!(row[3], Value::Null) && integer(&row[3])? > ts_nanos(now))
                    || integer(&row[7])? == 0
                {
                    ClaimByItemIdClass::NotEligible
                } else {
                    ClaimByItemIdClass::Claimable
                }
            } else {
                ClaimByItemIdClass::NotFound
            };
            result.push((*id, class));
        }
        transaction.commit().await.map_err(storage)?;
        Ok(result)
    }

    #[doc(hidden)]
    pub async fn server_query_claim_replay(
        &self,
        shard: &QueueKey,
        operation: &str,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<serde_json::Value>> {
        let connection = self.reader.lock().await;
        let mut params = queue_params(shard);
        params.extend([
            operation.to_owned().into(),
            request_id.as_str().to_owned().into(),
        ]);
        let rows = rows_on(&connection, "SELECT request_fingerprint,response_payload,expires_at FROM fireweed_request_idempotency \
            WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4", params).await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        if blob(&row[0])? != fingerprint.to_be_bytes() {
            return Err(EngineError::RequestIdConflict);
        }
        if integer(&row[2])? <= ts_nanos(now) {
            return Err(EngineError::RequestExpired);
        }
        serde_json::from_str(text(&row[1])?)
            .map(Some)
            .map_err(storage)
    }
}

fn matches_fields(
    spec: &QueueIndex,
    fields: &BTreeMap<String, TypedValue>,
    filters: &[QueryFilter],
) -> EngineResult<bool> {
    let declared = index_fields(spec);
    for filter in filters {
        let at = position(spec, &filter.field)?;
        let expected = encode(&filter.value, declared[at].1)?;
        let Some(value) = fields.get(&filter.field) else {
            return Ok(false);
        };
        let actual = encode(value, declared[at].1)?;
        let comparison = actual.cmp(&expected);
        if !match filter.op {
            FilterOp::Eq => comparison.is_eq(),
            FilterOp::Gt => comparison.is_gt(),
            FilterOp::Gte => comparison.is_ge(),
            FilterOp::Lt => comparison.is_lt(),
            FilterOp::Lte => comparison.is_le(),
        } {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Aggregates stream canonical keys, grouped by identical key where possible. Sparse keys need only
/// their compact indexed-field map (or legacy entity), never item payloads or a whole queue image.
/// The sparse branch is necessary: a compound key is absent if *any* declared component is null.
async fn visit_aggregate_fields(
    connection: &Connection,
    shard: &QueueKey,
    definition: &QueueDefinition,
    spec: &QueueIndex,
    filters: &[QueryFilter],
    mut visit: impl FnMut(BTreeMap<String, TypedValue>, u64) -> EngineResult<()>,
) -> EngineResult<()> {
    let query = IndexedSql::new(shard, spec, filters)?;
    let sql = query.select("k.index_key,COUNT(*)", "GROUP BY k.index_key");
    let mut rows = connection
        .query(&sql, query.params)
        .await
        .map_err(storage)?;
    while let Some(row) = rows.next().await.map_err(storage)? {
        let key = row.get::<Vec<u8>>(0).map_err(storage)?;
        let count = u64::try_from(row.get::<i64>(1).map_err(storage)?).map_err(storage)?;
        visit(decode_fields(spec, &key)?, count)?;
    }
    drop(rows);
    visit_sparse_fields(connection, shard, definition, spec, filters, |fields, _| {
        visit(fields, 1)
    })
    .await
}

async fn visit_sparse_fields(
    connection: &Connection,
    shard: &QueueKey,
    definition: &QueueDefinition,
    spec: &QueueIndex,
    filters: &[QueryFilter],
    mut visit: impl FnMut(BTreeMap<String, TypedValue>, &str) -> EngineResult<()>,
) -> EngineResult<()> {
    // A filter on every component makes a missing canonical key unable to match. In that common
    // case (single-field metrics and fully constrained compound queries), no sparse scan is needed.
    if index_fields(spec)
        .iter()
        .all(|(name, _)| filters.iter().any(|filter| filter.field == *name))
    {
        return Ok(());
    }
    let mut params = queue_params(shard);
    params.push(spec.name.clone().into());
    let mut rows = connection
        .query(
            "SELECT i.index_fields,i.entity_document,i.lifecycle_state FROM fireweed_items i \
        WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.superseded=0 \
        AND NOT EXISTS (SELECT 1 FROM fireweed_item_index idx WHERE idx.tenant_id=i.tenant_id \
            AND idx.queue_id=i.queue_id AND idx.index_name=?3 AND idx.item_id=i.item_id)",
            params,
        )
        .await
        .map_err(storage)?;
    while let Some(row) = rows.next().await.map_err(storage)? {
        let native = row.get_value(0).map_err(storage)?;
        let mut fields = decode_index_fields_blob(match &native {
            Value::Null => None,
            value => Some(blob(value)?),
        })?;
        if fields.is_empty() {
            let entity = row.get_value(1).map_err(storage)?;
            if !matches!(entity, Value::Null) {
                fields = extract_index_fields_from_entity(
                    &definition.typed_indexes,
                    &serde_json::from_str(text(&entity)?).map_err(storage)?,
                )?;
            }
        }
        if matches_fields(spec, &fields, filters)? {
            visit(fields, &row.get::<String>(2).map_err(storage)?)?;
        }
    }
    Ok(())
}

fn accumulate_group(
    request: &GroupedAggregateRequest,
    fields: &BTreeMap<String, TypedValue>,
    count: u64,
    groups: &mut BTreeMap<String, AggregateGroup>,
) -> EngineResult<()> {
    let mut key = BTreeMap::new();
    for group in &request.group_by {
        let Some(value) = fields.get(&group.field) else {
            return Ok(());
        };
        let value = match (group.time_bucket, value) {
            (Some(bucket), TypedValue::DateTime(timestamp)) => {
                let width = if bucket == TimeBucket::Hour {
                    3_600
                } else {
                    86_400
                };
                TypedValue::DateTime(UtcTimestamp {
                    seconds: timestamp.seconds.div_euclid(width) * width,
                    nanoseconds: 0,
                })
            }
            (Some(_), _) => return Err(EngineError::Invalid("unsupported time bucket")),
            (None, value) => value.clone(),
        };
        key.insert(group.field.clone(), value);
    }
    let serialized = serde_json::to_string(&key).map_err(storage)?;
    if !groups.contains_key(&serialized) && groups.len() >= request.max_groups as usize {
        return Err(EngineError::Invalid("aggregate-too-large"));
    }
    let group = groups
        .entry(serialized)
        .or_insert(AggregateGroup { key, count: 0 });
    group.count = group
        .count
        .checked_add(count)
        .ok_or_else(|| storage("aggregate count overflow"))?;
    Ok(())
}

fn bucket_match(value: f64, rule: &fireweed_core::BucketRule) -> bool {
    if let Some(exact) = rule.exact {
        return value == exact;
    }
    rule.gt.is_none_or(|bound| value > bound)
        && rule.gte.is_none_or(|bound| value >= bound)
        && rule.lt.is_none_or(|bound| value < bound)
        && rule.lte.is_none_or(|bound| value <= bound)
}

impl TursoRelational {
    pub async fn server_grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        if request.group_by.is_empty() {
            return Err(EngineError::Invalid("group-by required"));
        }
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        let spec = index_spec(&definition, request.index.as_deref())?;
        let fields = index_fields(spec);
        for group in &request.group_by {
            let at = position(spec, &group.field)?;
            if group.time_bucket.is_some() && !matches!(fields[at].1, IndexType::Datetime) {
                return Err(EngineError::Invalid("unsupported time bucket"));
            }
        }
        let mut groups = BTreeMap::new();
        visit_aggregate_fields(
            &transaction,
            shard,
            &definition,
            spec,
            &request.filters,
            |fields, count| accumulate_group(&request, &fields, count, &mut groups),
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(GroupedAggregateResponse {
            groups: groups.into_values().collect(),
        })
    }

    pub async fn server_declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        request
            .validate(MAX_PAGE as u32)
            .map_err(|_| EngineError::Invalid("invalid request"))?;
        if request
            .buckets
            .iter()
            .flat_map(|bucket| [bucket.exact, bucket.gt, bucket.gte, bucket.lt, bucket.lte])
            .flatten()
            .any(|bound| !bound.is_finite())
        {
            return Err(EngineError::Invalid("invalid request"));
        }
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        let spec = index_spec(&definition, request.index.as_deref())?;
        let at = position(spec, &request.field)?;
        if !matches!(
            index_fields(spec)[at].1,
            IndexType::Integer | IndexType::Float
        ) {
            return Err(EngineError::Invalid("unsupported bucket field"));
        }
        let mut counts = vec![0_u64; request.buckets.len() + 1];
        visit_aggregate_fields(
            &transaction,
            shard,
            &definition,
            spec,
            &request.filters,
            |fields, count| {
                let value = match fields.get(&request.field) {
                    Some(TypedValue::Integer(value)) => Some(*value as f64),
                    Some(TypedValue::Float(value)) => Some(*value),
                    None => None,
                    _ => return Err(EngineError::Invalid("unsupported bucket field")),
                };
                let bucket = match value {
                    Some(value) => request
                        .buckets
                        .iter()
                        .position(|rule| bucket_match(value, rule)),
                    None => Some(request.buckets.len()),
                };
                if let Some(bucket) = bucket {
                    counts[bucket] = counts[bucket]
                        .checked_add(count)
                        .ok_or_else(|| storage("bucket count overflow"))?;
                }
                Ok(())
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        let labels = request
            .buckets
            .into_iter()
            .map(|bucket| bucket.label)
            .chain(std::iter::once(request.null_bucket_label));
        Ok(DeclaredBucketSegmentResponse {
            buckets: labels
                .zip(counts)
                .map(|(label, count)| BucketCount { label, count })
                .collect(),
        })
    }

    pub async fn server_metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        if request.filters.is_empty() {
            if request.index.is_some() {
                index_spec(&definition, request.index.as_deref())?;
            }
            let metrics = crate::projection::server_metrics_on(&transaction, shard).await?;
            transaction.commit().await.map_err(storage)?;
            return Ok(metrics);
        }
        let spec = index_spec(&definition, request.index.as_deref())?;
        let query = IndexedSql::new(shard, spec, &request.filters)?;
        let sql = query.select(
            "SUM(CASE WHEN i.lifecycle_state='Pending' THEN 1 ELSE 0 END),\
            SUM(CASE WHEN i.lifecycle_state='Leased' THEN 1 ELSE 0 END),\
            SUM(CASE WHEN i.lifecycle_state='Complete' THEN 1 ELSE 0 END),\
            SUM(CASE WHEN i.lifecycle_state='Failed' THEN 1 ELSE 0 END)",
            "",
        );
        let rows = rows_on(&transaction, &sql, query.params).await?;
        let row = rows
            .first()
            .ok_or_else(|| storage("metrics aggregate returned no row"))?;
        let count = |value: &Value| {
            if matches!(value, Value::Null) {
                Ok(0)
            } else {
                u64::try_from(integer(value)?).map_err(storage)
            }
        };
        let mut metrics = QueueMetrics {
            pending: count(&row[0])?,
            leased: count(&row[1])?,
            complete: count(&row[2])?,
            failed: count(&row[3])?,
            ..QueueMetrics::default()
        };
        visit_sparse_fields(
            &transaction,
            shard,
            &definition,
            spec,
            &request.filters,
            |_, state| {
                let count = match state {
                    "Pending" => &mut metrics.pending,
                    "Leased" => &mut metrics.leased,
                    "Complete" => &mut metrics.complete,
                    "Failed" => &mut metrics.failed,
                    _ => return Err(storage("invalid item lifecycle state")),
                };
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| storage("metrics count overflow"))?;
                Ok(())
            },
        )
        .await?;
        metrics.resident_terminal_count = metrics.complete + metrics.failed;
        transaction.commit().await.map_err(storage)?;
        Ok(metrics)
    }

    /// Select bounded pages through the declared index, then decode only those current rows. The
    /// caller must retain its queue mutation fence across this plan and log append; every update also
    /// carries the observed item version. This method never writes projection state.
    pub async fn server_plan_bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationPlan> {
        if request.max_scan_rows == 0 {
            return Err(EngineError::Invalid("invalid page size"));
        }
        let mut connection = self.reader.lock().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await
            .map_err(storage)?;
        let (definition, _) = definition_on(&transaction, shard).await?;
        let spec = index_spec(&definition, request.index.as_deref())?;
        // Validate even an empty candidate set, including undeclared filters and value types.
        IndexedSql::new(shard, spec, &request.filters)?;
        let compiled = definition
            .entity_schema
            .as_ref()
            .and_then(|schema| schema.entity_schema.as_ref())
            .map(fireweed_engine::compile_entity_schema)
            .transpose()?;
        let page_size = (request.max_scan_rows as usize).min(MAX_PAGE);
        let mut after: Option<ItemId> = None;
        let mut results = Vec::new();
        let mut updates = Vec::new();
        let mut reservations = BTreeMap::<(String, Vec<u8>), ItemId>::new();
        loop {
            let mut query = IndexedSql::new(shard, spec, &request.filters)?;
            if let Some(id) = after {
                let bind = query.bind(id.to_string());
                query.predicates.push(format!("(length(i.item_id)>length({bind}) OR (length(i.item_id)=length({bind}) AND i.item_id>{bind}))"));
            }
            let limit = query.bind(page_size as i64);
            let sql = query.select(
                "i.item_id",
                &format!("ORDER BY length(i.item_id),i.item_id LIMIT {limit}"),
            );
            let rows = rows_on(&transaction, &sql, query.params).await?;
            let ids = rows
                .iter()
                .map(|row| ItemId::new(text(&row[0])?).map_err(storage))
                .collect::<EngineResult<Vec<_>>>()?;
            if ids.is_empty() {
                break;
            }
            after = ids.last().copied();
            let items =
                crate::projection::load_mutation_items_on(&transaction, shard, &ids, false).await?;
            let mut page = BoundedMutationPlan {
                response: BoundedMutationResponse {
                    results: Vec::with_capacity(items.len()),
                },
                updates: Vec::new(),
            };
            // Plan each candidate independently. Existing unique holders (including another item
            // in this page) are checked below against SQL, producing per-record Conflict instead of
            // making max_scan_rows change a collision into a whole-request planner error.
            for item in items {
                let image = ProjectionImage {
                    high_water: None,
                    paused: false,
                    pause_drain_intake: false,
                    blocked_gates: BTreeSet::new(),
                    next_seq: 0,
                    items: vec![item],
                    side_records: BTreeMap::new(),
                    instance_fences: BTreeMap::new(),
                    metrics: QueueMetrics::default(),
                };
                let projection = ProjectionData::from_image(&definition, image)?;
                let candidate = projection.plan_bounded_mutation(request.clone())?;
                page.response.results.extend(candidate.response.results);
                page.updates.extend(candidate.updates);
            }
            for update in page.updates {
                fireweed_engine::validate_entity(
                    compiled.as_ref(),
                    update.command.set_entity_document.as_ref(),
                )?;
                let empty_fields = BTreeMap::new();
                let keys = fireweed_relational::secondary_index_keys(
                    &definition,
                    update.command.set_fields.as_ref().unwrap_or(&empty_fields),
                    &BTreeMap::new(),
                    update.command.set_entity_document.as_ref(),
                )?;
                let mut unique_keys = Vec::new();
                let mut conflict = false;
                for (name, key) in keys {
                    if !fireweed_relational::secondary_index_is_unique(&definition, &name) {
                        continue;
                    }
                    if reservations
                        .get(&(name.clone(), key.clone()))
                        .is_some_and(|id| *id != update.command.item_id)
                    {
                        conflict = true;
                        break;
                    }
                    let mut params = queue_params(shard);
                    params.extend([
                        name.clone().into(),
                        key.clone().into(),
                        update.command.item_id.to_string().into(),
                    ]);
                    if !rows_on(&transaction, "SELECT item_id FROM fireweed_item_index INDEXED BY fireweed_item_index_key_numeric_asc_idx \
                        WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 AND item_id<>?5 LIMIT 1", params).await?.is_empty() {
                        conflict = true; break;
                    }
                    unique_keys.push((name, key));
                }
                if conflict {
                    if let Some(result) = page
                        .response
                        .results
                        .iter_mut()
                        .find(|result| result.item_id == update.command.item_id)
                    {
                        result.outcome = MutationOutcome::Conflict;
                    }
                } else {
                    for key in unique_keys {
                        reservations.insert(key, update.command.item_id);
                    }
                    updates.push(update);
                }
            }
            results.extend(page.response.results);
            if ids.len() < page_size {
                break;
            }
        }
        transaction.commit().await.map_err(storage)?;
        Ok(BoundedMutationPlan {
            response: BoundedMutationResponse { results },
            updates,
        })
    }
}
