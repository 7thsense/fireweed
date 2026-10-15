//! Constant-depth indexed filters shared by native claims and queries.
use super::*;
use fireweed_engine::queue_index::scalar_type_prefix;
/// One bound byte pool keeps key/IN sets independent of native parameter caps.
/// Only numeric slices and fixed SQL vocabulary enter statement text.
#[derive(Default)]
pub(super) struct BytePool {
    pub(super) literal_bytes: Vec<u8>,
    metadata: NumericMetadata,
}
#[derive(Default)]
struct NumericMetadata {
    probes: Option<Vec<[Option<i64>; 25]>>,
    dimensions: Option<Vec<[Option<i64>; 6]>>,
    key_hashes: Option<Vec<[i64; 2]>>,
    keys: Option<Vec<[i64; 2]>>,
}
impl BytePool {
    fn span(&mut self, value: &[u8]) -> QueueResult<(i64, i64)> {
        let start = i64::try_from(self.literal_bytes.len())
            .map_err(|_| QueueError::TooLarge)?
            .checked_add(1)
            .ok_or(QueueError::TooLarge)?;
        let len = i64::try_from(value.len()).map_err(|_| QueueError::TooLarge)?;
        start.checked_add(len).ok_or(QueueError::TooLarge)?;
        self.literal_bytes.extend_from_slice(value);
        Ok((start, len))
    }
    pub(super) fn bytes(&mut self, value: &[u8]) -> QueueResult<String> {
        let (start, len) = self.span(value)?;
        Ok(format!("substr(?1,{start},{len})"))
    }
    /// Bucket scalars use a separate small binding: repeated aggregate
    /// expressions must never copy the potentially wide filter literal pool.
    pub(super) fn bucket_bytes(&mut self, value: &[u8]) -> QueueResult<String> {
        let (start, len) = self.span(value)?;
        Ok(format!("substr(?3,{start},{len})"))
    }
    fn list(
        &mut self,
        values: impl IntoIterator<Item = Vec<u8>>,
        hashes: bool,
    ) -> QueueResult<String> {
        let rows = values
            .into_iter()
            .map(|value| {
                let (start, len) = self.span(&value)?;
                Ok([start, len])
            })
            .collect::<QueueResult<Vec<_>>>()?;
        if rows.is_empty() {
            return Ok("SELECT NULL WHERE 0".into());
        }
        let (slot, path) = if hashes {
            (&mut self.metadata.key_hashes, "$[2]")
        } else {
            (&mut self.metadata.keys, "$[3]")
        };
        if slot.replace(rows).is_some() {
            return Err(unknown("duplicate key metadata"));
        }
        Ok(format!(
            "SELECT substr(?1,json_extract(j.value,'$[0]'),json_extract(j.value,'$[1]')) FROM json_each(?2,'{path}') j"
        ))
    }
    fn has_metadata(&self) -> bool {
        self.metadata.probes.is_some()
            || self.metadata.dimensions.is_some()
            || self.metadata.key_hashes.is_some()
            || self.metadata.keys.is_some()
    }
    fn encoded_metadata(&self) -> QueueResult<String> {
        serde_json::to_string(&(
            &self.metadata.probes,
            &self.metadata.dimensions,
            &self.metadata.key_hashes,
            &self.metadata.keys,
        ))
        .map_err(unknown)
    }
    pub(super) fn params(&self) -> QueueResult<Vec<Value>> {
        if self.has_metadata() {
            Ok(vec![
                Value::Blob(self.literal_bytes.clone()),
                Value::Text(self.encoded_metadata()?),
            ])
        } else if self.literal_bytes.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![Value::Blob(self.literal_bytes.clone())])
        }
    }
    pub(super) fn into_params(self) -> QueueResult<Vec<Value>> {
        if self.has_metadata() {
            let metadata = self.encoded_metadata()?;
            Ok(vec![Value::Blob(self.literal_bytes), Value::Text(metadata)])
        } else if self.literal_bytes.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![Value::Blob(self.literal_bytes)])
        }
    }
    pub(super) fn into_scalar_bytes(self) -> QueueResult<Vec<u8>> {
        if self.has_metadata() {
            return Err(unknown("numeric metadata omitted from scalar binding"));
        }
        Ok(self.literal_bytes)
    }
    pub(super) fn dimensions<'a>(
        &mut self,
        names: impl Iterator<Item = &'a str>,
        definition: &CollectionDefinition,
    ) -> QueueResult<String> {
        let mut rows = Vec::new();
        for (n, name) in names.enumerate() {
            let field = field_number(definition, name)?;
            let mut row = [None; 6];
            row[0] = Some(i64::try_from(n).map_err(|_| QueueError::TooLarge)?);
            row[1] = Some(i64::from(field));
            if field < 0 {
                let (start, len) = self.span(&discovery_digest(name.as_bytes()))?;
                row[2] = Some(start);
                row[3] = Some(len);
                let (start, len) = self.span(name.as_bytes())?;
                row[4] = Some(start);
                row[5] = Some(len);
            }
            rows.push(row);
        }
        if self.metadata.dimensions.replace(rows).is_some() {
            return Err(unknown("duplicate grouping metadata"));
        }
        Ok("group_dims AS MATERIALIZED (SELECT json_extract(j.value,'$[0]') AS n,json_extract(j.value,'$[1]') AS field,substr(?1,json_extract(j.value,'$[2]'),json_extract(j.value,'$[3]')) AS name_hash,substr(?1,json_extract(j.value,'$[4]'),json_extract(j.value,'$[5]')) AS name FROM json_each(?2,'$[1]') j)".into())
    }
}
/// Only numeric offsets appear per probe. Repeating substr(?1,...) in every
/// VALUES row makes Turso retain a full bound-pool copy per parameter reference.
/// Project the ten byte columns once over this relation instead.
struct ProbeRow {
    n: usize,
    field: i32,
    kind: u8,
    lower_mode: u8,
    upper_mode: u8,
    spans: [Option<(i64, i64)>; 10],
}
impl ProbeRow {
    fn metadata(&self) -> QueueResult<[Option<i64>; 25]> {
        let mut row = [None; 25];
        row[0] = Some(i64::try_from(self.n).map_err(|_| QueueError::TooLarge)?);
        row[1] = Some(i64::from(self.field));
        row[2] = Some(i64::from(self.kind));
        row[3] = Some(i64::from(self.lower_mode));
        row[4] = Some(i64::from(self.upper_mode));
        for (index, span) in self.spans.iter().enumerate() {
            if let Some((start, len)) = span {
                row[5 + index * 2] = Some(*start);
                row[6 + index * 2] = Some(*len);
            }
        }
        Ok(row)
    }
}
fn probe_rows(
    filter: &Filter,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<Vec<ProbeRow>> {
    let mut rows = Vec::new();
    for (n, (name, predicate)) in filter.fields.iter().enumerate() {
        let field = field_number(definition, name)?;
        let mut identity = [None; 10];
        if field < 0 {
            identity[0] = Some(pool.span(&discovery_digest(name.as_bytes()))?);
            identity[1] = Some(pool.span(name.as_bytes())?);
        }
        let mut equality = |value: &FieldValue| -> QueueResult<()> {
            let key = ordered_key(&[Some(value)])?;
            let mut spans = identity;
            spans[2] = Some(pool.span(&discovery_digest(&key))?);
            spans[3] = Some(pool.span(&key)?);
            rows.push(ProbeRow {
                n,
                field,
                kind: 0,
                lower_mode: 0,
                upper_mode: 0,
                spans,
            });
            Ok(())
        };
        match predicate {
            Predicate::Equal(value) => equality(value)?,
            Predicate::In(values) => {
                for value in values {
                    equality(value)?;
                }
            }
            Predicate::Range { lower, upper } => {
                let first = lower
                    .as_ref()
                    .or(upper.as_ref())
                    .ok_or_else(|| QueueError::Invalid("empty range".into()))?;
                let value = match first {
                    Bound::Included(value) | Bound::Excluded(value) => value,
                };
                let floor = scalar_type_prefix(value.field_type());
                let mut ceiling = floor;
                ceiling[2] += 1;
                let mut spans = identity;
                spans[4] = Some(pool.span(&floor)?);
                spans[5] = Some(pool.span(&ceiling)?);
                let mut modes = [0; 2];
                for (index, bound) in [lower, upper].into_iter().enumerate() {
                    if let Some(bound) = bound {
                        let (value, mode) = match bound {
                            Bound::Included(value) => (value, 1),
                            Bound::Excluded(value) => (value, 2),
                        };
                        let key = ordered_key(&[Some(value)])?;
                        modes[index] = mode;
                        spans[6 + index * 2] = Some(pool.span(&discovery_prefix(&key))?);
                        spans[7 + index * 2] = Some(pool.span(&key)?);
                    }
                }
                rows.push(ProbeRow {
                    n,
                    field,
                    kind: 1,
                    lower_mode: modes[0],
                    upper_mode: modes[1],
                    spans,
                });
            }
        }
    }
    Ok(rows)
}
fn indexed_field_intersection(
    rows: &[ProbeRow],
    count: usize,
    queue_id: i64,
    collection: i32,
    pool: &mut BytePool,
) -> QueueResult<(String, String)> {
    if rows.is_empty() {
        return Ok((String::new(), String::new()));
    }
    let range = "f.prefix>=p.floor AND f.prefix<p.ceiling AND (p.lower_mode=0 OR (f.prefix>=p.lower_prefix AND ((p.lower_mode=1 AND f.full_key>=p.lower_key) OR (p.lower_mode=2 AND f.full_key>p.lower_key)))) AND (p.upper_mode=0 OR (f.prefix<=p.upper_prefix AND ((p.upper_mode=1 AND f.full_key<=p.upper_key) OR (p.upper_mode=2 AND f.full_key<p.upper_key))))";
    let mut branches = Vec::new();
    for (table, identity, is_declared) in [
        ("q7_fields", "f.field=p.field", true),
        (
            "q7_additional",
            "f.name_hash=p.name_hash AND f.name=p.name",
            false,
        ),
    ] {
        let no = if is_declared {
            "p.field>=0"
        } else {
            "p.field<0"
        };
        for (kind, predicate) in [(0, "f.hash=p.hash AND f.full_key=p.full_key"), (1, range)] {
            let index = match (is_declared, kind) {
                (true, 0) => "q7_field_equal",
                (true, _) => "q7_field_range",
                (false, 0) => "q7_additional_equal",
                (false, _) => "q7_additional_range",
            };
            branches.push(format!("SELECT f.record_id,p.n FROM probes p CROSS JOIN {table} f INDEXED BY {index} WHERE f.queue_id={queue_id} AND f.collection={collection} AND {identity} AND {predicate} AND {no} AND p.kind={kind}"));
        }
    }
    let byte_columns = [
        "name_hash",
        "name",
        "hash",
        "full_key",
        "floor",
        "ceiling",
        "lower_prefix",
        "lower_key",
        "upper_prefix",
        "upper_key",
    ];
    let metadata = rows
        .iter()
        .map(ProbeRow::metadata)
        .collect::<QueueResult<Vec<_>>>()?;
    if pool.metadata.probes.replace(metadata).is_some() {
        return Err(unknown("duplicate filter metadata"));
    }
    let projection = byte_columns.iter().enumerate().map(|(index,column)| {
        let start=5+index*2;let len=start+1;
        format!("substr(?1,json_extract(j.value,'$[{start}]'),json_extract(j.value,'$[{len}]')) AS {column}")
    }).collect::<Vec<_>>().join(",");
    Ok((
        format!(
            "WITH probes AS MATERIALIZED (SELECT json_extract(j.value,'$[0]') AS n,json_extract(j.value,'$[1]') AS field,json_extract(j.value,'$[2]') AS kind,json_extract(j.value,'$[3]') AS lower_mode,json_extract(j.value,'$[4]') AS upper_mode,{projection} FROM json_each(?2,'$[0]') j),matches AS ({}) ",
            branches.join(" UNION ALL ")
        ),
        format!(
            "r.id IN (SELECT record_id FROM matches GROUP BY record_id HAVING COUNT(DISTINCT n)={count})"
        ),
    ))
}

pub(super) fn filter_sql(
    filter: &Filter,
    definition: &CollectionDefinition,
    queue_id: i64,
    collection: i32,
    pool: &mut BytePool,
) -> QueueResult<(String, String)> {
    let mut conjuncts = Vec::new();
    if let Some(keys) = &filter.keys {
        let hashes = pool.list(
            keys.iter()
                .map(|key| discovery_digest(key.as_bytes()).to_vec()),
            true,
        )?;
        let keys = pool.list(keys.iter().map(|key| key.as_bytes().to_vec()), false)?;
        conjuncts.push(format!("r.key_hash IN ({hashes}) AND r.key IN ({keys})"));
    }
    let rows = probe_rows(filter, definition, pool)?;
    let (with, fields) =
        indexed_field_intersection(&rows, filter.fields.len(), queue_id, collection, pool)?;
    if !fields.is_empty() {
        conjuncts.push(fields);
    }
    Ok((
        with,
        format!(
            " AND {}",
            fireweed_engine::queue_sql::balanced_boolean(conjuncts, true)
        ),
    ))
}

/// Bound transient probe rows, composed SQL and pooled bound values before
/// constructing them, including compact membership values with large headers.
pub(super) fn filter_build_charge(filter: &Filter) -> QueueResult<u64> {
    let entries = filter.fields.values().try_fold(
        filter.keys.as_ref().map_or(0, |keys| keys.len()) as u64,
        |n, predicate| {
            n.checked_add(match predicate {
                Predicate::In(values) => values.len() as u64,
                _ => 2,
            })
            .ok_or(QueueError::TooLarge)
        },
    )?;
    entries.checked_mul(2048).ok_or(QueueError::TooLarge)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn key_list_sql_is_independent_of_cardinality_and_preserves_bytes() {
        let mut small = BytePool::default();
        let small_sql = small.list([b"a".to_vec()], false).unwrap();
        let mut wide = BytePool::default();
        let values: Vec<_> = (0..5001).map(|n| format!("é\0{n}").into_bytes()).collect();
        let wide_sql = wide.list(values.clone(), false).unwrap();
        assert_eq!(small_sql, wide_sql);
        let params = wide.into_params().unwrap();
        let (Value::Blob(bytes), Value::Text(metadata)) = (&params[0], &params[1]) else {
            panic!("typed pool bindings")
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
        assert!(metadata[0].is_null());
        assert!(metadata[1].is_null());
        assert!(metadata[2].is_null());
        assert_eq!(metadata[3].as_array().unwrap().len(), values.len());
        for (row, expected) in metadata[3].as_array().unwrap().iter().zip(values) {
            let start = usize::try_from(row[0].as_i64().unwrap() - 1).unwrap();
            let len = usize::try_from(row[1].as_i64().unwrap()).unwrap();
            assert_eq!(&bytes[start..start + len], expected);
        }
    }

    #[test]
    fn declared_dimensions_bind_metadata_with_an_empty_literal_pool() {
        let definition = CollectionDefinition {
            fields: [("declared".into(), FieldType::Integer)].into(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        };
        let mut pool = BytePool::default();
        let sql = pool
            .dimensions(["declared"].into_iter(), &definition)
            .unwrap();
        assert!(sql.contains("json_each(?2,'$[1]')"));
        let params = pool.into_params().unwrap();
        assert!(matches!(&params[0], Value::Blob(bytes) if bytes.is_empty()));
        let Value::Text(metadata) = &params[1] else {
            panic!("metadata binding")
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(
            metadata[1][0],
            serde_json::json!([0, 0, null, null, null, null])
        );
    }

    #[test]
    fn empty_key_lists_need_no_bindings_and_empty_bytes_remain_blobs() {
        let mut empty = BytePool::default();
        assert_eq!(empty.list([], false).unwrap(), "SELECT NULL WHERE 0");
        assert!(empty.into_params().unwrap().is_empty());
        let mut zero = BytePool::default();
        zero.list([Vec::new()], false).unwrap();
        let params = zero.into_params().unwrap();
        assert!(matches!(&params[0], Value::Blob(bytes) if bytes.is_empty()));
        let Value::Text(metadata) = &params[1] else {
            panic!("metadata binding")
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
        assert_eq!(metadata[3][0], serde_json::json!([1, 0]));
    }
}
