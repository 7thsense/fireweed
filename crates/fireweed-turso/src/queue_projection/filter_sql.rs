//! Constant-depth indexed filters shared by native claims and queries.
use super::*;
use fireweed_engine::queue_index::scalar_type_prefix;
/// One bound byte pool keeps key/IN sets independent of native parameter caps.
/// Only numeric slices and fixed SQL vocabulary enter statement text.
pub(super) struct BytePool(pub(super) Vec<u8>);
impl BytePool {
    pub(super) fn bytes(&mut self, value: &[u8]) -> String {
        let start = self.0.len() + 1;
        self.0.extend_from_slice(value);
        format!("substr(?1,{start},{})", value.len())
    }
    pub(super) fn list(&mut self, values: impl IntoIterator<Item = Vec<u8>>) -> String {
        let expressions = values
            .into_iter()
            .map(|value| format!("({})", self.bytes(&value)))
            .collect::<Vec<_>>();
        if expressions.is_empty() {
            "SELECT NULL WHERE 0".into()
        } else {
            format!("SELECT column1 FROM (VALUES {})", expressions.join(","))
        }
    }
}
fn probe_rows(
    filter: &Filter,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<Vec<String>> {
    let mut rows = Vec::new();
    for (id, (name, predicate)) in filter.fields.iter().enumerate() {
        let no = field_number(definition, name)?;
        let (name_hash, full_name) = if no >= 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (
                pool.bytes(&discovery_digest(name.as_bytes())),
                pool.bytes(name.as_bytes()),
            )
        };
        let identity = format!("{id},{no},{name_hash},{full_name}");
        let mut equality = |value: &FieldValue| -> QueueResult<()> {
            let key = ordered_key(&[Some(value)])?;
            let hash = pool.bytes(&discovery_digest(&key));
            let key = pool.bytes(&key);
            rows.push(format!(
                "({identity},0,{hash},{key},NULL,NULL,NULL,NULL,0,NULL,NULL,0)"
            ));
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
                let floor = pool.bytes(&floor);
                let ceiling = pool.bytes(&ceiling);
                let mut bound = |bound: &Option<Bound>| -> QueueResult<String> {
                    Ok(match bound {
                        None => "NULL,NULL,0".into(),
                        Some(bound) => {
                            let (value, mode) = match bound {
                                Bound::Included(value) => (value, 1),
                                Bound::Excluded(value) => (value, 2),
                            };
                            let key = ordered_key(&[Some(value)])?;
                            let prefix = pool.bytes(&discovery_prefix(&key));
                            let key = pool.bytes(&key);
                            format!("{prefix},{key},{mode}")
                        }
                    })
                };
                let lower = bound(lower)?;
                let upper = bound(upper)?;
                rows.push(format!(
                    "({identity},1,NULL,NULL,{floor},{ceiling},{lower},{upper})"
                ));
            }
        }
    }
    Ok(rows)
}
fn indexed_field_intersection(
    rows: &[String],
    count: usize,
    queue_id: i64,
    collection: i32,
) -> (String, String) {
    if rows.is_empty() {
        return (String::new(), String::new());
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
    (
        format!(
            "WITH probes(n,field,name_hash,name,kind,hash,full_key,floor,ceiling,lower_prefix,lower_key,lower_mode,upper_prefix,upper_key,upper_mode) AS (VALUES {}),matches AS ({}) ",
            rows.join(","),
            branches.join(" UNION ALL ")
        ),
        format!(
            "r.id IN (SELECT record_id FROM matches GROUP BY record_id HAVING COUNT(DISTINCT n)={count})"
        ),
    )
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
        );
        let keys = pool.list(keys.iter().map(|key| key.as_bytes().to_vec()));
        conjuncts.push(format!("r.key_hash IN ({hashes}) AND r.key IN ({keys})"));
    }
    let rows = probe_rows(filter, definition, pool)?;
    let (with, fields) =
        indexed_field_intersection(&rows, filter.fields.len(), queue_id, collection);
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
