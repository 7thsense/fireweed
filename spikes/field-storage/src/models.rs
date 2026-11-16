//! The four field-storage models under comparison, plus the index side table
//! shared by all of them (external-interface-kernel-contract-draft.md §3.1:
//! "Indexes are declared at queue creation"; today's shape is
//! `fireweed_item_index`, crates/fireweed-relational/src/schema.rs:257-263).
use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use turso::{Connection, Value as SqlValue};

use crate::gen::ValueTemplate;
use crate::sql::{exec_chunked, select_in_chunked};
use crate::value::field_name;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Model {
    JsonText,
    Binary,
    Eav,
    BinaryDeclared,
}

impl Model {
    pub fn name(self) -> &'static str {
        match self {
            Model::JsonText => "(a) JSON TEXT + json_patch",
            Model::Binary => "(b) postcard, String keys, RMW",
            Model::Eav => "(c) EAV side table",
            Model::BinaryDeclared => "(d) postcard, declared u16 keys, RMW",
        }
    }

    pub fn short(self) -> &'static str {
        match self {
            Model::JsonText => "json",
            Model::Binary => "binary",
            Model::Eav => "eav",
            Model::BinaryDeclared => "declared",
        }
    }
}

pub const ALL_MODELS: [Model; 4] = [
    Model::JsonText,
    Model::Binary,
    Model::Eav,
    Model::BinaryDeclared,
];

/// The one postcard-encoded scalar, mirroring the contract's `Value`
/// (external-interface-kernel-contract-draft.md:151). Every generated value
/// is `Str`, sized by the run's `value_size` (§ methodology in the results
/// doc explains why).
#[derive(Clone, Serialize, Deserialize)]
pub enum PValue {
    Str(String),
}

/// A field key a binary model's hot-row map is keyed on. `String` is model
/// (b) (dynamic field names, as the current draft's `fields: BTreeMap<String,
/// Value>` is); `u16` is model (d) (small ints assigned when the queue
/// declares its field schema).
pub trait FieldKey: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    fn from_index(i: usize) -> Self;
}
impl FieldKey for String {
    fn from_index(i: usize) -> Self {
        field_name(i)
    }
}
impl FieldKey for u16 {
    fn from_index(i: usize) -> Self {
        i as u16
    }
}

pub async fn create_schema(conn: &Connection, model: Model) -> turso::Result<()> {
    match model {
        Model::JsonText => {
            conn.execute(
                "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields TEXT NOT NULL)",
                (),
            )
            .await?;
        }
        Model::Binary | Model::BinaryDeclared => {
            conn.execute(
                "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields BLOB NOT NULL)",
                (),
            )
            .await?;
        }
        Model::Eav => {
            conn.execute(
                "CREATE TABLE eav (item_no INTEGER NOT NULL, field_no INTEGER NOT NULL, \
                 value TEXT NOT NULL, PRIMARY KEY (item_no, field_no))",
                (),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn create_index_table(conn: &Connection) -> turso::Result<()> {
    conn.execute(
        "CREATE TABLE item_index (item_no INTEGER PRIMARY KEY, index_key TEXT NOT NULL)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX item_index_key_idx ON item_index (index_key, item_no)",
        (),
    )
    .await?;
    Ok(())
}

/// Upsert the declared index's entries for the touched (item_no, key) pairs.
/// Only called when the run's config marks the update as touching the
/// indexed field (field 0).
pub async fn upsert_index_rows(conn: &Connection, rows: &[(i64, String)]) -> turso::Result<()> {
    let params: Vec<Vec<SqlValue>> = rows
        .iter()
        .map(|(id, key)| vec![SqlValue::Integer(*id), SqlValue::Text(key.clone())])
        .collect();
    exec_chunked(
        conn,
        "INSERT INTO item_index (item_no, index_key) VALUES ",
        " ON CONFLICT(item_no) DO UPDATE SET index_key = excluded.index_key",
        &params,
    )
    .await
}

// ---------------------------------------------------------------- (a) JSON

fn json_tagged_str(s: &str) -> serde_json::Value {
    serde_json::json!(["s", s])
}

pub async fn json_ingest(
    conn: &Connection,
    records: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let rows: Vec<Vec<SqlValue>> = records
        .iter()
        .map(|(id, fields)| {
            let mut obj = serde_json::Map::new();
            for (idx, v) in fields {
                obj.insert(field_name(*idx), json_tagged_str(v));
            }
            let text = serde_json::Value::Object(obj).to_string();
            vec![SqlValue::Integer(*id), SqlValue::Text(text)]
        })
        .collect();
    exec_chunked(
        conn,
        "INSERT INTO items (item_no, fields) VALUES ",
        "",
        &rows,
    )
    .await
}

/// Set-based patch: one `UPDATE ... FROM (VALUES ...)` per chunk, `json_patch`
/// doing the per-row merge in SQL (external-interface-kernel-contract-draft.md:757,
/// "[B] Field patches are a NULL-safe merge... `json_patch(fields, v.patch)`").
pub async fn json_update(
    conn: &Connection,
    updates: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let rows: Vec<Vec<SqlValue>> = updates
        .iter()
        .map(|(id, fields)| {
            let mut obj = serde_json::Map::new();
            for (idx, v) in fields {
                obj.insert(field_name(*idx), json_tagged_str(v));
            }
            let patch = serde_json::Value::Object(obj).to_string();
            vec![SqlValue::Integer(*id), SqlValue::Text(patch)]
        })
        .collect();
    // Turso's parser rejects an explicit column-alias list on a VALUES-derived
    // table (`(VALUES ...) AS v(a, b)` — probed empirically, "near \"(\":
    // syntax error"), but accepts the same construct unaliased, addressing
    // columns by its own positional names (`column1`, `column2`, ...).
    exec_chunked(
        conn,
        "UPDATE items SET fields = json_patch(fields, v.column2) FROM (VALUES ",
        ") AS v WHERE items.item_no = v.column1",
        &rows,
    )
    .await
}

// --------------------------------------------------------- (b)/(d) binary

pub async fn binary_ingest<K: FieldKey>(
    conn: &Connection,
    records: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let rows: Vec<Vec<SqlValue>> = records
        .iter()
        .map(|(id, fields)| {
            let map: BTreeMap<K, PValue> = fields
                .iter()
                .map(|(idx, v)| (K::from_index(*idx), PValue::Str(v.clone())))
                .collect();
            let blob = postcard::to_allocvec(&map).expect("postcard encode");
            vec![SqlValue::Integer(*id), SqlValue::Blob(blob)]
        })
        .collect();
    exec_chunked(
        conn,
        "INSERT INTO items (item_no, fields) VALUES ",
        "",
        &rows,
    )
    .await
}

/// Read-modify-write: one batched `SELECT ... WHERE item_no IN (...)`, decode
/// + patch + re-encode in Rust, one batched `UPDATE ... FROM (VALUES ...)`
/// write-back (external-interface-kernel-contract-draft.md task brief, model b).
pub async fn binary_update<K: FieldKey>(
    conn: &Connection,
    updates: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let ids: Vec<i64> = updates.iter().map(|(id, _)| *id).collect();
    let current = select_in_chunked(conn, "items", "item_no", "item_no, fields", &ids).await?;
    let mut by_id: BTreeMap<i64, BTreeMap<K, PValue>> = BTreeMap::new();
    for row in current {
        let id = match &row[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("expected item_no INTEGER, got {other:?}"),
        };
        let blob = match &row[1] {
            SqlValue::Blob(b) => b,
            other => panic!("expected fields BLOB, got {other:?}"),
        };
        let map: BTreeMap<K, PValue> = postcard::from_bytes(blob).expect("postcard decode");
        by_id.insert(id, map);
    }
    let mut writeback = Vec::with_capacity(updates.len());
    for (id, fields) in updates {
        let map = by_id.get_mut(id).unwrap_or_else(|| panic!("row {id} missing for update"));
        for (idx, v) in fields {
            map.insert(K::from_index(*idx), PValue::Str(v.clone()));
        }
        let blob = postcard::to_allocvec(map).expect("postcard encode");
        writeback.push(vec![SqlValue::Integer(*id), SqlValue::Blob(blob)]);
    }
    exec_chunked(
        conn,
        "UPDATE items SET fields = v.column2 FROM (VALUES ",
        ") AS v WHERE items.item_no = v.column1",
        &writeback,
    )
    .await
}

/// Single-row point read + postcard decode, for the point-read cost measurement.
pub async fn binary_point_read<K: FieldKey>(
    stmt: &mut turso::Statement,
    id: i64,
) -> turso::Result<usize> {
    let mut rows = stmt.query([id]).await?;
    let row = rows.next().await?.expect("row must exist");
    let blob: Vec<u8> = row.get(0)?;
    let map: BTreeMap<K, PValue> = postcard::from_bytes(&blob).expect("postcard decode");
    Ok(map.len())
}

// -------------------------------------------------------------------- (c) EAV

pub async fn eav_ingest(
    conn: &Connection,
    records: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let mut rows = Vec::with_capacity(records.iter().map(|(_, f)| f.len()).sum());
    for (id, fields) in records {
        for (idx, v) in fields {
            rows.push(vec![
                SqlValue::Integer(*id),
                SqlValue::Integer(*idx as i64),
                SqlValue::Text(v.clone()),
            ]);
        }
    }
    exec_chunked(
        conn,
        "INSERT INTO eav (item_no, field_no, value) VALUES ",
        "",
        &rows,
    )
    .await
}

pub async fn eav_update(
    conn: &Connection,
    updates: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    let mut rows = Vec::with_capacity(updates.iter().map(|(_, f)| f.len()).sum());
    for (id, fields) in updates {
        for (idx, v) in fields {
            rows.push(vec![
                SqlValue::Integer(*id),
                SqlValue::Integer(*idx as i64),
                SqlValue::Text(v.clone()),
            ]);
        }
    }
    exec_chunked(
        conn,
        "INSERT INTO eav (item_no, field_no, value) VALUES ",
        " ON CONFLICT(item_no, field_no) DO UPDATE SET value = excluded.value",
        &rows,
    )
    .await
}

pub async fn eav_point_read(stmt: &mut turso::Statement, id: i64) -> turso::Result<usize> {
    let mut rows = stmt.query([id]).await?;
    let mut n = 0usize;
    while rows.next().await?.is_some() {
        n += 1;
    }
    Ok(n)
}

pub async fn json_point_read(stmt: &mut turso::Statement, id: i64) -> turso::Result<usize> {
    let mut rows = stmt.query([id]).await?;
    let row = rows.next().await?.expect("row must exist");
    let text: String = row.get(0)?;
    let obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).expect("valid JSON object");
    Ok(obj.len())
}

/// Value template helper shared by main.rs's generators.
pub fn value_size_hint(tpl: &ValueTemplate) -> usize {
    tpl.size()
}

// ------------------------------------------------------------- dispatch

pub async fn ingest(
    conn: &Connection,
    model: Model,
    records: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    match model {
        Model::JsonText => json_ingest(conn, records).await,
        Model::Binary => binary_ingest::<String>(conn, records).await,
        Model::BinaryDeclared => binary_ingest::<u16>(conn, records).await,
        Model::Eav => eav_ingest(conn, records).await,
    }
}

pub async fn update(
    conn: &Connection,
    model: Model,
    updates: &[(i64, Vec<(usize, String)>)],
) -> turso::Result<()> {
    match model {
        Model::JsonText => json_update(conn, updates).await,
        Model::Binary => binary_update::<String>(conn, updates).await,
        Model::BinaryDeclared => binary_update::<u16>(conn, updates).await,
        Model::Eav => eav_update(conn, updates).await,
    }
}

pub fn point_read_sql(model: Model) -> &'static str {
    match model {
        Model::Eav => "SELECT field_no, value FROM eav WHERE item_no = ?",
        _ => "SELECT fields FROM items WHERE item_no = ?",
    }
}

pub async fn point_read(
    stmt: &mut turso::Statement,
    model: Model,
    id: i64,
) -> turso::Result<usize> {
    match model {
        Model::JsonText => json_point_read(stmt, id).await,
        Model::Binary => binary_point_read::<String>(stmt, id).await,
        Model::BinaryDeclared => binary_point_read::<u16>(stmt, id).await,
        Model::Eav => eav_point_read(stmt, id).await,
    }
}
