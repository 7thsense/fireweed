//! SPIKE-006: the four candidate physical layouts for a declared 100-field
//! queue schema (see results doc for the full description):
//!
//!  (A) baseline -- all fields as JSON TEXT on the hot row, `json_patch`ed.
//!  (B) declared field ids -- a compact postcard binary map of small+medium
//!      fields on the hot row; large fields spilled to a values table.
//!  (C) hot/cold split -- the claim-path row carries no fields at all
//!      (key/stage/rank/claim_id/group/gates/version only); small+medium
//!      fields live in a cold table keyed by item_no; large fields spilled.
//!  (D) per-queue physical tables -- real typed SQL columns for small+medium
//!      fields, native SQL indexes on the declared indexed columns, large
//!      fields in a side table.
//!
//! Every layout shares one lease/claims design (`claim_id` on the item row
//! plus a global `claims` table), the design SPIKE-004 already measured and
//! recommended -- this spike holds that variable constant and only varies
//! field-storage placement, per the task's framing ("what physical layout is
//! fastest" for the *declared-fields* question, not the lease question).
//!
//! Every hot SELECT/UPDATE below is issued with an explicit `INDEXED BY`
//! (task requirement; SPIKE-004 Finding 0 found Turso's planner does not
//! reliably choose the right index on its own, for either statement kind).
//! Every write is a single-row, single-key statement -- SPIKE-005's
//! query-plan finding (a batched `UPDATE...FROM (VALUES...)` against a
//! non-rowid composite PK gets a ~100x-slower nested-loop plan) only bites a
//! *batched* multi-row form, and this spike's per-operation measurements are
//! all single-record transactions (matching the granularity of a `write` or
//! one claimed record's read), so that trap does not apply here except where
//! noted for D's rowid-aliased per-queue tables.

use turso::{Connection, Value as SqlValue};

use crate::codec;
use crate::gen::{self, FieldValue};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    A,
    B,
    C,
    D,
}

impl Layout {
    pub fn parse(s: &str) -> Self {
        match s {
            "a" => Layout::A,
            "b" => Layout::B,
            "c" => Layout::C,
            "d" => Layout::D,
            other => panic!("unknown layout {other} (want a|b|c|d)"),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Layout::A => "(A) baseline JSON",
            Layout::B => "(B) declared-id binary map",
            Layout::C => "(C) hot/cold split",
            Layout::D => "(D) per-queue physical tables",
        }
    }
}
impl std::fmt::Display for Layout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub const STAGE_WORK: i64 = 0; // claimable
pub const STAGE_DONE: i64 = 1; // terminal (95% of ingested rows, matching claim-scale's 95/5 split)

fn items_table(layout: Layout, queue_no: i64) -> String {
    match layout {
        Layout::D => format!("items_q{queue_no}"),
        _ => "items".to_string(),
    }
}
fn values_table(layout: Layout, queue_no: i64) -> String {
    match layout {
        Layout::D => format!("values_q{queue_no}"),
        _ => "item_values".to_string(),
    }
}
fn claim_idx_name(layout: Layout, queue_no: i64) -> String {
    match layout {
        Layout::D => format!("items_q{queue_no}_claim_idx"),
        _ => "items_claim_idx".to_string(),
    }
}
fn items_pk_idx(layout: Layout) -> &'static str {
    match layout {
        Layout::D => "", // rowid table, no named PK autoindex
        _ => "sqlite_autoindex_items_1",
    }
}

/// Declared column name for field `idx` on D's typed table (`f000`..`f094`).
fn col_name(idx: usize) -> String {
    format!("f{idx:03}")
}
fn col_sql_type(idx: usize) -> &'static str {
    match gen::field_kind(idx) {
        gen::FieldKind::Int => "INTEGER",
        gen::FieldKind::Str => "TEXT",
        gen::FieldKind::Bytes => unreachable!("large fields are never a D column"),
    }
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

/// Created once per file, before any queue is ingested. Holds the tables
/// that are genuinely shared across every queue: the lease/claims table for
/// every layout, plus (for A/B/C only) the one multi-queue `items` table and
/// its side tables -- "many queues share one Turso file" (owner decision).
pub fn shared_schema_ddl(layout: Layout) -> Vec<String> {
    let mut ddl = vec![
        "CREATE TABLE claims (\n\
            claim_id INTEGER PRIMARY KEY,\n\
            token_hash BLOB NOT NULL,\n\
            holder TEXT,\n\
            expires_at INTEGER NOT NULL,\n\
            record_count INTEGER NOT NULL\n\
        )"
        .to_string(),
    ];
    match layout {
        Layout::A | Layout::B => {
            let fields_col = match layout {
                Layout::A => "fields TEXT NOT NULL",
                Layout::B => "fields BLOB NOT NULL",
                _ => unreachable!(),
            };
            ddl.push(format!(
                "CREATE TABLE items (\n\
                    queue_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    stage INTEGER NOT NULL,\n\
                    rank INTEGER NOT NULL,\n\
                    claim_id INTEGER,\n\
                    version INTEGER NOT NULL,\n\
                    {fields_col},\n\
                    PRIMARY KEY (queue_no, item_no)\n\
                )"
            ));
            ddl.push(
                "CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, item_no) \
                 WHERE claim_id IS NULL"
                    .to_string(),
            );
            if layout == Layout::B {
                ddl.push(
                    "CREATE TABLE item_values (\n\
                        queue_no INTEGER NOT NULL,\n\
                        item_no INTEGER NOT NULL,\n\
                        field_no INTEGER NOT NULL,\n\
                        value BLOB NOT NULL,\n\
                        PRIMARY KEY (queue_no, item_no, field_no)\n\
                    )"
                    .to_string(),
                );
            }
            ddl.push(
                "CREATE TABLE item_index (\n\
                    queue_no INTEGER NOT NULL,\n\
                    field_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    index_key BLOB NOT NULL,\n\
                    PRIMARY KEY (queue_no, field_no, item_no)\n\
                )"
                .to_string(),
            );
            ddl.push(
                "CREATE INDEX item_index_key_idx ON item_index (queue_no, field_no, index_key, item_no)"
                    .to_string(),
            );
        }
        Layout::C => {
            ddl.push(
                "CREATE TABLE items (\n\
                    queue_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    stage INTEGER NOT NULL,\n\
                    rank INTEGER NOT NULL,\n\
                    claim_id INTEGER,\n\
                    version INTEGER NOT NULL,\n\
                    PRIMARY KEY (queue_no, item_no)\n\
                )"
                .to_string(),
            );
            ddl.push(
                "CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, item_no) \
                 WHERE claim_id IS NULL"
                    .to_string(),
            );
            ddl.push(
                "CREATE TABLE item_cold (\n\
                    queue_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    fields BLOB NOT NULL,\n\
                    PRIMARY KEY (queue_no, item_no)\n\
                )"
                .to_string(),
            );
            ddl.push(
                "CREATE TABLE item_values (\n\
                    queue_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    field_no INTEGER NOT NULL,\n\
                    value BLOB NOT NULL,\n\
                    PRIMARY KEY (queue_no, item_no, field_no)\n\
                )"
                .to_string(),
            );
            ddl.push(
                "CREATE TABLE item_index (\n\
                    queue_no INTEGER NOT NULL,\n\
                    field_no INTEGER NOT NULL,\n\
                    item_no INTEGER NOT NULL,\n\
                    index_key BLOB NOT NULL,\n\
                    PRIMARY KEY (queue_no, field_no, item_no)\n\
                )"
                .to_string(),
            );
            ddl.push(
                "CREATE INDEX item_index_key_idx ON item_index (queue_no, field_no, index_key, item_no)"
                    .to_string(),
            );
        }
        Layout::D => {
            // Per-queue tables are created by per_queue_schema_ddl; nothing
            // shared beyond `claims`.
        }
    }
    ddl
}

/// D only: real per-queue tables, typed columns for the 95 small+medium
/// fields, native indexes on the 4 declared indexed columns plus the
/// claim-order index. A/B/C return an empty vec (their tables are already
/// shared, created once by `shared_schema_ddl`).
pub fn per_queue_schema_ddl(layout: Layout, queue_no: i64) -> Vec<String> {
    if layout != Layout::D {
        return Vec::new();
    }
    let t = items_table(layout, queue_no);
    let v = values_table(layout, queue_no);
    let mut cols = String::new();
    for idx in 0..(gen::SMALL_COUNT + gen::MEDIUM_COUNT) {
        cols.push_str(&format!(",\n    {} {}", col_name(idx), col_sql_type(idx)));
    }
    let mut ddl = vec![format!(
        "CREATE TABLE {t} (\n\
            item_no INTEGER PRIMARY KEY,\n\
            stage INTEGER NOT NULL,\n\
            rank INTEGER NOT NULL,\n\
            claim_id INTEGER,\n\
            version INTEGER NOT NULL{cols}\n\
        )"
    )];
    ddl.push(format!(
        "CREATE INDEX {} ON {t} (stage, rank, item_no) WHERE claim_id IS NULL",
        claim_idx_name(layout, queue_no)
    ));
    for &f in gen::INDEXED_FIELDS.iter() {
        ddl.push(format!(
            "CREATE INDEX {t}_{}_idx ON {t} ({})",
            col_name(f),
            col_name(f)
        ));
    }
    ddl.push(format!(
        "CREATE TABLE {v} (\n\
            item_no INTEGER NOT NULL,\n\
            field_no INTEGER NOT NULL,\n\
            value BLOB NOT NULL,\n\
            PRIMARY KEY (item_no, field_no)\n\
        )"
    ));
    ddl
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

pub async fn ingest_row(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    item_no: i64,
    stage: i64,
    rank: i64,
) -> turso::Result<()> {
    let record = gen::gen_record(queue_no, item_no);
    let items = items_table(layout, queue_no);
    let values = values_table(layout, queue_no);

    match layout {
        Layout::A => {
            let text = codec::json_encode_full(&record);
            conn.execute(
                &format!(
                    "INSERT INTO {items} (queue_no,item_no,stage,rank,claim_id,version,fields) \
                     VALUES (?1,?2,?3,?4,NULL,1,?5)"
                ),
                (
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                    SqlValue::Integer(stage),
                    SqlValue::Integer(rank),
                    SqlValue::Text(text),
                ),
            )
            .await?;
        }
        Layout::B => {
            let map = codec::small_medium_map(&record);
            let blob = codec::encode_map(&map);
            conn.execute(
                &format!(
                    "INSERT INTO {items} (queue_no,item_no,stage,rank,claim_id,version,fields) \
                     VALUES (?1,?2,?3,?4,NULL,1,?5)"
                ),
                (
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                    SqlValue::Integer(stage),
                    SqlValue::Integer(rank),
                    SqlValue::Blob(blob),
                ),
            )
            .await?;
        }
        Layout::C => {
            conn.execute(
                &format!(
                    "INSERT INTO {items} (queue_no,item_no,stage,rank,claim_id,version) \
                     VALUES (?1,?2,?3,?4,NULL,1)"
                ),
                (
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                    SqlValue::Integer(stage),
                    SqlValue::Integer(rank),
                ),
            )
            .await?;
            let map = codec::small_medium_map(&record);
            let blob = codec::encode_map(&map);
            conn.execute(
                "INSERT INTO item_cold (queue_no,item_no,fields) VALUES (?1,?2,?3)",
                (
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                    SqlValue::Blob(blob),
                ),
            )
            .await?;
        }
        Layout::D => {
            let mut cols = String::from("item_no,stage,rank,claim_id,version");
            let mut placeholders = String::from("?1,?2,?3,NULL,1");
            let mut params: Vec<SqlValue> = vec![
                SqlValue::Integer(item_no),
                SqlValue::Integer(stage),
                SqlValue::Integer(rank),
            ];
            let mut p = 4;
            for (idx, v) in record.iter().filter(|(i, _)| gen::is_small(*i) || gen::is_medium(*i)) {
                cols.push(',');
                cols.push_str(&col_name(*idx));
                placeholders.push_str(&format!(",?{p}"));
                params.push(match v {
                    FieldValue::Int(i) => SqlValue::Integer(*i),
                    FieldValue::Str(s) => SqlValue::Text(s.clone()),
                    FieldValue::Bytes(_) => unreachable!(),
                });
                p += 1;
            }
            conn.execute(
                &format!("INSERT INTO {items} ({cols}) VALUES ({placeholders})"),
                params,
            )
            .await?;
        }
    }

    // Large fields: always spilled (B/C/D), never spilled for A (inline).
    if layout != Layout::A {
        for (idx, v) in record.iter().filter(|(i, _)| gen::is_large(*i)) {
            let FieldValue::Bytes(b) = v else { unreachable!() };
            let sql = match layout {
                Layout::D => format!("INSERT INTO {values} (item_no,field_no,value) VALUES (?1,?2,?3)"),
                _ => format!(
                    "INSERT INTO {values} (queue_no,item_no,field_no,value) VALUES (?1,?2,?3,?4)"
                ),
            };
            let params: Vec<SqlValue> = match layout {
                Layout::D => vec![
                    SqlValue::Integer(item_no),
                    SqlValue::Integer(*idx as i64),
                    SqlValue::Blob(b.clone()),
                ],
                _ => vec![
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                    SqlValue::Integer(*idx as i64),
                    SqlValue::Blob(b.clone()),
                ],
            };
            conn.execute(&sql, params).await?;
        }
    }

    // Index rows (A/B/C only; D uses native SQL indexes on its typed columns).
    if layout != Layout::D {
        for &f in gen::INDEXED_FIELDS.iter() {
            let v = &record[f].1;
            let key = gen::index_key_bytes(v);
            conn.execute(
                "INSERT INTO item_index (queue_no,field_no,item_no,index_key) VALUES (?1,?2,?3,?4)",
                (
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(f as i64),
                    SqlValue::Integer(item_no),
                    SqlValue::Blob(key),
                ),
            )
            .await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Claim: select `n` free WORK-stage items by rank, lease via claims table,
// then fetch the small fields for each claimed item.
// ---------------------------------------------------------------------------

pub async fn claim_select(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    n: usize,
) -> turso::Result<Vec<i64>> {
    let items = items_table(layout, queue_no);
    let idx = claim_idx_name(layout, queue_no);
    let (sql, params): (String, Vec<SqlValue>) = if layout == Layout::D {
        (
            format!(
                "SELECT item_no FROM {items} INDEXED BY {idx} \
                 WHERE stage=?1 AND claim_id IS NULL ORDER BY rank, item_no LIMIT {n}"
            ),
            vec![SqlValue::Integer(STAGE_WORK)],
        )
    } else {
        (
            format!(
                "SELECT item_no FROM {items} INDEXED BY {idx} \
                 WHERE queue_no=?1 AND stage=?2 AND claim_id IS NULL \
                 ORDER BY rank, item_no LIMIT {n}"
            ),
            vec![SqlValue::Integer(queue_no), SqlValue::Integer(STAGE_WORK)],
        )
    };
    let mut stmt = conn.prepare(&sql).await?;
    let mut rows = stmt.query(params).await?;
    let mut out = Vec::with_capacity(n);
    while let Some(row) = rows.next().await? {
        if let SqlValue::Integer(id) = row.get_value(0)? {
            out.push(id);
        }
    }
    Ok(out)
}

/// Lease every claimed row with ONE prepared statement reused across all
/// `ids`, instead of `claim_lease_row` called per row (which re-prepares
/// the identical SQL text on every call). Found necessary the same way the
/// missing-transaction bug was: claim-scale's own `do_claim` always
/// prepared once and looped `stmt.execute()`, and this spike should have
/// matched that from the start rather than introducing 100 fresh
/// prepare-parse-plan cycles per claim.
pub async fn claim_lease_all(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    ids: &[i64],
    claim_id: i64,
) -> turso::Result<()> {
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);
    if layout == Layout::D {
        let mut stmt = conn.prepare(&format!("UPDATE {items} SET claim_id=?1 WHERE item_no=?2")).await?;
        for &id in ids {
            stmt.execute((SqlValue::Integer(claim_id), SqlValue::Integer(id))).await?;
        }
    } else {
        let mut stmt = conn
            .prepare(&format!(
                "UPDATE {items} INDEXED BY {pk} SET claim_id=?1 WHERE queue_no=?2 AND item_no=?3"
            ))
            .await?;
        for &id in ids {
            stmt.execute((SqlValue::Integer(claim_id), SqlValue::Integer(queue_no), SqlValue::Integer(id)))
                .await?;
        }
    }
    Ok(())
}

/// Fetch and decode every claimed row's small fields with ONE prepared
/// statement reused across all `ids` (see `claim_lease_all`'s doc comment
/// -- the same per-row-reprepare defect, fixed the same way). Layout-aware
/// decode logic duplicated from `read_small` in batched-prepare form.
pub async fn claim_fetch_small_all(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    ids: &[i64],
) -> turso::Result<usize> {
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);
    let mut total = 0usize;
    match layout {
        Layout::A => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            for &id in ids {
                let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(id))).await?;
                let row = rows.next().await?.expect("row exists");
                let text: String = row.get(0)?;
                total += codec::json_decode_full(&text).iter().filter(|(idx, _)| gen::is_small(**idx)).count();
            }
        }
        Layout::B => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            for &id in ids {
                let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(id))).await?;
                let row = rows.next().await?.expect("row exists");
                let blob: Vec<u8> = row.get(0)?;
                total += codec::decode_map(&blob).keys().filter(|k| gen::is_small(**k as usize)).count();
            }
        }
        Layout::C => {
            let mut stmt = conn
                .prepare("SELECT fields FROM item_cold INDEXED BY sqlite_autoindex_item_cold_1 WHERE queue_no=?1 AND item_no=?2")
                .await?;
            for &id in ids {
                let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(id))).await?;
                let row = rows.next().await?.expect("row exists");
                let blob: Vec<u8> = row.get(0)?;
                total += codec::decode_map(&blob).keys().filter(|k| gen::is_small(**k as usize)).count();
            }
        }
        Layout::D => {
            let cols: Vec<String> = (0..gen::SMALL_COUNT).map(col_name).collect();
            let mut stmt = conn.prepare(&format!("SELECT {} FROM {items} WHERE item_no=?1", cols.join(","))).await?;
            for &id in ids {
                let mut rows = stmt.query((SqlValue::Integer(id),)).await?;
                let row = rows.next().await?.expect("row exists");
                total += row.column_count();
            }
        }
    }
    Ok(total)
}

pub async fn insert_claim(conn: &Connection, claim_id: i64, expires_at: i64, n: usize) -> turso::Result<()> {
    conn.execute(
        "INSERT INTO claims (claim_id,token_hash,holder,expires_at,record_count) \
         VALUES (?1,?2,?3,?4,?5)",
        (
            SqlValue::Integer(claim_id),
            SqlValue::Blob(claim_id.to_be_bytes().to_vec()),
            SqlValue::Text("worker-0".to_string()),
            SqlValue::Integer(expires_at),
            SqlValue::Integer(n as i64),
        ),
    )
    .await?;
    Ok(())
}


// ---------------------------------------------------------------------------
// Update: 1-3 small unindexed fields
// ---------------------------------------------------------------------------

pub async fn update_small_fields(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    item_no: i64,
    field_ids: &[usize],
    round: u64,
) -> turso::Result<()> {
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);
    let patch: Vec<(usize, FieldValue)> = field_ids
        .iter()
        .map(|&f| (f, gen::gen_field(queue_no, item_no, f, round)))
        .collect();

    match layout {
        Layout::A => {
            let text = codec::json_encode_patch(&patch);
            conn.execute(
                &format!(
                    "UPDATE {items} INDEXED BY {pk} SET fields=json_patch(fields,?1), version=version+1 \
                     WHERE queue_no=?2 AND item_no=?3"
                ),
                (
                    SqlValue::Text(text),
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                ),
            )
            .await?;
        }
        Layout::B => {
            rmw_patch_blob(conn, &items, pk, queue_no, item_no, "fields", &patch, true).await?;
        }
        Layout::C => {
            rmw_patch_blob(conn, "item_cold", "sqlite_autoindex_item_cold_1", queue_no, item_no, "fields", &patch, false)
                .await?;
            conn.execute(
                &format!("UPDATE {items} INDEXED BY {pk} SET version=version+1 WHERE queue_no=?1 AND item_no=?2"),
                (SqlValue::Integer(queue_no), SqlValue::Integer(item_no)),
            )
            .await?;
        }
        Layout::D => {
            let mut set_clause = String::from("version=version+1");
            let mut params: Vec<SqlValue> = Vec::new();
            let mut p = 1;
            for (idx, v) in &patch {
                set_clause.push_str(&format!(", {}=?{p}", col_name(*idx)));
                params.push(match v {
                    FieldValue::Int(i) => SqlValue::Integer(*i),
                    FieldValue::Str(s) => SqlValue::Text(s.clone()),
                    FieldValue::Bytes(_) => unreachable!(),
                });
                p += 1;
            }
            params.push(SqlValue::Integer(item_no));
            conn.execute(
                &format!("UPDATE {items} SET {set_clause} WHERE item_no=?{p}"),
                params,
            )
            .await?;
        }
    }
    Ok(())
}

/// RMW helper for B/C: SELECT the blob, decode, patch the named keys,
/// re-encode, write back -- one point SELECT + one point UPDATE, both
/// INDEXED BY the table's PK autoindex. `bump_version` also increments
/// `version` in the same UPDATE (B, where fields lives on the hot row
/// itself); C bumps `version` separately on `items` (its own, much smaller,
/// row) right after calling this.
async fn rmw_patch_blob(
    conn: &Connection,
    table: &str,
    pk_idx: &str,
    queue_no: i64,
    item_no: i64,
    col: &str,
    patch: &[(usize, FieldValue)],
    bump_version: bool,
) -> turso::Result<()> {
    let blob: Vec<u8> = {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {col} FROM {table} INDEXED BY {pk_idx} WHERE queue_no=?1 AND item_no=?2"
            ))
            .await?;
        let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
        let row = rows.next().await?.expect("row must exist");
        row.get(0)?
    };
    let mut map = codec::decode_map(&blob);
    for (idx, v) in patch {
        map.insert(*idx as u16, (&*v).into());
    }
    let new_blob = codec::encode_map(&map);
    let set_clause = if bump_version {
        format!("{col}=?1, version=version+1")
    } else {
        format!("{col}=?1")
    };
    conn.execute(
        &format!("UPDATE {table} INDEXED BY {pk_idx} SET {set_clause} WHERE queue_no=?2 AND item_no=?3"),
        (
            SqlValue::Blob(new_blob),
            SqlValue::Integer(queue_no),
            SqlValue::Integer(item_no),
        ),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Update: one indexed field
// ---------------------------------------------------------------------------

pub async fn update_indexed_field(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    item_no: i64,
    round: u64,
) -> turso::Result<()> {
    let f = gen::UPDATE_INDEXED_FIELD;
    let new_val = gen::gen_field(queue_no, item_no, f, round);
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);

    match layout {
        Layout::A => {
            let text = codec::json_encode_patch(&[(f, new_val.clone())]);
            conn.execute(
                &format!(
                    "UPDATE {items} INDEXED BY {pk} SET fields=json_patch(fields,?1), version=version+1 \
                     WHERE queue_no=?2 AND item_no=?3"
                ),
                (
                    SqlValue::Text(text),
                    SqlValue::Integer(queue_no),
                    SqlValue::Integer(item_no),
                ),
            )
            .await?;
        }
        Layout::B => {
            rmw_patch_blob(conn, &items, pk, queue_no, item_no, "fields", &[(f, new_val.clone())], true).await?;
        }
        Layout::C => {
            rmw_patch_blob(conn, "item_cold", "sqlite_autoindex_item_cold_1", queue_no, item_no, "fields", &[(f, new_val.clone())], false)
                .await?;
            conn.execute(
                &format!("UPDATE {items} INDEXED BY {pk} SET version=version+1 WHERE queue_no=?1 AND item_no=?2"),
                (SqlValue::Integer(queue_no), SqlValue::Integer(item_no)),
            )
            .await?;
        }
        Layout::D => {
            let param = match &new_val {
                FieldValue::Int(i) => SqlValue::Integer(*i),
                FieldValue::Str(s) => SqlValue::Text(s.clone()),
                FieldValue::Bytes(_) => unreachable!(),
            };
            conn.execute(
                &format!(
                    "UPDATE {items} SET {}=?1, version=version+1 WHERE item_no=?2",
                    col_name(f)
                ),
                (param, SqlValue::Integer(item_no)),
            )
            .await?;
            // Native SQL index on this column is maintained automatically by
            // the UPDATE above -- no manual index-table bookkeeping, unlike
            // A/B/C below.
            return Ok(());
        }
    }

    // A/B/C: the index side table needs its own row rekeyed (D's native
    // index is maintained by the engine, handled above).
    let key = gen::index_key_bytes(&new_val);
    conn.execute(
        "UPDATE item_index INDEXED BY sqlite_autoindex_item_index_1 SET index_key=?1 \
         WHERE queue_no=?2 AND field_no=?3 AND item_no=?4",
        (
            SqlValue::Blob(key),
            SqlValue::Integer(queue_no),
            SqlValue::Integer(f as i64),
            SqlValue::Integer(item_no),
        ),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Update: one large field (rare)
// ---------------------------------------------------------------------------

pub async fn update_large_field(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    item_no: i64,
    round: u64,
) -> turso::Result<()> {
    let f = gen::UPDATE_LARGE_FIELD;
    let new_val = gen::gen_field(queue_no, item_no, f, round);
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);

    if layout == Layout::A {
        let text = codec::json_encode_patch(&[(f, new_val)]);
        conn.execute(
            &format!(
                "UPDATE {items} INDEXED BY {pk} SET fields=json_patch(fields,?1), version=version+1 \
                 WHERE queue_no=?2 AND item_no=?3"
            ),
            (
                SqlValue::Text(text),
                SqlValue::Integer(queue_no),
                SqlValue::Integer(item_no),
            ),
        )
        .await?;
        return Ok(());
    }

    let FieldValue::Bytes(b) = new_val else { unreachable!() };
    let values = values_table(layout, queue_no);
    if layout == Layout::D {
        conn.execute(
            &format!("UPDATE {values} SET value=?1 WHERE item_no=?2 AND field_no=?3"),
            (SqlValue::Blob(b), SqlValue::Integer(item_no), SqlValue::Integer(f as i64)),
        )
        .await?;
        conn.execute(
            &format!("UPDATE {items} SET version=version+1 WHERE item_no=?1"),
            (SqlValue::Integer(item_no),),
        )
        .await?;
    } else {
        conn.execute(
            &format!(
                "UPDATE {values} INDEXED BY sqlite_autoindex_item_values_1 SET value=?1 \
                 WHERE queue_no=?2 AND item_no=?3 AND field_no=?4"
            ),
            (
                SqlValue::Blob(b),
                SqlValue::Integer(queue_no),
                SqlValue::Integer(item_no),
                SqlValue::Integer(f as i64),
            ),
        )
        .await?;
        // Every layout's `version` lives on the hot `items` row: for B this
        // physically rewrites the *entire* small+medium blob even though
        // only `version` is named in SET (SPIKE-003 Finding 3's
        // whole-row-rewrite mechanic); for C, `items` carries no fields at
        // all, so this is a tiny write. This is the point of the hot/cold
        // split -- see the results doc.
        conn.execute(
            &format!("UPDATE {items} INDEXED BY {pk} SET version=version+1 WHERE queue_no=?1 AND item_no=?2"),
            (SqlValue::Integer(queue_no), SqlValue::Integer(item_no)),
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Point reads
// ---------------------------------------------------------------------------

/// Full record: every field, small/medium/large alike.
pub async fn read_full(conn: &Connection, layout: Layout, queue_no: i64, item_no: i64) -> turso::Result<usize> {
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);
    let mut n = 0usize;

    let small_medium: std::collections::BTreeMap<u16, codec::PValue> = match layout {
        Layout::A => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let text: String = row.get(0)?;
            let all = codec::json_decode_full(&text);
            n += all.len();
            Default::default()
        }
        Layout::B => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let blob: Vec<u8> = row.get(0)?;
            codec::decode_map(&blob)
        }
        Layout::C => {
            let mut stmt = conn
                .prepare("SELECT fields FROM item_cold INDEXED BY sqlite_autoindex_item_cold_1 WHERE queue_no=?1 AND item_no=?2")
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let blob: Vec<u8> = row.get(0)?;
            codec::decode_map(&blob)
        }
        Layout::D => {
            let cols: Vec<String> = (0..(gen::SMALL_COUNT + gen::MEDIUM_COUNT)).map(col_name).collect();
            let sql = format!("SELECT {} FROM {items} WHERE item_no=?1", cols.join(","));
            let mut stmt = conn.prepare(&sql).await?;
            let mut rows = stmt.query((SqlValue::Integer(item_no),)).await?;
            let row = rows.next().await?.expect("row exists");
            n += row.column_count();
            Default::default()
        }
    };
    n += small_medium.len();

    // Large fields: 2 point reads against the spill table (A has none --
    // everything is already in the JSON blob read above).
    if layout != Layout::A {
        let values = values_table(layout, queue_no);
        for &f in &[95usize, 96] {
            let sql = if layout == Layout::D {
                format!("SELECT value FROM {values} WHERE item_no=?1 AND field_no=?2")
            } else {
                format!(
                    "SELECT value FROM {values} INDEXED BY sqlite_autoindex_item_values_1 \
                     WHERE queue_no=?1 AND item_no=?2 AND field_no=?3"
                )
            };
            let mut stmt = conn.prepare(&sql).await?;
            let params: Vec<SqlValue> = if layout == Layout::D {
                vec![SqlValue::Integer(item_no), SqlValue::Integer(f as i64)]
            } else {
                vec![SqlValue::Integer(queue_no), SqlValue::Integer(item_no), SqlValue::Integer(f as i64)]
            };
            let mut rows = stmt.query(params).await?;
            let row = rows.next().await?.expect("large value row exists");
            let b: Vec<u8> = row.get(0)?;
            n += b.len();
        }
    }
    Ok(n)
}

/// Only the small fields -- the read a claim, or a "what's on this record"
/// caller that never needs the profile blob, actually does.
pub async fn read_small(conn: &Connection, layout: Layout, queue_no: i64, item_no: i64) -> turso::Result<usize> {
    let items = items_table(layout, queue_no);
    let pk = items_pk_idx(layout);
    match layout {
        Layout::A => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let text: String = row.get(0)?;
            let all = codec::json_decode_full(&text);
            Ok(all.iter().filter(|(idx, _)| gen::is_small(**idx)).count())
        }
        Layout::B => {
            let mut stmt = conn
                .prepare(&format!("SELECT fields FROM {items} INDEXED BY {pk} WHERE queue_no=?1 AND item_no=?2"))
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let blob: Vec<u8> = row.get(0)?;
            let map = codec::decode_map(&blob);
            Ok(map.keys().filter(|k| gen::is_small(**k as usize)).count())
        }
        Layout::C => {
            let mut stmt = conn
                .prepare("SELECT fields FROM item_cold INDEXED BY sqlite_autoindex_item_cold_1 WHERE queue_no=?1 AND item_no=?2")
                .await?;
            let mut rows = stmt.query((SqlValue::Integer(queue_no), SqlValue::Integer(item_no))).await?;
            let row = rows.next().await?.expect("row exists");
            let blob: Vec<u8> = row.get(0)?;
            let map = codec::decode_map(&blob);
            Ok(map.keys().filter(|k| gen::is_small(**k as usize)).count())
        }
        Layout::D => {
            let cols: Vec<String> = (0..gen::SMALL_COUNT).map(col_name).collect();
            let sql = format!("SELECT {} FROM {items} WHERE item_no=?1", cols.join(","));
            let mut stmt = conn.prepare(&sql).await?;
            let mut rows = stmt.query((SqlValue::Integer(item_no),)).await?;
            let row = rows.next().await?.expect("row exists");
            Ok(row.column_count())
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate: count grouped by the indexed AGGREGATE field over a range.
// ---------------------------------------------------------------------------

pub async fn aggregate_count(
    conn: &Connection,
    layout: Layout,
    queue_no: i64,
    lo: i64,
    hi: i64,
) -> turso::Result<(usize, i64)> {
    let f = gen::UPDATE_INDEXED_FIELD; // field 0, bounded range for meaningful group cardinality
    let (sql, params): (String, Vec<SqlValue>) = if layout == Layout::D {
        let items = items_table(layout, queue_no);
        let col = col_name(f);
        (
            format!(
                "SELECT {col}, COUNT(*) FROM {items} INDEXED BY {items}_{col}_idx \
                 WHERE {col} BETWEEN ?1 AND ?2 GROUP BY {col}"
            ),
            vec![SqlValue::Integer(lo), SqlValue::Integer(hi)],
        )
    } else {
        let lo_key = gen::index_key_bytes(&FieldValue::Int(lo));
        let hi_key = gen::index_key_bytes(&FieldValue::Int(hi));
        (
            "SELECT index_key, COUNT(*) FROM item_index INDEXED BY item_index_key_idx \
             WHERE queue_no=?1 AND field_no=?2 AND index_key BETWEEN ?3 AND ?4 GROUP BY index_key"
                .to_string(),
            vec![
                SqlValue::Integer(queue_no),
                SqlValue::Integer(f as i64),
                SqlValue::Blob(lo_key),
                SqlValue::Blob(hi_key),
            ],
        )
    };
    let mut stmt = conn.prepare(&sql).await?;
    let mut rows = stmt.query(params).await?;
    let mut groups = 0usize;
    let mut total = 0i64;
    while let Some(row) = rows.next().await? {
        groups += 1;
        if let SqlValue::Integer(c) = row.get_value(1)? {
            total += c;
        }
    }
    Ok((groups, total))
}
