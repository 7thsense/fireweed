//! SPIKE-002 measurement harness: record-key storage layout cost on the vendored
//! Turso 0.7.2 engine, configured the way `crates/fireweed-turso/src/local.rs`
//! opens its projection (WAL journal, synchronous=NORMAL, cache_size=-131072 KiB,
//! temp_store=MEMORY, wal_autocheckpoint=0 — the `with_log_backed_projection()`
//! profile that `crates/fireweed/src/turso_compose.rs:136` actually opens in
//! production).
//!
//! Standalone crate. Not a member of the fireweed workspace. See
//! docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md for the write-up.

use std::env;
use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use turso::{Builder, Connection, Value, transaction::TransactionBehavior};

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn batch_size() -> usize {
    env_usize("FW_SPIKE_BATCH_SIZE", 1000)
}
fn lookup_ops() -> usize {
    env_usize("FW_SPIKE_LOOKUP_OPS", 20_000)
}
fn upsert_ops() -> usize {
    env_usize("FW_SPIKE_UPSERT_OPS", 20_000)
}
fn claim_batches() -> usize {
    env_usize("FW_SPIKE_CLAIM_BATCHES", 200)
}
const CLAIM_BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// Deterministic key generation. `mix64` is the SplitMix64 finalizer: a
// bijection over u64, so distinct row indices `i` never collide in the low
// bits we keep. No external RNG crate is needed for a spike whose point is
// storage/index cost, not statistical randomness quality.
// ---------------------------------------------------------------------------

fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn fnv1a64(bytes: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// Fixed 16-byte hash of a key's bytes (layout D). Spike-grade combiner
/// (two salted FNV-1a-64 lanes), not a vetted 128-bit hash. See the results
/// doc for the production recommendation (e.g. xxh3-128 or blake3) and the
/// required collision fallback.
fn hash16(bytes: &[u8]) -> Vec<u8> {
    let h1 = fnv1a64(bytes, 0xCBF2_9CE4_8422_2325);
    let h2 = fnv1a64(bytes, 0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&h1.to_be_bytes());
    out.extend_from_slice(&h2.to_be_bytes());
    out
}

fn rand_hex_string(seed: u64, len: usize) -> String {
    let mut out = String::with_capacity(len + 16);
    let mut counter: u64 = 0;
    while out.len() < len {
        let v = mix64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(counter));
        let _ = write!(out, "{v:016x}");
        counter += 1;
    }
    out.truncate(len);
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    IntId,
    Uuid,
    Email,
    Str128,
    Str256,
}

impl Shape {
    fn parse(s: &str) -> Self {
        match s {
            "int" => Shape::IntId,
            "uuid" => Shape::Uuid,
            "email" => Shape::Email,
            "str128" => Shape::Str128,
            "str256" => Shape::Str256,
            other => panic!("unknown shape {other}"),
        }
    }

    /// Text representation of the key (used by layouts A/B, and as the
    /// hash-input / unindexed stored copy in layout D).
    fn text(self, i: u64) -> String {
        match self {
            Shape::IntId => {
                let v = mix64(i) & 0x7FFF_FFFF_FFFF_FFFF;
                v.to_string()
            }
            Shape::Uuid => {
                let bytes = self.blob(i);
                let mut s = String::with_capacity(36);
                for (idx, b) in bytes.iter().enumerate() {
                    if idx == 4 || idx == 6 || idx == 8 || idx == 10 {
                        s.push('-');
                    }
                    let _ = write!(s, "{b:02x}");
                }
                s
            }
            Shape::Email => {
                let v = mix64(i);
                let domains = ["example.com", "corp-mail.example.org", "mail.example.co"];
                let domain = domains[(i % domains.len() as u64) as usize];
                format!("user{:x}@{domain}", v & 0xFF_FFFF_FFFF)
            }
            Shape::Str128 => rand_hex_string(i, 128),
            Shape::Str256 => rand_hex_string(i, 256),
        }
    }

    /// Blob representation (used by layout C, and as the compact binary key
    /// for the int/uuid shapes where one naturally exists).
    fn blob(self, i: u64) -> Vec<u8> {
        match self {
            Shape::IntId => {
                let v = mix64(i) & 0x7FFF_FFFF_FFFF_FFFF;
                v.to_be_bytes().to_vec()
            }
            Shape::Uuid => {
                let a = mix64(i);
                let b = mix64(i ^ 0x9E37_79B9_7F4A_7C15);
                let mut v = Vec::with_capacity(16);
                v.extend_from_slice(&a.to_be_bytes());
                v.extend_from_slice(&b.to_be_bytes());
                v
            }
            // Email/Str128/Str256 have no tighter binary encoding upstream;
            // layout C for these shapes stores the same bytes as BLOB instead
            // of TEXT, isolating SQLite/Turso TEXT-affinity/collation cost
            // from BLOB byte-compare cost at an identical byte width.
            Shape::Email | Shape::Str128 | Shape::Str256 => self.text(i).into_bytes(),
        }
    }

    /// Native INTEGER representation. Only defined for the int shape
    /// (layout E is int-only, per the owner's question 3).
    fn int(self, i: u64) -> Option<i64> {
        match self {
            Shape::IntId => Some((mix64(i) & 0x7FFF_FFFF_FFFF_FFFF) as i64),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// (a) Today's shape: PK (tenant_id, queue_id, item_id) TEXT surrogate +
    /// a separate UNIQUE INDEX (tenant_id, queue_id, client_item_key) TEXT.
    /// Mirrors `crates/fireweed-relational/src/schema.rs:15-19,53,65-66`
    /// exactly, minus the `WHERE superseded=0` partial predicate: the new
    /// contract drops tombstones/supersede (draft §3.3), so a clean
    /// non-partial unique index is the honest shape to carry forward.
    A,
    /// (b) surrogate `queue_no INTEGER` in place of the two tenant/queue TEXT
    /// columns, key TEXT unique.
    B,
    /// (c) queue_no + key BLOB unique.
    C,
    /// (d) queue_no + a fixed 16-byte hash of the key (unique); the key
    /// itself is stored unindexed alongside it.
    D,
    /// (e) queue_no + key INTEGER (int shape only).
    E,
}

impl Layout {
    fn parse(s: &str) -> Self {
        match s {
            "a" => Layout::A,
            "b" => Layout::B,
            "c" => Layout::C,
            "d" => Layout::D,
            "e" => Layout::E,
            other => panic!("unknown layout {other}"),
        }
    }
}

/// The 27 non-identity "hot" columns of `fireweed_items`, copied verbatim
/// (name, type, nullability, default) from
/// `crates/fireweed-relational/src/schema.rs:20-52`, minus tenant_id,
/// queue_id, item_id and client_item_key. Included so every layout pays a
/// realistic per-row cost, not a bare two-column table.
const HOT_COLUMNS_DDL: &str = "
    lifecycle_state TEXT NOT NULL,
    priority TEXT,
    priority_sort BLOB NOT NULL,
    not_before INTEGER,
    eligible_since INTEGER,
    group_key TEXT,
    cohort_size INTEGER,
    recurrence_until INTEGER,
    payload BLOB,
    fields TEXT NOT NULL DEFAULT '{}',
    metadata TEXT NOT NULL DEFAULT '{}',
    entity_document TEXT,
    index_fields BLOB,
    retry_count INTEGER NOT NULL DEFAULT 0,
    item_version INTEGER NOT NULL,
    lease_token_hash BLOB,
    lease_expires_at INTEGER,
    worker_id TEXT,
    last_command_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER,
    terminal_command_epoch INTEGER,
    fenced INTEGER NOT NULL DEFAULT 0,
    superseded INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    created_seq INTEGER NOT NULL
";

const HOT_COLUMN_NAMES: &str = "lifecycle_state,priority,priority_sort,not_before,eligible_since,\
group_key,cohort_size,recurrence_until,payload,fields,metadata,entity_document,index_fields,\
retry_count,item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,\
created_at,updated_at,terminal_at,terminal_command_epoch,fenced,superseded,max_attempts,created_seq";

const HOT_COLUMN_COUNT: usize = 27;

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn hot_column_values(i: u64, base_ts: i64) -> Vec<Value> {
    let created_at = base_ts + i as i64;
    vec![
        Value::Text("Pending".to_string()),
        Value::Null,
        Value::Blob(vec![0u8; 8].into_iter().chain((i as u64).to_be_bytes()).collect()),
        Value::Null,
        Value::Integer(created_at),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text("{\"kind\":\"job\",\"attempt\":0}".to_string()),
        Value::Text("{}".to_string()),
        Value::Null,
        Value::Blob(vec![0xAB; 24]),
        Value::Integer(0),
        Value::Integer(1),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Integer(i as i64),
        Value::Integer(created_at),
        Value::Integer(created_at),
        Value::Null,
        Value::Null,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(5),
        Value::Integer(i as i64),
    ]
}

struct LayoutSql {
    create_table: String,
    create_index: String,
    insert: String,
    identity_placeholders: usize,
}

fn layout_sql(layout: Layout) -> LayoutSql {
    match layout {
        Layout::A => LayoutSql {
            create_table: format!(
                "CREATE TABLE items (\n\
                    tenant_id TEXT NOT NULL,\n\
                    queue_id TEXT NOT NULL,\n\
                    item_id TEXT NOT NULL,\n\
                    client_item_key TEXT NOT NULL,\n\
                    {HOT_COLUMNS_DDL},\n\
                    PRIMARY KEY (tenant_id, queue_id, item_id)\n\
                )"
            ),
            create_index: "CREATE UNIQUE INDEX items_key_idx ON items (tenant_id, queue_id, client_item_key)"
                .to_string(),
            insert: format!(
                "INSERT INTO items (tenant_id,queue_id,item_id,client_item_key,{HOT_COLUMN_NAMES}) \
                 VALUES ({})",
                placeholders(4 + HOT_COLUMN_COUNT)
            ),
            identity_placeholders: 4,
        },
        Layout::B | Layout::C | Layout::E => {
            let key_type = match layout {
                Layout::B => "TEXT",
                Layout::C => "BLOB",
                Layout::E => "INTEGER",
                _ => unreachable!(),
            };
            LayoutSql {
                create_table: format!(
                    "CREATE TABLE items (\n\
                        queue_no INTEGER NOT NULL,\n\
                        row_seq INTEGER NOT NULL,\n\
                        client_item_key {key_type} NOT NULL,\n\
                        {HOT_COLUMNS_DDL},\n\
                        PRIMARY KEY (queue_no, row_seq)\n\
                    )"
                ),
                create_index: "CREATE UNIQUE INDEX items_key_idx ON items (queue_no, client_item_key)"
                    .to_string(),
                insert: format!(
                    "INSERT INTO items (queue_no,row_seq,client_item_key,{HOT_COLUMN_NAMES}) VALUES ({})",
                    placeholders(3 + HOT_COLUMN_COUNT)
                ),
                identity_placeholders: 3,
            }
        }
        Layout::D => LayoutSql {
            create_table: format!(
                "CREATE TABLE items (\n\
                    queue_no INTEGER NOT NULL,\n\
                    row_seq INTEGER NOT NULL,\n\
                    client_item_key TEXT NOT NULL,\n\
                    key_hash BLOB NOT NULL,\n\
                    {HOT_COLUMNS_DDL},\n\
                    PRIMARY KEY (queue_no, row_seq)\n\
                )"
            ),
            create_index: "CREATE UNIQUE INDEX items_key_hash_idx ON items (queue_no, key_hash)"
                .to_string(),
            insert: format!(
                "INSERT INTO items (queue_no,row_seq,client_item_key,key_hash,{HOT_COLUMN_NAMES}) \
                 VALUES ({})",
                placeholders(4 + HOT_COLUMN_COUNT)
            ),
            identity_placeholders: 4,
        },
    }
}

fn placeholders(n: usize) -> String {
    (1..=n).map(|i| format!("?{i}")).collect::<Vec<_>>().join(",")
}

fn identity_values(layout: Layout, shape: Shape, i: u64) -> Vec<Value> {
    match layout {
        Layout::A => vec![
            Value::Text("t0".to_string()),
            Value::Text("q0".to_string()),
            Value::Text(format!("{i:020}")),
            Value::Text(shape.text(i)),
        ],
        Layout::B => vec![
            Value::Integer(1),
            Value::Integer(i as i64),
            Value::Text(shape.text(i)),
        ],
        Layout::C => vec![
            Value::Integer(1),
            Value::Integer(i as i64),
            Value::Blob(shape.blob(i)),
        ],
        Layout::D => {
            let text = shape.text(i);
            let hash = hash16(text.as_bytes());
            vec![
                Value::Integer(1),
                Value::Integer(i as i64),
                Value::Text(text),
                Value::Blob(hash),
            ]
        }
        Layout::E => vec![
            Value::Integer(1),
            Value::Integer(i as i64),
            Value::Integer(shape.int(i).expect("layout E requires the int shape")),
        ],
    }
}

/// WHERE-clause params that identify an existing row by its key, in the
/// order the lookup/upsert-conflict-target queries below expect.
fn lookup_key_values(layout: Layout, shape: Shape, i: u64) -> Vec<Value> {
    match layout {
        Layout::A => vec![
            Value::Text("t0".to_string()),
            Value::Text("q0".to_string()),
            Value::Text(shape.text(i)),
        ],
        Layout::B => vec![Value::Integer(1), Value::Text(shape.text(i))],
        Layout::C => vec![Value::Integer(1), Value::Blob(shape.blob(i))],
        Layout::D => vec![Value::Integer(1), Value::Blob(hash16(shape.text(i).as_bytes()))],
        Layout::E => vec![
            Value::Integer(1),
            Value::Integer(shape.int(i).expect("layout E requires the int shape")),
        ],
    }
}

fn lookup_where(layout: Layout) -> &'static str {
    match layout {
        Layout::A => "tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
        Layout::B | Layout::C | Layout::E => "queue_no=?1 AND client_item_key=?2",
        Layout::D => "queue_no=?1 AND key_hash=?2",
    }
}

fn index_name(layout: Layout) -> &'static str {
    match layout {
        Layout::D => "items_key_hash_idx",
        _ => "items_key_idx",
    }
}

fn conflict_target(layout: Layout) -> &'static str {
    match layout {
        Layout::A => "tenant_id,queue_id,client_item_key",
        Layout::B | Layout::C | Layout::E => "queue_no,client_item_key",
        Layout::D => "queue_no,key_hash",
    }
}

async fn configure_connection(connection: &Connection) -> turso::Result<()> {
    // Mirrors crates/fireweed-turso/src/local.rs:3407-3444 `configure_connection`
    // under the `with_log_backed_projection()` profile that production actually
    // opens (`crates/fireweed/src/turso_compose.rs:136`): rebuildable_io=true,
    // so synchronous=NORMAL and temp_store=MEMORY, not the OFF/absent settings
    // of a from-scratch in-process default.
    connection.pragma_update("page_size", "4096").await?;
    connection.pragma_update("journal_mode", "WAL").await?;
    connection.pragma_update("synchronous", "NORMAL").await?;
    connection.pragma_update("cache_size", "-131072").await?;
    connection.pragma_update("temp_store", "MEMORY").await?;
    connection.pragma_update("wal_autocheckpoint", "0").await?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(())
}

fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn run_combo(layout: Layout, shape: Shape, db_path: &str, rows: u64) -> turso::Result<()> {
    let batch_size = batch_size();
    let lookup_ops = lookup_ops();
    let upsert_ops = upsert_ops();
    let claim_batches = claim_batches();

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));

    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;

    let sql = layout_sql(layout);
    conn.execute(&sql.create_table, ()).await?;
    conn.execute(&sql.create_index, ()).await?;

    let base_ts = now_micros();

    // --- Ingest: `rows` rows in batch_size-row Immediate transactions. ---
    let ingest_start = Instant::now();
    let mut i: u64 = 0;
    while i < rows {
        let batch_end = (i + batch_size as u64).min(rows);
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        {
            let mut stmt = tx.prepare(&sql.insert).await?;
            for row in i..batch_end {
                let mut params = identity_values(layout, shape, row);
                params.extend(hot_column_values(row, base_ts));
                stmt.execute(params).await?;
            }
        }
        tx.commit().await?;
        i = batch_end;
    }
    let ingest_elapsed = ingest_start.elapsed();

    let pre_checkpoint_main = file_len(db_path);
    let pre_checkpoint_wal = file_len(&format!("{db_path}-wal"));
    let checkpoint_start = Instant::now();
    {
        let mut checkpoint_rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
        while checkpoint_rows.next().await?.is_some() {}
    }
    let checkpoint_elapsed = checkpoint_start.elapsed();
    let post_checkpoint_main = file_len(db_path);
    let post_checkpoint_wal = file_len(&format!("{db_path}-wal"));

    // --- Point lookup: lookup_ops single-key equality reads, scattered. ---
    // `INDEXED BY` is required: Turso 0.7.2's query planner does not reliably
    // pick the secondary unique key index over the table's PK autoindex for
    // this shape of predicate (measured — see the results doc "query-plan
    // selection" finding). Production fireweed-relational code would need
    // the same hint (or an upstream planner fix) to get index-seek lookups.
    let lookup_sql = format!(
        "SELECT item_version FROM items INDEXED BY {} WHERE {}",
        index_name(layout),
        lookup_where(layout)
    );
    if env::var("FW_SPIKE_EXPLAIN").is_ok() {
        let mut idx_stmt = conn
            .prepare("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='items'")
            .await?;
        let mut idx_rows = idx_stmt.query(()).await?;
        eprintln!("-- indexes on items");
        while let Some(row) = idx_rows.next().await? {
            eprintln!("   {:?} {:?}", row.get_value(0)?, row.get_value(1)?);
        }
        // Unhinted plan first, to document the planner's default (unforced)
        // choice — this is the finding, not the fix.
        let unhinted_sql = format!("SELECT item_version FROM items WHERE {}", lookup_where(layout));
        let plan_sql = format!("EXPLAIN QUERY PLAN {unhinted_sql}");
        let mut plan_stmt = conn.prepare(&plan_sql).await?;
        let dummy = lookup_key_values(layout, shape, 0);
        let mut plan_rows = plan_stmt.query(dummy).await?;
        eprintln!("-- EXPLAIN QUERY PLAN (no hint) {unhinted_sql}");
        while let Some(row) = plan_rows.next().await? {
            eprintln!("   {:?}", row.get_value(row.column_count() - 1)?);
        }
        let plan_sql2 = format!("EXPLAIN QUERY PLAN {lookup_sql}");
        let mut plan_stmt2 = conn.prepare(&plan_sql2).await?;
        let dummy2 = lookup_key_values(layout, shape, 0);
        let mut plan_rows2 = plan_stmt2.query(dummy2).await?;
        eprintln!("-- EXPLAIN QUERY PLAN (INDEXED BY hint, used by this harness) {lookup_sql}");
        while let Some(row) = plan_rows2.next().await? {
            eprintln!("   {:?}", row.get_value(row.column_count() - 1)?);
        }
    }
    let mut lookup_stmt = conn.prepare(&lookup_sql).await?;
    let lookup_start = Instant::now();
    let mut lookup_hits: u64 = 0;
    for j in 0..lookup_ops as u64 {
        let idx = mix64(j ^ 0xABCD_EF12_3456_7890) % rows;
        let mut rows_out = lookup_stmt.query(lookup_key_values(layout, shape, idx)).await?;
        if rows_out.next().await?.is_some() {
            lookup_hits += 1;
        }
    }
    let lookup_elapsed = lookup_start.elapsed();

    // --- Upsert: upsert_ops INSERT..ON CONFLICT DO UPDATE, all hitting an
    // existing key (the DO UPDATE / conflict path), each attempt using a
    // fresh never-before-used identity so the PK itself never conflicts. ---
    let upsert_sql = format!(
        "INSERT INTO items ({id_cols},{HOT_COLUMN_NAMES}) VALUES ({placeholders}) \
         ON CONFLICT({target}) DO UPDATE SET item_version=excluded.item_version,\
         updated_at=excluded.updated_at,last_command_sequence=excluded.last_command_sequence",
        id_cols = match layout {
            Layout::A => "tenant_id,queue_id,item_id,client_item_key",
            Layout::D => "queue_no,row_seq,client_item_key,key_hash",
            _ => "queue_no,row_seq,client_item_key",
        },
        placeholders = placeholders(sql.identity_placeholders + HOT_COLUMN_COUNT),
        target = conflict_target(layout),
    );
    let upsert_start = Instant::now();
    let mut fresh_seq = rows; // never used by ingest, so the PK never conflicts
    let mut u: u64 = 0;
    while u < upsert_ops as u64 {
        let batch_end = (u + batch_size as u64).min(upsert_ops as u64);
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        {
            let mut stmt = tx.prepare(&upsert_sql).await?;
            for op in u..batch_end {
                let idx = mix64(op ^ 0x1357_9BDF_2468_ACE0) % rows;
                let mut params = match layout {
                    Layout::A => vec![
                        Value::Text("t0".to_string()),
                        Value::Text("q0".to_string()),
                        Value::Text(format!("{fresh_seq:020}")),
                        Value::Text(shape.text(idx)),
                    ],
                    Layout::D => {
                        let text = shape.text(idx);
                        let hash = hash16(text.as_bytes());
                        vec![
                            Value::Integer(1),
                            Value::Integer(fresh_seq as i64),
                            Value::Text(text),
                            Value::Blob(hash),
                        ]
                    }
                    Layout::B => vec![Value::Integer(1), Value::Integer(fresh_seq as i64), Value::Text(shape.text(idx))],
                    Layout::C => vec![Value::Integer(1), Value::Integer(fresh_seq as i64), Value::Blob(shape.blob(idx))],
                    Layout::E => vec![
                        Value::Integer(1),
                        Value::Integer(fresh_seq as i64),
                        Value::Integer(shape.int(idx).expect("layout E requires the int shape")),
                    ],
                };
                params.extend(hot_column_values(fresh_seq, base_ts));
                stmt.execute(params).await?;
                fresh_seq += 1;
            }
        }
        tx.commit().await?;
        u = batch_end;
    }
    let upsert_elapsed = upsert_start.elapsed();

    // --- Claim-style key-list walk: the shape of a contract `Select::Keys`
    // claim (draft §2.3 C2), claim_batches batches of CLAIM_BATCH_SIZE keys
    // each. MEASURED (see FW_SPIKE_EXPLAIN diagnostic and the results doc
    // "IN-list does not seek" finding): a single `client_item_key IN (...)`
    // against the second column of the composite index is NOT compiled into
    // per-value index seeks by Turso 0.7.2, even under an `INDEXED BY` hint
    // — `EXPLAIN QUERY PLAN` still shows only `(queue_no=?)` bound, i.e. a
    // full per-queue scan. One indexed point lookup per key (below) measured
    // ~28x faster at CLAIM_BATCH_SIZE=100 than the single IN-list query, so
    // that is what this harness (and the production recommendation) uses.
    if env::var("FW_SPIKE_EXPLAIN").is_ok() {
        let claim_where = match layout {
            Layout::A => format!(
                "tenant_id=?1 AND queue_id=?2 AND client_item_key IN ({})",
                placeholders_from(3, CLAIM_BATCH_SIZE)
            ),
            Layout::D => format!(
                "queue_no=?1 AND key_hash IN ({})",
                placeholders_from(2, CLAIM_BATCH_SIZE)
            ),
            _ => format!(
                "queue_no=?1 AND client_item_key IN ({})",
                placeholders_from(2, CLAIM_BATCH_SIZE)
            ),
        };
        let claim_sql = format!(
            "SELECT item_version FROM items INDEXED BY {} WHERE {claim_where}",
            index_name(layout)
        );
        let plan_sql = format!("EXPLAIN QUERY PLAN {claim_sql}");
        let mut plan_stmt = conn.prepare(&plan_sql).await?;
        let dummy: Vec<Value> = std::iter::once(Value::Integer(1))
            .chain((0..CLAIM_BATCH_SIZE).map(|_| Value::Text(String::new())))
            .collect();
        let mut plan_rows = plan_stmt.query(dummy).await?;
        eprintln!("-- EXPLAIN QUERY PLAN (claim IN-list, NOT used by this harness's measurement) {claim_sql}");
        while let Some(row) = plan_rows.next().await? {
            eprintln!("   {:?}", row.get_value(row.column_count() - 1)?);
        }
    }
    let claim_key_sql = format!(
        "SELECT item_version FROM items INDEXED BY {} WHERE {}",
        index_name(layout),
        lookup_where(layout)
    );
    let mut claim_stmt = conn.prepare(&claim_key_sql).await?;
    let claim_start = Instant::now();
    let mut claim_hits: u64 = 0;
    for b in 0..claim_batches as u64 {
        for k in 0..CLAIM_BATCH_SIZE as u64 {
            let idx = mix64((b * CLAIM_BATCH_SIZE as u64 + k) ^ 0x0F0F_0F0F_0F0F_0F0F) % rows;
            let mut rows_out = claim_stmt.query(lookup_key_values(layout, shape, idx)).await?;
            if rows_out.next().await?.is_some() {
                claim_hits += 1;
            }
        }
    }
    let claim_elapsed = claim_start.elapsed();

    println!(
        "{{\"layout\":\"{layout:?}\",\"shape\":\"{shape:?}\",\"rows\":{rows},\
         \"ingest_ms\":{ingest_ms},\"ingest_rows_per_sec\":{ingest_rps:.0},\
         \"pre_checkpoint_main_bytes\":{pre_main},\"pre_checkpoint_wal_bytes\":{pre_wal},\
         \"post_checkpoint_main_bytes\":{post_main},\"post_checkpoint_wal_bytes\":{post_wal},\
         \"checkpoint_ms\":{checkpoint_ms},\
         \"lookup_ops\":{lookup_ops},\"lookup_ms\":{lookup_ms},\"lookup_ops_per_sec\":{lookup_ops_per_sec:.0},\"lookup_hits\":{lookup_hits},\
         \"upsert_ops\":{upsert_ops},\"upsert_ms\":{upsert_ms},\"upsert_ops_per_sec\":{upsert_ops_per_sec:.0},\
         \"claim_batches\":{claim_batches},\"claim_batch_size\":{claim_batch_size},\"claim_ms\":{claim_ms},\
         \"claim_keys_per_sec\":{claim_keys_per_sec:.0},\"claim_hits\":{claim_hits}}}",
        layout = layout,
        shape = shape,
        rows = rows,
        ingest_ms = ingest_elapsed.as_secs_f64() * 1000.0,
        ingest_rps = rows as f64 / ingest_elapsed.as_secs_f64(),
        pre_main = pre_checkpoint_main,
        pre_wal = pre_checkpoint_wal,
        post_main = post_checkpoint_main,
        post_wal = post_checkpoint_wal,
        checkpoint_ms = checkpoint_elapsed.as_secs_f64() * 1000.0,
        lookup_ops = lookup_ops,
        lookup_ms = lookup_elapsed.as_secs_f64() * 1000.0,
        lookup_ops_per_sec = lookup_ops as f64 / lookup_elapsed.as_secs_f64(),
        lookup_hits = lookup_hits,
        upsert_ops = upsert_ops,
        upsert_ms = upsert_elapsed.as_secs_f64() * 1000.0,
        upsert_ops_per_sec = upsert_ops as f64 / upsert_elapsed.as_secs_f64(),
        claim_batches = claim_batches,
        claim_batch_size = CLAIM_BATCH_SIZE,
        claim_ms = claim_elapsed.as_secs_f64() * 1000.0,
        claim_keys_per_sec = (claim_batches * CLAIM_BATCH_SIZE) as f64 / claim_elapsed.as_secs_f64(),
        claim_hits = claim_hits,
    );

    drop(claim_stmt);
    drop(lookup_stmt);
    drop(conn);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    Ok(())
}

fn placeholders_from(start: usize, n: usize) -> String {
    (start..start + n).map(|i| format!("?{i}")).collect::<Vec<_>>().join(",")
}

/// Baseline: no secondary unique index at all, so the file-size delta against
/// the matching indexed layout at the same key width estimates the index's
/// own on-disk cost (the `dbstat` virtual table is not implemented by this
/// Turso version — verified: no `dbstat` module registration under
/// vendor/turso_core/ or vendor/turso/src/, only a comment referencing
/// upstream sqlite's dbstat.c).
async fn run_baseline(kind: &str, width: u64, db_path: &str, rows: u64) -> turso::Result<()> {
    let batch_size = batch_size();
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));

    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;

    let key_type = if kind == "int" { "INTEGER" } else { "TEXT" };
    conn.execute(
        &format!(
            "CREATE TABLE items (\n\
                queue_no INTEGER NOT NULL,\n\
                row_seq INTEGER NOT NULL,\n\
                client_item_key {key_type} NOT NULL,\n\
                {HOT_COLUMNS_DDL},\n\
                PRIMARY KEY (queue_no, row_seq)\n\
            )"
        ),
        (),
    )
    .await?;
    let insert_sql = format!(
        "INSERT INTO items (queue_no,row_seq,client_item_key,{HOT_COLUMN_NAMES}) VALUES ({})",
        placeholders(3 + HOT_COLUMN_COUNT)
    );

    let base_ts = now_micros();
    let ingest_start = Instant::now();
    let mut i: u64 = 0;
    while i < rows {
        let batch_end = (i + batch_size as u64).min(rows);
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        {
            let mut stmt = tx.prepare(&insert_sql).await?;
            for row in i..batch_end {
                let key_value = if kind == "int" {
                    Value::Integer((mix64(row) & 0x7FFF_FFFF_FFFF_FFFF) as i64)
                } else {
                    Value::Text(rand_hex_string(row, width as usize))
                };
                let mut params = vec![Value::Integer(1), Value::Integer(row as i64), key_value];
                params.extend(hot_column_values(row, base_ts));
                stmt.execute(params).await?;
            }
        }
        tx.commit().await?;
        i = batch_end;
    }
    let ingest_elapsed = ingest_start.elapsed();
    {
        let mut checkpoint_rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
        while checkpoint_rows.next().await?.is_some() {}
    }
    let size = file_len(db_path);

    println!(
        "{{\"baseline\":\"{kind}\",\"width\":{width},\"rows\":{rows},\
         \"ingest_ms\":{ingest_ms},\"post_checkpoint_main_bytes\":{size}}}",
        ingest_ms = ingest_elapsed.as_secs_f64() * 1000.0,
    );

    drop(conn);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    Ok(())
}

impl std::fmt::Display for Layout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::fmt::Display for Shape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    if args.len() >= 2 && args[1] == "baseline" {
        // spike baseline <text|int> <width> <db_path> <rows>
        let kind = &args[2];
        let width: u64 = args[3].parse().expect("width");
        let db_path = &args[4];
        let rows: u64 = args[5].parse().expect("rows");
        rt.block_on(run_baseline(kind, width, db_path, rows))
            .expect("baseline run failed");
        return;
    }

    // spike <layout> <shape> <db_path> <rows>
    if args.len() < 5 {
        eprintln!(
            "usage: spike <a|b|c|d|e> <int|uuid|email|str128|str256> <db_path> <rows>\n       spike baseline <text|int> <width> <db_path> <rows>"
        );
        std::process::exit(2);
    }
    let layout = Layout::parse(&args[1]);
    let shape = Shape::parse(&args[2]);
    let db_path = &args[3];
    let rows: u64 = args[4].parse().expect("rows");

    if !Path::new(db_path)
        .parent()
        .map(|p| p.as_os_str().is_empty() || p.exists())
        .unwrap_or(true)
    {
        std::fs::create_dir_all(Path::new(db_path).parent().unwrap()).expect("mkdir db dir");
    }

    rt.block_on(run_combo(layout, shape, db_path, rows))
        .expect("combo run failed");
}
