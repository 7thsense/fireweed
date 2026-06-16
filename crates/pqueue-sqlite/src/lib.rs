#![forbid(unsafe_code)]

use std::collections::VecDeque;

use pqueue_storage::types::ShardKey;
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GroupSummary {
    pub group_key: Option<String>,
    pub oldest_eligible_at_ms: i64,
    pub eligible_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CohortRow {
    pub group_key: String,
    pub member_count: u64,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyLagStatus {
    pub committed_sequence: u64,
    pub applied_sequence: u64,
    pub lag_sequences: u64,
    pub within_bound: bool,
}

pub struct SqliteProjection {
    conn: Connection,
    shard_key: ShardKey,
}

impl SqliteProjection {
    pub fn new_in_memory(shard_key: ShardKey) -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        initialize_schema(&conn)?;
        Ok(Self { conn, shard_key })
    }

    pub fn shard_key(&self) -> &ShardKey {
        &self.shard_key
    }

    pub fn insert_item(
        &self,
        item_id: &str,
        group_key: Option<&str>,
        gate_key: Option<&str>,
        eligible_since_ms: i64,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO pqueue_items (
                item_id, group_key, gate_key, lifecycle_state, eligible_since_ms
             ) VALUES (?1, ?2, ?3, 'pending', ?4)",
            params![item_id, group_key, gate_key, eligible_since_ms],
        )?;
        Ok(())
    }

    pub fn set_gate(&self, gate_key: &str, blocked: bool) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO pqueue_gate_state (gate_key, blocked)
             VALUES (?1, ?2)
             ON CONFLICT(gate_key) DO UPDATE SET blocked = excluded.blocked",
            params![gate_key, i64::from(blocked)],
        )?;
        Ok(())
    }

    pub fn insert_cohort(
        &self,
        group_key: &str,
        member_count: u64,
        state: &str,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO pqueue_cohorts (group_key, member_count, state)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(group_key) DO UPDATE
             SET member_count = excluded.member_count, state = excluded.state",
            params![group_key, member_count as i64, state],
        )?;
        Ok(())
    }

    pub fn recompute_group_summary(&self) -> rusqlite::Result<()> {
        self.conn.execute("DELETE FROM pqueue_group_summary", [])?;
        self.conn.execute(
            "INSERT INTO pqueue_group_summary (
                tenant_id, queue_id, shard_id, group_key, oldest_eligible_at_ms, eligible_count
             )
             SELECT ?1, ?2, ?3, i.group_key, MIN(i.eligible_since_ms), COUNT(*)
             FROM pqueue_items i
             LEFT JOIN pqueue_gate_state g ON g.gate_key = i.gate_key
             WHERE i.lifecycle_state = 'pending'
               AND COALESCE(g.blocked, 0) = 0
             GROUP BY i.group_key",
            params![
                self.shard_key.tenant_id.as_str(),
                self.shard_key.queue_id.as_str(),
                self.shard_key.shard_id.as_u32() as i64
            ],
        )?;
        Ok(())
    }

    pub fn group_summary(&self, group_key: Option<&str>) -> rusqlite::Result<Option<GroupSummary>> {
        self.conn
            .query_row(
                "SELECT group_key, oldest_eligible_at_ms, eligible_count
                 FROM pqueue_group_summary
                 WHERE (group_key IS ?1 OR group_key = ?1)",
                params![group_key],
                |row| {
                    Ok(GroupSummary {
                        group_key: row.get(0)?,
                        oldest_eligible_at_ms: row.get(1)?,
                        eligible_count: row.get::<_, i64>(2)? as u64,
                    })
                },
            )
            .optional()
    }

    pub fn cohort(&self, group_key: &str) -> rusqlite::Result<Option<CohortRow>> {
        self.conn
            .query_row(
                "SELECT group_key, member_count, state
                 FROM pqueue_cohorts
                 WHERE group_key = ?1",
                params![group_key],
                |row| {
                    Ok(CohortRow {
                        group_key: row.get(0)?,
                        member_count: row.get::<_, i64>(1)? as u64,
                        state: row.get(2)?,
                    })
                },
            )
            .optional()
    }

    pub fn set_applied_sequence(&self, sequence: u64) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE pqueue_applied_position SET sequence = ?1 WHERE id = 1",
            params![sequence as i64],
        )?;
        Ok(())
    }

    pub fn applied_sequence(&self) -> rusqlite::Result<u64> {
        self.conn
            .query_row(
                "SELECT sequence FROM pqueue_applied_position WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|value| value as u64)
    }

    pub fn apply_tail_sequences(&self, sequences: &[u64]) -> rusqlite::Result<Vec<u64>> {
        let applied = self.applied_sequence()?;
        let mut applied_tail = Vec::new();
        for sequence in sequences
            .iter()
            .copied()
            .filter(|sequence| *sequence > applied)
        {
            self.set_applied_sequence(sequence)?;
            applied_tail.push(sequence);
        }
        Ok(applied_tail)
    }

    pub fn apply_before_return(&self, committed_sequence: u64) -> rusqlite::Result<u64> {
        let applied = self.applied_sequence()?;
        if committed_sequence > applied {
            self.set_applied_sequence(committed_sequence)?;
        }
        self.applied_sequence()
    }

    pub fn apply_lag_status(
        &self,
        committed_sequence: u64,
        max_lag_sequences: u64,
    ) -> rusqlite::Result<ApplyLagStatus> {
        let applied_sequence = self.applied_sequence()?;
        let lag_sequences = committed_sequence.saturating_sub(applied_sequence);
        Ok(ApplyLagStatus {
            committed_sequence,
            applied_sequence,
            lag_sequences,
            within_bound: lag_sequences <= max_lag_sequences,
        })
    }

    pub fn snapshot_bytes(&self) -> rusqlite::Result<Vec<u8>> {
        let snapshot = ProjectionSnapshot {
            applied_sequence: self.applied_sequence()?,
            groups: self.all_group_summaries()?,
            cohorts: self.all_cohorts()?,
        };
        serde_json::to_vec(&snapshot)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(err.into()))
    }

    pub fn restore_from_snapshot(shard_key: ShardKey, snapshot: &[u8]) -> rusqlite::Result<Self> {
        let restored: ProjectionSnapshot = serde_json::from_slice(snapshot)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(err.into()))?;
        let projection = Self::new_in_memory(shard_key)?;
        projection.set_applied_sequence(restored.applied_sequence)?;
        for group in restored.groups {
            projection.conn.execute(
                "INSERT INTO pqueue_group_summary (
                    tenant_id, queue_id, shard_id, group_key, oldest_eligible_at_ms, eligible_count
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    projection.shard_key.tenant_id.as_str(),
                    projection.shard_key.queue_id.as_str(),
                    projection.shard_key.shard_id.as_u32() as i64,
                    group.group_key,
                    group.oldest_eligible_at_ms,
                    group.eligible_count as i64
                ],
            )?;
        }
        for cohort in restored.cohorts {
            projection.insert_cohort(&cohort.group_key, cohort.member_count, &cohort.state)?;
        }
        Ok(projection)
    }

    fn all_group_summaries(&self) -> rusqlite::Result<Vec<GroupSummary>> {
        let mut stmt = self.conn.prepare(
            "SELECT group_key, oldest_eligible_at_ms, eligible_count
             FROM pqueue_group_summary
             ORDER BY group_key",
        )?;
        stmt.query_map([], |row| {
            Ok(GroupSummary {
                group_key: row.get(0)?,
                oldest_eligible_at_ms: row.get(1)?,
                eligible_count: row.get::<_, i64>(2)? as u64,
            })
        })?
        .collect()
    }

    fn all_cohorts(&self) -> rusqlite::Result<Vec<CohortRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT group_key, member_count, state
             FROM pqueue_cohorts
             ORDER BY group_key",
        )?;
        stmt.query_map([], |row| {
            Ok(CohortRow {
                group_key: row.get(0)?,
                member_count: row.get::<_, i64>(1)? as u64,
                state: row.get(2)?,
            })
        })?
        .collect()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProjectionSnapshot {
    applied_sequence: u64,
    groups: Vec<GroupSummary>,
    cohorts: Vec<CohortRow>,
}

pub struct ProjectionHandleCache {
    capacity: usize,
    handles: VecDeque<ShardKey>,
}

impl ProjectionHandleCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            handles: VecDeque::new(),
        }
    }

    pub fn touch(&mut self, shard_key: ShardKey) {
        if self.capacity == 0 {
            return;
        }
        if let Some(index) = self
            .handles
            .iter()
            .position(|existing| existing == &shard_key)
        {
            self.handles.remove(index);
        }
        self.handles.push_front(shard_key);
        while self.handles.len() > self.capacity {
            self.handles.pop_back();
        }
    }

    pub fn contains(&self, shard_key: &ShardKey) -> bool {
        self.handles.iter().any(|existing| existing == shard_key)
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }
}

fn initialize_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE pqueue_items (
            item_id TEXT PRIMARY KEY,
            group_key TEXT,
            gate_key TEXT,
            lifecycle_state TEXT NOT NULL,
            eligible_since_ms INTEGER NOT NULL
        );
        CREATE TABLE pqueue_group_summary (
            tenant_id TEXT NOT NULL,
            queue_id TEXT NOT NULL,
            shard_id INTEGER NOT NULL,
            group_key TEXT,
            oldest_eligible_at_ms INTEGER NOT NULL,
            eligible_count INTEGER NOT NULL,
            UNIQUE (tenant_id, queue_id, shard_id, group_key)
        );
        CREATE TABLE pqueue_gate_state (
            gate_key TEXT PRIMARY KEY,
            blocked INTEGER NOT NULL
        );
        CREATE TABLE pqueue_cohorts (
            group_key TEXT PRIMARY KEY,
            member_count INTEGER NOT NULL,
            state TEXT NOT NULL
        );
        CREATE TABLE pqueue_applied_position (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            sequence INTEGER NOT NULL
        );
        INSERT INTO pqueue_applied_position (id, sequence) VALUES (1, 0);",
    )
}
