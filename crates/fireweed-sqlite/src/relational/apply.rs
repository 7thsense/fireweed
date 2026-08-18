use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use axon_esf::CompiledSchema;
use fireweed_core::{
    CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityModel, QueueDefinition,
    QueueId, TenantId, UtcTimestamp, is_retry_exhausted,
};
use fireweed_engine::{
    CommandPosition, EngineError, EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    PayloadUpdate, PushItem, QueueCommand, QueueKey, ResolvedItemMutationAction, ScheduleUpdate,
    SetGatesCommand, compile_entity_schema,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};

use super::*;

type UpdateFieldsRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    i64,
    Option<Vec<u8>>,
    String,
);

// ---------------------------------------------------------------------------
// Inner: the durable connection + the queue-definition cache + the live-token map
// ---------------------------------------------------------------------------

pub(crate) struct Inner {
    pub(crate) conn: Connection,
    /// Definitions cache (priority model for `priority_sort`, retry bound). Rebuilt from `queues` on open.
    pub(crate) queues: HashMap<QueueKey, QueueDefinition>,
    /// Compiled entity schemas (ADR-011). Rebuilt from `queues` on open; keyed by queue.
    pub(crate) schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    pub(crate) grouped_shards: HashSet<QueueKey>,
    /// Process-local rowid cursor for high-volume FIFO claim scans. Never persisted; reset on reopen or rich
    /// queue shapes, so correctness comes from the fallback SQL path rather than from the hint.
    pub(crate) claim_scan_hints: HashMap<QueueKey, i64>,
    pub(crate) claim_scan_default_fifo: HashMap<QueueKey, bool>,
    /// Ephemeral live lease tokens (cleartext is never persisted; only the hash is). Lost on reopen.
    /// Item ids are queue-local, so the queue key is part of the identity here as it is in SQLite.
    pub(crate) live_tokens: HashMap<QueueKey, BTreeMap<ItemId, LeaseToken>>,
    pub(crate) live_tokens_by_consumer: HashMap<QueueKey, HashMap<LeaseToken, BTreeSet<ItemId>>>,
}

impl Inner {
    /// Rebuild the in-RAM definition cache from the durable `queues` table. The item projection itself is
    /// already durable in `fireweed_items` as a rebuildable cache - nothing to replay.
    pub(crate) fn reload(&mut self) -> EngineResult<()> {
        let rows: Vec<String> = {
            let mut stmt = st(self.conn.prepare("SELECT definition FROM queues"))?;
            let mapped = st(stmt.query_map([], |row| row.get::<_, String>(0)))?;
            let mut out = Vec::new();
            for r in mapped {
                out.push(st(r)?);
            }
            out
        };
        for def_json in rows {
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(cs) = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?
            {
                self.schemas.insert(key.clone(), cs);
            }
            self.queues.insert(key, definition);
        }
        self.grouped_shards.clear();
        let mut stmt = st(self.conn.prepare(
            "SELECT DISTINCT tenant_id, queue_id FROM fireweed_items WHERE group_key IS NOT NULL",
        ))?;
        let mapped = st(stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }))?;
        for r in mapped {
            let (tenant, queue) = st(r)?;
            self.grouped_shards.insert(QueueKey::new(
                TenantId::new(tenant).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(queue).map_err(|e| EngineError::Storage(e.to_string()))?,
            ));
        }
        // NOTE: item-id restart-safety is handled by `restore_counters` (it seeds `QueueCounters` past the
        // highest durable id, decoding `(epoch, counter)` straight from the packed id — ADR-009).
        Ok(())
    }

    /// Assign the next command sequence for `shard`, apply `command` to `fireweed_items`, and advance the
    /// cursor — all in one transaction (the atomic append+apply UoW the async ports rely on).
    ///
    /// BQ-20/BQ-21/BQ-22 (bead pqueue-7bac12ce): the owner's cached `fence_epoch` is now threaded through
    /// every data-plane port as `expected_epoch`, and this function checks it against the durable cursor
    /// epoch — a stale value is `EpochFenced` (see the `expected_epoch.is_some_and` check below). This
    /// closes the end-to-end fencing gap for the data-plane fast path.
    pub(crate) fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let Inner {
            conn,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            live_tokens,
            live_tokens_by_consumer,
            ..
        } = self;
        let (t, q) = parts(shard);
        let tx = st(conn.transaction())?;
        // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence against the owner's
        // cached acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
        // `None` is the degenerate sole-owner path (no fence). Brings this data-plane path to parity with the
        // typed relational commit seam.
        let (seq, epoch): (i64, i64) = st(tx
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
        let mut token_ops = Vec::new();
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            claim_scan_hints,
            claim_scan_default_fifo,
            &mut token_ops,
            shard,
            &CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
            seq as u64,
            now,
            &command,
        )?;
        st(tx.execute(
            "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
            params![t, q, seq + 1],
        ))?;
        st(tx.commit())?;
        apply_token_ops(live_tokens, live_tokens_by_consumer, token_ops); // only after a durable commit (F4)
        Ok(())
    }
}

// Shared SQLite-family apply. The SQL and command arms live in `fireweed-relational`.
pub(crate) use fireweed_relational::{
    COHORT_EXPIRY_SWEEP_LIMIT, GROUP_DUE_REFRESH_LIMIT, InsertItemSpec, SQLITE_BATCH, TokenOp,
    apply_token_ops, cohort_expiry_deadline, collect_token_ops_from_command,
    finalize_completes_claim, opt_blob, opt_int, opt_text,
};

use super::rusqlite_tx::SqliteRel;

fn rel(conn: &Connection) -> SqliteRel<'_> {
    SqliteRel(conn)
}

pub(crate) fn apply_command_sql(
    tx: &Connection,
    queues: &std::collections::HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut std::collections::HashSet<QueueKey>,
    claim_scan_hints: &mut std::collections::HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut std::collections::HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    position: &CommandPosition,
    seq: u64,
    now: UtcTimestamp,
    command: &QueueCommand,
) -> EngineResult<()> {
    fireweed_relational::apply_command_sql(
        &rel(tx),
        queues,
        grouped_shards,
        claim_scan_hints,
        claim_scan_default_fifo,
        token_ops,
        shard,
        position,
        seq,
        now,
        command,
    )
}

pub(crate) fn insert_item_specs(
    tx: &Connection,
    queues: &std::collections::HashMap<QueueKey, QueueDefinition>,
    model: &fireweed_core::PriorityModel,
    shard: &QueueKey,
    specs: &[InsertItemSpec<'_>],
) -> EngineResult<()> {
    fireweed_relational::insert_item_specs(&rel(tx), queues, model, shard, specs)
}

pub(crate) fn insert_items(
    tx: &Connection,
    queues: &std::collections::HashMap<QueueKey, QueueDefinition>,
    model: &fireweed_core::PriorityModel,
    shard: &QueueKey,
    items: &[PushItem],
    seq: u64,
    now: UtcTimestamp,
) -> EngineResult<()> {
    fireweed_relational::insert_items(&rel(tx), queues, model, shard, items, seq, now)
}

pub(crate) fn reap_terminal_items_sql(
    tx: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    terminal_retention_ms: u64,
    emit_change_records: bool,
    emission_cursor: Option<&CommandPosition>,
) -> EngineResult<Vec<ItemId>> {
    fireweed_relational::reap_terminal_items_sql(
        &rel(tx),
        shard,
        now,
        terminal_retention_ms,
        emit_change_records,
        emission_cursor,
    )
}

pub(crate) fn groups_of(
    tx: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<fireweed_core::GroupKey>> {
    fireweed_relational::groups_of(&rel(tx), shard, ids)
}

pub(crate) fn refresh_group_summaries(
    tx: &Connection,
    shard: &QueueKey,
    group_keys: &[fireweed_core::GroupKey],
    now: UtcTimestamp,
) -> EngineResult<()> {
    fireweed_relational::refresh_group_summaries(&rel(tx), shard, group_keys, now)
}

pub(crate) fn apply_fused_claim_complete_sql(
    tx: &Connection,
    claim_scan_hints: &mut std::collections::HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut std::collections::HashMap<QueueKey, bool>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    finalize_position: &CommandPosition,
    now: UtcTimestamp,
    claim: &fireweed_engine::ClaimCommand,
) -> EngineResult<()> {
    fireweed_relational::apply_fused_claim_complete_sql(
        &rel(tx),
        claim_scan_hints,
        claim_scan_default_fifo,
        token_ops,
        shard,
        finalize_position,
        now,
        claim,
    )
}

#[cfg(test)]
mod class_s_apply_tests {
    use std::collections::{HashMap, HashSet};

    use fireweed_core::{LeaseToken, QueueId, TenantId, UtcTimestamp};
    use fireweed_engine::{ClaimCommand, CommandPosition, QueueCommand, QueueKey};
    use fireweed_relational::RELATIONAL_SCHEMA;
    use rusqlite::Connection;

    use super::*;

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("t").expect("tenant"),
            QueueId::new("q").expect("queue"),
        )
    }

    fn seed_pending(conn: &Connection, item_id: &str) {
        conn.execute_batch(RELATIONAL_SCHEMA).expect("schema");
        conn.execute(
            "INSERT INTO fireweed_items(\
             tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority_sort,\
             eligible_since,payload,fields,metadata,retry_count,item_version,\
             last_command_sequence,created_at,updated_at,fenced,superseded,max_attempts,created_seq) \
             VALUES('t','q',?1,'key','Pending',X'00',1,X'CAFE','{}','{}',0,1,1,1,1,0,0,3,1)",
            [item_id],
        )
        .expect("insert pending");
    }

    fn claim_cmd(item_id: fireweed_core::ItemId, token: &LeaseToken) -> QueueCommand {
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![item_id],
            lease_token: token.clone(),
            lease_expires_at: UtcTimestamp::new(60, 0).expect("expiry"),
            worker_id: None,
        })
    }

    #[test]
    fn apply_claim_twice_does_not_poison() {
        let conn = Connection::open_in_memory().expect("memory");
        let item_id = fireweed_core::ItemId::mint(1, 0, 1);
        seed_pending(&conn, &item_id.to_string());
        let shard = shard();
        let token = LeaseToken::new("token-a").expect("token");
        let now = UtcTimestamp::new(10, 0).expect("now");
        let position = CommandPosition::new(shard.clone(), 1, 2);
        let queues = HashMap::new();
        let mut grouped = HashSet::new();
        let mut hints = HashMap::new();
        let mut fifo = HashMap::new();
        let mut tokens = Vec::new();
        apply_command_sql(
            &conn,
            &queues,
            &mut grouped,
            &mut hints,
            &mut fifo,
            &mut tokens,
            &shard,
            &position,
            2,
            now,
            &claim_cmd(item_id, &token),
        )
        .expect("first claim apply");
        apply_command_sql(
            &conn,
            &queues,
            &mut grouped,
            &mut hints,
            &mut fifo,
            &mut tokens,
            &shard,
            &CommandPosition::new(shard.clone(), 1, 3),
            3,
            now,
            &claim_cmd(item_id, &token),
        )
        .expect("replay claim apply must not poison");
        let state: String = conn
            .query_row(
                "SELECT lifecycle_state FROM fireweed_items WHERE item_id=?1",
                [item_id.to_string()],
                |row| row.get(0),
            )
            .expect("state");
        assert_eq!(state, "Leased");
    }

    #[test]
    fn apply_fused_claim_complete_on_already_leased_does_not_poison() {
        let conn = Connection::open_in_memory().expect("memory");
        let item_id = fireweed_core::ItemId::mint(1, 0, 2);
        seed_pending(&conn, &item_id.to_string());
        let shard = shard();
        let token = LeaseToken::new("token-b").expect("token");
        let now = UtcTimestamp::new(10, 0).expect("now");
        let queues = HashMap::new();
        let mut grouped = HashSet::new();
        let mut hints = HashMap::new();
        let mut fifo = HashMap::new();
        let mut tokens = Vec::new();
        apply_command_sql(
            &conn,
            &queues,
            &mut grouped,
            &mut hints,
            &mut fifo,
            &mut tokens,
            &shard,
            &CommandPosition::new(shard.clone(), 1, 2),
            2,
            now,
            &claim_cmd(item_id, &token),
        )
        .expect("class S-equivalent first lease");
        let claim = ClaimCommand {
            item_ids: vec![item_id],
            lease_token: token,
            lease_expires_at: UtcTimestamp::new(60, 0).expect("expiry"),
            worker_id: None,
        };
        apply_fused_claim_complete_sql(
            &conn,
            &mut hints,
            &mut fifo,
            &mut tokens,
            &shard,
            &CommandPosition::new(shard.clone(), 1, 3),
            now,
            &claim,
        )
        .expect("fused complete of already-leased same token");
        let state: String = conn
            .query_row(
                "SELECT lifecycle_state FROM fireweed_items WHERE item_id=?1",
                [item_id.to_string()],
                |row| row.get(0),
            )
            .expect("state");
        assert_eq!(state, "Complete");
    }
}
