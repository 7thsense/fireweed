#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef, ts};
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, CommandEnvelope, CommandPosition, FinalizeCommand,
    FinalizeKind, FinalizeOutcome, PushCommand, QueueCommand, QueueKey, RenewLeaseCommand,
};
use fireweed_projection::{ProjectionImage, ProjectionImageItem};
use fireweed_relational::{fields_from_json, metadata_from_json, parse_priority, parse_state};
use fireweed_sqlite::AsyncSqliteProjectionStore;
use fireweed_turso::TursoRelational;
use turso::Value;

pub struct Pair {
    pub sqlite: AsyncSqliteProjectionStore,
    pub turso: TursoRelational,
    pub shard: QueueKey,
}

impl Pair {
    pub async fn memory() -> Self {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let sqlite = AsyncSqliteProjectionStore::open(":memory:").await.unwrap();
        let turso = TursoRelational::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&sqlite, definition.clone())
            .await
            .unwrap();
        AsyncProjectionStore::ensure_shard(&turso, definition)
            .await
            .unwrap();
        Self {
            sqlite,
            turso,
            shard,
        }
    }

    pub async fn apply(&self, sequence: u64, command: CommandEnvelope) {
        let position = CommandPosition::new(self.shard.clone(), 0, sequence);
        AsyncProjectionStore::apply_live(
            &self.sqlite,
            vec![position.clone()],
            vec![command.clone()],
        )
        .await
        .unwrap();
        AsyncProjectionStore::apply_live(&self.turso, vec![position], vec![command])
            .await
            .unwrap();
    }

    pub async fn assert_items_equal(&self, ids: &[ItemId]) {
        for id in ids {
            assert_eq!(
                AsyncProjectionStore::item_state(&self.turso, self.shard.clone(), *id)
                    .await
                    .unwrap(),
                AsyncProjectionStore::item_state(&self.sqlite, self.shard.clone(), *id)
                    .await
                    .unwrap(),
                "state mismatch for {id}"
            );
            assert_eq!(
                AsyncProjectionStore::item_version(&self.turso, self.shard.clone(), *id)
                    .await
                    .unwrap(),
                AsyncProjectionStore::item_version(&self.sqlite, self.shard.clone(), *id)
                    .await
                    .unwrap(),
                "version mismatch for {id}"
            );
        }
        assert_eq!(
            AsyncProjectionStore::recovery_high_water(&self.turso, self.shard.clone())
                .await
                .unwrap(),
            AsyncProjectionStore::recovery_high_water(&self.sqlite, self.shard.clone())
                .await
                .unwrap()
        );
        assert_eq!(
            AsyncProjectionStore::recover_definitions(&self.turso)
                .await
                .unwrap(),
            AsyncProjectionStore::recover_definitions(&self.sqlite)
                .await
                .unwrap()
        );
        for now in [ts(0), ts(19), ts(20), ts(30), ts(100)] {
            assert_eq!(
                AsyncProjectionStore::eligible_candidates(
                    &self.turso,
                    self.shard.clone(),
                    now,
                    100,
                )
                .await
                .unwrap(),
                AsyncProjectionStore::eligible_candidates(
                    &self.sqlite,
                    self.shard.clone(),
                    now,
                    100,
                )
                .await
                .unwrap(),
                "eligibility mismatch at {now:?}"
            );
            assert_eq!(
                AsyncProjectionStore::expired_leases(&self.turso, self.shard.clone(), now, 100,)
                    .await
                    .unwrap(),
                AsyncProjectionStore::expired_leases(&self.sqlite, self.shard.clone(), now, 100,)
                    .await
                    .unwrap(),
                "expired-lease mismatch at {now:?}"
            );
        }
        let turso_claimed =
            AsyncProjectionStore::render_claimed(&self.turso, self.shard.clone(), ids.to_vec())
                .await
                .unwrap();
        let sqlite_claimed =
            AsyncProjectionStore::render_claimed(&self.sqlite, self.shard.clone(), ids.to_vec())
                .await
                .unwrap();
        assert_eq!(turso_claimed.len(), sqlite_claimed.len());
        for (turso, sqlite) in turso_claimed.iter().zip(&sqlite_claimed) {
            assert_eq!(turso.item_id, sqlite.item_id);
            assert_eq!(turso.client_item_key, sqlite.client_item_key);
            assert_eq!(turso.item_version, sqlite.item_version);
            assert_eq!(turso.priority, sqlite.priority);
            assert_eq!(turso.group_key, sqlite.group_key);
            assert_eq!(turso.not_before, sqlite.not_before);
            assert_eq!(turso.lease_token, sqlite.lease_token);
            assert_eq!(turso.lease_expires_at, sqlite.lease_expires_at);
            assert_eq!(turso.attempt_count, sqlite.attempt_count);
            assert_eq!(turso.payload, sqlite.payload);
            assert_eq!(turso.fields, sqlite.fields);
            assert_eq!(turso.metadata, sqlite.metadata);
            assert_eq!(turso.gate_keys, sqlite.gate_keys);
        }
    }

    pub async fn assert_projection_image_and_reads_equal(&self, ids: &[ItemId]) {
        self.assert_items_equal(ids).await;
        let sqlite = self
            .sqlite
            .export_projection_image(self.shard.clone())
            .await
            .unwrap();
        let turso = turso_projection_image(&self.turso, &self.shard).await;
        assert_eq!(turso, sqlite, "complete ProjectionImage mismatch");
    }
}

fn integer(value: &Value) -> i64 {
    match value {
        Value::Integer(value) => *value,
        other => panic!("expected integer, got {other:?}"),
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn optional_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(value) => Some(value.clone()),
        other => panic!("expected nullable text, got {other:?}"),
    }
}

fn optional_integer(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::Integer(value) => Some(*value),
        other => panic!("expected nullable integer, got {other:?}"),
    }
}

fn optional_blob(value: &Value) -> Option<Bytes> {
    match value {
        Value::Null => None,
        Value::Blob(value) => Some(Bytes::copy_from_slice(value)),
        other => panic!("expected nullable blob, got {other:?}"),
    }
}

fn nanos(value: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
    .unwrap()
}

async fn turso_projection_image(store: &TursoRelational, shard: &QueueKey) -> ProjectionImage {
    let tenant = Value::Text(shard.tenant_id.as_str().to_string());
    let queue = Value::Text(shard.queue_id.as_str().to_string());
    let cursor = store
        .query(
            "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=?1 AND queue=?2",
            vec![tenant.clone(), queue.clone()],
        )
        .await
        .unwrap();
    let cursor = &cursor[0].values;
    let next_seq = integer(&cursor[0]) as u64;
    let next_item_seq = integer(&cursor[1]) as u64;
    let assignment_epoch = integer(&cursor[2]) as u64;
    let queue_row = store
        .query(
            "SELECT paused,pause_drain_intake FROM queues WHERE tenant=?1 AND queue=?2",
            vec![tenant.clone(), queue.clone()],
        )
        .await
        .unwrap();
    let queue_row = &queue_row[0].values;

    let gate_rows = store
        .query(
            "SELECT item_id,gate_key FROM fireweed_item_gates WHERE tenant_id=?1 AND queue_id=?2 \
             ORDER BY item_id,gate_key",
            vec![tenant.clone(), queue.clone()],
        )
        .await
        .unwrap();
    let mut gates: HashMap<String, Vec<String>> = HashMap::new();
    for row in gate_rows {
        gates
            .entry(text(&row.values[0]))
            .or_default()
            .push(text(&row.values[1]));
    }

    let rows = store
        .query(
            "SELECT item_id,client_item_key,lifecycle_state,priority,not_before,eligible_since,group_key,cohort_size,payload,\
             fields,metadata,entity_document,retry_count,item_version,lease_expires_at,worker_id,\
             fenced,superseded,max_attempts,created_seq FROM fireweed_items \
             WHERE tenant_id=?1 AND queue_id=?2 ORDER BY created_seq,item_id",
            vec![tenant.clone(), queue.clone()],
        )
        .await
        .unwrap();
    let items = rows
        .into_iter()
        .map(|row| {
            let values = row.values;
            let item_id_text = text(&values[0]);
            ProjectionImageItem {
                item_id: ItemId::new(&item_id_text).unwrap(),
                client_item_key: ClientItemKey::new(text(&values[1])).unwrap(),
                state: parse_state(&text(&values[2])).unwrap(),
                priority: parse_priority(optional_text(&values[3])).unwrap(),
                not_before: optional_integer(&values[4]).map(nanos),
                eligible_since: optional_integer(&values[5]).map(nanos),
                group_key: optional_text(&values[6]).map(|value| GroupKey::new(value).unwrap()),
                cohort_size: optional_integer(&values[7]).map(|value| value as u64),
                payload: optional_blob(&values[8]),
                fields: fields_from_json(text(&values[9])).unwrap(),
                metadata: metadata_from_json(text(&values[10])).unwrap(),
                gate_keys: gates.remove(&item_id_text).unwrap_or_default(),
                entity_document: optional_text(&values[11])
                    .map(|value| serde_json::from_str(&value).unwrap()),
                attempt_count: integer(&values[12]) as u32,
                item_version: integer(&values[13]) as u64,
                lease_token: None,
                lease_expires_at: optional_integer(&values[14]).map(nanos),
                lease_is_cohort: optional_integer(&values[7]).is_some(),
                worker_id: optional_text(&values[15]).map(|value| WorkerId::new(value).unwrap()),
                fenced: integer(&values[16]) != 0,
                superseded: integer(&values[17]) != 0,
                max_attempts: integer(&values[18]) as u32,
                created_seq: integer(&values[19]) as u64,
                terminal_at: None,
                terminal_position: None,
            }
        })
        .collect();

    let mut side_records = BTreeMap::new();
    for row in store
        .query(
            "SELECT key,payload FROM fireweed_side_records WHERE tenant_id=?1 AND queue_id=?2 ORDER BY key",
            vec![tenant.clone(), queue.clone()],
        )
        .await
        .unwrap()
    {
        let [Value::Blob(key), Value::Blob(payload)] = row.values.as_slice() else {
            panic!("invalid side-record row")
        };
        side_records.insert(key.clone(), Bytes::copy_from_slice(payload));
    }
    let mut instance_fences = BTreeMap::new();
    for row in store
        .query(
            "SELECT instance_key,fence FROM fireweed_instance_fences WHERE tenant_id=?1 AND queue_id=?2 ORDER BY instance_key",
            vec![tenant, queue],
        )
        .await
        .unwrap()
    {
        let Value::Blob(key) = &row.values[0] else {
            panic!("invalid instance-fence key")
        };
        instance_fences.insert(key.clone(), integer(&row.values[1]) as u64);
    }
    ProjectionImage {
        high_water: (next_seq > 0).then(|| {
            CommandPosition::new(shard.clone(), assignment_epoch, next_seq.saturating_sub(1))
        }),
        paused: integer(&queue_row[0]) != 0,
        pause_drain_intake: integer(&queue_row[1]) != 0,
        blocked_gates: Default::default(),
        next_seq: next_item_seq,
        items,
        side_records,
        instance_fences,
        metrics: store.server_metrics(shard).await.unwrap(),
    }
}

pub fn lifecycle(id: ItemId) -> Vec<CommandEnvelope> {
    let token = LeaseToken::new("differential-lease").unwrap();
    vec![
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(&id.to_string(), "differential-key", 7)],
            }),
            vec![id],
        ),
        envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![id],
                lease_token: token,
                lease_expires_at: ts(20),
                worker_id: None,
            }),
            vec![id],
        ),
        envelope(
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![id],
                lease_expires_at: ts(30),
            }),
            vec![id],
        ),
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
            }),
            vec![id],
        ),
    ]
}

pub async fn assert_state<S: AsyncProjectionStore>(
    store: &S,
    shard: QueueKey,
    id: ItemId,
    expected: Option<ItemState>,
) {
    assert_eq!(
        AsyncProjectionStore::item_state(store, shard, id)
            .await
            .unwrap(),
        expected
    );
}
