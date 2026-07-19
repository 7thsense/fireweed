#![allow(dead_code)]

use pqueue_conformance::{envelope, item, qdef, ts};
use pqueue_core::{ItemId, ItemState, LeaseToken};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCommand, CommandEnvelope, CommandPosition, FinalizeCommand,
    FinalizeKind, FinalizeOutcome, PushCommand, QueueCommand, QueueKey, RenewLeaseCommand,
};
use pqueue_sqlite::AsyncSqliteProjectionStore;
use pqueue_turso::TursoRelational;

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
