//! Unit tests for the durable SQLite `LogStore` (TD-005).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{ClientItemKey, ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_sqlite::log::SqliteLogStore;
use pqueue_storage::commands::{
    BatchPushCommand, CommandEnvelope, CommandId, PushItem, QueueCommand,
};
use pqueue_storage::traits::{CommandPage, DurabilityProfile, LogStore, LogStoreError};
use pqueue_storage::types::{CommandChecksum, ShardId, ShardKey};

static UNIQUE: AtomicU64 = AtomicU64::new(0);

fn temp_db_path() -> PathBuf {
    let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("sqlite-log-{}-{n}.db", std::process::id()))
}

fn shard(tenant: &str, queue: &str, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        shard_id: ShardId::new(shard_id),
    }
}

fn push_envelope(shard: &ShardKey, index: u64) -> CommandEnvelope {
    let item_id = ItemId::new(format!("itm-{index}")).unwrap();
    CommandEnvelope {
        command_id: CommandId::new(format!("cmd-{index}")),
        request_id: None,
        tenant_id: shard.tenant_id.clone(),
        queue_id: shard.queue_id.clone(),
        shard_id: shard.shard_id.clone(),
        item_ids: vec![item_id.clone()],
        command: QueueCommand::BatchPush(BatchPushCommand {
            items: vec![PushItem {
                client_item_key: ClientItemKey::new(format!("key-{index}")).unwrap(),
                item_id,
                priority: None,
                not_before: None,
                max_attempts: 3,
                payload: None,
            }],
        }),
        checksum: CommandChecksum(index as u32),
        created_at: UtcTimestamp::new(index as i64, 0).unwrap(),
    }
}

async fn read_all(store: &SqliteLogStore, shard: &ShardKey) -> CommandPage {
    store.read_from(shard, None, usize::MAX).await.unwrap()
}

#[tokio::test]
async fn append_then_read_round_trips_commands() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);

    let result = store
        .append_batch(
            &s,
            Some(0),
            vec![push_envelope(&s, 0), push_envelope(&s, 1)],
        )
        .await
        .unwrap();
    assert_eq!(result.last_position.sequence, 1);
    assert_eq!(result.last_position.backend_epoch, 0);

    let page = read_all(&store, &s).await;
    assert_eq!(page.commands.len(), 2);
    assert_eq!(page.commands[0].0.sequence, 0);
    assert_eq!(page.commands[1].0.sequence, 1);
    // The decoded command id survives the codec round-trip.
    assert_eq!(page.commands[0].1.command_id.0, "cmd-0");
    assert!(matches!(
        page.commands[1].1.command,
        QueueCommand::BatchPush(_)
    ));
    assert!(page.next_position.is_none());
}

#[tokio::test]
async fn sequence_is_monotonic_across_appends() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);
    store
        .append_batch(&s, None, vec![push_envelope(&s, 0)])
        .await
        .unwrap();
    let second = store
        .append_batch(&s, None, vec![push_envelope(&s, 1), push_envelope(&s, 2)])
        .await
        .unwrap();
    assert_eq!(second.last_position.sequence, 2);
    let page = read_all(&store, &s).await;
    let seqs: Vec<u64> = page.commands.iter().map(|(p, _)| p.sequence).collect();
    assert_eq!(seqs, vec![0, 1, 2]);
}

#[tokio::test]
async fn stale_epoch_append_is_rejected() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);
    store
        .append_batch(&s, Some(0), vec![push_envelope(&s, 0)])
        .await
        .unwrap();
    store.set_shard_epoch(&s, 2).unwrap();

    let err = store
        .append_batch(&s, Some(1), vec![push_envelope(&s, 1)])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        LogStoreError::StalEpoch {
            expected: 1,
            current: 2
        }
    );

    // Correct epoch succeeds and records the new backend_epoch.
    let ok = store
        .append_batch(&s, Some(2), vec![push_envelope(&s, 1)])
        .await
        .unwrap();
    assert_eq!(ok.last_position.backend_epoch, 2);
}

#[tokio::test]
async fn read_unknown_shard_is_shard_not_found() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 7);
    let err = store.read_from(&s, None, 10).await.unwrap_err();
    assert_eq!(err, LogStoreError::ShardNotFound);
}

#[tokio::test]
async fn read_paginates_with_next_position() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);
    for i in 0..5 {
        store
            .append_batch(&s, None, vec![push_envelope(&s, i)])
            .await
            .unwrap();
    }
    let page1 = store.read_from(&s, None, 2).await.unwrap();
    assert_eq!(page1.commands.len(), 2);
    let next = page1.next_position.expect("more remain");
    assert_eq!(next.sequence, 1);

    let page2 = store.read_from(&s, Some(next), 2).await.unwrap();
    assert_eq!(page2.commands.len(), 2);
    assert_eq!(page2.commands[0].0.sequence, 2);
    let next2 = page2.next_position.expect("more remain");

    let page3 = store.read_from(&s, Some(next2), 2).await.unwrap();
    assert_eq!(page3.commands.len(), 1);
    assert_eq!(page3.commands[0].0.sequence, 4);
    assert!(page3.next_position.is_none());
}

#[tokio::test]
async fn durability_profile_reflects_backing() {
    assert_eq!(
        SqliteLogStore::open_in_memory()
            .unwrap()
            .durability_profile(),
        DurabilityProfile::None
    );
    let path = temp_db_path();
    let store = SqliteLogStore::open(&path).unwrap();
    assert_eq!(store.durability_profile(), DurabilityProfile::LocalDisk);
    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn empty_append_creates_shard_without_commands() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);
    let result = store.append_batch(&s, Some(0), vec![]).await.unwrap();
    assert_eq!(result.last_position.sequence, 0);
    // The shard now exists: read returns an empty page, not ShardNotFound.
    let page = read_all(&store, &s).await;
    assert!(page.commands.is_empty());
    assert!(page.next_position.is_none());
}

#[tokio::test]
async fn read_from_tail_position_is_empty_not_error() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let s = shard("t", "q", 0);
    let appended = store
        .append_batch(&s, None, vec![push_envelope(&s, 0), push_envelope(&s, 1)])
        .await
        .unwrap();
    // Reading from the last position (the "am I caught up?" poll) is an empty
    // page with no next_position — must not panic or error.
    let page = store
        .read_from(&s, Some(appended.last_position), 10)
        .await
        .unwrap();
    assert!(page.commands.is_empty());
    assert!(page.next_position.is_none());
}

#[tokio::test]
async fn shards_have_independent_sequence_spaces() {
    let store = SqliteLogStore::open_in_memory().unwrap();
    let a = shard("t", "q", 0);
    let b = shard("t", "q", 1);
    store
        .append_batch(&a, None, vec![push_envelope(&a, 0), push_envelope(&a, 1)])
        .await
        .unwrap();
    store
        .append_batch(&b, None, vec![push_envelope(&b, 0)])
        .await
        .unwrap();

    let page_a = read_all(&store, &a).await;
    let page_b = read_all(&store, &b).await;
    assert_eq!(page_a.commands.len(), 2);
    assert_eq!(page_b.commands.len(), 1);
    // Each shard restarts its own sequence at 0; no bleed across shards.
    assert_eq!(page_b.commands[0].0.sequence, 0);
    assert_eq!(page_b.commands[0].0.shard_key, b);
    assert!(page_a.commands.iter().all(|(p, _)| p.shard_key == a));
}

#[tokio::test]
async fn reopen_file_recovers_committed_log() {
    let path = temp_db_path();
    let s = shard("t", "q", 0);
    {
        let store = SqliteLogStore::open(&path).unwrap();
        store
            .append_batch(
                &s,
                Some(0),
                vec![push_envelope(&s, 0), push_envelope(&s, 1)],
            )
            .await
            .unwrap();
    } // drop closes the connection

    let reopened = SqliteLogStore::open(&path).unwrap();
    let page = read_all(&reopened, &s).await;
    assert_eq!(
        page.commands
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(page.commands[0].1.command_id.0, "cmd-0");
    // Appends after reopen continue the sequence (durable max+1).
    let appended = reopened
        .append_batch(&s, Some(0), vec![push_envelope(&s, 2)])
        .await
        .unwrap();
    assert_eq!(appended.last_position.sequence, 2);

    drop(reopened);
    let _ = std::fs::remove_file(&path);
}
