//! Terminal items on the production object-log and Turso path, using the local object log.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, EligibilityPolicy, ItemState, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use fireweed_engine::{AsyncProjectionSpec, EngineError, QueueKey};
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};

use crate::{Fireweed, ManualClock, NewItem, RuntimeCore, turso_compose};

const TARGET_BYTES: usize = 262_144;
const KEY_RETENTION_SECONDS: i64 = 3_600;

fn queue() -> QueueKey {
    QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant").unwrap(),
        queue_id: QueueId::new("queue").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: (KEY_RETENTION_SECONDS as u64) * 1_000,
        terminal_retention_ms: 1,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: Vec::new(),
        entity_schema: None,
        typed_indexes: Vec::new(),
        emit_change_records: false,
    }
}

fn item(key: &str) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        payload: Some(Bytes::from(format!("payload-{key}"))),
        ..NewItem::default()
    }
}

struct Fixture {
    root: PathBuf,
    clock: Arc<ManualClock>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "fireweed-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root,
            clock: Arc::new(ManualClock::at(1_000)),
        }
    }

    fn log_root(&self) -> PathBuf {
        self.root.join("log")
    }

    fn projection(&self) -> PathBuf {
        self.root.join("projection.turso")
    }

    async fn open(&self) -> Fireweed {
        let log = ObjectLogEngineStore::open_local(
            self.log_root(),
            flush_config_from_segment(TARGET_BYTES, 0),
        )
        .await
        .unwrap();
        let backend = turso_compose::assemble_objectlog_turso_async(
            log,
            self.projection(),
            Some(AsyncProjectionSpec::default()),
        )
        .await
        .unwrap();
        Fireweed::from_runtime(RuntimeCore::new(Arc::new(backend), self.clock.clone()))
    }

    async fn rebuild(&self) -> u64 {
        turso_compose::rebuild_filesystem_turso_projection(
            self.log_root(),
            self.projection(),
            TARGET_BYTES,
            0,
            Some(AsyncProjectionSpec::default()),
            u64::MAX,
        )
        .await
        .unwrap()
        .tail_commands_replayed
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn complete_one(fireweed: &Fireweed, key: &str) -> fireweed_core::ItemId {
    let id = fireweed.push(&queue(), item(key)).await.unwrap();
    let claimed = fireweed.claim(&queue(), 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, id);
    fireweed.ack(&queue(), vec![id]).await.unwrap();
    id
}

fn retained_keys(items: &[fireweed_engine::RetainedItemView]) -> Vec<&str> {
    items
        .iter()
        .map(|view| view.client_item_key.as_str())
        .collect()
}

async fn retained(fireweed: &Fireweed) -> Vec<fireweed_engine::RetainedItemView> {
    fireweed.retained_items(&queue(), None, 1000).await.unwrap()
}

/// Question a: nothing removes a terminal item on this path except an explicit purge.
#[tokio::test(flavor = "multi_thread")]
async fn terminal_items_stay_until_purged() {
    let fixture = Fixture::new("terminal-stay");
    let fireweed = fixture.open().await;
    fireweed.create_queue(definition()).await.unwrap();
    let id = complete_one(&fireweed, "done").await;

    // Far past terminal_retention_ms, and well past key retention too.
    fixture.clock.set(1_000 + KEY_RETENTION_SECONDS * 2);
    fireweed.reclaim_expired(&queue(), None).await.unwrap();

    let items = retained(&fireweed).await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].item_id, id);
    assert_eq!(items[0].lifecycle_state, ItemState::Complete);
    assert_eq!(fireweed.metrics(&queue()).await.unwrap().complete, 1);
}

/// Question c: the existing page returns terminal rows with payload, mixed with live rows, in
/// numeric item-id order.
#[tokio::test(flavor = "multi_thread")]
async fn retained_items_page_returns_terminal_payloads_in_item_id_order() {
    let fixture = Fixture::new("terminal-page");
    let fireweed = fixture.open().await;
    fireweed.create_queue(definition()).await.unwrap();
    let finished = complete_one(&fireweed, "done").await;
    let waiting = fireweed.push(&queue(), item("waiting")).await.unwrap();
    let late = fireweed.push(&queue(), item("late")).await.unwrap();

    let page = fireweed.retained_items(&queue(), None, 2).await.unwrap();
    assert_eq!(retained_keys(&page), ["done", "waiting"]);
    assert_eq!(page[0].item_id, finished);
    assert_eq!(page[0].lifecycle_state, ItemState::Complete);
    assert_eq!(page[0].payload.as_deref(), Some(&b"payload-done"[..]));
    assert_eq!(page[1].lifecycle_state, ItemState::Pending);

    let next = fireweed
        .retained_items(&queue(), Some(waiting), 2)
        .await
        .unwrap();
    assert_eq!(retained_keys(&next), ["late"]);
    assert_eq!(next[0].item_id, late);

    // A low item id that finishes after a cursor passed it is behind that cursor.
    let claimed = fireweed.claim(&queue(), 1, 30_000).await.unwrap();
    assert_eq!(claimed[0].item_id, waiting);
    fireweed.ack(&queue(), vec![waiting]).await.unwrap();
    let after_cursor = fireweed
        .retained_items(&queue(), Some(waiting), 2)
        .await
        .unwrap();
    assert_eq!(retained_keys(&after_cursor), ["late"]);
}

/// Questions b and C2: a non-forced purge removes the item and keeps its key refused, live and
/// after the projection is rebuilt from the log, until the original retention expires.
#[tokio::test(flavor = "multi_thread")]
async fn purge_tombstone_survives_log_replay_and_expires_on_its_original_clock() {
    let fixture = Fixture::new("terminal-purge");
    let fireweed = fixture.open().await;
    fireweed.create_queue(definition()).await.unwrap();
    let id = complete_one(&fireweed, "done").await;
    let live = fireweed.push(&queue(), item("live")).await.unwrap();

    assert_eq!(fireweed.purge(&queue(), vec![id], false).await.unwrap(), 1);
    assert_eq!(retained_keys(&retained(&fireweed).await), ["live"]);
    assert!(matches!(
        fireweed.push(&queue(), item("done")).await,
        Err(EngineError::Conflict)
    ));

    fireweed.shutdown().await.unwrap();
    drop(fireweed);
    assert!(fixture.rebuild().await > 0);

    fixture.clock.set(1_000 + KEY_RETENTION_SECONDS - 1);
    let reopened = fixture.open().await;
    assert_eq!(retained_keys(&retained(&reopened).await), ["live"]);
    assert!(matches!(
        reopened.push(&queue(), item("done")).await,
        Err(EngineError::Conflict)
    ));

    fixture.clock.set(1_000 + KEY_RETENTION_SECONDS + 1);
    let fresh = reopened.push(&queue(), item("done")).await.unwrap();
    assert_ne!(fresh, id);
    assert_eq!(retained_keys(&retained(&reopened).await), ["live", "done"]);
}

/// The same tombstone proof on the S3 object log, through the live projection lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_tombstone_survives_s3_projection_rebuild() {
    let fixture = Fixture::new("terminal-s3");
    let s3 = fireweed_objectlog::shared_s3_test_env();
    let mut config = crate::StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        fixture.projection(),
    );
    config.authority = Some(crate::ObjectLogAuthority::NativeConditionalWrite);
    config.namespace = format!(
        "terminal-purge-{}",
        fixture.root.file_name().unwrap().to_string_lossy()
    );
    let fireweed = crate::open_async(config, fixture.clock.clone())
        .await
        .unwrap();
    fireweed.create_queue(definition()).await.unwrap();
    let id = complete_one(&fireweed, "done").await;
    fireweed.push(&queue(), item("live")).await.unwrap();
    assert_eq!(fireweed.purge(&queue(), vec![id], false).await.unwrap(), 1);

    let control = fireweed.projection_control().unwrap();
    control.delete().await.unwrap();
    assert!(control.rebuild().await.unwrap().tail_commands_replayed > 0);

    assert_eq!(retained_keys(&retained(&fireweed).await), ["live"]);
    assert!(matches!(
        fireweed.push(&queue(), item("done")).await,
        Err(EngineError::Conflict)
    ));
    fixture.clock.set(1_000 + KEY_RETENTION_SECONDS + 1);
    fireweed.push(&queue(), item("done")).await.unwrap();
    fireweed.shutdown().await.unwrap();
}

/// API-001: `force` protects leased items only. A non-forced purge is not limited to terminal
/// items, so a caller that means "terminal only" must check state first.
#[tokio::test(flavor = "multi_thread")]
async fn non_forced_purge_refuses_leased_items_but_removes_pending_ones() {
    let fixture = Fixture::new("terminal-force");
    let fireweed = fixture.open().await;
    fireweed.create_queue(definition()).await.unwrap();
    let leased = fireweed.push(&queue(), item("leased")).await.unwrap();
    let pending = fireweed.push(&queue(), item("pending")).await.unwrap();
    assert_eq!(
        fireweed.claim(&queue(), 1, 30_000).await.unwrap()[0].item_id,
        leased
    );

    assert!(matches!(
        fireweed.purge(&queue(), vec![leased], false).await,
        Err(EngineError::Conflict)
    ));
    assert_eq!(
        fireweed
            .purge(&queue(), vec![pending], false)
            .await
            .unwrap(),
        1
    );
    assert_eq!(retained_keys(&retained(&fireweed).await), ["leased"]);
}

/// The approved disposable constructor serves the same public path.
#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_disk_opens_a_usable_fireweed() {
    let clock = Arc::new(ManualClock::at(1_000));
    let fireweed = tokio::task::spawn_blocking(move || crate::open_ephemeral_disk(clock))
        .await
        .unwrap()
        .unwrap();
    fireweed.create_queue(definition()).await.unwrap();
    let id = complete_one(&fireweed, "done").await;
    assert_eq!(fireweed.purge(&queue(), vec![id], false).await.unwrap(), 1);
    assert!(retained(&fireweed).await.is_empty());
}
