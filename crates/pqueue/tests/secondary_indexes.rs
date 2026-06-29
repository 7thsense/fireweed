//! Per-queue secondary indexes (ADR-010, Phase 1 / in-memory family) exercised over the ergonomic
//! facade on the memory backend: unique-get + non-unique-lookup, the sparse rule, read-after-write on
//! `update_fields`, atomic unique-conflict rejection (nothing committed), and removal on purge.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use pqueue::{EngineError, IndexSpec, NewItem, PayloadUpdate, Pqueue};
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

/// A queue with one UNIQUE index over `external_id` and one NON-UNIQUE index over `tenant`.
fn index_bearing_qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![
            IndexSpec {
                name: "by_external_id".to_string(),
                fields: vec!["external_id".to_string()],
                unique: true,
            },
            IndexSpec {
                name: "by_tenant".to_string(),
                fields: vec!["tenant".to_string()],
                unique: false,
            },
        ],
    }
}

fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, Bytes> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Bytes::from(v.as_bytes().to_vec())))
        .collect()
}

fn item(fields_pairs: &[(&str, &str)]) -> NewItem {
    NewItem {
        fields: fields(fields_pairs),
        ..Default::default()
    }
}

fn key(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

async fn new_pq() -> Pqueue<MemoryBackend> {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    pq.create_queue(index_bearing_qdef()).await.unwrap();
    pq
}

#[tokio::test]
async fn unique_get_and_nonunique_lookup_after_push() {
    let pq = new_pq().await;
    let q = qkey();

    let id_a = pq
        .push(&q, item(&[("external_id", "A"), ("tenant", "acme")]))
        .await
        .unwrap();
    let id_b = pq
        .push(&q, item(&[("external_id", "B"), ("tenant", "acme")]))
        .await
        .unwrap();
    // Sparse rule: this item has no `external_id`, so it is absent from the unique index entirely, and a
    // different `tenant` value for the non-unique index.
    let _id_c = pq.push(&q, item(&[("tenant", "globex")])).await.unwrap();

    // Unique-get returns the single holder by its value.
    let hit_a = pq
        .query_index_unique(&q, "by_external_id", key(&["A"]))
        .await
        .unwrap()
        .expect("A is indexed");
    assert_eq!(hit_a.item_id, id_a);
    assert_eq!(hit_a.item_version, 1);

    // A missing value resolves to None.
    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["ZZZ"]))
            .await
            .unwrap()
            .is_none()
    );

    // Non-unique lookup returns all matching items in item_id ascending order; the sparse `globex` item
    // is not under `acme`.
    let acme = pq
        .query_index(&q, "by_tenant", key(&["acme"]))
        .await
        .unwrap();
    let acme_ids: Vec<_> = acme.iter().map(|h| h.item_id).collect();
    let mut expected = vec![id_a, id_b];
    expected.sort();
    assert_eq!(acme_ids, expected);

    // The sparse item carries no `external_id` index entry.
    assert!(
        pq.query_index(&q, "by_external_id", key(&["does-not-exist"]))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn update_fields_moves_the_indexed_entry_read_after_write() {
    let pq = new_pq().await;
    let q = qkey();

    let id = pq
        .push(&q, item(&[("external_id", "OLD"), ("tenant", "acme")]))
        .await
        .unwrap();

    // Change the indexed field; the OLD key must stop resolving and the NEW key must resolve to the item
    // with its bumped version (read-after-write).
    let new_version = pq
        .update_fields(
            &q,
            id,
            BTreeMap::from([("external_id".to_string(), Some(Bytes::from_static(b"NEW")))]),
            PayloadUpdate::Keep,
            None,
        )
        .await
        .unwrap();
    assert_eq!(new_version, 2);

    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["OLD"]))
            .await
            .unwrap()
            .is_none(),
        "old key no longer resolves"
    );
    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["NEW"]))
        .await
        .unwrap()
        .expect("new key resolves");
    assert_eq!(hit.item_id, id);
    assert_eq!(hit.item_version, 2, "hit carries the bumped version");
}

#[tokio::test]
async fn unique_conflict_is_rejected_atomically_and_commits_nothing() {
    let pq = new_pq().await;
    let q = qkey();

    let id_a = pq
        .push(&q, item(&[("external_id", "DUP"), ("tenant", "acme")]))
        .await
        .unwrap();

    // A second PUSH onto the same unique key is rejected, and nothing is committed.
    let push_err = pq
        .push(&q, item(&[("external_id", "DUP"), ("tenant", "acme")]))
        .await
        .unwrap_err();
    assert_eq!(push_err, EngineError::Conflict);

    // An UPSERT onto the same unique key (different client key) is likewise rejected.
    let upsert_err = pq
        .upsert(
            &q,
            ClientItemKey::new("other-key").unwrap(),
            item(&[("external_id", "DUP"), ("tenant", "acme")]),
        )
        .await
        .unwrap_err();
    assert_eq!(upsert_err, EngineError::Conflict);

    // The unique key still resolves to the ORIGINAL item only, and the non-unique index did not gain a
    // phantom second member (nothing committed on the rejected calls).
    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["DUP"]))
        .await
        .unwrap()
        .expect("original still indexed");
    assert_eq!(hit.item_id, id_a);
    assert_eq!(
        pq.query_index(&q, "by_tenant", key(&["acme"]))
            .await
            .unwrap()
            .len(),
        1,
        "rejected push/upsert committed nothing"
    );
}

#[tokio::test]
async fn upsert_replace_moves_the_unique_key() {
    let pq = new_pq().await;
    let q = qkey();

    let client_key = ClientItemKey::new("ck-1").unwrap();
    pq.upsert(&q, client_key.clone(), item(&[("external_id", "V1")]))
        .await
        .unwrap();

    // Re-upsert the SAME client key with a new indexed value: the old value stops resolving, the new
    // value resolves to the replacement id.
    let outcome = pq
        .upsert(&q, client_key, item(&[("external_id", "V2")]))
        .await
        .unwrap();
    let new_id = match outcome {
        pqueue::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
        other => panic!("expected Replaced, got {other:?}"),
    };

    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["V1"]))
            .await
            .unwrap()
            .is_none(),
        "superseded value no longer resolves"
    );
    let hit = pq
        .query_index_unique(&q, "by_external_id", key(&["V2"]))
        .await
        .unwrap()
        .expect("replacement value resolves");
    assert_eq!(hit.item_id, new_id);
}

#[tokio::test]
async fn purge_removes_the_index_entry() {
    let pq = new_pq().await;
    let q = qkey();

    let id = pq
        .push(&q, item(&[("external_id", "GONE"), ("tenant", "acme")]))
        .await
        .unwrap();

    pq.purge(&q, [id], false).await.unwrap();

    assert!(
        pq.query_index_unique(&q, "by_external_id", key(&["GONE"]))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        pq.query_index(&q, "by_tenant", key(&["acme"]))
            .await
            .unwrap()
            .is_empty()
    );
}
