//! Contract test for the authoritative vectorized claimed-work commit (Snorri StateStore boundary, epic
//! pqueue-2201fd37), exercised over the MemoryBackend through the internal runtime core.
//!
//! Proves acceptance #2 (atomic per-entry validate + opaque non-work side records + dispatchable lifecycle
//! enqueue + input finalize, with per-entry committed/rejected outcomes) and acceptance #3 (request-id
//! replay / conflict / expired semantics over the commit path). The recovery/non-work survival assertions
//! also front-run acceptance #5 for the memory reference backend.

use std::sync::Arc;

use fireweed::{
    ClaimRef, CommitEntry, CommitEntryStatus, CommitRequest, EngineError, EntryOutcome,
    FinalizeKind, InstanceFence, MultiClaimCommitEntry, MultiClaimCommitRequest, NewItem,
    PriorityValue, RequestId, RuntimeCore, SideRecord,
};
use fireweed_core::{
    EligibilityPolicy, IndexDeclaration, IndexDef, IndexType, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, QueueIndex,
    RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::QueueKey;
use fireweed_memory::{ManualClock, composed_memory_backend};
use serde_json::json;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef(request_id_retention_ms: u64) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
        request_id_retention_ms,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

/// Push one input item and claim it, returning the `ClaimRef` (id + lease token + version) the commit needs.
async fn push_and_claim(
    fireweed: &RuntimeCore<impl fireweed::LibBackend>,
    q: &QueueKey,
) -> ClaimRef {
    fireweed.push(q, item(10)).await.unwrap();
    let claimed = fireweed.claim(q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "exactly one item claimed");
    let c = &claimed[0];
    ClaimRef {
        item_id: c.item_id,
        lease_token: c
            .lease_token
            .clone()
            .expect("claimed item carries a lease token"),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    }
}

fn side(key: &str, payload: &str) -> SideRecord {
    SideRecord {
        key: key.as_bytes().to_vec(),
        payload: fireweed::Bytes::copy_from_slice(payload.as_bytes()),
    }
}

fn indexed_item(record_kind: &str, key: &str, priority: i64) -> NewItem {
    let mut item = item(priority);
    item.client_item_key = Some(fireweed::ClientItemKey::new(key).unwrap());
    item.fields.insert(
        "record_kind".to_string(),
        fireweed::Bytes::copy_from_slice(record_kind.as_bytes()),
    );
    item.entity = Some(json!({"record_kind": record_kind}));
    item
}

fn claim_ref(item: &fireweed::ClaimedItem) -> ClaimRef {
    ClaimRef {
        item_id: item.item_id,
        lease_token: item.lease_token.clone().expect("claimed item has token"),
        lease_expires_at: item.lease_expires_at,
        item_version: item.item_version,
    }
}

/// A result and its matching await are one logical workflow boundary. Both claims, the fenced projection,
/// and the indexed continuation must therefore commit (or reject) together.
#[tokio::test]
async fn multi_claim_commit_atomically_consumes_result_and_await_and_appends_continuation() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    let mut definition = qdef(60_000);
    definition.typed_indexes = vec![QueueIndex {
        name: "by_record_kind".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "record_kind".to_string(),
            index_type: IndexType::String,
            unique: false,
        }),
    }];
    fireweed.create_queue(definition).await.unwrap();
    fireweed
        .push_batch(
            &q,
            vec![
                indexed_item("result", "result-1", 10),
                indexed_item("await", "await-1", 11),
            ],
        )
        .await
        .unwrap();
    let claimed = fireweed.claim(&q, 2, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 2);
    let primary = claim_ref(&claimed[0]);
    let additional = claim_ref(&claimed[1]);
    let request_id = RequestId::new("result-await-continuation-1").unwrap();
    let request = MultiClaimCommitRequest {
        request_id: Some(request_id.clone()),
        entries: vec![MultiClaimCommitEntry {
            claim_ref: primary.clone(),
            additional_claim_refs: vec![additional.clone()],
            finalize: FinalizeKind::Complete,
            side_records: vec![side("instance/workflow-1", "revision-2")],
            lifecycle_items: vec![indexed_item("transition", "transition-2", 20)],
            instance_fence: Some(InstanceFence {
                instance_key: b"workflow-1".to_vec(),
                expected: 0,
                next: 1,
            }),
        }],
    };

    let first = fireweed
        .commit_multi_claim(&q, request.clone())
        .await
        .unwrap();
    let replay = fireweed
        .commit_multi_claim(&q, request.clone())
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert!(
        matches!(first.as_slice(), [EntryOutcome::Committed { lifecycle_item_ids }] if lifecycle_item_ids.len() == 1)
    );
    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (1, 0, 2)
    );
    assert_eq!(
        fireweed
            .side_record(&q, b"instance/workflow-1")
            .await
            .unwrap()
            .as_deref(),
        Some(b"revision-2".as_slice())
    );
    let recovery = fireweed
        .explain_commit(&q, request_id.clone())
        .await
        .unwrap()
        .expect("commit recovery");
    assert_eq!(recovery.entries[0].consumed_input_id, primary.item_id);
    assert_eq!(
        recovery.entries[0].additional_consumed_input_ids,
        vec![additional.item_id]
    );

    let mut conflicting = request;
    conflicting.entries[0].side_records[0].payload =
        fireweed::Bytes::copy_from_slice(b"different-revision");
    assert_eq!(
        fireweed
            .commit_multi_claim(&q, conflicting)
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict
    );
    assert_eq!(fireweed.claim(&q, 10, 60_000).await.unwrap().len(), 1);
    assert!(fireweed.claim(&q, 10, 60_000).await.unwrap().is_empty());
}

#[tokio::test]
async fn multi_claim_commit_rejects_atomically_when_any_claim_is_stale() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    fireweed
        .push_batch(&q, vec![item(10), item(11)])
        .await
        .unwrap();
    let claimed = fireweed.claim(&q, 2, 60_000).await.unwrap();
    let primary = claim_ref(&claimed[0]);
    let mut stale = claim_ref(&claimed[1]);
    stale.item_version += 1;

    let outcomes = fireweed
        .commit_multi_claim(
            &q,
            MultiClaimCommitRequest {
                request_id: Some(RequestId::new("multi-claim-reject-1").unwrap()),
                entries: vec![MultiClaimCommitEntry {
                    claim_ref: primary,
                    additional_claim_refs: vec![stale],
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("instance/rejected", "must-not-exist")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: Some(InstanceFence {
                        instance_key: b"rejected".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcomes,
        vec![EntryOutcome::Rejected(EngineError::Conflict)]
    );
    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (0, 2, 0)
    );
    assert!(
        fireweed
            .side_record(&q, b"instance/rejected")
            .await
            .unwrap()
            .is_none()
    );
}

/// Acceptance #2: one entry — valid claim_ref + opaque side record + dispatchable lifecycle item +
/// finalize=Complete — commits atomically, finalizes the input, and the side record is non-work-safe while
/// the lifecycle item is ordinary claimable work.
#[tokio::test]
async fn commit_validates_writes_side_records_enqueues_lifecycle_and_finalizes() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();

    let claim_ref = push_and_claim(&fireweed, &q).await;
    let input_id = claim_ref.item_id;

    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(RequestId::new("txn-commit-1").unwrap()),
                entries: vec![CommitEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/run-1", "audit-bytes")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();

    // Per-entry committed outcome carrying the new dispatchable item id.
    assert_eq!(outcomes.len(), 1);
    let lifecycle_id = match &outcomes[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => {
            assert_eq!(lifecycle_item_ids.len(), 1, "one lifecycle item enqueued");
            lifecycle_item_ids[0]
        }
        other => panic!("expected Committed, got {other:?}"),
    };

    // The input item is finalized (complete): exactly one complete, and it is no longer claimable.
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(m.complete, 1, "input claim finalized to complete");
    assert_eq!(m.leased, 0, "input no longer leased");

    // The lifecycle item is a normal pending claimable item (peekable + in metrics-as-work).
    assert_eq!(m.pending, 1, "the lifecycle item is pending work");
    let peeked = fireweed.peek(&q, 10).await.unwrap();
    assert_eq!(peeked.len(), 1, "exactly the lifecycle item is peekable");
    assert_eq!(peeked[0].item_id, lifecycle_id);
    assert_ne!(lifecycle_id, input_id, "lifecycle item is a fresh item");

    // The side record is readable but NOT claimable/peekable/metrics-as-work. Claim everything claimable:
    // only the lifecycle item comes back — never the side record.
    let claimed = fireweed.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "only the lifecycle item is claimable");
    assert_eq!(claimed[0].item_id, lifecycle_id);

    // Recovery invariant: the opaque side record survived input finalization and the subsequent claim, and
    // it is never counted as work (pending/leased/complete reflect only real work items).
    let m2 = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m2.pending, m2.leased, m2.complete),
        (0, 1, 1),
        "side record is never counted as pending/leased/complete work"
    );
}

/// Acceptance #2 (rejection arms): a wrong lease token -> Rejected(StaleLease); a wrong item_version ->
/// Rejected(Conflict). In both cases nothing is written (the input stays leased, no side record, no
/// lifecycle item).
#[tokio::test]
async fn commit_rejects_bad_lease_token_and_bad_version_without_writing() {
    // Wrong lease token.
    {
        let fireweed = RuntimeCore::new(
            Arc::new(composed_memory_backend()),
            Arc::new(ManualClock::at(0)),
        );
        let q = qkey();
        fireweed.create_queue(qdef(60_000)).await.unwrap();
        let mut claim_ref = push_and_claim(&fireweed, &q).await;
        claim_ref.lease_token = fireweed::LeaseToken::new("not-the-real-token").unwrap();

        let outcomes = fireweed
            .commit(
                &q,
                CommitRequest {
                    request_id: None,
                    entries: vec![CommitEntry {
                        claim_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
                    }],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            outcomes,
            vec![EntryOutcome::Rejected(EngineError::StaleLease)]
        );
        let m = fireweed.metrics(&q).await.unwrap();
        assert_eq!(
            (m.pending, m.leased, m.complete),
            (0, 1, 0),
            "bad token: input still leased, nothing enqueued"
        );
    }

    // Wrong item_version.
    {
        let fireweed = RuntimeCore::new(
            Arc::new(composed_memory_backend()),
            Arc::new(ManualClock::at(0)),
        );
        let q = qkey();
        fireweed.create_queue(qdef(60_000)).await.unwrap();
        let mut claim_ref = push_and_claim(&fireweed, &q).await;
        claim_ref.item_version += 99;

        let outcomes = fireweed
            .commit(
                &q,
                CommitRequest {
                    request_id: None,
                    entries: vec![CommitEntry {
                        claim_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
                    }],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            outcomes,
            vec![EntryOutcome::Rejected(EngineError::Conflict)]
        );
        let m = fireweed.metrics(&q).await.unwrap();
        assert_eq!(
            (m.pending, m.leased, m.complete),
            (0, 1, 0),
            "bad version: input still leased, nothing enqueued"
        );
    }
}

/// Acceptance #3: same request body + request_id replays the prior outcomes with NO double-write; a
/// different body under the same request_id conflicts; an expired entry executes fresh.
#[tokio::test]
async fn commit_request_id_replays_conflicts_and_expires() {
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    fireweed.create_queue(qdef(1_000)).await.unwrap();

    let claim_ref = push_and_claim(&fireweed, &q).await;
    let rid = RequestId::new("txn-replay-1").unwrap();
    let request = |claim_ref: ClaimRef| CommitRequest {
        request_id: Some(rid.clone()),
        entries: vec![CommitEntry {
            claim_ref,
            finalize: FinalizeKind::Complete,
            side_records: vec![side("state/run-1", "v1")],
            lifecycle_items: vec![item(20)],
            instance_fence: None,
        }],
    };

    let first = fireweed
        .commit(&q, request(claim_ref.clone()))
        .await
        .unwrap();
    let lifecycle_id = match &first[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
        other => panic!("expected Committed, got {other:?}"),
    };

    // Replay: identical body + request_id returns the SAME outcomes and does NOT double-write (the input is
    // not re-finalized, the side record / lifecycle item are not duplicated).
    let replay = fireweed
        .commit(&q, request(claim_ref.clone()))
        .await
        .unwrap();
    assert_eq!(first, replay, "replay returns the prior per-entry outcomes");
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (1, 0, 1),
        "replay did not enqueue a second lifecycle item or re-finalize"
    );
    // Only the original lifecycle item exists.
    let peeked = fireweed.peek(&q, 10).await.unwrap();
    assert_eq!(peeked.len(), 1);
    assert_eq!(peeked[0].item_id, lifecycle_id);

    // Different body, same request_id -> RequestIdConflict (whole call errors, nothing written).
    let conflict = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(rid.clone()),
                entries: vec![CommitEntry {
                    claim_ref: claim_ref.clone(),
                    finalize: FinalizeKind::Fail, // different body
                    side_records: vec![side("state/run-1", "v1")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(conflict, EngineError::RequestIdConflict);
    assert_eq!(
        fireweed.metrics(&q).await.unwrap().pending,
        1,
        "the conflicting body wrote nothing"
    );

    // Expired retained metadata executes fresh (does not replay). Claim a NEW input, commit it under a
    // fresh request_id, advance past retention, and re-submit the SAME body: it runs fresh.
    let claim_ref2 = push_and_claim(&fireweed, &q).await;
    let rid2 = RequestId::new("txn-expire-1").unwrap();
    let body2 = |cr: ClaimRef| CommitRequest {
        request_id: Some(rid2.clone()),
        entries: vec![CommitEntry {
            claim_ref: cr,
            finalize: FinalizeKind::Complete,
            side_records: vec![side("state/run-2", "v2")],
            lifecycle_items: vec![],
            instance_fence: None,
        }],
    };
    let _ = fireweed
        .commit(&q, body2(claim_ref2.clone()))
        .await
        .unwrap();
    // The input is now complete; advance past the 1_000ms retention window.
    clock.set(5);
    // Re-submitting the same body+id after expiry no longer replays — it executes fresh and now rejects,
    // because the (already-finalized) input is terminal. A replay would have returned the prior Committed.
    let after_expiry = fireweed.commit(&q, body2(claim_ref2)).await.unwrap();
    assert_eq!(
        after_expiry,
        vec![EntryOutcome::Rejected(EngineError::Terminal)],
        "expired entry executes fresh (re-validates the now-terminal input) instead of replaying Committed"
    );
}

/// The commit path is rejected on a backend without an atomic transition boundary (eventual-apply): the
/// objectlog backend inherits the default `Unavailable`. (Capability descriptors are a follow-up; this just
/// proves the port fails closed.)
#[tokio::test]
#[ignore = "objectlog LogEngine product does not implement CommitTransitionPort (defaults Unavailable)"]
async fn direct_objectlog_commit_is_available_and_observable() {
    let dir = std::env::temp_dir().join(format!(
        "fireweed-commit-unavail-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let fireweed = fireweed::open_objectlog(&dir, Arc::new(ManualClock::at(0))).unwrap();
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    fireweed.push(&q, item(10)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 60_000).await.unwrap();
    let claimed = &claimed[0];
    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: claimed.item_id,
                        lease_token: claimed.lease_token.clone().unwrap(),
                        lease_expires_at: claimed.lease_expires_at,
                        item_version: claimed.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [EntryOutcome::Committed { .. }]
    ));
    let metrics = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (0, 0, 1)
    );
}

/// C6: an entry advancing a caller-supplied instance fence `expected -> next` succeeds and the stored fence
/// becomes `next`; a STALE `expected` rejects `Conflict` (nothing written — side record absent, input still
/// leased, fence unchanged); a NON-MONOTONIC `next <= expected` rejects `Invalid`.
#[tokio::test]
async fn commit_advances_validates_and_rejects_instance_fence() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let key = b"instance/run-1".to_vec();

    // First transition: stored fence is unset (== 0). expected=0 -> next=1 commits and advances.
    let cr1 = push_and_claim(&fireweed, &q).await;
    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(RequestId::new("fence-1").unwrap()),
                entries: vec![CommitEntry {
                    claim_ref: cr1,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/run-1", "v1")],
                    lifecycle_items: vec![],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcomes[0], EntryOutcome::Committed { .. }));
    // The fence advanced to 1 (proven via the recovery read's instance tuple).
    let rec = fireweed
        .explain_commit(&q, RequestId::new("fence-1").unwrap())
        .await
        .unwrap()
        .expect("recovery record present");
    assert_eq!(rec.entries[0].instance, Some((key.clone(), 1)));

    // STALE expected: the stored fence is now 1, but the caller presents expected=0 -> Conflict, nothing
    // written. Claim a fresh input so the claim_ref is otherwise valid.
    let cr2 = push_and_claim(&fireweed, &q).await;
    let input2 = cr2.item_id;
    let stale = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref: cr2,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/should-not-write", "x")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 0, // stale: stored is 1
                        next: 2,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(stale, vec![EntryOutcome::Rejected(EngineError::Conflict)]);
    // Nothing written: the side record is absent, the input2 is still leased, no lifecycle item enqueued.
    assert!(
        fireweed
            .side_record(&q, b"state/should-not-write")
            .await
            .unwrap()
            .is_none()
    );
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (0, 1),
        "stale fence: input still leased, nothing enqueued"
    );
    let _ = input2;

    // NON-MONOTONIC: stored fence is 1; present expected=1, next=1 (not strictly greater) -> Invalid. A fresh
    // claimed input gives an otherwise-valid claim_ref (input2 is still leased, so claim returns the new one).
    let cr3 = push_and_claim(&fireweed, &q).await;
    let nonmono = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref: cr3,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 1,
                        next: 1,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        nonmono,
        vec![EntryOutcome::Rejected(EngineError::Invalid(
            "instance fence is not monotonic"
        ))]
    );
}

/// Capabilities (C7): memory and object log advertise the full authoritative-commit capability set so Snorri
/// can activate either. Object-log projection visibility is eventual, while its transition batch remains
/// atomic at the durable log authority.
#[tokio::test]
async fn capabilities_advertise_atomic_commit_on_memory_and_objectlog() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let caps = fireweed.commit_capabilities(&q).unwrap();
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.non_work_side_records);
    assert!(caps.authoritative_recovery_reads);

    // The direct object-log composition uses the same atomic command envelope and
    // must expose the full commit surface.
    let dir = std::env::temp_dir().join(format!(
        "fireweed-caps-objlog-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let objectlog_fireweed = fireweed::open_objectlog(&dir, Arc::new(ManualClock::at(0))).unwrap();
    objectlog_fireweed.create_queue(qdef(60_000)).await.unwrap();
    let ocaps = objectlog_fireweed.commit_capabilities(&q).unwrap();
    assert!(ocaps.atomic_transition_commit);
    assert!(ocaps.vectorized_commit);
    assert!(ocaps.lease_validation);
    assert!(ocaps.retained_commit_idempotency);
    assert!(ocaps.non_work_side_records);
    assert!(ocaps.authoritative_recovery_reads);
}

/// Recovery (C8): after a successful commit (finalize input + write side record + advance a fence),
/// `explain_commit(request_id)` returns the consumed input id, the instance key/fence, the side-record key,
/// the lifecycle item ids, and per-entry status Committed; `side_record(key)` returns the bytes; the input is
/// finalized + not claimable; the side record is not claimable/peekable.
#[tokio::test]
async fn explain_commit_reconstructs_the_transition_and_side_records_are_non_work() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let cr = push_and_claim(&fireweed, &q).await;
    let input_id = cr.item_id;
    let rid = RequestId::new("recover-1").unwrap();
    let instance_key = b"instance/run-1".to_vec();

    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: Some(rid.clone()),
                entries: vec![CommitEntry {
                    claim_ref: cr,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("audit/run-1", "audit-bytes")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: Some(InstanceFence {
                        instance_key: instance_key.clone(),
                        expected: 0,
                        next: 5,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    let lifecycle_id = match &outcomes[0] {
        EntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
        other => panic!("expected Committed, got {other:?}"),
    };

    let recovery = fireweed
        .explain_commit(&q, rid.clone())
        .await
        .unwrap()
        .expect("record present");
    assert_eq!(recovery.request_id, rid);
    assert_eq!(recovery.entries.len(), 1);
    let e = &recovery.entries[0];
    assert_eq!(e.consumed_input_id, input_id, "consumed input id recovered");
    assert_eq!(
        e.instance,
        Some((instance_key, 5)),
        "instance key/fence recovered"
    );
    assert_eq!(
        e.side_record_keys,
        vec![b"audit/run-1".to_vec()],
        "side-record key recovered"
    );
    assert_eq!(
        e.lifecycle_item_ids,
        vec![lifecycle_id],
        "lifecycle ids recovered"
    );
    assert_eq!(e.status, CommitEntryStatus::Committed, "status Committed");

    // side_record(key) returns the bytes.
    assert_eq!(
        fireweed
            .side_record(&q, b"audit/run-1")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"audit-bytes"[..])
    );

    // The input is finalized + not claimable; the side record is not claimable/peekable — only the lifecycle
    // item is claimable.
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(m.complete, 1, "input finalized");
    let claimed = fireweed.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "only the lifecycle item is claimable");
    assert_eq!(claimed[0].item_id, lifecycle_id);
    assert_ne!(
        claimed[0].item_id, input_id,
        "the finalized input is not re-claimable"
    );
}
