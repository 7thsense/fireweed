//! The conformance scenarios. Each is generic over a [`ConformanceBackend`](crate::ConformanceBackend)
//! and takes a `make: impl Fn() -> B` factory (some build a second backend for replay reconstruction).
//! Each fails if the port under test returns a default/no-op — the behavioral no-stub proof (plan §6).

use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GateKeyPolicy, GroupKey, ItemId, ItemState, LeaseToken, Metadata, MetadataValue,
    PriorityValue,
};
use pqueue_engine::{
    ClaimCommand, ClaimCompatibility, CommandPosition, EngineError, EngineResult,
    FenceLeaseCommand, FinalizeCommand, FinalizeKind, FinalizeOutcome, GroupBatching,
    PayloadUpdate, ProjectionSnapshot, PushCommand, QueueCommand, ReplacePendingCommand,
    UnfenceLeaseCommand, UpsertOutcome,
};

// Method calls resolve through the `ConformanceBackend` bound's supertraits, so the individual port
// traits need not be imported here.
use crate::{
    ConformanceBackend, ConformanceCore, claim_req, commit, envelope, item, item_max, qdef, qkey,
    shard, ts,
};

/// Eventual-apply backends MUST refuse upsert (Invariant 2 / TD-007 §2.3: the atomic XDEL+XADD
/// `replace_if_pending` is offered only on the atomic durability class). The refusal is the structured
/// `Unavailable` (RESP `-ERR pqueue unavailable`). Used by the eventual-apply conformance variant in
/// place of the three atomic-class upsert scenarios.
pub async fn upsert_is_unavailable<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &ClientItemKey::new("dup").unwrap(),
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::Unavailable,
        "eventual-apply backends must refuse upsert with Unavailable (Invariant 2)"
    );
}

/// FAC-1 (atomic class): `update_fields` merges a LIVE item's hot-storage fields/payload in place
/// (set + remove), bumps `item_version`, honors the `expected_item_version` CAS (`Conflict` on mismatch),
/// and rejects unknown/terminal ids — the write half of the `live_items` read.
pub async fn update_fields_merges_and_cas<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased
    let id = ItemId::new("1").unwrap();
    let key = ClientItemKey::new("ka").unwrap();

    // Set two fields + payload on the leased item; version bumps off the genesis 1.
    let v = b
        .update_fields(
            &shard(),
            id,
            BTreeMap::from([
                ("state".to_string(), Some(Bytes::from_static(b"sent"))),
                ("n".to_string(), Some(Bytes::from_static(b"1"))),
            ]),
            PayloadUpdate::Set(Some(Bytes::from_static(b"body"))),
            None,
            ts(20),
            None,
        )
        .await
        .unwrap();
    let live = b
        .live_items(&shard(), std::slice::from_ref(&key))
        .await
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .expect("live");
    assert_eq!(
        live.fields.get("state").map(|x| x.as_ref()),
        Some(&b"sent"[..])
    );
    assert_eq!(live.payload.as_deref(), Some(&b"body"[..]));
    assert_eq!(live.item_version, v);

    // Merge: remove a key, add another, KEEP payload; CAS on the current version.
    let v2 = b
        .update_fields(
            &shard(),
            id,
            BTreeMap::from([
                ("n".to_string(), None),
                ("attempts".to_string(), Some(Bytes::from_static(b"2"))),
            ]),
            PayloadUpdate::Keep,
            Some(v),
            ts(21),
            None,
        )
        .await
        .unwrap();
    assert!(v2 > v, "version advances on each update");
    let live = b
        .live_items(&shard(), &[key])
        .await
        .unwrap()
        .into_iter()
        .next()
        .flatten()
        .expect("live");
    assert!(!live.fields.contains_key("n"), "removed key is gone");
    assert_eq!(
        live.fields.get("attempts").map(|x| x.as_ref()),
        Some(&b"2"[..])
    );
    assert_eq!(
        live.fields.get("state").map(|x| x.as_ref()),
        Some(&b"sent"[..]),
        "untouched key survives the merge"
    );
    assert_eq!(
        live.payload.as_deref(),
        Some(&b"body"[..]),
        "Keep left the payload"
    );

    // Stale CAS -> Conflict, nothing changes.
    assert_eq!(
        b.update_fields(
            &shard(),
            id,
            BTreeMap::from([("state".to_string(), Some(Bytes::from_static(b"x")))]),
            PayloadUpdate::Keep,
            Some(v),
            ts(22),
            None,
        )
        .await,
        Err(EngineError::Conflict)
    );
    // Unknown id -> NotFound.
    assert_eq!(
        b.update_fields(
            &shard(),
            ItemId::new("90").unwrap(),
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            ts(23),
            None,
        )
        .await,
        Err(EngineError::NotFound)
    );

    // After completion the item is Terminal and rejects further updates.
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
        ts(30),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        b.update_fields(
            &shard(),
            id,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            ts(31),
            None
        )
        .await,
        Err(EngineError::Terminal)
    );
}

/// FAC-1 (eventual-apply class): the read-your-write field mutation is refused with `Unavailable`
/// (parity with `upsert_is_unavailable`).
pub async fn update_fields_is_unavailable<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    assert_eq!(
        b.update_fields(
            &shard(),
            ItemId::new("1").unwrap(),
            BTreeMap::new(),
            PayloadUpdate::Keep,
            None,
            ts(20),
            None
        )
        .await,
        Err(EngineError::Unavailable)
    );
}

/// FAC-2 (every class): `reclaim_expired` is the per-queue, host-driven lease sweep — expired leases
/// return to Pending (claimable again), the reclaimed ids are returned, and it is idempotent + half-open.
pub async fn reclaim_expired_sweeps_per_queue<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // leased, expires ts(500)
    let id = ItemId::new("1").unwrap();

    // Half-open: at exactly the expiry the lease is still valid — nothing reclaimed.
    assert!(
        b.reclaim_expired(&shard(), None, ts(500), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Past expiry: the id is returned and the item is Pending again.
    assert_eq!(
        b.reclaim_expired(&shard(), None, ts(600), None)
            .await
            .unwrap(),
        vec![id]
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    // Idempotent: a second sweep finds nothing.
    assert!(
        b.reclaim_expired(&shard(), None, ts(700), None)
            .await
            .unwrap()
            .is_empty()
    );
    // Claimable again.
    assert_eq!(
        b.claim(claim_req(1, 1000, 800)).await.unwrap().items.len(),
        1
    );
}

/// `ProjectionRead::peek` — non-destructive, priority-ordered eligible view (fails if it returns a
/// default/empty no-op).
pub async fn peek_is_priority_ordered_and_nondestructive<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;
    let views = b.peek(&shard(), 10).await.unwrap();
    let peeked: Vec<String> = views.iter().map(|v| v.item_id.to_string()).collect();
    assert_eq!(
        peeked,
        vec!["2", "3", "1"],
        "peek is priority-ordered (10,20,30)"
    );
    // Non-destructive: peeking again returns the same items (peek must not consume/claim).
    assert_eq!(
        b.peek(&shard(), 10).await.unwrap().len(),
        3,
        "peek does not consume"
    );
    assert_eq!(
        b.peek(&shard(), 1).await.unwrap().len(),
        1,
        "peek honors the limit"
    );
}

/// `ProjectionRead::pending` — lists in-flight (leased) items (fails on a default/empty no-op).
pub async fn pending_lists_leased_items<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "nothing leased yet"
    );
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending.len(), 1, "the leased item appears in pending");
    assert_eq!(pending[0].item_id.to_string(), "1");
    assert_eq!(pending[0].lease_token.as_str(), "lease-1");
}

/// `SnapshotStore::write_snapshot`/`read_snapshot`/`latest_snapshot` — durable round-trip (fails on a
/// no-op store: latest must reflect the most-recent write and read must return the exact payload).
pub async fn snapshots_write_read_latest<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(
        b.latest_snapshot(&sk).await.unwrap().is_none(),
        "no snapshot yet"
    );
    let pos = CommandPosition::new(sk.clone(), 0, 0);
    let r1 = b
        .write_snapshot(
            &sk,
            pos.clone(),
            ProjectionSnapshot {
                payload: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    let r2 = b
        .write_snapshot(
            &sk,
            pos,
            ProjectionSnapshot {
                payload: vec![4, 5, 6],
            },
        )
        .await
        .unwrap();
    assert_ne!(r1.ref_id, r2.ref_id, "each snapshot gets a distinct ref");
    assert_eq!(
        b.latest_snapshot(&sk)
            .await
            .unwrap()
            .expect("a snapshot exists")
            .ref_id,
        r2.ref_id,
        "latest is the most-recent write"
    );
    assert_eq!(b.read_snapshot(&r1).await.unwrap().payload, vec![1, 2, 3]);
    assert_eq!(b.read_snapshot(&r2).await.unwrap().payload, vec![4, 5, 6]);
}

pub async fn push_then_select_eligible_in_priority_order<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();

    // Push out of priority order: 30, 10, 20.
    let push = QueueCommand::Push(PushCommand {
        items: vec![
            item("1", "ka", 30),
            item("2", "kb", 10),
            item("3", "kc", 20),
        ],
    });
    commit(&b, envelope(push, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<String> = eligible.iter().map(|i| i.to_string()).collect();
    // Ascending Int64 priority => 10(b), 20(c), 30(a). Fails if select_eligible is a no-op.
    assert_eq!(
        ids,
        vec!["2", "3", "1"],
        "must be priority-ordered, not insertion order"
    );
}

pub async fn claim_then_complete_lifecycle<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Claim it.
    let claim = QueueCommand::Claim(ClaimCommand {
        item_ids: vec![ItemId::new("1").unwrap()],
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(200),
    });
    commit(&b, envelope(claim, vec![ItemId::new("1").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.leased, 1, "claim must move item to leased");
    assert_eq!(m.pending, 0);
    // Claimed item is no longer eligible.
    assert!(
        b.select_eligible(&shard(), ts(300), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Complete it.
    let fin = QueueCommand::Finalize(FinalizeCommand {
        outcomes: vec![FinalizeOutcome::new(
            ItemId::new("1").unwrap(),
            FinalizeKind::Complete,
        )],
    });
    commit(&b, envelope(fin, vec![ItemId::new("1").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        m.complete, 1,
        "finalize-complete must move item to complete"
    );
    assert_eq!(m.leased, 0);
}

pub async fn replace_pending_supersedes_old<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("5", "dup", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Upsert: same client_item_key replaces the pending item with a new id.
    let replace = QueueCommand::ReplacePending(ReplacePendingCommand {
        client_item_key: ClientItemKey::new("dup").unwrap(),
        superseded_item_id: ItemId::new("5").unwrap(),
        replacement: item("6", "dup", 5),
    });
    commit(&b, envelope(replace, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<String> = eligible.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        ids,
        vec!["6"],
        "superseded old id must not be eligible; new id is"
    );

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1, "superseded item excluded from counts");
}

pub async fn high_water_is_monotonic<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let p1 = CommandPosition::new(shard(), 0, 1);
    let p2 = CommandPosition::new(shard(), 0, 2);

    b.set_high_water(&shard(), p2.clone()).await.unwrap();
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2.clone()));
    // Regression must be rejected (TD-007 §4).
    assert_eq!(
        b.set_high_water(&shard(), p1).await,
        Err(EngineError::Invalid("high-water regression"))
    );
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2));
}

pub async fn claim_returns_priority_ordered_rich_items<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(2, 500, 100)).await.unwrap();
    let ids: Vec<String> = claimed
        .items
        .iter()
        .map(|i| i.item_id.to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["2", "3"],
        "claim must deliver highest priority first"
    );
    // Rich shape populated (would fail if claim returned a stub).
    let first = &claimed.items[0];
    assert_eq!(
        first.lease_token.as_ref().map(|token| token.as_str()),
        Some("lease-1")
    );
    assert_eq!(first.item_version, 2, "claim bumps item_version");
    assert_eq!(first.attempt_count, 1, "first delivery");
    assert_eq!(first.lease_expires_at, ts(500));

    // The unclaimed lowest-priority item remains eligible.
    let remaining = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        remaining.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
        vec!["1"]
    );
}

pub async fn claim_empty_when_nothing_eligible<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert!(claimed.items.is_empty());
}

pub async fn claimed_item_shape_includes_payload_fields_and_gate_keys<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    let mut def = qdef();
    def.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    def.eligibility_policy.max_gate_keys_per_item = Some(8);
    b.create_queue(def).await.unwrap();
    let mut item = item("1", "ka", 5);
    item.not_before = Some(ts(50));
    item.group_key = Some(GroupKey::new("group-a").unwrap());
    item.payload = Some(Bytes::from_static(b"opaque-payload"));
    item.fields = BTreeMap::from([("field-a".to_string(), Bytes::from_static(b"value-a"))]);
    item.metadata
        .insert("tenant_segment", MetadataValue::String("vip".to_string()));
    item.gate_keys = vec!["gate-a".to_string(), "gate-b".to_string()];
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand { items: vec![item] }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let got = &claimed.items[0];
    assert_eq!(got.item_id, ItemId::new("1").unwrap());
    assert_eq!(got.client_item_key, ClientItemKey::new("ka").unwrap());
    assert_eq!(got.item_version, 2, "claim bumps item_version");
    assert_eq!(got.priority, Some(PriorityValue::Int64(5)));
    assert_eq!(got.not_before, Some(ts(50)));
    assert_eq!(got.group_key, Some(GroupKey::new("group-a").unwrap()));
    assert_eq!(got.lease_token, Some(LeaseToken::new("lease-1").unwrap()));
    assert_eq!(got.lease_expires_at, ts(500));
    assert_eq!(got.attempt_count, 1);
    assert_eq!(got.payload.as_deref(), Some(&b"opaque-payload"[..]));
    assert_eq!(
        got.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a"[..])
    );
    assert_eq!(
        got.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert_eq!(got.gate_keys, vec!["gate-a", "gate-b"]);

    let view = b
        .claimed_view(&shard(), &[ItemId::new("1").unwrap()])
        .await
        .unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(
        view[0].metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string())),
        "claimed_view must render the same claimed-item metadata shape"
    );
    assert_eq!(
        view[0].gate_keys,
        vec!["gate-a", "gate-b"],
        "claimed_view must render the same claimed-item gate-key shape"
    );
}

pub async fn structured_live_items_are_ordered_and_only_live<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let mut fields = BTreeMap::new();
    fields.insert("recipient_ref".to_string(), Bytes::from_static(b"r-1"));
    fields.insert("payload_ref".to_string(), Bytes::from_static(b"work-1"));
    let mut pushed = item("7", "hot-key", 5);
    pushed.payload = Some(Bytes::from_static(b"opaque"));
    pushed.fields = fields.clone();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![pushed],
            }),
            vec![],
        ),
    )
    .await;

    let keys = vec![
        ClientItemKey::new("missing").unwrap(),
        ClientItemKey::new("hot-key").unwrap(),
    ];
    let live = b.live_items(&shard(), &keys).await.unwrap();
    assert!(live[0].is_none(), "missing keys render as absent");
    let Some(item) = &live[1] else {
        panic!("hot-key should render while pending");
    };
    assert_eq!(item.lifecycle_state, ItemState::Pending);
    assert_eq!(item.payload.as_deref(), Some(&b"opaque"[..]));
    assert_eq!(item.fields, fields);

    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].fields, fields);
    let live = b
        .live_items(&shard(), &[ClientItemKey::new("hot-key").unwrap()])
        .await
        .unwrap();
    assert_eq!(
        live[0].as_ref().map(|i| i.lifecycle_state),
        Some(ItemState::Leased),
        "leased items are still live hot-storage records"
    );

    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            claimed.items[0].item_id,
            FinalizeKind::Complete,
        )],
        ts(20),
        None,
    )
    .await
    .unwrap();
    let live = b
        .live_items(&shard(), &[ClientItemKey::new("hot-key").unwrap()])
        .await
        .unwrap();
    assert!(live[0].is_none(), "terminal items are no longer live");
}

pub async fn upsert_inserts_then_replaces_pending<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();

    // First upsert → Inserted with a BACKEND-ASSIGNED id (capture it).
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Second upsert (same key) → Replaced; the new id is backend-assigned and supersedes id1.
    let id2 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(2),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Replaced {
            new_item_id,
            superseded_item_id,
        } => {
            assert_eq!(superseded_item_id, id1, "the first id is superseded");
            assert_ne!(
                new_item_id, id1,
                "the replacement got a fresh backend-assigned id"
            );
            new_item_id
        }
        other => panic!("expected Replaced, got {other:?}"),
    };
    // Only the replacement is eligible; the superseded id is gone.
    let elig = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(elig, vec![id2], "only the replacement is eligible");
}

pub async fn upsert_rejects_claimed_and_terminal<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Claim it → leased. Upsert must be rejected with Invalid (no transition on in-flight work).
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(20),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Invalid("collision with claimed item"));

    // Finalize-complete the leased item → terminal. Upsert must then be rejected with Terminal.
    commit(
        &b,
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(id1, FinalizeKind::Complete)],
            }),
            vec![id1],
        ),
    )
    .await;
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Metadata::default(),
            ts(30),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Terminal);
}

pub async fn upsert_preserves_group_delay_and_payload_in_claim_shape<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("grouped").unwrap();
    let group = GroupKey::new("group-a").unwrap();

    let assigned = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            Some(group.clone()),
            Some(ts(250)),
            Some(Bytes::from_static(b"payload")),
            BTreeMap::new(),
            Metadata::default(),
            ts(1),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty(),
        "not_before must keep the upserted item out of early claims"
    );

    let claimed = b.claim(claim_req(10, 500, 300)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let item = &claimed.items[0];
    assert_eq!(
        item.item_id, assigned,
        "the claimed item is the backend-assigned upsert id"
    );
    assert_eq!(item.group_key.as_ref(), Some(&group));
    assert_eq!(item.not_before, Some(ts(250)));
    assert_eq!(item.payload.as_deref(), Some(&b"payload"[..]));
}

pub async fn tick_reclaims_expired_lease_with_no_client_traffic<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim with a lease expiring at t=100.
    b.claim(claim_req(10, 100, 10)).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Before expiry: tick is a no-op.
    let r = b.tick(ts(50)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // After expiry: tick reclaims — WITH ZERO intervening client commands (DoD, TD-007 §3).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 1);
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1);
    assert_eq!(m.leased, 0);

    // Idempotent: re-ticking at the same time reclaims nothing (item already pending).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);

    // The reclaimed item is back to pending/eligible. (The reclaim itself does NOT charge an attempt —
    // attempt_count = number of deliveries; a fresh claim of this item would charge the next one.)
    let pending = b.select_eligible(&shard(), ts(300), 10).await.unwrap();
    assert_eq!(
        pending.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
        vec!["1"]
    );
}

pub async fn tick_lease_boundary_is_half_open<B: ConformanceCore>(make: impl Fn() -> B) {
    // Convention: a lease is valid THROUGH `lease_expires_at`; reclaim fires only at now > exp (B1).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(10, 100, 10)).await.unwrap(); // lease_expires_at = ts(100)

    // At exactly the expiry instant: lease still held, nothing reclaimed.
    assert_eq!(b.tick(ts(100)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // One unit past expiry: reclaimed.
    assert_eq!(b.tick(ts(101)).await.unwrap().leases_reclaimed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 0);
}

pub async fn paused_queue_yields_no_claims<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Pause: nothing eligible/claimable.
    commit(&b, envelope(QueueCommand::PauseQueue, vec![])).await;
    assert!(
        b.claim(claim_req(10, 500, 10))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        b.select_eligible(&shard(), ts(10), 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Resume: claimable again.
    commit(&b, envelope(QueueCommand::ResumeQueue, vec![])).await;
    assert_eq!(
        b.claim(claim_req(10, 500, 10)).await.unwrap().items.len(),
        1
    );
}

pub async fn fenced_lease_finalize_is_stale<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let id = ItemId::new("1").unwrap();

    // Operator fences the lease.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    // The holder's finalize is rejected StaleLease, and nothing is committed (still leased).
    let outcomes = vec![FinalizeOutcome::new(id, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes.clone(), ts(20), None).await,
        Err(EngineError::StaleLease)
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Operator unfences: finalize now succeeds.
    commit(
        &b,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    b.finalize(&shard(), outcomes, ts(30), None).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}

pub async fn renew_extends_lease_and_rejects<B: ConformanceCore>(make: impl Fn() -> B) {
    // renew_validate MIRRORS finalize_validate: only a live, non-fenced, non-terminal, non-superseded
    // leased item may be renewed; a renew extends the lease WITHOUT charging an attempt (TD-006:129).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased, expires at ts(500)
    let id = ItemId::new("1").unwrap();

    // Unknown id -> NotFound, and NOTHING appended (reject before commit, B1).
    assert_eq!(
        b.renew(
            &shard(),
            vec![ItemId::new("90").unwrap()],
            ts(2000),
            ts(20),
            None
        )
        .await,
        Err(EngineError::NotFound)
    );

    // Happy path: extend the lease to ts(2000). Ticking PAST the old expiry (500) reclaims nothing,
    // and the attempt_count is unchanged (renew does not charge a delivery).
    b.renew(&shard(), vec![id], ts(2000), ts(20), None)
        .await
        .unwrap();
    assert_eq!(
        b.tick(ts(600)).await.unwrap().leases_reclaimed,
        0,
        "extended lease must not be reclaimed past the OLD expiry"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("item still in-flight");
    assert_eq!(lease.attempt_count, 1, "renew does not charge a delivery");
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "renew extended the lease deadline"
    );

    // A never-leased (Pending) item -> Invalid, same as finalize_validate's catch-all.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    assert_eq!(
        b.renew(
            &shard(),
            vec![ItemId::new("4").unwrap()],
            ts(2000),
            ts(21),
            None
        )
        .await,
        Err(EngineError::Invalid("item is not leased"))
    );

    // Fenced lease -> StaleLease, exactly as finalize_validate rejects it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    assert_eq!(
        b.renew(&shard(), vec![id], ts(3000), ts(30), None).await,
        Err(EngineError::StaleLease)
    );
}

pub async fn reassign_swaps_token_and_charges_attempt<B: ConformanceCore>(make: impl Fn() -> B) {
    // Cross-consumer XCLAIM: ReassignLease swaps the lease token to a new consumer AND charges exactly one
    // delivery (TD-006:129). Rejection semantics mirror renew/finalize (validate_leased), appending
    // nothing on reject.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "1" leased by "lease-1", attempt_count = 1
    let id = ItemId::new("1").unwrap();
    let new_token = LeaseToken::new("lease-2").unwrap();

    // Unknown id -> NotFound, and NOTHING appended.
    assert_eq!(
        b.reassign(
            &shard(),
            vec![ItemId::new("90").unwrap()],
            new_token.clone(),
            ts(2000),
            ts(20),
            None
        )
        .await,
        Err(EngineError::NotFound)
    );

    // Happy path: transfer the lease to "lease-2", extend to ts(2000), charge exactly one delivery.
    b.reassign(
        &shard(),
        vec![id],
        new_token.clone(),
        ts(2000),
        ts(20),
        None,
    )
    .await
    .unwrap();
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("still in-flight under the new consumer");
    assert_eq!(
        lease.lease_token, new_token,
        "lease transferred to the new consumer"
    );
    assert_eq!(
        lease.attempt_count, 2,
        "reassign charges one delivery (claim=1 + reassign=1)"
    );
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "reassign extended the deadline"
    );
    // The new lease is live: ticking past the OLD expiry (500) reclaims nothing.
    assert_eq!(b.tick(ts(600)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Fenced lease -> StaleLease, exactly as renew/finalize reject it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            vec![id],
        ),
    )
    .await;
    assert_eq!(
        b.reassign(&shard(), vec![id], new_token, ts(3000), ts(30), None)
            .await,
        Err(EngineError::StaleLease)
    );
}

pub async fn claimed_view_renders_leased_items<B: ConformanceCore>(make: impl Fn() -> B) {
    // `claimed_view` renders the rich claim shape for currently-leased ids; pending + unknown ids are
    // omitted (the RESP `XCLAIM` reply source).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim only the top-priority item "1" (5 < 9, ascending); "4" stays pending.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let a = ItemId::new("1").unwrap();
    let p = ItemId::new("4").unwrap();

    let view = b
        .claimed_view(&shard(), &[a, p, ItemId::new("90").unwrap()])
        .await
        .unwrap();
    assert_eq!(
        view.len(),
        1,
        "only the leased item renders; the pending + unknown ids are omitted"
    );
    assert_eq!(view[0].item_id, a);
    assert_eq!(
        view[0].lease_token,
        Some(LeaseToken::new("lease-1").unwrap())
    );
    assert_eq!(view[0].attempt_count, 1);
}

pub async fn purge_removes_present_items_and_gates_leased<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    // PurgePort (RESP XDEL / operator purge): removes present items, returns the count actually removed,
    // no-ops on absent ids, and gates a LEASED purge behind `force` (API-001) — appending nothing on the
    // gate rejection.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("2", "kb", 9)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim "1" (top priority) → leased; "2" stays pending.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let a = ItemId::new("1").unwrap();
    let b_id = ItemId::new("2").unwrap();

    // Purging a LEASED item without force is gated (Conflict), appending nothing.
    assert_eq!(
        b.purge(&shard(), vec![a], false, ts(20), None).await,
        Err(EngineError::Conflict)
    );
    // Mixed batch [pending, leased] without force: the gate rejects ALL-OR-NOTHING regardless of order —
    // the pending id is NOT purged even though it precedes the leased one in the batch.
    assert_eq!(
        b.purge(&shard(), vec![b_id, a], false, ts(20), None).await,
        Err(EngineError::Conflict)
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "the pending id in a gate-rejected mixed batch is NOT purged"
    );

    // Purge a PENDING item, REPEATED, plus an ABSENT id: the repeat counts once (de-dup), the absent id
    // is a no-op → count 1.
    let removed = b
        .purge(
            &shard(),
            vec![b_id, b_id, ItemId::new("90").unwrap()],
            false,
            ts(21),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        removed, 1,
        "a repeated present id removes/counts once; absent is a no-op"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 0, "b is gone");

    // Force-purge the leased item "1": removed, count 1, no longer leased.
    let removed_a = b
        .purge(&shard(), vec![a], true, ts(22), None)
        .await
        .unwrap();
    assert_eq!(removed_a, 1);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        0,
        "a force-purged"
    );
}

pub async fn retry_beyond_max_attempts_goes_terminal<B: ConformanceCore>(make: impl Fn() -> B) {
    // Retry-exhaustion (B'): `attempt_count` = deliveries. A `Finalize{Retry}` UNDER `max_attempts` returns
    // the item to pending (claimable again); the retry once it has used all `max_attempts` deliveries drives
    // it TERMINAL (Failed). With max_attempts = 2: delivery 1 → retry → pending; delivery 2 → retry → failed.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item_max("1", "ka", 5, 2)],
            }),
            vec![],
        ),
    )
    .await;
    let id = ItemId::new("1").unwrap();
    let retry_outcome = || {
        vec![FinalizeOutcome::new(
            ItemId::new("1").unwrap(),
            FinalizeKind::Retry,
        )]
    };

    // Delivery 1: claim → attempt_count = 1.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(
        b.pending(&shard()).await.unwrap()[0].attempt_count,
        1,
        "first delivery"
    );
    // Retry UNDER the bound (1 < 2) → back to pending, still claimable.
    b.finalize(&shard(), retry_outcome(), ts(20), None)
        .await
        .unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.failed),
        (1, 0, 0),
        "retry under max → pending"
    );
    assert!(
        !b.select_eligible(&shard(), ts(30), 10)
            .await
            .unwrap()
            .is_empty(),
        "the retried item is claimable again"
    );

    // Delivery 2: claim again → attempt_count = 2 (now AT the bound).
    b.claim(claim_req(1, 500, 30)).await.unwrap();
    assert_eq!(
        b.pending(&shard()).await.unwrap()[0].attempt_count,
        2,
        "second delivery"
    );
    // Retry AT the bound (2 >= 2) → TERMINAL (Failed), NOT back to pending.
    b.finalize(&shard(), retry_outcome(), ts(40), None)
        .await
        .unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.failed),
        (0, 0, 1),
        "retry at/beyond max_attempts → terminal Failed"
    );
    assert!(
        b.select_eligible(&shard(), ts(50), 10)
            .await
            .unwrap()
            .is_empty(),
        "the exhausted item is terminal — not claimable"
    );
    // It is now terminal: a further finalize is rejected (Terminal), not a silent re-queue.
    assert_eq!(
        b.finalize(
            &shard(),
            vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
            ts(60),
            None
        )
        .await,
        Err(EngineError::Terminal)
    );

    // Boundary: max_attempts = 1 means ONE delivery, no retries — the first retry exhausts immediately
    // (pins `>=`, not `>`). Push a second item "2" with max_attempts = 1.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item_max("2", "kb", 9, 1)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 70)).await.unwrap(); // delivery 1 (attempt_count = 1 == max)
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            ItemId::new("2").unwrap(),
            FinalizeKind::Retry,
        )],
        ts(80),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().failed,
        2,
        "max_attempts=1: the very first retry exhausts → Failed (b joins the earlier a)"
    );
}

pub async fn retry_with_backoff_defers_eligibility<B: ConformanceCore>(make: impl Fn() -> B) {
    // Queue-native retry backoff: a `Finalize{Retry}` carrying `not_before` returns the item to Pending
    // (still under the attempt bound) but DEFERS its re-eligibility until that timestamp. The item shows
    // up Pending in metrics, yet `select_eligible` skips it until `now >= not_before` (half-open `<= now`).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Delivery 1: claim (lease to ts(500)), then Retry under the bound with a backoff to ts(100).
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: ItemId::new("1").unwrap(),
            kind: FinalizeKind::Retry,
            not_before: Some(ts(100)),
        }],
        ts(20),
        None,
    )
    .await
    .unwrap();

    // Back to Pending (not terminal).
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (1, 0),
        "retry under max → pending, not terminal"
    );

    // Still deferred: 50 < 100, so nothing is eligible yet.
    assert!(
        b.select_eligible(&shard(), ts(50), 10)
            .await
            .unwrap()
            .is_empty(),
        "backed off before not_before — not eligible"
    );
    // Eligible AT the boundary (half-open `<= now` convention).
    assert!(
        !b.select_eligible(&shard(), ts(100), 10)
            .await
            .unwrap()
            .is_empty(),
        "eligible at the not_before boundary"
    );
    // And actually claimable once the backoff elapses.
    assert_eq!(
        b.claim(claim_req(1, 600, 100)).await.unwrap().items.len(),
        1,
        "claimable at the not_before boundary"
    );
}

pub async fn finalize_of_nonleased_item_is_rejected_without_appending<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let id = ItemId::new("1").unwrap();
    // Item is Pending (never claimed) -> finalize rejected, and NOTHING is appended (no divergence, B1).
    let outcomes = vec![FinalizeOutcome::new(id, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(10), None).await,
        Err(EngineError::Invalid("item is not leased"))
    );
}

pub async fn pause_and_fence_reconstruct_from_log<B: ConformanceBackend>(make: impl Fn() -> B) {
    // Backend A: push two items, claim+fence one, leave one pending, pause the queue.
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    a.claim(claim_req(1, 500, 10)).await.unwrap(); // claims "1" (priority 5 < 9)
    let aid = ItemId::new("1").unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![aid],
            }),
            vec![aid],
        ),
    )
    .await;
    commit(&a, envelope(QueueCommand::PauseQueue, vec![])).await;

    // Replay A's full log into a fresh backend B (TD-007 §4 replay reconstruction).
    let page = a.read_from(&shard(), None, 1000).await.unwrap();
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let b_epoch = b.current_epoch(&shard()).await.unwrap();
    for (_pos, env) in &page.entries {
        let env = env.clone();
        b.write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), b_epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .unwrap();
    }

    // B reconstructed the durable state: pause withholds the pending item, and the fence holds.
    assert!(
        b.claim(claim_req(10, 500, 50))
            .await
            .unwrap()
            .items
            .is_empty(),
        "pause reconstructed"
    );
    let outcomes = vec![FinalizeOutcome::new(aid, FinalizeKind::Complete)];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(60), None).await,
        Err(EngineError::StaleLease),
        "fence reconstructed"
    );
}

pub async fn high_water_advances_on_each_commit<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(b.high_water(&sk).await.unwrap().is_none(), "no commits yet");
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let h1 = b.high_water(&sk).await.unwrap().expect("after push");
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let h2 = b.high_water(&sk).await.unwrap().expect("after claim");
    assert!(
        h1.precedes(&h2),
        "command_position high-water must advance on each commit (push -> claim)"
    );
}

/// Relational-reconnect class (ADR-008 §2 / TD-001 conformance capability classes): committed state
/// **survives a process restart via reopen-the-store**, with no test-driven manual log replay. Commit
/// items, drop the backend handle (simulated crash), build a fresh backend from the **same durable
/// store** (the `make` factory MUST reopen it, and the queue definition MUST persist — the second handle
/// does NOT re-`create_queue`), and assert the committed state is present.
///
/// This is a black-box durability assertion: it does not (and as a `ConformanceCore`-bounded scenario
/// CANNOT) assert *how* the state is restored. A log-bearing backend may satisfy it via log replay on
/// open; the **transactional-authoritative relational backend (`postgres_native`, BQ-12) is the intended
/// exemplar** — TD-001: "only a transactional-authoritative relational projection runs the
/// reconnect-after-crash class." The sqlite smoke (BQ-10) only proves this scenario + the
/// `relational_reconnect_suite!` macro compile and run.
///
/// Only durable backends whose `make` reopens shared state belong to this class; an in-memory backend
/// sees a fresh empty store on the second `make()` and is not in this class (it does not run this suite).
pub async fn reconnect_after_crash_preserves_committed_state<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;

    // Simulated crash: drop the handle, then reopen the SAME durable store.
    drop(a);
    let b = make();

    // No manual log replay: the DB-resident projection is authoritative, so the committed items are
    // present (in ascending Int64 priority order: 10(b), 20(c), 30(a)). Fails if reopen lost state.
    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        eligible,
        vec![
            ItemId::new("2").unwrap(),
            ItemId::new("3").unwrap(),
            ItemId::new("1").unwrap(),
        ],
        "committed items present in priority order after reconnect (no log replay)"
    );
}

/// Reconnect preserves NON-pending lifecycle state too: a completed item stays terminal and the
/// untouched items stay pending across a reopen. (Relational: read from the DB-resident projection;
/// log-bearing: reconstructed by replay — the scenario asserts only the recovered *state*, so both
/// recovery models satisfy it.)
pub async fn reconnect_preserves_terminal_and_pending_state<B: ConformanceCore>(
    make: impl Fn() -> B,
) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("1", "ka", 30),
                    item("2", "kb", 10),
                    item("3", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;
    // Claim the priority-10 item ("2") and complete it -> terminal; "1"/"3" stay pending.
    let claimed = a.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(claimed.items[0].item_id.to_string(), "2");
    a.finalize(
        &shard(),
        vec![FinalizeOutcome::new(
            ItemId::new("2").unwrap(),
            FinalizeKind::Complete,
        )],
        ts(20),
        None,
    )
    .await
    .unwrap();

    drop(a);
    let b = make();

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (2, 0, 1),
        "terminal + pending counts survive reconnect"
    );
    assert_eq!(
        b.select_eligible(&shard(), ts(100), 10).await.unwrap(),
        vec![ItemId::new("3").unwrap(), ItemId::new("1").unwrap()],
        "the two untouched items remain pending in priority order; the completed one does not reappear"
    );
}

/// Reconnect preserves a LEASED item as leased (token-contract-safe: asserts the recovered lifecycle
/// *state* via metrics, NOT the lease token — the relational family deliberately loses the cleartext token
/// on reopen, while a log-bearing family reconstructs it; both keep the item `Leased`). The tokenless
/// in-flight lease is still reclaimable by the owner: a tick past the deadline returns it to pending.
pub async fn reconnect_preserves_leased_item_state<B: ConformanceCore>(make: impl Fn() -> B) {
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    a.claim(claim_req(1, 500, 10)).await.unwrap(); // leased through ts(500)

    drop(a);
    let b = make();

    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        1,
        "the in-flight lease survives reconnect as Leased"
    );
    // The lease deadline survived too, so the reclaim tick can return the tokenless lease to pending.
    b.tick(ts(501)).await.unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.leased, m.pending),
        (0, 1),
        "the reclaim tick recovers the tokenless in-flight lease"
    );
}

/// **Log-class** durability guarantee (B1, no-divergence): a REJECTED mutation must not append any
/// command — the durable log length is unchanged. The behavioral rejection itself (the structured
/// `NotFound`/`Conflict`/`Invalid` error) is asserted in the CORE class (the renew/reassign/purge/
/// finalize scenarios, which every family runs); this scenario adds the log-tail guarantee that the
/// reject happens BEFORE any append. Bounded by [`ConformanceBackend`] (needs `LogRead`).
pub async fn rejected_mutations_do_not_append_commands<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    // Push two items, claim the higher-priority one ("1", priority 5) → leased; "4" stays pending.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5), item("4", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // leases "1"

    let before = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();

    let unknown = ItemId::new("91").unwrap();
    let _ = b
        .renew(&shard(), vec![unknown], ts(2000), ts(20), None)
        .await; // unknown id → NotFound
    let _ = b
        .reassign(
            &shard(),
            vec![unknown],
            LeaseToken::new("l2").unwrap(),
            ts(2000),
            ts(20),
            None,
        )
        .await; // unknown id → NotFound
    let _ = b
        .purge(
            &shard(),
            vec![ItemId::new("1").unwrap()],
            false,
            ts(20),
            None,
        )
        .await; // leased without force → Conflict
    let _ = b
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(
                ItemId::new("4").unwrap(),
                FinalizeKind::Complete,
            )],
            ts(20),
            None,
        )
        .await; // pending, not leased → Invalid

    let after = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(
        before, after,
        "rejected renew/reassign/purge/finalize must NOT append any command (B1 no-divergence)"
    );
}

/// **Two-family CORE parity** (BQ-13, ADR-008 §2): drive the SAME arbitrary command sequence against two
/// backends from DIFFERENT projection families and assert their observable read-state is identical at
/// every step — a head-to-head differential proof of "behaviorally identical on the core class", stronger
/// than each family passing fixed scenarios separately. Pushes use the write-UoW with EXPLICIT item ids
/// (server-minted ids differ per backend by construction), so the compared state — metrics, eligibility
/// order, peek, and the (token-bearing) pending set — is family-independent.
pub async fn cross_family_core_parity<A: ConformanceCore, B: ConformanceCore>(
    make_a: impl Fn() -> A,
    make_b: impl Fn() -> B,
) {
    let a = make_a();
    let b = make_b();
    a.create_queue(qdef()).await.unwrap();
    b.create_queue(qdef()).await.unwrap();

    async fn commit_both<A: ConformanceCore, B: ConformanceCore>(a: &A, b: &B, cmd: QueueCommand) {
        commit(a, envelope(cmd.clone(), vec![])).await;
        commit(b, envelope(cmd, vec![])).await;
    }

    /// Assert the two backends present identical observable state. `select_eligible`/`peek` are compared in
    /// order (both families order by the strict-claim key); `pending` is sorted first (it is unordered).
    async fn parity<A: ConformanceCore, B: ConformanceCore>(a: &A, b: &B, now: i64, label: &str) {
        assert_eq!(
            a.metrics(&qkey()).await.unwrap(),
            b.metrics(&qkey()).await.unwrap(),
            "metrics diverge @ {label}"
        );
        assert_eq!(
            a.select_eligible(&shard(), ts(now), 100).await.unwrap(),
            b.select_eligible(&shard(), ts(now), 100).await.unwrap(),
            "select_eligible diverge @ {label}"
        );
        let pa: Vec<(String, Option<PriorityValue>, u64)> = a
            .peek(&shard(), 100)
            .await
            .unwrap()
            .into_iter()
            .map(|v| (v.item_id.to_string(), v.priority, v.item_version))
            .collect();
        let pb: Vec<(String, Option<PriorityValue>, u64)> = b
            .peek(&shard(), 100)
            .await
            .unwrap()
            .into_iter()
            .map(|v| (v.item_id.to_string(), v.priority, v.item_version))
            .collect();
        assert_eq!(pa, pb, "peek diverge @ {label}");
        let sort_pending = |v: Vec<pqueue_engine::LeaseView>| {
            let mut s: Vec<(String, String, i64, u32)> = v
                .into_iter()
                .map(|l| {
                    (
                        l.item_id.to_string(),
                        l.lease_token.as_str().to_string(),
                        l.lease_expires_at.seconds,
                        l.attempt_count,
                    )
                })
                .collect();
            s.sort();
            s
        };
        assert_eq!(
            sort_pending(a.pending(&shard()).await.unwrap()),
            sort_pending(b.pending(&shard()).await.unwrap()),
            "pending diverge @ {label}"
        );
    }

    parity(&a, &b, 100, "empty").await;

    // Push out of priority order (explicit ids so the families agree on identity).
    commit_both(
        &a,
        &b,
        QueueCommand::Push(PushCommand {
            items: vec![
                item("1", "ka", 30),
                item("2", "kb", 10),
                item("3", "kc", 20),
            ],
        }),
    )
    .await;
    parity(&a, &b, 100, "after push").await;

    // Claim the priority-10 head ("2") on both — identical request, identical selection + lease.
    a.claim(claim_req(1, 500, 10)).await.unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    parity(&a, &b, 100, "after claim b").await;

    // Renew, then complete "2".
    a.renew(
        &shard(),
        vec![ItemId::new("2").unwrap()],
        ts(900),
        ts(20),
        None,
    )
    .await
    .unwrap();
    b.renew(
        &shard(),
        vec![ItemId::new("2").unwrap()],
        ts(900),
        ts(20),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 100, "after renew b").await;
    let fin_b = vec![FinalizeOutcome::new(
        ItemId::new("2").unwrap(),
        FinalizeKind::Complete,
    )];
    a.finalize(&shard(), fin_b.clone(), ts(30), None)
        .await
        .unwrap();
    b.finalize(&shard(), fin_b, ts(30), None).await.unwrap();
    parity(&a, &b, 100, "after complete b").await;

    // Claim "3" (now the head), reassign it to a new consumer, then retry it back to pending.
    a.claim(claim_req(1, 500, 40)).await.unwrap();
    b.claim(claim_req(1, 500, 40)).await.unwrap();
    parity(&a, &b, 100, "after claim c").await;
    let l2 = LeaseToken::new("lease-2").unwrap();
    a.reassign(
        &shard(),
        vec![ItemId::new("3").unwrap()],
        l2.clone(),
        ts(800),
        ts(50),
        None,
    )
    .await
    .unwrap();
    b.reassign(
        &shard(),
        vec![ItemId::new("3").unwrap()],
        l2,
        ts(800),
        ts(50),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 100, "after reassign c").await;
    let retry_c = vec![FinalizeOutcome::new(
        ItemId::new("3").unwrap(),
        FinalizeKind::Retry,
    )];
    a.finalize(&shard(), retry_c.clone(), ts(60), None)
        .await
        .unwrap();
    b.finalize(&shard(), retry_c, ts(60), None).await.unwrap();
    parity(&a, &b, 100, "after retry c").await;

    // Fence-then-finalize "3" after a re-claim: both families reject the fenced finalize identically.
    a.claim(claim_req(1, 500, 70)).await.unwrap();
    b.claim(claim_req(1, 500, 70)).await.unwrap();
    commit_both(
        &a,
        &b,
        QueueCommand::FenceLease(FenceLeaseCommand {
            item_ids: vec![ItemId::new("3").unwrap()],
        }),
    )
    .await;
    let fin_c = vec![FinalizeOutcome::new(
        ItemId::new("3").unwrap(),
        FinalizeKind::Complete,
    )];
    assert!(
        a.finalize(&shard(), fin_c.clone(), ts(80), None)
            .await
            .is_err()
    );
    assert!(b.finalize(&shard(), fin_c, ts(80), None).await.is_err());
    parity(&a, &b, 100, "after fenced-finalize reject").await;

    // Lease-expiry reclaim tick: "3" was leased through ts(500); tick past it returns it to pending.
    a.tick(ts(501)).await.unwrap();
    b.tick(ts(501)).await.unwrap();
    parity(&a, &b, 600, "after reclaim tick").await;

    // Purge the still-pending "1".
    a.purge(
        &shard(),
        vec![ItemId::new("1").unwrap()],
        false,
        ts(90),
        None,
    )
    .await
    .unwrap();
    b.purge(
        &shard(),
        vec![ItemId::new("1").unwrap()],
        false,
        ts(90),
        None,
    )
    .await
    .unwrap();
    parity(&a, &b, 600, "after purge a").await;

    // Pause hides eligibility on both; resume restores it.
    commit_both(&a, &b, QueueCommand::PauseQueue).await;
    parity(&a, &b, 600, "after pause").await;
    commit_both(&a, &b, QueueCommand::ResumeQueue).await;
    parity(&a, &b, 600, "after resume").await;

    // ReplacePending (upsert via the write-UoW with explicit ids): supersede the pending "3" with "8".
    commit_both(
        &a,
        &b,
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("kc").unwrap(),
            superseded_item_id: ItemId::new("3").unwrap(),
            replacement: item("8", "kc", 20),
        }),
    )
    .await;
    parity(&a, &b, 600, "after replace c->c2").await;
}

/// **BQ-14a — claim compatibility is resolved and gated.** The claim resolves its `ClaimUnit` from the
/// request's compatibility options (API-001 Batch Claim) and gates non-item units. Item-level (the
/// default) is byte-identical to the existing claim; a valid group/cohort/same-group unit is refused with
/// the structured `Unavailable` (its selection lands in BQ-14b/c — an honest not-yet-implemented, not a
/// silent item-claim); an invalid combination is rejected with the structured validation error. Every
/// backend resolves identically (the shared `require_item_level_claim`).
pub async fn claim_compatibility_is_resolved_and_gated<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // An invalid combination (group_batching + whole_cohort) is rejected with the structured validation
    // error on EVERY backend — family-agnostic (no def fields read, no projection family difference).
    let mut bad = claim_req(1, 500, 10);
    bad.compatibility = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        whole_cohort: true,
        ..Default::default()
    };
    assert!(
        matches!(b.claim(bad).await, Err(EngineError::Invalid(_))),
        "an invalid compatibility combination is rejected with the structured error"
    );

    // The rejected claim changed nothing — an item-level (default) claim still leases "1", proving the
    // compatibility gate rejects BEFORE any selection/commit (no partial state).
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "item-level claim is unchanged by the compatibility gate"
    );

    // NOTE: the behavior of a VALID compatibility unit (same_group_key / group_batching / whole_cohort) is
    // family-specific — the relational family implements group/cohort selection (BQ-14b/c), the in-memory
    // family does not maintain `group_summary` and refuses with `Unavailable`. That is RELATIONAL-class,
    // deliberately NOT asserted here (it would diverge across families); see the relational backends'
    // own `group_batching_*` / `same_group_key_*` tests.
}

// ---------------------------------------------------------------------------
// BQ-20 — the Single Authoritative Fencing Rule (TD-003). The durable `assignment_epoch` is the one
// fencing authority: `acquire_epoch` advances it strictly + durably (step 1, "durable fence before
// use"), and `LogWriter::append` rejects any non-current `expected_epoch` (step 2). Both projection
// families run these (a CORE guarantee; TD-001 lease/epoch fencing is the core class).
// ---------------------------------------------------------------------------

/// Append `command` to the queue under `expected_epoch` through the atomic write UoW, returning the
/// fence outcome (`EpochFenced` when stale). Apply only runs if the append is admitted.
async fn append_at_epoch<B: ConformanceCore>(
    b: &B,
    expected_epoch: u64,
    command: QueueCommand,
) -> EngineResult<()> {
    let env = envelope(command, vec![]);
    b.write(move |lw, pw| {
        let pos = lw.append(&shard(), std::slice::from_ref(&env), expected_epoch)?;
        pw.apply(&pos, std::slice::from_ref(&env))?;
        Ok(())
    })
    .await
}

/// A stale (non-current) epoch is fenced at append; the current epoch is admitted; `acquire_epoch`
/// advances the durable epoch strictly. Rejection is on "not equal to current", not "<= current" — a
/// future epoch is rejected too.
pub async fn stale_epoch_append_is_fenced<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let e0 = b.current_epoch(&shard()).await.unwrap();

    // An append at the current epoch is admitted.
    append_at_epoch(&b, e0, QueueCommand::PauseQueue)
        .await
        .expect("append at the current epoch is admitted");

    // Acquire allocates a strictly-greater, durably-recorded epoch (TD-003 monotonicity).
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(
        e1 > e0,
        "acquire_epoch must allocate a strictly-greater epoch"
    );
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        e1,
        "acquire durably advances the recorded current epoch"
    );

    // The superseded owner's old epoch is fenced...
    assert_eq!(
        append_at_epoch(&b, e0, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "a stale (old) epoch is fenced"
    );
    // ...and a NON-current FUTURE epoch is also rejected (the rule is "not current", not "<= current").
    assert_eq!(
        append_at_epoch(&b, e1 + 1, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "a future (non-current) epoch is fenced too"
    );
    // The current owner appends fine.
    append_at_epoch(&b, e1, QueueCommand::ResumeQueue)
        .await
        .expect("the current-epoch owner is admitted");
}

/// The post-advance / pre-segment window is closed: the instant `acquire_epoch` advances to E+1 — before
/// the new owner writes ANY E+1 segment — a stale epoch-E writer is already fenced (TD-003 step 2: reject
/// against the recorded current epoch, which step 1 advanced at acquire, not lazily on first data write).
pub async fn epoch_fence_closes_pre_segment_window<B: ConformanceCore>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let e0 = b.current_epoch(&shard()).await.unwrap();
    // An epoch-E segment exists (the previous owner wrote data at E).
    append_at_epoch(&b, e0, QueueCommand::PauseQueue)
        .await
        .unwrap();

    // The new owner acquires E+1 — durably fenced — but has NOT written any E+1 segment yet.
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 > e0);

    // The stale E writer's VERY NEXT append — in the window before any E+1 segment exists — is fenced.
    assert_eq!(
        append_at_epoch(&b, e0, QueueCommand::ResumeQueue).await,
        Err(EngineError::EpochFenced),
        "the pre-segment window is closed: a stale writer is fenced at handoff, not at first conflict"
    );

    // Only now does the new owner write the first E+1 segment.
    append_at_epoch(&b, e1, QueueCommand::ResumeQueue)
        .await
        .expect("the new owner writes the first new-epoch segment");
}
