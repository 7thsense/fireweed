//! API-001 BatchClaimByItemIds / claim_by_item_ids on AsyncLogReplayBackend.
//!
//! Covers partial outcomes, point-lookup (no full-shard eligible scan), and ordinary lease lifecycle
//! hooks (inspect / reclaim / fence) for leases created by id-set selection.

use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{
    ClaimByItemIdClass, ClaimByItemIdsDisposition, ClaimByItemIdsOutcome, ClaimByItemIdsRequest,
    ClientItemKey, ItemId, LeaseToken, PriorityValue, RequestId, WorkerId,
};
use fireweed_engine::{
    Backend, ClaimByQueryContext, CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore,
    EngineError, FenceLeaseCommand, FinalizeKind, FinalizeOutcome, FinalizePort,
    HotProjectionQueryPort, ProjectionRead, ProjectionStore, PushPort, PushSpec, QueueCommand,
    RawCommitRequest, ReassignLeasePort, ReclaimPort, RenewLeasePort, assemble_async_log_replay,
};
use fireweed_projection::{InMemoryProjection, MemoryLog};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

fn backend() -> fireweed_engine::AsyncLogReplayBackend<MemoryLog, InMemoryProjection> {
    assemble_async_log_replay(MemoryLog::new(), InMemoryProjection::new(), 1).expect("assemble")
}

fn worker(id: &str) -> WorkerId {
    WorkerId::new(id).unwrap()
}

fn request_id(id: &str) -> RequestId {
    RequestId::new(id).unwrap()
}

fn push_spec(key: &str, priority: i64) -> PushSpec {
    PushSpec {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: None,
        payload: None,
        fields: BTreeMap::new(),
        metadata: Default::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

fn claim_req(item_ids: Vec<ItemId>, rid: &str, lease_ms: u64) -> ClaimByItemIdsRequest {
    ClaimByItemIdsRequest {
        item_ids,
        lease_duration_ms: lease_ms,
        worker_id: worker("w1"),
        request_id: request_id(rid),
        lease_token: None,
    }
}

fn ctx(now_s: i64) -> ClaimByQueryContext {
    ClaimByQueryContext {
        now: ts(now_s),
        eligibility_time: None,
        expected_epoch: None,
    }
}

#[test]
fn claim_by_item_ids_partial_outcomes_and_never_leases_outside_set() {
    futures::executor::block_on(async {
        let b = backend();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(
                &shard(),
                vec![push_spec("a", 10), push_spec("b", 20), push_spec("c", 30)],
                ts(0),
                None,
            )
            .await
            .unwrap();
        let [id_a, id_b, id_c] = [ids[0], ids[1], ids[2]];

        let first = b
            .claim_by_item_ids(&shard(), claim_req(vec![id_b], "r-lease-b", 1_000), ctx(10))
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].item_id, id_b);
        assert_eq!(
            first.outcomes,
            vec![ClaimByItemIdsOutcome {
                item_id: id_b,
                disposition: ClaimByItemIdsDisposition::Claimed,
            }]
        );

        let lease_c = b
            .claim_by_item_ids(&shard(), claim_req(vec![id_c], "r-lease-c", 1_000), ctx(20))
            .await
            .unwrap();
        assert_eq!(lease_c.items.len(), 1);
        b.finalize(
            &shard(),
            vec![FinalizeOutcome::new(id_c, FinalizeKind::Complete)],
            ts(21),
            None,
        )
        .await
        .unwrap();

        let missing = ItemId::from_u64(9_999_999);
        let mixed = b
            .claim_by_item_ids(
                &shard(),
                claim_req(vec![id_a, id_b, id_c, missing, id_a], "r-mixed", 1_000),
                ctx(30),
            )
            .await
            .unwrap();

        assert_eq!(mixed.items.len(), 1, "only a is claimable");
        assert_eq!(mixed.items[0].item_id, id_a);
        assert_eq!(
            mixed.outcomes,
            vec![
                ClaimByItemIdsOutcome {
                    item_id: id_a,
                    disposition: ClaimByItemIdsDisposition::Claimed,
                },
                ClaimByItemIdsOutcome {
                    item_id: id_b,
                    disposition: ClaimByItemIdsDisposition::Leased,
                },
                ClaimByItemIdsOutcome {
                    item_id: id_c,
                    disposition: ClaimByItemIdsDisposition::Terminal,
                },
                ClaimByItemIdsOutcome {
                    item_id: missing,
                    disposition: ClaimByItemIdsDisposition::NotFound,
                },
            ]
        );

        let metrics = b.metrics(&shard()).await.unwrap();
        assert_eq!(metrics.leased, 2);
        assert_eq!(metrics.complete, 1);
        assert_eq!(metrics.pending, 0);

        assert!(
            b.hot_projection_capabilities(&shard()).claim_by_item_ids,
            "compose memory product advertises claim_by_item_ids"
        );
    });
}

#[test]
fn claim_by_item_ids_point_lookup_cost_independent_of_unrelated_pending() {
    futures::executor::block_on(async {
        let b = backend();
        b.create_queue(qdef()).await.unwrap();

        let targets = b
            .push(
                &shard(),
                vec![push_spec("target-0", 1), push_spec("target-1", 2)],
                ts(0),
                None,
            )
            .await
            .unwrap();
        assert_eq!(targets.len(), 2);

        const N: usize = 2_000;
        let mut batch = Vec::with_capacity(100);
        for i in 0..N {
            batch.push(push_spec(&format!("noise-{i}"), 100 + i as i64));
            if batch.len() == 100 {
                b.push(&shard(), std::mem::take(&mut batch), ts(0), None)
                    .await
                    .unwrap();
            }
        }
        if !batch.is_empty() {
            b.push(&shard(), batch, ts(0), None).await.unwrap();
        }

        let class = b
            .with_projection(|p| {
                ProjectionStore::classify_claim_by_item_id(p, &shard(), &targets[0], ts(1))
            })
            .unwrap();
        assert_eq!(class, ClaimByItemIdClass::Claimable);

        let start = Instant::now();
        let resp = b
            .claim_by_item_ids(
                &shard(),
                claim_req(targets.clone(), "r-point", 5_000),
                ctx(1),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.items.len(), 2);
        let claimed: HashSet<_> = resp.items.iter().map(|i| i.item_id).collect();
        assert_eq!(claimed, HashSet::from([targets[0], targets[1]]));
        assert!(
            elapsed.as_millis() < 500,
            "claim_by_item_ids of 2 ids with {N} distractors took {elapsed:?} — possible full-shard scan"
        );

        let metrics = b.metrics(&shard()).await.unwrap();
        assert_eq!(metrics.leased, 2);
        assert_eq!(metrics.pending, N as u64);
    });
}

#[test]
fn claim_by_item_ids_lease_is_first_class_inspect_reclaim_fence() {
    // fireweed-cad0ab40: leases from claim_by_item_ids are ordinary API-001 leases —
    // inspect (claimed_view), timeout/reclaim_expired, and force fence → StaleLease.
    futures::executor::block_on(async {
        let b = backend();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(&shard(), vec![push_spec("life", 5)], ts(0), None)
            .await
            .unwrap();
        let id = ids[0];

        let claimed = b
            .claim_by_item_ids(&shard(), claim_req(vec![id], "r-life", 1_000), ctx(100))
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);
        let item = &claimed.items[0];
        assert_eq!(item.item_id, id);
        assert!(item.lease_token.is_some());
        assert_eq!(item.lease_expires_at, ts(101));
        let token = item.lease_token.clone().unwrap();

        let views = b.claimed_view(&shard(), &[id]).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].lease_token.as_ref(), Some(&token));
        assert_eq!(views[0].lease_expires_at, ts(101));

        let reclaimed = b
            .reclaim_expired(&shard(), Some(10), ts(200), None)
            .await
            .unwrap();
        assert_eq!(reclaimed, vec![id]);

        let after = b
            .claim_by_item_ids(&shard(), claim_req(vec![id], "r-reclaim", 1_000), ctx(210))
            .await
            .unwrap();
        assert_eq!(
            after.outcomes[0].disposition,
            ClaimByItemIdsDisposition::Claimed,
            "item claimable again after reclaim"
        );

        b.reassign(
            &shard(),
            vec![id],
            LeaseToken::new("forced-owner").unwrap(),
            ts(500),
            ts(220),
            None,
        )
        .await
        .unwrap();
        let views = b.claimed_view(&shard(), &[id]).await.unwrap();
        assert_eq!(
            views[0].lease_token.as_ref().map(|t| t.as_str()),
            Some("forced-owner")
        );

        let epoch = 0u64;
        let envelope = CommandEnvelope {
            command_id: CommandId::new("fence-1"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![id],
            command: QueueCommand::FenceLease(FenceLeaseCommand { item_ids: vec![id] }),
            checksum: CommandChecksum(0),
            created_at: ts(230),
        };
        b.commit_raw(RawCommitRequest::new(shard(), vec![envelope], epoch))
            .await
            .expect("fence commit");

        assert_eq!(
            b.renew(&shard(), vec![id], ts(600), ts(240), None).await,
            Err(EngineError::StaleLease)
        );
        assert_eq!(
            b.finalize(
                &shard(),
                vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
                ts(240),
                None,
            )
            .await,
            Err(EngineError::StaleLease)
        );
    });
}

#[test]
fn claim_by_item_ids_idempotent_replay() {
    futures::executor::block_on(async {
        let b = backend();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(&shard(), vec![push_spec("idem", 1)], ts(0), None)
            .await
            .unwrap();
        let id = ids[0];
        let req = claim_req(vec![id], "same-rid", 5_000);
        let first = b
            .claim_by_item_ids(&shard(), req.clone(), ctx(10))
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        let second = b.claim_by_item_ids(&shard(), req, ctx(10)).await.unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(
            first.items[0].lease_token, second.items[0].lease_token,
            "same-body request_id replay returns same lease token"
        );
        assert_eq!(b.metrics(&shard()).await.unwrap().leased, 1);

        let conflict = ClaimByItemIdsRequest {
            item_ids: vec![id],
            lease_duration_ms: 9_000,
            worker_id: worker("w1"),
            request_id: request_id("same-rid"),
            lease_token: None,
        };
        assert_eq!(
            b.claim_by_item_ids(&shard(), conflict, ctx(10)).await,
            Err(EngineError::RequestIdConflict)
        );
    });
}
