//! Conformance for the memory backend.
//!
//! The full **port-level** behavioral no-stub suite is shared (`pqueue-conformance`) and run against
//! `MemoryBackend` by the `conformance_suite!` invocation below. The projection-internals white-box
//! tests (item_version monotonicity, high-water survives compaction) now live in `pqueue-projection`,
//! where that state lives. What remains here is the test-only `ManualClock`/`SeqIdGen` helpers, which
//! are memory-specific and not expressible through the ports.

use super::*;
use pqueue_conformance::ts;

// The full shared backend-conformance suite (16 port-level scenarios) against the composed memory backend
// (`ComposedBackend<MemoryLog, InMemoryProjection, InProcessControlPlane>`).
pqueue_conformance::conformance_suite!(composed_memory_backend);

#[tokio::test]
async fn filtered_lifecycle_metrics_conformance() {
    pqueue_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(
        composed_memory_backend,
    )
    .await;
}

/// ADR-012 Phase 1b-i: CAPABILITY PARITY between the composed memory backend and the monolithic
/// `MemoryBackend`. The shared conformance suite above already covers the data-plane ports; these
/// white-box tests cover the commit-class ports the monolith implements that the composition previously
/// took `Unavailable` defaults for — the Snorri authoritative vectorized commit boundary + its recovery
/// reads, reschedule, and the relational-only refusals (set_gates / discover) — run against
/// `composed_memory_backend()`.
mod composed_capability_parity {
    use super::*;
    use crate::composed_memory_backend;
    use bytes::Bytes;
    use pqueue_conformance::{claim_req, qdef, qkey, shard};
    use pqueue_core::{PriorityValue, RequestId};
    use pqueue_engine::{
        Backend, ClaimPort, ClaimRef, CommitTransition, CommitTransitionEntry,
        CommitTransitionPort, ControlPlaneStore, DiscoveryGranularity, DiscoveryPort, EngineError,
        FinalizeKind, InstanceFence, LogRead, ProjectionRead, PushPort, PushSpec, QueueCommand,
        ReschedulePort, ScheduleUpdate, SetGatesCommand, SetGatesPort, SideRecord,
    };
    pub(super) async fn seeded_commit_transition_memory_backend() -> crate::ComposedMemoryBackend {
        let b = composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        b.push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let claimed = b.claim(claim_req(1, 60, 0)).await.unwrap();
        let c = &claimed.items[0];
        let claim_ref = ClaimRef {
            item_id: c.item_id,
            lease_token: c.lease_token.clone().unwrap(),
            lease_expires_at: c.lease_expires_at,
            item_version: c.item_version,
        };
        let rid = RequestId::new("txn-commit-transition-1").unwrap();
        b.commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(rid),
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::from_static(b"audit-bytes"),
                    }],
                    lifecycle_items: vec![PushSpec {
                        priority: Some(PriorityValue::Int64(20)),
                        ..Default::default()
                    }],
                    instance_fence: Some(InstanceFence {
                        instance_key: b"wf-1".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
        b
    }

    /// Capability descriptors: the composed memory backend advertises the SAME full vectorized-commit
    /// boundary as `MemoryBackend` (atomic class, the Snorri StateStore guarantees).
    #[tokio::test]
    async fn commit_capabilities_reach_memory_parity() {
        let composed = composed_memory_backend().commit_capabilities();
        let mono = composed_memory_backend().commit_capabilities();
        // Every guarantee field must match the monolith (the `consistency` note is an intentionally
        // substrate-descriptive string, not a capability).
        assert_eq!(
            composed.atomic_transition_commit,
            mono.atomic_transition_commit
        );
        assert_eq!(composed.vectorized_commit, mono.vectorized_commit);
        assert_eq!(composed.lease_validation, mono.lease_validation);
        assert_eq!(
            composed.retained_commit_idempotency,
            mono.retained_commit_idempotency
        );
        assert_eq!(composed.non_work_side_records, mono.non_work_side_records);
        assert_eq!(
            composed.authoritative_recovery_reads,
            mono.authoritative_recovery_reads
        );
        assert_eq!(composed.delayed_awaits_timers, mono.delayed_awaits_timers);
        assert_eq!(composed.durability_class, mono.durability_class);
        assert!(composed.atomic_transition_commit);
        assert!(composed.authoritative_recovery_reads);
    }

    /// Request-id CONFLICT on a body change under the same id (commit-path idempotency parity).
    #[tokio::test]
    async fn commit_request_id_conflicts_on_body_change() {
        let b = composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        b.push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let claimed = b.claim(claim_req(1, 60, 0)).await.unwrap();
        let c = &claimed.items[0];
        let mk_ref = || ClaimRef {
            item_id: c.item_id,
            lease_token: c.lease_token.clone().unwrap(),
            lease_expires_at: c.lease_expires_at,
            item_version: c.item_version,
        };
        let rid = RequestId::new("txn-conflict").unwrap();
        b.commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![CommitTransitionEntry {
                    claim_ref: mk_ref(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
        // Same id, DIFFERENT body (a lifecycle item now) -> RequestIdConflict.
        let err = b
            .commit_transition(
                &shard(),
                CommitTransition {
                    request_id: Some(rid),
                    entries: vec![CommitTransitionEntry {
                        claim_ref: mk_ref(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![PushSpec::default()],
                        instance_fence: None,
                    }],
                },
                ts(2),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err, EngineError::RequestIdConflict);
    }

    /// Reschedule (atomic priority/not_before change) works through the composition and bumps item_version.
    #[tokio::test]
    async fn reschedule_reprices_live_item() {
        let b = composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        let id = b
            .push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap()[0];
        let v0 = b.peek(&shard(), 10).await.unwrap()[0].item_version;
        let v1 = b
            .reschedule(
                &shard(),
                id,
                ScheduleUpdate::Set(Some(PriorityValue::Int64(42))),
                ScheduleUpdate::Keep,
                Some(v0),
                ts(1),
                None,
            )
            .await
            .unwrap();
        assert!(v1 > v0, "reschedule bumps the item version");
    }

    /// The relational-only ports stay refused on the log-replay composition, exactly like `MemoryBackend`.
    #[tokio::test]
    async fn set_gates_and_discover_are_unavailable() {
        let b = composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        let set_gates = b
            .set_gates(
                &shard(),
                SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: true,
                },
                ts(0),
                None,
            )
            .await;
        assert_eq!(set_gates, Err(EngineError::Unavailable));
        let discover = b
            .discover_active_scopes(&qkey(), DiscoveryGranularity::Queue, ts(0))
            .await;
        assert!(matches!(discover, Err(EngineError::Unavailable)));
    }

    /// The commit envelope's request id propagates into every appended commit-path command (acceptance #4),
    /// proven against the composition the same way the monolith proves it.
    #[tokio::test]
    async fn commit_path_propagates_request_id_into_every_command_envelope() {
        let b = composed_memory_backend();
        b.create_queue(qdef()).await.unwrap();
        b.push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let claimed = b.claim(claim_req(1, 60, 0)).await.unwrap();
        let c = &claimed.items[0];
        let claim_ref = ClaimRef {
            item_id: c.item_id,
            lease_token: c.lease_token.clone().unwrap(),
            lease_expires_at: c.lease_expires_at,
            item_version: c.item_version,
        };
        let rid = RequestId::new("txn-c10").unwrap();
        b.commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: b"state/run".to_vec(),
                        payload: Bytes::from_static(b"opaque"),
                    }],
                    lifecycle_items: vec![PushSpec::default()],
                    instance_fence: Some(InstanceFence {
                        instance_key: b"wf-1".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();

        let page = b.read_from(&shard(), None, 1000).await.unwrap();
        let mut commit_path_envs = 0;
        for (_pos, env) in &page.entries {
            match &env.command {
                QueueCommand::WriteSideRecords(_)
                | QueueCommand::AdvanceInstanceFence(_)
                | QueueCommand::Finalize(_) => {
                    assert_eq!(env.request_id.as_ref(), Some(&rid));
                    commit_path_envs += 1;
                }
                QueueCommand::Push(_) if env.request_id.is_some() => {
                    assert_eq!(env.request_id.as_ref(), Some(&rid));
                    commit_path_envs += 1;
                }
                _ => {}
            }
        }
        assert_eq!(commit_path_envs, 4);
    }
}

#[tokio::test]
async fn manual_clock_and_idgen_are_real() {
    let clock = ManualClock::at(10);
    assert_eq!(clock.now(), ts(10));
    clock.set(20);
    assert_eq!(clock.now(), ts(20));

    let ids = SeqIdGen::default();
    let a = ids.next_item_id();
    let b = ids.next_item_id();
    assert_ne!(a, b, "ids must be unique, not a no-op constant");
}

/// ADR-009 collision fix: two instances with distinct `node_id`s minting into the same queue at the same
/// epoch+counter produce DISTINCT ids (the node byte disambiguates). The pre-fix per-connection counter
/// gave both writers identical ids — this is the regression guard.
#[tokio::test]
async fn distinct_node_ids_never_collide_on_concurrent_push() {
    use pqueue_conformance::{qdef, shard};
    use pqueue_engine::{ControlPlaneStore, PushPort, PushSpec};

    let a = composed_memory_backend().with_node_id(1);
    let b = composed_memory_backend().with_node_id(7);
    a.create_queue(qdef()).await.unwrap();
    b.create_queue(qdef()).await.unwrap();

    // Both writers push the FIRST item into the same queue at the genesis epoch (counter base 0 on each).
    let ida = a
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap()[0];
    let idb = b
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap()[0];

    assert_ne!(ida, idb, "same epoch+counter on two nodes must not collide");
    assert_eq!((ida.node(), ida.counter()), (1, 0));
    assert_eq!((idb.node(), idb.counter()), (7, 0));
    // The dedup client_item_key (defaulting to the id) is likewise distinct.
    assert_ne!(ida.to_string(), idb.to_string());
}

#[tokio::test]
async fn gate_bearing_push_and_raw_setgates_are_rejected_before_commit() {
    use pqueue_conformance::{envelope, qdef, qkey, shard, ts};
    use pqueue_engine::{
        Backend, ControlPlaneStore, EngineError, LogWriter, ProjectionRead, ProjectionWriter,
        PushPort, PushSpec, QueueCommand, SetGatesCommand,
    };

    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();

    let err = b
        .push(
            &shard(),
            vec![PushSpec {
                gate_keys: vec!["hold".to_string()],
                ..Default::default()
            }],
            ts(0),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 0);

    let env = envelope(
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["hold".to_string()],
            blocked: true,
        }),
        vec![],
    );
    let epoch = b.current_epoch(&shard()).await.unwrap();
    let err = b
        .write(
            move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
                let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
                pw.apply(&pos, std::slice::from_ref(&env))?;
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
}

/// B1a (ADR-009 / TD-003 In-Process Library Owner-Runtime): a claim stamped with the owner's *cached*
/// acquire-time epoch is fenced at commit once a newer epoch is acquired (the owner was superseded), and
/// leases nothing; the current-epoch owner claims normally; `None` (sole-owner) is unaffected.
#[tokio::test]
async fn claim_fences_superseded_owner_epoch() {
    use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, ProjectionRead, PushCommand,
        QueueCommand,
    };

    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();
    // Push one item at the current (genesis) epoch via the shared commit helper (degenerate owner).
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

    // Ownership handoff: acquire a strictly-greater epoch (0 -> 1), durably superseding the epoch-0 owner.
    let e1 = b.acquire_epoch(&shard()).await.unwrap();
    assert!(e1 >= 1, "acquire advances the durable epoch");

    // A claim carrying the STALE cached epoch (0) is fenced at commit and leases nothing.
    let stale = ClaimRequest {
        eligibility_time: None,
        expected_epoch: Some(0),
        ..claim_req(10, 500, 100)
    };
    assert!(
        matches!(b.claim(stale).await, Err(EngineError::EpochFenced)),
        "a superseded owner's claim must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().leased,
        0,
        "a fenced claim must lease nothing (atomic reject before apply)"
    );

    // The current-epoch owner claims normally.
    let ok = ClaimRequest {
        eligibility_time: None,
        expected_epoch: Some(e1),
        ..claim_req(10, 500, 100)
    };
    let claimed = b.claim(ok).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "current-epoch owner claims the item"
    );
}

/// B1b (ADR-009 / TD-003): the same cached-epoch fence applies to `PushPort::push` — a superseded owner's
/// push is `EpochFenced` and appends nothing; the current-epoch owner appends normally.
#[tokio::test]
async fn push_fences_superseded_owner_epoch() {
    use pqueue_conformance::{qdef, qkey, shard};
    use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort, PushSpec};

    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();
    let e1 = b.acquire_epoch(&shard()).await.unwrap(); // advance genesis 0 -> 1
    assert!(e1 >= 1);

    // Stale-epoch push is fenced and appends nothing.
    assert!(
        matches!(
            b.push(&shard(), vec![PushSpec::default()], ts(0), Some(0))
                .await,
            Err(EngineError::EpochFenced)
        ),
        "a superseded owner's push must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        0,
        "a fenced push must append nothing"
    );

    // Current-epoch push succeeds.
    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(1), Some(e1))
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
}

/// Graceful-degradation guard (bead pqueue-79178303): a sole owner whose lease LAPSES under CPU starvation
/// re-acquires its OWN queue WITHOUT self-fencing. This drives the real ownership primitive
/// (`acquire_and_fence`) over the reference control plane + the memory storage fence, then proves an
/// in-flight push stamped with the PRIOR fence epoch is STILL accepted (no `EpochFenced`). Before the fix
/// the re-acquire bumped the epoch, advanced the storage fence, and fenced the node's own in-flight writes
/// — a self-inflicted retry storm that collapsed throughput instead of degrading gracefully.
#[tokio::test]
async fn lapsed_same_owner_reacquire_does_not_self_fence_inflight_writes() {
    use pqueue_conformance::{qdef, qkey, shard};
    use pqueue_core::OwnerId;
    use pqueue_engine::{
        ControlPlaneConfig, ControlPlaneStore, InMemoryControlPlane, OwnershipOutcome,
        ProjectionRead, PushPort, PushSpec, QueueControlPlane, acquire_and_fence,
    };

    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();

    // Reference control plane: heartbeat TTL 5s, lease TTL 15s (the defaults that expose the bug).
    let cp = InMemoryControlPlane::new(ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 15_000,
    });
    let a = OwnerId::new("node-a").unwrap();
    cp.register_owner(&a, ts(0)).unwrap();

    // First acquire binds the lease to the storage fence (genesis 0 -> 1).
    let OwnershipOutcome::Owned(session1) = acquire_and_fence(&cp, &b, &shard(), &a, ts(0))
        .await
        .unwrap()
    else {
        panic!("expected Owned");
    };
    assert_eq!(session1.fence_epoch, 1);

    // The owner serves: an in-flight push at its fence epoch lands.
    b.push(
        &shard(),
        vec![PushSpec::default()],
        ts(1),
        Some(session1.fence_epoch),
    )
    .await
    .unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);

    // Under CPU starvation the renew task ran late: the lease TTL (15s) lapsed by ts(100). The owner is
    // still heartbeat-live (it re-registers) and re-resolves to itself, so it re-acquires its OWN queue.
    cp.register_owner(&a, ts(100)).unwrap();
    let OwnershipOutcome::Owned(session2) = acquire_and_fence(&cp, &b, &shard(), &a, ts(100))
        .await
        .unwrap()
    else {
        panic!("expected Owned on same-owner re-affirm");
    };

    // The fix: the epoch is PRESERVED across the lapse, so the storage fence does NOT advance.
    assert_eq!(
        session2.fence_epoch, session1.fence_epoch,
        "re-acquiring a lapsed OWN lease must keep the fence epoch (no self-advance)"
    );
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        session1.fence_epoch,
        "the durable storage fence must not advance on a same-owner re-affirm"
    );

    // The proof of graceful degradation: an in-flight write stamped with the PRIOR fence epoch is NOT
    // fenced — the slow-but-alive owner keeps serving; it did not collapse itself.
    b.push(
        &shard(),
        vec![PushSpec::default()],
        ts(101),
        Some(session1.fence_epoch),
    )
    .await
    .unwrap();
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        2,
        "the prior-epoch in-flight write must still commit (no self-fence storm)"
    );
}

/// B1b (ADR-009 / TD-003): the cached-epoch fence also covers `FinalizePort::finalize` — completing the
/// TD-003 explicit Push/Claim/Finalize fence MUST. A superseded owner's finalize is `EpochFenced` and
/// makes no lifecycle transition; the current-epoch owner finalizes normally.
#[tokio::test]
async fn finalize_fences_superseded_owner_epoch() {
    use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
    use pqueue_engine::{
        ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, FinalizeKind, FinalizeOutcome,
        FinalizePort, ProjectionRead, PushCommand, QueueCommand,
    };

    let b = composed_memory_backend();
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
    // Lease the item under the degenerate (sole-owner) path.
    let claimed = b
        .claim(ClaimRequest {
            eligibility_time: None,
            expected_epoch: None,
            ..claim_req(10, 500, 10)
        })
        .await
        .unwrap();
    let id = claimed.items[0].item_id;

    let e1 = b.acquire_epoch(&shard()).await.unwrap(); // ownership handoff 0 -> 1
    let outcomes = vec![FinalizeOutcome::new(id, FinalizeKind::Complete)];

    assert!(
        matches!(
            b.finalize(&shard(), outcomes.clone(), ts(20), Some(0))
                .await,
            Err(EngineError::EpochFenced)
        ),
        "a superseded owner's finalize must be EpochFenced"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().complete,
        0,
        "a fenced finalize must make no transition"
    );

    b.finalize(&shard(), outcomes, ts(20), Some(e1))
        .await
        .unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}

/// Acceptance #4 (epic pqueue-2201fd37): the vectorized claimed-work commit path PROPAGATES the caller's
/// request id into EVERY backend command envelope it appends — `WriteSideRecords`, `AdvanceInstanceFence`,
/// the lifecycle `Push`, and `Finalize` — and does NOT construct any of them with `request_id: None`.
///
/// This is a RUNTIME assertion, not a source grep: it commits one entry that forces all four commit-path
/// command kinds, then reads the durable log back through [`LogRead`] and asserts each commit-path envelope
/// carries `request_id == Some(rid)`. The request-id-less input `Push` and the `Claim` are kept in the same
/// log as negative controls, proving the assertion actually discriminates `Some` from `None`.
#[tokio::test]
async fn commit_path_propagates_request_id_into_every_command_envelope() {
    use bytes::Bytes;
    use pqueue_conformance::{claim_req, qdef, shard, ts};
    use pqueue_core::RequestId;
    use pqueue_engine::{
        ClaimPort, ClaimRef, CommitTransition, CommitTransitionEntry, CommitTransitionPort,
        ControlPlaneStore, FinalizeKind, InstanceFence, LogRead, PushPort, PushSpec, QueueCommand,
        SideRecord,
    };

    let b = composed_memory_backend();
    b.create_queue(qdef()).await.unwrap();

    // Push one input item WITHOUT a request id: this envelope MUST carry `request_id: None`. It is the
    // negative control that proves the commit-path assertion below isn't trivially true.
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    // Claim it to obtain the lease-token + version-bearing claim_ref the commit validates inside its boundary.
    let claimed = b.claim(claim_req(1, 60, 0)).await.unwrap();
    let c = &claimed.items[0];
    let claim_ref = ClaimRef {
        item_id: c.item_id,
        lease_token: c
            .lease_token
            .clone()
            .expect("claimed item carries a lease token"),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    };

    // One entry that forces ALL FOUR commit-path envelope kinds in a single transition: a side record
    // (WriteSideRecords), an instance fence (AdvanceInstanceFence), a lifecycle item (Push), and a finalize
    // (Finalize). The caller request id must thread into every one of them.
    let rid = RequestId::new("txn-c10").unwrap();
    let outcomes = b
        .commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![SideRecord {
                        key: b"state/run".to_vec(),
                        payload: Bytes::from_static(b"opaque"),
                    }],
                    lifecycle_items: vec![PushSpec::default()],
                    instance_fence: Some(InstanceFence {
                        instance_key: b"wf-1".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1, "one entry committed");

    // Read the durable log back and inspect every appended envelope's `request_id`.
    let page = b.read_from(&shard(), None, 1000).await.unwrap();
    let mut commit_path_envs = 0;
    let mut saw_input_push_without_rid = false;
    let mut saw_claim_without_rid = false;
    for (_pos, env) in &page.entries {
        match &env.command {
            // The three commit-path command kinds that are unambiguous (no non-commit producer in this test).
            QueueCommand::WriteSideRecords(_)
            | QueueCommand::AdvanceInstanceFence(_)
            | QueueCommand::Finalize(_) => {
                assert_eq!(
                    env.request_id.as_ref(),
                    Some(&rid),
                    "commit-path envelope must carry the caller request id, not None"
                );
                commit_path_envs += 1;
            }
            // Two pushes reach the log: the request-id-less input push (None) and the commit's lifecycle push
            // (Some(rid)). Discriminate by request_id — the commit one MUST be Some(rid).
            QueueCommand::Push(_) => {
                if env.request_id.is_some() {
                    assert_eq!(env.request_id.as_ref(), Some(&rid));
                    commit_path_envs += 1;
                } else {
                    saw_input_push_without_rid = true;
                }
            }
            // The claim is NOT on the commit path: it must carry no request id (negative control).
            QueueCommand::Claim(_) => {
                assert!(
                    env.request_id.is_none(),
                    "non-commit claim envelope carries no request id"
                );
                saw_claim_without_rid = true;
            }
            _ => {}
        }
    }
    assert_eq!(
        commit_path_envs, 4,
        "WriteSideRecords + AdvanceInstanceFence + lifecycle Push + Finalize all propagated Some(rid)"
    );
    assert!(
        saw_input_push_without_rid,
        "the request-id-less input push is the negative control proving None is observable"
    );
    assert!(
        saw_claim_without_rid,
        "the non-commit claim envelope is a second negative control"
    );
}

/// Shared commit-transition positive scenario wired through the composed memory backend.
///
/// The reopen leg is reconstructed on the second `make()` call because the composed memory backend is
/// in-process only; the point of the test is to exercise the shared contract against the backend, not to
/// change its storage model.
#[tokio::test]
async fn commit_transition_shared_scenario_runs_against_composed_memory() {
    use pqueue_conformance::scenarios::commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen;

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let make_calls = std::sync::Arc::clone(&calls);
    let make = move || match make_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
        0 => composed_memory_backend(),
        _ => std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(composed_capability_parity::seeded_commit_transition_memory_backend())
        })
        .join()
        .unwrap(),
    };

    commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(make)
        .await;
}
