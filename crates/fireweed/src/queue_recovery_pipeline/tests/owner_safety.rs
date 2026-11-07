//! Fault tests, one per safety rule of the owner loop. Time is manual: the lease
//! clock and the wall clock move together, and stalls are injected by advancing
//! them from a stage hook.
use super::ownership::TTL;
use super::record_counts::{Env, commit, delta, grow, insert, now, reads};
use super::*;
use fireweed_conformance::queue_contract as suite;
use fireweed_engine::{AsyncQueueControlPlane, LeaseState, queue_contract::QueueStore};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const SECOND: Duration = Duration::from_secs(1);

pub(super) async fn stats(
    store: &NativeQueueStore,
    queue: &QueueAddress,
) -> QueueResult<QueueStats> {
    store
        .stats(StatsRequest {
            buckets: None,
            queue: queue.clone(),
            filter: suite::filter("counts"),
            group_by: vec![],
            eligible_at: 0,
        })
        .await
}
pub(super) async fn pending_items(store: &NativeQueueStore, queue: &QueueAddress) -> u64 {
    stats(store, queue).await.unwrap().totals.pending
}
fn hook_once(
    store: &NativeQueueStore,
    stage: owner::Stage,
    action: impl Fn() + Send + Sync + 'static,
) {
    let fired = Arc::new(AtomicBool::new(false));
    *store.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |at| {
        if at == stage && !fired.swap(true, Ordering::SeqCst) {
            action();
        }
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_handle_gets_busy() {
    let env = Env::new("busy", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    let b = env.open("b.db").await;
    grow(&a, &queue, 4).await;
    let before_b = now(&b);
    assert_eq!(stats(&b, &queue).await.err(), Some(QueueError::Busy));
    assert_eq!(
        b.insert(InsertRequest {
            queue: queue.clone(),
            request_id: "busy-insert".into(),
            mode: InsertMode::InsertOnly,
            items: vec![suite::item("busy", "counts", 0, 0)],
        })
        .await
        .err(),
        Some(QueueError::Busy)
    );
    // No log record read, no fence, and nothing changed for the owner.
    assert_eq!(delta(&b, before_b), (0, 0, 0));
    let before = now(&a);
    assert_eq!(pending_items(&a, &queue).await, 3);
    insert(&a, &queue, 77).await;
    assert_eq!(pending_items(&a, &queue).await, 4);
    assert_eq!(delta(&a, before), (0, 0, 0));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_stops_at_deadline_with_control_plane_down() {
    let env = Env::new("down", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    env.ownership.plane.down.store(true, Ordering::SeqCst);
    // Before the deadline the owner needs no contact with anything.
    env.ownership.advance(5 * SECOND);
    assert_eq!(pending_items(&a, &queue).await, 3);
    // Past renew_at the renewal fails; the deadline stays where it was.
    env.ownership.advance(3 * SECOND);
    assert_eq!(pending_items(&a, &queue).await, 3);
    // Past the deadline no read is served, and no call decided that.
    env.ownership.advance(8 * SECOND);
    assert_eq!(stats(&a, &queue).await.err(), Some(QueueError::Busy));
    env.ownership.plane.down.store(false, Ordering::SeqCst);
    assert_eq!(pending_items(&a, &queue).await, 3);
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_is_measured_from_before_the_request() {
    let env = Env::new("slow-renew", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    env.ownership.advance(8 * SECOND);
    // The renewal is sent at t = 8 s and answered 10 s later.
    let slow = env.ownership.clone();
    *env.ownership.plane.during_renew.lock().unwrap() =
        Some(Box::new(move || slow.advance(10 * SECOND)));
    assert_eq!(pending_items(&a, &queue).await, 3);
    *env.ownership.plane.during_renew.lock().unwrap() = None;
    assert_eq!(env.ownership.plane.renewals.load(Ordering::SeqCst), 1);
    // Now t = 18 s. The deadline is 8 + 15 = 23 s, not 18 + 15 = 33 s. Keep
    // later renewals from moving it.
    env.ownership.plane.down.store(true, Ordering::SeqCst);
    env.ownership.advance(4 * SECOND);
    assert_eq!(
        pending_items(&a, &queue).await,
        3,
        "t = 22 s is within 23 s"
    );
    env.ownership.advance(2 * SECOND);
    assert_eq!(
        stats(&a, &queue).await.err(),
        Some(QueueError::Busy),
        "t = 24 s is past the deadline measured from before the request"
    );
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_owner_waits_ttl_plus_skew() {
    let env = Env::new("skew", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    let skew = 3 * SECOND;
    let b = env
        .open_with("b.db", env.ownership.handle_with_skew("b", skew))
        .await;
    grow(&a, &queue, 4).await;
    // While the first lease is live the second handle is refused.
    env.ownership.advance(TTL - SECOND);
    assert_eq!(stats(&b, &queue).await.err(), Some(QueueError::Busy));
    // The lease has expired, but the new owner still serves nothing.
    env.ownership.advance(2 * SECOND);
    let waiting = {
        let b = b.clone();
        let queue = queue.clone();
        tokio::spawn(async move { stats(&b, &queue).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "served before the skew allowance");
    env.ownership.lease_clock.advance(skew);
    let served = tokio::time::timeout(Duration::from_secs(30), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(served.totals.pending, 3);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_failure_drops_projection_and_exits() {
    // A lost reply, a superseded fence and a foreign writer all take one path.
    for case in ["lost-reply", "fence-bumped", "foreign-writer"] {
        let env = Env::new(case, false);
        let queue = env.address("q");
        let a = env.open("a.db").await;
        grow(&a, &queue, 4).await;
        let pipeline = a.pipeline_for_test();
        let foreign = match case {
            "lost-reply" => {
                pipeline.lose_append_reply.store(true, Ordering::SeqCst);
                None
            }
            "fence-bumped" => {
                let job = Arc::new(
                    QueueAdmission::new(1, 64 << 20)
                        .unwrap()
                        .reserve(0)
                        .unwrap(),
                );
                let namespace = pipeline
                    .store
                    .bind_api007_namespace(&queue, &job)
                    .await
                    .unwrap();
                pipeline
                    .store
                    .acquire_api007_state(namespace, &job, &pipeline.workers)
                    .await
                    .unwrap();
                None
            }
            _ => {
                // A handle with its own control plane writes behind the owner's back.
                let other = env
                    .open_with("x.db", TestOwnership::new().handle("x"))
                    .await;
                insert(&other, &queue, 500).await;
                Some(other)
            }
        };
        let before = now(&a);
        let outcome = a
            .insert(InsertRequest {
                queue: queue.clone(),
                request_id: "after-fault".into(),
                mode: InsertMode::InsertOnly,
                items: vec![suite::item("after-fault", "counts", 0, 0)],
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome[0], InsertOutcome::Inserted { .. }),
            "{case}: the original is found by id, once"
        );
        let (records, _, fences) = delta(&a, before);
        let log = match case {
            "fence-bumped" => 4,
            // the published record whose reply was lost, or the foreign record
            _ => 5,
        };
        assert_eq!(
            (records, fences),
            (log, 1),
            "{case}: re-entry rebuilt from the log"
        );
        let expected = 3 + 1 + u64::from(case == "foreign-writer");
        assert_eq!(pending_items(&a, &queue).await, expected, "{case}");
        let before = now(&a);
        insert(&a, &queue, 600).await;
        assert_eq!(delta(&a, before), (0, 0, 0), "{case}: healthy again");
        if let Some(other) = foreign {
            other.shutdown().await.unwrap();
        }
        a.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zombie_publish_is_rejected() {
    // A handle that lost the queue to a newer fence cannot publish; its request
    // is replanned under a newer fence, never written over the newer owner.
    let env = Env::new("zombie", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    let other = env
        .open_with("x.db", TestOwnership::new().handle("x"))
        .await;
    insert(&other, &queue, 900).await;
    insert(&other, &queue, 901).await;
    insert(&a, &queue, 902).await;
    // Every insert is present exactly once: nothing was overwritten or doubled.
    assert_eq!(pending_items(&a, &queue).await, 3 + 3);
    other.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_owner_rechecks_before_ack() {
    for (stage, plane_down) in [
        (owner::Stage::BatchTaken, true),
        (owner::Stage::AfterRead, true),
        (owner::Stage::BatchTaken, false),
        (owner::Stage::AfterRead, false),
    ] {
        let env = Env::new("stall", false);
        let queue = env.address("q");
        let a = env.open("a.db").await;
        grow(&a, &queue, 4).await;
        let ownership = env.ownership.clone();
        hook_once(&a, stage, move || {
            // The batch was taken, or read, and then the process stalled past the deadline.
            ownership.advance(TTL + SECOND);
            if plane_down {
                ownership.plane.down.store(true, Ordering::SeqCst);
            }
        });
        let before = now(&a);
        let result = stats(&a, &queue).await;
        if plane_down {
            assert_eq!(
                result.err(),
                Some(QueueError::Busy),
                "{stage:?}: no stale read"
            );
        } else {
            // Answered only after re-entry rebuilt from the log.
            assert_eq!(result.unwrap().totals.pending, 3, "{stage:?}");
            assert_eq!(delta(&a, before), (4, 1, 1), "{stage:?}");
        }
        env.ownership.plane.down.store(false, Ordering::SeqCst);
        a.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_renewal_reply_is_safe() {
    let env = Env::new("lost-renewal", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    env.ownership.advance(8 * SECOND);
    env.ownership
        .plane
        .lose_renew_reply
        .store(true, Ordering::SeqCst);
    // The renewal lands in the control plane but the owner never learns it.
    assert_eq!(pending_items(&a, &queue).await, 3);
    assert_eq!(env.ownership.plane.renewals.load(Ordering::SeqCst), 1);
    // The deadline is still t0 + ttl: shortly past it the owner re-enters.
    env.ownership.lease_clock.advance(8 * SECOND);
    let before = now(&a);
    assert_eq!(pending_items(&a, &queue).await, 3);
    assert_eq!(delta(&a, before), (4, 1, 1), "stopped at the old deadline");
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_reads_only_the_suffix() {
    let env = Env::new("handoff", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    let b = env.open("b.db").await;
    grow(&a, &queue, 50).await;
    commit(&a, &queue, 51).await;
    env.ownership.advance(TTL + SECOND);
    let before = now(&b);
    reads(&b, &queue).await;
    let (records, _, fences) = delta(&b, before);
    assert_eq!((records, fences), (51, 1), "one rebuild from the log");
    let before = now(&b);
    reads(&b, &queue).await;
    assert_eq!(delta(&b, before), (0, 0, 0));
    // The old owner is Busy, not stale.
    assert_eq!(stats(&a, &queue).await.err(), Some(QueueError::Busy));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_instance_id_restart_waits_for_expiry() {
    // A restart that reuses its instance id is still a different owner.
    let env = Env::new("restart", false);
    let queue = env.address("q");
    let before = env
        .open_with("a.db", env.ownership.handle("instance"))
        .await;
    grow(&before, &queue, 4).await;
    let after = env
        .open_with("b.db", env.ownership.handle("instance"))
        .await;
    assert_eq!(stats(&after, &queue).await.err(), Some(QueueError::Busy));
    env.ownership.advance(TTL + SECOND);
    assert_eq!(pending_items(&after, &queue).await, 3);
    before.shutdown().await.unwrap();
    after.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requeued_mutation_is_never_busy() {
    let env = Env::new("requeued", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    let request = || InsertRequest {
        queue: queue.clone(),
        request_id: "requeued".into(),
        mode: InsertMode::InsertOnly,
        items: vec![suite::item("requeued", "counts", 0, 0)],
    };
    // The append lands, its reply is lost, and re-entry cannot reach the control plane.
    a.pipeline_for_test()
        .lose_append_reply
        .store(true, Ordering::SeqCst);
    env.ownership.plane.down.store(true, Ordering::SeqCst);
    assert!(matches!(
        a.insert(request()).await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    env.ownership.plane.down.store(false, Ordering::SeqCst);
    let outcome = a.insert(request()).await.unwrap();
    assert!(matches!(outcome[0], InsertOutcome::Inserted { .. }));
    assert_eq!(pending_items(&a, &queue).await, 4, "published once");
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_batch_renews_instead_of_lapsing() {
    let env = Env::new("long-batch", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    // The owner lapses; the next batch re-enters once and then takes longer than the lease.
    env.ownership.advance(TTL + SECOND);
    let ownership = env.ownership.clone();
    *a.pipeline_for_test().on_stage.lock().unwrap() = Some(Arc::new(move |stage| {
        if stage == owner::Stage::BeforeAppend {
            ownership.advance(4 * SECOND);
        }
    }));
    let before = now(&a);
    let mut tasks = Vec::new();
    for n in 0..6 {
        let a = a.clone();
        let queue = queue.clone();
        tasks.push(tokio::spawn(
            async move { insert(&a, &queue, 300 + n).await },
        ));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let (_, _, fences) = delta(&a, before);
    assert_eq!(fences, 1, "one entry; the batch renewed instead of lapsing");
    assert!(env.ownership.plane.renewals.load(Ordering::SeqCst) >= 1);
    *a.pipeline_for_test().on_stage.lock().unwrap() = None;
    assert_eq!(pending_items(&a, &queue).await, 3 + 6);
    a.shutdown().await.unwrap();
}

pub(super) async fn wait_for(what: &str, mut done: impl AsyncFnMut() -> bool) {
    for _ in 0..200 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_store_releases_its_lease() {
    let env = Env::new("dropped", false);
    let queue = env.address("q");
    let a = env.open("a.db").await;
    grow(&a, &queue, 4).await;
    let key = owner::queue_key(&queue).unwrap();
    assert_ne!(
        env.ownership.plane.lease(&key).await.unwrap().state,
        LeaseState::Unassigned
    );
    // No shutdown: the loop must neither keep the pipeline alive nor renew forever.
    drop(a);
    let plane = env.ownership.plane.clone();
    wait_for("the dropped store's lease", async || {
        plane.lease(&key).await.unwrap().state == LeaseState::Unassigned
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absent_queue_holds_nothing() {
    let env = Env::new("absent", false);
    let queue = env.address("missing");
    let a = env.open("a.db").await;
    assert_eq!(
        a.read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["none".into()],
            max_bytes: 1 << 20,
        })
        .await,
        Err(QueueError::QueueNotFound)
    );
    let pipeline = a.pipeline_for_test();
    let key = owner::queue_key(&queue).unwrap();
    let plane = env.ownership.plane.clone();
    wait_for("the absent queue's loop to end", async || {
        pipeline.queues.lock().unwrap().len() == 0
            && plane.lease(&key).await.unwrap().state == LeaseState::Unassigned
    })
    .await;
    // A later operation starts a fresh loop.
    grow(&a, &queue, 3).await;
    assert_eq!(pipeline.queues.lock().unwrap().len(), 1);
    drop(pipeline);
    a.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_entry_releases_its_lease() {
    let env = Env::new("failed-entry", false);
    let queue = env.address("q");
    let first = env.open("a.db").await;
    grow(&first, &queue, 4).await;
    env.ownership.advance(TTL + SECOND);
    // The next owner acquires, then fails before it serves anything.
    let failing = env.open("b.db").await;
    env.ownership
        .plane
        .fail_confirm
        .store(true, Ordering::SeqCst);
    assert_eq!(stats(&failing, &queue).await.err(), Some(QueueError::Busy));
    env.ownership
        .plane
        .fail_confirm
        .store(false, Ordering::SeqCst);
    // No wait for the TTL: the failed entry gave the lease back.
    let third = env.open("c.db").await;
    assert_eq!(pending_items(&third, &queue).await, 3);
    first.shutdown().await.unwrap();
    failing.shutdown().await.unwrap();
    third.shutdown().await.unwrap();
}
