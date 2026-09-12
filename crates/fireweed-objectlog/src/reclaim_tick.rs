//! Shared bounded lease-reclaim driver for native object-log products.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::time::Duration;

use fireweed_core::UtcTimestamp;
use fireweed_engine::{
    ControlPlane, EngineError, EngineResult, ExpiredLeaseCursor, ExpiredLeasePage,
    InProcessControlPlane, InProcessProjectionStore, ProjectionStore, QueueKey, ReclaimPort,
    TickReport,
};
use tokio::time::Instant;

/// Maximum expired projection rows inspected by one native object-log page.
///
/// The projection page may span up to this many queues. Durable projection families implement the
/// page as a storage-level keyset query; the memory family uses the bounded in-process fallback.
/// A subsequent tick scans the oldest rows still expired, so the bound guarantees finite work and
/// oldest-first progress instead of draining an arbitrarily large backlog in one call.
pub const EXPIRED_LEASE_SCAN_LIMIT: usize = 128;

/// Deduplicated callerless retry occupancy. One owner holds at most this many queue IDs.
pub const RECLAIM_RETRY_QUEUE_CAPACITY: usize = 1_024;

/// Closed injected saturated-key reclaim ceiling: 17 × 540 s + 1 s cadence + 1 s scheduling.
pub const RECLAIM_SATURATED_KEY_DEADLINE: Duration = Duration::from_secs(17 * 540 + 1 + 1);

/// 10/20/40/80/160/320/640 ms then a 1 s capped cadence.
pub const RECLAIM_RETRY_BACKOFF_MS: [u64; 8] = [10, 20, 40, 80, 160, 320, 640, 1_000];

const PER_KEY_CAPACITY_RESOURCE: &str = "keyed queue per-key waiters";
const GLOBAL_CAPACITY_RESOURCE: &str = "keyed queue waiters";

/// Gate wait, capacity rejection, and retry age stay on separate counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimRetryMetrics {
    pub per_key_capacity_rejections: u64,
    pub global_capacity_rejections: u64,
    pub gate_wait_ms: u64,
    pub gate_wait_attempts: u64,
    pub retry_age_ms_max: u64,
    pub retry_depth_max: u64,
    pub retry_queue_ceiling: u64,
    pub page_isolated_successes: u64,
    pub pages_fetched: u64,
    pub pages_skipped_while_full: u64,
}

/// Outcome of one owned reclaim pass, including retry-isolation metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimTickOutcome {
    pub report: TickReport,
    pub metrics: ReclaimRetryMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReclaimErrorClass {
    PerKeyCapacity,
    GlobalCapacity,
    RetryableContention,
    Stop,
}

struct RetryEntry {
    first_seen: Instant,
    attempts: u32,
    next_at: Instant,
}

struct ReclaimRetryQueue {
    order: VecDeque<QueueKey>,
    entries: HashMap<QueueKey, RetryEntry>,
}

impl ReclaimRetryQueue {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            entries: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= RECLAIM_RETRY_QUEUE_CAPACITY
    }

    fn contains(&self, shard: &QueueKey) -> bool {
        self.entries.contains_key(shard)
    }

    fn enqueue(&mut self, shard: QueueKey, now: Instant) -> bool {
        if self.entries.contains_key(&shard) {
            return true;
        }
        if self.entries.len() >= RECLAIM_RETRY_QUEUE_CAPACITY {
            return false;
        }
        self.entries.insert(
            shard.clone(),
            RetryEntry {
                first_seen: now,
                attempts: 0,
                next_at: now + backoff_delay(0),
            },
        );
        self.order.push_back(shard);
        true
    }

    fn take_due(&mut self, now: Instant) -> Vec<QueueKey> {
        let mut due = Vec::new();
        let queued = self.order.len();
        for _ in 0..queued {
            let Some(shard) = self.order.pop_front() else {
                break;
            };
            let ready = self
                .entries
                .get(&shard)
                .is_some_and(|entry| entry.next_at <= now);
            if ready {
                due.push(shard);
            } else {
                self.order.push_back(shard);
            }
        }
        due
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.entries.values().map(|entry| entry.next_at).min()
    }

    fn reschedule(&mut self, shard: QueueKey, now: Instant) {
        if let Some(entry) = self.entries.get_mut(&shard) {
            entry.attempts = entry.attempts.saturating_add(1);
            entry.next_at = now + backoff_delay(entry.attempts);
        }
        if !self.order.contains(&shard) {
            self.order.push_back(shard);
        }
    }

    fn remove(&mut self, shard: &QueueKey) -> Option<RetryEntry> {
        self.order.retain(|queued| queued != shard);
        self.entries.remove(shard)
    }
}

fn backoff_delay(attempts: u32) -> Duration {
    let index = (attempts as usize).min(RECLAIM_RETRY_BACKOFF_MS.len() - 1);
    Duration::from_millis(RECLAIM_RETRY_BACKOFF_MS[index])
}

fn classify_reclaim_error(error: &EngineError) -> ReclaimErrorClass {
    match error {
        EngineError::Backpressure { resource } => match *resource {
            PER_KEY_CAPACITY_RESOURCE => ReclaimErrorClass::PerKeyCapacity,
            GLOBAL_CAPACITY_RESOURCE => ReclaimErrorClass::GlobalCapacity,
            "mutation sequencer capacity" | "mutation sequencer wait" => {
                ReclaimErrorClass::RetryableContention
            }
            _ => ReclaimErrorClass::RetryableContention,
        },
        EngineError::EpochFenced | EngineError::DurableDataCorrupt { .. } => {
            ReclaimErrorClass::Stop
        }
        EngineError::Storage(message) => {
            if message.contains("Admission(PerKeyFull)")
                || message.contains(PER_KEY_CAPACITY_RESOURCE)
            {
                ReclaimErrorClass::PerKeyCapacity
            } else if message.contains("Admission(QueueFull)")
                || message.contains(GLOBAL_CAPACITY_RESOURCE)
            {
                ReclaimErrorClass::GlobalCapacity
            } else {
                ReclaimErrorClass::Stop
            }
        }
        _ => ReclaimErrorClass::Stop,
    }
}

fn note_capacity(metrics: &mut ReclaimRetryMetrics, class: ReclaimErrorClass) {
    match class {
        ReclaimErrorClass::PerKeyCapacity => {
            metrics.per_key_capacity_rejections =
                metrics.per_key_capacity_rejections.saturating_add(1);
        }
        ReclaimErrorClass::GlobalCapacity => {
            metrics.global_capacity_rejections =
                metrics.global_capacity_rejections.saturating_add(1);
        }
        ReclaimErrorClass::RetryableContention | ReclaimErrorClass::Stop => {}
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(
        Instant::now()
            .checked_duration_since(started)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn note_gate_wait(metrics: &mut ReclaimRetryMetrics, started: Instant) {
    metrics.gate_wait_attempts = metrics.gate_wait_attempts.saturating_add(1);
    metrics.gate_wait_ms = metrics.gate_wait_ms.saturating_add(elapsed_ms(started));
}

fn note_retry_age(metrics: &mut ReclaimRetryMetrics, first_seen: Instant) {
    metrics.retry_age_ms_max = metrics.retry_age_ms_max.max(elapsed_ms(first_seen));
}

fn note_depth(metrics: &mut ReclaimRetryMetrics, depth: usize) {
    metrics.retry_depth_max = metrics.retry_depth_max.max(depth as u64);
}

/// Reclaim one bounded projection page through the product's queue-scoped reclaim path.
///
/// Each queue operation is capped by both the rows observed in the global page and that queue's
/// `max_claim_batch_size`. `ReclaimPort` retains the product's existing per-queue permit, so reclaim
/// still serializes with claims. Only ids returned after a committed reclaim contribute to the report.
/// Retryable per-queue contention is owned here: one deduplicated round-robin queue, no per-queue task.
pub(crate) async fn tick_expired_leases<S, B>(
    projection: &InProcessProjectionStore<S>,
    control: &InProcessControlPlane,
    backend: &B,
    now: UtcTimestamp,
) -> EngineResult<TickReport>
where
    S: ProjectionStore + Send + 'static,
    B: ReclaimPort + Sync,
{
    let outcome = tick_owned_reclaim(
        backend,
        now,
        |cursor| async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::expired_leases_page(
                        projection,
                        now,
                        cursor.as_ref(),
                        EXPIRED_LEASE_SCAN_LIMIT,
                        None,
                    )
                })
                .await
        },
        |shard| {
            let definition = ControlPlane::queue_definition(control, shard)?;
            Ok(usize::try_from(definition.max_claim_batch_size).unwrap_or(usize::MAX))
        },
        1,
    )
    .await?;
    Ok(outcome.report)
}

/// Callerless reclaim retry owner. Fetches at most `max_new_pages` 128-row pages, holds at most
/// 1,024 queue IDs, and drains retries before another page when that occupancy is full.
pub async fn tick_owned_reclaim<B, F, Fut, L>(
    backend: &B,
    now: UtcTimestamp,
    mut fetch_page: F,
    mut queue_limit: L,
    max_new_pages: usize,
) -> EngineResult<ReclaimTickOutcome>
where
    B: ReclaimPort + Sync,
    F: FnMut(Option<ExpiredLeaseCursor>) -> Fut,
    Fut: Future<Output = EngineResult<ExpiredLeasePage>> + Send,
    L: FnMut(&QueueKey) -> EngineResult<usize>,
{
    let mut retries = ReclaimRetryQueue::new();
    let mut metrics = ReclaimRetryMetrics {
        retry_queue_ceiling: RECLAIM_RETRY_QUEUE_CAPACITY as u64,
        ..ReclaimRetryMetrics::default()
    };
    let mut leases_reclaimed = 0u64;
    let mut cursor = None;
    let mut pages_fetched = 0usize;
    let mut pages_exhausted = false;

    loop {
        drain_due_retries(
            backend,
            now,
            &mut retries,
            &mut queue_limit,
            &mut metrics,
            &mut leases_reclaimed,
        )
        .await?;

        if retries.is_empty() && (pages_exhausted || pages_fetched >= max_new_pages) {
            break;
        }

        if retries.is_full() {
            metrics.pages_skipped_while_full = metrics.pages_skipped_while_full.saturating_add(1);
            sleep_until_next_retry(&retries).await;
            continue;
        }

        if pages_exhausted || pages_fetched >= max_new_pages {
            sleep_until_next_retry(&retries).await;
            continue;
        }

        let page = fetch_page(cursor.clone()).await?;
        pages_fetched = pages_fetched.saturating_add(1);
        metrics.pages_fetched = pages_fetched as u64;
        cursor = page.next.clone();
        pages_exhausted = page.next.is_none();

        reclaim_page(
            backend,
            now,
            page,
            &mut retries,
            &mut queue_limit,
            &mut metrics,
            &mut leases_reclaimed,
        )
        .await?;
    }

    Ok(ReclaimTickOutcome {
        report: TickReport {
            leases_reclaimed,
            ..TickReport::default()
        },
        metrics,
    })
}

async fn reclaim_page<B, L>(
    backend: &B,
    now: UtcTimestamp,
    page: ExpiredLeasePage,
    retries: &mut ReclaimRetryQueue,
    queue_limit: &mut L,
    metrics: &mut ReclaimRetryMetrics,
    leases_reclaimed: &mut u64,
) -> EngineResult<()>
where
    B: ReclaimPort + Sync,
    L: FnMut(&QueueKey) -> EngineResult<usize>,
{
    let mut page_retry = false;
    let mut page_success = 0u64;
    for (shard, expired_ids) in page.leases {
        if retries.contains(&shard) {
            continue;
        }
        while retries.is_full() {
            metrics.pages_skipped_while_full = metrics.pages_skipped_while_full.saturating_add(1);
            drain_due_retries(
                backend,
                now,
                retries,
                queue_limit,
                metrics,
                leases_reclaimed,
            )
            .await?;
            if retries.is_full() {
                sleep_until_next_retry(retries).await;
            }
        }
        let limit = expired_ids.len().min(queue_limit(&shard)?);
        if limit == 0 {
            continue;
        }
        match attempt_reclaim(backend, &shard, Some(limit), now, metrics).await? {
            ReclaimAttempt::Reclaimed(count) => {
                *leases_reclaimed = leases_reclaimed.saturating_add(count);
                page_success = page_success.saturating_add(1);
            }
            ReclaimAttempt::Retry => {
                page_retry = true;
                let enqueued = retries.enqueue(shard, Instant::now());
                debug_assert!(enqueued, "retry occupancy was reserved before enqueue");
                note_depth(metrics, retries.len());
            }
        }
    }
    if page_retry {
        metrics.page_isolated_successes =
            metrics.page_isolated_successes.saturating_add(page_success);
    }
    Ok(())
}

enum ReclaimAttempt {
    Reclaimed(u64),
    Retry,
}

async fn attempt_reclaim<B>(
    backend: &B,
    shard: &QueueKey,
    limit: Option<usize>,
    now: UtcTimestamp,
    metrics: &mut ReclaimRetryMetrics,
) -> EngineResult<ReclaimAttempt>
where
    B: ReclaimPort + Sync,
{
    let started = Instant::now();
    match ReclaimPort::reclaim_expired(backend, shard, limit, now, None).await {
        Ok(reclaimed) => {
            note_gate_wait(metrics, started);
            Ok(ReclaimAttempt::Reclaimed(reclaimed.len() as u64))
        }
        Err(error) => match classify_reclaim_error(&error) {
            class @ (ReclaimErrorClass::PerKeyCapacity | ReclaimErrorClass::GlobalCapacity) => {
                note_capacity(metrics, class);
                Ok(ReclaimAttempt::Retry)
            }
            ReclaimErrorClass::RetryableContention => Ok(ReclaimAttempt::Retry),
            ReclaimErrorClass::Stop => Err(error),
        },
    }
}

async fn drain_due_retries<B, L>(
    backend: &B,
    now: UtcTimestamp,
    retries: &mut ReclaimRetryQueue,
    queue_limit: &mut L,
    metrics: &mut ReclaimRetryMetrics,
    leases_reclaimed: &mut u64,
) -> EngineResult<()>
where
    B: ReclaimPort + Sync,
    L: FnMut(&QueueKey) -> EngineResult<usize>,
{
    loop {
        let due = retries.take_due(Instant::now());
        if due.is_empty() {
            return Ok(());
        }
        for shard in due {
            let limit = queue_limit(&shard)?;
            if limit == 0 {
                if let Some(entry) = retries.remove(&shard) {
                    note_retry_age(metrics, entry.first_seen);
                }
                continue;
            }
            match attempt_reclaim(backend, &shard, Some(limit), now, metrics).await? {
                ReclaimAttempt::Reclaimed(count) => {
                    *leases_reclaimed = leases_reclaimed.saturating_add(count);
                    if let Some(entry) = retries.remove(&shard) {
                        note_retry_age(metrics, entry.first_seen);
                    }
                }
                ReclaimAttempt::Retry => {
                    retries.reschedule(shard, Instant::now());
                    note_depth(metrics, retries.len());
                }
            }
        }
    }
}

async fn sleep_until_next_retry(retries: &ReclaimRetryQueue) {
    let Some(deadline) = retries.next_deadline() else {
        return;
    };
    if let Some(wait) = deadline.checked_duration_since(Instant::now()) {
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use fireweed_core::{ItemId, QueueId, TenantId, UtcTimestamp};
    use fireweed_engine::{EngineError, EngineResult, ExpiredLeasePage, QueueKey, ReclaimPort};

    use super::{
        RECLAIM_RETRY_BACKOFF_MS, RECLAIM_RETRY_QUEUE_CAPACITY, RECLAIM_SATURATED_KEY_DEADLINE,
        tick_owned_reclaim,
    };

    fn shard(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn now() -> UtcTimestamp {
        UtcTimestamp::new(101, 0).unwrap()
    }

    struct ScriptedReclaim {
        isolated: QueueKey,
        saturated: QueueKey,
        isolated_ids: Vec<ItemId>,
        saturated_ids: Vec<ItemId>,
        remaining_saturated_failures: AtomicU32,
        isolated_completed_at: Mutex<Option<tokio::time::Instant>>,
        saturated_completed_at: Mutex<Option<tokio::time::Instant>>,
        saturated_attempts: Mutex<Vec<tokio::time::Instant>>,
        gate_wait_on_success: bool,
    }

    impl ReclaimPort for ScriptedReclaim {
        fn reclaim_expired(
            &self,
            shard: &QueueKey,
            _limit: Option<usize>,
            _now: UtcTimestamp,
            _expected_epoch: Option<u64>,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
            let shard = shard.clone();
            async move {
                if shard == self.isolated {
                    let mut done = self.isolated_completed_at.lock().expect("isolated time");
                    if done.is_none() {
                        *done = Some(tokio::time::Instant::now());
                    }
                    return Ok(self.isolated_ids.clone());
                }
                if shard == self.saturated {
                    self.saturated_attempts
                        .lock()
                        .expect("saturated attempts")
                        .push(tokio::time::Instant::now());
                    let remaining = self.remaining_saturated_failures.load(Ordering::SeqCst);
                    if remaining > 0 {
                        self.remaining_saturated_failures
                            .store(remaining - 1, Ordering::SeqCst);
                        return Err(EngineError::Backpressure {
                            resource: "keyed queue per-key waiters",
                        });
                    }
                    if self.gate_wait_on_success {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    let mut done = self.saturated_completed_at.lock().expect("saturated time");
                    if done.is_none() {
                        *done = Some(tokio::time::Instant::now());
                    }
                    return Ok(self.saturated_ids.clone());
                }
                Ok(Vec::new())
            }
        }
    }

    #[tokio::test]
    async fn reclaim_backpressure_isolated_and_retried_per_queue() {
        let isolated = shard("isolated");
        let saturated = shard("saturated");
        let isolated_id = ItemId::from_u64(1);
        let saturated_ids = vec![ItemId::from_u64(2), ItemId::from_u64(3)];
        let backend = ScriptedReclaim {
            isolated: isolated.clone(),
            saturated: saturated.clone(),
            isolated_ids: vec![isolated_id],
            saturated_ids: saturated_ids.clone(),
            remaining_saturated_failures: AtomicU32::new(3),
            isolated_completed_at: Mutex::new(None),
            saturated_completed_at: Mutex::new(None),
            saturated_attempts: Mutex::new(Vec::new()),
            gate_wait_on_success: true,
        };
        let started = tokio::time::Instant::now();
        let page = ExpiredLeasePage {
            leases: vec![
                (saturated.clone(), saturated_ids.clone()),
                (isolated.clone(), vec![isolated_id]),
            ],
            next: None,
        };
        let outcome = tick_owned_reclaim(
            &backend,
            now(),
            {
                let mut pages = Some(page);
                move |_| std::future::ready(Ok(pages.take().unwrap_or_default()))
            },
            |_| Ok(100),
            1,
        )
        .await
        .expect("owned reclaim must isolate retryable contention");

        let elapsed = started.elapsed();
        assert!(
            elapsed <= RECLAIM_SATURATED_KEY_DEADLINE,
            "closed injected saturated-key reclaim exceeded 17×540 s + 1 s cadence + 1 s scheduling ({elapsed:?})"
        );
        assert_eq!(outcome.report.leases_reclaimed, 3);
        let isolated_at = backend
            .isolated_completed_at
            .lock()
            .expect("isolated time")
            .expect("isolated queue must reclaim immediately");
        let saturated_at = backend
            .saturated_completed_at
            .lock()
            .expect("saturated time")
            .expect("saturated queue must eventually reclaim");
        assert!(
            isolated_at < saturated_at,
            "another queue in the same page must reclaim immediately"
        );
        assert_eq!(
            backend.saturated_ids, saturated_ids,
            "the saturated queue must return every expired lease"
        );

        let attempts = backend.saturated_attempts.lock().expect("attempts");
        assert!(
            attempts.len() >= 4,
            "three capacity rejections then a retry"
        );
        let first_gap = attempts[1].saturating_duration_since(attempts[0]);
        assert!(
            first_gap >= std::time::Duration::from_millis(RECLAIM_RETRY_BACKOFF_MS[0]),
            "first retry must wait the 10 ms cadence, got {first_gap:?}"
        );

        let metrics = &outcome.metrics;
        assert_eq!(
            metrics.retry_queue_ceiling,
            RECLAIM_RETRY_QUEUE_CAPACITY as u64
        );
        assert!(metrics.retry_depth_max >= 1);
        assert!(metrics.retry_depth_max <= metrics.retry_queue_ceiling);
        assert_eq!(metrics.page_isolated_successes, 1);
        assert!(metrics.per_key_capacity_rejections >= 3);
        assert_eq!(metrics.global_capacity_rejections, 0);
        assert!(metrics.gate_wait_ms >= 5);
        assert!(metrics.gate_wait_attempts >= 2);
        assert!(metrics.retry_age_ms_max >= RECLAIM_RETRY_BACKOFF_MS[0]);
        assert_ne!(
            metrics.per_key_capacity_rejections, metrics.gate_wait_ms,
            "capacity rejection and gate wait must stay distinct"
        );
        assert_ne!(
            metrics.gate_wait_ms, metrics.retry_age_ms_max,
            "gate wait and retry age must stay distinct"
        );
        assert!(
            metrics.retry_age_ms_max >= metrics.gate_wait_ms,
            "retry age covers cadence plus the successful gate wait"
        );

        let production = include_str!("reclaim_tick.rs")
            .rsplit_once("#[cfg(test)]")
            .expect("production / test split")
            .0;
        assert!(
            !production.contains("tokio::spawn")
                && !production.contains("task::spawn")
                && !production.contains("std::thread::spawn"),
            "callerless reclaim must spawn no per-queue task"
        );
    }

    struct CountingPages {
        pages: Vec<ExpiredLeasePage>,
        fetches: AtomicU32,
    }

    struct FailingReclaim {
        remaining: Mutex<HashMap<QueueKey, u32>>,
        successes: Mutex<Vec<QueueKey>>,
    }

    impl ReclaimPort for FailingReclaim {
        fn reclaim_expired(
            &self,
            shard: &QueueKey,
            _limit: Option<usize>,
            _now: UtcTimestamp,
            _expected_epoch: Option<u64>,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
            let shard = shard.clone();
            async move {
                let mut remaining = self.remaining.lock().expect("remaining");
                let left = remaining.get(&shard).copied().unwrap_or(0);
                if left > 0 {
                    remaining.insert(shard, left - 1);
                    return Err(EngineError::Backpressure {
                        resource: "keyed queue per-key waiters",
                    });
                }
                self.successes.lock().expect("successes").push(shard);
                Ok(vec![ItemId::from_u64(1)])
            }
        }
    }

    // Fill the queue before its first retry deadline regardless of debug-build
    // speed or host load. Tokio advances time when the full queue awaits a retry.
    #[tokio::test(start_paused = true)]
    async fn reclaim_retry_queue_drains_before_next_page_when_full() {
        let mut pages = Vec::new();
        let mut remaining = HashMap::new();
        for page_index in 0..9u32 {
            let mut leases = Vec::new();
            for offset in 0..128u32 {
                let id = page_index * 128 + offset + 1;
                let key = shard(&format!("q-{id}"));
                remaining.insert(key.clone(), if page_index < 8 { 1 } else { 0 });
                leases.push((key, vec![ItemId::from_u64(id as u64)]));
            }
            pages.push(ExpiredLeasePage {
                leases,
                next: (page_index < 8).then(|| {
                    fireweed_engine::ExpiredLeaseCursor::from_row(
                        0,
                        &shard("cursor"),
                        &ItemId::from_u64(1),
                    )
                }),
            });
        }
        let backend = FailingReclaim {
            remaining: Mutex::new(remaining),
            successes: Mutex::new(Vec::new()),
        };
        let source = CountingPages {
            pages,
            fetches: AtomicU32::new(0),
        };
        let outcome = tick_owned_reclaim(
            &backend,
            now(),
            {
                let fetches = &source.fetches;
                let pages = &source.pages;
                move |_| {
                    let index = fetches.fetch_add(1, Ordering::SeqCst) as usize;
                    std::future::ready(Ok(pages.get(index).cloned().unwrap_or_default()))
                }
            },
            |_| Ok(8),
            usize::MAX,
        )
        .await
        .unwrap();

        assert!(
            outcome.metrics.pages_fetched <= 9,
            "full retry occupancy must drain before pulling another page"
        );
        assert!(outcome.metrics.pages_skipped_while_full >= 1);
        assert!(outcome.metrics.retry_depth_max <= RECLAIM_RETRY_QUEUE_CAPACITY as u64);
        assert_eq!(
            outcome.metrics.retry_queue_ceiling,
            RECLAIM_RETRY_QUEUE_CAPACITY as u64
        );
        assert_eq!(
            backend.successes.lock().expect("successes").len() as u64,
            9 * 128
        );
        assert_eq!(outcome.report.leases_reclaimed, 9 * 128);
    }

    struct StoppingReclaim;

    impl ReclaimPort for StoppingReclaim {
        fn reclaim_expired(
            &self,
            shard: &QueueKey,
            _limit: Option<usize>,
            _now: UtcTimestamp,
            _expected_epoch: Option<u64>,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
            let shard = shard.clone();
            async move {
                if shard.queue_id.as_str() == "poison" {
                    return Err(EngineError::Storage("async projection poisoned".into()));
                }
                if shard.queue_id.as_str() == "epoch" {
                    return Err(EngineError::EpochFenced);
                }
                Ok(vec![ItemId::from_u64(1)])
            }
        }
    }

    #[tokio::test]
    async fn reclaim_stops_on_epoch_change_and_poison() {
        let epoch_page = ExpiredLeasePage {
            leases: vec![
                (shard("ok"), vec![ItemId::from_u64(1)]),
                (shard("epoch"), vec![ItemId::from_u64(2)]),
            ],
            next: None,
        };
        let epoch = tick_owned_reclaim(
            &StoppingReclaim,
            now(),
            {
                let mut page = Some(epoch_page);
                move |_| std::future::ready(Ok(page.take().unwrap_or_default()))
            },
            |_| Ok(8),
            1,
        )
        .await;
        assert!(matches!(epoch, Err(EngineError::EpochFenced)));

        let poison_page = ExpiredLeasePage {
            leases: vec![(shard("poison"), vec![ItemId::from_u64(3)])],
            next: None,
        };
        let poison = tick_owned_reclaim(
            &StoppingReclaim,
            now(),
            {
                let mut page = Some(poison_page);
                move |_| std::future::ready(Ok(page.take().unwrap_or_default()))
            },
            |_| Ok(8),
            1,
        )
        .await;
        assert!(matches!(poison, Err(EngineError::Storage(message)) if message.contains("poison")));
    }
}
