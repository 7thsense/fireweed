use fireweed_engine::{CommandPosition, EngineError, EngineResult};

// ---------------------------------------------------------------------------
// Async apply debt, backpressure, and poison (bead pqueue-6da52695,
// backend:objectlog-hybrid-async). TD-004 §"Async apply debt, backpressure, and poison thresholds".
// ---------------------------------------------------------------------------

/// Configured per-`(tenant, queue)` HARD bounds on `objectlog/hybrid-async` async SQLite apply debt
/// (TD-004 §"Async apply debt, backpressure, and poison thresholds"). Each debt field below is the HARD
/// limit; the normative hysteresis derives the SOFT (warn / prefer-flush) band at 75% and the CLEAR
/// (release backpressure) band at 50% of it. A zero bound is rejected — it would make the queue instantly
/// and permanently backpressured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HybridAsyncThresholds {
    /// `hybrid_async_sqlite_apply_lag_max_commands`: committed command sequences allowed to trail
    /// `sqlite_high_water`.
    pub apply_lag_max_commands: u64,
    /// Hard cap on `hybrid_async_apply_debt_bytes`: retained object-log bytes not yet trimmable.
    pub apply_debt_max_bytes: u64,
    /// Hard cap on `hybrid_async_apply_queue_depth`: sealed segment batches awaiting async apply.
    pub apply_queue_depth_max: u64,
    /// Hard cap on `hybrid_async_oldest_unapplied_age_ms`: age of the oldest unapplied committed command.
    pub oldest_unapplied_max_ms: u64,
    /// `hybrid_async_apply_retry_count` value that trips poison for the same batch (fail-closed).
    pub apply_poison_retry_threshold: u32,
}

impl Default for HybridAsyncThresholds {
    /// Conservative library defaults; the server's env populator overrides them per deployment.
    fn default() -> Self {
        Self {
            apply_lag_max_commands: 100_000,
            apply_debt_max_bytes: 512 * 1024 * 1024,
            apply_queue_depth_max: 1_024,
            oldest_unapplied_max_ms: 60_000,
            apply_poison_retry_threshold: 3,
        }
    }
}

impl HybridAsyncThresholds {
    /// Build validated thresholds. Every debt bound and the poison retry threshold MUST be `> 0`.
    pub fn new(
        apply_lag_max_commands: u64,
        apply_debt_max_bytes: u64,
        apply_queue_depth_max: u64,
        oldest_unapplied_max_ms: u64,
        apply_poison_retry_threshold: u32,
    ) -> EngineResult<Self> {
        let zero = |name: &str| {
            EngineError::Storage(format!(
                "hybrid-async threshold {name} must be > 0 (a zero bound is instantly backpressured)"
            ))
        };
        if apply_lag_max_commands == 0 {
            return Err(zero("apply_lag_max_commands"));
        }
        if apply_debt_max_bytes == 0 {
            return Err(zero("apply_debt_max_bytes"));
        }
        if apply_queue_depth_max == 0 {
            return Err(zero("apply_queue_depth_max"));
        }
        if oldest_unapplied_max_ms == 0 {
            return Err(zero("oldest_unapplied_max_ms"));
        }
        if apply_poison_retry_threshold == 0 {
            return Err(zero("apply_poison_retry_threshold"));
        }
        Ok(Self {
            apply_lag_max_commands,
            apply_debt_max_bytes,
            apply_queue_depth_max,
            oldest_unapplied_max_ms,
            apply_poison_retry_threshold,
        })
    }

    /// Classify a debt sample against these bounds WITHOUT hysteresis: `Hard` if any metric is at/over its
    /// hard limit, else `Soft` if any is at/over 75%, else `Clear`. The monitor layers hysteresis on top so
    /// a queue that tripped `Hard` only releases once ALL metrics fall below 50% (see [`HybridAsyncMonitor`]).
    fn classify(&self, debt: &HybridAsyncDebt) -> BackpressureLevel {
        let over = |v: u64, max: u64, num: u64| v.saturating_mul(100) >= max.saturating_mul(num);
        let any_hard = debt.apply_lag_commands >= self.apply_lag_max_commands
            || debt.apply_debt_bytes >= self.apply_debt_max_bytes
            || debt.apply_queue_depth >= self.apply_queue_depth_max
            || debt.oldest_unapplied_age_ms >= self.oldest_unapplied_max_ms;
        if any_hard {
            return BackpressureLevel::Hard;
        }
        let any_soft = over(debt.apply_lag_commands, self.apply_lag_max_commands, 75)
            || over(debt.apply_debt_bytes, self.apply_debt_max_bytes, 75)
            || over(debt.apply_queue_depth, self.apply_queue_depth_max, 75)
            || over(
                debt.oldest_unapplied_age_ms,
                self.oldest_unapplied_max_ms,
                75,
            );
        if any_soft {
            BackpressureLevel::Soft
        } else {
            BackpressureLevel::Clear
        }
    }

    /// Whether every metric in `debt` is strictly below its 50% CLEAR band — the necessary condition to
    /// release hard backpressure (the sufficient condition also requires a clean applied batch).
    fn all_below_clear(&self, debt: &HybridAsyncDebt) -> bool {
        let below = |v: u64, max: u64| v.saturating_mul(100) < max.saturating_mul(50);
        below(debt.apply_lag_commands, self.apply_lag_max_commands)
            && below(debt.apply_debt_bytes, self.apply_debt_max_bytes)
            && below(debt.apply_queue_depth, self.apply_queue_depth_max)
            && below(debt.oldest_unapplied_age_ms, self.oldest_unapplied_max_ms)
    }
}

/// A sampled snapshot of current async apply debt for one queue (TD-004 metric definitions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HybridAsyncDebt {
    /// `hybrid_async_sqlite_apply_lag`: committed command sequences not yet covered by `sqlite_high_water`.
    pub apply_lag_commands: u64,
    /// `hybrid_async_apply_debt_bytes`: retained object-log bytes not yet trimmable via async apply.
    pub apply_debt_bytes: u64,
    /// `hybrid_async_apply_queue_depth`: sealed segment batches waiting for async SQLite apply.
    pub apply_queue_depth: u64,
    /// `hybrid_async_oldest_unapplied_age_ms`: age of the oldest committed-but-unapplied command.
    pub oldest_unapplied_age_ms: u64,
}

/// Typed backpressure level for a queue's async apply pipeline (TD-004 backpressure table). `Soft` warns
/// and prefers flush/apply over accepting more backlog; `Hard` rejects new mutating operations with a
/// retryable error until debt clears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    /// Below the soft band; mutations flow normally.
    Clear,
    /// At/over 75% of a bound; emit warning telemetry and prefer flushing apply work.
    Soft,
    /// At/over a hard bound; new mutating operations MUST be rejected until debt clears.
    Hard,
}

/// A point-in-time export of the async-apply debt/backpressure/poison observability surface for one queue
/// (TD-004 §"Async apply debt": the metrics a release ledger records).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridAsyncMetrics {
    pub apply_lag_commands: u64,
    pub apply_debt_bytes: u64,
    pub apply_queue_depth: u64,
    pub oldest_unapplied_age_ms: u64,
    /// `hybrid_async_apply_retry_count`: consecutive failed SQLite apply attempts for the current batch.
    pub apply_retry_count: u32,
    /// Cumulative checkpoint/apply errors observed (a monotonic counter, distinct from the consecutive one).
    pub checkpoint_errors: u64,
    /// Latest observed SQLite WAL size in bytes (0 when unavailable / non-WAL).
    pub wal_size_bytes: u64,
    pub backpressure_level: BackpressureLevel,
    /// Number of times the queue entered `Hard` backpressure.
    pub backpressure_events: u64,
    /// Cumulative wall-clock milliseconds spent in `Hard` backpressure.
    pub backpressure_ms_total: u64,
    /// Whether the local projection is poisoned (fail-closed until repair/restart).
    pub poisoned: bool,
}

/// Runtime controller for one queue's `objectlog/hybrid-async` async SQLite apply pipeline: it turns raw
/// debt samples into a typed backpressure level (with the normative 75/50 hysteresis), tracks
/// backpressure count/duration, checkpoint error and consecutive-apply-retry counts, the latest WAL size,
/// and the poison latch — and gates mutations and recovery-high-water advertisement accordingly.
///
/// This is the enforcement point for the two normative invariants this bead adds:
/// - **Backpressure before SLO violation**: [`admit_mutation`](Self::admit_mutation) fails new mutating
///   operations closed once debt is `Hard`, before async debt can invalidate recovery/retention.
/// - **Fail-closed on poison**: a batch that retries past `apply_poison_retry_threshold`, or any
///   non-contiguous/divergent apply, poisons the queue so it stops serving and its lagging
///   `sqlite_high_water` is never advertised as a safe replay-skip point (high-water cannot advance past
///   poison).
pub struct HybridAsyncMonitor {
    thresholds: HybridAsyncThresholds,
    level: BackpressureLevel,
    last_debt: HybridAsyncDebt,
    consecutive_retries: u32,
    checkpoint_errors: u64,
    /// Set once an ordered batch applies cleanly; a precondition for releasing hard backpressure.
    clean_batch_since_hard: bool,
    backpressure_events: u64,
    backpressure_ms_total: u64,
    hard_entered_at_ms: Option<i64>,
    wal_size_bytes: u64,
    poisoned: Option<String>,
}

impl HybridAsyncMonitor {
    pub fn new(thresholds: HybridAsyncThresholds) -> Self {
        Self {
            thresholds,
            level: BackpressureLevel::Clear,
            last_debt: HybridAsyncDebt::default(),
            consecutive_retries: 0,
            checkpoint_errors: 0,
            clean_batch_since_hard: true,
            backpressure_events: 0,
            backpressure_ms_total: 0,
            hard_entered_at_ms: None,
            wal_size_bytes: 0,
            poisoned: None,
        }
    }

    pub fn thresholds(&self) -> HybridAsyncThresholds {
        self.thresholds
    }

    pub fn level(&self) -> BackpressureLevel {
        self.level
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    pub fn poison_reason(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    /// Fold a fresh debt sample taken at `now_ms` into the backpressure state machine and return the
    /// resulting level. Hysteresis: a queue already in `Hard` only leaves once ALL metrics fall below their
    /// 50% CLEAR band AND at least one ordered batch has applied cleanly since the queue tripped `Hard`
    /// (TD-004 "Clear threshold"). Entering `Hard` bumps the backpressure event counter and starts the
    /// duration clock; leaving `Hard` accumulates the elapsed duration.
    pub fn observe(&mut self, debt: HybridAsyncDebt, now_ms: i64) -> BackpressureLevel {
        self.last_debt = debt;
        let raw = self.thresholds.classify(&debt);
        let next = match self.level {
            BackpressureLevel::Hard => {
                let releasable =
                    self.thresholds.all_below_clear(&debt) && self.clean_batch_since_hard;
                if releasable {
                    raw
                } else {
                    BackpressureLevel::Hard
                }
            }
            _ => raw,
        };
        // Track entry into / exit from Hard for count + duration.
        match (self.level, next) {
            (BackpressureLevel::Hard, BackpressureLevel::Hard) => {}
            (_, BackpressureLevel::Hard) => {
                self.backpressure_events += 1;
                self.hard_entered_at_ms = Some(now_ms);
                self.clean_batch_since_hard = false;
            }
            (BackpressureLevel::Hard, _) => {
                if let Some(entered) = self.hard_entered_at_ms.take() {
                    self.backpressure_ms_total += now_ms.saturating_sub(entered).max(0) as u64;
                }
            }
            _ => {}
        }
        self.level = next;
        next
    }

    /// Record that an ordered SQLite apply batch completed cleanly: reset the consecutive-retry counter and
    /// mark the clean-batch precondition for releasing hard backpressure.
    pub fn record_apply_success(&mut self) {
        self.consecutive_retries = 0;
        self.clean_batch_since_hard = true;
    }

    /// Clear worker poison/debt bookkeeping after an operator-driven rebuild has proven a complete,
    /// contiguous SQLite image from authoritative history.
    pub(crate) fn clear_after_rebuild(&mut self) {
        self.level = BackpressureLevel::Clear;
        self.poisoned = None;
        self.consecutive_retries = 0;
        self.clean_batch_since_hard = true;
        self.hard_entered_at_ms = None;
        self.last_debt = HybridAsyncDebt::default();
    }

    /// Record a failed async SQLite apply/checkpoint attempt for the current batch. Increments both the
    /// cumulative error counter and the consecutive-retry counter; when consecutive retries reach the
    /// configured poison threshold the queue is poisoned (fail-closed). Returns `true` iff this call
    /// poisoned the queue.
    pub fn record_checkpoint_error(&mut self, reason: impl Into<String>) -> bool {
        self.checkpoint_errors += 1;
        self.consecutive_retries += 1;
        self.clean_batch_since_hard = false;
        if self.poisoned.is_none()
            && self.consecutive_retries >= self.thresholds.apply_poison_retry_threshold
        {
            self.poisoned = Some(format!(
                "async SQLite apply failed {} consecutive times (>= poison threshold {}): {}",
                self.consecutive_retries,
                self.thresholds.apply_poison_retry_threshold,
                reason.into()
            ));
            return true;
        }
        false
    }

    /// Poison the queue immediately for an unrepairable condition (non-contiguous apply, checksum/lineage
    /// divergence, an attempt to advance `sqlite_high_water` past an unapplied sequence). Idempotent — the
    /// first reason wins.
    pub fn poison(&mut self, reason: impl Into<String>) {
        if self.poisoned.is_none() {
            self.poisoned = Some(reason.into());
        }
    }

    /// Latest observed WAL size gauge (bytes).
    pub fn set_wal_size_bytes(&mut self, bytes: u64) {
        self.wal_size_bytes = bytes;
    }

    pub fn consecutive_retries(&self) -> u32 {
        self.consecutive_retries
    }

    /// The mutation-admission gate (TD-004 "Hard debt threshold"). A poisoned queue fails closed with a
    /// storage error; a queue in `Hard` backpressure fails new mutating operations with a retryable
    /// `Unavailable` error until ordered apply reduces debt below the clear band; otherwise mutations are
    /// admitted.
    pub fn admit_mutation(&self) -> EngineResult<()> {
        if let Some(reason) = &self.poisoned {
            return Err(EngineError::Storage(format!(
                "hybrid-async projection poisoned: {reason}"
            )));
        }
        if self.level == BackpressureLevel::Hard {
            return Err(EngineError::Unavailable);
        }
        Ok(())
    }

    /// The recovery/high-water backpressure rule (TD-004 "Recovery/high-water backpressure"): while the
    /// queue is poisoned OR in `Hard` backpressure, its lagging `sqlite_high_water` MUST NOT be advertised
    /// as a safe replay-skip point, so this returns `None` (forcing replay from an earlier durable
    /// source). Otherwise the supplied high-water is trusted.
    pub fn recovery_high_water_safe(
        &self,
        high_water: Option<CommandPosition>,
    ) -> Option<CommandPosition> {
        if self.poisoned.is_some() || self.level == BackpressureLevel::Hard {
            None
        } else {
            high_water
        }
    }

    /// Whether segment expiry / retention advancement is currently allowed (TD-004 "Retention
    /// backpressure"): never while debt is over budget, the worker is poisoned, or lineage is unproven.
    pub fn retention_may_advance(&self) -> bool {
        self.poisoned.is_none() && self.level == BackpressureLevel::Clear
    }

    /// Export the full observability surface for a release ledger / telemetry sink.
    pub fn metrics(&self) -> HybridAsyncMetrics {
        HybridAsyncMetrics {
            apply_lag_commands: self.last_debt.apply_lag_commands,
            apply_debt_bytes: self.last_debt.apply_debt_bytes,
            apply_queue_depth: self.last_debt.apply_queue_depth,
            oldest_unapplied_age_ms: self.last_debt.oldest_unapplied_age_ms,
            apply_retry_count: self.consecutive_retries,
            checkpoint_errors: self.checkpoint_errors,
            wal_size_bytes: self.wal_size_bytes,
            backpressure_level: self.level,
            backpressure_events: self.backpressure_events,
            backpressure_ms_total: self.backpressure_ms_total,
            poisoned: self.poisoned.is_some(),
        }
    }
}
