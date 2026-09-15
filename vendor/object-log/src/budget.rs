//! Durable-ops budget controller (TD-004).

use std::time::{Duration, Instant};

/// How the engine resolves budget pressure vs admitted latency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BudgetMode {
    /// Deadline flushes always run (may overdraft). Default.
    #[default]
    LatencyPriority,
    /// New produces may wait for tokens; admitted deadline flushes still complete.
    BudgetPriority,
    /// Refuse admission when predicted media ops cannot be reserved.
    FailClosed,
}

/// Configuration for the media-ops budget controller.
#[derive(Clone, Copy, Debug)]
pub struct BudgetConfig {
    /// Master switch. Default **true** (Fjord-first).
    pub enabled: bool,
    /// How deadline flushes interact with an empty token bucket.
    pub mode: BudgetMode,
    /// Hard cap on durable ops/sec (None = no hard cap beyond capacity×fraction).
    pub budget_per_sec_cap: Option<f64>,
    /// Soft floor on effective budget when set (must be ≤ cap).
    pub budget_per_sec_floor: Option<f64>,
    /// Fraction of capacity used as soft budget target.
    pub budget_fraction: f64,
    /// Capacity when no probe/EWMA is available (S3 default path).
    pub default_capacity_per_sec: f64,
    /// Token fill ratio above which early-flush (effective linger = 0) is allowed.
    pub early_flush_fill_ratio: f64,
    /// Suppress consecutive early-flushes for this long after one fires.
    pub early_flush_cooldown: Duration,
    /// Early-flush only when the mutable queue holds at most this many bytes.
    /// Combined with [`Self::early_flush_idle`]: bulk continuous ingest should
    /// not early-flush. `0` disables the byte gate (idle gate may still apply).
    pub early_flush_max_queued_bytes: usize,
    /// Early-flush only if no produce has been enqueued for at least this long
    /// (true idle). Sustained produce keeps resetting the timer so bulk waits
    /// full linger / `flush`. `ZERO` disables the idle gate.
    pub early_flush_idle: Duration,
    /// Max wait for tokens under [`BudgetMode::BudgetPriority`].
    pub admission_timeout: Duration,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: BudgetMode::LatencyPriority,
            budget_per_sec_cap: None,
            budget_per_sec_floor: None,
            budget_fraction: 0.5,
            // Conservative S3-class default; Local probe/EWMA will refine.
            default_capacity_per_sec: 50.0,
            early_flush_fill_ratio: 0.5,
            early_flush_cooldown: Duration::from_millis(5),
            // ~4 MiB: single-op / sparse OK when also idle.
            early_flush_max_queued_bytes: 4 * 1024 * 1024,
            // Require a quiet gap so continuous bulk cannot be sliced into 4MiB seals.
            early_flush_idle: Duration::from_millis(10),
            admission_timeout: Duration::from_millis(100),
        }
    }
}

impl BudgetConfig {
    /// Validate floor ≤ cap when both set.
    pub fn validate(&self) -> Result<(), String> {
        if let (Some(cap), Some(floor)) = (self.budget_per_sec_cap, self.budget_per_sec_floor)
            && floor > cap
        {
            return Err(format!(
                "budget_per_sec_floor ({floor}) > budget_per_sec_cap ({cap})"
            ));
        }
        if !(0.0..=1.0).contains(&self.budget_fraction) {
            return Err("budget_fraction must be in [0, 1]".into());
        }
        if self.default_capacity_per_sec < 0.0 {
            return Err("default_capacity_per_sec must be >= 0".into());
        }
        Ok(())
    }

    /// Compute effective durable_ops/sec from optional measurements.
    pub fn effective_budget_per_sec(
        &self,
        startup_capacity: Option<f64>,
        ongoing_capacity: Option<f64>,
    ) -> f64 {
        let capacity = ongoing_capacity
            .or(startup_capacity)
            .unwrap_or(self.default_capacity_per_sec)
            .max(0.0);
        let mut effective = capacity * self.budget_fraction;
        if let Some(cap) = self.budget_per_sec_cap {
            effective = effective.min(cap);
        }
        if let Some(floor) = self.budget_per_sec_floor {
            effective = effective.max(floor);
            if let Some(cap) = self.budget_per_sec_cap {
                effective = effective.min(cap);
            }
        }
        effective
    }
}

/// Runtime token-bucket + EWMA state (held under the engine queue lock or own mutex).
#[derive(Debug)]
pub struct BudgetRuntime {
    pub config: BudgetConfig,
    pub tokens: f64,
    pub effective_budget_per_sec: f64,
    pub last_refill: Instant,
    pub predicted_media_ops: f64,
    pub last_early_flush: Option<Instant>,
    pub media_ops_total: u64,
    pub overdraft_total: u64,
    pub flushes_total: u64,
    pub undersized_deadline_flushes: u64,
    pub startup_capacity: Option<f64>,
    pub ongoing_capacity: Option<f64>,
    /// EWMA of observed media ops per flush.
    media_ops_ewma: f64,
    ewma_inited: bool,
}

impl BudgetRuntime {
    pub fn new(config: BudgetConfig) -> Self {
        let effective = config.effective_budget_per_sec(None, None);
        Self {
            config,
            tokens: effective.max(1.0),
            effective_budget_per_sec: effective,
            last_refill: Instant::now(),
            predicted_media_ops: 1.0,
            last_early_flush: None,
            media_ops_total: 0,
            overdraft_total: 0,
            flushes_total: 0,
            undersized_deadline_flushes: 0,
            startup_capacity: None,
            ongoing_capacity: None,
            media_ops_ewma: 1.0,
            ewma_inited: false,
        }
    }

    pub fn refill(&mut self, now: Instant) {
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        if dt <= 0.0 {
            return;
        }
        let burst = self.effective_budget_per_sec.max(1.0);
        self.tokens = (self.tokens + dt * self.effective_budget_per_sec).min(burst);
        self.last_refill = now;
    }

    pub fn fill_ratio(&self) -> f64 {
        let burst = self.effective_budget_per_sec.max(1.0);
        (self.tokens / burst).clamp(0.0, 1.0)
    }

    /// Whether headroom + idle allow early flush (effective linger 0).
    ///
    /// - `queued_bytes` must be ≤ `early_flush_max_queued_bytes` (if that cap &gt; 0)
    /// - `last_enqueue` must be at least `early_flush_idle` ago (if idle &gt; 0)
    ///
    /// Sustained produce keeps `last_enqueue` fresh → no early-flush → linger/`flush` pack.
    pub fn allow_early_flush(
        &self,
        now: Instant,
        queued_bytes: usize,
        last_enqueue: Option<Instant>,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }
        let max_q = self.config.early_flush_max_queued_bytes;
        if max_q > 0 && queued_bytes > max_q {
            return false;
        }
        let idle_need = self.config.early_flush_idle;
        if !idle_need.is_zero() {
            let Some(last) = last_enqueue else {
                return false;
            };
            if now.duration_since(last) < idle_need {
                return false;
            }
        }
        if self.fill_ratio() < self.config.early_flush_fill_ratio {
            return false;
        }
        if let Some(last) = self.last_early_flush
            && now.duration_since(last) < self.config.early_flush_cooldown
        {
            return false;
        }
        true
    }

    pub fn note_early_flush(&mut self, now: Instant) {
        self.last_early_flush = Some(now);
    }

    /// Consume media ops after a flush (always succeeds; may overdraft).
    pub fn consume_after_flush(&mut self, media_ops: u64, now: Instant) {
        self.refill(now);
        let cost = media_ops as f64;
        if cost <= self.tokens {
            self.tokens -= cost;
        } else {
            self.overdraft_total += (cost - self.tokens).ceil() as u64;
            self.tokens = 0.0;
        }
        self.media_ops_total += media_ops;
        self.flushes_total += 1;
        // EWMA of cost
        const ALPHA: f64 = 0.2;
        if !self.ewma_inited {
            self.media_ops_ewma = cost.max(0.1);
            self.ewma_inited = true;
        } else {
            self.media_ops_ewma = ALPHA * cost + (1.0 - ALPHA) * self.media_ops_ewma;
        }
        self.predicted_media_ops = self.media_ops_ewma.max(0.1);
        // Ongoing capacity estimate from implied rate if we had overdraft recently — keep simple:
        // if we observed media ops, capacity estimate ≈ max(default, ops in last sec proxy)
        // Use inverse of nothing; leave ongoing for optional future. Bootstrap prediction only.
    }

    /// Admission check for Durable/Sequenced produces under fail_closed / budget_priority.
    /// Returns false if produce should be rejected (fail_closed) or caller should wait.
    pub fn can_admit_now(&mut self, now: Instant) -> bool {
        if !self.config.enabled {
            return true;
        }
        self.refill(now);
        match self.config.mode {
            BudgetMode::LatencyPriority => true,
            BudgetMode::BudgetPriority | BudgetMode::FailClosed => {
                self.tokens >= self.predicted_media_ops * 0.5
            }
        }
    }

    pub fn reserve_for_fail_closed(&mut self, now: Instant) -> bool {
        if !self.config.enabled || self.config.mode != BudgetMode::FailClosed {
            return true;
        }
        self.refill(now);
        let need = self.predicted_media_ops;
        if self.tokens >= need {
            self.tokens -= need;
            true
        } else {
            false
        }
    }
}

/// Why an effective value was chosen (for inspectability).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectiveReason {
    /// Operator config alone.
    Configured,
    /// Limited by config cap.
    ConfigCap,
    /// From startup probe.
    StartupProbe,
    /// From ongoing measurement.
    Ongoing,
    /// Library / profile default capacity.
    DefaultCapacity,
    /// Budget controller disabled.
    Disabled,
}

/// One inspectable knob with layered sources.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectiveKnob<T: Clone> {
    /// Operator-configured value, if any.
    pub configured: Option<T>,
    /// Startup measurement, if any.
    pub startup_measured: Option<T>,
    /// Ongoing measurement, if any.
    pub ongoing_measured: Option<T>,
    /// Value currently in force.
    pub effective: T,
    /// Why `effective` was chosen.
    pub reason: EffectiveReason,
}

/// Point-in-time controller + buffer view.
#[derive(Clone, Debug)]
pub struct PipelineSnapshot {
    /// Effective durable-ops/sec budget.
    pub budget_per_sec: EffectiveKnob<f64>,
    /// Effective linger currently used for scheduling (0 = early flush).
    pub effective_linger_ms: EffectiveKnob<u64>,
    /// Operator max linger (ms).
    pub max_linger_ms: u64,
    /// Token fill ratio in [0, 1].
    pub token_fill_ratio: f64,
    /// Cumulative media ops consumed.
    pub media_ops_total: u64,
    /// Cumulative overdraft media ops.
    pub overdraft_total: u64,
    /// Successful flushes.
    pub flushes_total: u64,
    /// Deadline flushes that were under size soft target (counted when metered).
    pub undersized_deadline_flushes: u64,
    /// Predicted media ops per flush (EWMA).
    pub predicted_media_ops: f64,
    /// Whether budget controller is enabled.
    pub budget_enabled: bool,
    /// Active budget conflict mode.
    pub budget_mode: BudgetMode,
}
