//! Runtime-neutral byte admission for owned async mutations.
//!
//! Admission is FIFO, cancellation-safe, and represented by a non-cloneable owned permit.  The permit is
//! deliberately independent of any executor clock: adapters that expose a wait deadline race the acquire
//! future against their runtime timer and translate a dropped waiter to retryable backpressure.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use fireweed_core::TenantId;

/// Checked peak bytes for retained records plus a simultaneously resident frame containing the same
/// records. Adapters supply their framing overhead; this keeps sync and async accounting identical without
/// moving an object-log format dependency into the engine.
pub fn retained_records_plus_frame_bytes(
    lengths: impl IntoIterator<Item = usize>,
    frame_fixed_bytes: usize,
    per_record_frame_bytes: usize,
) -> Option<usize> {
    lengths
        .into_iter()
        .try_fold((0usize, frame_fixed_bytes), |(records, frame), length| {
            Some((
                records.checked_add(length)?,
                frame
                    .checked_add(per_record_frame_bytes)?
                    .checked_add(length)?,
            ))
        })
        .and_then(|(records, frame)| records.checked_add(frame))
}

/// The resource limit that rejected an admission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteBudgetScope {
    Global,
    Tenant,
}

/// Typed admission failures. Oversize is permanent; closed and backpressure are retryable by adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteAdmissionError {
    Closed,
    Backpressure,
    Oversize {
        requested: usize,
        limit: usize,
        scope: ByteBudgetScope,
    },
}

/// Validated byte-budget settings.
#[derive(Debug, Clone)]
pub struct BufferedByteBudgetConfig {
    global_limit: usize,
    tenant_limit: Option<usize>,
}

impl BufferedByteBudgetConfig {
    pub fn new(global_limit: usize) -> Result<Self, &'static str> {
        if global_limit == 0 {
            return Err("global buffered-byte limit must be positive");
        }
        Ok(Self {
            global_limit,
            tenant_limit: None,
        })
    }

    pub fn with_uniform_tenant_limit(mut self, limit: usize) -> Result<Self, &'static str> {
        if limit == 0 {
            return Err("tenant buffered-byte limit must be positive");
        }
        if limit > self.global_limit {
            return Err("tenant buffered-byte limit cannot exceed the global limit");
        }
        self.tenant_limit = Some(limit);
        Ok(self)
    }

    pub fn global_limit(&self) -> usize {
        self.global_limit
    }

    pub fn tenant_limit(&self) -> Option<usize> {
        self.tenant_limit
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferedByteBudgetStats {
    pub charged_bytes: usize,
    pub peak_charged_bytes: usize,
    pub waiting_requests: usize,
    pub wait_count: u64,
    pub rejection_count: u64,
    pub total_wait_nanos: u128,
    pub max_wait_nanos: u128,
}

struct Waiter {
    id: u64,
    tenant: TenantId,
    bytes: usize,
    granted: bool,
    bypasses: u8,
    waker: Option<Waker>,
}

struct BudgetState {
    closed: bool,
    next_waiter_id: u64,
    charged: usize,
    peak: usize,
    tenant_charged: HashMap<TenantId, usize>,
    waiters: VecDeque<Waiter>,
    wait_count: u64,
    rejection_count: u64,
    total_wait_nanos: u128,
    max_wait_nanos: u128,
}

struct BudgetInner {
    config: BufferedByteBudgetConfig,
    state: Mutex<BudgetState>,
}

/// A node-global byte budget with optional per-tenant hard shares.
#[derive(Clone)]
pub struct BufferedByteBudget {
    inner: Arc<BudgetInner>,
}

impl BufferedByteBudget {
    pub fn new(config: BufferedByteBudgetConfig) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                config,
                state: Mutex::new(BudgetState {
                    closed: false,
                    next_waiter_id: 0,
                    charged: 0,
                    peak: 0,
                    tenant_charged: HashMap::new(),
                    waiters: VecDeque::new(),
                    wait_count: 0,
                    rejection_count: 0,
                    total_wait_nanos: 0,
                    max_wait_nanos: 0,
                }),
            }),
        }
    }

    pub fn config(&self) -> &BufferedByteBudgetConfig {
        &self.inner.config
    }

    /// Register on first poll. Dropping an unpolled or waiting future never consumes bytes.
    pub fn acquire(&self, tenant: TenantId, bytes: usize) -> ByteBudgetAcquire {
        ByteBudgetAcquire {
            inner: Arc::clone(&self.inner),
            tenant,
            bytes,
            waiter_id: None,
            completed: false,
            registered_at: None,
        }
    }

    /// Non-waiting admission for transports that must return typed backpressure immediately.
    pub fn try_acquire(
        &self,
        tenant: TenantId,
        bytes: usize,
    ) -> Result<OwnedBytePermit, ByteAdmissionError> {
        if let Err(error) = validate_request(&self.inner, &tenant, bytes) {
            self.inner
                .state
                .lock()
                .expect("byte budget poisoned")
                .rejection_count += 1;
            return Err(error);
        }
        let mut state = self.inner.state.lock().expect("byte budget poisoned");
        if state.closed {
            state.rejection_count += 1;
            return Err(ByteAdmissionError::Closed);
        }
        if !state.waiters.is_empty() || !fits(&self.inner, &state, &tenant, bytes) {
            state.rejection_count += 1;
            return Err(ByteAdmissionError::Backpressure);
        }
        charge(&mut state, &tenant, bytes);
        Ok(OwnedBytePermit::new(Arc::clone(&self.inner), tenant, bytes))
    }

    pub fn close(&self) {
        let wakers = {
            let mut state = self.inner.state.lock().expect("byte budget poisoned");
            state.closed = true;
            let waiters = std::mem::take(&mut state.waiters);
            let mut wakers = Vec::new();
            for mut waiter in waiters {
                if waiter.granted {
                    state.charged = state
                        .charged
                        .checked_sub(waiter.bytes)
                        .expect("granted waiter exceeded global byte charge");
                    let tenant_bytes = state
                        .tenant_charged
                        .get_mut(&waiter.tenant)
                        .expect("granted waiter tenant charge disappeared");
                    *tenant_bytes = tenant_bytes
                        .checked_sub(waiter.bytes)
                        .expect("granted waiter exceeded tenant byte charge");
                    if *tenant_bytes == 0 {
                        state.tenant_charged.remove(&waiter.tenant);
                    }
                }
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub fn stats(&self) -> BufferedByteBudgetStats {
        let state = self.inner.state.lock().expect("byte budget poisoned");
        BufferedByteBudgetStats {
            charged_bytes: state.charged,
            peak_charged_bytes: state.peak,
            waiting_requests: state.waiters.len(),
            wait_count: state.wait_count,
            rejection_count: state.rejection_count,
            total_wait_nanos: state.total_wait_nanos,
            max_wait_nanos: state.max_wait_nanos,
        }
    }

    pub fn tenant_charged_bytes(&self, tenant: &TenantId) -> usize {
        self.inner
            .state
            .lock()
            .expect("byte budget poisoned")
            .tenant_charged
            .get(tenant)
            .copied()
            .unwrap_or(0)
    }
}

fn validate_request(
    inner: &BudgetInner,
    _tenant: &TenantId,
    bytes: usize,
) -> Result<(), ByteAdmissionError> {
    if bytes > inner.config.global_limit {
        return Err(ByteAdmissionError::Oversize {
            requested: bytes,
            limit: inner.config.global_limit,
            scope: ByteBudgetScope::Global,
        });
    }
    if let Some(limit) = inner.config.tenant_limit()
        && bytes > limit
    {
        return Err(ByteAdmissionError::Oversize {
            requested: bytes,
            limit,
            scope: ByteBudgetScope::Tenant,
        });
    }
    Ok(())
}

fn fits(inner: &BudgetInner, state: &BudgetState, tenant: &TenantId, bytes: usize) -> bool {
    fits_global(inner, state, bytes) && fits_tenant(inner, state, tenant, bytes)
}

fn fits_global(inner: &BudgetInner, state: &BudgetState, bytes: usize) -> bool {
    state
        .charged
        .checked_add(bytes)
        .is_some_and(|charged| charged <= inner.config.global_limit)
}

fn fits_tenant(inner: &BudgetInner, state: &BudgetState, tenant: &TenantId, bytes: usize) -> bool {
    inner.config.tenant_limit().is_none_or(|limit| {
        state
            .tenant_charged
            .get(tenant)
            .copied()
            .unwrap_or(0)
            .checked_add(bytes)
            .is_some_and(|charged| charged <= limit)
    })
}

fn charge(state: &mut BudgetState, tenant: &TenantId, bytes: usize) {
    state.charged = state
        .charged
        .checked_add(bytes)
        .expect("validated global byte charge overflowed");
    state.peak = state.peak.max(state.charged);
    let tenant_charge = state.tenant_charged.entry(tenant.clone()).or_default();
    *tenant_charge = tenant_charge
        .checked_add(bytes)
        .expect("validated tenant byte charge overflowed");
}

fn uncharge(state: &mut BudgetState, tenant: &TenantId, bytes: usize) {
    state.charged = state
        .charged
        .checked_sub(bytes)
        .expect("byte permit released more global bytes than charged");
    let tenant_bytes = state
        .tenant_charged
        .get_mut(tenant)
        .expect("byte permit tenant accounting disappeared");
    *tenant_bytes = tenant_bytes
        .checked_sub(bytes)
        .expect("byte permit released more tenant bytes than charged");
    if *tenant_bytes == 0 {
        state.tenant_charged.remove(tenant);
    }
}

fn release(inner: &BudgetInner, tenant: &TenantId, bytes: usize) {
    let wakers = {
        let mut state = inner.state.lock().expect("byte budget poisoned");
        uncharge(&mut state, tenant, bytes);
        grant_waiters(inner, &mut state)
    };
    for waker in wakers {
        waker.wake();
    }
}

/// Grant the oldest waiter that fits, preserving FIFO within each constrained tenant without allowing a
/// tenant at its hard share to head-of-line block unrelated tenants that still have global capacity.
fn grant_waiters(inner: &BudgetInner, state: &mut BudgetState) -> Vec<Waker> {
    const MAX_BYPASSES: u8 = 8;
    let mut wakers = Vec::new();
    loop {
        // Starvation reservation applies only when an aged request lacks GLOBAL capacity. A request blocked
        // solely by its tenant share must remain bypassable or it could strand free global capacity forever.
        let aged_global = state.waiters.iter().position(|waiter| {
            !waiter.granted
                && waiter.bypasses >= MAX_BYPASSES
                && !fits_global(inner, state, waiter.bytes)
        });
        let index = match aged_global {
            Some(_) => break,
            None => match state
                .waiters
                .iter()
                .enumerate()
                .position(|(index, waiter)| {
                    !waiter.granted
                    && fits(inner, state, &waiter.tenant, waiter.bytes)
                    // Never bypass an older waiter from the same tenant, even when the younger request is
                    // smaller and would fit the remaining tenant share.
                    && !state.waiters.iter().take(index).any(|older| {
                        !older.granted && older.tenant == waiter.tenant
                    })
                }) {
                Some(index) => index,
                None => break,
            },
        };
        for bypassed in state.waiters.iter_mut().take(index) {
            if !bypassed.granted {
                bypassed.bypasses = bypassed.bypasses.saturating_add(1);
            }
        }
        let waiter = &mut state.waiters[index];
        let tenant = waiter.tenant.clone();
        let bytes = waiter.bytes;
        waiter.granted = true;
        if let Some(waker) = waiter.waker.take() {
            wakers.push(waker);
        }
        charge(state, &tenant, bytes);
    }
    wakers
}

/// Non-cloneable ownership token. Capacity is released exactly when the final resident byte owner drops it.
pub struct OwnedBytePermit {
    inner: Option<Arc<BudgetInner>>,
    tenant: TenantId,
    bytes: usize,
}

impl std::fmt::Debug for OwnedBytePermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedBytePermit")
            .field("tenant", &self.tenant)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl OwnedBytePermit {
    fn new(inner: Arc<BudgetInner>, tenant: TenantId, bytes: usize) -> Self {
        Self {
            inner: Some(inner),
            tenant,
            bytes,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for OwnedBytePermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            release(&inner, &self.tenant, self.bytes);
        }
    }
}

pub struct ByteBudgetAcquire {
    inner: Arc<BudgetInner>,
    tenant: TenantId,
    bytes: usize,
    waiter_id: Option<u64>,
    completed: bool,
    registered_at: Option<Instant>,
}

impl Future for ByteBudgetAcquire {
    type Output = Result<OwnedBytePermit, ByteAdmissionError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Err(error) = validate_request(&self.inner, &self.tenant, self.bytes) {
            self.inner
                .state
                .lock()
                .expect("byte budget poisoned")
                .rejection_count += 1;
            self.completed = true;
            return Poll::Ready(Err(error));
        }
        let mut state = self.inner.state.lock().expect("byte budget poisoned");
        if state.closed {
            state.rejection_count += 1;
            if let Some(id) = self.waiter_id
                && let Some(index) = state.waiters.iter().position(|waiter| waiter.id == id)
            {
                let waiter = state
                    .waiters
                    .remove(index)
                    .expect("registered byte waiter disappeared");
                if waiter.granted {
                    uncharge(&mut state, &waiter.tenant, waiter.bytes);
                }
            }
            drop(state);
            self.completed = true;
            return Poll::Ready(Err(ByteAdmissionError::Closed));
        }
        if let Some(id) = self.waiter_id {
            let index = state
                .waiters
                .iter()
                .position(|waiter| waiter.id == id)
                .expect("registered byte waiter disappeared");
            if state.waiters[index].granted {
                state.waiters.remove(index);
                if let Some(started) = self.registered_at {
                    let elapsed = started.elapsed().as_nanos();
                    state.total_wait_nanos = state.total_wait_nanos.saturating_add(elapsed);
                    state.max_wait_nanos = state.max_wait_nanos.max(elapsed);
                }
                let wakers = grant_waiters(&self.inner, &mut state);
                drop(state);
                for waker in wakers {
                    waker.wake();
                }
                self.completed = true;
                return Poll::Ready(Ok(OwnedBytePermit::new(
                    Arc::clone(&self.inner),
                    self.tenant.clone(),
                    self.bytes,
                )));
            }
            state.waiters[index].waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        if state.waiters.is_empty() && fits(&self.inner, &state, &self.tenant, self.bytes) {
            charge(&mut state, &self.tenant, self.bytes);
            drop(state);
            self.completed = true;
            return Poll::Ready(Ok(OwnedBytePermit::new(
                Arc::clone(&self.inner),
                self.tenant.clone(),
                self.bytes,
            )));
        }
        let id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.wait_count += 1;
        state.waiters.push_back(Waiter {
            id,
            tenant: self.tenant.clone(),
            bytes: self.bytes,
            granted: false,
            bypasses: 0,
            waker: Some(context.waker().clone()),
        });
        let wakers = grant_waiters(&self.inner, &mut state);
        drop(state);
        self.waiter_id = Some(id);
        self.registered_at = Some(Instant::now());
        for waker in wakers {
            waker.wake();
        }
        Poll::Pending
    }
}

impl Drop for ByteBudgetAcquire {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(id) = self.waiter_id else {
            return;
        };
        let wakers = {
            let mut state = self.inner.state.lock().expect("byte budget poisoned");
            let Some(index) = state.waiters.iter().position(|waiter| waiter.id == id) else {
                return;
            };
            let waiter = state
                .waiters
                .remove(index)
                .expect("byte waiter disappeared");
            if waiter.granted {
                uncharge(&mut state, &waiter.tenant, waiter.bytes);
            }
            grant_waiters(&self.inner, &mut state)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use fireweed_core::TenantId;

    use super::*;

    fn tenant(name: &str) -> TenantId {
        TenantId::new(name).unwrap()
    }

    fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
        let mut context = Context::from_waker(Waker::noop());
        future.poll(&mut context)
    }

    #[test]
    fn permit_conservation_and_peak_accounting() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(10).unwrap());
        let first = budget.try_acquire(tenant("a"), 4).unwrap();
        let second = budget.try_acquire(tenant("b"), 6).unwrap();
        assert_eq!(budget.stats().charged_bytes, 10);
        assert_eq!(budget.stats().peak_charged_bytes, 10);
        drop(first);
        assert_eq!(budget.stats().charged_bytes, 6);
        drop(second);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn cancelled_waiter_never_leaks_or_blocks_fifo() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(5).unwrap());
        let held = budget.try_acquire(tenant("a"), 5).unwrap();
        let mut cancelled = Box::pin(budget.acquire(tenant("a"), 5));
        let mut survivor = Box::pin(budget.acquire(tenant("b"), 5));
        assert!(poll_once(cancelled.as_mut()).is_pending());
        assert!(poll_once(survivor.as_mut()).is_pending());
        drop(cancelled);
        drop(held);
        let permit = match poll_once(survivor.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("surviving waiter was not granted"),
        };
        assert_eq!(budget.stats().charged_bytes, 5);
        drop(permit);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn uniform_tenant_limit_allows_independent_tenants_to_use_the_budget() {
        let config = BufferedByteBudgetConfig::new(10)
            .unwrap()
            .with_uniform_tenant_limit(4)
            .unwrap();
        let budget = BufferedByteBudget::new(config);
        let hot = budget.try_acquire(tenant("hot"), 4).unwrap();
        assert_eq!(
            budget.try_acquire(tenant("hot"), 1).unwrap_err(),
            ByteAdmissionError::Backpressure
        );
        let cold = budget.try_acquire(tenant("cold"), 4).unwrap();
        assert_eq!(budget.stats().charged_bytes, 8);
        drop((hot, cold));
    }

    #[test]
    fn tenant_blocked_waiter_does_not_head_of_line_block_cold_tenant() {
        let config = BufferedByteBudgetConfig::new(10)
            .unwrap()
            .with_uniform_tenant_limit(4)
            .unwrap();
        let budget = BufferedByteBudget::new(config);
        let held = budget.try_acquire(tenant("hot"), 4).unwrap();
        let mut hot_waiter = Box::pin(budget.acquire(tenant("hot"), 1));
        let mut cold_waiter = Box::pin(budget.acquire(tenant("cold"), 4));
        assert!(poll_once(hot_waiter.as_mut()).is_pending());
        assert!(poll_once(cold_waiter.as_mut()).is_pending());
        let cold = match poll_once(cold_waiter.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("cold tenant was blocked by hot tenant's hard share"),
        };
        assert_eq!(budget.stats().charged_bytes, 8);
        drop(cold);
        drop(held);
        let hot = match poll_once(hot_waiter.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("hot tenant did not resume after its share cleared"),
        };
        drop(hot);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn generated_try_acquire_release_trace_conserves_bytes() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(128).unwrap());
        let tenants = [tenant("a"), tenant("b"), tenant("c")];
        let mut permits = Vec::<OwnedBytePermit>::new();
        let mut expected = 0usize;
        let mut seed = 0x9e37_79b9_u64;
        for _ in 0..10_000 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            if seed & 3 == 0 && !permits.is_empty() {
                let index = (seed as usize) % permits.len();
                expected -= permits.swap_remove(index).bytes();
            } else {
                let bytes = ((seed >> 16) as usize % 32) + 1;
                let tenant = tenants[(seed as usize) % tenants.len()].clone();
                if let Ok(permit) = budget.try_acquire(tenant, bytes) {
                    expected += bytes;
                    permits.push(permit);
                }
            }
            assert_eq!(budget.stats().charged_bytes, expected);
            assert!(expected <= 128);
        }
        drop(permits);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn bounded_bypass_prevents_large_waiter_starvation() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(10).unwrap());
        let held = budget.try_acquire(tenant("holder"), 9).unwrap();
        let mut large = Box::pin(budget.acquire(tenant("cold"), 10));
        assert!(poll_once(large.as_mut()).is_pending());
        for index in 0..8 {
            let mut small = Box::pin(budget.acquire(tenant(&format!("hot-{index}")), 1));
            assert!(poll_once(small.as_mut()).is_pending());
            let permit = match poll_once(small.as_mut()) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("bounded bypass allowance ended too early"),
            };
            drop(permit);
        }
        let mut blocked = Box::pin(budget.acquire(tenant("hot-last"), 1));
        assert!(poll_once(blocked.as_mut()).is_pending());
        assert!(poll_once(blocked.as_mut()).is_pending());
        drop(held);
        let large = match poll_once(large.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("aged large waiter was starved by small arrivals"),
        };
        drop(large);
        let small = match poll_once(blocked.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("small waiter did not resume after aged waiter"),
        };
        drop(small);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn aged_tenant_share_waiter_does_not_strand_cold_global_capacity() {
        let config = BufferedByteBudgetConfig::new(100)
            .unwrap()
            .with_uniform_tenant_limit(50)
            .unwrap();
        let budget = BufferedByteBudget::new(config);
        let held = budget.try_acquire(tenant("hot"), 50).unwrap();
        let mut hot = Box::pin(budget.acquire(tenant("hot"), 40));
        assert!(poll_once(hot.as_mut()).is_pending());

        // Age the hot waiter through the bounded-bypass threshold while its tenant is at its hard share.
        for index in 0..8 {
            let mut cold = Box::pin(budget.acquire(tenant(&format!("cold-{index}")), 5));
            assert!(poll_once(cold.as_mut()).is_pending());
            let permit = match poll_once(cold.as_mut()) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("cold tenant did not use free global capacity"),
            };
            drop(permit);
        }
        let mut final_cold = Box::pin(budget.acquire(tenant("cold-final"), 25));
        assert!(poll_once(final_cold.as_mut()).is_pending());
        let cold = match poll_once(final_cold.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("aged tenant-blocked waiter stranded free global capacity"),
        };
        assert!(poll_once(hot.as_mut()).is_pending());
        drop(cold);
        drop(held);
        let hot = match poll_once(hot.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("hot waiter did not resume after its tenant share cleared"),
        };
        drop(hot);
    }

    #[test]
    fn tenant_bypass_never_reorders_younger_same_tenant_waiter() {
        let config = BufferedByteBudgetConfig::new(100)
            .unwrap()
            .with_uniform_tenant_limit(50)
            .unwrap();
        let budget = BufferedByteBudget::new(config);
        let held = budget.try_acquire(tenant("hot"), 45).unwrap();
        let mut older = Box::pin(budget.acquire(tenant("hot"), 10));
        let mut younger = Box::pin(budget.acquire(tenant("hot"), 5));
        assert!(poll_once(older.as_mut()).is_pending());
        assert!(poll_once(younger.as_mut()).is_pending());
        assert!(poll_once(younger.as_mut()).is_pending());
        drop(held);
        let older = match poll_once(older.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("older same-tenant waiter was reordered"),
        };
        let younger = match poll_once(younger.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("younger same-tenant waiter did not follow older waiter"),
        };
        drop((older, younger));
    }

    #[test]
    fn oversize_is_permanent_and_scoped() {
        let config = BufferedByteBudgetConfig::new(10)
            .unwrap()
            .with_uniform_tenant_limit(4)
            .unwrap();
        let budget = BufferedByteBudget::new(config);
        assert_eq!(
            budget.try_acquire(tenant("a"), 5).unwrap_err(),
            ByteAdmissionError::Oversize {
                requested: 5,
                limit: 4,
                scope: ByteBudgetScope::Tenant,
            }
        );
        assert_eq!(
            budget.try_acquire(tenant("b"), 11).unwrap_err(),
            ByteAdmissionError::Oversize {
                requested: 11,
                limit: 10,
                scope: ByteBudgetScope::Global,
            }
        );
    }

    #[test]
    fn close_wakes_waiters_and_preserves_held_permits() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(1).unwrap());
        let held = budget.try_acquire(tenant("a"), 1).unwrap();
        let mut waiter = Box::pin(budget.acquire(tenant("b"), 1));
        assert!(poll_once(waiter.as_mut()).is_pending());
        budget.close();
        assert!(matches!(
            poll_once(waiter.as_mut()),
            Poll::Ready(Err(ByteAdmissionError::Closed))
        ));
        assert_eq!(budget.stats().charged_bytes, 1);
        drop(held);
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn close_eagerly_revokes_granted_unconsumed_waiters() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(1).unwrap());
        let held = budget.try_acquire(tenant("a"), 1).unwrap();
        let mut waiter = Box::pin(budget.acquire(tenant("b"), 1));
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(held);
        assert_eq!(budget.stats().charged_bytes, 1);
        budget.close();
        assert_eq!(budget.stats().charged_bytes, 0);
        assert_eq!(budget.stats().waiting_requests, 0);
        assert!(matches!(
            poll_once(waiter.as_mut()),
            Poll::Ready(Err(ByteAdmissionError::Closed))
        ));
        assert_eq!(budget.stats().charged_bytes, 0);
    }

    #[test]
    fn close_reclaims_a_granted_waiter_that_has_not_consumed_its_permit() {
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(1).unwrap());
        let held = budget.try_acquire(tenant("a"), 1).unwrap();
        let mut waiter = Box::pin(budget.acquire(tenant("b"), 1));
        assert!(poll_once(waiter.as_mut()).is_pending());

        drop(held);
        assert_eq!(budget.stats().charged_bytes, 1);
        budget.close();
        assert!(matches!(
            poll_once(waiter.as_mut()),
            Poll::Ready(Err(ByteAdmissionError::Closed))
        ));
        assert_eq!(budget.stats().charged_bytes, 0);
    }
}
