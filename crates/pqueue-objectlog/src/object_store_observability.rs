//! Bounded, vendor-neutral object-store telemetry recorded below protocol retries.
//!
//! Metric identity is deliberately represented by enums. Object keys and error
//! messages are data, never labels, so even hostile inputs cannot increase
//! cardinality.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use pqueue_engine::{EngineError, EngineResult};

use crate::segmented::{BlobStore, ObjectStoreStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum BlobOperation {
    Put,
    PutIfAbsent,
    Get,
    Delete,
    List,
    ListPage,
    Stats,
    ReadManifestHead,
    UpdateManifestHead,
    AcquireEpoch,
    FenceEpoch,
    Branch,
    ValidateSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum BlobObjectClass {
    Segment,
    Manifest,
    ManifestHead,
    Epoch,
    RetentionFloor,
    DeletionWatermark,
    BranchPin,
    Other,
}

impl BlobObjectClass {
    /// Classifies complete path components only; arbitrary key material is never retained.
    pub fn from_key(key: &str) -> Self {
        if key
            .rsplit('/')
            .next()
            .is_some_and(|name| name.ends_with("~watermark.json") || name == "read_horizon.json")
        {
            return Self::DeletionWatermark;
        }
        for component in key.split('/') {
            let class = match component {
                "segments" | "segment" | "seg_candidates" | "seg_attempt" | "branch-seg" => {
                    Self::Segment
                }
                "manifest" | "manifest_candidates" => Self::Manifest,
                "manifest_head"
                | "manifest-head"
                | "authority_head"
                | "authority_protocol_v1"
                | "authority_initialized_v1" => Self::ManifestHead,
                "epoch" | "epoch.json" => Self::Epoch,
                "retention_floor" | "retention-floor" | "retention_floor.json" => {
                    Self::RetentionFloor
                }
                "deletion_watermark" | "deletion-watermark" | "read_horizon.json" => {
                    Self::DeletionWatermark
                }
                "branch_pin" | "branch-pins" | "branch_pin.json" | "branches" | "branch.json"
                | "branch.pending" => Self::BranchPin,
                _ => continue,
            };
            return class;
        }
        Self::Other
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum BlobResultClass {
    Success,
    NotFound,
    PreconditionLost,
    Throttled,
    Timeout,
    Transport,
    Corrupt,
    OtherError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum BlobBackendKind {
    Memory,
    LocalFs,
    S3,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobStoreFault {
    pub result: BlobResultClass,
    pub retryable: bool,
    pub throttled: bool,
    pub timeout: bool,
}

#[derive(Debug)]
pub struct ClassifiedBlobError {
    pub outward: EngineError,
    pub fault: BlobStoreFault,
    pub attempts: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

impl ClassifiedBlobError {
    pub fn fallback(store: &(impl BlobStore + ?Sized), outward: EngineError) -> Self {
        let fault = store.classify_fault(&outward);
        Self {
            outward,
            fault,
            attempts: 1,
            request_bytes: 0,
            response_bytes: 0,
        }
    }
}

pub type ClassifiedBlobResult<T> = Result<T, ClassifiedBlobError>;

#[derive(Debug)]
pub struct ObservedBlobCall<T> {
    pub value: T,
    pub attempts: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

impl<T> ObservedBlobCall<T> {
    pub fn new(value: T, attempts: u64, request_bytes: u64, response_bytes: u64) -> Self {
        Self {
            value,
            attempts,
            request_bytes,
            response_bytes,
        }
    }
}

impl BlobStoreFault {
    pub const fn new(
        result: BlobResultClass,
        retryable: bool,
        throttled: bool,
        timeout: bool,
    ) -> Self {
        Self {
            result,
            retryable,
            throttled,
            timeout,
        }
    }

    /// Conservative fallback for stores which have not yet supplied provider-level classification.
    /// This matches variants, never display strings.
    pub fn from_engine_error(error: &EngineError) -> Self {
        match error {
            EngineError::NotFound => Self::new(BlobResultClass::NotFound, false, false, false),
            EngineError::Conflict => {
                Self::new(BlobResultClass::PreconditionLost, true, false, false)
            }
            EngineError::Storage(_) => Self::new(BlobResultClass::OtherError, false, false, false),
            EngineError::DurableDataCorrupt { .. } => {
                Self::new(BlobResultClass::Corrupt, false, false, false)
            }
            _ => Self::new(BlobResultClass::OtherError, false, false, false),
        }
    }
}

const OP_COUNT: usize = 13;
const OBJECT_COUNT: usize = 8;
const RESULT_COUNT: usize = 8;
const BACKEND_COUNT: usize = 4;
const RETRYABLE_COUNT: usize = 2;
const BUCKET_COUNT: usize =
    OP_COUNT * OBJECT_COUNT * RESULT_COUNT * BACKEND_COUNT * RETRYABLE_COUNT;

#[derive(Default)]
struct AtomicBucket {
    completions: AtomicU64,
    attempts: AtomicU64,
    retries: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    latency_ns: AtomicU64,
    errors: AtomicU64,
    throttles: AtomicU64,
    timeouts: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlobMetricValues {
    pub completions: u64,
    pub attempts: u64,
    pub retries: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub latency_ns: u64,
    pub errors: u64,
    pub throttles: u64,
    pub timeouts: u64,
}

impl BlobMetricValues {
    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            completions: self.completions.saturating_sub(earlier.completions),
            attempts: self.attempts.saturating_sub(earlier.attempts),
            retries: self.retries.saturating_sub(earlier.retries),
            request_bytes: self.request_bytes.saturating_sub(earlier.request_bytes),
            response_bytes: self.response_bytes.saturating_sub(earlier.response_bytes),
            latency_ns: self.latency_ns.saturating_sub(earlier.latency_ns),
            errors: self.errors.saturating_sub(earlier.errors),
            throttles: self.throttles.saturating_sub(earlier.throttles),
            timeouts: self.timeouts.saturating_sub(earlier.timeouts),
        }
    }

    pub fn is_zero(self) -> bool {
        self == Self::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobMetricRow {
    pub operation: BlobOperation,
    pub object_class: BlobObjectClass,
    pub result: BlobResultClass,
    pub retryable: bool,
    pub backend: BlobBackendKind,
    pub values: BlobMetricValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMetricsSnapshot {
    pub rows: Vec<BlobMetricRow>,
    pub in_flight: u64,
    pub peak_in_flight: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlobPhysicalTotals {
    pub puts: u64,
    pub gets: u64,
    pub lists: u64,
    pub deletes: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

impl BlobMetricsSnapshot {
    pub fn physical_totals(&self) -> BlobPhysicalTotals {
        let mut totals = BlobPhysicalTotals::default();
        for row in &self.rows {
            match row.operation {
                BlobOperation::Put | BlobOperation::PutIfAbsent => {
                    totals.puts += row.values.attempts
                }
                BlobOperation::Get => totals.gets += row.values.attempts,
                BlobOperation::List | BlobOperation::ListPage => {
                    totals.lists += row.values.attempts
                }
                BlobOperation::Delete => totals.deletes += row.values.attempts,
                BlobOperation::Stats
                | BlobOperation::ReadManifestHead
                | BlobOperation::UpdateManifestHead
                | BlobOperation::AcquireEpoch
                | BlobOperation::FenceEpoch
                | BlobOperation::Branch
                | BlobOperation::ValidateSegment => continue,
            }
            totals.request_bytes += row.values.request_bytes;
            totals.response_bytes += row.values.response_bytes;
        }
        totals
    }

    pub fn row(
        &self,
        operation: BlobOperation,
        object_class: BlobObjectClass,
        result: BlobResultClass,
        retryable: bool,
        backend: BlobBackendKind,
    ) -> BlobMetricValues {
        self.rows
            .iter()
            .find(|row| {
                row.operation == operation
                    && row.object_class == object_class
                    && row.result == result
                    && row.retryable == retryable
                    && row.backend == backend
            })
            .map_or_else(BlobMetricValues::default, |row| row.values)
    }

    pub fn delta(&self, earlier: &Self) -> Self {
        let mut rows = Vec::new();
        for row in &self.rows {
            let values = row.values.saturating_sub(earlier.row(
                row.operation,
                row.object_class,
                row.result,
                row.retryable,
                row.backend,
            ));
            if !values.is_zero() {
                rows.push(BlobMetricRow { values, ..*row });
            }
        }
        Self {
            rows,
            // Gauges are observations, not monotonic counters.
            in_flight: self.in_flight,
            peak_in_flight: self.peak_in_flight,
        }
    }
}

pub struct BlobMetricsRecorder {
    buckets: Option<Box<[AtomicBucket]>>,
    in_flight: AtomicU64,
    peak_in_flight: AtomicU64,
}

impl Default for BlobMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobMetricsRecorder {
    pub fn new() -> Self {
        Self {
            buckets: Some(
                (0..BUCKET_COUNT)
                    .map(|_| AtomicBucket::default())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            in_flight: AtomicU64::new(0),
            peak_in_flight: AtomicU64::new(0),
        }
    }

    pub fn disabled() -> Self {
        Self {
            buckets: None,
            in_flight: AtomicU64::new(0),
            peak_in_flight: AtomicU64::new(0),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.buckets.is_some()
    }

    /// One process-wide recorder bounds enabled instrumentation memory independently of log count.
    pub fn production_shared() -> Arc<Self> {
        static RECORDER: OnceLock<Arc<BlobMetricsRecorder>> = OnceLock::new();
        Arc::clone(RECORDER.get_or_init(|| Arc::new(Self::new())))
    }

    pub fn disabled_shared() -> Arc<Self> {
        static RECORDER: OnceLock<Arc<BlobMetricsRecorder>> = OnceLock::new();
        Arc::clone(RECORDER.get_or_init(|| Arc::new(Self::disabled())))
    }

    fn index(
        operation: BlobOperation,
        object: BlobObjectClass,
        result: BlobResultClass,
        retryable: bool,
        backend: BlobBackendKind,
    ) -> usize {
        (((operation as usize * OBJECT_COUNT + object as usize) * RESULT_COUNT + result as usize)
            * RETRYABLE_COUNT
            + usize::from(retryable))
            * BACKEND_COUNT
            + backend as usize
    }

    fn begin(&self) -> InFlightGuard<'_> {
        debug_assert!(self.is_enabled());
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_in_flight.fetch_max(current, Ordering::Relaxed);
        InFlightGuard { recorder: self }
    }

    fn record(&self, event: Event) {
        let bucket = &self.buckets.as_ref().expect("enabled recorder")[Self::index(
            event.operation,
            event.object,
            event.result,
            event.retryable,
            event.backend,
        )];
        bucket.completions.fetch_add(1, Ordering::Relaxed);
        bucket.attempts.fetch_add(event.attempts, Ordering::Relaxed);
        bucket.retries.fetch_add(event.retries, Ordering::Relaxed);
        bucket
            .request_bytes
            .fetch_add(event.request_bytes, Ordering::Relaxed);
        bucket
            .response_bytes
            .fetch_add(event.response_bytes, Ordering::Relaxed);
        bucket
            .latency_ns
            .fetch_add(event.latency_ns, Ordering::Relaxed);
        bucket
            .errors
            .fetch_add(u64::from(event.error), Ordering::Relaxed);
        bucket
            .throttles
            .fetch_add(u64::from(event.throttled), Ordering::Relaxed);
        bucket
            .timeouts
            .fetch_add(u64::from(event.timeout), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BlobMetricsSnapshot {
        let mut rows = Vec::new();
        for operation in ALL_OPERATIONS {
            for object_class in ALL_OBJECT_CLASSES {
                for result in ALL_RESULTS {
                    for retryable in [false, true] {
                        for backend in ALL_BACKENDS {
                            let Some(buckets) = self.buckets.as_ref() else {
                                continue;
                            };
                            let bucket = &buckets
                                [Self::index(operation, object_class, result, retryable, backend)];
                            let values = BlobMetricValues {
                                completions: bucket.completions.load(Ordering::Relaxed),
                                attempts: bucket.attempts.load(Ordering::Relaxed),
                                retries: bucket.retries.load(Ordering::Relaxed),
                                request_bytes: bucket.request_bytes.load(Ordering::Relaxed),
                                response_bytes: bucket.response_bytes.load(Ordering::Relaxed),
                                latency_ns: bucket.latency_ns.load(Ordering::Relaxed),
                                errors: bucket.errors.load(Ordering::Relaxed),
                                throttles: bucket.throttles.load(Ordering::Relaxed),
                                timeouts: bucket.timeouts.load(Ordering::Relaxed),
                            };
                            if !values.is_zero() {
                                rows.push(BlobMetricRow {
                                    operation,
                                    object_class,
                                    result,
                                    retryable,
                                    backend,
                                    values,
                                });
                            }
                        }
                    }
                }
            }
        }
        BlobMetricsSnapshot {
            rows,
            in_flight: self.in_flight.load(Ordering::Relaxed),
            peak_in_flight: self.peak_in_flight.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_protocol<T>(
        &self,
        operation: BlobOperation,
        backend: BlobBackendKind,
        object: BlobObjectClass,
        attempts: u64,
        elapsed: std::time::Duration,
        result: &EngineResult<T>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let (result_class, retryable, error, throttled, timeout) = match result {
            Ok(_) => (BlobResultClass::Success, false, false, false, false),
            Err(error) => {
                let fault = BlobStoreFault::from_engine_error(error);
                (
                    fault.result,
                    fault.retryable,
                    true,
                    fault.throttled,
                    fault.timeout,
                )
            }
        };
        self.record(Event {
            operation,
            object,
            result: result_class,
            retryable,
            backend,
            attempts: attempts.max(1),
            retries: attempts.saturating_sub(1),
            request_bytes: 0,
            response_bytes: 0,
            latency_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            error,
            throttled,
            timeout,
        });
    }

    /// Record logical segment validation without inventing another physical
    /// object-store attempt or duplicating GET byte accounting.
    pub(crate) fn record_segment_validation<T>(
        &self,
        backend: BlobBackendKind,
        result: &EngineResult<T>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let (result_class, retryable, throttled, timeout, error) = match result {
            Ok(_) => (BlobResultClass::Success, false, false, false, false),
            Err(error) => {
                let fault = BlobStoreFault::from_engine_error(error);
                (
                    fault.result,
                    fault.retryable,
                    fault.throttled,
                    fault.timeout,
                    true,
                )
            }
        };
        self.record(Event {
            operation: BlobOperation::ValidateSegment,
            object: BlobObjectClass::Segment,
            result: result_class,
            retryable,
            backend,
            attempts: 0,
            retries: 0,
            request_bytes: 0,
            response_bytes: 0,
            latency_ns: 0,
            error,
            throttled,
            timeout,
        });
    }
}

const ALL_OPERATIONS: [BlobOperation; OP_COUNT] = [
    BlobOperation::Put,
    BlobOperation::PutIfAbsent,
    BlobOperation::Get,
    BlobOperation::Delete,
    BlobOperation::List,
    BlobOperation::ListPage,
    BlobOperation::Stats,
    BlobOperation::ReadManifestHead,
    BlobOperation::UpdateManifestHead,
    BlobOperation::AcquireEpoch,
    BlobOperation::FenceEpoch,
    BlobOperation::Branch,
    BlobOperation::ValidateSegment,
];
const ALL_OBJECT_CLASSES: [BlobObjectClass; OBJECT_COUNT] = [
    BlobObjectClass::Segment,
    BlobObjectClass::Manifest,
    BlobObjectClass::ManifestHead,
    BlobObjectClass::Epoch,
    BlobObjectClass::RetentionFloor,
    BlobObjectClass::DeletionWatermark,
    BlobObjectClass::BranchPin,
    BlobObjectClass::Other,
];
const ALL_RESULTS: [BlobResultClass; RESULT_COUNT] = [
    BlobResultClass::Success,
    BlobResultClass::NotFound,
    BlobResultClass::PreconditionLost,
    BlobResultClass::Throttled,
    BlobResultClass::Timeout,
    BlobResultClass::Transport,
    BlobResultClass::Corrupt,
    BlobResultClass::OtherError,
];
const ALL_BACKENDS: [BlobBackendKind; BACKEND_COUNT] = [
    BlobBackendKind::Memory,
    BlobBackendKind::LocalFs,
    BlobBackendKind::S3,
    BlobBackendKind::Other,
];

struct InFlightGuard<'a> {
    recorder: &'a BlobMetricsRecorder,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.recorder.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Event {
    operation: BlobOperation,
    object: BlobObjectClass,
    result: BlobResultClass,
    retryable: bool,
    backend: BlobBackendKind,
    attempts: u64,
    retries: u64,
    request_bytes: u64,
    response_bytes: u64,
    latency_ns: u64,
    error: bool,
    throttled: bool,
    timeout: bool,
}

pub struct InstrumentedBlobStore<S> {
    inner: S,
    recorder: Arc<BlobMetricsRecorder>,
    reported_recorder: Arc<BlobMetricsRecorder>,
    backend: BlobBackendKind,
}

impl<S> InstrumentedBlobStore<S> {
    pub fn new(inner: S, recorder: Arc<BlobMetricsRecorder>, _backend: BlobBackendKind) -> Self
    where
        S: BlobStore,
    {
        // The store is authoritative. Retain the argument for source compatibility with scoped
        // recorder call sites, but never allow a caller-provided label to misclassify provider work.
        let backend = inner.backend_kind();
        if inner.instrumentation_depth() == 0 {
            Self {
                inner,
                reported_recorder: Arc::clone(&recorder),
                recorder,
                backend,
            }
        } else {
            let reported_recorder = inner
                .instrumentation_recorder()
                .unwrap_or_else(BlobMetricsRecorder::disabled_shared);
            Self {
                inner,
                recorder: BlobMetricsRecorder::disabled_shared(),
                reported_recorder,
                backend,
            }
        }
    }

    pub fn effective_recorder(&self) -> &Arc<BlobMetricsRecorder> {
        &self.reported_recorder
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Avoids double attribution if a public generic constructor is handed an already instrumented store.
    pub(crate) fn production(inner: S, _backend: BlobBackendKind) -> Self
    where
        S: BlobStore,
    {
        let backend = inner.backend_kind();
        let (recorder, reported_recorder) = if inner.instrumentation_depth() == 0 {
            let recorder = BlobMetricsRecorder::production_shared();
            (Arc::clone(&recorder), recorder)
        } else {
            (
                BlobMetricsRecorder::disabled_shared(),
                inner
                    .instrumentation_recorder()
                    .unwrap_or_else(BlobMetricsRecorder::disabled_shared),
            )
        };
        Self {
            inner,
            recorder,
            reported_recorder,
            backend,
        }
    }
}

impl<S: BlobStore> InstrumentedBlobStore<S> {
    fn observe_classified<T>(
        &self,
        operation: BlobOperation,
        object: BlobObjectClass,
        call: impl FnOnce(&S) -> ClassifiedBlobResult<ObservedBlobCall<T>>,
    ) -> ClassifiedBlobResult<ObservedBlobCall<T>> {
        if !self.recorder.is_enabled() {
            return call(&self.inner);
        }
        let _in_flight = self.recorder.begin();
        let started = Instant::now();
        let result = call(&self.inner);
        let latency_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        match &result {
            Ok(observed) => self.recorder.record(Event {
                operation,
                object,
                result: BlobResultClass::Success,
                retryable: false,
                backend: self.backend,
                attempts: observed.attempts,
                retries: 0,
                request_bytes: observed.request_bytes,
                response_bytes: observed.response_bytes,
                latency_ns,
                error: false,
                throttled: false,
                timeout: false,
            }),
            Err(error) => self.recorder.record(Event {
                operation,
                object,
                result: error.fault.result,
                retryable: error.fault.retryable,
                backend: self.backend,
                attempts: error.attempts.max(1),
                retries: 0,
                request_bytes: error.request_bytes,
                response_bytes: error.response_bytes,
                latency_ns,
                error: true,
                throttled: error.fault.throttled,
                timeout: error.fault.timeout,
            }),
        }
        result
    }

    fn observe<T>(
        &self,
        operation: BlobOperation,
        object: BlobObjectClass,
        _request_bytes: u64,
        physical: bool,
        call: impl FnOnce(&S) -> ClassifiedBlobResult<(T, Outcome)>,
    ) -> EngineResult<T> {
        if !self.recorder.is_enabled() {
            return call(&self.inner)
                .map(|(value, _)| value)
                .map_err(|error| error.outward);
        }
        let _in_flight = physical.then(|| self.recorder.begin());
        let started = Instant::now();
        let result = call(&self.inner);
        let latency_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        match result {
            Ok((value, outcome)) => {
                self.recorder.record(Event {
                    operation,
                    object,
                    result: outcome.result,
                    retryable: false,
                    backend: self.backend,
                    attempts: outcome.attempts,
                    retries: outcome.retries,
                    request_bytes: outcome.request_bytes,
                    response_bytes: outcome.response_bytes,
                    latency_ns,
                    error: false,
                    throttled: false,
                    timeout: false,
                });
                Ok(value)
            }
            Err(error) => {
                let fault = error.fault;
                self.recorder.record(Event {
                    operation,
                    object,
                    result: fault.result,
                    retryable: fault.retryable,
                    backend: self.backend,
                    attempts: error.attempts.max(1),
                    retries: 0,
                    request_bytes: error.request_bytes,
                    response_bytes: error.response_bytes,
                    latency_ns,
                    error: true,
                    throttled: fault.throttled,
                    timeout: fault.timeout,
                });
                Err(error.outward)
            }
        }
    }

    fn list_counted(
        &self,
        prefix: &str,
        call: impl FnOnce(&S) -> ClassifiedBlobResult<ObservedBlobCall<Vec<String>>>,
    ) -> EngineResult<(Vec<String>, u64)> {
        self.observe(
            BlobOperation::List,
            BlobObjectClass::from_key(prefix),
            0,
            true,
            |inner| {
                let call = call(inner)?;
                let attempts = call.attempts;
                Ok((
                    (call.value, attempts),
                    Outcome::success(attempts, call.request_bytes, call.response_bytes),
                ))
            },
        )
    }

    fn observe_logical<T>(
        &self,
        operation: BlobOperation,
        object: BlobObjectClass,
        call: impl FnOnce() -> ClassifiedBlobResult<(T, Outcome)>,
    ) -> EngineResult<T> {
        self.observe(operation, object, 0, false, |_| call())
    }
}

struct Outcome {
    result: BlobResultClass,
    attempts: u64,
    retries: u64,
    response_bytes: u64,
    request_bytes: u64,
}

impl Outcome {
    fn success(attempts: u64, request_bytes: u64, response_bytes: u64) -> Self {
        Self {
            result: BlobResultClass::Success,
            attempts,
            retries: 0,
            response_bytes,
            request_bytes,
        }
    }
}

impl<S: BlobStore> BlobStore for InstrumentedBlobStore<S> {
    fn backend_kind(&self) -> BlobBackendKind {
        self.backend
    }

    fn instrumentation_depth(&self) -> u8 {
        self.inner.instrumentation_depth().saturating_add(1)
    }

    fn instrumentation_recorder(&self) -> Option<Arc<BlobMetricsRecorder>> {
        Some(Arc::clone(&self.reported_recorder))
    }

    fn observed_get(&self, key: &str) -> ClassifiedBlobResult<ObservedBlobCall<Option<Vec<u8>>>> {
        self.observe_classified(
            BlobOperation::Get,
            BlobObjectClass::from_key(key),
            |inner| inner.observed_get(key),
        )
    }

    fn observed_delete(&self, key: &str) -> ClassifiedBlobResult<ObservedBlobCall<bool>> {
        self.observe_classified(
            BlobOperation::Delete,
            BlobObjectClass::from_key(key),
            |inner| inner.observed_delete(key),
        )
    }

    fn observed_list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> ClassifiedBlobResult<ObservedBlobCall<Vec<String>>> {
        self.observe_classified(
            BlobOperation::ListPage,
            BlobObjectClass::from_key(prefix),
            |inner| inner.observed_list_page(prefix, start_after, limit),
        )
    }
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.observe(
            BlobOperation::Put,
            BlobObjectClass::from_key(key),
            body.len() as u64,
            true,
            |inner| {
                inner.observed_put(key, body).map(|call| {
                    let outcome =
                        Outcome::success(call.attempts, call.request_bytes, call.response_bytes);
                    (call.value, outcome)
                })
            },
        )
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        self.observe(
            BlobOperation::PutIfAbsent,
            BlobObjectClass::from_key(key),
            body.len() as u64,
            true,
            |inner| {
                inner.observed_put_if_absent(key, body).map(|call| {
                    let result = if call.value {
                        BlobResultClass::Success
                    } else {
                        BlobResultClass::PreconditionLost
                    };
                    (
                        call.value,
                        Outcome {
                            result,
                            attempts: call.attempts,
                            retries: 0,
                            response_bytes: call.response_bytes,
                            request_bytes: call.request_bytes,
                        },
                    )
                })
            },
        )
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        self.observe(
            BlobOperation::Get,
            BlobObjectClass::from_key(key),
            0,
            true,
            |inner| {
                inner.observed_get(key).map(|call| {
                    let outcome = match call.value.as_ref() {
                        Some(_) => {
                            Outcome::success(call.attempts, call.request_bytes, call.response_bytes)
                        }
                        None => Outcome {
                            result: BlobResultClass::NotFound,
                            attempts: call.attempts,
                            retries: 0,
                            response_bytes: call.response_bytes,
                            request_bytes: call.request_bytes,
                        },
                    };
                    (call.value, outcome)
                })
            },
        )
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        self.observe(
            BlobOperation::Delete,
            BlobObjectClass::from_key(key),
            0,
            true,
            |inner| {
                inner.observed_delete(key).map(|call| {
                    let result = if call.value {
                        BlobResultClass::Success
                    } else {
                        BlobResultClass::NotFound
                    };
                    (
                        call.value,
                        Outcome {
                            result,
                            attempts: call.attempts,
                            retries: 0,
                            response_bytes: call.response_bytes,
                            request_bytes: call.request_bytes,
                        },
                    )
                })
            },
        )
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.list_counted(prefix, |inner| {
            inner.observed_list_with_request_count(prefix)
        })
        .map(|(keys, _)| keys)
    }

    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        self.list_counted(prefix, |inner| {
            inner.observed_list_with_request_count(prefix)
        })
    }

    fn list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        self.list_counted(prefix, |inner| {
            inner.observed_list_from_with_request_count(prefix, start_after)
        })
        .map(|(keys, _)| keys)
    }

    fn list_from_with_request_count(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> EngineResult<(Vec<String>, u64)> {
        self.list_counted(prefix, |inner| {
            inner.observed_list_from_with_request_count(prefix, start_after)
        })
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        if limit == 0 {
            return self.observe(
                BlobOperation::ListPage,
                BlobObjectClass::from_key(prefix),
                0,
                false,
                |_| Ok((Vec::new(), Outcome::success(0, 0, 0))),
            );
        }
        self.observe(
            BlobOperation::ListPage,
            BlobObjectClass::from_key(prefix),
            0,
            true,
            |inner| {
                inner
                    .observed_list_page(prefix, start_after, limit)
                    .map(|call| {
                        let outcome = Outcome::success(
                            call.attempts,
                            call.request_bytes,
                            call.response_bytes,
                        );
                        (call.value, outcome)
                    })
            },
        )
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.observe(
            BlobOperation::Stats,
            BlobObjectClass::from_key(prefix),
            0,
            false,
            |inner| {
                inner.observed_stats(prefix).map(|call| {
                    let outcome =
                        Outcome::success(call.attempts, call.request_bytes, call.response_bytes);
                    (call.value, outcome)
                })
            },
        )
    }

    fn read_manifest_head(
        &self,
        prefix: &str,
    ) -> EngineResult<Option<crate::segmented::VersionedHead<crate::segmented::ManifestHeadBlob>>>
    {
        self.observe_logical(
            BlobOperation::ReadManifestHead,
            BlobObjectClass::from_key(prefix),
            || {
                crate::segmented::read_manifest_head_via(self, prefix)
                    .map_err(|error| ClassifiedBlobError::fallback(self, error))
                    .map(|value| (value, Outcome::success(1, 0, 0)))
            },
        )
    }

    fn update_manifest_head_if_version(
        &self,
        prefix: &str,
        expected_version: Option<u64>,
        value: &crate::segmented::ManifestHeadBlob,
    ) -> EngineResult<bool> {
        self.observe_logical(
            BlobOperation::UpdateManifestHead,
            BlobObjectClass::from_key(prefix),
            || {
                crate::segmented::update_manifest_head_via(self, prefix, expected_version, value)
                    .map_err(|error| ClassifiedBlobError::fallback(self, error))
                    .map(|updated| {
                        let result = if updated {
                            BlobResultClass::Success
                        } else {
                            BlobResultClass::PreconditionLost
                        };
                        (
                            updated,
                            Outcome {
                                result,
                                attempts: 1,
                                retries: 0,
                                response_bytes: 0,
                                request_bytes: 0,
                            },
                        )
                    })
            },
        )
    }

    fn classify_fault(&self, error: &EngineError) -> BlobStoreFault {
        self.inner.classify_fault(error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use crate::segmented::InMemoryBlobStore;

    fn values(
        recorder: &BlobMetricsRecorder,
        operation: BlobOperation,
        object: BlobObjectClass,
        result: BlobResultClass,
        retryable: bool,
    ) -> BlobMetricValues {
        recorder.snapshot().row(
            operation,
            object,
            result,
            retryable,
            BlobBackendKind::Memory,
        )
    }

    #[test]
    fn primitive_calls_record_result_bytes_and_attempts_once() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        );

        store.put("segments/one", b"abc").unwrap();
        assert!(!store.put_if_absent("segments/one", b"later").unwrap());
        assert_eq!(store.get("segments/one").unwrap(), Some(b"abc".to_vec()));
        assert_eq!(store.get("segments/missing").unwrap(), None);
        assert!(store.delete("segments/one").unwrap());
        assert!(!store.delete("segments/one").unwrap());

        let put = values(
            &recorder,
            BlobOperation::Put,
            BlobObjectClass::Segment,
            BlobResultClass::Success,
            false,
        );
        assert_eq!(
            (put.completions, put.attempts, put.request_bytes),
            (1, 1, 3)
        );
        let precondition = values(
            &recorder,
            BlobOperation::PutIfAbsent,
            BlobObjectClass::Segment,
            BlobResultClass::PreconditionLost,
            false,
        );
        assert_eq!(precondition.completions, 1);
        assert_eq!(precondition.errors, 0);
        assert_eq!(precondition.request_bytes, 5);
        let get = values(
            &recorder,
            BlobOperation::Get,
            BlobObjectClass::Segment,
            BlobResultClass::Success,
            false,
        );
        assert_eq!((get.completions, get.response_bytes), (1, 3));
        assert_eq!(
            values(
                &recorder,
                BlobOperation::Get,
                BlobObjectClass::Segment,
                BlobResultClass::NotFound,
                false,
            )
            .errors,
            0
        );
        assert_eq!(
            values(
                &recorder,
                BlobOperation::Delete,
                BlobObjectClass::Segment,
                BlobResultClass::NotFound,
                false,
            )
            .completions,
            1
        );
        assert_eq!(recorder.snapshot().in_flight, 0);
    }

    struct CountedListStore {
        calls: AtomicU64,
    }

    impl BlobStore for CountedListStore {
        fn backend_kind(&self) -> BlobBackendKind {
            BlobBackendKind::Memory
        }
        fn put(&self, _: &str, _: &[u8]) -> EngineResult<()> {
            unreachable!()
        }
        fn put_if_absent(&self, _: &str, _: &[u8]) -> EngineResult<bool> {
            unreachable!()
        }
        fn get(&self, _: &str) -> EngineResult<Option<Vec<u8>>> {
            unreachable!()
        }
        fn delete(&self, _: &str) -> EngineResult<bool> {
            unreachable!()
        }
        fn list(&self, _: &str) -> EngineResult<Vec<String>> {
            panic!("instrumented list must use the counted provider method")
        }
        fn list_page(&self, _: &str, _: Option<&str>, _: usize) -> EngineResult<Vec<String>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec!["unexpected".into()])
        }
        fn list_with_request_count(&self, _: &str) -> EngineResult<(Vec<String>, u64)> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok((vec!["manifest/a".into(), "manifest/bb".into()], 3))
        }
        fn list_from_with_request_count(
            &self,
            _: &str,
            _: &str,
        ) -> EngineResult<(Vec<String>, u64)> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok((vec!["manifest/bb".into()], 2))
        }
    }

    #[test]
    fn list_counts_physical_pages_without_double_counting_aggregate_latency() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let inner = CountedListStore {
            calls: AtomicU64::new(0),
        };
        let store =
            InstrumentedBlobStore::new(inner, Arc::clone(&recorder), BlobBackendKind::Memory);
        assert_eq!(store.list("manifest/").unwrap().len(), 2);
        assert_eq!(store.list_from("manifest/", "manifest/a").unwrap().len(), 1);
        assert_eq!(store.inner.calls.load(Ordering::Relaxed), 2);
        let row = values(
            &recorder,
            BlobOperation::List,
            BlobObjectClass::Manifest,
            BlobResultClass::Success,
            false,
        );
        assert_eq!(row.completions, 2);
        assert_eq!(row.attempts, 5);
        assert_eq!(row.retries, 0);
        assert_eq!(
            row.response_bytes, 0,
            "generic LIST wire payload is unknowable"
        );
    }

    #[test]
    fn zero_limit_list_page_records_zero_attempts_and_never_calls_provider() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            CountedListStore {
                calls: AtomicU64::new(0),
            },
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        );
        assert!(store.list_page("manifest/", None, 0).unwrap().is_empty());
        assert_eq!(store.inner.calls.load(Ordering::Relaxed), 0);
        let row = values(
            &recorder,
            BlobOperation::ListPage,
            BlobObjectClass::Manifest,
            BlobResultClass::Success,
            false,
        );
        assert_eq!((row.completions, row.attempts, row.retries), (1, 0, 0));
    }

    struct FaultStore;

    impl BlobStore for FaultStore {
        fn backend_kind(&self) -> BlobBackendKind {
            BlobBackendKind::Memory
        }
        fn classify_fault(&self, _: &EngineError) -> BlobStoreFault {
            BlobStoreFault::new(BlobResultClass::Throttled, true, true, false)
        }
        fn put(&self, _: &str, _: &[u8]) -> EngineResult<()> {
            Err(EngineError::Storage(
                "unbounded hostile provider text".into(),
            ))
        }
        fn put_if_absent(&self, _: &str, _: &[u8]) -> EngineResult<bool> {
            unreachable!()
        }
        fn get(&self, _: &str) -> EngineResult<Option<Vec<u8>>> {
            unreachable!()
        }
        fn delete(&self, _: &str) -> EngineResult<bool> {
            unreachable!()
        }
        fn list(&self, _: &str) -> EngineResult<Vec<String>> {
            unreachable!()
        }
    }

    #[test]
    fn structured_fault_drives_bounded_error_throttle_and_retryable_labels() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store =
            InstrumentedBlobStore::new(FaultStore, Arc::clone(&recorder), BlobBackendKind::Memory);
        assert!(store.put("totally/hostile/key", b"body").is_err());
        let row = values(
            &recorder,
            BlobOperation::Put,
            BlobObjectClass::Other,
            BlobResultClass::Throttled,
            true,
        );
        assert_eq!(row.completions, 1);
        assert_eq!(row.errors, 1);
        assert_eq!(row.throttles, 1);
        assert_eq!(row.timeouts, 0);
        assert_eq!(row.request_bytes, 4);
    }

    #[test]
    fn legacy_storage_error_does_not_fabricate_transport_or_retry_semantics() {
        let fault = BlobStoreFault::from_engine_error(&EngineError::Storage("anything".into()));
        assert_eq!(fault.result, BlobResultClass::OtherError);
        assert!(!fault.retryable);
        assert!(!fault.throttled);
        assert!(!fault.timeout);
    }

    #[test]
    fn typed_segment_corruption_is_one_logical_zero_attempt_observation() {
        let recorder = BlobMetricsRecorder::new();
        let error: EngineResult<()> = Err(EngineError::DurableDataCorrupt {
            stage: pqueue_engine::DurableIntegrityStage::FrameCrc32c,
            manifest_index: 7,
            locator: "0123456789abcdef".to_owned(),
        });
        recorder.record_segment_validation(BlobBackendKind::Memory, &error);
        let row = values(
            &recorder,
            BlobOperation::ValidateSegment,
            BlobObjectClass::Segment,
            BlobResultClass::Corrupt,
            false,
        );
        assert_eq!(
            (
                row.completions,
                row.attempts,
                row.request_bytes,
                row.response_bytes
            ),
            (1, 0, 0, 0)
        );
    }

    #[test]
    fn production_object_namespaces_have_exact_bounded_classes() {
        let cases = [
            (
                "t/aa/q/bb/seg_candidates/e000/i000/s000-dead.seg",
                BlobObjectClass::Segment,
            ),
            (
                "t/aa/q/bb/seg_attempt/e000/i000/s000-1-2.seg",
                BlobObjectClass::Segment,
            ),
            (
                "t/aa/q/bb/branch-seg/e000/s000.seg",
                BlobObjectClass::Segment,
            ),
            (
                "t/aa/q/bb/manifest/00000000000000000001.json",
                BlobObjectClass::Manifest,
            ),
            (
                "t/aa/q/bb/manifest_candidates/dead.json",
                BlobObjectClass::Manifest,
            ),
            (
                "t/aa/q/bb/manifest_head/00000000000000000001.json",
                BlobObjectClass::ManifestHead,
            ),
            (
                "t/aa/q/bb/authority_head/00000000000000000001.json",
                BlobObjectClass::ManifestHead,
            ),
            (
                "t/aa/q/bb/authority_protocol_v1",
                BlobObjectClass::ManifestHead,
            ),
            (
                "t/aa/q/bb/manifest_head/00000000000000000001~watermark.json",
                BlobObjectClass::DeletionWatermark,
            ),
            (
                "t/aa/q/bb/read_horizon.json",
                BlobObjectClass::DeletionWatermark,
            ),
            ("t/aa/q/bb/branches/cc/dd.json", BlobObjectClass::BranchPin),
            ("t/aa/q/bb/branch.json", BlobObjectClass::BranchPin),
            ("t/aa/q/bb/branch.pending", BlobObjectClass::BranchPin),
        ];
        for (key, expected) in cases {
            assert_eq!(BlobObjectClass::from_key(key), expected, "{key}");
        }
    }

    #[test]
    fn snapshots_produce_monotonic_deltas() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        );
        let before = recorder.snapshot();
        store.put("manifest/1", b"first").unwrap();
        let middle = recorder.snapshot();
        store.put("manifest/2", b"second").unwrap();
        let delta = recorder.snapshot().delta(&middle);
        let row = delta.row(
            BlobOperation::Put,
            BlobObjectClass::Manifest,
            BlobResultClass::Success,
            false,
            BlobBackendKind::Memory,
        );
        assert_eq!((row.completions, row.request_bytes), (1, 6));
        assert!(before.rows.is_empty());
    }

    #[test]
    fn hostile_keys_collapse_to_one_fixed_label_tuple() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        );
        for n in 0..100 {
            store
                .put(&format!("tenant-{n}/key-{n}?error={n}"), b"x")
                .unwrap();
        }
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.rows.len(), 1);
        let row = snapshot.rows[0];
        assert_eq!(row.object_class, BlobObjectClass::Other);
        assert_eq!(row.values.completions, 100);
        assert_eq!(row.values.request_bytes, 100);
    }

    struct BlockingStore {
        entered: Mutex<Option<mpsc::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl BlobStore for BlockingStore {
        fn backend_kind(&self) -> BlobBackendKind {
            BlobBackendKind::Memory
        }
        fn put(&self, _: &str, _: &[u8]) -> EngineResult<()> {
            if let Some(sender) = self.entered.lock().unwrap().take() {
                sender.send(()).unwrap();
            }
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        }
        fn put_if_absent(&self, _: &str, _: &[u8]) -> EngineResult<bool> {
            unreachable!()
        }
        fn get(&self, _: &str) -> EngineResult<Option<Vec<u8>>> {
            unreachable!()
        }
        fn delete(&self, _: &str) -> EngineResult<bool> {
            unreachable!()
        }
        fn list(&self, _: &str) -> EngineResult<Vec<String>> {
            unreachable!()
        }
    }

    #[test]
    fn in_flight_exposes_a_hung_provider_call() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let store = Arc::new(InstrumentedBlobStore::new(
            BlockingStore {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(release_rx),
            },
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        ));
        let worker_store = Arc::clone(&store);
        let worker = thread::spawn(move || worker_store.put("segments/hung", b"x").unwrap());
        entered_rx.recv().unwrap();
        assert_eq!(recorder.snapshot().in_flight, 1);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.peak_in_flight, 1);
    }

    fn manifest_head() -> crate::segmented::ManifestHeadBlob {
        crate::segmented::ManifestHeadBlob {
            current_epoch: 1,
            next_seq: 2,
            next_manifest_index: 3,
            retention_floor_through: None,
            tail_candidate_key: None,
            legacy_next_manifest_index: 0,
        }
    }

    #[test]
    fn composite_head_spans_use_instrumented_primitives_without_double_counting() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&recorder),
            BlobBackendKind::Memory,
        );
        let prefix = "authority_head/";
        assert!(
            store
                .update_manifest_head_if_version(prefix, None, &manifest_head())
                .unwrap()
        );
        assert_eq!(
            store.read_manifest_head(prefix).unwrap().unwrap().version,
            0
        );

        let snapshot = recorder.snapshot();
        let row = |operation, result| {
            snapshot.row(
                operation,
                BlobObjectClass::ManifestHead,
                result,
                false,
                BlobBackendKind::Memory,
            )
        };
        assert_eq!(
            row(BlobOperation::UpdateManifestHead, BlobResultClass::Success).completions,
            1
        );
        // One read inside update plus the explicit read.
        assert_eq!(
            row(BlobOperation::ReadManifestHead, BlobResultClass::Success).completions,
            2
        );
        assert_eq!(
            row(BlobOperation::List, BlobResultClass::Success).completions,
            2
        );
        assert_eq!(
            row(BlobOperation::PutIfAbsent, BlobResultClass::Success).completions,
            1
        );
        // Empty first read plus one GET for the explicit read: no provider composite bypass and no duplicate.
        assert_eq!(
            row(BlobOperation::Get, BlobResultClass::Success).completions,
            1
        );
        assert_eq!(
            snapshot.peak_in_flight, 1,
            "logical composite nesting is not physical in-flight work"
        );
    }

    #[test]
    fn stats_is_one_logical_span_and_does_not_reclassify_provider_introspection() {
        let inner = InMemoryBlobStore::new();
        inner.put("manifest/one", b"abc").unwrap();
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store =
            InstrumentedBlobStore::new(inner, Arc::clone(&recorder), BlobBackendKind::Memory);
        assert_eq!(store.stats("manifest/").unwrap().object_count, 1);
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(snapshot.rows[0].operation, BlobOperation::Stats);
        assert_eq!(snapshot.rows[0].values.completions, 1);
        assert_eq!((snapshot.in_flight, snapshot.peak_in_flight), (0, 0));
    }

    #[test]
    fn backend_identity_is_derived_from_the_provider() {
        let recorder = Arc::new(BlobMetricsRecorder::new());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&recorder),
            BlobBackendKind::S3,
        );
        assert_eq!(store.backend_kind(), BlobBackendKind::Memory);
        store.put("manifest/one", b"x").unwrap();
        assert_eq!(recorder.snapshot().rows[0].backend, BlobBackendKind::Memory);
    }

    #[test]
    fn disabled_and_nested_wrappers_are_transparent_and_attribute_once() {
        let disabled = Arc::new(BlobMetricsRecorder::disabled());
        let store = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&disabled),
            BlobBackendKind::Memory,
        );
        store.put("manifest/one", b"x").unwrap();
        assert!(disabled.snapshot().rows.is_empty());

        let enabled = Arc::new(BlobMetricsRecorder::new());
        let inner = InstrumentedBlobStore::new(
            InMemoryBlobStore::new(),
            Arc::clone(&enabled),
            BlobBackendKind::Memory,
        );
        let ignored_outer = Arc::new(BlobMetricsRecorder::new());
        let outer =
            InstrumentedBlobStore::new(inner, Arc::clone(&ignored_outer), BlobBackendKind::Memory);
        outer.put("manifest/two", b"yy").unwrap();
        let row = enabled.snapshot().row(
            BlobOperation::Put,
            BlobObjectClass::Manifest,
            BlobResultClass::Success,
            false,
            BlobBackendKind::Memory,
        );
        assert_eq!(row.completions, 1);
        assert!(Arc::ptr_eq(outer.effective_recorder(), &enabled));
        assert!(ignored_outer.snapshot().rows.is_empty());
    }

    #[test]
    fn protocol_iterations_record_one_completion_and_explicit_retries() {
        let recorder = BlobMetricsRecorder::new();
        recorder.record_protocol(
            BlobOperation::FenceEpoch,
            BlobBackendKind::Memory,
            BlobObjectClass::ManifestHead,
            4,
            std::time::Duration::from_micros(7),
            &Ok::<_, EngineError>(9),
        );
        recorder.record_protocol(
            BlobOperation::Branch,
            BlobBackendKind::Memory,
            BlobObjectClass::BranchPin,
            2,
            std::time::Duration::from_micros(3),
            &Err::<(), _>(EngineError::Conflict),
        );
        let snapshot = recorder.snapshot();
        let fence = snapshot.row(
            BlobOperation::FenceEpoch,
            BlobObjectClass::ManifestHead,
            BlobResultClass::Success,
            false,
            BlobBackendKind::Memory,
        );
        assert_eq!(
            (fence.completions, fence.attempts, fence.retries),
            (1, 4, 3)
        );
        let branch = snapshot.row(
            BlobOperation::Branch,
            BlobObjectClass::BranchPin,
            BlobResultClass::PreconditionLost,
            true,
            BlobBackendKind::Memory,
        );
        assert_eq!(
            (branch.completions, branch.attempts, branch.retries),
            (1, 2, 1)
        );
        assert_eq!(branch.errors, 1);
        assert_eq!(snapshot.physical_totals(), BlobPhysicalTotals::default());
    }
}
