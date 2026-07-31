//! Bounded, vendor-neutral object-store telemetry recorded below protocol retries.
//!
//! Metric identity is deliberately represented by enums. Object keys and error
//! messages are data, never labels, so even hostile inputs cannot increase
//! cardinality.
//!
//! The retired in-tree `BlobStore` instrumenting wrapper was removed with the segmented
//! substrate; metric types and the recorder remain for adapters that still report store work.

#![allow(dead_code)] // recorder helpers retained for future LogEngine instrumentation

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use fireweed_engine::{EngineError, EngineResult};


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
            .is_some_and(|name| name.ends_with("~watermark.json"))
        {
            return Self::DeletionWatermark;
        }
        for component in key.split('/') {
            let class = match component {
                "segments" | "segment" | "seg_candidates" | "branch-seg" => Self::Segment,
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
                "deletion_watermark" | "deletion-watermark" => Self::DeletionWatermark,
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
    pub fn fallback(outward: EngineError) -> Self {
        let fault = BlobStoreFault::from_engine_error(&outward);
        Self {
            outward,
            fault,
            attempts: 1,
            request_bytes: 0,
            response_bytes: 0,
        }
    }

    pub fn with_fault(outward: EngineError, fault: BlobStoreFault) -> Self {
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
