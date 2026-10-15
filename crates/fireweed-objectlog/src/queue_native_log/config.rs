//! Pure native transport mapping. Fireweed wire bounds remain in the engine;
//! SDK types and physical framing assumptions remain in this adapter.
use fireweed_core::queue_contract::{QueueError, QueueLimits, QueueResult};
use fireweed_engine::queue_log_record::QueueLogRecordBounds;
use std::time::Duration;

pub(super) const RECORD_IDENTITY_BYTES: usize = 111;
// SDK OND2 base framing plus the authenticated V2 transition extension.
// Reserve the full coupled frame before allocating even on legacy read paths.
const DATA_FRAME_HEADER_BYTES: u64 = 24 + 224;
// Current SDK ONB2 framing: eighty-byte header plus SHA-256 trailer.
const BINDING_OVERHEAD_BYTES: u64 = 112;

/// Explicit adapter execution and byte limits for bounded queue-log composition.
#[derive(Clone, Copy, Debug)]
pub struct QueueNativeLogOptions {
    pub workers: usize,
    pub blocking_workers: usize,
    pub operations: usize,
    pub bytes: u64,
    pub version_bytes: usize,
    pub binding_bytes: usize,
    pub group_entries: usize,
    pub group_bytes: usize,
}
impl QueueNativeLogOptions {
    pub(super) fn sdk(self) -> object_log::NativeLimits {
        object_log::NativeLimits {
            workers: self.workers,
            blocking_workers: self.blocking_workers,
            operations: self.operations,
            bytes: self.bytes,
            version_bytes: self.version_bytes,
            binding_bytes: self.binding_bytes,
            group_entries: self.group_entries,
            group_bytes: self.group_bytes,
        }
    }
}

impl QueueNativeLogOptions {
    /// Derive physical ceilings without allocating the maximum payload or pool.
    /// A separate native ledger covers SDK-owned pages and in-flight buffers;
    /// it is neither the Root operation pool nor total HTTP/TLS resident memory.
    pub fn for_queue_limits(
        limits: &QueueLimits,
        operations: usize,
        workers: usize,
        version_bytes: usize,
        retained_bytes: Option<u64>,
    ) -> QueueResult<Self> {
        let invalid = || QueueError::Invalid("unsupported native queue configuration".into());
        let record = QueueLogRecordBounds::for_limits(limits)
            .map_err(|_| invalid())?
            .maximum();
        u32::try_from(record).map_err(|_| invalid())?;
        let frame = record
            .checked_add(RECORD_IDENTITY_BYTES as u64)
            .and_then(|n| n.checked_add(DATA_FRAME_HEADER_BYTES))
            .ok_or_else(invalid)?;
        let binding = limits
            .max_request_bytes
            .checked_add(24)
            .and_then(|n| n.checked_add(BINDING_OVERHEAD_BYTES))
            .ok_or_else(invalid)?;
        let bytes = match retained_bytes {
            Some(bytes) => bytes,
            None => frame
                .checked_mul(4)
                .and_then(|n| n.checked_add(2 << 20))
                .ok_or_else(invalid)?,
        };
        let group_bytes = usize::try_from(frame).map_err(|_| invalid())?;
        let binding_bytes = usize::try_from(binding).map_err(|_| invalid())?;
        isize::try_from(bytes).map_err(|_| invalid())?;
        if bytes == 0 {
            return Err(invalid());
        }
        Ok(Self {
            workers,
            blocking_workers: workers,
            operations,
            bytes,
            version_bytes,
            binding_bytes,
            group_entries: 1,
            group_bytes,
        })
    }
}

/// Borrowed connection inputs; deliberately lacks Debug so credentials and
/// endpoint userinfo cannot enter diagnostics through derived formatting.
pub struct QueueNativeS3Connection<'a> {
    pub endpoint: &'a str,
    pub region: &'a str,
    pub bucket: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
}

/// SDK-independent adapter transport configuration; credential values are
/// borrowed separately. Debug reveals certificate length, never certificate data.
#[derive(Clone)]
pub struct QueueNativeS3TransportOptions {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub operation_timeout: Duration,
    pub disable_payload_signing: bool,
    pub max_config_bytes: usize,
    pub additional_ca_pem: Option<bytes::Bytes>,
}
impl Default for QueueNativeS3TransportOptions {
    fn default() -> Self {
        let options = object_log::S3ClientOptions::default();
        Self {
            connect_timeout: options.connect_timeout,
            read_timeout: options.read_timeout,
            operation_timeout: options.operation_timeout,
            disable_payload_signing: options.disable_payload_signing,
            max_config_bytes: options.max_config_bytes,
            additional_ca_pem: options.additional_ca_pem,
        }
    }
}
impl std::fmt::Debug for QueueNativeS3TransportOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueNativeS3TransportOptions")
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("disable_payload_signing", &self.disable_payload_signing)
            .field("max_config_bytes", &self.max_config_bytes)
            .field(
                "additional_ca_pem_bytes",
                &self.additional_ca_pem.as_ref().map(bytes::Bytes::len),
            )
            .finish()
    }
}
impl QueueNativeS3TransportOptions {
    pub(super) fn sdk(self) -> object_log::S3ClientOptions {
        object_log::S3ClientOptions {
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            operation_timeout: self.operation_timeout,
            disable_payload_signing: self.disable_payload_signing,
            max_config_bytes: self.max_config_bytes,
            additional_ca_pem: self.additional_ca_pem,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_default_wire_domain_is_not_clamped_to_root_or_native_credit() {
        let limits = QueueLimits::default();
        let bounds = QueueLogRecordBounds::for_limits(&limits).unwrap();
        let options = QueueNativeLogOptions::for_queue_limits(&limits, 64, 2, 4096, None).unwrap();
        assert_eq!(options.group_bytes as u64, bounds.maximum() + 359);
        assert_eq!(options.binding_bytes as u64, limits.max_request_bytes + 136);
        assert!(options.group_bytes as u64 > 2 << 30);
        object_log::NativeLog::preflight_s3("pure-default-config", "bucket", options.sdk())
            .unwrap();
        let restricted =
            QueueNativeLogOptions::for_queue_limits(&limits, 64, 2, 4096, Some(1 << 20)).unwrap();
        assert_eq!(restricted.group_bytes, options.group_bytes);
        assert_eq!(restricted.binding_bytes, options.binding_bytes);
        assert!(
            object_log::NativeLog::preflight_s3("pure-default-config", "bucket", restricted.sdk())
                .is_err()
        );
        for limits in [
            QueueLimits {
                max_request_bytes: 512 << 20,
                ..QueueLimits::default()
            },
            QueueLimits {
                max_request_bytes: u64::MAX,
                ..QueueLimits::default()
            },
            QueueLimits {
                max_response_bytes: u64::MAX,
                ..QueueLimits::default()
            },
        ] {
            assert!(matches!(
                QueueNativeLogOptions::for_queue_limits(&limits, 64, 2, 4096, None),
                Err(QueueError::Invalid(_))
            ));
        }
        assert!(matches!(
            QueueNativeLogOptions::for_queue_limits(
                &QueueLimits::default(),
                64,
                2,
                4096,
                Some(u64::MAX)
            ),
            Err(QueueError::Invalid(_))
        ));
    }
}
