//! S3 create-only (put-if-absent) publication for queue-definition authority.
//!
//! The crates.io `object_log::BlobStore` port exposes overwrite-only `put`. Fireweed
//! needs enforced create-only for immutable per-queue definition objects under
//! `NativeConditionalWrite`. This module issues `PutObject` with `If-None-Match: *`
//! against the same endpoint/credentials used for the log blob store.
//!
//! Endpoint must actually reject a second create (HTTP 412). P1s-qualified MinIO
//! does; Garage v2.2.0 does not and remains unsupported.

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    timeout::TimeoutConfig,
};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use fireweed_engine::{EngineError, EngineResult};
use std::time::Duration;

/// PutObject + If-None-Match:* publisher for one bucket.
pub struct S3CreateOnlyPut {
    client: Client,
    bucket: String,
}

impl S3CreateOnlyPut {
    /// Build a path-style S3 client matching `object_log::S3BlobStore` defaults.
    pub fn new(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Self {
        let creds = Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "fireweed-objectlog-create-only",
        );
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint_url)
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(env_u64(
                        "OBJECT_LOG_S3_CONNECT_TIMEOUT_SECS",
                        5,
                    )))
                    .read_timeout(Duration::from_secs(env_u64(
                        "OBJECT_LOG_S3_READ_TIMEOUT_SECS",
                        10,
                    )))
                    .operation_timeout(Duration::from_secs(env_u64(
                        "OBJECT_LOG_S3_OPERATION_TIMEOUT_SECS",
                        30,
                    )))
                    .build(),
            )
            .build();
        Self {
            client: Client::from_conf(conf),
            bucket: bucket.to_string(),
        }
    }

    /// Create-only put. `Ok(true)` = this call created the key; `Ok(false)` = key
    /// already existed (412 Precondition Failed or concurrent 409 after which the
    /// object is present). Other errors map to [`EngineError::Storage`].
    pub async fn put_if_absent(&self, key: &str, value: Bytes) -> EngineResult<bool> {
        // Retry a small bound of 409 ConditionalRequestConflict races.
        for attempt in 0..4u8 {
            let result = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .if_none_match("*")
                .body(ByteStream::from(value.clone()))
                .send()
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(err) => match classify_put_error(&err) {
                    PutClassify::AlreadyExists => return Ok(false),
                    PutClassify::ConflictRetry if attempt + 1 < 4 => continue,
                    PutClassify::ConflictRetry => {
                        // Last attempt still raced; treat as collision if object exists
                        // after re-read path (caller re-gets the key).
                        return Ok(false);
                    }
                    PutClassify::Other => {
                        return Err(EngineError::Storage(format!(
                            "S3 create-only PutObject (If-None-Match: *) failed for key {key}: {err}"
                        )));
                    }
                },
            }
        }
        Ok(false)
    }

    /// Best-effort delete (probe cleanup). Errors are ignored by the probe caller.
    pub async fn delete_object(&self, key: &str) -> EngineResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| {
                EngineError::Storage(format!("S3 DeleteObject failed for key {key}: {err}"))
            })?;
        Ok(())
    }

    /// Prove the endpoint **enforces** create-only: first put creates, second put must
    /// not create (412 / already-exists). Fail closed if the second put succeeds as a
    /// create (non-enforcing stores such as Garage v2.2.0).
    ///
    /// `probe_key` must be unique per open and under the meta prefix so it never collides
    /// with production objects. Best-effort deletes the probe key afterward.
    pub async fn probe_enforced_create_only(&self, probe_key: &str) -> EngineResult<()> {
        let payload = Bytes::from_static(b"fireweed-create-only-probe-v1");
        let first = self.put_if_absent(probe_key, payload.clone()).await?;
        if !first {
            // Key already present from a crashed prior probe; still require second put to
            // report already-exists rather than a second successful create.
            let second = self.put_if_absent(probe_key, payload.clone()).await?;
            if second {
                let _ = self.delete_object(probe_key).await;
                return Err(EngineError::Storage(
                    "S3 create-only probe failed: second PutObject with If-None-Match: * \
                     created an object that already existed; endpoint does not enforce \
                     conditional create (unsupported for NativeConditionalWrite, e.g. Garage \
                     v2.2.0). Use a P1s-qualified endpoint (MinIO/AWS S3)."
                        .into(),
                ));
            }
            let _ = self.delete_object(probe_key).await;
            return Ok(());
        }
        let second = self.put_if_absent(probe_key, payload).await?;
        if second {
            let _ = self.delete_object(probe_key).await;
            return Err(EngineError::Storage(
                "S3 create-only probe failed: second PutObject with If-None-Match: * \
                 reported create success for an existing key; endpoint does not enforce \
                 conditional create (unsupported for NativeConditionalWrite, e.g. Garage \
                 v2.2.0). Use a P1s-qualified endpoint (MinIO/AWS S3)."
                    .into(),
            ));
        }
        let _ = self.delete_object(probe_key).await;
        Ok(())
    }
}

enum PutClassify {
    AlreadyExists,
    ConflictRetry,
    Other,
}

fn classify_put_error(err: &SdkError<PutObjectError>) -> PutClassify {
    if let SdkError::ServiceError(ctx) = err {
        let status = ctx.raw().status().as_u16();
        if status == 412 {
            return PutClassify::AlreadyExists;
        }
        if status == 409 {
            return PutClassify::ConflictRetry;
        }
    }
    let text = err.to_string().to_ascii_lowercase();
    if text.contains("precondition") || text.contains("412") {
        return PutClassify::AlreadyExists;
    }
    if text.contains("conditionalrequestconflict") || text.contains("409") {
        return PutClassify::ConflictRetry;
    }
    PutClassify::Other
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_minio_probe_enforced_create_only_when_env_set() {
        let Ok(endpoint) = std::env::var("FIREWEED_S3_TEST_ENDPOINT") else {
            eprintln!("SKIP live create-only probe: FIREWEED_S3_TEST_ENDPOINT unset");
            return;
        };
        let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").expect("bucket");
        let region =
            std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").expect("access");
        let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY").expect("secret");
        let put = S3CreateOnlyPut::new(&endpoint, &region, &bucket, &access, &secret);
        let key = format!(
            "fwmeta/create-only-probe-test/{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        put.probe_enforced_create_only(&key)
            .await
            .expect("P1s MinIO must enforce If-None-Match create-only");
    }
}
