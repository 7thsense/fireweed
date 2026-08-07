//! S3 conditional writes for NativeConditionalWrite authority.
//!
//! The crates.io `object_log::BlobStore` port exposes overwrite-only `put`. Fireweed
//! needs:
//! - **Create-only** (`If-None-Match: *`) for immutable per-queue definition objects
//! - **Compare-and-swap** (`If-Match: <etag>`) for the durable emission cursor (P8cs)
//!
//! Both issue against the same endpoint/credentials used for the log blob store.
//! Endpoint must enforce preconditions (HTTP 412 on failure). P1s-qualified MinIO
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

/// S3 conditional PutObject/GetObject helper for one bucket (create-only + CAS).
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

    /// Get object body + ETag for native CAS (P8cs emission cursor).
    ///
    /// `None` means the key is absent (NoSuchKey / 404). Other failures map to Storage.
    pub async fn get_with_etag(&self, key: &str) -> EngineResult<Option<(Bytes, String)>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        match result {
            Ok(output) => {
                let etag = output
                    .e_tag()
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        EngineError::Storage(format!(
                            "S3 GetObject for key {key} returned no ETag (required for emission-cursor CAS)"
                        ))
                    })?;
                let body = output
                    .body
                    .collect()
                    .await
                    .map_err(|err| {
                        EngineError::Storage(format!(
                            "S3 GetObject body collect failed for key {key}: {err}"
                        ))
                    })?
                    .into_bytes();
                Ok(Some((body, etag)))
            }
            Err(err) => {
                if is_not_found(&err) {
                    Ok(None)
                } else {
                    Err(EngineError::Storage(format!(
                        "S3 GetObject failed for key {key}: {err}"
                    )))
                }
            }
        }
    }

    /// Conditional put: replace `key` only when its current ETag matches `expected_etag`.
    ///
    /// `Ok(true)` = this call won the CAS and wrote `value`.
    /// `Ok(false)` = precondition failed (412 / concurrent 409 after retries) — caller must
    /// re-read and retry. Other errors map to [`EngineError::Storage`].
    pub async fn put_if_match(
        &self,
        key: &str,
        value: Bytes,
        expected_etag: &str,
    ) -> EngineResult<bool> {
        for attempt in 0..4u8 {
            let result = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .if_match(expected_etag)
                .body(ByteStream::from(value.clone()))
                .send()
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(err) => match classify_put_error(&err) {
                    PutClassify::AlreadyExists => return Ok(false),
                    PutClassify::ConflictRetry if attempt + 1 < 4 => continue,
                    PutClassify::ConflictRetry => return Ok(false),
                    PutClassify::Other => {
                        return Err(EngineError::Storage(format!(
                            "S3 CAS PutObject (If-Match) failed for key {key}: {err}"
                        )));
                    }
                },
            }
        }
        Ok(false)
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

fn is_not_found<E>(err: &SdkError<E>) -> bool
where
    E: std::fmt::Display + std::fmt::Debug,
{
    if let SdkError::ServiceError(ctx) = err {
        let status = ctx.raw().status().as_u16();
        if status == 404 {
            return true;
        }
    }
    let text = err.to_string().to_ascii_lowercase();
    text.contains("nosuchkey")
        || text.contains("not found")
        || text.contains("404")
        || text.contains("notfound")
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
        let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT").expect(
            "FIREWEED_S3_TEST_ENDPOINT required for live create-only probe (fail-closed; no LOUD skip)",
        );
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
