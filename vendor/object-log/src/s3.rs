//! S3-compatible [`BlobStore`] adapter (AWS S3, MinIO, self-hosted Garage).
//!
//! Enabled by the `s3` cargo feature. Uses path-style addressing and static
//! credentials. `put` is durable-on-return (an S3 `PutObject` 200 means the
//! object is durably stored) and dispatches multipart upload above a size
//! threshold so large coalesced objects are not a single whole-buffer PUT.

use crate::{BlobStore, MediaOpStats, ObjectLogError};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    timeout::TimeoutConfig,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::{Bytes, BytesMut};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn unavailable<E: std::fmt::Display>(e: E) -> ObjectLogError {
    ObjectLogError::StorageUnavailable(e.to_string())
}

/// S3-compatible blob store.
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    /// Objects larger than this use multipart upload.
    multipart_threshold: usize,
    /// Multipart part size.
    part_size: usize,
    /// Use SigV4's UNSIGNED-PAYLOAD mode for PUT/UploadPart requests.
    disable_payload_signing: bool,
    media_ops: AtomicU64,
    bytes_written: AtomicU64,
}

impl S3BlobStore {
    /// Build a store for an S3-compatible endpoint with static credentials and
    /// path-style addressing (required by Garage/MinIO).
    pub fn new(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Self {
        let creds = Credentials::new(access_key_id, secret_access_key, None, None, "object-log");
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
            multipart_threshold: 16 * 1024 * 1024,
            part_size: 8 * 1024 * 1024,
            disable_payload_signing: env_flag("OBJECT_LOG_S3_DISABLE_PAYLOAD_SIGNING"),
            media_ops: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    fn record_media(&self, ops: u64, bytes: u64) {
        self.media_ops.fetch_add(ops, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Override the multipart threshold and part size (both in bytes).
    pub fn with_multipart(mut self, threshold: usize, part_size: usize) -> Self {
        self.multipart_threshold = threshold;
        self.part_size = part_size.max(5 * 1024 * 1024); // S3 minimum part size
        self
    }

    /// Disable SigV4 payload hashing for PUT/UploadPart requests.
    ///
    /// This sends `x-amz-content-sha256: UNSIGNED-PAYLOAD`, which is useful for
    /// trusted S3-compatible deployments where TLS/private-network integrity is
    /// sufficient and per-byte SHA-256 signing is the throughput bottleneck.
    pub fn with_payload_signing_disabled(mut self, disabled: bool) -> Self {
        self.disable_payload_signing = disabled;
        self
    }

    async fn put_multipart(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(unavailable)?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| ObjectLogError::StorageUnavailable("missing upload id".into()))?
            .to_string();

        let byte_len = value.len() as u64;
        match self.upload_parts(key, &upload_id, &value).await {
            Ok(part_count) => {
                // create + N parts + complete
                self.record_media(2 + part_count, byte_len);
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                // create (+ abort best-effort); still billable attempt noise
                self.record_media(2, 0);
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        value: &Bytes,
    ) -> Result<u64, ObjectLogError> {
        let mut parts = Vec::new();
        let mut offset = 0usize;
        let mut part_number = 1i32;
        while offset < value.len() {
            let end = (offset + self.part_size).min(value.len());
            let chunk = value.slice(offset..end);
            let request = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk));
            let uploaded = if self.disable_payload_signing {
                request.customize().disable_payload_signing().send().await
            } else {
                request.send().await
            }
            .map_err(unavailable)?;
            parts.push(
                CompletedPart::builder()
                    .set_e_tag(uploaded.e_tag().map(str::to_string))
                    .part_number(part_number)
                    .build(),
            );
            part_number += 1;
            offset = end;
        }
        let part_count = parts.len() as u64;
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(unavailable)?;
        Ok(part_count)
    }

    async fn upload_one_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        chunk: Bytes,
    ) -> Result<CompletedPart, ObjectLogError> {
        let request = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk));
        let uploaded = if self.disable_payload_signing {
            request.customize().disable_payload_signing().send().await
        } else {
            request.send().await
        }
        .map_err(unavailable)?;
        Ok(CompletedPart::builder()
            .set_e_tag(uploaded.e_tag().map(str::to_string))
            .part_number(part_number)
            .build())
    }

    async fn put_multipart_chunks(
        &self,
        key: &str,
        chunks: Vec<Bytes>,
    ) -> Result<(), ObjectLogError> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(unavailable)?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| ObjectLogError::StorageUnavailable("missing upload id".into()))?
            .to_string();

        let byte_len: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        match self.upload_chunk_parts(key, &upload_id, chunks).await {
            Ok(part_count) => {
                self.record_media(2 + part_count, byte_len);
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                self.record_media(2, 0);
                Err(e)
            }
        }
    }

    async fn upload_chunk_parts(
        &self,
        key: &str,
        upload_id: &str,
        chunks: Vec<Bytes>,
    ) -> Result<u64, ObjectLogError> {
        let mut parts = Vec::new();
        let mut part_number = 1i32;
        let mut pending = BytesMut::with_capacity(self.part_size);

        for chunk in chunks {
            let mut offset = 0usize;
            while offset < chunk.len() {
                if pending.is_empty() && chunk.len() - offset >= self.part_size {
                    let end = offset + self.part_size;
                    parts.push(
                        self.upload_one_part(key, upload_id, part_number, chunk.slice(offset..end))
                            .await?,
                    );
                    part_number += 1;
                    offset = end;
                    continue;
                }

                let take = (self.part_size - pending.len()).min(chunk.len() - offset);
                pending.extend_from_slice(&chunk.slice(offset..offset + take));
                offset += take;

                if pending.len() == self.part_size {
                    parts.push(
                        self.upload_one_part(key, upload_id, part_number, pending.freeze())
                            .await?,
                    );
                    part_number += 1;
                    pending = BytesMut::with_capacity(self.part_size);
                }
            }
        }

        if !pending.is_empty() {
            parts.push(
                self.upload_one_part(key, upload_id, part_number, pending.freeze())
                    .await?,
            );
        }

        let part_count = parts.len() as u64;
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(unavailable)?;
        Ok(part_count)
    }
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        if value.len() > self.multipart_threshold {
            return self.put_multipart(key, value).await;
        }
        let byte_len = value.len() as u64;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(value));
        if self.disable_payload_signing {
            request.customize().disable_payload_signing().send().await
        } else {
            request.send().await
        }
        .map_err(unavailable)?;
        self.record_media(1, byte_len);
        Ok(())
    }

    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        Some(MediaOpStats {
            media_ops: self.media_ops.swap(0, Ordering::Relaxed),
            bytes: self.bytes_written.swap(0, Ordering::Relaxed),
        })
    }

    async fn put_chunks(&self, key: &str, chunks: Vec<Bytes>) -> Result<(), ObjectLogError> {
        let total: usize = chunks.iter().map(Bytes::len).sum();
        if total > self.multipart_threshold {
            return self.put_multipart_chunks(key, chunks).await;
        }
        match chunks.len() {
            0 => self.put(key, Bytes::new()).await,
            1 => self.put(key, chunks.into_iter().next().unwrap()).await,
            _ => {
                let mut value = BytesMut::with_capacity(total);
                for chunk in chunks {
                    value.extend_from_slice(&chunk);
                }
                self.put(key, value.freeze()).await
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => {
                let data = resp.body.collect().await.map_err(unavailable)?;
                Ok(Some(data.into_bytes()))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(unavailable(svc))
                }
            }
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        if range.start > range.end {
            return Err(ObjectLogError::RangeOutOfBounds(format!(
                "start {} > end {}",
                range.start, range.end
            )));
        }
        if range.start == range.end {
            // S3 has no zero-length range; treat as an empty read (if the key exists).
            return match self.get(key).await? {
                Some(_) => Ok(Some(Bytes::new())),
                None => Ok(None),
            };
        }
        if std::env::var_os("OBJECT_LOG_S3_RANGE_FALLBACK").is_some() {
            let Some(bytes) = self.get(key).await? else {
                return Ok(None);
            };
            let start = range.start as usize;
            let end = range.end as usize;
            if start > bytes.len() || end > bytes.len() {
                return Err(ObjectLogError::RangeOutOfBounds(format!(
                    "{}..{} outside object length {}",
                    range.start,
                    range.end,
                    bytes.len()
                )));
            }
            return Ok(Some(bytes.slice(start..end)));
        }
        // HTTP byte ranges are inclusive: bytes=start-(end-1).
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(header)
            .send()
            .await
        {
            Ok(resp) => {
                let data = resp.body.collect().await.map_err(unavailable)?;
                Ok(Some(data.into_bytes()))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Ok(None)
                } else {
                    // A 416 (range past EOF) and other errors both surface here.
                    Err(unavailable(svc))
                }
            }
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let resp = req.send().await.map_err(unavailable)?;
            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            match resp.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        }
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}
