//! Object-log storage-generation detection (FWSG → LogEngine).
//!
//! v0.24.0 deleted the in-tree segmented FWSG substrate and cut over to crates.io
//! `object_log::LogEngine`. On-disk FWSG objects are **not** readable by LogEngine.
//! Open paths fail closed with a stable, matchable [`EngineError::Storage`] message
//! rather than undefined reads — see
//! `docs/operator/object-log-storage-generation.md`.

use std::sync::Arc;

use fireweed_engine::{EngineError, EngineResult};
use object_log::BlobStore;

/// Stable token embedded in open-time storage errors for the retired FWSG layout.
///
/// Consumers (e.g. Snorri) may match this substring on `EngineError::Storage` to
/// distinguish an intentional generation mismatch from generic I/O failures.
pub const INCOMPATIBLE_OBJECT_LOG_GENERATION: &str = "INCOMPATIBLE_OBJECT_LOG_GENERATION";

/// Stable token when both LogEngine (`fwlog`/`fwmeta`) and FWSG layout markers coexist.
pub const MIXED_OBJECT_LOG_GENERATION: &str = "MIXED_OBJECT_LOG_GENERATION";

/// Magic bytes at the start of a sealed FWSG segment object.
pub const FWSG_SEGMENT_MAGIC: &[u8; 4] = b"FWSG";

/// Operator-facing documentation path (relative to repo root).
pub const STORAGE_GENERATION_DOC: &str = "docs/operator/object-log-storage-generation.md";

/// True when `err` is the documented incompatible / mixed generation open error.
pub fn is_incompatible_generation_error(err: &EngineError) -> bool {
    match err {
        EngineError::Storage(msg) => {
            msg.contains(INCOMPATIBLE_OBJECT_LOG_GENERATION)
                || msg.contains(MIXED_OBJECT_LOG_GENERATION)
        }
        _ => false,
    }
}

/// Derive the blob key prefix that should be scanned for generation markers.
///
/// LogEngine product opens use data/meta prefixes ending in `fwlog/` and `fwmeta/`.
/// FWSG keys live beside those under the same namespace parent
/// (`t/{tenant_hex}/q/{queue_hex}/…`).
pub fn generation_scan_prefix(data_prefix: &str, meta_prefix: &str) -> String {
    let strip_fwlog = |p: &str| p.strip_suffix("fwlog/").map(str::to_owned);
    let strip_fwmeta = |p: &str| p.strip_suffix("fwmeta/").map(str::to_owned);
    match (strip_fwlog(data_prefix), strip_fwmeta(meta_prefix)) {
        (Some(a), Some(b)) if a == b => a,
        (Some(a), _) => a,
        (_, Some(b)) => b,
        _ => String::new(),
    }
}

/// Key-shape heuristics for the retired FWSG substrate (TD-004 object layout).
pub fn key_looks_like_fwsg(key: &str) -> bool {
    if key.contains("/seg_candidates/")
        || key.contains("/branch-seg/")
        || key.contains("/manifest_head/")
        || key.contains("/authority_head/")
        || key.contains("/seg_attempt/")
        || key.ends_with("/authority_protocol_v1")
        || key.ends_with("/authority_initialized_v1")
        || key.ends_with("/read_horizon.json")
        || key.ends_with("/branch.json")
        || key.ends_with("/branch.pending")
        || key.ends_with(".seg")
    {
        return true;
    }
    // Queue shard tree: `t/{hex}/q/{hex}/…` (and optional namespace parent).
    looks_like_fwsg_shard_tree(key)
}

fn looks_like_fwsg_shard_tree(key: &str) -> bool {
    // Accept optional leading path components (namespace hex) before `t/`.
    let Some(t_at) = key.find("t/") else {
        return false;
    };
    let rest = &key[t_at..];
    let mut parts = rest.split('/');
    // t / tenant_hex / q / queue_hex / …
    if parts.next() != Some("t") {
        return false;
    }
    let Some(tenant) = parts.next() else {
        return false;
    };
    if tenant.is_empty() || !tenant.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts.next() != Some("q") {
        return false;
    }
    let Some(queue) = parts.next() else {
        return false;
    };
    if queue.is_empty() || !queue.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    // Require at least one more path component (manifest, seg_candidates, …).
    parts.next().is_some()
}

/// True when a key is under the LogEngine product prefixes for this open.
pub fn key_looks_like_log_engine(key: &str, data_prefix: &str, meta_prefix: &str) -> bool {
    key.starts_with(data_prefix) || key.starts_with(meta_prefix)
}

fn incompatible_err(detail: &str) -> EngineError {
    EngineError::Storage(format!(
        "{INCOMPATIBLE_OBJECT_LOG_GENERATION}: retired FWSG object-log layout detected ({detail}). \
         LogEngine (v0.24+) cannot open this data; regenerate under a fresh prefix/root. \
         See {STORAGE_GENERATION_DOC}"
    ))
}

fn mixed_err(detail: &str) -> EngineError {
    EngineError::Storage(format!(
        "{MIXED_OBJECT_LOG_GENERATION}: both LogEngine (fwlog/fwmeta) and retired FWSG markers \
         present ({detail}). Refuse open to avoid undefined reads; isolate generations. \
         See {STORAGE_GENERATION_DOC}"
    ))
}

/// Fail closed if the blob namespace contains retired FWSG layout markers.
///
/// - Pure FWSG data → [`INCOMPATIBLE_OBJECT_LOG_GENERATION`]
/// - FWSG markers coexisting with LogEngine keys → [`MIXED_OBJECT_LOG_GENERATION`]
/// - Empty / LogEngine-only → `Ok(())`
pub async fn reject_incompatible_storage_generation(
    blob: &Arc<dyn BlobStore>,
    data_prefix: &str,
    meta_prefix: &str,
) -> EngineResult<()> {
    let scan = generation_scan_prefix(data_prefix, meta_prefix);
    // Prefer a bounded LIST under the FWSG tenant tree; fall back to a wider scan.
    let tenant_prefix = format!("{scan}t/");
    let mut keys = blob.list(&tenant_prefix).await.map_err(store_err)?;
    if keys.is_empty() {
        // Wider scan catches `.seg` objects or non-`t/` residue under the namespace.
        keys = blob.list(&scan).await.map_err(store_err)?;
    }

    let mut fwsg_hit: Option<String> = None;

    for key in keys {
        if key_looks_like_log_engine(&key, data_prefix, meta_prefix) {
            continue;
        }
        if key_looks_like_fwsg(&key) {
            // Prefer magic confirmation for `.seg` objects when present.
            if key.ends_with(".seg")
                && let Some(bytes) = blob.get(&key).await.map_err(store_err)?
            {
                if bytes.len() >= 4 && &bytes[..4] == FWSG_SEGMENT_MAGIC.as_slice() {
                    fwsg_hit = Some(format!("FWSG magic in object key shape …{}", tail(&key)));
                    break;
                }
                // Non-magic `.seg` still indicates old layout naming.
                fwsg_hit = Some(format!(".seg key without LogEngine prefix …{}", tail(&key)));
                break;
            }
            fwsg_hit = Some(format!("FWSG key shape …{}", tail(&key)));
            break;
        }
    }

    let Some(detail) = fwsg_hit else {
        return Ok(());
    };

    // Probe LogEngine presence separately so a tenant-tree LIST still classifies mixed roots.
    let log_engine_hit = has_log_engine_keys(blob, data_prefix, meta_prefix).await?;
    if log_engine_hit {
        Err(mixed_err(&detail))
    } else {
        Err(incompatible_err(&detail))
    }
}

async fn has_log_engine_keys(
    blob: &Arc<dyn BlobStore>,
    data_prefix: &str,
    meta_prefix: &str,
) -> EngineResult<bool> {
    if !blob.list(meta_prefix).await.map_err(store_err)?.is_empty() {
        return Ok(true);
    }
    if !blob.list(data_prefix).await.map_err(store_err)?.is_empty() {
        return Ok(true);
    }
    Ok(false)
}

fn tail(key: &str) -> &str {
    const MAX: usize = 96;
    if key.len() <= MAX {
        key
    } else {
        &key[key.len() - MAX..]
    }
}

fn store_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_log::{FlushConfig, LocalBlobStore, MemoryBlobStore};

    #[test]
    fn scan_prefix_strips_fw_prefixes() {
        assert_eq!(generation_scan_prefix("fwlog/", "fwmeta/"), "");
        assert_eq!(
            generation_scan_prefix("deadbeef/fwlog/", "deadbeef/fwmeta/"),
            "deadbeef/"
        );
    }

    #[test]
    fn fwsg_key_shapes_detected() {
        assert!(key_looks_like_fwsg(
            "t/7465/q/71/seg_candidates/e00000000000000000001/i00000000000000000000/s00000000000000000000-abcd.seg"
        ));
        assert!(key_looks_like_fwsg(
            "t/aa/q/bb/manifest_head/00000000000000000001"
        ));
        assert!(key_looks_like_fwsg("ns/t/aa/q/bb/authority_protocol_v1"));
        assert!(!key_looks_like_fwsg("fwmeta/catalog.json"));
        assert!(!key_looks_like_fwsg("fwlog/part-0"));
    }

    #[tokio::test]
    async fn empty_root_opens_cleanly_via_reject() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        reject_incompatible_storage_generation(&blob, "fwlog/", "fwmeta/")
            .await
            .expect("empty store is fine");
    }

    #[tokio::test]
    async fn fwsg_segment_magic_fails_open_detectably() {
        let dir = std::env::temp_dir().join(format!(
            "fwsg-gen-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(&dir));
        let key = "t/7465/q/71/seg_candidates/e00000000000000000001/i00000000000000000000/s00000000000000000000-deadbeef.seg";
        let mut body = FWSG_SEGMENT_MAGIC.to_vec();
        body.extend_from_slice(&[2u8, 0, 0, 0, 0, 0, 0, 0, 0]); // version + padding
        blob.put(key, bytes::Bytes::from(body)).await.unwrap();

        let err = reject_incompatible_storage_generation(&blob, "fwlog/", "fwmeta/")
            .await
            .expect_err("FWSG residue must fail closed");
        assert!(
            is_incompatible_generation_error(&err),
            "expected detectable generation error, got {err:?}"
        );
        match &err {
            EngineError::Storage(msg) => {
                assert!(
                    msg.contains(INCOMPATIBLE_OBJECT_LOG_GENERATION),
                    "msg={msg}"
                );
                assert!(msg.contains(STORAGE_GENERATION_DOC), "msg={msg}");
            }
            other => panic!("expected Storage error, got {other:?}"),
        }

        // Product open must surface the same failure.
        match crate::ObjectLogEngineStore::open_local(
            &dir,
            FlushConfig {
                linger: std::time::Duration::ZERO,
                ..FlushConfig::default()
            },
        )
        .await
        {
            Ok(_) => panic!("open_local must reject FWSG root"),
            Err(open_err) => {
                assert!(
                    is_incompatible_generation_error(&open_err),
                    "open_local err={open_err:?}"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn mixed_fwsg_and_logengine_fails_with_mixed_token() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        blob.put(
            "fwmeta/catalog.json",
            bytes::Bytes::from_static(b"{\"definitions\":[]}"),
        )
        .await
        .unwrap();
        blob.put(
            "t/aa/q/bb/authority_protocol_v1",
            bytes::Bytes::from_static(b"authority-head-v1"),
        )
        .await
        .unwrap();

        let err = reject_incompatible_storage_generation(&blob, "fwlog/", "fwmeta/")
            .await
            .expect_err("mixed generation must fail");
        match &err {
            EngineError::Storage(msg) => {
                assert!(msg.contains(MIXED_OBJECT_LOG_GENERATION), "msg={msg}");
            }
            other => panic!("expected Storage error, got {other:?}"),
        }
        assert!(is_incompatible_generation_error(&err));
    }
}
