#![cfg(feature = "s3")]
//! Live S3-compatible adapter + engine tests.
//!
//! Runs only when credentials + endpoint are configured; otherwise skips and
//! passes so default CI stays hermetic.
//!
//! ```text
//! OBJECT_LOG_S3_ENDPOINT=http://127.0.0.1:3900 \
//! OBJECT_LOG_S3_BUCKET=object-log-evidence \
//! OBJECT_LOG_S3_KEY_ID=… \
//! OBJECT_LOG_S3_SECRET=… \
//! OBJECT_LOG_S3_REGION=garage \
//!   cargo test --features s3 --test s3 -- --nocapture
//! ```
//!
//! Optional label for evidence logs: `OBJECT_LOG_S3_PROVIDER=garage|minio|aws|r2`.
//!
//! Legacy aliases: `FJORD_GARAGE_*`.

use bytes::Bytes;
use object_log::{
    BlobStore, Durability, FlushConfig, LogEngine, ManifestSequencer, PartitionKey, S3BlobStore,
};
use std::sync::Arc;
use std::time::Duration;

struct S3Env {
    endpoint: String,
    bucket: String,
    key_id: String,
    secret: String,
    region: String,
    provider: String,
}

fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

fn load_env() -> Option<S3Env> {
    let endpoint = env_first(&["OBJECT_LOG_S3_ENDPOINT", "FJORD_GARAGE_ENDPOINT"])?;
    let bucket = env_first(&["OBJECT_LOG_S3_BUCKET", "FJORD_GARAGE_BUCKET"])?;
    let key_id = env_first(&["OBJECT_LOG_S3_KEY_ID", "FJORD_GARAGE_KEY_ID"])?;
    let secret = env_first(&["OBJECT_LOG_S3_SECRET", "FJORD_GARAGE_SECRET"])?;
    let region = env_first(&["OBJECT_LOG_S3_REGION", "FJORD_GARAGE_REGION"])
        .unwrap_or_else(|| "us-east-1".into());
    let provider = env_first(&["OBJECT_LOG_S3_PROVIDER"]).unwrap_or_else(|| {
        if endpoint.contains("3900") {
            "garage?".into()
        } else if endpoint.contains("9000") || endpoint.contains("19000") {
            "minio?".into()
        } else if endpoint.contains("r2.cloudflarestorage.com") {
            "r2?".into()
        } else if endpoint.contains("amazonaws.com") {
            "aws?".into()
        } else {
            "unknown".into()
        }
    });
    Some(S3Env {
        endpoint,
        bucket,
        key_id,
        secret,
        region,
        provider,
    })
}

fn store(env: &S3Env) -> S3BlobStore {
    S3BlobStore::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        &env.key_id,
        &env.secret,
    )
}

fn banner(env: &S3Env, test: &str) {
    eprintln!(
        "s3 evidence: test={test} provider={} endpoint={} bucket={} region={}",
        env.provider, env.endpoint, env.bucket, env.region
    );
}

fn skip() {
    eprintln!("OBJECT_LOG_S3_* (or FJORD_GARAGE_*) not fully set — skipping live S3 tests");
}

#[tokio::test]
async fn s3_blob_store_round_trip() {
    let Some(env) = load_env() else {
        skip();
        return;
    };
    banner(&env, "s3_blob_store_round_trip");
    let store = store(&env);

    let key = format!(
        "object-log-test/{}/{}-roundtrip",
        env.provider,
        std::process::id()
    );
    store
        .put(&key, Bytes::from_static(b"hello world"))
        .await
        .unwrap();
    assert_eq!(
        store.get(&key).await.unwrap().unwrap(),
        Bytes::from_static(b"hello world")
    );
    assert_eq!(
        store.get_range(&key, 6..11).await.unwrap().unwrap(),
        Bytes::from_static(b"world")
    );
    let prefix = format!("object-log-test/{}/", env.provider);
    assert!(store.list(&prefix).await.unwrap().iter().any(|k| k == &key));
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
}

/// Large put forces multipart when threshold is lowered below payload size.
#[tokio::test]
async fn s3_multipart_put_get_range_round_trip() {
    let Some(env) = load_env() else {
        skip();
        return;
    };
    banner(&env, "s3_multipart_put_get_range_round_trip");
    // 6 MiB payload; threshold 1 MiB, part size 5 MiB (S3 minimum).
    let store = store(&env).with_multipart(1024 * 1024, 5 * 1024 * 1024);
    let key = format!(
        "object-log-test/{}/{}-multipart",
        env.provider,
        std::process::id()
    );

    let mut payload = vec![0u8; 6 * 1024 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let payload = Bytes::from(payload);

    store.put(&key, payload.clone()).await.unwrap();
    let got = store.get(&key).await.unwrap().unwrap();
    assert_eq!(got.len(), payload.len());
    assert_eq!(got, payload);

    let mid = store
        .get_range(&key, 1000..1000 + 64)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid, payload.slice(1000..1000 + 64));

    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.unwrap().is_none());
}

/// Full engine path: produce → ManifestSequencer → fetch over S3.
#[tokio::test]
async fn s3_engine_produce_fetch_round_trip() {
    let Some(env) = load_env() else {
        skip();
        return;
    };
    banner(&env, "s3_engine_produce_fetch_round_trip");
    let blob: Arc<dyn BlobStore> = Arc::new(store(&env));
    let pid = std::process::id();
    let data_prefix = format!("object-log-test/{}/engine-{pid}/log/", env.provider);
    let manifest_prefix = format!("object-log-test/{}/engine-{pid}/manifest/", env.provider);

    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), manifest_prefix)
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        blob,
        seq,
        FlushConfig {
            max_batches: 1,
            linger: Duration::from_secs(3600),
            budget: object_log::BudgetConfig {
                enabled: false,
                ..Default::default()
            },
            ..FlushConfig::default()
        },
        data_prefix,
    );

    let p = PartitionKey(format!("ev-{pid}"));
    for msg in [b"alpha" as &[u8], b"beta", b"gamma"] {
        engine
            .produce(
                p.clone(),
                Bytes::copy_from_slice(msg),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    }
    let batches = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].payload, "alpha");
    assert_eq!(batches[1].payload, "beta");
    assert_eq!(batches[2].payload, "gamma");
    assert_eq!(batches[2].base_offset, 2);
}
