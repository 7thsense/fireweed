// Provenance: crates/fireweed/examples/queue_lifecycle.rs::open_production
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn open_production() -> QueueResult<Queue> {
    let secret = |name: &str| ConfigSecret::new(std::env::var(name).unwrap_or_default());
    let options = S3TursoOptions::new(
        S3LogOptions {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "work-queues".into(),
            namespace: "prod/mail".into(),
            access_key_id: secret("S3_ACCESS_KEY_ID"),
            secret_access_key: secret("S3_SECRET_ACCESS_KEY"),
            allow_insecure_http: false,
        },
        TursoCacheOptions {
            directory: "/var/lib/fireweed/cache".into(),
        },
        PostgresControlPlaneOptions {
            url: secret("DATABASE_URL"),
            schema: "fireweed".into(),
            instance_id: OwnerId::new("worker-1")
                .map_err(|e| QueueError::Invalid(e.to_string()))?,
            heartbeat_ttl_ms: 10_000,
            lease_ttl_ms: 30_000,
            ownership_clock: None,
        },
    );
    Queue::s3_turso(options, Arc::new(SystemClock)).await
}
