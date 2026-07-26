use std::collections::BTreeMap;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use fireweed_objectlog::segmented::{BlobStore, S3BlobStore};
use fireweed_server::{Config, start};

const MINIO_IMAGE: &str =
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
const ACCESS_KEY: &str = "fireweed-production-access";
const SECRET_KEY: &str = "fireweed-production-secret";

struct DisposableMinio {
    name: String,
    endpoint: String,
}

impl DisposableMinio {
    fn start() -> Self {
        let name = format!("fireweed-production-s3-{}", std::process::id());
        let status = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &name,
                "--env",
                &format!("MINIO_ROOT_USER={ACCESS_KEY}"),
                "--env",
                &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
                MINIO_IMAGE,
                "server",
                "/data",
            ])
            .status()
            .expect("docker is required for the disposable MinIO acceptance test");
        assert!(status.success(), "docker run for disposable MinIO failed");

        let inspect = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &name,
            ])
            .output()
            .expect("inspect disposable MinIO");
        assert!(inspect.status.success(), "docker inspect MinIO failed");
        let ip = String::from_utf8(inspect.stdout)
            .expect("docker inspect IP is utf8")
            .trim()
            .to_string();
        assert!(!ip.is_empty(), "docker did not report a MinIO container IP");
        Self {
            name,
            endpoint: format!("http://{ip}:9000"),
        }
    }

    fn ensure_bucket(&self, bucket: &str) -> S3BlobStore {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let store =
                S3BlobStore::new(&self.endpoint, bucket, ACCESS_KEY, SECRET_KEY, "us-east-1")
                    .expect("valid MinIO client");
            match store.create_bucket() {
                Ok(()) => return store,
                Err(error) if Instant::now() < deadline => {
                    eprintln!("waiting for disposable MinIO: {error}");
                    thread::sleep(Duration::from_millis(250));
                }
                Err(error) => panic!("disposable MinIO did not become ready: {error}"),
            }
        }
    }
}

impl Drop for DisposableMinio {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .status();
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn production_s3_env(endpoint: &str, bucket: &str) -> BTreeMap<String, String> {
    map(&[
        ("FIREWEED_LOG_BACKEND", "objectlog"),
        ("FIREWEED_PROJECTION_BACKEND", "inmemory"),
        ("FIREWEED_OBJECT_LOG_STORE", "s3"),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", endpoint),
        ("FIREWEED_OBJECT_LOG_S3_BUCKET", bucket),
        ("FIREWEED_OBJECT_LOG_S3_REGION", "us-east-1"),
        ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", ACCESS_KEY),
        ("FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY", SECRET_KEY),
        ("FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP", "true"),
        ("FIREWEED_SEGMENT_TARGET_BYTES", "1048576"),
        ("FIREWEED_SEGMENT_MAX_LATENCY_MS", "5"),
        ("FIREWEED_LISTEN_ADDR", "127.0.0.1:0"),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_s3_object_log_config_builds_segmented_backend() {
    let minio = DisposableMinio::start();
    let bucket = format!("fireweed-production-{}", std::process::id());
    let store = minio.ensure_bucket(&bucket);

    let config = Config::from_env(&production_s3_env(&minio.endpoint, &bucket))
        .expect("production S3 env builds typed config");
    let server = start(config)
        .await
        .expect("production S3 config builds and starts segmented backend");
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut connection)
        .await
        .expect("write through production S3 segmented backend");

    let keys = store.list("").expect("list production MinIO bucket");
    assert!(
        keys.iter().any(|key| key.ends_with(".seg")),
        "server write must create a segmented object in shared storage: {keys:?}"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_authority_starts_on_s3_without_native_create_only() {
    let required = [
        "FIREWEED_S3_TEST_ENDPOINT",
        "FIREWEED_S3_TEST_BUCKET",
        "FIREWEED_S3_TEST_REGION",
        "FIREWEED_S3_TEST_ACCESS_KEY",
        "FIREWEED_S3_TEST_SECRET_KEY",
        "FIREWEED_PG_TEST_URL",
    ];
    let values: Option<Vec<_>> = required
        .iter()
        .map(|name| std::env::var(name).ok().map(|value| (*name, value)))
        .collect();
    let Some(values) = values else {
        eprintln!(
            "S3 + PostgreSQL authority integration skipped; set {}",
            required.join(", ")
        );
        return;
    };
    let lookup = |name: &str| {
        values
            .iter()
            .find_map(|(key, value)| (*key == name).then_some(value.as_str()))
            .expect("required live-test variable")
    };

    let endpoint = lookup("FIREWEED_S3_TEST_ENDPOINT");
    let bucket = lookup("FIREWEED_S3_TEST_BUCKET");
    let region = lookup("FIREWEED_S3_TEST_REGION");
    let access = lookup("FIREWEED_S3_TEST_ACCESS_KEY");
    let secret = lookup("FIREWEED_S3_TEST_SECRET_KEY");
    S3BlobStore::new(endpoint, bucket, access, secret, region)
        .expect("valid live S3 client")
        .create_bucket()
        .expect("create/ensure live S3 bucket");

    let postgres_url = lookup("FIREWEED_PG_TEST_URL").to_owned();
    let schema = format!("fireweed_s3_authority_{}", std::process::id());
    let setup_url = postgres_url.clone();
    let setup_schema = schema.clone();
    tokio::task::spawn_blocking(move || {
        let mut client = postgres::Client::connect(&setup_url, postgres::NoTls)
            .expect("connect live Postgres for schema setup");
        client
            .batch_execute(&format!("CREATE SCHEMA {setup_schema}"))
            .expect("create isolated authority schema");
    })
    .await
    .expect("authority schema setup task");
    let separator = if postgres_url.contains('?') { '&' } else { '?' };
    let isolated_postgres_url =
        format!("{postgres_url}{separator}options=-c%20search_path%3D{schema}");

    let mut environment = production_s3_env(endpoint, bucket);
    environment.insert("FIREWEED_OBJECT_LOG_S3_REGION".into(), region.into());
    environment.insert("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID".into(), access.into());
    environment.insert(
        "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY".into(),
        secret.into(),
    );
    environment.insert("FIREWEED_CONTROL_PLANE".into(), "postgres".into());
    environment.insert(
        "FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL".into(),
        isolated_postgres_url,
    );
    environment.insert(
        "FIREWEED_BOOTSTRAP_QUEUES".into(),
        format!("t1:garage-authority-{}", std::process::id()),
    );

    let config = Config::from_env(&environment).expect("live authority profile config");
    let server = start(config)
        .await
        .expect("PostgreSQL authority must allow startup on no-CAS S3");
    server.shutdown_and_drain(Duration::from_secs(5)).await;

    tokio::task::spawn_blocking(move || {
        let mut client = postgres::Client::connect(&postgres_url, postgres::NoTls)
            .expect("connect live Postgres for schema cleanup");
        client
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .expect("drop isolated authority schema");
    })
    .await
    .expect("authority schema cleanup task");
}

#[test]
fn production_s3_object_log_config_rejects_incomplete_credentials_and_local_fallback() {
    let complete = production_s3_env("http://127.0.0.1:9000", "fireweed");
    for missing in [
        "FIREWEED_OBJECT_LOG_S3_ENDPOINT",
        "FIREWEED_OBJECT_LOG_S3_BUCKET",
        "FIREWEED_OBJECT_LOG_S3_REGION",
        "FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE",
        "FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID",
        "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
    ] {
        let mut env = complete.clone();
        env.remove(missing);
        let Err(error) = Config::from_env(&env) else {
            panic!("incomplete S3 config must fail closed: missing {missing}");
        };
        assert!(error.0.contains(missing), "{}: {}", missing, error.0);
    }

    let local_fallback = map(&[
        ("FIREWEED_OBJECT_LOG_STORE", "local"),
        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "http://minio:9000"),
        ("FIREWEED_OBJECT_LOG_ROOT", "/tmp/would-silently-fallback"),
    ]);
    let Err(error) = Config::from_env(&local_fallback) else {
        panic!("S3-shaped config must not be ignored in favor of local files");
    };
    assert!(
        error
            .0
            .contains("refusing to ignore shared S3 configuration")
    );
}
