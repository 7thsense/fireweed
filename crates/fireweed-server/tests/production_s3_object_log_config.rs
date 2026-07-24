use std::collections::BTreeMap;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use fireweed_objectlog::segmented::{BlobStore, S3BlobStore};
use fireweed_server::{Config, start};

const MINIO_IMAGE: &str =
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
const ACCESS_KEY: &str = "pqueue-production-access";
const SECRET_KEY: &str = "pqueue-production-secret";

struct DisposableMinio {
    name: String,
    endpoint: String,
}

impl DisposableMinio {
    fn start() -> Self {
        let name = format!("pqueue-production-s3-{}", std::process::id());
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
        ("PQUEUE_LOG_BACKEND", "objectlog"),
        ("PQUEUE_PROJECTION_BACKEND", "inmemory"),
        ("PQUEUE_OBJECT_LOG_STORE", "s3"),
        ("PQUEUE_OBJECT_LOG_S3_ENDPOINT", endpoint),
        ("PQUEUE_OBJECT_LOG_S3_BUCKET", bucket),
        ("PQUEUE_OBJECT_LOG_S3_REGION", "us-east-1"),
        ("PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
        ("PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID", ACCESS_KEY),
        ("PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY", SECRET_KEY),
        ("PQUEUE_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP", "true"),
        ("PQUEUE_SEGMENT_TARGET_BYTES", "1048576"),
        ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "5"),
        ("PQUEUE_LISTEN_ADDR", "127.0.0.1:0"),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_s3_object_log_config_builds_segmented_backend() {
    let minio = DisposableMinio::start();
    let bucket = format!("pqueue-production-{}", std::process::id());
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

#[test]
fn production_s3_object_log_config_rejects_incomplete_credentials_and_local_fallback() {
    let complete = production_s3_env("http://127.0.0.1:9000", "pqueue");
    for missing in [
        "PQUEUE_OBJECT_LOG_S3_ENDPOINT",
        "PQUEUE_OBJECT_LOG_S3_BUCKET",
        "PQUEUE_OBJECT_LOG_S3_REGION",
        "PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE",
        "PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID",
        "PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
    ] {
        let mut env = complete.clone();
        env.remove(missing);
        let Err(error) = Config::from_env(&env) else {
            panic!("incomplete S3 config must fail closed: missing {missing}");
        };
        assert!(error.0.contains(missing), "{}: {}", missing, error.0);
    }

    let local_fallback = map(&[
        ("PQUEUE_OBJECT_LOG_STORE", "local"),
        ("PQUEUE_OBJECT_LOG_S3_ENDPOINT", "http://minio:9000"),
        ("PQUEUE_OBJECT_LOG_ROOT", "/tmp/would-silently-fallback"),
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
