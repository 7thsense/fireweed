//! Process-wide local MinIO for product tests and local runtime.
//!
//! Reuses `FIREWEED_S3_TEST_*` when those variables already point at a live
//! endpoint. Otherwise starts the digest-pinned MinIO binary (or `minio` on
//! PATH) on loopback and creates a test bucket.

use crate::S3CreateOnlyPut;
use fireweed_engine::EngineResult;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_PORT: u16 = 19_000;
const ACCESS_KEY: &str = "fireweed";
const SECRET_KEY: &str = "fireweed-test-minio";
const BUCKET: &str = "fireweed-test";
const REGION: &str = "us-east-1";

/// Live S3-compatible endpoint used by the public s3 × turso product cell.
#[derive(Clone, Debug)]
pub struct S3TestEnv {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3TestEnv {
    pub fn allow_insecure_http(&self) -> bool {
        self.endpoint.starts_with("http://")
    }
}

static ENV: OnceLock<S3TestEnv> = OnceLock::new();
static CHILD: Mutex<Option<Child>> = Mutex::new(None);

/// Shared MinIO (or operator-provided) S3 endpoint for this process.
pub fn shared_s3_test_env() -> &'static S3TestEnv {
    ENV.get_or_init(|| {
        thread::spawn(init_s3_test_env)
            .join()
            .unwrap_or_else(|_| panic!("minio init thread panicked"))
    })
}

fn init_s3_test_env() -> S3TestEnv {
    if let Some(env) = env_from_process_environment() {
        if endpoint_live(&env.endpoint) {
            ensure_bucket_blocking(&env).expect("create bucket on provided S3 endpoint");
            return env;
        }
    }
    spawn_local_minio()
}

fn env_from_process_environment() -> Option<S3TestEnv> {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT").ok()?;
    if endpoint.trim().is_empty() {
        return None;
    }
    Some(S3TestEnv {
        endpoint,
        bucket: std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| BUCKET.to_owned()),
        region: std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| REGION.to_owned()),
        access_key: std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
            .unwrap_or_else(|_| ACCESS_KEY.to_owned()),
        secret_key: std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
            .unwrap_or_else(|_| SECRET_KEY.to_owned()),
    })
}

fn endpoint_live(endpoint: &str) -> bool {
    let url = endpoint.trim().trim_end_matches('/');
    let host_port = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let Ok(mut addrs) = host_port.to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(250)).is_ok()
}

fn spawn_local_minio() -> S3TestEnv {
    let endpoint = format!("http://127.0.0.1:{DEFAULT_PORT}");
    let env = S3TestEnv {
        endpoint: endpoint.clone(),
        bucket: BUCKET.to_owned(),
        region: REGION.to_owned(),
        access_key: ACCESS_KEY.to_owned(),
        secret_key: SECRET_KEY.to_owned(),
    };
    if endpoint_live(&endpoint) {
        ensure_bucket_blocking(&env).expect("create bucket on reused MinIO");
        return env;
    }

    let bin = find_minio_binary();
    let data = minio_data_dir();
    std::fs::create_dir_all(&data).expect("minio data dir");

    let mut command = Command::new(&bin);
    command
        .arg("server")
        .arg(&data)
        .arg("--address")
        .arg(format!("127.0.0.1:{DEFAULT_PORT}"))
        .arg("--quiet")
        .env("MINIO_ROOT_USER", ACCESS_KEY)
        .env("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = command
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn minio at {}: {error}", bin.display()));
    *CHILD.lock().expect("minio child mutex") = Some(child);

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if endpoint_live(&endpoint) {
            ensure_bucket_blocking(&env).expect("create bucket on spawned MinIO");
            return env;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("MinIO did not become ready on {endpoint} within 10s");
}

fn find_minio_binary() -> PathBuf {
    if let Ok(path) = std::env::var("FIREWEED_MINIO_BIN") {
        return PathBuf::from(path);
    }
    let pinned = Path::new("/tmp/fireweed-maintenance-tools/minio");
    if pinned.is_file() {
        return pinned.to_path_buf();
    }
    PathBuf::from("minio")
}

fn minio_data_dir() -> PathBuf {
    std::env::temp_dir().join("fireweed-test-minio-data")
}

fn ensure_bucket_blocking(env: &S3TestEnv) -> EngineResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("minio bucket runtime");
    let client = S3CreateOnlyPut::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        &env.access_key,
        &env.secret_key,
    );
    runtime.block_on(client.ensure_bucket())
}
