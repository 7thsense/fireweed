//! Process-wide local MinIO for product tests and local runtime.
//!
//! Reuses `FIREWEED_S3_TEST_*` when those variables already point at a live
//! endpoint. Otherwise starts the digest-pinned MinIO binary (or `minio` on
//! PATH) on loopback and creates a test bucket. Object data is kept off tmpfs
//! (`FIREWEED_MINIO_DATA`, else `/var/tmp/fireweed-test-minio-data` when `/tmp`
//! is ram-backed) so capacity logs do not fill RAM.

use crate::S3CreateOnlyPut;
use fireweed_engine::EngineResult;
use std::io::{Read, Write};
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
    match env_from_process_environment() {
        Some(env) => {
            if !endpoint_live(&env.endpoint) {
                panic!(
                    "FIREWEED_S3_TEST_ENDPOINT={} is unset as a live listener. \
                     This is test setup, not a product S3 dispatch failure.",
                    env.endpoint
                );
            }
            if !minio_health_live(&env.endpoint) {
                panic!(
                    "FIREWEED_S3_TEST_ENDPOINT={} is a foreign listener, not MinIO. \
                     Refusing a generic S3 dispatch failure.",
                    env.endpoint
                );
            }
            ensure_bucket_or_explain(&env);
            env
        }
        None => {
            let local = format!("http://127.0.0.1:{DEFAULT_PORT}");
            if endpoint_live(&local) && !minio_health_live(&local) {
                panic!(
                    "FIREWEED_S3_TEST_ENDPOINT is unset and port {DEFAULT_PORT} is a foreign listener, \
                     not the expected MinIO. Refusing a generic S3 dispatch failure."
                );
            }
            spawn_local_minio()
        }
    }
}

fn ensure_bucket_or_explain(env: &S3TestEnv) {
    if let Err(error) = ensure_bucket_blocking(env) {
        let endpoint_named = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let who = match endpoint_named {
            Some(endpoint) => format!("FIREWEED_S3_TEST_ENDPOINT={endpoint}"),
            None => format!(
                "FIREWEED_S3_TEST_ENDPOINT is unset; listener {} is not the expected MinIO \
                 (credentials {ACCESS_KEY})",
                env.endpoint
            ),
        };
        panic!(
            "{who} rejected the test bucket setup (wrong credentials or not MinIO). \
             This is test setup, not a product S3 dispatch failure. Underlying: {error}"
        );
    }
}

fn minio_health_live(endpoint: &str) -> bool {
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
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(250)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let host = host_port.split('/').next().unwrap_or(host_port);
    let request =
        format!("GET /minio/health/live HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0_u8; 256];
    let Ok(n) = stream.read(&mut buf) else {
        return false;
    };
    let text = String::from_utf8_lossy(&buf[..n]);
    text.starts_with("HTTP/1.1 200") || text.starts_with("HTTP/1.0 200")
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
    let data = minio_data_dir();
    if endpoint_live(&endpoint) {
        if existing_test_minio_on_tmpfs() && !path_on_tmpfs(&data) {
            stop_tmpfs_test_minio();
            let deadline = Instant::now() + Duration::from_secs(5);
            while endpoint_live(&endpoint) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(50));
            }
        } else {
            ensure_bucket_or_explain(&env);
            return env;
        }
    }

    let bin = find_minio_binary();
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
    let child = command.spawn().unwrap_or_else(|error| {
        panic!(
            "FIREWEED_S3_TEST_ENDPOINT is unset and MinIO could not be started from {}: {error}. \
             This is test setup, not a product S3 dispatch failure.",
            bin.display()
        )
    });
    *CHILD.lock().expect("minio child mutex") = Some(child);

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if endpoint_live(&endpoint) {
            if !minio_health_live(&endpoint) {
                panic!(
                    "FIREWEED_S3_TEST_ENDPOINT is unset and port {DEFAULT_PORT} did not become MinIO. \
                     Refusing a generic S3 dispatch failure."
                );
            }
            ensure_bucket_or_explain(&env);
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
    if let Ok(path) = std::env::var("FIREWEED_MINIO_DATA") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    let tmp = std::env::temp_dir().join("fireweed-test-minio-data");
    if path_on_tmpfs(&tmp) {
        return PathBuf::from("/var/tmp/fireweed-test-minio-data");
    }
    tmp
}

fn path_on_tmpfs(path: &Path) -> bool {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return false;
    };
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let mut best: Option<(usize, bool)> = None;
    for line in mounts.lines() {
        let mut parts = line.split_whitespace();
        let _source = parts.next();
        let Some(target) = parts.next() else {
            continue;
        };
        let Some(fstype) = parts.next() else {
            continue;
        };
        let target_path = Path::new(target);
        if canonical == target_path || canonical.starts_with(target_path) {
            let len = target.len();
            if best.map(|(prev, _)| len >= prev).unwrap_or(true) {
                best = Some((len, fstype == "tmpfs" || fstype == "ramfs"));
            }
        }
    }
    best.map(|(_, tmpfs)| tmpfs).unwrap_or(false)
}

fn existing_test_minio_on_tmpfs() -> bool {
    test_minio_pids_with_data()
        .into_iter()
        .any(|(_, data)| path_on_tmpfs(&data))
}

fn test_minio_pids_with_data() -> Vec<(i32, PathBuf)> {
    let Ok(proc) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in proc.flatten() {
        let pid = match entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        {
            Some(pid) => pid,
            None => continue,
        };
        let Ok(raw) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let cmdline = String::from_utf8_lossy(&raw);
        if !cmdline.contains("minio") || !cmdline.contains("fireweed-test-minio-data") {
            continue;
        }
        let args: Vec<&str> = cmdline
            .split('\0')
            .filter(|part| !part.is_empty())
            .collect();
        let Some(server_at) = args.iter().position(|arg| *arg == "server") else {
            continue;
        };
        let Some(data) = args.get(server_at + 1) else {
            continue;
        };
        found.push((pid, PathBuf::from(data)));
    }
    found
}

fn stop_tmpfs_test_minio() {
    for (pid, data) in test_minio_pids_with_data() {
        if !path_on_tmpfs(&data) {
            continue;
        }
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
    if let Ok(mut child) = CHILD.lock() {
        if let Some(mut owned) = child.take() {
            let _ = owned.kill();
            let _ = owned.wait();
        }
    }
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
