//! Container runtime contract tests for the `pqueue-service` entrypoint surface.
//!
//! These exercise the health router that the production container image exposes
//! for Kubernetes liveness/readiness probes plus the full service router built
//! from runtime configuration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use pqueue_objectlog::{
    DeploymentProfile, ManifestMode, S3CompatibleCredentials, S3CompatibleObjectLogConfig,
};
use pqueue_service::runtime::{
    LIVENESS_PATH, ObjectLogRuntimeConfig, READINESS_PATH, ReadinessCheck, RuntimeConfig,
    health_router, service_router, service_router_with_readiness,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;

async fn body_text(body: Body) -> String {
    let bytes = to_bytes(body, 1024).await.expect("body should be readable");
    String::from_utf8(bytes.to_vec()).expect("body should be utf8")
}

fn test_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "pqueue-container-runtime-{name}-{}-{nanos}",
        std::process::id()
    ))
}

fn valid_object_log_env(sqlite_dir: &Path, endpoint: &str) -> HashMap<&'static str, String> {
    HashMap::from([
        (
            "PQUEUE_BACKEND_PROFILE",
            "object_log_sqlite_projection".to_string(),
        ),
        (
            "PQUEUE_POSTGRES_DATABASE_URL",
            "postgres://pqueue:pqueue@postgres:5432/pqueue".to_string(),
        ),
        ("PQUEUE_OBJECT_LOG_ENDPOINT", endpoint.to_string()),
        ("PQUEUE_OBJECT_LOG_BUCKET", "pqueue-object-log".to_string()),
        ("PQUEUE_OBJECT_LOG_REGION", "us-east-1".to_string()),
        ("PQUEUE_OBJECT_LOG_ACCESS_KEY_ID", "minioadmin".to_string()),
        (
            "PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY",
            "minioadmin-secret".to_string(),
        ),
        ("PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS", "1024".to_string()),
        (
            "PQUEUE_SQLITE_PROJECTION_DIR",
            sqlite_dir.display().to_string(),
        ),
    ])
}

fn runtime_config_from_env(env: &HashMap<&'static str, String>) -> RuntimeConfig {
    RuntimeConfig::from_getter(|key| env.get(key).cloned()).expect("runtime env should be valid")
}

fn object_log_runtime_config(
    endpoint: &str,
    sqlite_projection_dir: PathBuf,
) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        postgres_control_plane_url: "postgres://pqueue:pqueue@postgres:5432/pqueue".to_string(),
        s3: S3CompatibleObjectLogConfig {
            endpoint_url: endpoint.to_string(),
            bucket: "pqueue-object-log".to_string(),
            region: "us-east-1".to_string(),
            credentials: S3CompatibleCredentials {
                access_key_id: "minioadmin".to_string(),
                secret_access_key: "minioadmin-secret".to_string(),
            },
            force_path_style: true,
            deployment_profile: DeploymentProfile::Production,
            manifest_mode: ManifestMode::ObjectStoreCas,
            max_commands_per_segment: 1024,
            dev_unsafe_one_command_segments: false,
        },
        sqlite_projection_dir,
    }
}

#[tokio::test]
async fn liveness_probe_returns_ok() {
    let response = health_router()
        .oneshot(
            Request::builder()
                .uri(LIVENESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response.into_body()).await, "ok");
}

#[tokio::test]
async fn readiness_probe_returns_ready() {
    let response = health_router()
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response.into_body()).await, "ready");
}

#[tokio::test]
async fn service_router_serves_health_alongside_api() {
    let config = RuntimeConfig::from_getter(|_| None).expect("defaults are valid");
    let router = service_router(pqueue_service::AppState::new(config.auth_context()));

    let health = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(LIVENESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(health.status(), StatusCode::OK);

    // An unknown API route still falls through to the API-001 not-found handler,
    // proving the health routes merged without displacing the app fallback.
    let unknown = router
        .oneshot(
            Request::builder()
                .uri("/v1/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn production_readiness_fails_without_postgres_url_for_postgres_native() {
    let config =
        RuntimeConfig::from_getter(|key| (key == "PQUEUE_TENANTS").then(|| "tenant-a".to_string()))
            .expect("defaults are valid");

    let response = config
        .router()
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn production_readiness_can_be_configured_as_ready_for_non_postgres_profiles() {
    let router = service_router_with_readiness(
        pqueue_service::AppState::new(RuntimeConfig::from_getter(|_| None).unwrap().auth_context()),
        ReadinessCheck::Ready,
    );

    let response = router
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response.into_body()).await, "ready");
}

#[tokio::test]
async fn test_object_log_runtime_env_validation() {
    let sqlite_dir = test_temp_dir("env-validation");
    let env = valid_object_log_env(&sqlite_dir, "http://minio.local:9000");
    let config = runtime_config_from_env(&env);
    let object_log = config
        .object_log
        .as_ref()
        .expect("object-log profile should parse object-log runtime config");

    assert_eq!(
        config.backend_profile.as_str(),
        "object_log_sqlite_projection"
    );
    assert_eq!(
        object_log.postgres_control_plane_url,
        "postgres://pqueue:pqueue@postgres:5432/pqueue"
    );
    assert_eq!(object_log.s3.endpoint_url, "http://minio.local:9000");
    assert_eq!(object_log.s3.bucket, "pqueue-object-log");
    assert_eq!(object_log.s3.region, "us-east-1");
    assert_eq!(object_log.s3.credentials.access_key_id, "minioadmin");
    assert_eq!(
        object_log.s3.credentials.secret_access_key,
        "minioadmin-secret"
    );
    assert_eq!(object_log.s3.max_commands_per_segment, 1024);
    assert_eq!(object_log.sqlite_projection_dir, sqlite_dir);

    for required_key in [
        "PQUEUE_POSTGRES_DATABASE_URL",
        "PQUEUE_OBJECT_LOG_ENDPOINT",
        "PQUEUE_OBJECT_LOG_BUCKET",
        "PQUEUE_OBJECT_LOG_REGION",
        "PQUEUE_OBJECT_LOG_ACCESS_KEY_ID",
        "PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY",
        "PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS",
        "PQUEUE_SQLITE_PROJECTION_DIR",
    ] {
        let mut missing = env.clone();
        missing.remove(required_key);
        let err = RuntimeConfig::from_getter(|key| missing.get(key).cloned())
            .expect_err("missing required object-log env must fail validation");
        assert!(
            err.to_string().contains(required_key),
            "error `{err}` should name `{required_key}`"
        );
    }

    let mut invalid_segment = env.clone();
    invalid_segment.insert(
        "PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS",
        "not-a-number".to_string(),
    );
    let err = RuntimeConfig::from_getter(|key| invalid_segment.get(key).cloned())
        .expect_err("invalid segment max commands must fail validation");
    assert!(
        err.to_string()
            .contains("PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS")
    );

    let mut unsafe_segment = env.clone();
    unsafe_segment.insert("PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS", "1".to_string());
    let err = RuntimeConfig::from_getter(|key| unsafe_segment.get(key).cloned())
        .expect_err("one-command production segments must fail validation");
    assert!(err.to_string().contains("one-command object segments"));

    let mut invalid_bucket = env;
    invalid_bucket.insert("PQUEUE_OBJECT_LOG_BUCKET", "Bad Bucket".to_string());
    let err = RuntimeConfig::from_getter(|key| invalid_bucket.get(key).cloned())
        .expect_err("invalid bucket must fail validation");
    assert!(err.to_string().contains("bucket"));
}

#[tokio::test]
async fn test_object_log_readiness_requires_configured_dependencies() {
    let router = service_router_with_readiness(
        pqueue_service::AppState::new(RuntimeConfig::from_getter(|_| None).unwrap().auth_context()),
        ReadinessCheck::ObjectLogConfigurationError(
            "PQUEUE_OBJECT_LOG_ENDPOINT is required".to_string(),
        ),
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body_text(response.into_body())
            .await
            .contains("PQUEUE_OBJECT_LOG_ENDPOINT")
    );

    let unusable_path = test_temp_dir("unusable-sqlite-dir");
    std::fs::write(&unusable_path, b"not a directory").unwrap();
    let router = service_router_with_readiness(
        pqueue_service::AppState::new(RuntimeConfig::from_getter(|_| None).unwrap().auth_context()),
        ReadinessCheck::ObjectLogSqliteProjection(object_log_runtime_config(
            "http://127.0.0.1:1",
            unusable_path,
        )),
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body_text(response.into_body())
            .await
            .contains("sqlite projection directory")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_object_log_readiness_probes_configured_path() {
    let server = S3ProbeServer::start().await;
    let sqlite_dir = test_temp_dir("readiness-probes-path");
    let env = valid_object_log_env(&sqlite_dir, &server.endpoint);
    let config = runtime_config_from_env(&env);

    let response = config
        .router()
        .oneshot(
            Request::builder()
                .uri(READINESS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    let status = response.status();
    let body = body_text(response.into_body()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "readiness body: {body}; requests: {:?}",
        server.requests()
    );
    assert_eq!(body, "ready");

    let requests = server.requests();
    assert!(
        requests.iter().any(|(method, path)| {
            method == "PUT" && path.starts_with("/pqueue-object-log/pqueue/readiness/")
        }),
        "readiness should PUT a probe object under the configured bucket: {requests:?}"
    );
    assert!(
        requests.iter().any(|(method, path)| {
            method == "GET" && path.starts_with("/pqueue-object-log/pqueue/readiness/")
        }),
        "readiness should GET the probe object under the configured bucket: {requests:?}"
    );
}

struct S3ProbeServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    handle: JoinHandle<()>,
}

impl S3ProbeServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("probe server should bind");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let objects = Arc::new(Mutex::new(HashMap::new()));
        let handle_requests = Arc::clone(&requests);
        let handle_objects = Arc::clone(&objects);
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let requests = Arc::clone(&handle_requests);
                let objects = Arc::clone(&handle_objects);
                tokio::spawn(async move {
                    handle_s3_probe_connection(stream, requests, objects).await;
                });
            }
        });

        Self {
            endpoint,
            requests,
            handle,
        }
    }

    fn requests(&self) -> Vec<(String, String)> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for S3ProbeServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn handle_s3_probe_connection(
    mut stream: tokio::net::TcpStream,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
) {
    let Some((method, path, body)) = read_http_request(&mut stream).await else {
        return;
    };
    requests
        .lock()
        .unwrap()
        .push((method.clone(), path.clone()));

    match method.as_str() {
        "PUT" => {
            objects.lock().unwrap().insert(object_key(&path), body);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        }
        "GET" => {
            let body = objects.lock().unwrap().get(&object_key(&path)).cloned();
            match body {
                Some(body) => {
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(header.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
                }
                None => {
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                }
            }
        }
        _ => {
            let _ = stream
                .write_all(
                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
        }
    }
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(String, String, Vec<u8>)> {
    let mut data = Vec::new();
    let header_end = loop {
        if let Some(index) = find_header_end(&data) {
            break index;
        }
        let mut chunk = [0; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        data.extend_from_slice(&chunk[..read]);
    };

    let header_text = String::from_utf8_lossy(&data[..header_end]);
    let mut lines = header_text.lines();
    let request_line = lines.next()?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next()?.to_string();
    let path = request_parts.next()?.to_string();
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let expects_continue = header_text.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("expect") && value.trim() == "100-continue"
        })
    });
    if expects_continue {
        stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .ok()?;
    }

    let body_start = header_end + 4;
    while data.len() < body_start + content_length {
        let mut chunk = [0; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        data.extend_from_slice(&chunk[..read]);
    }
    Some((
        method,
        path,
        data[body_start..body_start + content_length].to_vec(),
    ))
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

fn object_key(path: &str) -> String {
    path.split('?').next().unwrap_or(path).to_string()
}
