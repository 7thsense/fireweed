use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use pqueue_core::{QueueDefinition, UtcTimestamp};
use pqueue_engine::{
    ChangeRecordSink, ComposedBackend, ControlPlane, EngineError, EngineResult, LogStore,
    ProjectionStore, QueueKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRecordSinkConfig {
    pub enabled: bool,
    pub endpoint: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub tick_interval: Duration,
    pub batch_size: usize,
}

impl Default for ChangeRecordSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            headers: BTreeMap::new(),
            tick_interval: Duration::from_millis(250),
            batch_size: 256,
        }
    }
}

impl ChangeRecordSinkConfig {
    pub(crate) fn validate(&self) -> EngineResult<()> {
        match self.endpoint.as_deref() {
            Some(endpoint) => {
                parse_http_endpoint(endpoint)?;
            }
            None if self.enabled => {
                return Err(EngineError::Invalid(
                    "change record sink endpoint is required",
                ));
            }
            None => {}
        }
        Ok(())
    }
}

pub trait ChangeRecordEmissionBackend {
    fn emit_change_record_tail<S: ChangeRecordSink>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<pqueue_core::OwnerId>,
    ) -> EngineResult<usize>;
}

impl<L, P, C> ChangeRecordEmissionBackend for ComposedBackend<L, P, C>
where
    L: LogStore,
    P: ProjectionStore,
    C: ControlPlane,
{
    fn emit_change_record_tail<S: ChangeRecordSink>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<pqueue_core::OwnerId>,
    ) -> EngineResult<usize> {
        ComposedBackend::emit_change_record_tail(
            self,
            shard,
            sink,
            limit,
            emitted_at,
            source_owner_id,
        )
    }
}

#[derive(Debug, Clone)]
struct ParsedEndpoint {
    host: String,
    port: u16,
    path: String,
}

#[derive(Debug, Clone)]
pub struct NiflheimChangeRecordSink {
    endpoint: ParsedEndpoint,
    headers: BTreeMap<String, String>,
}

impl NiflheimChangeRecordSink {
    pub fn new(config: &ChangeRecordSinkConfig) -> EngineResult<Self> {
        if !config.enabled {
            return Err(EngineError::Invalid(
                "change record sink is disabled in config",
            ));
        }
        config.validate()?;
        let endpoint = parse_http_endpoint(config.endpoint.as_deref().ok_or(
            EngineError::Invalid("change record sink endpoint is required"),
        )?)?;
        Ok(Self {
            endpoint,
            headers: config.headers.clone(),
        })
    }
}

fn build_delivery_request(
    endpoint: &ParsedEndpoint,
    shard: &QueueKey,
    headers: &BTreeMap<String, String>,
    records: &[pqueue_engine::ChangeRecord],
) -> EngineResult<Vec<u8>> {
    let body = serde_json::to_vec(records)
        .map_err(|e| EngineError::Storage(format!("serialize change records: {e}")))?;
    let mut request = Vec::new();
    write_request_line(&mut request, endpoint, body.len())?;
    write_header(&mut request, "Content-Type", "application/json")?;
    write_header(&mut request, "Connection", "close")?;
    write_header(&mut request, "X-Pqueue-Tenant", shard.tenant_id.as_str())?;
    write_header(&mut request, "X-Pqueue-Queue", shard.queue_id.as_str())?;
    for (name, value) in headers {
        write_header(&mut request, name, value)?;
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(&body);
    Ok(request)
}

impl ChangeRecordSink for NiflheimChangeRecordSink {
    fn emit(&self, shard: &QueueKey, records: &[pqueue_engine::ChangeRecord]) -> EngineResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let request = build_delivery_request(&self.endpoint, shard, &self.headers, records)?;

        let addr = format!("{}:{}", self.endpoint.host, self.endpoint.port);
        let mut stream = TcpStream::connect(addr)
            .map_err(|e| EngineError::Storage(format!("connect durable-ingest endpoint: {e}")))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(5))))
            .map_err(|e| EngineError::Storage(format!("configure durable-ingest socket: {e}")))?;
        stream
            .write_all(&request)
            .map_err(|e| EngineError::Storage(format!("write durable-ingest request: {e}")))?;
        let _ = stream.shutdown(std::net::Shutdown::Write);
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(|e| EngineError::Storage(format!("read durable-ingest response: {e}")))?;
        let status = parse_status_code(&response)?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "durable-ingest returned HTTP {status}: {}",
                response.lines().next().unwrap_or("<empty>")
            )))
        }
    }
}

fn parse_http_endpoint(input: &str) -> EngineResult<ParsedEndpoint> {
    let trimmed = input.trim();
    let without_scheme = trimmed.strip_prefix("http://").ok_or(EngineError::Invalid(
        "durable-ingest endpoint must use http://",
    ))?;
    let (host_port, path) = match without_scheme.split_once('/') {
        Some((host_port, path)) => (host_port, format!("/{}", path)),
        None => (without_scheme, "/".to_string()),
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| EngineError::Invalid("durable-ingest endpoint port must be a u16"))?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 80),
    };
    if host.is_empty() {
        return Err(EngineError::Invalid(
            "durable-ingest endpoint host is required",
        ));
    }
    Ok(ParsedEndpoint { host, port, path })
}

fn write_request_line(
    request: &mut Vec<u8>,
    endpoint: &ParsedEndpoint,
    content_length: usize,
) -> EngineResult<()> {
    write!(
        request,
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Length: {}\r\n",
        endpoint.path, endpoint.host, endpoint.port, content_length
    )
    .map_err(|e| EngineError::Storage(format!("format durable-ingest request: {e}")))
}

fn write_header(request: &mut Vec<u8>, name: &str, value: &str) -> EngineResult<()> {
    write!(request, "{}: {}\r\n", name, value)
        .map_err(|e| EngineError::Storage(format!("format durable-ingest header: {e}")))
}

fn parse_status_code(response: &str) -> EngineResult<u16> {
    let status_line = response.lines().next().ok_or(EngineError::Storage(
        "durable-ingest response was empty".into(),
    ))?;
    let mut parts = status_line.split_whitespace();
    let _http = parts.next().ok_or(EngineError::Storage(
        "durable-ingest response missing HTTP version".into(),
    ))?;
    let status = parts
        .next()
        .ok_or(EngineError::Storage(
            "durable-ingest response missing status code".into(),
        ))?
        .parse::<u16>()
        .map_err(|_| {
            EngineError::Storage("durable-ingest response had invalid status code".into())
        })?;
    Ok(status)
}

fn now_timestamp() -> UtcTimestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    UtcTimestamp::new(
        now.as_secs().min(i64::MAX as u64) as i64,
        now.subsec_nanos(),
    )
    .expect("system time produces a valid UTC timestamp")
}

pub fn emit_change_record_tick<B>(
    backend: &B,
    sink: &impl ChangeRecordSink,
    queues: &[QueueDefinition],
    batch_size: usize,
) -> EngineResult<usize>
where
    B: ChangeRecordEmissionBackend,
{
    let emitted_at = now_timestamp();
    let mut total = 0usize;
    for definition in queues.iter().filter(|queue| queue.emit_change_records) {
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        match backend.emit_change_record_tail(&shard, sink, batch_size, emitted_at, None) {
            Ok(emitted) => total += emitted,
            Err(e) => eprintln!(
                "[change-record] emit failed for {}:{}: {e}",
                definition.tenant_id, definition.queue_id
            ),
        }
    }
    Ok(total)
}

pub fn spawn_change_record_emitter<B>(
    backend: Arc<B>,
    sink: Arc<NiflheimChangeRecordSink>,
    queues: Vec<QueueDefinition>,
    config: ChangeRecordSinkConfig,
) -> tokio::task::JoinHandle<()>
where
    B: ChangeRecordEmissionBackend + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(config.tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) =
                emit_change_record_tick(backend.as_ref(), sink.as_ref(), &queues, config.batch_size)
            {
                eprintln!("[change-record] emission tick failed: {e}");
            }
        }
    })
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn TestChangeRecordSinkDefaultsDisabledUntilEndpointIsSet() {
        let config = ChangeRecordSinkConfig::default();
        assert!(!config.enabled);
        assert!(config.endpoint.is_none());
        config
            .validate()
            .expect("disabled sink config remains valid");
    }

    #[test]
    fn TestChangeRecordSinkRejectsInvalidEndpointAndKeepsDisabled() {
        let config = ChangeRecordSinkConfig {
            endpoint: Some("not-a-url".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        assert!(!config.enabled);
        let err = config.validate().expect_err("malformed endpoint must fail");
        assert!(
            err.to_string()
                .contains("durable-ingest endpoint must use http://"),
            "{}",
            err
        );
        assert!(!config.enabled);
    }

    #[test]
    fn TestChangeRecordSinkWritesTenantQueueAndCustomHeaders() {
        let endpoint = ParsedEndpoint {
            host: "127.0.0.1".to_string(),
            port: 8080,
            path: "/ingest".to_string(),
        };
        let shard = QueueKey::new(
            pqueue_core::TenantId::new("tenant-a").unwrap(),
            pqueue_core::QueueId::new("queue-a").unwrap(),
        );
        let mut headers = BTreeMap::new();
        headers.insert("authorization".to_string(), "Bearer test".to_string());
        headers.insert("x-custom-header".to_string(), "custom-value".to_string());
        let records = vec![pqueue_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: None,
            position: pqueue_engine::ChangeRecordPosition {
                backend_epoch: 7,
                sequence: 42,
            },
            command_kind: pqueue_engine::ChangeRecordKind::PauseQueue,
            new_state: None,
            item_version: None,
            terminal_at: None,
            emitted_at: Some(UtcTimestamp::new(1, 0).unwrap()),
            source_owner_id: None,
            source_epoch: 7,
        }];

        let request = build_delivery_request(&endpoint, &shard, &headers, &records)
            .expect("request should build");
        let request = String::from_utf8(request).expect("request should be utf8");
        assert!(request.contains("POST /ingest HTTP/1.1"));
        assert!(request.contains("X-Pqueue-Tenant: tenant-a"));
        assert!(request.contains("X-Pqueue-Queue: queue-a"));
        assert!(request.contains("authorization: Bearer test"));
        assert!(request.contains("x-custom-header: custom-value"));
    }

    #[test]
    fn TestChangeRecordSinkBuildsDeliveryRequestWithTenantAndQueueMetadata() {
        let endpoint = ParsedEndpoint {
            host: "127.0.0.1".to_string(),
            port: 8080,
            path: "/ingest".to_string(),
        };
        let shard = QueueKey::new(
            pqueue_core::TenantId::new("tenant-a").unwrap(),
            pqueue_core::QueueId::new("queue-a").unwrap(),
        );
        let headers = BTreeMap::new();
        let records = vec![pqueue_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: Some(pqueue_core::ItemId::from_u64(17)),
            position: pqueue_engine::ChangeRecordPosition {
                backend_epoch: 9,
                sequence: 3,
            },
            command_kind: pqueue_engine::ChangeRecordKind::Finalize,
            new_state: Some(pqueue_engine::ChangeRecordState::Complete),
            item_version: Some(2),
            terminal_at: Some(UtcTimestamp::new(2, 0).unwrap()),
            emitted_at: Some(UtcTimestamp::new(3, 0).unwrap()),
            source_owner_id: Some(pqueue_core::OwnerId::new("node-a").unwrap()),
            source_epoch: 9,
        }];

        let request = build_delivery_request(&endpoint, &shard, &headers, &records)
            .expect("request should build");
        let request = String::from_utf8(request).expect("request should be utf8");
        let (head, body) = request
            .split_once("\r\n\r\n")
            .expect("request should include headers and body");
        assert!(head.contains("X-Pqueue-Tenant: tenant-a"));
        assert!(head.contains("X-Pqueue-Queue: queue-a"));
        let parsed: Vec<pqueue_engine::ChangeRecord> =
            serde_json::from_str(body).expect("request body should be valid json");
        assert_eq!(parsed, records);
    }
}
