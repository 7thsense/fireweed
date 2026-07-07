use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::fjord_topic_name;
use pqueue_core::{QueueDefinition, UtcTimestamp};
use pqueue_engine::{
    ChangeRecordSink, ComposedBackend, ControlPlane, EngineError, EngineResult, LogStore,
    ProjectionStore, QueueKey,
};
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::task::JoinHandle;

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
                parse_delivery_endpoint(endpoint)?;
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
    fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
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
    fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
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
struct ParsedKafkaEndpoint {
    bootstrap_servers: String,
}

#[derive(Debug, Clone)]
enum ParsedDeliveryEndpoint {
    Http(ParsedEndpoint),
    Kafka(ParsedKafkaEndpoint),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeRecordSinkSelection {
    Http,
    Kafka,
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
        let endpoint = match parse_delivery_endpoint(config.endpoint.as_deref().ok_or(
            EngineError::Invalid("change record sink endpoint is required"),
        )?)? {
            ParsedDeliveryEndpoint::Http(endpoint) => endpoint,
            ParsedDeliveryEndpoint::Kafka(_) => {
                return Err(EngineError::Invalid(
                    "change record sink endpoint must use http:// for Niflheim delivery",
                ));
            }
        };
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

fn change_record_key(record: &pqueue_engine::ChangeRecord) -> String {
    let item_id = record
        .item_id
        .map(|item_id| item_id.to_string())
        .unwrap_or_default();
    format!(
        "{item_id}:{}:{}",
        record.position.backend_epoch, record.position.sequence
    )
}

fn change_record_headers(record: &pqueue_engine::ChangeRecord) -> OwnedHeaders {
    let backend_epoch = record.position.backend_epoch.to_string();
    let sequence = record.position.sequence.to_string();
    let command_kind = format!("{:?}", record.command_kind);
    let item_id = record.item_id.map(|value| value.to_string());
    let mut headers = OwnedHeaders::new()
        .insert(Header {
            key: "pq-tenant-id",
            value: Some(record.tenant_id.as_str()),
        })
        .insert(Header {
            key: "pq-queue-id",
            value: Some(record.queue_id.as_str()),
        })
        .insert(Header {
            key: "pq-backend-epoch",
            value: Some(backend_epoch.as_str()),
        })
        .insert(Header {
            key: "pq-sequence",
            value: Some(sequence.as_str()),
        })
        .insert(Header {
            key: "pq-command-kind",
            value: Some(command_kind.as_str()),
        });
    if let Some(item_id) = item_id.as_deref() {
        headers = headers.insert(Header {
            key: "pq-item-id",
            value: Some(item_id),
        });
    }
    headers
}

fn block_on_sync<F: Future>(fut: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(move || handle.block_on(fut)),
        Err(_) => {
            static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
            RT.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("fallback runtime")
            })
            .block_on(fut)
        }
    }
}

impl ChangeRecordSink for FjordChangeRecordSink {
    fn emit(&self, shard: &QueueKey, records: &[pqueue_engine::ChangeRecord]) -> EngineResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let topic = fjord_topic_name(shard);
        block_on_sync(async {
            for record in records {
                let key = change_record_key(record);
                let payload = serde_json::to_vec(record)
                    .map_err(|e| EngineError::Storage(format!("serialize change record: {e}")))?;
                let headers = change_record_headers(record);
                self.producer
                    .send(
                        FutureRecord::to(&topic)
                            .partition(0)
                            .key(&key)
                            .payload(&payload)
                            .headers(headers),
                        Duration::from_secs(10),
                    )
                    .await
                    .map_err(|(e, _)| {
                        EngineError::Storage(format!("produce change record to fjord: {e}"))
                    })?;
            }
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct FjordChangeRecordSink {
    producer: FutureProducer,
}

impl FjordChangeRecordSink {
    pub fn new(config: &ChangeRecordSinkConfig) -> EngineResult<Self> {
        if !config.enabled {
            return Err(EngineError::Invalid(
                "change record sink is disabled in config",
            ));
        }
        config.validate()?;
        let endpoint = match parse_delivery_endpoint(config.endpoint.as_deref().ok_or(
            EngineError::Invalid("change record sink endpoint is required"),
        )?)? {
            ParsedDeliveryEndpoint::Kafka(endpoint) => endpoint,
            ParsedDeliveryEndpoint::Http(_) => {
                return Err(EngineError::Invalid(
                    "change record sink endpoint must use kafka:// for fjord delivery",
                ));
            }
        };

        let producer = ClientConfig::new()
            .set("bootstrap.servers", endpoint.bootstrap_servers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "10000")
            .set("retries", "2147483647")
            .create()
            .map_err(|e| EngineError::Storage(format!("create fjord producer: {e}")))?;

        Ok(Self { producer })
    }
}

fn parse_delivery_endpoint(input: &str) -> EngineResult<ParsedDeliveryEndpoint> {
    let trimmed = input.trim();
    if let Some(without_scheme) = trimmed.strip_prefix("http://") {
        return Ok(ParsedDeliveryEndpoint::Http(parse_http_endpoint(
            without_scheme,
        )?));
    }
    if let Some(without_scheme) = trimmed.strip_prefix("kafka://") {
        return Ok(ParsedDeliveryEndpoint::Kafka(parse_kafka_endpoint(
            without_scheme,
        )?));
    }
    if trimmed.contains("://") {
        return Err(EngineError::Invalid(
            "change record sink endpoint must use http:// or kafka://",
        ));
    }
    Ok(ParsedDeliveryEndpoint::Kafka(parse_kafka_endpoint(
        trimmed,
    )?))
}

pub(crate) fn change_record_sink_is_fjord(endpoint: Option<&str>) -> EngineResult<bool> {
    Ok(matches!(
        change_record_sink_selection(endpoint)?,
        Some(ChangeRecordSinkSelection::Kafka)
    ))
}

fn change_record_sink_selection(
    endpoint: Option<&str>,
) -> EngineResult<Option<ChangeRecordSinkSelection>> {
    match endpoint {
        Some(endpoint) => Ok(Some(match parse_delivery_endpoint(endpoint)? {
            ParsedDeliveryEndpoint::Http(_) => ChangeRecordSinkSelection::Http,
            ParsedDeliveryEndpoint::Kafka(_) => ChangeRecordSinkSelection::Kafka,
        })),
        None => Ok(None),
    }
}

fn build_change_record_sink(
    config: &ChangeRecordSinkConfig,
) -> EngineResult<Arc<dyn ChangeRecordSink>> {
    match change_record_sink_selection(config.endpoint.as_deref())? {
        Some(ChangeRecordSinkSelection::Http) => {
            Ok(Arc::new(NiflheimChangeRecordSink::new(config)?) as Arc<dyn ChangeRecordSink>)
        }
        Some(ChangeRecordSinkSelection::Kafka) => {
            Ok(Arc::new(FjordChangeRecordSink::new(config)?) as Arc<dyn ChangeRecordSink>)
        }
        None => Err(EngineError::Invalid(
            "change record sink endpoint is required",
        )),
    }
}

fn parse_http_endpoint(without_scheme: &str) -> EngineResult<ParsedEndpoint> {
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

fn parse_kafka_endpoint(input: &str) -> EngineResult<ParsedKafkaEndpoint> {
    let trimmed = input.trim();
    let (host, port) = trimmed.rsplit_once(':').ok_or(EngineError::Invalid(
        "change record sink endpoint must include host:port",
    ))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| EngineError::Invalid("change record sink port must be a u16"))?;
    if host.is_empty() {
        return Err(EngineError::Invalid("change record sink host is required"));
    }
    Ok(ParsedKafkaEndpoint {
        bootstrap_servers: format!("{host}:{port}"),
    })
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

fn enabled_change_record_queues<'a>(
    queues: &'a [QueueDefinition],
) -> impl Iterator<Item = &'a QueueDefinition> + 'a {
    queues.iter().filter(|queue| queue.emit_change_records)
}

pub fn emit_change_record_tick<B>(
    backend: &B,
    sink: &(impl ChangeRecordSink + ?Sized),
    queues: &[QueueDefinition],
    batch_size: usize,
) -> EngineResult<usize>
where
    B: ChangeRecordEmissionBackend,
{
    let emitted_at = now_timestamp();
    let mut total = 0usize;
    for definition in enabled_change_record_queues(queues) {
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
    sink: Arc<dyn ChangeRecordSink>,
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

fn spawn_change_record_emitter_if_enabled_with_builders<B, FHttp, FKafka>(
    backend: Arc<B>,
    queues: &[QueueDefinition],
    config: &ChangeRecordSinkConfig,
    build_http_sink: FHttp,
    build_kafka_sink: FKafka,
) -> EngineResult<Option<JoinHandle<()>>>
where
    B: ChangeRecordEmissionBackend + Send + Sync + 'static,
    FHttp: FnOnce(&ChangeRecordSinkConfig) -> EngineResult<Arc<dyn ChangeRecordSink>>,
    FKafka: FnOnce(&ChangeRecordSinkConfig) -> EngineResult<Arc<dyn ChangeRecordSink>>,
{
    if !config.enabled {
        return Ok(None);
    }
    let queues = queues
        .iter()
        .filter(|queue| queue.emit_change_records)
        .cloned()
        .collect::<Vec<_>>();
    if queues.is_empty() {
        return Ok(None);
    }
    let sink = match change_record_sink_selection(config.endpoint.as_deref())? {
        Some(ChangeRecordSinkSelection::Http) => build_http_sink(config)?,
        Some(ChangeRecordSinkSelection::Kafka) => build_kafka_sink(config)?,
        None => {
            return Err(EngineError::Invalid(
                "change record sink endpoint is required",
            ));
        }
    };
    Ok(Some(spawn_change_record_emitter(
        backend,
        sink,
        queues,
        config.clone(),
    )))
}

pub(crate) fn spawn_change_record_emitter_if_enabled<B>(
    backend: Arc<B>,
    queues: &[QueueDefinition],
    config: &ChangeRecordSinkConfig,
) -> EngineResult<Option<JoinHandle<()>>>
where
    B: ChangeRecordEmissionBackend + Send + Sync + 'static,
{
    if !config.enabled {
        return Ok(None);
    }
    let queues = queues
        .iter()
        .filter(|queue| queue.emit_change_records)
        .cloned()
        .collect::<Vec<_>>();
    if queues.is_empty() {
        return Ok(None);
    }
    let sink = build_change_record_sink(config)?;
    Ok(Some(spawn_change_record_emitter(
        backend,
        sink,
        queues,
        config.clone(),
    )))
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                .contains("change record sink endpoint must include host:port"),
            "{}",
            err
        );
        assert!(!config.enabled);
    }

    #[test]
    fn TestChangeRecordSinkRecognizesKafkaEndpoints() {
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        config.validate().expect("kafka endpoint should validate");
        assert!(change_record_sink_is_fjord(config.endpoint.as_deref()).unwrap());
    }

    #[test]
    fn TestChangeRecordSinkConfigSelectsKafkaProducerPath() {
        let http = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("http://127.0.0.1:8080/ingest".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        let kafka = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };

        assert!(matches!(
            change_record_sink_selection(http.endpoint.as_deref()).unwrap(),
            Some(ChangeRecordSinkSelection::Http)
        ));
        assert!(matches!(
            change_record_sink_selection(kafka.endpoint.as_deref()).unwrap(),
            Some(ChangeRecordSinkSelection::Kafka)
        ));
        assert!(
            !change_record_sink_is_fjord(http.endpoint.as_deref()).unwrap(),
            "http endpoint must keep the default niflheim path"
        );
        assert!(
            change_record_sink_is_fjord(kafka.endpoint.as_deref()).unwrap(),
            "kafka endpoint must select the producer path"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn TestChangeRecordEmitterStartsChosenSinkOnly() {
        #[derive(Default)]
        struct NoopBackend;

        impl ChangeRecordEmissionBackend for NoopBackend {
            fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
                &self,
                _shard: &QueueKey,
                _sink: &S,
                _limit: usize,
                _emitted_at: UtcTimestamp,
                _source_owner_id: Option<pqueue_core::OwnerId>,
            ) -> EngineResult<usize> {
                Ok(0)
            }
        }

        let backend = Arc::new(NoopBackend::default());
        let queues = vec![queue_definition("tenant-a", "queue-a", true)];
        let http_calls = Arc::new(AtomicUsize::new(0));
        let kafka_calls = Arc::new(AtomicUsize::new(0));
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            tick_interval: Duration::from_millis(1),
            ..ChangeRecordSinkConfig::default()
        };

        let handle = spawn_change_record_emitter_if_enabled_with_builders(
            backend,
            &queues,
            &config,
            {
                let http_calls = Arc::clone(&http_calls);
                move |_| {
                    http_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(RecordingSink::default()) as Arc<dyn ChangeRecordSink>)
                }
            },
            {
                let kafka_calls = Arc::clone(&kafka_calls);
                move |_| {
                    kafka_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(RecordingSink::default()) as Arc<dyn ChangeRecordSink>)
                }
            },
        )
        .expect("emitter should start")
        .expect("enabled config with emit-change-record queues should spawn");

        assert_eq!(http_calls.load(Ordering::SeqCst), 0);
        assert_eq!(kafka_calls.load(Ordering::SeqCst), 1);
        handle.abort();
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

    #[derive(Default)]
    struct RecordingEmissionBackend {
        emitted_shards: Mutex<Vec<QueueKey>>,
        cursor_advances: Mutex<Vec<QueueKey>>,
    }

    impl ChangeRecordEmissionBackend for RecordingEmissionBackend {
        fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
            &self,
            shard: &QueueKey,
            sink: &S,
            _limit: usize,
            emitted_at: UtcTimestamp,
            _source_owner_id: Option<pqueue_core::OwnerId>,
        ) -> EngineResult<usize> {
            self.emitted_shards
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            self.cursor_advances
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            let record = pqueue_engine::ChangeRecord {
                tenant_id: shard.tenant_id.clone(),
                queue_id: shard.queue_id.clone(),
                item_id: Some(pqueue_core::ItemId::from_u64(1)),
                position: pqueue_engine::ChangeRecordPosition {
                    backend_epoch: 7,
                    sequence: 1,
                },
                command_kind: pqueue_engine::ChangeRecordKind::Push,
                new_state: Some(pqueue_engine::ChangeRecordState::Pending),
                item_version: Some(1),
                terminal_at: None,
                emitted_at: Some(emitted_at),
                source_owner_id: None,
                source_epoch: 7,
            };
            sink.emit(shard, &[record])?;
            Ok(1)
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        emitted_shards: Mutex<Vec<QueueKey>>,
    }

    impl ChangeRecordSink for RecordingSink {
        fn emit(
            &self,
            shard: &QueueKey,
            _records: &[pqueue_engine::ChangeRecord],
        ) -> EngineResult<()> {
            self.emitted_shards
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            Ok(())
        }
    }

    fn queue_definition(tenant: &str, queue: &str, emit_change_records: bool) -> QueueDefinition {
        QueueDefinition {
            tenant_id: pqueue_core::TenantId::new(tenant).unwrap(),
            queue_id: pqueue_core::QueueId::new(queue).unwrap(),
            priority_model: pqueue_core::PriorityModel {
                kind: pqueue_core::PriorityModelKind::Int64,
                direction: pqueue_core::PriorityDirection::Ascending,
                tie_breaker: pqueue_core::PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: pqueue_core::OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: pqueue_core::EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: pqueue_core::RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: pqueue_core::RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records,
        }
    }

    #[test]
    fn TestEmitChangeRecordTickSkipsOptedOutQueues() {
        let backend = RecordingEmissionBackend::default();
        let sink = RecordingSink::default();
        let enabled_def = queue_definition("tenant-a", "queue-a", true);
        let disabled_def = queue_definition("tenant-b", "queue-b", false);
        let enabled_shard =
            QueueKey::new(enabled_def.tenant_id.clone(), enabled_def.queue_id.clone());
        let disabled_shard = QueueKey::new(
            disabled_def.tenant_id.clone(),
            disabled_def.queue_id.clone(),
        );

        let emitted = emit_change_record_tick(&backend, &sink, &[enabled_def, disabled_def], 64)
            .expect("tick should succeed");

        assert_eq!(emitted, 1);
        assert_eq!(
            backend.emitted_shards.lock().expect("poisoned").as_slice(),
            std::slice::from_ref(&enabled_shard)
        );
        assert_eq!(
            sink.emitted_shards.lock().expect("poisoned").as_slice(),
            &[enabled_shard]
        );
        assert!(
            !backend
                .emitted_shards
                .lock()
                .expect("poisoned")
                .contains(&disabled_shard)
        );
    }

    #[test]
    fn TestEmitChangeRecordTickDoesNotAdvanceCursorForOptOut() {
        let backend = RecordingEmissionBackend::default();
        let sink = RecordingSink::default();
        let enabled_def = queue_definition("tenant-a", "queue-a", true);
        let disabled_def = queue_definition("tenant-b", "queue-b", false);
        let enabled_shard =
            QueueKey::new(enabled_def.tenant_id.clone(), enabled_def.queue_id.clone());
        let disabled_shard = QueueKey::new(
            disabled_def.tenant_id.clone(),
            disabled_def.queue_id.clone(),
        );

        emit_change_record_tick(&backend, &sink, &[enabled_def, disabled_def], 64)
            .expect("tick should succeed");

        let advances = backend.cursor_advances.lock().expect("poisoned").clone();
        assert_eq!(advances, vec![enabled_shard.clone()]);
        assert!(!advances.contains(&disabled_shard));
        assert_eq!(
            sink.emitted_shards.lock().expect("poisoned").as_slice(),
            &[enabled_shard]
        );
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
