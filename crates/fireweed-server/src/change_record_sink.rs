use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::fjord_topic_name;
use bytes::{Bytes, BytesMut};
use fireweed_core::{QueueDefinition, UtcTimestamp};
use fireweed_engine::{
    BoundedBlockingExecutor, ChangeRecordSink, ComposedBackend, ControlPlane, ControlPlaneStore,
    EngineError, EngineResult, LogStore, ProjectionStore, QueueKey,
};
use heimq_broker::storage::LogBackend;
use heimq_protocol::indexmap::IndexMap;
use heimq_protocol::protocol::StrBytes;
use heimq_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::task::JoinHandle;

// One stalled synchronous provider consumes one slot, not the emitter. If all slots stall, admission
// pauses at this explicit resource bound until one returns. Dropping the emitter cancels unresolved
// async lookups and prevents new admission; already-started blocking calls cannot be cancelled and may
// finish detached, but their number remains bounded by this constant.
const CHANGE_RECORD_EMISSION_CONCURRENCY: usize = 8;
const CHANGE_RECORD_REGISTRY_PAGE: usize = 64;

/// Delivery mode for the background change-record emission task (ADR-014). The mode is derived from the
/// legacy `enabled` + `endpoint` fields so existing config plumbing keeps working:
///
/// - [`ChangeRecordSinkMode::Disabled`] — emission off (`enabled = false`).
/// - [`ChangeRecordSinkMode::Embedded`] — the DEFAULT: append change records directly, in-process, to the
///   embedded fjord broker's Rust log (no endpoint, no loopback socket, no C Kafka client).
/// - [`ChangeRecordSinkMode::ExternalKafka`] — opt-in: publish to an EXTERNAL Kafka via the pure-Rust
///   `rskafka` producer (`kafka://host:port` endpoint, behind the `external-kafka` cargo feature).
/// - [`ChangeRecordSinkMode::Http`] — the existing niflheim durable-ingest HTTP binding (`http://` endpoint).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeRecordSinkMode {
    Disabled,
    Embedded,
    ExternalKafka,
    Http,
}

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
    /// Resolve the explicit delivery mode from the `enabled` + `endpoint` fields. A disabled sink is
    /// `Disabled`; an enabled sink with no endpoint selects the in-process `Embedded` default; an enabled
    /// `kafka://` endpoint selects `ExternalKafka`; an enabled `http://` endpoint selects `Http`.
    pub fn mode(&self) -> ChangeRecordSinkMode {
        if !self.enabled {
            return ChangeRecordSinkMode::Disabled;
        }
        match self.endpoint.as_deref() {
            None => ChangeRecordSinkMode::Embedded,
            Some(endpoint) => match parse_delivery_endpoint(endpoint) {
                Ok(ParsedDeliveryEndpoint::Http(_)) => ChangeRecordSinkMode::Http,
                // A malformed endpoint is rejected by `validate`; treat parse failures as external-kafka so
                // the error surfaces there rather than silently downgrading to the embedded default.
                Ok(ParsedDeliveryEndpoint::Kafka(_)) | Err(_) => {
                    ChangeRecordSinkMode::ExternalKafka
                }
            },
        }
    }

    pub(crate) fn validate(&self) -> EngineResult<()> {
        // The Embedded default needs no endpoint; only a *present* endpoint must be well-formed.
        if let Some(endpoint) = self.endpoint.as_deref() {
            parse_delivery_endpoint(endpoint)?;
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
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize>;

    fn supports_change_record_emission_cursor(&self) -> bool {
        false
    }
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
        source_owner_id: Option<fireweed_core::OwnerId>,
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

    fn supports_change_record_emission_cursor(&self) -> bool {
        self.with_log(|log| log.supports_emission_cursor())
    }
}

impl<B> ChangeRecordEmissionBackend for crate::PostgresWholeOperationAdapter<B>
where
    B: ChangeRecordEmissionBackend + fireweed_resp::RespBackend,
{
    fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize> {
        self.backend_for_queue(shard).emit_change_record_tail(
            shard,
            sink,
            limit,
            emitted_at,
            source_owner_id,
        )
    }

    fn supports_change_record_emission_cursor(&self) -> bool {
        self.backend_for_queue(&QueueKey::new(
            fireweed_core::TenantId::new("pqueue-internal").expect("valid tenant"),
            fireweed_core::QueueId::new("emission-capability").expect("valid queue"),
        ))
        .supports_change_record_emission_cursor()
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
    // Only read by the `external-kafka` (rskafka) sink; the default build parses the endpoint solely to
    // classify the delivery mode (`kafka://` → ExternalKafka), so the resolved bootstrap is unused there.
    #[cfg_attr(not(feature = "external-kafka"), allow(dead_code))]
    bootstrap_servers: String,
}

#[derive(Debug, Clone)]
enum ParsedDeliveryEndpoint {
    Http(ParsedEndpoint),
    #[cfg_attr(not(feature = "external-kafka"), allow(dead_code))]
    Kafka(ParsedKafkaEndpoint),
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
    records: &[fireweed_engine::ChangeRecord],
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
    fn emit(
        &self,
        shard: &QueueKey,
        records: &[fireweed_engine::ChangeRecord],
    ) -> EngineResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let request = build_delivery_request(&self.endpoint, shard, &self.headers, records)?;
        let addr = format!("{}:{}", self.endpoint.host, self.endpoint.port);
        block_on_sync(async move {
            tokio::task::spawn_blocking(move || {
                let mut stream = TcpStream::connect(addr).map_err(|e| {
                    EngineError::Storage(format!("connect durable-ingest endpoint: {e}"))
                })?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(5))))
                    .map_err(|e| {
                        EngineError::Storage(format!("configure durable-ingest socket: {e}"))
                    })?;
                stream.write_all(&request).map_err(|e| {
                    EngineError::Storage(format!("write durable-ingest request: {e}"))
                })?;
                let _ = stream.shutdown(std::net::Shutdown::Write);
                let mut response = String::new();
                stream.read_to_string(&mut response).map_err(|e| {
                    EngineError::Storage(format!("read durable-ingest response: {e}"))
                })?;
                let status = parse_status_code(&response)?;
                if (200..300).contains(&status) {
                    Ok(())
                } else {
                    Err(EngineError::Storage(format!(
                        "durable-ingest returned HTTP {status}: {}",
                        response.lines().next().unwrap_or("<empty>")
                    )))
                }
            })
            .await
            .map_err(|e| EngineError::Storage(format!("durable-ingest worker join failed: {e}")))?
        })
    }
}

fn change_record_key(record: &fireweed_engine::ChangeRecord) -> String {
    let item_id = record
        .item_id
        .map(|item_id| item_id.to_string())
        .unwrap_or_default();
    format!(
        "{item_id}:{}:{}",
        record.position.backend_epoch, record.position.sequence
    )
}

/// The ADR-014 "Normative consumer contract" change-record headers, in the PINNED wire order
/// (ADR-014:116): `pq-tenant-id`, `pq-queue-id`, `pq-item-id`, `pq-backend-epoch`, `pq-sequence`,
/// `pq-command-kind`. Kafka headers are an ordered list and the order is part of the consumer contract, so
/// `pq-item-id` sits in its pinned position (third) when present and is omitted only for queue-scoped
/// records. This is the SINGLE source of truth for header key/value/order shared by the in-process embedded
/// encoder and the external-Kafka producer path.
fn change_record_headers(record: &fireweed_engine::ChangeRecord) -> Vec<(&'static str, Vec<u8>)> {
    let mut headers = vec![
        (
            "pq-tenant-id",
            record.tenant_id.as_str().as_bytes().to_vec(),
        ),
        ("pq-queue-id", record.queue_id.as_str().as_bytes().to_vec()),
    ];
    if let Some(item_id) = record.item_id {
        headers.push(("pq-item-id", item_id.to_string().into_bytes()));
    }
    headers.push((
        "pq-backend-epoch",
        record.position.backend_epoch.to_string().into_bytes(),
    ));
    headers.push((
        "pq-sequence",
        record.position.sequence.to_string().into_bytes(),
    ));
    headers.push((
        "pq-command-kind",
        change_record_kind_wire_value(record.command_kind)
            .as_bytes()
            .to_vec(),
    ));
    headers
}

/// Encode a batch of change records as a single Kafka v2 record batch (partition 0), the exact wire form
/// `heimq_broker::storage::RecordBatchView::from_bytes` decodes and `FjordLog::append` stores. Each record
/// carries the ADR-014 "Normative consumer contract" shape: key `"{item_id}:{backend_epoch}:{sequence}"`,
/// the pinned `pq-*` headers, and the TD-008 `ChangeRecord` JSON as the payload.
fn encode_change_record_batch(records: &[fireweed_engine::ChangeRecord]) -> EngineResult<Vec<u8>> {
    let mut kafka_records = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let key = change_record_key(record);
        let payload = serde_json::to_vec(record)
            .map_err(|e| EngineError::Storage(format!("serialize change record: {e}")))?;
        let mut headers = IndexMap::new();
        for (name, value) in change_record_headers(record) {
            headers.insert(
                StrBytes::from_string(name.to_string()),
                Some(Bytes::from(value)),
            );
        }
        let timestamp = record
            .emitted_at
            .map(|ts| ts.seconds * 1000 + i64::from(ts.nanoseconds) / 1_000_000)
            .unwrap_or(-1);
        kafka_records.push(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            // Non-idempotent producer sentinel: the embedded log assigns offsets; TD-008 idempotency is
            // carried by the record key, not the Kafka producer id.
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: index as i64,
            sequence: index as i32,
            timestamp,
            key: Some(Bytes::from(key.into_bytes())),
            value: Some(Bytes::from(payload)),
            headers,
        });
    }
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &kafka_records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|e| EngineError::Storage(format!("encode change-record batch: {e}")))?;
    Ok(buf.to_vec())
}

fn change_record_kind_wire_value(kind: fireweed_engine::ChangeRecordKind) -> &'static str {
    match kind {
        fireweed_engine::ChangeRecordKind::Push => "push",
        fireweed_engine::ChangeRecordKind::Claim => "claim",
        fireweed_engine::ChangeRecordKind::CohortClaim => "cohort-claim",
        fireweed_engine::ChangeRecordKind::RenewLease => "renew-lease",
        fireweed_engine::ChangeRecordKind::CohortRenewLease => "cohort-renew-lease",
        fireweed_engine::ChangeRecordKind::ReassignLease => "reassign-lease",
        fireweed_engine::ChangeRecordKind::Finalize => "finalize",
        fireweed_engine::ChangeRecordKind::CohortFinalize => "cohort-finalize",
        fireweed_engine::ChangeRecordKind::ReplacePending => "replace-pending",
        fireweed_engine::ChangeRecordKind::UpdateFields => "update-fields",
        fireweed_engine::ChangeRecordKind::LeaseExpired => "lease-expired",
        fireweed_engine::ChangeRecordKind::CohortExpired => "cohort-expired",
        fireweed_engine::ChangeRecordKind::FenceLease => "fence-lease",
        fireweed_engine::ChangeRecordKind::UnfenceLease => "unfence-lease",
        fireweed_engine::ChangeRecordKind::PauseQueue => "pause-queue",
        fireweed_engine::ChangeRecordKind::ResumeQueue => "resume-queue",
        fireweed_engine::ChangeRecordKind::PurgeItems => "purge-items",
        fireweed_engine::ChangeRecordKind::SetGates => "set-gates",
    }
}

fn block_on_sync<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("blocking runtime")
                .block_on(fut)
        })
        .join()
        .expect("change record sync task panicked"),
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
    fn emit(
        &self,
        shard: &QueueKey,
        records: &[fireweed_engine::ChangeRecord],
    ) -> EngineResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let topic = fjord_topic_name(shard)?;
        // Ensure the queue topic exists before the first append: `FjordLog::append` returns
        // `TopicNotFound` for an unknown topic. The shared embedded broker also pre-registers these
        // topics; `get_or_create_topic` is idempotent, so a race with the broker is harmless.
        self.log.get_or_create_topic(&topic, 1);
        let batch = encode_change_record_batch(records)?;
        // Direct, in-process append to the embedded broker's Rust log — no loopback socket, no Kafka
        // client. Because this is the SAME `Arc<dyn LogBackend>` the embedded `HeimqServer` serves from,
        // the appended records are immediately visible to external-consumer fetches over the Kafka surface.
        self.log.append(&topic, 0, &batch).map_err(|e| {
            EngineError::Storage(format!(
                "append change record batch to embedded fjord log: {e}"
            ))
        })?;
        Ok(())
    }
}

/// The DEFAULT change-record sink: appends change records directly, in-process, to the embedded fjord
/// broker's Rust log. It holds the SAME `Arc<dyn LogBackend>` the embedded `HeimqServer` serves from, so
/// there is no loopback TCP socket and no C Kafka client on the write path (ADR-014).
#[derive(Clone)]
pub struct FjordChangeRecordSink {
    log: Arc<dyn LogBackend>,
}

impl FjordChangeRecordSink {
    /// Build an in-process sink over the shared embedded-broker log handle.
    pub fn new(log: Arc<dyn LogBackend>) -> Self {
        Self { log }
    }
}

/// The opt-in EXTERNAL-Kafka change-record sink (ADR-014 invariant #4, the swappable seam). Publishes
/// change records to an external Kafka cluster via the pure-Rust `rskafka` producer — no C Kafka client.
/// Gated behind the `external-kafka` cargo feature.
#[cfg(feature = "external-kafka")]
#[derive(Clone)]
pub struct ExternalKafkaChangeRecordSink {
    client: Arc<rskafka::client::Client>,
    partitions: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, Arc<rskafka::client::partition::PartitionClient>>,
        >,
    >,
}

#[cfg(feature = "external-kafka")]
impl ExternalKafkaChangeRecordSink {
    pub fn new(config: &ChangeRecordSinkConfig) -> EngineResult<Self> {
        if !config.enabled {
            return Err(EngineError::Invalid(
                "change record sink is disabled in config",
            ));
        }
        config.validate()?;
        let endpoint = match parse_delivery_endpoint(config.endpoint.as_deref().ok_or(
            EngineError::Invalid("external-kafka change record sink endpoint is required"),
        )?)? {
            ParsedDeliveryEndpoint::Kafka(endpoint) => endpoint,
            ParsedDeliveryEndpoint::Http(_) => {
                return Err(EngineError::Invalid(
                    "external-kafka change record sink endpoint must use kafka:// (host:port)",
                ));
            }
        };
        let bootstrap = endpoint.bootstrap_servers;
        let client = block_on_sync(async move {
            rskafka::client::ClientBuilder::new(vec![bootstrap])
                .build()
                .await
                .map_err(|e| EngineError::Storage(format!("connect external kafka: {e}")))
        })?;
        Ok(Self {
            client: Arc::new(client),
            partitions: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }

    fn partition_client(
        &self,
        topic: &str,
    ) -> EngineResult<Arc<rskafka::client::partition::PartitionClient>> {
        if let Some(existing) = self
            .partitions
            .lock()
            .expect("poisoned")
            .get(topic)
            .cloned()
        {
            return Ok(existing);
        }
        let client = self.client.clone();
        let topic_owned = topic.to_string();
        let partition = block_on_sync(async move {
            client
                .partition_client(
                    topic_owned,
                    0,
                    rskafka::client::partition::UnknownTopicHandling::Retry,
                )
                .await
                .map_err(|e| EngineError::Storage(format!("open external kafka partition: {e}")))
        })?;
        let partition = Arc::new(partition);
        self.partitions
            .lock()
            .expect("poisoned")
            .insert(topic.to_string(), partition.clone());
        Ok(partition)
    }
}

#[cfg(feature = "external-kafka")]
impl ChangeRecordSink for ExternalKafkaChangeRecordSink {
    fn emit(
        &self,
        shard: &QueueKey,
        records: &[fireweed_engine::ChangeRecord],
    ) -> EngineResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let topic = fjord_topic_name(shard)?;
        let partition = self.partition_client(&topic)?;
        let mut kafka_records = Vec::with_capacity(records.len());
        for record in records {
            let key = change_record_key(record);
            let payload = serde_json::to_vec(record)
                .map_err(|e| EngineError::Storage(format!("serialize change record: {e}")))?;
            // Build headers from the SAME single-source-of-truth `change_record_headers` the embedded
            // encoder uses, so external and embedded consumers receive the identical header key/value set
            // (and the identical key + JSON payload). NOTE: `rskafka::record::Record::headers` is a
            // `BTreeMap`, so rskafka re-sorts header keys on the wire; the ADR-014 insertion order cannot be
            // preserved byte-for-byte through rskafka's producer API. The embedded in-process path (the
            // default) does preserve the pinned order via the Kafka v2 encoder.
            let mut headers = std::collections::BTreeMap::new();
            for (name, value) in change_record_headers(record) {
                headers.insert(name.to_string(), value);
            }
            kafka_records.push(rskafka::record::Record {
                key: Some(key.into_bytes()),
                value: Some(payload),
                headers,
                timestamp: chrono::Utc::now(),
            });
        }
        block_on_sync(async move {
            partition
                .produce(
                    kafka_records,
                    rskafka::client::partition::Compression::NoCompression,
                )
                .await
                .map_err(|e| {
                    EngineError::Storage(format!("produce change record to external kafka: {e}"))
                })?;
            Ok(())
        })
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
    // B2.1: the sink endpoint is the EXTERNAL-Kafka bootstrap axis, orthogonal to the embedded broker's
    // listen bind (`EmbeddedFjordConfig.broker_listen`). Historically a schemeless `host:port` here silently
    // selected external Kafka, which conflated the two axes: ADR-014 external-Kafka mode could not be named
    // without also looking like a broker-endpoint bind. External Kafka must now be requested explicitly with
    // `kafka://`; a schemeless `host:port` (or any other scheme) is rejected with a clear error.
    Err(EngineError::Invalid(
        "change record sink endpoint must use an explicit scheme: `kafka://host:port` for external Kafka \
         or `http://host:port` for durable-ingest; a schemeless `host:port` is rejected",
    ))
}

/// Whether the configured sink uses the in-process embedded fjord surface (the default). `start()` spawns
/// the embedded `HeimqServer` (the external-consumer Kafka surface over the shared log) only in this mode.
pub(crate) fn change_record_sink_is_embedded(config: &ChangeRecordSinkConfig) -> bool {
    matches!(config.mode(), ChangeRecordSinkMode::Embedded)
}

/// Build the runtime sink for the resolved mode. The `Embedded` mode is wired to the shared embedded-broker
/// `log` handle; `Http` and `ExternalKafka` ignore it (they deliver out of process).
fn build_change_record_sink(
    config: &ChangeRecordSinkConfig,
    log: Arc<dyn LogBackend>,
) -> EngineResult<Arc<dyn ChangeRecordSink>> {
    match config.mode() {
        ChangeRecordSinkMode::Embedded => {
            Ok(Arc::new(FjordChangeRecordSink::new(log)) as Arc<dyn ChangeRecordSink>)
        }
        ChangeRecordSinkMode::Http => {
            Ok(Arc::new(NiflheimChangeRecordSink::new(config)?) as Arc<dyn ChangeRecordSink>)
        }
        ChangeRecordSinkMode::ExternalKafka => build_external_kafka_sink(config),
        ChangeRecordSinkMode::Disabled => Err(EngineError::Invalid(
            "change record sink is disabled in config",
        )),
    }
}

/// Build the opt-in external-Kafka sink over the pure-Rust `rskafka` producer. Gated behind the
/// `external-kafka` cargo feature (default-off); without it, selecting a `kafka://` endpoint is a config
/// error that names the feature instead of silently falling back.
#[cfg(feature = "external-kafka")]
fn build_external_kafka_sink(
    config: &ChangeRecordSinkConfig,
) -> EngineResult<Arc<dyn ChangeRecordSink>> {
    Ok(Arc::new(ExternalKafkaChangeRecordSink::new(config)?) as Arc<dyn ChangeRecordSink>)
}

#[cfg(not(feature = "external-kafka"))]
fn build_external_kafka_sink(
    _config: &ChangeRecordSinkConfig,
) -> EngineResult<Arc<dyn ChangeRecordSink>> {
    Err(EngineError::Invalid(
        "external-kafka change record sink requires the `external-kafka` cargo feature (pure-Rust rskafka); \
         the default in-process embedded surface needs no endpoint",
    ))
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

#[cfg(test)]
pub(crate) async fn resolve_change_record_queues<B>(
    backend: &B,
    queues: &[QueueDefinition],
) -> EngineResult<Vec<QueueDefinition>>
where
    B: ControlPlaneStore + ?Sized,
{
    let mut resolved = Vec::new();
    for queue in queues {
        let key = QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
        let definition = backend.queue_definition(&key).await?;
        if definition.emit_change_records {
            resolved.push(definition);
        }
    }
    Ok(resolved)
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
            Ok(emitted) => {
                total += emitted;
            }
            Err(e) => eprintln!(
                "[change-record] emit failed for {}:{}: {e}",
                definition.tenant_id, definition.queue_id
            ),
        }
    }
    Ok(total)
}

/// Stable, de-duplicated queue ring used by the emitter. A tick examines only a bounded page and
/// advances the cursor by the number of entries examined, including entries already in flight.
/// This prevents an early slow shard from pinning the scan while avoiding a full queue-list clone
/// on every tick.
struct ChangeRecordQueueRegistry {
    queues: Vec<QueueKey>,
    cursor: usize,
}

impl ChangeRecordQueueRegistry {
    fn new(queues: Vec<QueueDefinition>) -> Self {
        let mut seen = HashSet::with_capacity(queues.len());
        let queues = queues
            .into_iter()
            .map(|definition| QueueKey::new(definition.tenant_id, definition.queue_id))
            .filter(|shard| seen.insert(shard.clone()))
            .collect();
        Self { queues, cursor: 0 }
    }

    fn next_page(&mut self, active: &HashSet<QueueKey>, capacity: usize) -> Vec<QueueKey> {
        if self.queues.is_empty() || capacity == 0 {
            return Vec::new();
        }
        let inspect_limit = self.queues.len().min(CHANGE_RECORD_REGISTRY_PAGE);
        let mut selected = Vec::with_capacity(capacity.min(inspect_limit));
        let mut inspected = 0;
        while inspected < inspect_limit && selected.len() < capacity {
            let index = (self.cursor + inspected) % self.queues.len();
            let shard = &self.queues[index];
            inspected += 1;
            if !active.contains(shard) {
                selected.push(shard.clone());
            }
        }
        self.cursor = (self.cursor + inspected) % self.queues.len();
        selected
    }
}

async fn emit_change_record_shard<B>(
    backend: Arc<B>,
    sink: Arc<dyn ChangeRecordSink>,
    executor: BoundedBlockingExecutor,
    shard: QueueKey,
    batch_size: usize,
) -> (QueueKey, EngineResult<usize>)
where
    B: ChangeRecordEmissionBackend + ControlPlaneStore + Send + Sync + 'static,
{
    let definition = match backend.queue_definition(&shard).await {
        Ok(definition) => definition,
        Err(error) => return (shard, Err(error)),
    };
    if !definition.emit_change_records {
        return (shard, Ok(0));
    }

    let emit_shard = shard.clone();
    let result = executor
        .execute(move || {
            backend.emit_change_record_tail(
                &emit_shard,
                sink.as_ref(),
                batch_size,
                now_timestamp(),
                None,
            )
        })
        .await;
    (shard, result)
}

pub fn spawn_change_record_emitter<B>(
    backend: Arc<B>,
    sink: Arc<dyn ChangeRecordSink>,
    queues: Vec<QueueDefinition>,
    config: ChangeRecordSinkConfig,
) -> tokio::task::JoinHandle<()>
where
    B: ChangeRecordEmissionBackend + ControlPlaneStore + Send + Sync + 'static,
{
    fireweed_resp::spawn_governed(async move {
        use futures::stream::{FuturesUnordered, StreamExt};

        let executor = BoundedBlockingExecutor::new(CHANGE_RECORD_EMISSION_CONCURRENCY)
            .expect("change-record emission concurrency is nonzero");
        let mut registry = ChangeRecordQueueRegistry::new(queues);
        let mut active = HashSet::new();
        let mut in_flight = FuturesUnordered::new();
        let mut tick = tokio::time::interval(config.tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let capacity = CHANGE_RECORD_EMISSION_CONCURRENCY.saturating_sub(active.len());
                    for shard in registry.next_page(&active, capacity) {
                        active.insert(shard.clone());
                        in_flight.push(emit_change_record_shard(
                            Arc::clone(&backend),
                            Arc::clone(&sink),
                            executor.clone(),
                            shard,
                            config.batch_size,
                        ));
                    }
                }
                Some((shard, result)) = in_flight.next(), if !in_flight.is_empty() => {
                    active.remove(&shard);
                    if let Err(error) = result {
                        eprintln!(
                            "[change-record] emission failed for {}:{}: {error}",
                            shard.tenant_id, shard.queue_id
                        );
                    }
                }
            }
        }
    })
}

fn backend_supports_change_record_cursor<B>(backend: &B) -> bool
where
    B: ChangeRecordEmissionBackend + ?Sized,
{
    backend.supports_change_record_emission_cursor()
}

fn enabled_boot_queues(queues: &[QueueDefinition]) -> Vec<QueueDefinition> {
    queues
        .iter()
        .filter(|queue| queue.emit_change_records)
        .cloned()
        .collect()
}

fn change_record_sink_requires_durable_cursor<B>(backend: &B) -> EngineResult<()>
where
    B: ChangeRecordEmissionBackend + ?Sized,
{
    if backend_supports_change_record_cursor(backend) {
        Ok(())
    } else {
        Err(EngineError::Invalid(
            "change record sink requires a durable emission cursor store",
        ))
    }
}

pub(crate) fn spawn_change_record_emitter_if_enabled<B>(
    backend: Arc<B>,
    queues: &[QueueDefinition],
    config: &ChangeRecordSinkConfig,
    log: Arc<dyn LogBackend>,
) -> EngineResult<Option<JoinHandle<()>>>
where
    B: ChangeRecordEmissionBackend + ControlPlaneStore + Send + Sync + 'static,
{
    if !config.enabled {
        return Ok(None);
    }
    let queues = enabled_boot_queues(queues);
    if queues.is_empty() {
        return Ok(None);
    }
    change_record_sink_requires_durable_cursor(backend.as_ref())?;
    let sink = build_change_record_sink(config, log)?;
    Ok(Some(spawn_change_record_emitter(
        backend,
        sink,
        queues,
        config.clone(),
    )))
}

#[cfg(test)]
fn spawn_change_record_emitter_if_enabled_with_builders<B, FEmbedded, FHttp, FExternal>(
    backend: Arc<B>,
    queues: &[QueueDefinition],
    config: &ChangeRecordSinkConfig,
    build_embedded_sink: FEmbedded,
    build_http_sink: FHttp,
    build_external_sink: FExternal,
) -> EngineResult<Option<JoinHandle<()>>>
where
    B: ChangeRecordEmissionBackend + Send + Sync + 'static,
    FEmbedded: FnOnce(&ChangeRecordSinkConfig) -> EngineResult<Arc<dyn ChangeRecordSink>>,
    FHttp: FnOnce(&ChangeRecordSinkConfig) -> EngineResult<Arc<dyn ChangeRecordSink>>,
    FExternal: FnOnce(&ChangeRecordSinkConfig) -> EngineResult<Arc<dyn ChangeRecordSink>>,
{
    if !config.enabled {
        return Ok(None);
    }
    let queues = enabled_boot_queues(queues);
    if queues.is_empty() {
        return Ok(None);
    }
    let sink = match config.mode() {
        ChangeRecordSinkMode::Embedded => build_embedded_sink(config)?,
        ChangeRecordSinkMode::Http => build_http_sink(config)?,
        ChangeRecordSinkMode::ExternalKafka => build_external_sink(config)?,
        ChangeRecordSinkMode::Disabled => {
            return Err(EngineError::Invalid(
                "change record sink is disabled in config",
            ));
        }
    };
    let tick_interval = config.tick_interval;
    let batch_size = config.batch_size;
    Ok(Some(fireweed_resp::spawn_governed(async move {
        let mut tick = tokio::time::interval(tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let emit_backend = Arc::clone(&backend);
            let emit_sink = Arc::clone(&sink);
            let emit_queues = queues.clone();
            match tokio::task::spawn_blocking(move || {
                emit_change_record_tick(
                    emit_backend.as_ref(),
                    emit_sink.as_ref(),
                    &emit_queues,
                    batch_size,
                )
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => eprintln!("[change-record] emission tick failed: {e}"),
                Err(e) => eprintln!("[change-record] emission task failed: {e}"),
            }
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::ChangeRecordKind;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    #[test]
    fn change_record_sink_defaults_disabled_until_endpoint_is_set() {
        let config = ChangeRecordSinkConfig::default();
        assert!(!config.enabled);
        assert!(config.endpoint.is_none());
        assert_eq!(config.mode(), ChangeRecordSinkMode::Disabled);
        config
            .validate()
            .expect("disabled sink config remains valid");
    }

    #[test]
    fn change_record_sink_selects_embedded_mode_without_endpoint() {
        // The in-process embedded surface is the default: enabled + no endpoint is valid and needs no
        // network endpoint.
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: None,
            ..ChangeRecordSinkConfig::default()
        };
        config
            .validate()
            .expect("embedded (endpoint-less) sink config is valid");
        assert_eq!(config.mode(), ChangeRecordSinkMode::Embedded);
        assert!(change_record_sink_is_embedded(&config));
    }

    #[test]
    fn change_record_sink_external_kafka_mode_uses_rskafka() {
        // A kafka:// endpoint selects the opt-in external-Kafka (rskafka) seam, distinct from the
        // in-process embedded default.
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        config.validate().expect("kafka endpoint validates");
        assert_eq!(config.mode(), ChangeRecordSinkMode::ExternalKafka);
        assert!(!change_record_sink_is_embedded(&config));

        // Without the `external-kafka` feature, building the external sink is a config error that names
        // the feature (no C Kafka client, no silent fallback to the embedded default).
        #[cfg(not(feature = "external-kafka"))]
        {
            match build_external_kafka_sink(&config) {
                Ok(_) => panic!("external-kafka sink must be feature-gated off"),
                Err(err) => assert!(
                    err.to_string().contains("external-kafka"),
                    "error must name the external-kafka feature: {err}"
                ),
            }
        }
        // With the feature, the built sink is the rskafka-backed external sink (construction may fail to
        // connect since no broker is running here — we only assert it selects the rskafka type/path).
        #[cfg(feature = "external-kafka")]
        {
            let _ = ExternalKafkaChangeRecordSink::new(&config);
        }
    }

    #[test]
    fn change_record_sink_rejects_invalid_endpoint_and_keeps_disabled() {
        let config = ChangeRecordSinkConfig {
            endpoint: Some("not-a-url".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        assert!(!config.enabled);
        let err = config.validate().expect_err("malformed endpoint must fail");
        assert!(
            err.to_string().contains("must use an explicit scheme"),
            "{}",
            err
        );
        assert!(!config.enabled);
    }

    #[test]
    fn change_record_sink_recognizes_kafka_endpoints() {
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        config.validate().expect("kafka endpoint should validate");
        assert_eq!(config.mode(), ChangeRecordSinkMode::ExternalKafka);
    }

    #[test]
    fn change_record_sink_config_selects_kafka_producer_path() {
        let embedded = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: None,
            ..ChangeRecordSinkConfig::default()
        };
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

        assert_eq!(embedded.mode(), ChangeRecordSinkMode::Embedded);
        assert_eq!(http.mode(), ChangeRecordSinkMode::Http);
        assert_eq!(kafka.mode(), ChangeRecordSinkMode::ExternalKafka);
        assert!(
            change_record_sink_is_embedded(&embedded),
            "endpoint-less enabled config must select the in-process embedded surface"
        );
        assert!(
            !change_record_sink_is_embedded(&http),
            "http endpoint must keep the niflheim path"
        );
        assert!(
            !change_record_sink_is_embedded(&kafka),
            "kafka endpoint must select the external-kafka seam"
        );
    }

    #[test]
    fn reject_schemeless_bootstrap_endpoints() {
        // B2.1: a schemeless `host:port` must NOT silently select external Kafka. It is rejected so the
        // external-Kafka bootstrap axis can only be named explicitly with `kafka://`, keeping it distinct
        // from the embedded broker's listen bind.
        for endpoint in ["127.0.0.1:9092", "localhost:9092", "broker.internal:9092"] {
            let config = ChangeRecordSinkConfig {
                enabled: true,
                endpoint: Some(endpoint.to_string()),
                ..ChangeRecordSinkConfig::default()
            };
            let err = config
                .validate()
                .expect_err("schemeless bootstrap endpoint must be rejected");
            assert!(
                err.to_string().contains("must use an explicit scheme"),
                "unexpected error for {endpoint}: {err}"
            );
        }
        // An unknown scheme is likewise rejected (not silently downgraded).
        let bad_scheme = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("tcp://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        bad_scheme
            .validate()
            .expect_err("non-http/kafka scheme must be rejected");

        // The explicit `kafka://` form is accepted and selects the external-Kafka mode.
        let explicit = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        explicit.validate().expect("kafka:// bootstrap validates");
        assert_eq!(explicit.mode(), ChangeRecordSinkMode::ExternalKafka);
    }

    #[test]
    fn listen_and_bootstrap_are_independent() {
        use crate::EmbeddedFjordConfig;

        // B2.1: axis (a) is the embedded broker's external-consumer TCP bind
        // (`EmbeddedFjordConfig.broker_listen`); axis (b) is the external-Kafka bootstrap for the
        // change-record sink (`ChangeRecordSinkConfig.endpoint`). They are separate typed fields and
        // neither derives from the other.
        let broker = EmbeddedFjordConfig {
            broker_listen: Some("127.0.0.1:19092".to_string()),
            ..EmbeddedFjordConfig::default()
        };

        // An Embedded sink (no endpoint) alongside a broker_listen bind keeps mode Embedded; the bind
        // address is untouched by the sink config.
        let embedded_sink = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: None,
            ..ChangeRecordSinkConfig::default()
        };
        assert_eq!(embedded_sink.mode(), ChangeRecordSinkMode::Embedded);
        assert_eq!(broker.broker_listen.as_deref(), Some("127.0.0.1:19092"));

        // An external-Kafka sink pointed at a DIFFERENT bootstrap than the broker bind selects
        // ExternalKafka purely from its own `kafka://` endpoint; the broker bind neither changes nor is
        // consulted.
        let external_sink = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://10.0.0.5:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        assert_eq!(external_sink.mode(), ChangeRecordSinkMode::ExternalKafka);
        assert_eq!(broker.broker_listen.as_deref(), Some("127.0.0.1:19092"));
        assert_ne!(
            broker.broker_listen.as_deref(),
            external_sink.endpoint.as_deref(),
            "the two axes carry independent host:port values with no coupling"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_kafka_mode_configures_without_binding_broker_port() {
        use crate::{
            EmbeddedFjordConfig, build_embedded_fjord_surface, maybe_spawn_embedded_broker,
        };

        // B2.1 / ADR-014: even with a broker_listen bind configured, an ExternalKafka sink must configure
        // WITHOUT binding the embedded broker's TCP surface. The embedded surface is spawned only for the
        // in-process Embedded mode; the external-Kafka producer connects out to its own bootstrap instead.
        let surface = build_embedded_fjord_surface(0, &EmbeddedFjordConfig::default());
        let external_sink = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: Some("kafka://127.0.0.1:9092".to_string()),
            ..ChangeRecordSinkConfig::default()
        };
        assert_eq!(external_sink.mode(), ChangeRecordSinkMode::ExternalKafka);

        // Had the ExternalKafka path erroneously bound the embedded surface, this would spawn a listener on
        // the loopback bind; instead it must return None and leave the port unbound.
        let handle =
            maybe_spawn_embedded_broker(&surface, Some("127.0.0.1:0"), &external_sink, &[])
                .await
                .expect("external-kafka mode configures without error");
        assert!(
            handle.is_none(),
            "external-kafka sink mode must NOT bind the embedded broker surface"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn change_record_emitter_starts_chosen_sink_only() {
        #[derive(Default)]
        struct NoopBackend;

        impl ChangeRecordEmissionBackend for NoopBackend {
            fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
                &self,
                _shard: &QueueKey,
                _sink: &S,
                _limit: usize,
                _emitted_at: UtcTimestamp,
                _source_owner_id: Option<fireweed_core::OwnerId>,
            ) -> EngineResult<usize> {
                Ok(0)
            }
        }

        let backend = Arc::new(NoopBackend);
        let queues = vec![queue_definition("tenant-a", "queue-a", true)];
        let embedded_calls = Arc::new(AtomicUsize::new(0));
        let http_calls = Arc::new(AtomicUsize::new(0));
        let external_calls = Arc::new(AtomicUsize::new(0));
        // Endpoint-less enabled config selects the in-process embedded surface.
        let config = ChangeRecordSinkConfig {
            enabled: true,
            endpoint: None,
            tick_interval: Duration::from_millis(1),
            ..ChangeRecordSinkConfig::default()
        };

        let handle = spawn_change_record_emitter_if_enabled_with_builders(
            backend,
            &queues,
            &config,
            {
                let embedded_calls = Arc::clone(&embedded_calls);
                move |_| {
                    embedded_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(RecordingSink::default()) as Arc<dyn ChangeRecordSink>)
                }
            },
            {
                let http_calls = Arc::clone(&http_calls);
                move |_| {
                    http_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(RecordingSink::default()) as Arc<dyn ChangeRecordSink>)
                }
            },
            {
                let external_calls = Arc::clone(&external_calls);
                move |_| {
                    external_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(RecordingSink::default()) as Arc<dyn ChangeRecordSink>)
                }
            },
        )
        .expect("emitter should start")
        .expect("enabled config with emit-change-record queues should spawn");

        assert_eq!(embedded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(http_calls.load(Ordering::SeqCst), 0);
        assert_eq!(external_calls.load(Ordering::SeqCst), 0);
        handle.abort();
    }

    struct MutableControlPlane {
        queue: Mutex<QueueDefinition>,
    }

    impl MutableControlPlane {
        fn new(queue: QueueDefinition) -> Self {
            Self {
                queue: Mutex::new(queue),
            }
        }

        fn set_emit_change_records(&self, enabled: bool) {
            self.queue.lock().expect("poisoned").emit_change_records = enabled;
        }
    }

    impl ControlPlaneStore for MutableControlPlane {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CreateQueueOutcome>> + Send
        {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn queue_definition(
            &self,
            _key: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
            std::future::ready(Ok(self.queue.lock().expect("poisoned").clone()))
        }

        fn list_queues(
            &self,
            _tenant: &fireweed_core::TenantId,
        ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_core::QueueId>>> + Send
        {
            std::future::ready(Ok(Vec::new()))
        }

        fn current_epoch(
            &self,
            _shard: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn acquire_epoch(
            &self,
            _shard: &QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }
    }

    #[test]
    fn resolve_change_record_queues_uses_control_plane_definitions() {
        let queue = queue_definition("tenant-a", "queue-a", false);
        let backend = MutableControlPlane::new(queue.clone());
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        let resolved = runtime
            .block_on(resolve_change_record_queues(
                &backend,
                std::slice::from_ref(&queue),
            ))
            .expect("resolve definitions");
        assert!(resolved.is_empty());

        backend.set_emit_change_records(true);
        let resolved = runtime
            .block_on(resolve_change_record_queues(
                &backend,
                std::slice::from_ref(&queue),
            ))
            .expect("resolve definitions");
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].emit_change_records);
    }

    #[test]
    fn change_record_sink_writes_tenant_queue_and_custom_headers() {
        let endpoint = ParsedEndpoint {
            host: "127.0.0.1".to_string(),
            port: 8080,
            path: "/ingest".to_string(),
        };
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant-a").unwrap(),
            fireweed_core::QueueId::new("queue-a").unwrap(),
        );
        let mut headers = BTreeMap::new();
        headers.insert("authorization".to_string(), "Bearer test".to_string());
        headers.insert("x-custom-header".to_string(), "custom-value".to_string());
        let records = vec![fireweed_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: None,
            position: fireweed_engine::ChangeRecordPosition {
                backend_epoch: 7,
                sequence: 42,
            },
            command_kind: fireweed_engine::ChangeRecordKind::PauseQueue,
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
    fn command_kind_wire_serialization_is_stable() {
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant-a").unwrap(),
            fireweed_core::QueueId::new("queue-a").unwrap(),
        );
        let record = fireweed_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: None,
            position: fireweed_engine::ChangeRecordPosition {
                backend_epoch: 7,
                sequence: 42,
            },
            command_kind: fireweed_engine::ChangeRecordKind::PauseQueue,
            new_state: None,
            item_version: None,
            terminal_at: None,
            emitted_at: Some(UtcTimestamp::new(1, 0).unwrap()),
            source_owner_id: None,
            source_epoch: 7,
        };

        assert_eq!(
            change_record_kind_wire_value(fireweed_engine::ChangeRecordKind::Push),
            "push"
        );
        assert_eq!(
            change_record_kind_wire_value(fireweed_engine::ChangeRecordKind::Finalize),
            "finalize"
        );

        let headers = change_record_headers(&record);
        let command_kind = &headers[4];
        assert_eq!(command_kind.0, "pq-command-kind");
        assert_eq!(command_kind.1, b"pause-queue".to_vec());
    }

    #[test]
    fn record_key_includes_pq_item_id() {
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant-a").unwrap(),
            fireweed_core::QueueId::new("queue-a").unwrap(),
        );
        let with_item = fireweed_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: Some(fireweed_core::ItemId::from_u64(17)),
            position: fireweed_engine::ChangeRecordPosition {
                backend_epoch: 9,
                sequence: 3,
            },
            command_kind: fireweed_engine::ChangeRecordKind::Push,
            new_state: Some(fireweed_engine::ChangeRecordState::Pending),
            item_version: Some(1),
            terminal_at: None,
            emitted_at: Some(UtcTimestamp::new(1, 0).unwrap()),
            source_owner_id: None,
            source_epoch: 9,
        };
        let without_item = fireweed_engine::ChangeRecord {
            item_id: None,
            ..with_item.clone()
        };

        assert_eq!(change_record_key(&with_item), "17:9:3");
        assert_eq!(change_record_key(&without_item), ":9:3");

        // ADR-014:116 pinned order: tenant, queue, item-id, backend-epoch, sequence, command-kind.
        let headers_with_item = change_record_headers(&with_item);
        let keys: Vec<&str> = headers_with_item.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys,
            vec![
                "pq-tenant-id",
                "pq-queue-id",
                "pq-item-id",
                "pq-backend-epoch",
                "pq-sequence",
                "pq-command-kind",
            ]
        );
        let pq_item_id = &headers_with_item[2];
        assert_eq!(pq_item_id.0, "pq-item-id");
        assert_eq!(pq_item_id.1, b"17".to_vec());

        // Queue-scoped record: pq-item-id omitted entirely, the rest keep their pinned order.
        let headers_without_item = change_record_headers(&without_item);
        let keys_without: Vec<&str> = headers_without_item.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys_without,
            vec![
                "pq-tenant-id",
                "pq-queue-id",
                "pq-backend-epoch",
                "pq-sequence",
                "pq-command-kind",
            ]
        );
    }

    fn sample_change_record(
        tenant: &str,
        queue: &str,
        item_id: Option<u64>,
        backend_epoch: u64,
        sequence: u64,
        kind: fireweed_engine::ChangeRecordKind,
    ) -> fireweed_engine::ChangeRecord {
        fireweed_engine::ChangeRecord {
            tenant_id: fireweed_core::TenantId::new(tenant).unwrap(),
            queue_id: fireweed_core::QueueId::new(queue).unwrap(),
            item_id: item_id.map(fireweed_core::ItemId::from_u64),
            position: fireweed_engine::ChangeRecordPosition {
                backend_epoch,
                sequence,
            },
            command_kind: kind,
            new_state: Some(fireweed_engine::ChangeRecordState::Pending),
            item_version: Some(1),
            terminal_at: None,
            emitted_at: Some(UtcTimestamp::new(1, 0).unwrap()),
            source_owner_id: None,
            source_epoch: backend_epoch,
        }
    }

    #[test]
    fn change_record_batch_round_trips_through_record_batch_view() {
        use heimq_broker::storage::RecordBatchView;
        let records = vec![
            sample_change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push),
            sample_change_record(
                "tenant-a",
                "queue-a",
                Some(2),
                7,
                2,
                ChangeRecordKind::Claim,
            ),
            sample_change_record(
                "tenant-a",
                "queue-a",
                Some(3),
                7,
                3,
                ChangeRecordKind::Finalize,
            ),
        ];
        let batch = encode_change_record_batch(&records).expect("encode batch");
        let view = RecordBatchView::from_bytes(&batch).expect("decode via heimq record-batch view");
        assert_eq!(view.record_count(), 3);
        assert_eq!(view.base_offset(), 0);
        for (idx, record_view) in view.records().enumerate() {
            assert_eq!(record_view.offset_delta, idx as i32);
            let payload = record_view.value.expect("payload present");
            let decoded: fireweed_engine::ChangeRecord =
                serde_json::from_slice(payload).expect("payload is ChangeRecord json");
            assert_eq!(decoded, records[idx]);
        }
    }

    #[test]
    fn change_record_batch_encodes_key_headers_payload_partition() {
        use heimq_broker::storage::RecordBatchView;
        let record = sample_change_record(
            "tenant-a",
            "queue-a",
            Some(17),
            9,
            3,
            ChangeRecordKind::Push,
        );
        let batch =
            encode_change_record_batch(std::slice::from_ref(&record)).expect("encode batch");
        let view = RecordBatchView::from_bytes(&batch).expect("decode batch");
        let record_view = view.records().next().expect("one record");

        // Key: TD-008 idempotency identity "{item_id}:{backend_epoch}:{sequence}".
        assert_eq!(
            record_view.key.map(|b| b.as_ref().to_vec()),
            Some(b"17:9:3".to_vec())
        );
        // Payload: the TD-008 ChangeRecord JSON.
        let decoded: fireweed_engine::ChangeRecord =
            serde_json::from_slice(record_view.value.expect("payload")).expect("json");
        assert_eq!(decoded, record);
        // Headers: the pinned pq-* order.
        let headers: Vec<(String, Option<Vec<u8>>)> = record_view
            .headers()
            .map(|(k, v)| (k.to_string(), v.map(|b| b.to_vec())))
            .collect();
        // ADR-014:116 pinned wire order: pq-item-id sits third (before backend-epoch/sequence/command-kind).
        assert_eq!(
            headers,
            vec![
                ("pq-tenant-id".to_string(), Some(b"tenant-a".to_vec())),
                ("pq-queue-id".to_string(), Some(b"queue-a".to_vec())),
                ("pq-item-id".to_string(), Some(b"17".to_vec())),
                ("pq-backend-epoch".to_string(), Some(b"9".to_vec())),
                ("pq-sequence".to_string(), Some(b"3".to_vec())),
                ("pq-command-kind".to_string(), Some(b"push".to_vec())),
            ]
        );
    }

    #[test]
    fn append_creates_topic_when_absent() {
        // A fresh embedded log has no topics; the sink must create the queue topic before appending
        // (FjordLog::append returns TopicNotFound otherwise).
        let log: Arc<dyn LogBackend> = Arc::new(fjord::FjordLog::new());
        let sink = FjordChangeRecordSink::new(Arc::clone(&log));
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant-a").unwrap(),
            fireweed_core::QueueId::new("queue-a").unwrap(),
        );
        assert!(
            log.topic("tenant-a.queue-a").is_none(),
            "topic must be absent before emit"
        );
        let record =
            sample_change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);
        sink.emit(&shard, std::slice::from_ref(&record))
            .expect("emit creates topic and appends");
        assert!(
            log.topic("tenant-a.queue-a").is_some(),
            "topic must exist after emit"
        );
        assert_eq!(
            log.high_watermark("tenant-a.queue-a", 0).expect("hwm"),
            1,
            "one record appended at partition 0"
        );
    }

    #[derive(Default)]
    struct RecordingEmissionBackend {
        emitted_shards: Mutex<Vec<QueueKey>>,
        cursor_advances: Mutex<Vec<QueueKey>>,
    }

    #[derive(Default)]
    struct BatchRecordingEmissionBackend {
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
            _source_owner_id: Option<fireweed_core::OwnerId>,
        ) -> EngineResult<usize> {
            self.emitted_shards
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            self.cursor_advances
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            let record = fireweed_engine::ChangeRecord {
                tenant_id: shard.tenant_id.clone(),
                queue_id: shard.queue_id.clone(),
                item_id: Some(fireweed_core::ItemId::from_u64(1)),
                position: fireweed_engine::ChangeRecordPosition {
                    backend_epoch: 7,
                    sequence: 1,
                },
                command_kind: fireweed_engine::ChangeRecordKind::Push,
                new_state: Some(fireweed_engine::ChangeRecordState::Pending),
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

    impl ChangeRecordEmissionBackend for BatchRecordingEmissionBackend {
        fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
            &self,
            shard: &QueueKey,
            sink: &S,
            _limit: usize,
            emitted_at: UtcTimestamp,
            _source_owner_id: Option<fireweed_core::OwnerId>,
        ) -> EngineResult<usize> {
            self.emitted_shards
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            self.cursor_advances
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            let records = vec![
                fireweed_engine::ChangeRecord {
                    tenant_id: shard.tenant_id.clone(),
                    queue_id: shard.queue_id.clone(),
                    item_id: Some(fireweed_core::ItemId::from_u64(1)),
                    position: fireweed_engine::ChangeRecordPosition {
                        backend_epoch: 7,
                        sequence: 1,
                    },
                    command_kind: fireweed_engine::ChangeRecordKind::Push,
                    new_state: Some(fireweed_engine::ChangeRecordState::Pending),
                    item_version: Some(1),
                    terminal_at: None,
                    emitted_at: Some(emitted_at),
                    source_owner_id: None,
                    source_epoch: 7,
                },
                fireweed_engine::ChangeRecord {
                    tenant_id: shard.tenant_id.clone(),
                    queue_id: shard.queue_id.clone(),
                    item_id: Some(fireweed_core::ItemId::from_u64(2)),
                    position: fireweed_engine::ChangeRecordPosition {
                        backend_epoch: 7,
                        sequence: 2,
                    },
                    command_kind: fireweed_engine::ChangeRecordKind::Claim,
                    new_state: Some(fireweed_engine::ChangeRecordState::Leased),
                    item_version: Some(1),
                    terminal_at: None,
                    emitted_at: Some(emitted_at),
                    source_owner_id: None,
                    source_epoch: 7,
                },
            ];
            sink.emit(shard, &records)?;
            Ok(records.len())
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        emitted_shards: Mutex<Vec<QueueKey>>,
        batch_sizes: Mutex<Vec<usize>>,
    }

    impl ChangeRecordSink for RecordingSink {
        fn emit(
            &self,
            shard: &QueueKey,
            records: &[fireweed_engine::ChangeRecord],
        ) -> EngineResult<()> {
            self.emitted_shards
                .lock()
                .expect("poisoned")
                .push(shard.clone());
            self.batch_sizes
                .lock()
                .expect("poisoned")
                .push(records.len());
            Ok(())
        }
    }

    fn queue_definition(tenant: &str, queue: &str, emit_change_records: bool) -> QueueDefinition {
        QueueDefinition {
            tenant_id: fireweed_core::TenantId::new(tenant).unwrap(),
            queue_id: fireweed_core::QueueId::new(queue).unwrap(),
            priority_model: fireweed_core::PriorityModel {
                kind: fireweed_core::PriorityModelKind::Int64,
                direction: fireweed_core::PriorityDirection::Ascending,
                tie_breaker: fireweed_core::PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: fireweed_core::OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: fireweed_core::EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: fireweed_core::RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: fireweed_core::RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records,
        }
    }

    struct FairEmissionBackend {
        definitions: HashMap<QueueKey, QueueDefinition>,
        blocked_shard: Option<QueueKey>,
        blocked_definition: Option<QueueKey>,
        definition_release: Arc<tokio::sync::Notify>,
        release: Arc<(Mutex<bool>, Condvar)>,
        emitted: Mutex<HashSet<QueueKey>>,
        current: AtomicUsize,
        max_current: AtomicUsize,
    }

    impl FairEmissionBackend {
        fn new(definitions: &[QueueDefinition], blocked_shard: Option<QueueKey>) -> Self {
            Self {
                definitions: definitions
                    .iter()
                    .cloned()
                    .map(|definition| {
                        (
                            QueueKey::new(
                                definition.tenant_id.clone(),
                                definition.queue_id.clone(),
                            ),
                            definition,
                        )
                    })
                    .collect(),
                blocked_shard,
                blocked_definition: None,
                definition_release: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new((Mutex::new(false), Condvar::new())),
                emitted: Mutex::new(HashSet::new()),
                current: AtomicUsize::new(0),
                max_current: AtomicUsize::new(0),
            }
        }

        fn release_blocked(&self) {
            let (released, wake) = self.release.as_ref();
            *released.lock().expect("poisoned") = true;
            wake.notify_all();
        }

        fn with_blocked_definition(mut self, shard: QueueKey) -> Self {
            self.blocked_definition = Some(shard);
            self
        }
    }

    impl ChangeRecordEmissionBackend for FairEmissionBackend {
        fn emit_change_record_tail<S: ChangeRecordSink + ?Sized>(
            &self,
            shard: &QueueKey,
            _sink: &S,
            _limit: usize,
            _emitted_at: UtcTimestamp,
            _source_owner_id: Option<fireweed_core::OwnerId>,
        ) -> EngineResult<usize> {
            let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_current.fetch_max(current, Ordering::SeqCst);
            if self.blocked_shard.as_ref() == Some(shard) {
                let (released, wake) = self.release.as_ref();
                let mut released = released.lock().expect("poisoned");
                while !*released {
                    released = wake.wait(released).expect("poisoned");
                }
            }
            self.emitted.lock().expect("poisoned").insert(shard.clone());
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(1)
        }
    }

    impl ControlPlaneStore for FairEmissionBackend {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> impl Future<Output = EngineResult<fireweed_engine::CreateQueueOutcome>> + Send
        {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn queue_definition(
            &self,
            key: &QueueKey,
        ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
            let blocked = self.blocked_definition.as_ref() == Some(key);
            let release = Arc::clone(&self.definition_release);
            let definition = self
                .definitions
                .get(key)
                .cloned()
                .ok_or(EngineError::Unavailable);
            async move {
                if blocked {
                    release.notified().await;
                }
                definition
            }
        }

        fn list_queues(
            &self,
            _tenant: &fireweed_core::TenantId,
        ) -> impl Future<Output = EngineResult<Vec<fireweed_core::QueueId>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn current_epoch(
            &self,
            _shard: &QueueKey,
        ) -> impl Future<Output = EngineResult<u64>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn acquire_epoch(
            &self,
            _shard: &QueueKey,
        ) -> impl Future<Output = EngineResult<u64>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_shard_does_not_block_later_change_record_queue() {
        let definitions = vec![
            queue_definition("tenant-a", "queue-a", true),
            queue_definition("tenant-b", "queue-b", true),
        ];
        let blocked = QueueKey::new(
            definitions[0].tenant_id.clone(),
            definitions[0].queue_id.clone(),
        );
        let later = QueueKey::new(
            definitions[1].tenant_id.clone(),
            definitions[1].queue_id.clone(),
        );
        let backend = Arc::new(FairEmissionBackend::new(
            &definitions,
            Some(blocked.clone()),
        ));
        let handle = spawn_change_record_emitter(
            Arc::clone(&backend),
            Arc::new(RecordingSink::default()),
            definitions,
            ChangeRecordSinkConfig {
                enabled: true,
                tick_interval: Duration::from_millis(1),
                ..ChangeRecordSinkConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let later_advanced = {
                    let emitted = backend.emitted.lock().expect("poisoned");
                    if emitted.contains(&later) {
                        assert!(!emitted.contains(&blocked));
                        true
                    } else {
                        false
                    }
                };
                if later_advanced {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("later queue must advance while the first shard is stalled");

        backend.release_blocked();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if backend.emitted.lock().expect("poisoned").contains(&blocked) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("released blocking provider must finish before test shutdown");
        handle.abort();
        assert!(
            handle
                .await
                .expect_err("emitter should be cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_control_plane_lookup_does_not_block_later_queue() {
        let definitions = vec![
            queue_definition("tenant-a", "queue-a", true),
            queue_definition("tenant-b", "queue-b", true),
        ];
        let blocked = QueueKey::new(
            definitions[0].tenant_id.clone(),
            definitions[0].queue_id.clone(),
        );
        let later = QueueKey::new(
            definitions[1].tenant_id.clone(),
            definitions[1].queue_id.clone(),
        );
        let backend = Arc::new(
            FairEmissionBackend::new(&definitions, None).with_blocked_definition(blocked.clone()),
        );
        let handle = spawn_change_record_emitter(
            Arc::clone(&backend),
            Arc::new(RecordingSink::default()),
            definitions,
            ChangeRecordSinkConfig {
                enabled: true,
                tick_interval: Duration::from_millis(1),
                ..ChangeRecordSinkConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let later_advanced = {
                    let emitted = backend.emitted.lock().expect("poisoned");
                    if emitted.contains(&later) {
                        assert!(!emitted.contains(&blocked));
                        true
                    } else {
                        false
                    }
                };
                if later_advanced {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("later queue must advance while the first definition lookup is stalled");

        // Aborting the emitter drops the unresolved async lookup. No synchronous provider work was
        // started for this shard, and the scheduler cannot admit any new work after shutdown.
        handle.abort();
        assert!(
            handle
                .await
                .expect_err("emitter should be cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn change_record_registry_pages_all_queues_with_bounded_work() {
        let definitions: Vec<_> = (0..CHANGE_RECORD_REGISTRY_PAGE + 5)
            .map(|index| queue_definition("tenant", &format!("queue-{index:03}"), true))
            .collect();
        let backend = Arc::new(FairEmissionBackend::new(&definitions, None));
        let handle = spawn_change_record_emitter(
            Arc::clone(&backend),
            Arc::new(RecordingSink::default()),
            definitions.clone(),
            ChangeRecordSinkConfig {
                enabled: true,
                tick_interval: Duration::from_millis(1),
                ..ChangeRecordSinkConfig::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if backend.emitted.lock().expect("poisoned").len() == definitions.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("persistent registry cursor must eventually visit queues beyond one page");
        assert!(
            backend.max_current.load(Ordering::SeqCst) <= CHANGE_RECORD_EMISSION_CONCURRENCY,
            "synchronous provider work must remain within its fixed admission bound"
        );

        handle.abort();
        assert!(
            handle
                .await
                .expect_err("emitter should be cancelled")
                .is_cancelled()
        );
    }

    #[test]
    fn emit_change_record_tick_skips_opted_out_queues() {
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
    fn emit_change_record_tick_does_not_advance_cursor_for_opt_out() {
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
    fn produces_batch_records_without_block_in_place() {
        let backend = BatchRecordingEmissionBackend::default();
        let sink = RecordingSink::default();
        let enabled_def = queue_definition("tenant-a", "queue-a", true);

        let emitted = emit_change_record_tick(&backend, &sink, &[enabled_def], 64)
            .expect("tick should succeed");

        assert_eq!(emitted, 2);
        assert_eq!(sink.batch_sizes.lock().expect("poisoned").as_slice(), &[2]);
    }

    #[test]
    fn change_record_sink_builds_delivery_request_with_tenant_and_queue_metadata() {
        let endpoint = ParsedEndpoint {
            host: "127.0.0.1".to_string(),
            port: 8080,
            path: "/ingest".to_string(),
        };
        let shard = QueueKey::new(
            fireweed_core::TenantId::new("tenant-a").unwrap(),
            fireweed_core::QueueId::new("queue-a").unwrap(),
        );
        let headers = BTreeMap::new();
        let records = vec![fireweed_engine::ChangeRecord {
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            item_id: Some(fireweed_core::ItemId::from_u64(17)),
            position: fireweed_engine::ChangeRecordPosition {
                backend_epoch: 9,
                sequence: 3,
            },
            command_kind: fireweed_engine::ChangeRecordKind::Finalize,
            new_state: Some(fireweed_engine::ChangeRecordState::Complete),
            item_version: Some(2),
            terminal_at: Some(UtcTimestamp::new(2, 0).unwrap()),
            emitted_at: Some(UtcTimestamp::new(3, 0).unwrap()),
            source_owner_id: Some(fireweed_core::OwnerId::new("node-a").unwrap()),
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
        let parsed: Vec<fireweed_engine::ChangeRecord> =
            serde_json::from_str(body).expect("request body should be valid json");
        assert_eq!(parsed, records);
    }
}
