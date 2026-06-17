#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fjord_coordinator::{CommitOutcome, CoordinatorStore};
use fjord_log::{BlobStore, FetchedBatch, ProduceBatch, ReadPath, WritePath};
use parking_lot::Mutex;
use pqueue_core::{
    ClientItemKey, DecimalValue, ItemId, PriorityValue, QueueId, RequestId, TenantId, UtcTimestamp,
};
use pqueue_storage::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, BatchRenewLeasesCommand,
    BatchUpdateCommand, CohortExpiredCommand, CommandEnvelope, CommandId, FinalizeKind,
    FinalizeOutcome, LeaseExpiredCommand, PurgeItemsCommand, PushItem, QueueCommand,
};
use pqueue_storage::traits::{
    AppendBatchResult, CommandPage, DurabilityProfile, LogStore, LogStoreError,
};
use pqueue_storage::types::{CommandChecksum, CommandPosition, ShardKey};

pub use fjord_coordinator::memory::MemoryCoordinator;
pub use fjord_log::MemoryBlobStore;
pub use fjord_log::s3::S3BlobStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentProfile {
    Production,
    Development,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestMode {
    ObjectStoreCas,
    PostgresManifestPointerFallback,
    NoConditionalWrite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqueueObjectLogConfig {
    pub deployment_profile: DeploymentProfile,
    pub manifest_mode: ManifestMode,
    pub max_commands_per_segment: usize,
    pub dev_unsafe_one_command_segments: bool,
}

impl Default for PqueueObjectLogConfig {
    fn default() -> Self {
        Self {
            deployment_profile: DeploymentProfile::Production,
            manifest_mode: ManifestMode::ObjectStoreCas,
            max_commands_per_segment: 1024,
            dev_unsafe_one_command_segments: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    EmptySegment,
    OneCommandSegmentInProduction,
    DevUnsafeFlagInProduction,
    MissingConditionalWriteWithoutFallback,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySegment => write!(f, "max_commands_per_segment must be greater than zero"),
            Self::OneCommandSegmentInProduction => {
                write!(f, "one-command object segments are rejected in production")
            }
            Self::DevUnsafeFlagInProduction => {
                write!(
                    f,
                    "dev_unsafe_one_command_segments cannot be set in production"
                )
            }
            Self::MissingConditionalWriteWithoutFallback => write!(
                f,
                "object store without conditional write requires Postgres manifest pointer fallback"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

impl PqueueObjectLogConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_commands_per_segment == 0 {
            return Err(ConfigError::EmptySegment);
        }
        if self.deployment_profile == DeploymentProfile::Production
            && self.dev_unsafe_one_command_segments
        {
            return Err(ConfigError::DevUnsafeFlagInProduction);
        }
        if self.deployment_profile == DeploymentProfile::Production
            && self.max_commands_per_segment == 1
        {
            return Err(ConfigError::OneCommandSegmentInProduction);
        }
        if self.manifest_mode == ManifestMode::NoConditionalWrite {
            return Err(ConfigError::MissingConditionalWriteWithoutFallback);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleObjectLogConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub region: String,
    pub credentials: S3CompatibleCredentials,
    pub force_path_style: bool,
    pub deployment_profile: DeploymentProfile,
    pub manifest_mode: ManifestMode,
    pub max_commands_per_segment: usize,
    pub dev_unsafe_one_command_segments: bool,
}

impl S3CompatibleObjectLogConfig {
    pub fn pqueue_config(&self) -> PqueueObjectLogConfig {
        PqueueObjectLogConfig {
            deployment_profile: self.deployment_profile,
            manifest_mode: self.manifest_mode,
            max_commands_per_segment: self.max_commands_per_segment,
            dev_unsafe_one_command_segments: self.dev_unsafe_one_command_segments,
        }
    }

    pub fn validate(&self) -> Result<(), S3CompatibleConfigError> {
        validate_endpoint_url(&self.endpoint_url)?;
        validate_bucket(&self.bucket)?;
        validate_region(&self.region)?;
        validate_credentials(&self.credentials)?;
        if !self.force_path_style {
            return Err(S3CompatibleConfigError::UnsupportedAddressingMode);
        }
        self.pqueue_config()
            .validate()
            .map_err(S3CompatibleConfigError::ObjectLog)?;
        Ok(())
    }

    pub fn blob_store(&self) -> Result<S3BlobStore, S3CompatibleConfigError> {
        self.validate()?;
        Ok(S3BlobStore::new(
            self.endpoint_url.trim(),
            self.region.trim(),
            self.bucket.trim(),
            self.credentials.access_key_id.trim(),
            self.credentials.secret_access_key.trim(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3CompatibleConfigError {
    MissingEndpoint,
    InvalidEndpoint,
    MissingBucket,
    InvalidBucket,
    MissingRegion,
    MissingCredentials,
    UnsupportedAddressingMode,
    ObjectLog(ConfigError),
}

impl std::fmt::Display for S3CompatibleConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEndpoint => write!(f, "S3-compatible endpoint URL is required"),
            Self::InvalidEndpoint => write!(
                f,
                "S3-compatible endpoint URL must be http(s) with a non-empty host"
            ),
            Self::MissingBucket => write!(f, "S3-compatible bucket is required"),
            Self::InvalidBucket => write!(f, "S3-compatible bucket name is invalid"),
            Self::MissingRegion => write!(f, "S3-compatible region is required"),
            Self::MissingCredentials => {
                write!(f, "S3-compatible access key and secret key are required")
            }
            Self::UnsupportedAddressingMode => write!(
                f,
                "pqueue-objectlog S3-compatible runtime currently requires path-style addressing"
            ),
            Self::ObjectLog(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for S3CompatibleConfigError {}

impl From<ConfigError> for S3CompatibleConfigError {
    fn from(value: ConfigError) -> Self {
        Self::ObjectLog(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3CompatibleProbeError {
    Config(S3CompatibleConfigError),
    Put(String),
    Get(String),
    MissingProbeObject,
    ProbePayloadMismatch,
}

impl std::fmt::Display for S3CompatibleProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::Put(err) => write!(f, "S3-compatible object-log PUT probe failed: {err}"),
            Self::Get(err) => write!(f, "S3-compatible object-log GET probe failed: {err}"),
            Self::MissingProbeObject => {
                write!(f, "S3-compatible object-log probe object is missing")
            }
            Self::ProbePayloadMismatch => {
                write!(f, "S3-compatible object-log probe payload mismatch")
            }
        }
    }
}

impl std::error::Error for S3CompatibleProbeError {}

impl From<S3CompatibleConfigError> for S3CompatibleProbeError {
    fn from(value: S3CompatibleConfigError) -> Self {
        Self::Config(value)
    }
}

pub fn probe_s3_compatible_object_path(
    config: &S3CompatibleObjectLogConfig,
    key: &str,
    payload: &[u8],
) -> Result<(), S3CompatibleProbeError> {
    let blob = config.blob_store()?;
    blob.put(key, payload.to_vec())
        .map_err(S3CompatibleProbeError::Put)?;
    let fetched = blob.get(key).map_err(S3CompatibleProbeError::Get)?;
    match fetched {
        Some(bytes) if bytes == payload => Ok(()),
        Some(_) => Err(S3CompatibleProbeError::ProbePayloadMismatch),
        None => Err(S3CompatibleProbeError::MissingProbeObject),
    }
}

pub struct FjordObjectLogStore {
    coordinator: Arc<dyn CoordinatorStore>,
    writer: WritePath,
    reader: ReadPath,
    config: PqueueObjectLogConfig,
    epochs: Mutex<HashMap<ShardKey, u64>>,
    topics: Mutex<HashSet<String>>,
}

impl FjordObjectLogStore {
    pub fn new(coordinator: Arc<dyn CoordinatorStore>, blob: Arc<dyn BlobStore>) -> Self {
        Self::new_with_config(coordinator, blob, PqueueObjectLogConfig::default())
            .expect("default pqueue object-log config is valid")
    }

    pub fn new_with_config(
        coordinator: Arc<dyn CoordinatorStore>,
        blob: Arc<dyn BlobStore>,
        config: PqueueObjectLogConfig,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            writer: WritePath::new(Arc::clone(&coordinator), Arc::clone(&blob)),
            reader: ReadPath::new(Arc::clone(&coordinator), blob),
            coordinator,
            config,
            epochs: Mutex::new(HashMap::new()),
            topics: Mutex::new(HashSet::new()),
        })
    }

    pub fn new_s3_compatible(
        coordinator: Arc<dyn CoordinatorStore>,
        config: S3CompatibleObjectLogConfig,
    ) -> Result<Self, S3CompatibleConfigError> {
        config.validate()?;
        let pqueue_config = config.pqueue_config();
        let blob: Arc<dyn BlobStore> = Arc::new(S3BlobStore::new(
            config.endpoint_url.trim(),
            config.region.trim(),
            config.bucket.trim(),
            config.credentials.access_key_id.trim(),
            config.credentials.secret_access_key.trim(),
        ));
        Self::new_with_config(coordinator, blob, pqueue_config).map_err(Into::into)
    }

    pub fn new_memory() -> (Self, Arc<MemoryBlobStore>) {
        let coordinator: Arc<dyn CoordinatorStore> = Arc::new(MemoryCoordinator::new());
        let blob = Arc::new(MemoryBlobStore::new());
        let blob_dyn: Arc<dyn BlobStore> = blob.clone();
        (Self::new(coordinator, blob_dyn), blob)
    }

    pub fn config(&self) -> &PqueueObjectLogConfig {
        &self.config
    }

    pub fn advance_epoch(&self, shard: &ShardKey, epoch: u64) {
        self.commit_epoch_fence(shard, epoch)
            .expect("epoch fence commit must succeed");
    }

    pub fn commit_epoch_fence(&self, shard: &ShardKey, epoch: u64) -> Result<(), LogStoreError> {
        let topic = epoch_topic_for_shard(shard);
        self.ensure_topic(&topic)?;
        self.writer
            .produce(&[ProduceBatch {
                topic,
                partition: 0,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                record_count: 1,
                payload: epoch.to_be_bytes().to_vec(),
            }])
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
        self.epochs.lock().insert(shard.clone(), epoch);
        Ok(())
    }

    pub fn find_by_request_id(
        &self,
        shard: &ShardKey,
        request_id: &RequestId,
    ) -> Result<Option<(CommandPosition, CommandEnvelope)>, LogStoreError> {
        let page = self.read_all(shard)?;
        Ok(page
            .commands
            .into_iter()
            .find(|(_, envelope)| envelope.request_id.as_ref() == Some(request_id)))
    }

    fn current_epoch(&self, shard: &ShardKey) -> Result<u64, LogStoreError> {
        if let Some(epoch) = self.epochs.lock().get(shard).copied() {
            return Ok(epoch);
        }
        let topic = epoch_topic_for_shard(shard);
        if self
            .coordinator
            .topic_partitions(&topic)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?
            .is_none()
        {
            self.epochs.lock().insert(shard.clone(), 0);
            return Ok(0);
        }
        let fetched = self
            .reader
            .fetch(&topic, 0, 0)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
        let epoch = fetched
            .last()
            .map(|batch| decode_epoch(&batch.payload))
            .transpose()?
            .unwrap_or(0);
        self.epochs.lock().insert(shard.clone(), epoch);
        Ok(epoch)
    }

    fn ensure_topic(&self, topic: &str) -> Result<(), LogStoreError> {
        if self.topics.lock().contains(topic) {
            return Ok(());
        }
        match self.coordinator.topic_partitions(topic) {
            Ok(Some(_)) => {}
            Ok(None) => self
                .coordinator
                .create_topic(topic, 1)
                .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?,
            Err(err) => return Err(LogStoreError::StorageFailure(err.to_string())),
        }
        self.topics.lock().insert(topic.to_string());
        Ok(())
    }

    fn read_all(&self, shard: &ShardKey) -> Result<CommandPage, LogStoreError> {
        let topic = topic_for_shard(shard);
        if self
            .coordinator
            .topic_partitions(&topic)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?
            .is_none()
        {
            return Err(LogStoreError::ShardNotFound);
        }
        let fetched = self
            .reader
            .fetch(&topic, 0, 0)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
        decode_page(shard, fetched, usize::MAX)
    }
}

fn validate_endpoint_url(endpoint_url: &str) -> Result<(), S3CompatibleConfigError> {
    let endpoint_url = endpoint_url.trim();
    if endpoint_url.is_empty() {
        return Err(S3CompatibleConfigError::MissingEndpoint);
    }
    if endpoint_url.chars().any(char::is_whitespace) {
        return Err(S3CompatibleConfigError::InvalidEndpoint);
    }
    let without_scheme = endpoint_url
        .strip_prefix("http://")
        .or_else(|| endpoint_url.strip_prefix("https://"))
        .ok_or(S3CompatibleConfigError::InvalidEndpoint)?;
    let host = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if host.is_empty() || host == ":" || host.starts_with(':') {
        return Err(S3CompatibleConfigError::InvalidEndpoint);
    }
    Ok(())
}

fn validate_bucket(bucket: &str) -> Result<(), S3CompatibleConfigError> {
    let bucket = bucket.trim();
    if bucket.is_empty() {
        return Err(S3CompatibleConfigError::MissingBucket);
    }
    if !(3..=63).contains(&bucket.len())
        || !bucket
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '.')
        || bucket.starts_with(['-', '.'])
        || bucket.ends_with(['-', '.'])
        || bucket.contains("..")
    {
        return Err(S3CompatibleConfigError::InvalidBucket);
    }
    Ok(())
}

fn validate_region(region: &str) -> Result<(), S3CompatibleConfigError> {
    if region.trim().is_empty() {
        return Err(S3CompatibleConfigError::MissingRegion);
    }
    Ok(())
}

fn validate_credentials(
    credentials: &S3CompatibleCredentials,
) -> Result<(), S3CompatibleConfigError> {
    if credentials.access_key_id.trim().is_empty()
        || credentials.secret_access_key.trim().is_empty()
    {
        return Err(S3CompatibleConfigError::MissingCredentials);
    }
    Ok(())
}

impl LogStore for FjordObjectLogStore {
    async fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError> {
        let current_epoch = self.current_epoch(shard)?;
        if expected_epoch.is_some_and(|expected| expected != current_epoch) {
            return Err(LogStoreError::StalEpoch {
                expected: expected_epoch.unwrap(),
                current: current_epoch,
            });
        }
        let topic = topic_for_shard(shard);
        self.ensure_topic(&topic)?;

        let batches = commands
            .iter()
            .map(|command| {
                encode_record(current_epoch, command).map(|payload| ProduceBatch {
                    topic: topic.clone(),
                    partition: 0,
                    producer_id: -1,
                    producer_epoch: -1,
                    base_sequence: -1,
                    record_count: 1,
                    payload,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = self
            .writer
            .produce(&batches)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
        let last_sequence = outcomes
            .last()
            .map(|outcome| match outcome {
                CommitOutcome::Assigned { base_offset, .. } => *base_offset as u64,
                CommitOutcome::Duplicate { base_offset } => *base_offset as u64,
            })
            .unwrap_or(0);

        Ok(AppendBatchResult {
            last_position: CommandPosition {
                shard_key: shard.clone(),
                sequence: last_sequence,
                backend_epoch: current_epoch,
            },
        })
    }

    async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, LogStoreError> {
        let topic = topic_for_shard(shard);
        if self
            .coordinator
            .topic_partitions(&topic)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?
            .is_none()
        {
            return Err(LogStoreError::ShardNotFound);
        }
        if limit == 0 {
            return Ok(CommandPage {
                commands: Vec::new(),
                next_position: None,
            });
        }

        let fetch_offset = position.map(|pos| pos.sequence as i64 + 1).unwrap_or(0);
        let fetched = self
            .reader
            .fetch(&topic, 0, fetch_offset)
            .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
        decode_page(shard, fetched, limit)
    }

    fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile::Replicated
    }
}

fn topic_for_shard(shard: &ShardKey) -> String {
    format!(
        "pqueue_{}_{}_s{}",
        sanitize(shard.tenant_id.as_str()),
        sanitize(shard.queue_id.as_str()),
        shard.shard_id.as_u32()
    )
}

fn epoch_topic_for_shard(shard: &ShardKey) -> String {
    format!("{}_epoch", topic_for_shard(shard))
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn decode_epoch(payload: &[u8]) -> Result<u64, LogStoreError> {
    let bytes: [u8; 8] = payload
        .try_into()
        .map_err(|_| LogStoreError::StorageFailure("invalid epoch fence payload".to_string()))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_page(
    shard: &ShardKey,
    fetched: Vec<FetchedBatch>,
    limit: usize,
) -> Result<CommandPage, LogStoreError> {
    let has_more = fetched.len() > limit;
    let mut commands = Vec::new();
    for batch in fetched.into_iter().take(limit) {
        let record = decode_record(&batch.payload)?;
        let position = CommandPosition {
            shard_key: shard.clone(),
            sequence: batch.base_offset as u64,
            backend_epoch: record.backend_epoch,
        };
        commands.push((position, record.envelope));
    }
    let next_position = has_more
        .then(|| commands.last().map(|(position, _)| position.clone()))
        .flatten();
    Ok(CommandPage {
        commands,
        next_position,
    })
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct EnvelopeRecordDto {
    backend_epoch: u64,
    envelope: EnvelopeDto,
}

struct DecodedRecord {
    backend_epoch: u64,
    envelope: CommandEnvelope,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct EnvelopeDto {
    command_id: String,
    request_id: Option<String>,
    tenant_id: String,
    queue_id: String,
    shard_id: u32,
    item_ids: Vec<String>,
    checksum: u32,
    created_at: TimestampDto,
    command: CommandDto,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "body")]
enum CommandDto {
    BatchPush {
        items: Vec<PushItemDto>,
    },
    BatchUpdate {
        item_ids: Vec<String>,
    },
    BatchClaim {
        item_ids: Vec<String>,
        lease_token: String,
        lease_expires_at: TimestampDto,
    },
    BatchRenewLeases {
        item_ids: Vec<String>,
        lease_expires_at: TimestampDto,
    },
    BatchFinalize {
        outcomes: Vec<FinalizeOutcomeDto>,
    },
    LeaseExpired {
        item_ids: Vec<String>,
    },
    CohortExpired {
        group_key: String,
    },
    PurgeItems {
        item_ids: Vec<String>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PushItemDto {
    client_item_key: String,
    item_id: String,
    priority: Option<PriorityDto>,
    not_before: Option<TimestampDto>,
    max_attempts: u32,
    payload: Option<Vec<u8>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "value")]
enum PriorityDto {
    Timestamp(TimestampDto),
    Int64(i64),
    Decimal { mantissa: i128, scale: u32 },
    Text(String),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FinalizeOutcomeDto {
    item_id: String,
    kind: FinalizeKindDto,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum FinalizeKindDto {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimestampDto {
    seconds: i64,
    nanoseconds: u32,
}

fn encode_record(backend_epoch: u64, envelope: &CommandEnvelope) -> Result<Vec<u8>, LogStoreError> {
    serde_json::to_vec(&EnvelopeRecordDto {
        backend_epoch,
        envelope: envelope_to_dto(envelope)?,
    })
    .map_err(|err| LogStoreError::StorageFailure(err.to_string()))
}

fn decode_record(payload: &[u8]) -> Result<DecodedRecord, LogStoreError> {
    let dto: EnvelopeRecordDto = serde_json::from_slice(payload)
        .map_err(|err| LogStoreError::StorageFailure(err.to_string()))?;
    Ok(DecodedRecord {
        backend_epoch: dto.backend_epoch,
        envelope: dto_to_envelope(dto.envelope)?,
    })
}

fn envelope_to_dto(envelope: &CommandEnvelope) -> Result<EnvelopeDto, LogStoreError> {
    Ok(EnvelopeDto {
        command_id: envelope.command_id.0.clone(),
        request_id: envelope
            .request_id
            .as_ref()
            .map(|id| id.as_str().to_string()),
        tenant_id: envelope.tenant_id.as_str().to_string(),
        queue_id: envelope.queue_id.as_str().to_string(),
        shard_id: envelope.shard_id.as_u32(),
        item_ids: envelope
            .item_ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect(),
        checksum: envelope.checksum.0,
        created_at: timestamp_to_dto(&envelope.created_at),
        command: command_to_dto(&envelope.command)?,
    })
}

fn command_to_dto(command: &QueueCommand) -> Result<CommandDto, LogStoreError> {
    match command {
        QueueCommand::CreateQueue(_) => Err(LogStoreError::StorageFailure(
            "CreateQueue is control-plane state and is not encoded in pqueue-objectlog".to_string(),
        )),
        QueueCommand::BatchPush(command) => Ok(CommandDto::BatchPush {
            items: command.items.iter().map(push_item_to_dto).collect(),
        }),
        QueueCommand::BatchUpdate(command) => Ok(CommandDto::BatchUpdate {
            item_ids: ids_to_strings(&command.item_ids),
        }),
        QueueCommand::BatchClaim(command) => Ok(CommandDto::BatchClaim {
            item_ids: ids_to_strings(&command.item_ids),
            lease_token: command.lease_token.clone(),
            lease_expires_at: timestamp_to_dto(&command.lease_expires_at),
        }),
        QueueCommand::BatchRenewLeases(command) => Ok(CommandDto::BatchRenewLeases {
            item_ids: ids_to_strings(&command.item_ids),
            lease_expires_at: timestamp_to_dto(&command.lease_expires_at),
        }),
        QueueCommand::BatchFinalize(command) => Ok(CommandDto::BatchFinalize {
            outcomes: command
                .outcomes
                .iter()
                .map(|outcome| FinalizeOutcomeDto {
                    item_id: outcome.item_id.as_str().to_string(),
                    kind: finalize_kind_to_dto(outcome.kind),
                })
                .collect(),
        }),
        QueueCommand::LeaseExpired(command) => Ok(CommandDto::LeaseExpired {
            item_ids: ids_to_strings(&command.item_ids),
        }),
        QueueCommand::CohortExpired(command) => Ok(CommandDto::CohortExpired {
            group_key: command.group_key.clone(),
        }),
        QueueCommand::PurgeItems(command) => Ok(CommandDto::PurgeItems {
            item_ids: ids_to_strings(&command.item_ids),
        }),
    }
}

fn ids_to_strings(ids: &[ItemId]) -> Vec<String> {
    ids.iter().map(|id| id.as_str().to_string()).collect()
}

fn push_item_to_dto(item: &PushItem) -> PushItemDto {
    PushItemDto {
        client_item_key: item.client_item_key.as_str().to_string(),
        item_id: item.item_id.as_str().to_string(),
        priority: item.priority.as_ref().map(priority_to_dto),
        not_before: item.not_before.as_ref().map(timestamp_to_dto),
        max_attempts: item.max_attempts,
        payload: item.payload.as_ref().map(|payload| payload.to_vec()),
    }
}

fn priority_to_dto(priority: &PriorityValue) -> PriorityDto {
    match priority {
        PriorityValue::Timestamp(value) => PriorityDto::Timestamp(timestamp_to_dto(value)),
        PriorityValue::Int64(value) => PriorityDto::Int64(*value),
        PriorityValue::Decimal(value) => PriorityDto::Decimal {
            mantissa: value.mantissa,
            scale: value.scale,
        },
        PriorityValue::Text(value) => PriorityDto::Text(value.clone()),
    }
}

fn timestamp_to_dto(value: &UtcTimestamp) -> TimestampDto {
    TimestampDto {
        seconds: value.seconds,
        nanoseconds: value.nanoseconds,
    }
}

fn finalize_kind_to_dto(kind: FinalizeKind) -> FinalizeKindDto {
    match kind {
        FinalizeKind::Complete => FinalizeKindDto::Complete,
        FinalizeKind::Fail => FinalizeKindDto::Fail,
        FinalizeKind::Retry => FinalizeKindDto::Retry,
        FinalizeKind::Release => FinalizeKindDto::Release,
        FinalizeKind::Rearm => FinalizeKindDto::Rearm,
    }
}

fn dto_to_envelope(dto: EnvelopeDto) -> Result<CommandEnvelope, LogStoreError> {
    let command = dto_to_command(dto.command)?;
    Ok(CommandEnvelope {
        command_id: CommandId(dto.command_id),
        request_id: dto
            .request_id
            .map(RequestId::new)
            .transpose()
            .map_err(id_err)?,
        tenant_id: TenantId::new(dto.tenant_id).map_err(id_err)?,
        queue_id: QueueId::new(dto.queue_id).map_err(id_err)?,
        shard_id: pqueue_storage::types::ShardId::new(dto.shard_id),
        item_ids: dto
            .item_ids
            .into_iter()
            .map(ItemId::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(id_err)?,
        command,
        checksum: CommandChecksum(dto.checksum),
        created_at: dto_to_timestamp(dto.created_at)?,
    })
}

fn dto_to_command(dto: CommandDto) -> Result<QueueCommand, LogStoreError> {
    match dto {
        CommandDto::BatchPush { items } => Ok(QueueCommand::BatchPush(BatchPushCommand {
            items: items
                .into_iter()
                .map(dto_to_push_item)
                .collect::<Result<Vec<_>, _>>()?,
        })),
        CommandDto::BatchUpdate { item_ids } => Ok(QueueCommand::BatchUpdate(BatchUpdateCommand {
            item_ids: strings_to_ids(item_ids)?,
        })),
        CommandDto::BatchClaim {
            item_ids,
            lease_token,
            lease_expires_at,
        } => Ok(QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids: strings_to_ids(item_ids)?,
            lease_token,
            lease_expires_at: dto_to_timestamp(lease_expires_at)?,
        })),
        CommandDto::BatchRenewLeases {
            item_ids,
            lease_expires_at,
        } => Ok(QueueCommand::BatchRenewLeases(BatchRenewLeasesCommand {
            item_ids: strings_to_ids(item_ids)?,
            lease_expires_at: dto_to_timestamp(lease_expires_at)?,
        })),
        CommandDto::BatchFinalize { outcomes } => {
            Ok(QueueCommand::BatchFinalize(BatchFinalizeCommand {
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        Ok(FinalizeOutcome {
                            item_id: ItemId::new(outcome.item_id).map_err(id_err)?,
                            kind: dto_to_finalize_kind(outcome.kind),
                        })
                    })
                    .collect::<Result<Vec<_>, LogStoreError>>()?,
            }))
        }
        CommandDto::LeaseExpired { item_ids } => {
            Ok(QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: strings_to_ids(item_ids)?,
            }))
        }
        CommandDto::CohortExpired { group_key } => {
            Ok(QueueCommand::CohortExpired(CohortExpiredCommand {
                group_key,
            }))
        }
        CommandDto::PurgeItems { item_ids } => Ok(QueueCommand::PurgeItems(PurgeItemsCommand {
            item_ids: strings_to_ids(item_ids)?,
        })),
    }
}

fn dto_to_push_item(dto: PushItemDto) -> Result<PushItem, LogStoreError> {
    Ok(PushItem {
        client_item_key: ClientItemKey::new(dto.client_item_key).map_err(id_err)?,
        item_id: ItemId::new(dto.item_id).map_err(id_err)?,
        priority: dto.priority.map(dto_to_priority).transpose()?,
        not_before: dto.not_before.map(dto_to_timestamp).transpose()?,
        max_attempts: dto.max_attempts,
        payload: dto.payload.map(bytes::Bytes::from),
    })
}

fn dto_to_priority(dto: PriorityDto) -> Result<PriorityValue, LogStoreError> {
    Ok(match dto {
        PriorityDto::Timestamp(value) => PriorityValue::Timestamp(dto_to_timestamp(value)?),
        PriorityDto::Int64(value) => PriorityValue::Int64(value),
        PriorityDto::Decimal { mantissa, scale } => {
            PriorityValue::Decimal(DecimalValue { mantissa, scale })
        }
        PriorityDto::Text(value) => PriorityValue::Text(value),
    })
}

fn strings_to_ids(ids: Vec<String>) -> Result<Vec<ItemId>, LogStoreError> {
    ids.into_iter()
        .map(ItemId::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(id_err)
}

fn dto_to_timestamp(dto: TimestampDto) -> Result<UtcTimestamp, LogStoreError> {
    UtcTimestamp::new(dto.seconds, dto.nanoseconds)
        .map_err(|err| LogStoreError::StorageFailure(err.to_string()))
}

fn dto_to_finalize_kind(dto: FinalizeKindDto) -> FinalizeKind {
    match dto {
        FinalizeKindDto::Complete => FinalizeKind::Complete,
        FinalizeKindDto::Fail => FinalizeKind::Fail,
        FinalizeKindDto::Retry => FinalizeKind::Retry,
        FinalizeKindDto::Release => FinalizeKind::Release,
        FinalizeKindDto::Rearm => FinalizeKind::Rearm,
    }
}

fn id_err(err: pqueue_core::IdentifierError) -> LogStoreError {
    LogStoreError::StorageFailure(err.to_string())
}
