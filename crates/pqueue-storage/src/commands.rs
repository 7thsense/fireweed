use pqueue_core::{
    ClientItemKey, ItemId, PriorityValue, QueueDefinition, RequestId, TenantId, QueueId,
    UtcTimestamp,
};

use crate::types::{CommandChecksum, ShardId};

/// Unique identifier for a committed command record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandId(pub String);

impl CommandId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// The typed command payload variants.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueueCommand {
    CreateQueue(CreateQueueCommand),
    BatchPush(BatchPushCommand),
    BatchUpdate(BatchUpdateCommand),
    BatchClaim(BatchClaimCommand),
    BatchRenewLeases(BatchRenewLeasesCommand),
    BatchFinalize(BatchFinalizeCommand),
    LeaseExpired(LeaseExpiredCommand),
    CohortExpired(CohortExpiredCommand),
    PurgeItems(PurgeItemsCommand),
}

#[derive(Debug, Clone)]
pub struct CreateQueueCommand {
    pub definition: QueueDefinition,
}

#[derive(Debug, Clone)]
pub struct BatchPushCommand {
    pub items: Vec<PushItem>,
}

#[derive(Debug, Clone)]
pub struct PushItem {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub max_attempts: u32,
    /// Opaque payload bytes (e.g. Kafka record value). None when not supplied.
    pub payload: Option<bytes::Bytes>,
}

#[derive(Debug, Clone)]
pub struct BatchUpdateCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone)]
pub struct BatchClaimCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_token: String,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct BatchRenewLeasesCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct BatchFinalizeCommand {
    pub outcomes: Vec<FinalizeOutcome>,
}

#[derive(Debug, Clone)]
pub struct FinalizeOutcome {
    pub item_id: ItemId,
    pub kind: FinalizeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeKind {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone)]
pub struct LeaseExpiredCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone)]
pub struct CohortExpiredCommand {
    pub group_key: String,
}

#[derive(Debug, Clone)]
pub struct PurgeItemsCommand {
    pub item_ids: Vec<ItemId>,
}

/// A durable command record — the append unit for `LogStore`.
#[derive(Debug, Clone)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub request_id: Option<RequestId>,
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub shard_id: ShardId,
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: UtcTimestamp,
}
