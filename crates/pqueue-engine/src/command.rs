//! Engine-owned command model — the durable append unit of the log and the input to the
//! projection. Commands are the only way state changes (CQRS write side, ADR-001).

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition, RequestId,
    UtcTimestamp,
};

use crate::types::ShardId;

/// Unique id for a committed command record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandId(pub String);

impl CommandId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// CRC-32 of the command payload for in-transit integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandChecksum(pub u32);

/// The typed command variants. Client-driven commands plus the transitions the
/// `ReclaimDriver` fires (TD-007 §3) and the durable-state commands (TD-007 §4).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueueCommand {
    CreateQueue(CreateQueueCommand),
    Push(PushCommand),
    Claim(ClaimCommand),
    RenewLease(RenewLeaseCommand),
    Finalize(FinalizeCommand),
    /// Pending-item replacement (RESP `XADD`-on-key upsert, Invariant 2). Atomic class only.
    ReplacePending(ReplacePendingCommand),
    // --- ReclaimDriver-fired (TD-007 §3) ---
    LeaseExpired(LeaseExpiredCommand),
    CohortExpired(CohortExpiredCommand),
    // --- durable state (TD-007 §4) ---
    FenceLease(FenceLeaseCommand),
    UnfenceLease(UnfenceLeaseCommand),
    PauseQueue,
    ResumeQueue,
    PurgeItems(PurgeItemsCommand),
}

#[derive(Debug, Clone)]
pub struct CreateQueueCommand {
    pub definition: QueueDefinition,
}

#[derive(Debug, Clone)]
pub struct PushCommand {
    pub items: Vec<PushItem>,
}

#[derive(Debug, Clone)]
pub struct PushItem {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub max_attempts: u32,
    pub payload: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub struct ClaimCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct RenewLeaseCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct FinalizeCommand {
    pub outcomes: Vec<FinalizeOutcome>,
}

#[derive(Debug, Clone)]
pub struct FinalizeOutcome {
    pub item_id: ItemId,
    pub kind: FinalizeKind,
}

/// The five finalize dispositions (API-001). Over RESP only `Complete` is a stock `XACK`;
/// the rest are library-only (plan §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeKind {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone)]
pub struct ReplacePendingCommand {
    /// The key whose pending item is being superseded.
    pub client_item_key: ClientItemKey,
    /// The superseded (old) item id — reads as deleted afterwards.
    pub superseded_item_id: ItemId,
    /// The replacement item.
    pub replacement: PushItem,
}

#[derive(Debug, Clone)]
pub struct LeaseExpiredCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone)]
pub struct CohortExpiredCommand {
    pub group_key: GroupKey,
}

#[derive(Debug, Clone)]
pub struct FenceLeaseCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone)]
pub struct UnfenceLeaseCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone)]
pub struct PurgeItemsCommand {
    pub item_ids: Vec<ItemId>,
    pub force: bool,
}

/// A durable command record — the append unit for the log.
#[derive(Debug, Clone)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub request_id: Option<RequestId>,
    pub shard_id: ShardId,
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: UtcTimestamp,
}
