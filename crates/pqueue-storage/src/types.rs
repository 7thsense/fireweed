use pqueue_core::{QueueId, TenantId};

/// Identifies a shard within a queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardId(pub u32);

impl ShardId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Tenant + queue identity key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
}

/// Tenant + queue + shard identity key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub shard_id: ShardId,
}

/// Position of a committed command within a shard's log.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandPosition {
    /// The shard this position belongs to.
    pub shard_key: ShardKey,
    /// Monotonically increasing sequence number within the shard epoch.
    pub sequence: u64,
    /// Assignment epoch at the time of append.
    pub backend_epoch: u64,
}

/// CRC-32 checksum of the command payload for in-transit integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandChecksum(pub u32);

impl PartialOrd for ShardKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShardKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tenant_id
            .as_str()
            .cmp(other.tenant_id.as_str())
            .then(self.queue_id.as_str().cmp(other.queue_id.as_str()))
            .then(self.shard_id.0.cmp(&other.shard_id.0))
    }
}
