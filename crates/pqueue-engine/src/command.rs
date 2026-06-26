//! Engine-owned command model — the durable append unit of the log and the input to the
//! projection. Commands are the only way state changes (CQRS write side, ADR-001).

use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition, RequestId,
    UtcTimestamp,
};

/// Unique id for a committed command record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CommandId(pub String);

impl CommandId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// CRC-32 of the command payload for in-transit integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CommandChecksum(pub u32);

/// The typed command variants. Client-driven commands plus the transitions the
/// `ReclaimDriver` fires (TD-007 §3) and the durable-state commands (TD-007 §4).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum QueueCommand {
    CreateQueue(CreateQueueCommand),
    Push(PushCommand),
    Claim(ClaimCommand),
    RenewLease(RenewLeaseCommand),
    /// Transfer an in-flight lease to a new consumer (RESP cross-consumer `XCLAIM`): swap the lease token
    /// AND charge one delivery (it is a re-delivery to a different worker). Same-consumer `XCLAIM` is a
    /// no-charge [`RenewLeaseCommand`] instead.
    ReassignLease(ReassignLeaseCommand),
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
    /// Operator gate flip (BQ-14d, API-001 g2 `SetGates`): block or unblock the given gate keys for the
    /// queue. A blocked gate key makes every item carrying it ineligible (relational anti-join against
    /// `pqueue_gate_state`); unblocking restores eligibility. A relational-mode feature — the in-memory
    /// family applies this as a no-op (it stores no gate state).
    SetGates(SetGatesCommand),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CreateQueueCommand {
    pub definition: QueueDefinition,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PushCommand {
    pub items: Vec<PushItem>,
}

/// Build `PushItem`s + their ids for one push, deriving server ids from a backend command sequence `n`
/// (unique across handles + restart). The dedup `client_item_key` defaults to the item id (a unique
/// append) when the spec omits it. Shared by every backend's `PushPort` impl.
pub fn build_push_items(
    specs: Vec<crate::PushSpec>,
    n: u64,
    prefix: &str,
    max_attempts: u32,
) -> (Vec<PushItem>, Vec<ItemId>) {
    let mut items = Vec::with_capacity(specs.len());
    let mut ids = Vec::with_capacity(specs.len());
    for (i, s) in specs.into_iter().enumerate() {
        let item_id = ItemId::new(format!("{prefix}-{n}-{i}")).expect("id");
        let key = s
            .client_item_key
            .unwrap_or_else(|| ClientItemKey::new(format!("{prefix}-{n}-{i}")).expect("key"));
        ids.push(item_id.clone());
        items.push(PushItem {
            client_item_key: key,
            item_id,
            priority: s.priority,
            not_before: s.not_before,
            group_key: s.group_key,
            max_attempts,
            payload: s.payload,
            fields: s.fields,
            cohort_size: s.cohort_size,
            gate_keys: s.gate_keys,
        });
    }
    (items, ids)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PushItem {
    pub client_item_key: ClientItemKey,
    pub item_id: ItemId,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub max_attempts: u32,
    pub payload: Option<Bytes>,
    /// Structured hot-storage fields for compound work records. Defaulted for backwards-compatible log
    /// replay of commands written before structured fields existed.
    #[serde(default)]
    pub fields: BTreeMap<String, Bytes>,
    /// Declared cohort size (BQ-14c, TD-002 cohort formation): when set together with `group_key`, this
    /// item is a member of a cohort of `cohort_size` total members (the cohort key IS the `group_key`). The
    /// relational projection forms `pqueue_cohorts` from these declarations and a `whole_cohort` claim is
    /// admissible once the cohort is complete (`member_count == cohort_size`). `None` = not a cohort member
    /// (the common case; the in-memory family does not form cohorts and ignores this field). A divergent
    /// `cohort_size` for the same `group_key` is a conflict (TD-002 §cohort).
    #[serde(default)]
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d, TD-002 §gate / API-001 g2). When ANY of these keys is in a
    /// `blocked` state for the queue (set via the `SetGates` command), the item is INELIGIBLE — the
    /// relational eligibility predicate anti-joins item gate keys against `pqueue_gate_state`. Empty = no
    /// gates (the common case).
    ///
    /// SCOPE: gates are a RELATIONAL-mode feature only (like cohorts/group batching). The in-memory
    /// log-replay family does not store gate keys, does not enforce `SetGates`, and treats both as inert —
    /// so carrying gate keys on a log-replay-backed queue is silently non-enforcing. Enforcing this at the
    /// port (rejecting gate use on a non-gate-capable backend) is the operator-facing follow-up tracked by
    /// the BQ-14d fresh-eyes review.
    #[serde(default)]
    pub gate_keys: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClaimCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RenewLeaseCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReassignLeaseCommand {
    pub item_ids: Vec<ItemId>,
    /// The new owner's lease token (the `XCLAIM` consumer).
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FinalizeCommand {
    pub outcomes: Vec<FinalizeOutcome>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FinalizeOutcome {
    pub item_id: ItemId,
    pub kind: FinalizeKind,
}

/// The five finalize dispositions (API-001). Over RESP only `Complete` is a stock `XACK`;
/// the rest are library-only (plan §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FinalizeKind {
    Complete,
    Fail,
    Retry,
    Release,
    Rearm,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplacePendingCommand {
    /// The key whose pending item is being superseded.
    pub client_item_key: ClientItemKey,
    /// The superseded (old) item id — reads as deleted afterwards.
    pub superseded_item_id: ItemId,
    /// The replacement item.
    pub replacement: PushItem,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LeaseExpiredCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CohortExpiredCommand {
    pub group_key: GroupKey,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FenceLeaseCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnfenceLeaseCommand {
    pub item_ids: Vec<ItemId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PurgeItemsCommand {
    pub item_ids: Vec<ItemId>,
    pub force: bool,
}

/// Block or unblock gate keys for the queue (BQ-14d, API-001 g2 `SetGates`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetGatesCommand {
    pub gate_keys: Vec<String>,
    /// `true` blocks the keys (items carrying them become ineligible); `false` unblocks them.
    pub blocked: bool,
}

/// A durable command record — the append unit for the log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub request_id: Option<RequestId>,
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: UtcTimestamp,
}

#[cfg(test)]
mod serde_tests {
    //! Round-trip every command variant through JSON, so a durable backend can persist the log and
    //! replay it (Phase 3 enabler). No `PartialEq` on the command tree, so fidelity is checked by
    //! re-serializing the decoded value and comparing the JSON.
    use super::*;
    use bytes::Bytes;
    use pqueue_core::{PriorityValue, UtcTimestamp};

    fn iid(s: &str) -> ItemId {
        ItemId::new(s).unwrap()
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn item() -> PushItem {
        PushItem {
            client_item_key: ClientItemKey::new("k").unwrap(),
            item_id: iid("a"),
            priority: Some(PriorityValue::Int64(7)),
            not_before: Some(ts(5)),
            group_key: Some(GroupKey::new("g").unwrap()),
            max_attempts: 3,
            payload: Some(Bytes::from_static(b"payload")),
            fields: BTreeMap::new(),
            cohort_size: Some(4),
            gate_keys: Vec::new(),
        }
    }

    fn envelope(command: QueueCommand) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("c1"),
            request_id: Some(RequestId::new("r1").unwrap()),
            item_ids: vec![iid("a")],
            command,
            checksum: CommandChecksum(42),
            created_at: ts(1),
        }
    }

    fn all_variants() -> Vec<QueueCommand> {
        vec![
            QueueCommand::Push(PushCommand {
                items: vec![item()],
            }),
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("a")],
                lease_token: LeaseToken::new("lease").unwrap(),
                lease_expires_at: ts(100),
            }),
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("a")],
                lease_expires_at: ts(200),
            }),
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: iid("a"),
                    kind: FinalizeKind::Rearm,
                }],
            }),
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("k").unwrap(),
                superseded_item_id: iid("old"),
                replacement: item(),
            }),
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![iid("a")],
            }),
            QueueCommand::CohortExpired(CohortExpiredCommand {
                group_key: GroupKey::new("g").unwrap(),
            }),
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![iid("a")],
            }),
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![iid("a")],
            }),
            QueueCommand::PauseQueue,
            QueueCommand::ResumeQueue,
            QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![iid("a")],
                force: true,
            }),
        ]
    }

    #[test]
    fn every_command_variant_round_trips_through_json() {
        for command in all_variants() {
            let env = envelope(command);
            let json = serde_json::to_string(&env).expect("serialize");
            let decoded: CommandEnvelope = serde_json::from_str(&json).expect("deserialize");
            let reencoded = serde_json::to_string(&decoded).expect("re-serialize");
            assert_eq!(json, reencoded, "round-trip mismatch for {json}");
        }
    }

    #[test]
    fn payload_bytes_and_priority_survive_round_trip() {
        let env = envelope(QueueCommand::Push(PushCommand {
            items: vec![item()],
        }));
        let json = serde_json::to_string(&env).unwrap();
        let decoded: CommandEnvelope = serde_json::from_str(&json).unwrap();
        let QueueCommand::Push(p) = &decoded.command else {
            panic!("expected push");
        };
        assert_eq!(p.items[0].payload.as_deref(), Some(&b"payload"[..]));
        assert_eq!(p.items[0].priority, Some(PriorityValue::Int64(7)));
    }
}
