//! Engine-owned command model — the durable append unit of the log and the input to the
//! projection. Commands are the only way state changes (CQRS write side, ADR-001).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, CohortId, GroupKey, ItemId, ItemState, LeaseToken, Metadata, OwnerId,
    PriorityValue, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp, WorkerId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::QueueKey;
use crate::error::{EngineError, EngineResult};
use crate::types::CommandPosition;

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
    CohortClaim(CohortClaimCommand),
    RenewLease(RenewLeaseCommand),
    CohortRenewLease(CohortRenewLeaseCommand),
    /// Transfer an in-flight lease to a new consumer (RESP cross-consumer `XCLAIM`): swap the lease token
    /// AND charge one delivery (it is a re-delivery to a different worker). Same-consumer `XCLAIM` is a
    /// no-charge [`RenewLeaseCommand`] instead.
    ReassignLease(ReassignLeaseCommand),
    Finalize(FinalizeCommand),
    CohortFinalize(CohortFinalizeCommand),
    /// Pending-item replacement (RESP `XADD`-on-key upsert, Invariant 2). Atomic class only.
    ReplacePending(ReplacePendingCommand),
    /// In-place merge of a live (Pending or Leased) item's hot-storage `fields`/`payload` with no lifecycle
    /// change (FAC-1, ADR-009). The write side of the `LiveItemView` map; bumps `item_version`. Atomic
    /// class only. Lets an owner-runtime keep compound per-item work state in fireweed instead of a shadow.
    UpdateFields(UpdateFieldsCommand),
    /// One resolved, durable backend-erased mutation. Selector predicates are evaluated before append;
    /// this command contains only exact item ids and complete post-mutation values.
    MutateItems(MutateItemsCommand),
    // --- ReclaimDriver-fired (TD-007 §3) ---
    LeaseExpired(LeaseExpiredCommand),
    CohortExpired(CohortExpiredCommand),
    // --- durable state (TD-007 §4) ---
    FenceLease(FenceLeaseCommand),
    UnfenceLease(UnfenceLeaseCommand),
    PauseQueue(PauseQueueCommand),
    ResumeQueue,
    PurgeItems(PurgeItemsCommand),
    /// Operator gate flip (BQ-14d, API-001 g2 `SetGates`): block or unblock the given gate keys for the
    /// queue. A blocked gate key makes every item carrying it ineligible (relational anti-join against
    /// `fireweed_gate_state`); unblocking restores eligibility. A relational-mode feature — the in-memory
    /// family applies this as a no-op (it stores no gate state).
    SetGates(SetGatesCommand),
    /// Write bounded OPAQUE non-work side records (Snorri authoritative-commit boundary, ADR-009 / epic
    /// pqueue-2201fd37). Each record is a `key -> payload` pair stored in a projection map that is
    /// ENTIRELY SEPARATE from the work-item index: a side record is NOT claimable/peekable work, never
    /// enters the eligibility index, `by_key`, or metrics-as-work, and survives input finalization. fireweed
    /// treats both key and payload as opaque bytes (the consumer owns any meaning). Emitted only on the
    /// vectorized claimed-work commit path.
    WriteSideRecords(WriteSideRecordsCommand),
    /// Advance a caller-supplied OPAQUE instance/state fence (Snorri authoritative-commit boundary, ADR-009
    /// / epic pqueue-2201fd37). Sets the stored fence for `instance_key` to `next` — validated pre-commit
    /// (stored == `expected`, `next > expected`) so the apply is infallible. The fence map is SEPARATE from
    /// the work-item projection (not claimable/peekable). `instance_key` is opaque bytes fireweed never
    /// interprets. Emitted only on the vectorized claimed-work commit path.
    AdvanceInstanceFence(AdvanceInstanceFenceCommand),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CreateQueueCommand {
    pub definition: QueueDefinition,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PushCommand {
    pub items: Vec<PushItem>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutateItemsCommand {
    pub items: Vec<ResolvedItemMutation>,
    pub gate_changes: Vec<crate::port::GateChange>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolvedItemMutation {
    pub item_id: ItemId,
    pub action: ResolvedItemMutationAction,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ResolvedItemMutationAction {
    Purge,
    Replace(ResolvedItemValues),
}

/// Complete post-mutation values. Applying this value is deterministic and performs exactly one version
/// bump already chosen by the planner; replay never re-evaluates a selector or JSON pointer edit.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolvedItemValues {
    pub state: ItemState,
    pub item_version: u64,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub eligible_since: UtcTimestamp,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub gate_keys: Vec<String>,
    pub entity_document: Option<serde_json::Value>,
    pub invalidate_lease: bool,
}

/// Build `PushItem`s + their ids for one push (ADR-009). Each id is minted **locally** as
/// `ItemId::mint(epoch, node, counter_base + i)`: `epoch` is the owner's fence epoch, `node` the owning
/// node id, and `counter_base..` a per-(queue, epoch) sequence reserved from [`QueueCounters`].
/// Single-writer-per-epoch makes `(epoch, counter)` unique within a queue; `node` is defense-in-depth so
/// even a split-brain (two writers, same epoch) cannot collide. No central sequence is consulted — this
/// works identically on the log (the only cross-node backend). The dedup `client_item_key` defaults to the
/// item id's string when the spec omits it. Shared by every backend's `PushPort` impl.
pub fn build_push_items(
    specs: Vec<crate::PushSpec>,
    epoch: u64,
    node: u8,
    counter_base: u32,
    max_attempts: u32,
) -> (Vec<PushItem>, Vec<ItemId>) {
    let mut items = Vec::with_capacity(specs.len());
    let mut ids = Vec::with_capacity(specs.len());
    for (i, s) in specs.into_iter().enumerate() {
        let item_id = ItemId::mint(epoch, node, counter_base.wrapping_add(i as u32));
        let key = s
            .client_item_key
            .unwrap_or_else(|| ClientItemKey::new(item_id.to_string()).expect("id is non-empty"));
        ids.push(item_id);
        items.push(PushItem {
            client_item_key: key,
            item_id,
            priority: s.priority,
            not_before: s.not_before,
            group_key: s.group_key,
            max_attempts,
            payload: s.payload,
            fields: s.fields,
            metadata: s.metadata,
            cohort_size: s.cohort_size,
            gate_keys: s.gate_keys,
            entity_document: s.entity,
        });
    }
    (items, ids)
}

/// Reject gate-bearing pushes on backends that do not enforce gate state.
///
/// This is deliberately separate from [`crate::DurabilityClass`]: the in-memory reference backend is
/// atomic, but it is still not gate-capable because its shared log-replay projection does not store or
/// evaluate gate state.
pub fn validate_gate_push(supports_gates: bool, specs: &[crate::PushSpec]) -> EngineResult<()> {
    if !supports_gates && specs.iter().any(|spec| !spec.gate_keys.is_empty()) {
        return Err(EngineError::Unavailable);
    }
    Ok(())
}

/// Reject gate-state commands on backends that would otherwise log them without enforcing them.
pub fn validate_gate_command(supports_gates: bool, command: &QueueCommand) -> EngineResult<()> {
    if supports_gates {
        return Ok(());
    }
    match command {
        QueueCommand::SetGates(_) => Err(EngineError::Unavailable),
        QueueCommand::Push(c) if c.items.iter().any(|item| !item.gate_keys.is_empty()) => {
            Err(EngineError::Unavailable)
        }
        QueueCommand::ReplacePending(c) if !c.replacement.gate_keys.is_empty() => {
            Err(EngineError::Unavailable)
        }
        _ => Ok(()),
    }
}

/// Per-queue item-id counter that **resets when the fence epoch advances**, so the 32-bit `counter` field
/// of [`ItemId`] only ever spans a single owner tenure — it cannot wrap in practice (a tenure pushing 2^32
/// items is centuries away at any real rate; see ADR-009). Each backend embeds one and reserves a
/// contiguous base per push batch under a brief leaf lock (never held while any other lock is taken).
#[derive(Default)]
pub struct QueueCounters {
    inner: Mutex<HashMap<QueueKey, (u64, u32)>>,
}

impl QueueCounters {
    /// Reserve `count` consecutive counter values for `queue` at `epoch`, returning the base. Advancing the
    /// epoch (a re-acquire) resets the sequence to 0 so a fresh tenure starts low and dense.
    pub fn reserve(&self, queue: &QueueKey, epoch: u64, count: u32) -> u32 {
        let mut g = self.inner.lock().expect("queue-counter mutex poisoned");
        let entry = g.entry(queue.clone()).or_insert((epoch, 0));
        if entry.0 != epoch {
            *entry = (epoch, 0);
        }
        let base = entry.1;
        entry.1 = entry.1.wrapping_add(count);
        base
    }

    /// Restart recovery: ensure `queue` resumes minting *past* an id already present in durable storage.
    /// Call once per recovered [`ItemId`] (or just the max) during rebuild/reopen — a push afterward then
    /// never re-mints an existing id. Decoding `(epoch, counter)` straight from the id keeps this format-
    /// agnostic. Monotone: only ever advances the stored `(epoch, next)` for a queue, never rewinds.
    pub fn observe(&self, queue: &QueueKey, id: ItemId) {
        let (epoch, next) = (id.epoch(), id.counter().wrapping_add(1));
        let mut g = self.inner.lock().expect("queue-counter mutex poisoned");
        let entry = g.entry(queue.clone()).or_insert((epoch, next));
        if epoch > entry.0 || (epoch == entry.0 && next > entry.1) {
            *entry = (epoch, next);
        }
    }
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
    /// Caller-owned metadata for compatibility predicates and claim responses. Defaulted for log replay of
    /// commands written before metadata existed.
    #[serde(default)]
    pub metadata: Metadata,
    /// Declared cohort size (BQ-14c, TD-002 cohort formation): when set together with `group_key`, this
    /// item is a member of a cohort of `cohort_size` total members (the cohort key IS the `group_key`). The
    /// relational projection forms `fireweed_cohorts` from these declarations and a `whole_cohort` claim is
    /// admissible once the cohort is complete (`member_count == cohort_size`). `None` = not a cohort member
    /// (the common case; the in-memory family does not form cohorts and ignores this field). A divergent
    /// `cohort_size` for the same `group_key` is a conflict (TD-002 §cohort).
    #[serde(default)]
    pub cohort_size: Option<u64>,
    /// Gate keys this item carries (BQ-14d, TD-002 §gate / API-001 g2). When ANY of these keys is in a
    /// `blocked` state for the queue (set via the `SetGates` command), the item is INELIGIBLE — the
    /// relational eligibility predicate anti-joins item gate keys against `fireweed_gate_state`. Empty = no
    /// gates (the common case).
    ///
    /// SCOPE: gates are a RELATIONAL-mode feature only (like cohorts/group batching). The in-memory
    /// log-replay family does not store gate keys, does not enforce `SetGates`, and treats both as inert —
    /// so carrying gate keys on a log-replay-backed queue is silently non-enforcing. Enforcing this at the
    /// port (rejecting gate use on a non-gate-capable backend) is the operator-facing follow-up tracked by
    /// the BQ-14d fresh-eyes review.
    #[serde(default)]
    pub gate_keys: Vec<String>,
    /// Typed JSON entity document (ADR-011). The canonical typed representation for schema-validated typed
    /// queues — used by schema validation and axon_esf index-key computation at push time.
    /// `#[serde(default)]` preserves replay compatibility for log entries written before this field existed.
    /// `None` for schema-less queues (which use the opaque `payload` bytes carrier).
    #[serde(default)]
    pub entity_document: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClaimCommand {
    pub item_ids: Vec<ItemId>,
    pub lease_token: LeaseToken,
    pub lease_expires_at: UtcTimestamp,
    /// Caller-supplied observability label; never an authorization principal.
    #[serde(default)]
    pub worker_id: Option<WorkerId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CohortClaimCommand {
    pub cohort_id: CohortId,
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
pub struct CohortRenewLeaseCommand {
    pub cohort_id: CohortId,
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
pub struct CohortFinalizeCommand {
    pub cohort_id: CohortId,
    pub kind: FinalizeKind,
    #[serde(default)]
    pub not_before: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FinalizeOutcome {
    pub item_id: ItemId,
    pub kind: FinalizeKind,
    /// The item state produced by applying this finalize outcome. Filled by the commit path so the
    /// durable command envelope can later synthesize the same history shape without re-reading state.
    #[serde(default)]
    pub applied_state: Option<ItemState>,
    /// Queue-native retry backoff: when `kind == Retry` and the item returns to Pending, it is ineligible
    /// until this timestamp. `None` = immediately re-eligible (the default). Ignored for non-Retry kinds.
    /// `#[serde(default)]` keeps logs written before retry backoff existed replay-compatible.
    #[serde(default)]
    pub not_before: Option<UtcTimestamp>,
}

impl FinalizeOutcome {
    /// A finalize outcome with no retry backoff (`not_before: None`) — the common case for
    /// complete/fail/release/rearm and an immediate retry.
    pub fn new(item_id: ItemId, kind: FinalizeKind) -> Self {
        Self {
            item_id,
            kind,
            applied_state: None,
            not_before: None,
        }
    }
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

/// In-place merge of a live item's hot-storage fields/payload (FAC-1). `field_ops` is a per-key delta:
/// `Some(bytes)` sets/overwrites the key, `None` removes it. `payload` either leaves the payload untouched
/// or replaces it (`Set(None)` clears). Bumps `item_version`. Touches neither lifecycle state nor the
/// lease — orthogonal to claim/renew/finalize.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpdateFieldsCommand {
    pub item_id: ItemId,
    pub field_ops: BTreeMap<String, Option<Bytes>>,
    pub payload: PayloadUpdate,
    /// Reschedule the item's priority (BQ pqueue-7a96f929). `Keep` leaves it; `Set(Some(p))` re-prices the
    /// item (which re-keys it in the eligibility order) and `Set(None)` clears the priority. Legal while the
    /// item is Pending or Leased. Defaults to `Keep` (a bare field/payload update is unchanged).
    #[serde(default)]
    pub set_priority: ScheduleUpdate<PriorityValue>,
    /// Reschedule the item's `not_before` (BQ pqueue-7a96f929). `Keep` leaves it; `Set(Some(t))` defers the
    /// item until `t` (re-eligible at that time); `Set(None)` clears it (immediately eligible). Defaults to
    /// `Keep`.
    #[serde(default)]
    pub set_not_before: ScheduleUpdate<UtcTimestamp>,
    /// Replace the item's entity document (ADR-011). `None` leaves it unchanged; `Some(doc)` replaces
    /// it and triggers schema validation if the queue has a compiled schema. `#[serde(default)]` keeps
    /// log-replay compatible with pre-ADR-011 commands (absent field → `None`).
    #[serde(default)]
    pub set_entity_document: Option<serde_json::Value>,
    /// API-001 `BatchUpdate` uses full replacement for the hot field map. `None` preserves the legacy
    /// FAC-1 delta behavior; `Some` replaces the complete map before any (normally empty) `field_ops`.
    #[serde(default)]
    pub set_fields: Option<BTreeMap<String, Bytes>>,
    /// API-001 full metadata replacement. Kept on the durable command so projection rebuild is exact.
    #[serde(default)]
    pub set_metadata: Option<Metadata>,
    /// API-001 full gate-membership replacement. `Some(vec![])` clears all memberships.
    #[serde(default)]
    pub set_gate_keys: Option<Vec<String>>,
    /// Selects API-001 BatchUpdate semantics (pending-only validation and preserved `eligible_since`) from
    /// the older FAC-1/reschedule command behavior while retaining one replay-compatible command variant.
    #[serde(default)]
    pub api001_batch: bool,
}

/// A field-reschedule disposition under [`UpdateFieldsCommand`] (BQ pqueue-7a96f929): leave the value as-is,
/// or set it (`Set(None)` clears an optional value). Distinct from a bare field/payload merge.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub enum ScheduleUpdate<T> {
    #[default]
    Keep,
    Set(Option<T>),
}

/// Disposition of an item's payload under [`UpdateFieldsCommand`]: leave it as-is, or replace it
/// (`Set(None)` clears it).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PayloadUpdate {
    Keep,
    Set(Option<Bytes>),
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

/// One opaque non-work side record (Snorri authoritative-commit boundary). Both `key` and `payload` are
/// OPAQUE bytes — fireweed stores them verbatim and never interprets them. Distinct from a work item: a side
/// record carries no lifecycle, lease, priority, or eligibility.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct SideRecord {
    #[serde(default)]
    pub key: Vec<u8>,
    #[serde(default)]
    pub payload: Bytes,
}

/// Write a batch of opaque non-work [`SideRecord`]s in one durable command. Apply is infallible
/// (insert-or-overwrite by key) and touches nothing in the work-item / eligibility projection.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WriteSideRecordsCommand {
    #[serde(default)]
    pub records: Vec<SideRecord>,
}

/// Advance a caller-supplied opaque instance/state fence to `next` (Snorri authoritative-commit boundary).
/// Validated pre-commit, so apply is infallible (overwrite the stored fence for `instance_key` with `next`).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AdvanceInstanceFenceCommand {
    #[serde(default)]
    pub instance_key: Vec<u8>,
    #[serde(default)]
    pub expected: u64,
    #[serde(default)]
    pub next: u64,
}

/// Block or unblock gate keys for the queue (BQ-14d, API-001 g2 `SetGates`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetGatesCommand {
    pub gate_keys: Vec<String>,
    /// `true` blocks the keys (items carrying them become ineligible); `false` unblocks them.
    pub blocked: bool,
}

/// Durable queue pause state. `drain_intake = false` is the legacy "claims stop, pushes still land"
/// mode. `drain_intake = true` additionally blocks intake so the queue can quiesce to a stable
/// position before branching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PauseQueueCommand {
    pub drain_intake: bool,
}

impl Serialize for PauseQueueCommand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if !self.drain_intake {
            return serializer.serialize_unit();
        }
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("drain_intake", &self.drain_intake)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for PauseQueueCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PauseQueueCommandVisitor;

        impl<'de> serde::de::Visitor<'de> for PauseQueueCommandVisitor {
            type Value = PauseQueueCommand;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("null or an object with drain_intake")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(PauseQueueCommand::default())
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(PauseQueueCommand::default())
            }

            fn visit_bool<E>(self, drain_intake: bool) -> Result<Self::Value, E> {
                Ok(PauseQueueCommand { drain_intake })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut drain_intake = false;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "drain_intake" => drain_intake = map.next_value()?,
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(PauseQueueCommand { drain_intake })
            }
        }

        deserializer.deserialize_any(PauseQueueCommandVisitor)
    }
}

/// The durable change-record position: the log epoch plus sequence that produced the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ChangeRecordPosition {
    pub backend_epoch: u64,
    pub sequence: u64,
}

impl From<&CommandPosition> for ChangeRecordPosition {
    fn from(value: &CommandPosition) -> Self {
        Self {
            backend_epoch: value.backend_epoch,
            sequence: value.sequence,
        }
    }
}

/// Change-record command classification. One committed command can fan out to many records, but the
/// command kind stays the same across the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ChangeRecordKind {
    Push,
    Claim,
    CohortClaim,
    RenewLease,
    CohortRenewLease,
    ReassignLease,
    Finalize,
    CohortFinalize,
    ReplacePending,
    UpdateFields,
    MutateItems,
    LeaseExpired,
    CohortExpired,
    FenceLease,
    UnfenceLease,
    PauseQueue,
    ResumeQueue,
    PurgeItems,
    SetGates,
}

/// Serde-safe lifecycle state for emitted history records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChangeRecordState {
    Pending,
    Leased,
    Complete,
    Failed,
}

/// One emitted history record derived from a committed command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangeRecord {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub item_id: Option<ItemId>,
    pub position: ChangeRecordPosition,
    pub command_kind: ChangeRecordKind,
    #[serde(default)]
    pub new_state: Option<ChangeRecordState>,
    pub item_version: Option<u64>,
    pub terminal_at: Option<UtcTimestamp>,
    #[serde(default)]
    pub emitted_at: Option<UtcTimestamp>,
    #[serde(default)]
    pub source_owner_id: Option<OwnerId>,
    pub source_epoch: u64,
}

impl ChangeRecord {
    pub fn idempotency_key(&self) -> (TenantId, QueueId, Option<ItemId>, u64, u64) {
        (
            self.tenant_id.clone(),
            self.queue_id.clone(),
            self.item_id,
            self.position.backend_epoch,
            self.position.sequence,
        )
    }
}

fn queue_scoped_change_record(
    shard: &QueueKey,
    position: &CommandPosition,
    command_kind: ChangeRecordKind,
    emitted_at: UtcTimestamp,
    source_owner_id: Option<OwnerId>,
    source_epoch: u64,
) -> ChangeRecord {
    ChangeRecord {
        tenant_id: shard.tenant_id.clone(),
        queue_id: shard.queue_id.clone(),
        item_id: None,
        position: position.into(),
        command_kind,
        new_state: None,
        item_version: None,
        terminal_at: None,
        emitted_at: Some(emitted_at),
        source_owner_id,
        source_epoch,
    }
}

#[allow(clippy::too_many_arguments)]
fn item_change_record(
    shard: &QueueKey,
    item_id: ItemId,
    position: &CommandPosition,
    command_kind: ChangeRecordKind,
    new_state: Option<ChangeRecordState>,
    item_version: Option<u64>,
    terminal_at: Option<UtcTimestamp>,
    emitted_at: UtcTimestamp,
    source_owner_id: Option<OwnerId>,
    source_epoch: u64,
) -> ChangeRecord {
    ChangeRecord {
        tenant_id: shard.tenant_id.clone(),
        queue_id: shard.queue_id.clone(),
        item_id: Some(item_id),
        position: position.into(),
        command_kind,
        new_state,
        item_version,
        terminal_at,
        emitted_at: Some(emitted_at),
        source_owner_id,
        source_epoch,
    }
}

fn finalize_state(kind: FinalizeKind, applied_state: Option<ItemState>) -> Option<ItemState> {
    match kind {
        FinalizeKind::Complete => Some(ItemState::Complete),
        FinalizeKind::Fail => Some(ItemState::Failed),
        FinalizeKind::Retry => applied_state.or(Some(ItemState::Pending)),
        FinalizeKind::Release | FinalizeKind::Rearm => Some(ItemState::Pending),
    }
}

fn change_record_state(state: ItemState) -> ChangeRecordState {
    match state {
        ItemState::Pending => ChangeRecordState::Pending,
        ItemState::Leased => ChangeRecordState::Leased,
        ItemState::Complete => ChangeRecordState::Complete,
        ItemState::Failed => ChangeRecordState::Failed,
    }
}

/// Map one committed command envelope to zero or more history records.
pub fn command_envelope_change_records(
    shard: &QueueKey,
    position: &CommandPosition,
    env: &CommandEnvelope,
    emitted_at: UtcTimestamp,
    source_owner_id: Option<OwnerId>,
) -> Vec<ChangeRecord> {
    let source_epoch = position.backend_epoch;
    match &env.command {
        QueueCommand::Push(push) => push
            .items
            .iter()
            .map(|item| {
                item_change_record(
                    shard,
                    item.item_id,
                    position,
                    ChangeRecordKind::Push,
                    Some(ChangeRecordState::Pending),
                    Some(1),
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::Claim(claim) => claim
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::Claim,
                    Some(ChangeRecordState::Leased),
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::CohortClaim(claim) => claim
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::CohortClaim,
                    Some(ChangeRecordState::Leased),
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::RenewLease(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::RenewLease,
                    Some(ChangeRecordState::Leased),
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::CohortRenewLease(_) => vec![queue_scoped_change_record(
            shard,
            position,
            ChangeRecordKind::CohortRenewLease,
            emitted_at,
            source_owner_id,
            source_epoch,
        )],
        QueueCommand::ReassignLease(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::ReassignLease,
                    Some(ChangeRecordState::Leased),
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::Finalize(c) => c
            .outcomes
            .iter()
            .map(|outcome| {
                let new_state =
                    finalize_state(outcome.kind, outcome.applied_state).map(change_record_state);
                item_change_record(
                    shard,
                    outcome.item_id,
                    position,
                    ChangeRecordKind::Finalize,
                    new_state,
                    None,
                    matches!(
                        outcome.applied_state,
                        Some(ItemState::Complete) | Some(ItemState::Failed)
                    )
                    .then_some(env.created_at),
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::CohortFinalize(c) => env
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                let new_state = finalize_state(c.kind, None).map(change_record_state);
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::CohortFinalize,
                    new_state,
                    None,
                    matches!(
                        new_state,
                        Some(ChangeRecordState::Complete) | Some(ChangeRecordState::Failed)
                    )
                    .then_some(env.created_at),
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::ReplacePending(c) => vec![
            item_change_record(
                shard,
                c.superseded_item_id,
                position,
                ChangeRecordKind::ReplacePending,
                Some(ChangeRecordState::Pending),
                None,
                None,
                emitted_at,
                source_owner_id.clone(),
                source_epoch,
            ),
            item_change_record(
                shard,
                c.replacement.item_id,
                position,
                ChangeRecordKind::ReplacePending,
                Some(ChangeRecordState::Pending),
                Some(1),
                None,
                emitted_at,
                source_owner_id,
                source_epoch,
            ),
        ],
        QueueCommand::UpdateFields(c) => vec![item_change_record(
            shard,
            c.item_id,
            position,
            ChangeRecordKind::UpdateFields,
            None,
            None,
            None,
            emitted_at,
            source_owner_id,
            source_epoch,
        )],
        QueueCommand::LeaseExpired(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::LeaseExpired,
                    Some(ChangeRecordState::Pending),
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::CohortExpired(_) => env
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::CohortExpired,
                    Some(ChangeRecordState::Failed),
                    None,
                    Some(env.created_at),
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::FenceLease(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::FenceLease,
                    None,
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::UnfenceLease(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::UnfenceLease,
                    None,
                    None,
                    None,
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::PauseQueue(_) => vec![queue_scoped_change_record(
            shard,
            position,
            ChangeRecordKind::PauseQueue,
            emitted_at,
            source_owner_id,
            source_epoch,
        )],
        QueueCommand::ResumeQueue => vec![queue_scoped_change_record(
            shard,
            position,
            ChangeRecordKind::ResumeQueue,
            emitted_at,
            source_owner_id,
            source_epoch,
        )],
        QueueCommand::PurgeItems(c) => c
            .item_ids
            .iter()
            .copied()
            .map(|item_id| {
                item_change_record(
                    shard,
                    item_id,
                    position,
                    ChangeRecordKind::PurgeItems,
                    None,
                    None,
                    Some(env.created_at),
                    emitted_at,
                    source_owner_id.clone(),
                    source_epoch,
                )
            })
            .collect(),
        QueueCommand::MutateItems(c) => {
            let mut records = c
                .items
                .iter()
                .map(|mutation| {
                    let (state, version, terminal_at) = match &mutation.action {
                        ResolvedItemMutationAction::Purge => (None, None, Some(env.created_at)),
                        ResolvedItemMutationAction::Replace(values) => (
                            Some(change_record_state(values.state)),
                            Some(values.item_version),
                            values.state.is_terminal().then_some(env.created_at),
                        ),
                    };
                    item_change_record(
                        shard,
                        mutation.item_id,
                        position,
                        ChangeRecordKind::MutateItems,
                        state,
                        version,
                        terminal_at,
                        emitted_at,
                        source_owner_id.clone(),
                        source_epoch,
                    )
                })
                .collect::<Vec<_>>();
            if !c.gate_changes.is_empty() {
                records.push(queue_scoped_change_record(
                    shard,
                    position,
                    ChangeRecordKind::MutateItems,
                    emitted_at,
                    source_owner_id,
                    source_epoch,
                ));
            }
            records
        }
        QueueCommand::SetGates(_) => vec![queue_scoped_change_record(
            shard,
            position,
            ChangeRecordKind::SetGates,
            emitted_at,
            source_owner_id,
            source_epoch,
        )],
        QueueCommand::CreateQueue(_)
        | QueueCommand::WriteSideRecords(_)
        | QueueCommand::AdvanceInstanceFence(_) => Vec::new(),
    }
}

/// A durable command record — the append unit for the log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub request_id: Option<RequestId>,
    /// Stable fingerprint of the request body for `request_id` replay/conflict decisions.
    ///
    /// This is durable log metadata, not projection state. Hybrid object-log modes can rebuild
    /// replay-response idempotency from committed envelopes even when the SQLite projection has not yet
    /// applied the request-id row. `None` is allowed for legacy log entries and for commands with no
    /// `request_id`.
    #[serde(default)]
    pub request_fingerprint: Option<u64>,
    /// Replayable response material for the request-id-bearing command. The first supported family is push:
    /// after an unknown outcome, a retry must return the originally assigned item ids without appending a
    /// second push.
    #[serde(default)]
    pub request_outcome: Option<RequestOutcome>,
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RequestOutcome {
    Push {
        item_ids: Vec<ItemId>,
    },
    /// Serialized ordered API-001 BatchUpdate result vector. The payload is deliberately opaque to the
    /// command model; the owning backend decodes it into its public outcome type during idempotent replay.
    BatchUpdate {
        response_payload: String,
    },
    /// Serialized ordered backend-erased mutation response. The durable envelope position is restored
    /// from the enclosing log record on replay.
    ItemMutation {
        response_payload: String,
    },
    /// Durable replay payload for API-004 ClaimByQuery. The clear lease token is part of the response and
    /// must be returned unchanged on same-body request-id replay while the recorded leases remain active.
    ClaimByQuery {
        item_ids: Vec<ItemId>,
        lease_token: LeaseToken,
        #[serde(default)]
        worker_id: Option<WorkerId>,
    },
    /// The full per-entry outcome of a `commit_transition` (committed AND rejected entries), recorded on a
    /// terminal marker envelope so recovery can rebuild the whole `Vec<EntryRecovery>` — not just the
    /// committed, `Finalize`-delimited subset — for a mixed committed+rejected commit (bead pqueue-db60657d).
    /// A rejected entry mutates and appends nothing itself, so without this record its outcome is lost across
    /// a restart. Additive: logs written before this variant existed simply lack it, and recovery falls back
    /// to the committed-only reconstruction + length-guard (see `rebuild_commit_idempotency_from_log`).
    CommitTransition {
        entries: Vec<CommitOutcomeEntry>,
    },
}

/// Durable, serializable mirror of one commit entry's [`EntryRecovery`](crate::port::EntryRecovery) (which
/// carries non-`Serialize` types — an [`EngineError`](crate::error::EngineError) in its rejected arm). Carried
/// in [`RequestOutcome::CommitTransition`] so a mixed commit's per-entry outcome — committed AND rejected,
/// with the rejection's structured error — round-trips byte-identically across a restart (bead
/// pqueue-db60657d). `rejection == None` means the entry committed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitOutcomeEntry {
    pub consumed_input_id: ItemId,
    #[serde(default)]
    pub additional_consumed_input_ids: Vec<ItemId>,
    #[serde(default)]
    pub instance: Option<(Vec<u8>, u64)>,
    #[serde(default)]
    pub side_record_keys: Vec<Vec<u8>>,
    #[serde(default)]
    pub lifecycle_item_ids: Vec<ItemId>,
    /// `None` = committed; `Some` = rejected, carrying the structured rejection.
    #[serde(default)]
    pub rejection: Option<crate::error::CommitRejection>,
}

/// Fail closed when a hybrid/object-log request-id command is missing the metadata required to replay a
/// committed-but-unreturned outcome. Callers use this on hybrid-async admission paths where the protocol
/// must not accept a request-id-bearing mutation unless the committed envelope can reconstruct both the
/// fingerprint and the response.
pub fn validate_request_replay_metadata(env: &CommandEnvelope) -> EngineResult<()> {
    if env.request_id.is_some()
        && (env.request_fingerprint.is_none() || env.request_outcome.is_none())
    {
        return Err(EngineError::Unavailable);
    }
    Ok(())
}

#[cfg(test)]
mod serde_tests {
    //! Round-trip every command variant through JSON, so a durable backend can persist the log and
    //! replay it (Phase 3 enabler). No `PartialEq` on the command tree, so fidelity is checked by
    //! re-serializing the decoded value and comparing the JSON.
    use super::*;
    use bytes::Bytes;
    use fireweed_core::{PriorityValue, UtcTimestamp};

    fn iid(s: &str) -> ItemId {
        // Test ids are arbitrary labels; map each to a stable, distinct packed `ItemId` (these tests only
        // assert serde round-trips the value — the exact bits are immaterial).
        ItemId::from_u64(
            s.bytes()
                .fold(0u64, |a, b| a.wrapping_mul(131).wrapping_add(b as u64)),
        )
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
            metadata: Metadata::default(),
            cohort_size: Some(4),
            gate_keys: Vec::new(),
            entity_document: None,
        }
    }

    fn envelope(command: QueueCommand) -> CommandEnvelope {
        envelope_with_item_ids(command, vec![iid("a")])
    }

    fn envelope_with_item_ids(command: QueueCommand, item_ids: Vec<ItemId>) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("c1"),
            request_id: Some(RequestId::new("r1").unwrap()),
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(42),
            created_at: ts(1),
        }
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
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
                worker_id: None,
            }),
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("a")],
                lease_expires_at: ts(200),
            }),
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: iid("a"),
                    kind: FinalizeKind::Retry,
                    applied_state: None,
                    not_before: Some(ts(500)),
                }],
            }),
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("k").unwrap(),
                superseded_item_id: iid("old"),
                replacement: item(),
            }),
            QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: iid("a"),
                field_ops: BTreeMap::from([
                    ("state".to_string(), Some(Bytes::from_static(b"leased"))),
                    ("stale".to_string(), None),
                ]),
                payload: PayloadUpdate::Set(Some(Bytes::from_static(b"body"))),
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Keep,
                set_entity_document: None,
                set_fields: None,
                set_metadata: None,
                set_gate_keys: None,
                api001_batch: false,
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
            QueueCommand::PauseQueue(PauseQueueCommand::default()),
            QueueCommand::ResumeQueue,
            QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: vec![iid("a")],
                force: true,
            }),
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![SideRecord {
                    key: b"state/run-1".to_vec(),
                    payload: Bytes::from_static(b"opaque-state"),
                }],
            }),
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance/run-1".to_vec(),
                expected: 7,
                next: 8,
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

    #[test]
    fn legacy_envelope_defaults_request_replay_metadata() {
        let json = r#"{
            "command_id":"c1",
            "request_id":"r1",
            "item_ids":[],
            "command":{"PauseQueue":null},
            "checksum":42,
            "created_at":{"seconds":1,"nanoseconds":0}
        }"#;
        let decoded: CommandEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.request_fingerprint, None);
        assert_eq!(decoded.request_outcome, None);
    }

    #[test]
    fn intake_blocking_pause_round_trips_through_json() {
        let env = envelope(QueueCommand::PauseQueue(PauseQueueCommand {
            drain_intake: true,
        }));
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains(r#""drain_intake":true"#));
        let decoded: CommandEnvelope = serde_json::from_str(&json).unwrap();
        let QueueCommand::PauseQueue(c) = decoded.command else {
            panic!("expected pause command");
        };
        assert!(c.drain_intake);
    }

    #[test]
    fn hybrid_request_id_envelopes_require_replay_metadata() {
        let mut env = envelope(QueueCommand::PauseQueue(PauseQueueCommand::default()));
        assert_eq!(
            validate_request_replay_metadata(&env),
            Err(EngineError::Unavailable)
        );

        env.request_fingerprint = Some(7);
        env.request_outcome = Some(RequestOutcome::Push { item_ids: vec![] });
        assert_eq!(validate_request_replay_metadata(&env), Ok(()));
    }

    #[test]
    fn change_record_synthesis_cohort_variants() {
        #[derive(Debug)]
        struct ExpectedRecord {
            item_id: Option<ItemId>,
            kind: ChangeRecordKind,
            new_state: Option<ChangeRecordState>,
            item_version: Option<u64>,
            terminal_at: Option<UtcTimestamp>,
        }

        fn expect_records(
            records: &[ChangeRecord],
            expected: &[ExpectedRecord],
            emitted_at: UtcTimestamp,
        ) {
            assert_eq!(records.len(), expected.len());
            for (record, expected) in records.iter().zip(expected.iter()) {
                assert_eq!(record.item_id, expected.item_id);
                assert_eq!(record.command_kind, expected.kind);
                assert_eq!(record.new_state, expected.new_state);
                assert_eq!(record.item_version, expected.item_version);
                assert_eq!(record.terminal_at, expected.terminal_at);
                assert_eq!(record.emitted_at, Some(emitted_at));
            }
        }

        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let emitted_at = ts(99);
        let cases: Vec<(&str, Vec<ItemId>, QueueCommand, Vec<ExpectedRecord>)> = vec![
            (
                "push",
                vec![iid("a")],
                QueueCommand::Push(PushCommand {
                    items: vec![
                        item(),
                        PushItem {
                            item_id: iid("b"),
                            ..item()
                        },
                    ],
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::Push,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: Some(1),
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::Push,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: Some(1),
                        terminal_at: None,
                    },
                ],
            ),
            (
                "claim",
                vec![iid("a")],
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![iid("a"), iid("b")],
                    lease_token: LeaseToken::new("lease").unwrap(),
                    lease_expires_at: ts(100),
                    worker_id: None,
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::Claim,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::Claim,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "cohort-claim",
                vec![iid("a")],
                QueueCommand::CohortClaim(CohortClaimCommand {
                    cohort_id: CohortId::new("cohort").unwrap(),
                    item_ids: vec![iid("a"), iid("b")],
                    lease_token: LeaseToken::new("lease").unwrap(),
                    lease_expires_at: ts(100),
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::CohortClaim,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::CohortClaim,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "renew-lease",
                vec![iid("a")],
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: vec![iid("a"), iid("b")],
                    lease_expires_at: ts(200),
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::RenewLease,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::RenewLease,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "cohort-renew-lease",
                vec![iid("a")],
                QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
                    cohort_id: CohortId::new("cohort").unwrap(),
                    lease_expires_at: ts(200),
                }),
                vec![ExpectedRecord {
                    item_id: None,
                    kind: ChangeRecordKind::CohortRenewLease,
                    new_state: None,
                    item_version: None,
                    terminal_at: None,
                }],
            ),
            (
                "reassign-lease",
                vec![iid("a")],
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: vec![iid("a"), iid("b")],
                    lease_token: LeaseToken::new("lease").unwrap(),
                    lease_expires_at: ts(200),
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::ReassignLease,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::ReassignLease,
                        new_state: Some(ChangeRecordState::Leased),
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "finalize",
                vec![iid("a")],
                QueueCommand::Finalize(FinalizeCommand {
                    outcomes: vec![
                        FinalizeOutcome {
                            item_id: iid("a"),
                            kind: FinalizeKind::Complete,
                            applied_state: Some(ItemState::Complete),
                            not_before: None,
                        },
                        FinalizeOutcome {
                            item_id: iid("b"),
                            kind: FinalizeKind::Retry,
                            applied_state: Some(ItemState::Failed),
                            not_before: Some(ts(500)),
                        },
                    ],
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::Finalize,
                        new_state: Some(ChangeRecordState::Complete),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::Finalize,
                        new_state: Some(ChangeRecordState::Failed),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                ],
            ),
            (
                "cohort-finalize",
                vec![iid("a"), iid("b")],
                QueueCommand::CohortFinalize(CohortFinalizeCommand {
                    cohort_id: CohortId::new("cohort").unwrap(),
                    kind: FinalizeKind::Fail,
                    not_before: Some(ts(500)),
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::CohortFinalize,
                        new_state: Some(ChangeRecordState::Failed),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::CohortFinalize,
                        new_state: Some(ChangeRecordState::Failed),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                ],
            ),
            (
                "replace-pending",
                vec![iid("a")],
                QueueCommand::ReplacePending(ReplacePendingCommand {
                    client_item_key: ClientItemKey::new("k").unwrap(),
                    superseded_item_id: iid("old"),
                    replacement: PushItem {
                        item_id: iid("new"),
                        ..item()
                    },
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("old")),
                        kind: ChangeRecordKind::ReplacePending,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("new")),
                        kind: ChangeRecordKind::ReplacePending,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: Some(1),
                        terminal_at: None,
                    },
                ],
            ),
            (
                "update-fields",
                vec![iid("a")],
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id: iid("a"),
                    field_ops: BTreeMap::from([
                        ("state".to_string(), Some(Bytes::from_static(b"leased"))),
                        ("stale".to_string(), None),
                    ]),
                    payload: PayloadUpdate::Set(Some(Bytes::from_static(b"body"))),
                    set_priority: ScheduleUpdate::Keep,
                    set_not_before: ScheduleUpdate::Keep,
                    set_entity_document: None,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![ExpectedRecord {
                    item_id: Some(iid("a")),
                    kind: ChangeRecordKind::UpdateFields,
                    new_state: None,
                    item_version: None,
                    terminal_at: None,
                }],
            ),
            (
                "lease-expired",
                vec![iid("a")],
                QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: vec![iid("a"), iid("b")],
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::LeaseExpired,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::LeaseExpired,
                        new_state: Some(ChangeRecordState::Pending),
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "cohort-expired",
                vec![iid("a"), iid("b")],
                QueueCommand::CohortExpired(CohortExpiredCommand {
                    group_key: GroupKey::new("g").unwrap(),
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::CohortExpired,
                        new_state: Some(ChangeRecordState::Failed),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::CohortExpired,
                        new_state: Some(ChangeRecordState::Failed),
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                ],
            ),
            (
                "fence-lease",
                vec![iid("a")],
                QueueCommand::FenceLease(FenceLeaseCommand {
                    item_ids: vec![iid("a"), iid("b")],
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::FenceLease,
                        new_state: None,
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::FenceLease,
                        new_state: None,
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "unfence-lease",
                vec![iid("a")],
                QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                    item_ids: vec![iid("a"), iid("b")],
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::UnfenceLease,
                        new_state: None,
                        item_version: None,
                        terminal_at: None,
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::UnfenceLease,
                        new_state: None,
                        item_version: None,
                        terminal_at: None,
                    },
                ],
            ),
            (
                "pause-queue",
                vec![iid("a")],
                QueueCommand::PauseQueue(PauseQueueCommand::default()),
                vec![ExpectedRecord {
                    item_id: None,
                    kind: ChangeRecordKind::PauseQueue,
                    new_state: None,
                    item_version: None,
                    terminal_at: None,
                }],
            ),
            (
                "resume-queue",
                vec![iid("a")],
                QueueCommand::ResumeQueue,
                vec![ExpectedRecord {
                    item_id: None,
                    kind: ChangeRecordKind::ResumeQueue,
                    new_state: None,
                    item_version: None,
                    terminal_at: None,
                }],
            ),
            (
                "purge-items",
                vec![iid("a")],
                QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: vec![iid("a"), iid("b")],
                    force: true,
                }),
                vec![
                    ExpectedRecord {
                        item_id: Some(iid("a")),
                        kind: ChangeRecordKind::PurgeItems,
                        new_state: None,
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                    ExpectedRecord {
                        item_id: Some(iid("b")),
                        kind: ChangeRecordKind::PurgeItems,
                        new_state: None,
                        item_version: None,
                        terminal_at: Some(ts(1)),
                    },
                ],
            ),
            (
                "set-gates",
                vec![iid("a")],
                QueueCommand::SetGates(SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: true,
                }),
                vec![ExpectedRecord {
                    item_id: None,
                    kind: ChangeRecordKind::SetGates,
                    new_state: None,
                    item_version: None,
                    terminal_at: None,
                }],
            ),
            (
                "write-side-records",
                vec![iid("a")],
                QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                    records: vec![SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::from_static(b"opaque-state"),
                    }],
                }),
                vec![],
            ),
            (
                "advance-instance-fence",
                vec![iid("a")],
                QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                    instance_key: b"instance/run-1".to_vec(),
                    expected: 7,
                    next: 8,
                }),
                vec![],
            ),
        ];

        for (label, item_ids, command, expected) in cases {
            let records = command_envelope_change_records(
                &shard,
                &position,
                &envelope_with_item_ids(command, item_ids),
                emitted_at,
                None,
            );
            expect_records(&records, &expected, emitted_at);
            assert!(
                records
                    .iter()
                    .all(|record| record.position.backend_epoch == 7
                        && record.position.sequence == 11),
                "unexpected position in {label}"
            );
            assert!(
                records
                    .iter()
                    .all(|record| record.tenant_id == shard.tenant_id
                        && record.queue_id == shard.queue_id),
                "unexpected shard identity in {label}"
            );
        }
    }

    #[test]
    fn finalize_retry_exhaustion_synthesizes_terminal_failed_record() {
        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let emitted_at = ts(99);
        let env = envelope(QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: iid("a"),
                kind: FinalizeKind::Retry,
                applied_state: Some(ItemState::Failed),
                not_before: Some(ts(500)),
            }],
        }));

        let records = command_envelope_change_records(&shard, &position, &env, emitted_at, None);
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.command_kind, ChangeRecordKind::Finalize);
        assert_eq!(record.new_state, Some(ChangeRecordState::Failed));
        assert_eq!(record.terminal_at, Some(ts(1)));
        assert_eq!(record.emitted_at, Some(emitted_at));
    }

    #[test]
    fn cohort_finalize_synthesizes_per_member_terminal_records() {
        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let emitted_at = ts(99);
        let env = envelope_with_item_ids(
            QueueCommand::CohortFinalize(CohortFinalizeCommand {
                cohort_id: CohortId::new("cohort").unwrap(),
                kind: FinalizeKind::Fail,
                not_before: Some(ts(500)),
            }),
            vec![iid("a"), iid("b")],
        );

        let records = command_envelope_change_records(&shard, &position, &env, emitted_at, None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].item_id, Some(iid("a")));
        assert_eq!(records[0].command_kind, ChangeRecordKind::CohortFinalize);
        assert_eq!(records[0].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[0].terminal_at, Some(ts(1)));
        assert_eq!(records[1].item_id, Some(iid("b")));
        assert_eq!(records[1].command_kind, ChangeRecordKind::CohortFinalize);
        assert_eq!(records[1].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[1].terminal_at, Some(ts(1)));
    }

    #[test]
    fn exhausted_cohort_retry_change_records_use_uniform_fail_disposition() {
        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let env = envelope_with_item_ids(
            QueueCommand::CohortFinalize(CohortFinalizeCommand {
                cohort_id: CohortId::new("cohort").unwrap(),
                kind: FinalizeKind::Fail,
                not_before: None,
            }),
            vec![iid("a"), iid("b")],
        );

        let records = command_envelope_change_records(&shard, &position, &env, ts(99), None);
        assert_eq!(records[0].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[0].terminal_at, Some(ts(1)));
        assert_eq!(records[1].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[1].terminal_at, Some(ts(1)));
    }

    #[test]
    fn cohort_expired_synthesizes_per_member_terminal_records() {
        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let emitted_at = ts(99);
        let env = envelope_with_item_ids(
            QueueCommand::CohortExpired(CohortExpiredCommand {
                group_key: GroupKey::new("g").unwrap(),
            }),
            vec![iid("a"), iid("b")],
        );

        let records = command_envelope_change_records(&shard, &position, &env, emitted_at, None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].item_id, Some(iid("a")));
        assert_eq!(records[0].command_kind, ChangeRecordKind::CohortExpired);
        assert_eq!(records[0].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[0].terminal_at, Some(ts(1)));
        assert_eq!(records[1].item_id, Some(iid("b")));
        assert_eq!(records[1].command_kind, ChangeRecordKind::CohortExpired);
        assert_eq!(records[1].new_state, Some(ChangeRecordState::Failed));
        assert_eq!(records[1].terminal_at, Some(ts(1)));
    }

    #[test]
    fn test_change_record_idempotency_key_includes_tenant_queue_item_backend_epoch_sequence() {
        let shard = shard();
        let position = CommandPosition::new(shard.clone(), 7, 11);
        let emitted_at = ts(99);
        let records = command_envelope_change_records(
            &shard,
            &position,
            &envelope(QueueCommand::Push(PushCommand {
                items: vec![item()],
            })),
            emitted_at,
            None,
        );
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].idempotency_key(),
            (
                shard.tenant_id.clone(),
                shard.queue_id.clone(),
                Some(iid("a")),
                7,
                11
            )
        );
        assert_eq!(records[0].tenant_id, shard.tenant_id);
        assert_eq!(records[0].queue_id, shard.queue_id);
    }

    #[test]
    fn test_change_record_batch_preserves_command_position_order() {
        let shard = shard();
        let other_shard = QueueKey::new(
            TenantId::new("tenant-2").unwrap(),
            QueueId::new("queue-2").unwrap(),
        );
        let emitted_at = ts(99);
        let batch = [
            (
                CommandPosition::new(shard.clone(), 7, 1),
                envelope(QueueCommand::Push(PushCommand {
                    items: vec![item()],
                })),
            ),
            (
                CommandPosition::new(shard.clone(), 7, 2),
                envelope(QueueCommand::Finalize(FinalizeCommand {
                    outcomes: vec![FinalizeOutcome::new(iid("a"), FinalizeKind::Complete)],
                })),
            ),
        ];
        let records = batch
            .iter()
            .flat_map(|(position, env)| {
                command_envelope_change_records(&shard, position, env, emitted_at, None)
            })
            .collect::<Vec<_>>();
        let positions = records
            .iter()
            .map(|record| (record.position.backend_epoch, record.position.sequence))
            .collect::<Vec<_>>();
        assert_eq!(positions, vec![(7, 1), (7, 2)]);

        let other_records = command_envelope_change_records(
            &other_shard,
            &CommandPosition::new(other_shard.clone(), 7, 1),
            &envelope(QueueCommand::Push(PushCommand {
                items: vec![item()],
            })),
            emitted_at,
            None,
        );
        assert_eq!(
            other_records[0].idempotency_key(),
            (
                other_shard.tenant_id.clone(),
                other_shard.queue_id.clone(),
                Some(iid("a")),
                7,
                1,
            )
        );
        assert_ne!(
            records[0].idempotency_key(),
            other_records[0].idempotency_key()
        );
    }
}
