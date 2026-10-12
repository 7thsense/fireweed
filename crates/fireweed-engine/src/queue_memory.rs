//! Shared bounded-memory accounting for the API-007 kernel. Request structure,
//! index amplification, and borrowed persisted scans are shared by physical stores.
//! Byte charges cover payload buffers; these charges also cover owned container
//! headers and repeated names before allocation. No identifier length cap is added.
use crate::queue_planner::RecordSnapshot;
use fireweed_core::queue_contract::*;
use serde::{Deserialize, de::DeserializeOwned};
use std::collections::BTreeSet;
fn decode<T: DeserializeOwned>(raw: &[u8]) -> QueueResult<T> {
    let (value, rest) =
        postcard::take_from_bytes(raw).map_err(|e| QueueError::OutcomeUnknown(e.to_string()))?;
    if !rest.is_empty() {
        return Err(QueueError::OutcomeUnknown(
            "trailing measured persisted bytes".into(),
        ));
    }
    Ok(value)
}

pub fn record_headers(records: usize, fields: usize) -> QueueResult<u64> {
    (fields as u64)
        .checked_mul(256)
        .and_then(|n| n.checked_add(1024))
        .and_then(|n| n.checked_mul(records as u64))
        .ok_or(QueueError::TooLarge)
}

pub fn value_key_bound(value: Option<&FieldValue>) -> QueueResult<u64> {
    match value {
        Some(FieldValue::Text(v)) => (v.len() as u64)
            .checked_mul(2)
            .and_then(|n| n.checked_add(4))
            .ok_or(QueueError::TooLarge),
        Some(FieldValue::Bytes(v)) => (v.len() as u64)
            .checked_mul(2)
            .and_then(|n| n.checked_add(4))
            .ok_or(QueueError::TooLarge),
        Some(_) => Ok(10),
        None => Ok(1),
    }
}
pub fn fields_expansion<'a>(
    values: impl IntoIterator<Item = (&'a str, Option<&'a FieldValue>)>,
    def: &CollectionDefinition,
) -> QueueResult<u64> {
    let mut bytes = 0u64;
    for (name, value) in values {
        let bound = value_key_bound(value)?;
        let multiplicity = 1 + def
            .indexes
            .values()
            .map(|columns| columns.iter().filter(|col| col.as_str() == name).count() as u64)
            .sum::<u64>();
        let expanded = bound
            .checked_mul(multiplicity)
            .and_then(|n| n.checked_add(name.len() as u64))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        bytes = bytes.checked_add(expanded).ok_or(QueueError::TooLarge)?;
    }
    for name in def.indexes.keys() {
        bytes = bytes
            .checked_add(1024)
            .and_then(|n| n.checked_add(name.len() as u64))
            .ok_or(QueueError::TooLarge)?;
    }
    Ok(bytes)
}
pub fn group_expansion_budget(
    group: &CommitGroup,
    snapshot: &RecordSnapshot,
    schema: &QueueSchema,
) -> QueueResult<u64> {
    let mut bytes = 0u64;
    let mut add = |n: u64| -> QueueResult<()> {
        bytes = bytes.checked_add(n).ok_or(QueueError::TooLarge)?;
        Ok(())
    };
    let old_work: BTreeSet<_> = group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        // Insert vectors may replace existing pending rows. Releasing their old
        // unique tuples is materialization too, even when new fields shrink.
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
        .collect();
    for key in old_work {
        if let Some(old) = snapshot.work.get(key) {
            add(fields_expansion(
                old.record
                    .work
                    .fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), Some(v))),
                &schema.work,
            )?)?;
        }
    }
    for work in &group.work_inserts {
        add(fields_expansion(
            work.fields.iter().map(|(k, v)| (k.as_str(), Some(v))),
            &schema.work,
        )?)?;
    }
    for update in &group.claimed_updates {
        add(fields_expansion(
            update
                .patch
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref())),
            &schema.work,
        )?)?;
    }
    for update in &group.work_updates {
        match &update.change {
            UnclaimedChange::Patch(patch) | UnclaimedChange::Administrative { patch, .. } => {
                add(fields_expansion(
                    patch.fields.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                    &schema.work,
                )?)?
            }
            UnclaimedChange::Replace(work) => add(fields_expansion(
                work.fields.iter().map(|(k, v)| (k.as_str(), Some(v))),
                &schema.work,
            )?)?,
            UnclaimedChange::Delete => {}
        }
    }
    for update in &group.state_updates {
        let def = schema
            .state
            .get(&update.collection)
            .ok_or_else(|| QueueError::Invalid("undeclared collection".into()))?;
        if let Some(old) = snapshot
            .state
            .get(&(update.collection.clone(), update.key.clone()))
        {
            add(fields_expansion(
                old.fields.iter().map(|(k, v)| (k.as_str(), Some(v))),
                def,
            )?)?;
        }
        if let StateChange::Put { fields, .. } = &update.change {
            add(fields_expansion(
                fields.iter().map(|(k, v)| (k.as_str(), Some(v))),
                def,
            )?)?;
        }
    }
    for key in group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
    {
        add(identity_expansion(&schema.work, key, None)?)?;
    }
    for u in &group.state_updates {
        let def = &schema.state[&u.collection];
        add(identity_expansion(def, &u.key, Some(&u.collection))?)?;
    }
    bytes.checked_mul(4).ok_or(QueueError::TooLarge)
}
pub fn identity_expansion(
    def: &CollectionDefinition,
    key: &str,
    collection: Option<&str>,
) -> QueueResult<u64> {
    let per = (key.len() as u64)
        .checked_add(collection.map_or(0, |name| name.len() as u64))
        .and_then(|n| n.checked_add(512))
        .ok_or(QueueError::TooLarge)?;
    per.checked_mul(def.indexes.len() as u64 + 1)
        .ok_or(QueueError::TooLarge)
}

// Count the persisted schema's allocation overhead without constructing maps or
// owned strings. Raw schema bytes are already charged before this borrowed scan.
#[derive(Deserialize)]
struct MeasuredSchema {
    work: MeasuredDefinition,
    state: MeasuredState,
    progress_bound_nanos: u64,
    max_attempts: u32,
}
impl MeasuredSchema {
    fn headers(&self) -> QueueResult<u64> {
        let _ = (self.progress_bound_nanos, self.max_attempts);
        self.work
            .headers()?
            .checked_add(self.state.0)
            .ok_or(QueueError::TooLarge)
    }
}
#[derive(Deserialize)]
struct MeasuredDefinition {
    fields: MeasuredFields,
    indexes: MeasuredIndexes,
    unique_indexes: MeasuredNames,
    additional_field_prefixes: MeasuredNames,
}
impl MeasuredDefinition {
    fn headers(&self) -> QueueResult<u64> {
        self.fields
            .0
            .checked_add(self.indexes.0)
            .and_then(|n| n.checked_add(self.unique_indexes.0))
            .and_then(|n| n.checked_add(self.additional_field_prefixes.0))
            .ok_or(QueueError::TooLarge)
    }
}
struct MeasuredFields(u64);
struct MeasuredIndexes(u64);
struct MeasuredNames(u64);
struct MeasuredState(u64);
fn measured_add<E: serde::de::Error>(total: &mut u64, value: u64) -> Result<(), E> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| E::custom("schema allocation count overflow"))?;
    Ok(())
}
impl<'de> Deserialize<'de> for MeasuredFields {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredFields;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("field map")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut total = 0;
                while map.next_key::<&'de str>()?.is_some() {
                    map.next_value::<FieldType>()?;
                    measured_add::<A::Error>(&mut total, 256)?;
                }
                Ok(MeasuredFields(total))
            }
        }
        d.deserialize_map(V)
    }
}
impl<'de> Deserialize<'de> for MeasuredNames {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredNames;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("name sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut total = 0;
                while seq.next_element::<&'de str>()?.is_some() {
                    measured_add::<A::Error>(&mut total, 64)?;
                }
                Ok(MeasuredNames(total))
            }
        }
        d.deserialize_seq(V)
    }
}
impl<'de> Deserialize<'de> for MeasuredIndexes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredIndexes;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("index map")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut total = 0;
                while map.next_key::<&'de str>()?.is_some() {
                    let names = map.next_value::<MeasuredNames>()?;
                    measured_add::<A::Error>(&mut total, 256)?;
                    measured_add::<A::Error>(&mut total, names.0)?;
                }
                Ok(MeasuredIndexes(total))
            }
        }
        d.deserialize_map(V)
    }
}
impl<'de> Deserialize<'de> for MeasuredState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredState;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("collection map")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut total = 0;
                while map.next_key::<&'de str>()?.is_some() {
                    let def = map.next_value::<MeasuredDefinition>()?;
                    measured_add::<A::Error>(&mut total, 512)?;
                    let n = def
                        .headers()
                        .map_err(|_| serde::de::Error::custom("schema header count overflow"))?;
                    measured_add::<A::Error>(&mut total, n)?;
                }
                Ok(MeasuredState(total))
            }
        }
        d.deserialize_map(V)
    }
}

pub trait IncomingHeaders {
    fn headers(&self) -> QueueResult<u64>;
}
pub fn incoming_headers(request: &impl IncomingHeaders) -> QueueResult<u64> {
    request
        .headers()?
        .checked_mul(4)
        .ok_or(QueueError::TooLarge)
}
impl IncomingHeaders for InsertRequest {
    fn headers(&self) -> QueueResult<u64> {
        self.items.iter().try_fold(0u64, |n, item| {
            n.checked_add(record_headers(1, item.fields.len())?)
                .ok_or(QueueError::TooLarge)
        })
    }
}
impl IncomingHeaders for PreviewRequest {
    fn headers(&self) -> QueueResult<u64> {
        self.group.headers()
    }
}
impl IncomingHeaders for CommitBatch {
    fn headers(&self) -> QueueResult<u64> {
        self.groups.iter().try_fold(0u64, |bytes, group| {
            bytes
                .checked_add(group.headers()?)
                .ok_or(QueueError::TooLarge)
        })
    }
}

impl IncomingHeaders for CommitGroup {
    fn headers(&self) -> QueueResult<u64> {
        let mut bytes = 0u64;
        let mut add = |n: u64| -> QueueResult<()> {
            bytes = bytes.checked_add(n).ok_or(QueueError::TooLarge)?;
            Ok(())
        };
        let group = self;
        {
            for work in &group.work_inserts {
                add(record_headers(1, work.fields.len())?)?;
            }
            for u in &group.claimed_updates {
                add(record_headers(1, u.patch.fields.len())?)?;
            }
            for u in &group.work_updates {
                let fields = match &u.change {
                    UnclaimedChange::Patch(p)
                    | UnclaimedChange::Administrative { patch: p, .. } => p.fields.len(),
                    UnclaimedChange::Replace(w) => w.fields.len(),
                    UnclaimedChange::Delete => 0,
                };
                add(record_headers(1, fields)?)?;
            }
            for u in &group.state_updates {
                let fields = match &u.change {
                    StateChange::Put { fields, .. } => fields.len(),
                    StateChange::Delete | StateChange::Check => 0,
                };
                add(record_headers(1, fields)?)?;
            }
        }
        if let Some(admin) = &group.administration {
            add(admin.intent.headers()?)?;
        }
        Ok(bytes)
    }
}

impl IncomingHeaders for fireweed_core::queue_administration::AdministrativeIntent {
    fn headers(&self) -> QueueResult<u64> {
        use fireweed_core::queue_administration::*;
        let selector = |selector: &WorkSelector| -> QueueResult<u64> {
            let mut bytes = (selector.nodes.len() as u64)
                .checked_mul(256)
                .ok_or(QueueError::TooLarge)?;
            for node in &selector.nodes {
                let count = match node {
                    SelectorNode::Keys(keys) => keys.len(),
                    SelectorNode::Field {
                        predicate: Predicate::In(values),
                        ..
                    } => values.len(),
                    SelectorNode::StoredLifecycleIn(values)
                    | SelectorNode::EffectiveLifecycleIn(values) => values.len(),
                    _ => 0,
                };
                bytes = bytes
                    .checked_add(
                        (count as u64)
                            .checked_mul(256)
                            .ok_or(QueueError::TooLarge)?,
                    )
                    .ok_or(QueueError::TooLarge)?;
            }
            Ok(bytes)
        };
        let action = |action: &AdministrativeAction| -> QueueResult<u64> {
            match action {
                AdministrativeAction::Update {
                    patch,
                    remove_field_prefixes,
                    ..
                } => record_headers(1, patch.fields.len())?
                    .checked_add(
                        (remove_field_prefixes.len() as u64)
                            .checked_mul(256)
                            .ok_or(QueueError::TooLarge)?,
                    )
                    .ok_or(QueueError::TooLarge),
                AdministrativeAction::Delete { .. } => Ok(1024),
            }
        };
        let mut bytes = self
            .before_fields
            .as_ref()
            .map_or(0, |fields| fields.len() as u64)
            .checked_mul(256)
            .ok_or(QueueError::TooLarge)?;
        match &self.operation {
            AdministrativeOperation::Addressed(entries) => {
                for entry in entries {
                    bytes = bytes
                        .checked_add(selector(&entry.precondition)?)
                        .and_then(|n| n.checked_add(action(&entry.action).ok()?))
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(QueueError::TooLarge)?;
                }
            }
            AdministrativeOperation::SelectFirst(clauses) => {
                for clause in clauses {
                    bytes = bytes
                        .checked_add(selector(&clause.selector)?)
                        .and_then(|n| n.checked_add(selector(&clause.precondition).ok()?))
                        .and_then(|n| n.checked_add(action(&clause.action).ok()?))
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(QueueError::TooLarge)?;
                }
            }
        }
        Ok(bytes)
    }
}
/// Storage-independent owned publication request copying, including both the
/// optional group intent and the separately borrowed intent supplied to assembly.
pub fn publication_request_copy_bound(
    address: &QueueAddress,
    group: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
) -> QueueResult<u64> {
    encoded_len(&(address, group, intent))?
        .checked_mul(12)
        .and_then(|n| n.checked_add(incoming_headers(group).ok()?))
        .and_then(|n| n.checked_add(incoming_headers(intent).ok()?))
        .and_then(|n| n.checked_add(65536))
        .ok_or(QueueError::TooLarge)
}
/// Complete fallback fit for every administrative group, before admission or
/// any sibling's effects. Pin bytes have fixed length, so no schema read is needed.
pub fn administrative_envelope_preflight(
    request: &CommitBatch,
    limits: &QueueLimits,
) -> QueueResult<()> {
    administrative_groups_preflight(&request.queue, &request.groups, limits)
}
pub fn administrative_groups_preflight(
    queue: &QueueAddress,
    groups: &[CommitGroup],
    limits: &QueueLimits,
) -> QueueResult<()> {
    use crate::queue_publication::{
        measure_administrative_publication_fallbacks, measure_administrative_receipt_fallbacks,
    };
    let pin = crate::queue_schema_codec::DefinitionPin::from_persisted_bytes(&[]);
    let mut declared = 10u64;
    for group in groups {
        if let Some(admin) = &group.administration {
            if measure_administrative_receipt_fallbacks(&group.request_id)
                .map_err(|_| QueueError::TooLarge)?
                > admin.max_result_bytes
                || measure_administrative_publication_fallbacks(queue, pin, &group.request_id)
                    .map_err(|_| QueueError::TooLarge)?
                    > limits.max_group_bytes
            {
                return Err(QueueError::TooLarge);
            }
            declared = declared
                .checked_add(admin.max_result_bytes)
                .ok_or(QueueError::TooLarge)?;
        }
    }
    if declared > limits.max_response_bytes {
        return Err(QueueError::TooLarge);
    }
    Ok(())
}

// Borrowed scans charge receipt vectors before constructing owned Strings/Vecs.
pub trait MeasuredValue {
    fn headers(&self) -> QueueResult<u64>;
}
pub struct MeasuredList<T> {
    pub headers: u64,
    _item: std::marker::PhantomData<T>,
}
impl<'de, T: Deserialize<'de> + MeasuredValue> Deserialize<'de> for MeasuredList<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de> + MeasuredValue> serde::de::Visitor<'de> for V<T> {
            type Value = MeasuredList<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("measured receipt sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut headers = 64;
                while let Some(item) = seq.next_element::<T>()? {
                    let size = item
                        .headers()
                        .map_err(|_| serde::de::Error::custom("receipt allocation overflow"))?;
                    measured_add::<A::Error>(&mut headers, size)?;
                }
                Ok(MeasuredList {
                    headers,
                    _item: std::marker::PhantomData,
                })
            }
        }
        d.deserialize_seq(V(std::marker::PhantomData))
    }
}
struct MeasuredString;
impl<'de> Deserialize<'de> for MeasuredString {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = <&'de str>::deserialize(d)?;
        Ok(Self)
    }
}
#[derive(Deserialize)]
struct MeasuredVersion {
    collection: Option<MeasuredString>,
    key: MeasuredString,
    version: u64,
}
impl MeasuredValue for MeasuredVersion {
    fn headers(&self) -> QueueResult<u64> {
        let _ = (&self.collection, &self.key, self.version);
        Ok(256)
    }
}
#[derive(Deserialize)]
enum MeasuredRejection {
    StaleLease,
    VersionConflict,
    AlreadyExists,
    NotFound,
    Leased,
    Invalid(MeasuredString),
    UniqueConflict,
    Terminal,
    TooLarge,
}
#[derive(Deserialize)]
enum MeasuredFieldValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(MeasuredString),
    Bytes(MeasuredBorrowedBlob),
}
struct MeasuredBorrowedBlob;
impl<'de> Deserialize<'de> for MeasuredBorrowedBlob {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = <&'de [u8]>::deserialize(d)?;
        Ok(Self)
    }
}
struct MeasuredValueFields(u64);
impl<'de> Deserialize<'de> for MeasuredValueFields {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredValueFields;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("sorted typed before fields")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut headers = 0;
                let mut previous = None;
                while let Some(name) = map.next_key::<&'de str>()? {
                    if previous.is_some_and(|old| old >= name) {
                        return Err(serde::de::Error::custom("invalid before field order"));
                    }
                    previous = Some(name);
                    let value = map.next_value::<MeasuredFieldValue>()?;
                    match value {
                        MeasuredFieldValue::Integer(n) => {
                            let _ = n;
                        }
                        MeasuredFieldValue::Boolean(n) => {
                            let _ = n;
                        }
                        MeasuredFieldValue::Float(n) if !n.is_finite() => {
                            return Err(serde::de::Error::custom("nonfinite before field"));
                        }
                        _ => {}
                    }
                    measured_add::<A::Error>(&mut headers, 256)?;
                }
                Ok(MeasuredValueFields(headers))
            }
        }
        d.deserialize_map(V)
    }
}
struct MeasuredHistogram(u64);
impl<'de> Deserialize<'de> for MeasuredHistogram {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Outcomes;
        impl<'de> Deserialize<'de> for Outcomes {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> serde::de::Visitor<'de> for V {
                    type Value = Outcomes;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("outcome counts")
                    }
                    fn visit_map<A: serde::de::MapAccess<'de>>(
                        self,
                        mut map: A,
                    ) -> Result<Self::Value, A::Error> {
                        let mut previous = None;
                        while let Some(kind) = map.next_key::<fireweed_core::queue_administration::AdministrativeOutcomeKind>()? {
                            if previous.is_some_and(|old| old >= kind) { return Err(serde::de::Error::custom("invalid outcome order")); }
                            previous = Some(kind);
                            if map.next_value::<u64>()? == 0 { return Err(serde::de::Error::custom("zero outcome count")); }
                        }
                        Ok(Outcomes)
                    }
                }
                d.deserialize_map(V)
            }
        }
        #[derive(Deserialize)]
        struct H {
            matched: u64,
            outcomes: Outcomes,
        }
        let h = H::deserialize(d)?;
        let _ = (h.matched, h.outcomes);
        Ok(Self(4096))
    }
}
#[derive(Deserialize)]
struct MeasuredAdministrativeBefore {
    version: u64,
    lifecycle: Lifecycle,
    available_at: i64,
    eligible_since: i64,
    attempts: u32,
    lease_expires_at: Option<i64>,
    fields: MeasuredValueFields,
}
#[derive(Deserialize)]
struct MeasuredAdministrativeItem {
    key: MeasuredString,
    outcome: fireweed_core::queue_administration::AdministrativeOutcome,
    before: Option<MeasuredAdministrativeBefore>,
}
impl MeasuredValue for MeasuredAdministrativeItem {
    fn headers(&self) -> QueueResult<u64> {
        let _ = (&self.key, &self.outcome);
        let before = match &self.before {
            Some(before) => {
                let _ = (
                    before.version,
                    before.lifecycle,
                    before.available_at,
                    before.eligible_since,
                    before.attempts,
                    before.lease_expires_at,
                );
                before.fields.0
            }
            None => 0,
        };
        before.checked_add(1024).ok_or(QueueError::TooLarge)
    }
}
#[derive(Deserialize)]
struct MeasuredAdministrativeClause {
    id: MeasuredString,
    histogram: MeasuredHistogram,
}
impl MeasuredValue for MeasuredAdministrativeClause {
    fn headers(&self) -> QueueResult<u64> {
        let _ = &self.id;
        self.histogram
            .0
            .checked_add(256)
            .ok_or(QueueError::TooLarge)
    }
}
#[derive(Deserialize)]
struct MeasuredAdministrativeResult {
    histogram: MeasuredHistogram,
    items: MeasuredList<MeasuredAdministrativeItem>,
    clauses: MeasuredList<MeasuredAdministrativeClause>,
}
impl MeasuredAdministrativeResult {
    fn headers(&self) -> QueueResult<u64> {
        self.histogram
            .0
            .checked_add(self.items.headers)
            .and_then(|n| n.checked_add(self.clauses.headers))
            .ok_or(QueueError::TooLarge)
    }
}

#[derive(Deserialize)]
enum MeasuredOutcome {
    RequestConflict,
    RequestExpired,
    Committed {
        versions: MeasuredList<MeasuredVersion>,
    },
    Rejected(MeasuredRejection),
    AdministrativeCommitted {
        versions: MeasuredList<MeasuredVersion>,
        administration: MeasuredAdministrativeResult,
    },
}
#[derive(Deserialize)]
struct MeasuredReceipt {
    request_id: MeasuredString,
    outcome: MeasuredOutcome,
}
impl MeasuredValue for MeasuredReceipt {
    fn headers(&self) -> QueueResult<u64> {
        let _ = &self.request_id;
        let size = match &self.outcome {
            MeasuredOutcome::Committed { versions } => versions.headers,
            MeasuredOutcome::AdministrativeCommitted {
                versions,
                administration,
            } => versions
                .headers
                .checked_add(administration.headers()?)
                .ok_or(QueueError::TooLarge)?,
            MeasuredOutcome::Rejected(reason) => {
                let _ = reason;
                64
            }
            _ => 0,
        };
        size.checked_add(256).ok_or(QueueError::TooLarge)
    }
}
#[derive(Deserialize)]
enum MeasuredInsert {
    Inserted {
        key: MeasuredString,
        version: u64,
    },
    Replaced {
        key: MeasuredString,
        version: u64,
    },
    Rejected {
        key: MeasuredString,
        reason: MeasuredRejection,
    },
}
impl MeasuredValue for MeasuredInsert {
    fn headers(&self) -> QueueResult<u64> {
        match self {
            Self::Inserted { key, version } | Self::Replaced { key, version } => {
                let _ = (key, version);
            }
            Self::Rejected { key, reason } => {
                let _ = (key, reason);
            }
        }
        Ok(256)
    }
}
pub struct MeasuredBytes;
impl<'de> Deserialize<'de> for MeasuredBytes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = MeasuredBytes;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("lease byte sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                while seq.next_element::<u8>()?.is_some() {}
                Ok(MeasuredBytes)
            }
        }
        d.deserialize_seq(V)
    }
}
pub trait PersistedStructure {
    fn stored_headers(raw: &[u8]) -> QueueResult<u64>;
    fn validate_persisted(&self) -> QueueResult<()>;
}
impl PersistedStructure for CommitReceipt {
    fn validate_persisted(&self) -> QueueResult<()> {
        let versions = match &self.outcome {
            GroupOutcome::Committed { versions }
            | GroupOutcome::AdministrativeCommitted { versions, .. } => Some(versions),
            _ => None,
        };
        if versions.is_some_and(|versions| versions.iter().any(|version| version.version == 0)) {
            return Err(QueueError::OutcomeUnknown(
                "zero persisted receipt version".into(),
            ));
        }
        if let GroupOutcome::AdministrativeCommitted { administration, .. } = &self.outcome {
            crate::queue_publication::measure_administrative_result(administration).map_err(
                |_| QueueError::OutcomeUnknown("invalid persisted administrative result".into()),
            )?;
        }
        Ok(())
    }
    fn stored_headers(raw: &[u8]) -> QueueResult<u64> {
        decode::<MeasuredReceipt>(raw)?.headers()
    }
}
impl PersistedStructure for Vec<InsertOutcome> {
    fn validate_persisted(&self) -> QueueResult<()> {
        if self.iter().any(|outcome| {
            matches!(
                outcome,
                InsertOutcome::Inserted { version: 0, .. }
                    | InsertOutcome::Replaced { version: 0, .. }
            )
        }) {
            return Err(QueueError::OutcomeUnknown(
                "zero persisted insert version".into(),
            ));
        }
        Ok(())
    }
    fn stored_headers(raw: &[u8]) -> QueueResult<u64> {
        Ok(decode::<MeasuredList<MeasuredInsert>>(raw)?.headers)
    }
}

pub fn administrative_result_headers(
    result: &fireweed_core::queue_administration::AdministrativeResult,
) -> QueueResult<u64> {
    let mut bytes = 4096u64;
    for item in &result.items {
        bytes = bytes
            .checked_add(1024)
            .and_then(|n| {
                n.checked_add(
                    item.before
                        .as_ref()
                        .map_or(0, |before| before.fields.len() as u64)
                        .checked_mul(256)?,
                )
            })
            .ok_or(QueueError::TooLarge)?;
    }
    bytes
        .checked_add(
            (result.clauses.len() as u64)
                .checked_mul(4352)
                .ok_or(QueueError::TooLarge)?,
        )
        .ok_or(QueueError::TooLarge)
}
pub fn receipt_headers(receipt: &CommitReceipt) -> QueueResult<u64> {
    let count = match &receipt.outcome {
        GroupOutcome::Committed { versions }
        | GroupOutcome::AdministrativeCommitted { versions, .. } => versions.len(),
        _ => 0,
    };
    let administration = match &receipt.outcome {
        GroupOutcome::AdministrativeCommitted { administration, .. } => {
            administrative_result_headers(administration)?
        }
        _ => 0,
    };
    (count as u64)
        .checked_mul(256)
        .and_then(|n| n.checked_add(256))
        .and_then(|n| n.checked_add(administration))
        .ok_or(QueueError::TooLarge)
}

pub fn schema_headers(schema: &QueueSchema) -> QueueResult<u64> {
    let definition = |def: &CollectionDefinition| -> QueueResult<u64> {
        let mut bytes = (def.fields.len() as u64)
            .checked_mul(256)
            .ok_or(QueueError::TooLarge)?;
        for columns in def.indexes.values() {
            bytes = bytes
                .checked_add(256)
                .and_then(|n| n.checked_add((columns.len() as u64).checked_mul(64)?))
                .ok_or(QueueError::TooLarge)?;
        }
        bytes
            .checked_add(
                (def.unique_indexes.len() as u64 + def.additional_field_prefixes.len() as u64)
                    .checked_mul(64)
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)
    };
    let mut bytes = definition(&schema.work)?;
    for def in schema.state.values() {
        bytes = bytes
            .checked_add(512)
            .and_then(|n| n.checked_add(definition(def).ok()?))
            .ok_or(QueueError::TooLarge)?;
    }
    Ok(bytes)
}
pub fn filter_headers(filter: &Filter) -> QueueResult<u64> {
    let mut bytes = (filter.keys.as_ref().map_or(0, BTreeSet::len) as u64)
        .checked_mul(256)
        .ok_or(QueueError::TooLarge)?;
    for predicate in filter.fields.values() {
        let values = match predicate {
            Predicate::Equal(_) => 1,
            Predicate::In(values) => values.len(),
            Predicate::Range { lower, upper } => {
                usize::from(lower.is_some()) + usize::from(upper.is_some())
            }
        };
        bytes = bytes
            .checked_add(256)
            .and_then(|n| n.checked_add((values as u64).checked_mul(128)?))
            .ok_or(QueueError::TooLarge)?;
    }
    Ok(bytes)
}
impl IncomingHeaders for ProvisionRequest {
    fn headers(&self) -> QueueResult<u64> {
        schema_headers(&self.schema)
    }
}
impl IncomingHeaders for ClaimRequest {
    fn headers(&self) -> QueueResult<u64> {
        filter_headers(&self.filter)?
            .checked_add(
                (self.fields.as_ref().map_or(0, BTreeSet::len) as u64)
                    .checked_mul(256)
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)
    }
}
impl IncomingHeaders for QueryRequest {
    fn headers(&self) -> QueueResult<u64> {
        filter_headers(&self.filter)?
            .checked_add(256)
            .ok_or(QueueError::TooLarge)
    }
}
impl IncomingHeaders for StatsRequest {
    fn headers(&self) -> QueueResult<u64> {
        let bucket_headers = self
            .buckets
            .as_ref()
            .map_or(0, |d| 256 + d.buckets.len() as u64 * 192);
        filter_headers(&self.filter)?
            .checked_add(bucket_headers)
            .ok_or(QueueError::TooLarge)?
            .checked_add(
                (self.group_by.len() as u64)
                    .checked_mul(64)
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)
    }
}
impl IncomingHeaders for StateReadRequest {
    fn headers(&self) -> QueueResult<u64> {
        (self.keys.len() as u64)
            .checked_mul(64)
            .ok_or(QueueError::TooLarge)
    }
}
impl IncomingHeaders for ReceiptReadRequest {
    fn headers(&self) -> QueueResult<u64> {
        (self.request_ids.len() as u64)
            .checked_mul(64)
            .ok_or(QueueError::TooLarge)
    }
}

#[derive(Deserialize)]
struct LegacyMeasuredSchema {
    work: LegacyMeasuredDefinition,
    state: LegacyMeasuredState,
    progress_bound_nanos: u64,
    max_attempts: u32,
}
#[derive(Deserialize)]
struct LegacyMeasuredDefinition {
    fields: MeasuredFields,
    indexes: MeasuredIndexes,
    unique_indexes: MeasuredNames,
}
impl LegacyMeasuredDefinition {
    fn headers(&self) -> QueueResult<u64> {
        self.fields
            .0
            .checked_add(self.indexes.0)
            .and_then(|n| n.checked_add(self.unique_indexes.0))
            .ok_or(QueueError::TooLarge)
    }
}
struct LegacyMeasuredState(u64);
impl<'de> Deserialize<'de> for LegacyMeasuredState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = LegacyMeasuredState;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("legacy state definitions")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut total = 0;
                while map.next_key::<&'de str>()?.is_some() {
                    measured_add::<A::Error>(&mut total, 512)?;
                    let def = map.next_value::<LegacyMeasuredDefinition>()?;
                    measured_add::<A::Error>(
                        &mut total,
                        def.headers().map_err(|_| {
                            serde::de::Error::custom("legacy schema header count overflow")
                        })?,
                    )?;
                }
                Ok(LegacyMeasuredState(total))
            }
        }
        d.deserialize_map(V)
    }
}
pub fn persisted_legacy_schema_headers(raw: &[u8]) -> QueueResult<u64> {
    let schema = decode::<LegacyMeasuredSchema>(raw)?;
    let _ = (schema.progress_bound_nanos, schema.max_attempts);
    schema
        .work
        .headers()?
        .checked_add(schema.state.0)
        .ok_or(QueueError::TooLarge)
}
pub fn persisted_schema_headers(raw: &[u8]) -> QueueResult<u64> {
    decode::<MeasuredSchema>(raw)?.headers()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn every_retained_receipt_golden_is_measured_without_changing_ordinals() {
        // Historical bytes, independent of current serializer output. These
        // must remain readable when new variants are appended later.
        let outcomes = [
            vec![0],
            vec![1],
            vec![2, 1, 1, 1, b's', 1, b'k', 128, 1],
            vec![3, 0],
            vec![3, 1],
            vec![3, 2],
            vec![3, 3],
            vec![3, 4],
            vec![3, 5, 1, b'x'],
            vec![3, 6],
            vec![3, 7],
        ];
        for outcome in outcomes {
            let raw = [vec![1, b'r'], outcome].concat();
            let receipt: CommitReceipt = postcard::from_bytes(&raw).unwrap();
            assert_eq!(postcard::to_allocvec(&receipt).unwrap(), raw);
            assert!(
                CommitReceipt::stored_headers(&raw).unwrap() >= receipt_headers(&receipt).unwrap()
            );
            let mut trailing = raw;
            trailing.push(0);
            assert!(CommitReceipt::stored_headers(&trailing).is_err());
        }
    }
    #[test]
    fn borrowed_measurements_cover_schema_and_all_receipt_shapes() {
        let schema = QueueSchema {
            work: CollectionDefinition {
                additional_field_prefixes: BTreeSet::new(),
                fields: BTreeMap::from([("k".into(), FieldType::Text)]),
                indexes: BTreeMap::from([("by_k".into(), vec!["k".into()])]),
                unique_indexes: BTreeSet::from(["by_k".into()]),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let raw = postcard::to_allocvec(&schema).unwrap();
        assert_eq!(
            persisted_schema_headers(&raw).unwrap(),
            schema_headers(&schema).unwrap()
        );
        let outcomes = [
            GroupOutcome::RequestConflict,
            GroupOutcome::RequestExpired,
            GroupOutcome::Committed {
                versions: vec![
                    RecordVersion {
                        collection: Some("c".into()),
                        key: "k".into(),
                        version: 1
                    };
                    4096
                ],
            },
            GroupOutcome::Rejected(Rejection::Invalid("why".into())),
            GroupOutcome::Rejected(Rejection::Terminal),
        ];
        for outcome in outcomes {
            let receipt = CommitReceipt {
                request_id: "r".into(),
                outcome,
            };
            let raw = postcard::to_allocvec(&receipt).unwrap();
            assert!(
                CommitReceipt::stored_headers(&raw).unwrap() >= receipt_headers(&receipt).unwrap()
            );
            let mut bad = raw;
            bad.push(0);
            assert!(CommitReceipt::stored_headers(&bad).is_err());
        }
        let outcomes = vec![
            InsertOutcome::Inserted {
                key: "a".into(),
                version: 1,
            },
            InsertOutcome::Replaced {
                key: "b".into(),
                version: 2,
            },
            InsertOutcome::Rejected {
                key: "c".into(),
                reason: Rejection::Leased,
            },
        ];
        assert!(
            Vec::<InsertOutcome>::stored_headers(&postcard::to_allocvec(&outcomes).unwrap())
                .unwrap()
                >= 3 * 256
        );
        let bytes = vec![3u8; 64];
        let raw = postcard::to_allocvec(&bytes).unwrap();
        assert!(decode::<MeasuredBytes>(&raw).is_ok());
    }
    #[test]
    fn amplification_budget_includes_long_names_owners_and_nul_expansion() {
        let index = "i".repeat(32768);
        let key = "k".repeat(32768);
        let collection = "c".repeat(32768);
        let def = CollectionDefinition {
            additional_field_prefixes: BTreeSet::new(),
            fields: BTreeMap::from([("field".into(), FieldType::Bytes)]),
            indexes: BTreeMap::from([(index, vec!["field".into()])]),
            unique_indexes: BTreeSet::new(),
        };
        let field = FieldValue::Bytes(vec![0; 100]);
        let encoded = crate::queue_index::ordered_key(&[Some(&field)]).unwrap();
        assert!(value_key_bound(Some(&field)).unwrap() >= encoded.len() as u64 - 1);
        assert!(fields_expansion([("field", Some(&field))], &def).unwrap() >= 32768 + 2 * 200);
        assert!(identity_expansion(&def, &key, Some(&collection)).unwrap() >= 2 * 65536);
        let schema = QueueSchema {
            work: def,
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let group = CommitGroup {
            administration: None,
            request_id: "r".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![NewWork {
                key,
                priority: 0,
                available_at: 0,
                fields: BTreeMap::from([("field".into(), field)]),
                body: Default::default(),
            }],
        };
        assert!(
            group_expansion_budget(&group, &RecordSnapshot::default(), &schema).unwrap()
                > 4 * 65536
        );
    }
    #[test]
    fn shrinking_insert_vector_charges_old_unique_tuple_release() {
        let mut schema = QueueSchema {
            work: CollectionDefinition {
                additional_field_prefixes: BTreeSet::new(),
                fields: BTreeMap::from([("value".into(), FieldType::Bytes)]),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        for n in 0..32 {
            let name = n.to_string();
            schema
                .work
                .indexes
                .insert(name.clone(), vec!["value".into()]);
            schema.work.unique_indexes.insert(name);
        }
        let work = NewWork {
            key: "old".into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([("value".into(), FieldValue::Bytes(vec![]))]),
            body: Default::default(),
        };
        let group = CommitGroup {
            administration: None,
            request_id: "shrink".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![work.clone()],
        };
        let empty = RecordSnapshot::default();
        let base = group_expansion_budget(&group, &empty, &schema).unwrap();
        let mut old = work;
        old.fields
            .insert("value".into(), FieldValue::Bytes(vec![0; 2048]));
        let mut snapshot = empty;
        snapshot.work.insert(
            "old".into(),
            crate::queue_planner::StoredWork {
                record: WorkRecord {
                    work: old,
                    version: 1,
                    lifecycle: Lifecycle::Pending,
                    attempts: 0,
                    insertion_sequence: 1,
                    eligible_since: 0,
                    lease_expires_at: None,
                },
                lease_hash: None,
                field_coverage: crate::queue_fields::FieldCoverage::Complete,
                body_loaded: true,
            },
        );
        let budget = group_expansion_budget(&group, &snapshot, &schema).unwrap();
        assert!(budget >= base + 4 * 32 * 4096);
    }
}
