use super::*;

use std::collections::HashMap;

use fireweed_core::{
    ClaimByQueryRequest, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken,
    QueueDefinition, QueueIndex, RequestId, UtcTimestamp,
};
use fireweed_engine::{
    ClaimRef, CommandPosition, CommitEntryOutcome, CommitEntryStatus, CommitTransitionEntry,
    EngineError, EngineResult, EntryRecovery, PushItem, PushSpec, QueueKey,
};
pub(crate) use fireweed_relational::*;
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// small conversions / error mapping
// ---------------------------------------------------------------------------

pub(crate) fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

pub(crate) const IDEMPOTENCY_OPERATION_PUSH: &str = "push";

pub(crate) fn push_request_fingerprint(items: &[PushSpec]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

pub(crate) fn request_expires_at(
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<i64> {
    let retention_ms = queues
        .get(shard)
        .map(|d| d.request_id_retention_ms)
        .ok_or(EngineError::NotFound)?;
    Ok(ts_nanos(now).saturating_add((retention_ms as i64).saturating_mul(1_000_000)))
}

pub(crate) fn item_ids_to_json(ids: &[ItemId]) -> EngineResult<String> {
    let raw: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    to_json(&raw)
}

pub(crate) fn item_ids_from_json(raw: String) -> EngineResult<Vec<ItemId>> {
    let decoded: Vec<String> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    decoded
        .into_iter()
        .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
        .collect()
}

pub(crate) fn positions_to_json(positions: &[CommandPosition]) -> EngineResult<String> {
    let raw: Vec<(u64, u64)> = positions
        .iter()
        .map(|pos| (pos.backend_epoch, pos.sequence))
        .collect();
    to_json(&raw)
}

pub(crate) fn positions_from_json(
    shard: &QueueKey,
    raw: &str,
) -> EngineResult<Vec<CommandPosition>> {
    let decoded: Vec<(u64, u64)> =
        serde_json::from_str(raw).map_err(|error| EngineError::Storage(error.to_string()))?;
    Ok(decoded
        .into_iter()
        .map(|(epoch, sequence)| CommandPosition::new(shard.clone(), epoch, sequence))
        .collect())
}

pub(crate) fn check_request_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<ItemId>>> {
    let (t, q) = parts(shard);
    let prior: Option<(Vec<u8>, String, i64)> = st(tx
        .query_row(
            "SELECT request_fingerprint, response_payload, expires_at \
             FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, operation, request_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let Some((prior_fingerprint, response_payload, expires_at)) = prior else {
        return Ok(None);
    };
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, operation, request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(item_ids_from_json(response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_request_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    response_ids: &[ItemId],
    positions: &[CommandPosition],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO fireweed_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
          command_positions,expires_at,created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=excluded.request_fingerprint, \
          response_payload=excluded.response_payload, \
          command_positions=excluded.command_positions, \
          expires_at=excluded.expires_at",
        params![
            t,
            q,
            operation,
            request_id.as_str(),
            fingerprint,
            item_ids_to_json(response_ids)?,
            positions_to_json(positions)?,
            expires_at,
            ts_nanos(now),
        ],
    ))?;
    Ok(())
}

pub(crate) const IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY: &str = "claim_by_query";

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ClaimByQueryReplay {
    pub(crate) item_ids: Vec<ItemId>,
    pub(crate) lease_token: LeaseToken,
    #[serde(default)]
    pub(crate) worker_id: Option<fireweed_core::WorkerId>,
}

pub(crate) fn claim_by_query_fingerprint(request: &ClaimByQueryRequest) -> EngineResult<Vec<u8>> {
    let mut canonical = request.clone();
    canonical.request_id = None;
    let bytes = serde_json::to_vec(&canonical).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

pub(crate) fn check_claim_by_query_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    now: UtcTimestamp,
) -> EngineResult<Option<ClaimByQueryReplay>> {
    let (t, q) = parts(shard);
    let prior: Option<(Vec<u8>, String, i64)> = st(tx
        .query_row(
            "SELECT request_fingerprint, response_payload, expires_at \
             FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![
                t,
                q,
                IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY,
                request_id.as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let Some((prior_fingerprint, response_payload, expires_at)) = prior else {
        return Ok(None);
    };
    if prior_fingerprint != fingerprint {
        return Err(EngineError::RequestIdConflict);
    }
    if expires_at <= ts_nanos(now) {
        return Err(EngineError::RequestExpired);
    }
    let replay: ClaimByQueryReplay =
        serde_json::from_str(&response_payload).map_err(|e| EngineError::Storage(e.to_string()))?;
    if replay.item_ids.is_empty() {
        return Ok(Some(replay));
    }
    let token_hash = lease_hash(&replay.lease_token);
    let item_ids = item_ids_to_json(&replay.item_ids)?;
    let (active_count, earliest_expiry): (i64, Option<i64>) = st(tx.query_row(
        "SELECT COUNT(*), MIN(i.lease_expires_at) \
         FROM json_each(?3) AS requested \
         JOIN fireweed_items AS i \
           ON i.tenant_id=?1 AND i.queue_id=?2 \
          AND i.item_id=CAST(requested.value AS TEXT) \
          AND i.lifecycle_state='Leased' AND i.superseded=0 AND i.fenced=0 \
          AND i.lease_token_hash=?4",
        params![t, q, item_ids, token_hash],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ))?;
    if active_count != i64::try_from(replay.item_ids.len()).map_err(|_| EngineError::Conflict)?
        || earliest_expiry.is_none_or(|expires_at| expires_at <= ts_nanos(now))
    {
        return Err(EngineError::RequestExpired);
    }
    Ok(Some(replay))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_claim_by_query_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    replay: &ClaimByQueryReplay,
    positions: &[CommandPosition],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let response_payload =
        serde_json::to_string(replay).map_err(|e| EngineError::Storage(e.to_string()))?;
    st(tx.execute(
        "INSERT INTO fireweed_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
          command_positions,expires_at,created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            t,
            q,
            IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY,
            request_id.as_str(),
            fingerprint,
            response_payload,
            positions_to_json(positions)?,
            expires_at,
            ts_nanos(now),
        ],
    ))?;
    let item_ids = serde_json::to_string(
        &replay
            .item_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| EngineError::Storage(error.to_string()))?;
    st(tx.execute(
        "INSERT OR IGNORE INTO fireweed_claim_replay_items \
         (tenant_id,queue_id,request_id,item_id) \
         SELECT ?1,?2,?3,value FROM json_each(?4)",
        params![t, q, request_id.as_str(), item_ids],
    ))?;
    Ok(())
}

pub(crate) fn extend_claim_by_query_idempotency_for_renewal(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    renewed_item_ids: &[ItemId],
    renewed_expires_at: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    if renewed_item_ids.is_empty() {
        return Ok(());
    }
    let renewed = serde_json::to_string(
        &renewed_item_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| EngineError::Storage(error.to_string()))?;
    let renewed_expires_at = ts_nanos(renewed_expires_at);
    st(tx.execute(
        "UPDATE fireweed_request_idempotency SET expires_at=max(expires_at,?4) \
         WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id IN ( \
           SELECT edge.request_id FROM fireweed_claim_replay_items edge \
           JOIN json_each(?5) renewed ON renewed.value=edge.item_id \
           WHERE edge.tenant_id=?1 AND edge.queue_id=?2 GROUP BY edge.request_id \
           HAVING COUNT(*)=(SELECT COUNT(*) FROM fireweed_claim_replay_items all_edges \
             WHERE all_edges.tenant_id=?1 AND all_edges.queue_id=?2 \
               AND all_edges.request_id=edge.request_id))",
        params![
            t,
            q,
            IDEMPOTENCY_OPERATION_CLAIM_BY_QUERY,
            renewed_expires_at,
            renewed
        ],
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// C9: authoritative vectorized claimed-work commit — idempotency + validation helpers
// (epic pqueue-2201fd37)
// ---------------------------------------------------------------------------

/// The retained-request-id operation key for the vectorized commit path, distinct from the push key so the
/// two operations never collide on a shared `request_id` in `fireweed_request_idempotency`.
pub(crate) const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";

/// Stable body fingerprint for the commit path: SHA-256 over the serialized entries (the `request_id` is the
/// cache KEY, not part of the body — same shape as [`push_request_fingerprint`]). A different body under the
/// same request id is a `RequestIdConflict`; an equal body replays the stored per-entry outcomes.
pub(crate) fn commit_request_fingerprint(
    entries: &[CommitTransitionEntry],
) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

/// Durable, replay-faithful mirror of an [`EntryRecovery`] (which carries non-`Serialize` types — an
/// [`EngineError`] in its rejected arm and an [`ItemId`]). Projected to this shape for the
/// `fireweed_request_idempotency.response_payload` column and reconstructed verbatim on replay AND for the
/// recovery/explain read (epic pqueue-2201fd37 acceptance #5). A `None` `rejected` means the entry committed.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredEntryRecovery {
    consumed_input_id: String,
    #[serde(default)]
    additional_consumed_input_ids: Vec<String>,
    #[serde(default)]
    instance: Option<(Vec<u8>, u64)>,
    #[serde(default)]
    side_record_keys: Vec<Vec<u8>>,
    #[serde(default)]
    lifecycle_item_ids: Vec<String>,
    /// `None` = committed; `Some((code, detail))` = the structured rejection.
    #[serde(default)]
    rejected: Option<(String, Option<String>)>,
}

/// Stable `(code, detail)` projection of an [`EngineError`] for durable replay. `Invalid`/`Forbidden` carry
/// their `&'static str` reason in `detail` so the exact variant round-trips for the reasons this path emits.
pub(crate) fn encode_engine_error(e: &EngineError) -> (&'static str, Option<String>) {
    match e {
        EngineError::NotFound => ("not_found", None),
        EngineError::QueueDefinitionConflict => ("queue_definition_conflict", None),
        EngineError::Invalid(why) => ("invalid", Some((*why).to_string())),
        EngineError::Terminal => ("terminal", None),
        EngineError::StaleLease => ("stale_lease", None),
        EngineError::Superseded => ("superseded", None),
        EngineError::Unavailable => ("unavailable", None),
        EngineError::Conflict => ("conflict", None),
        EngineError::BatchTooLarge => ("batch_too_large", None),
        EngineError::RequestIdConflict => ("request_id_conflict", None),
        EngineError::RequestExpired => ("request_expired", None),
        EngineError::EpochFenced => ("epoch_fenced", None),
        EngineError::Paused { drain_intake } => (
            "paused",
            Some((if *drain_intake { "true" } else { "false" }).to_string()),
        ),
        EngineError::Forbidden(why) => ("forbidden", Some((*why).to_string())),
        EngineError::Storage(msg) => ("storage", Some(msg.clone())),
        EngineError::DurableDataCorrupt {
            stage,
            manifest_index,
            locator,
        } => (
            "durable_data_corrupt",
            Some(format!("{stage}:{manifest_index}:{locator}")),
        ),
        EngineError::EntitySchemaViolation(msg) => ("entity_schema_violation", Some(msg.clone())),
        EngineError::RequestTooLarge { requested, limit } => {
            ("request_too_large", Some(format!("{requested}:{limit}")))
        }
        EngineError::Backpressure { resource } => ("backpressure", Some((*resource).to_string())),
        // Startup-only; unreachable from the commit path. Named for exhaustive-match completeness.
        EngineError::ChangeRecordsRequireDurableLog => ("change_records_require_durable_log", None),
    }
}

/// Reconstruct an [`EngineError`] from its durable `(code, detail)` projection. `Invalid` reasons this path
/// emits ("item is not leased") round-trip to the same `&'static str`; any other reason falls back to a
/// stable static so the variant (and its `PartialEq`) is preserved.
pub(crate) fn decode_engine_error(code: &str, detail: Option<String>) -> EngineError {
    match code {
        "durable_data_corrupt" => decode_durable_data_corrupt(detail),
        "not_found" => EngineError::NotFound,
        "queue_definition_conflict" => EngineError::QueueDefinitionConflict,
        "invalid" => EngineError::Invalid(match detail.as_deref() {
            Some("item is not leased") => "item is not leased",
            _ => "invalid",
        }),
        "terminal" => EngineError::Terminal,
        "stale_lease" => EngineError::StaleLease,
        "superseded" => EngineError::Superseded,
        "unavailable" => EngineError::Unavailable,
        "conflict" => EngineError::Conflict,
        "batch_too_large" => EngineError::BatchTooLarge,
        "request_id_conflict" => EngineError::RequestIdConflict,
        "request_expired" => EngineError::RequestExpired,
        "epoch_fenced" => EngineError::EpochFenced,
        "forbidden" => EngineError::Forbidden("forbidden"),
        "request_too_large" => {
            let (requested, limit) = detail
                .as_deref()
                .and_then(|value| value.split_once(':'))
                .and_then(|(requested, limit)| Some((requested.parse().ok()?, limit.parse().ok()?)))
                .unwrap_or((usize::MAX, 0));
            EngineError::RequestTooLarge { requested, limit }
        }
        "backpressure" => EngineError::Backpressure {
            resource: "bounded resource",
        },
        _ => EngineError::Storage(detail.unwrap_or_else(|| code.to_string())),
    }
}

fn decode_durable_data_corrupt(detail: Option<String>) -> EngineError {
    let detail = detail.unwrap_or_default();
    let mut fields = detail.splitn(3, ':');
    let stage = fireweed_engine::DurableIntegrityStage::parse(fields.next().unwrap_or_default())
        .unwrap_or(fireweed_engine::DurableIntegrityStage::Manifest);
    EngineError::DurableDataCorrupt {
        stage,
        manifest_index: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
        locator: fields.next().unwrap_or("unknown").to_owned(),
    }
}

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value), mirroring the in-memory `outcomes_from_recovery`.
pub(crate) fn recovery_to_outcomes(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
    recovery
        .iter()
        .map(|r| match &r.status {
            CommitEntryStatus::Committed => CommitEntryOutcome::Committed {
                lifecycle_item_ids: r.lifecycle_item_ids.clone(),
            },
            CommitEntryStatus::Rejected(e) => CommitEntryOutcome::Rejected(e.clone()),
        })
        .collect()
}

pub(crate) fn encode_commit_recovery(recovery: &[EntryRecovery]) -> EngineResult<String> {
    let stored: Vec<StoredEntryRecovery> = recovery
        .iter()
        .map(|r| StoredEntryRecovery {
            consumed_input_id: r.consumed_input_id.to_string(),
            additional_consumed_input_ids: r
                .additional_consumed_input_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
            instance: r.instance.clone(),
            side_record_keys: r.side_record_keys.clone(),
            lifecycle_item_ids: r
                .lifecycle_item_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
            rejected: match &r.status {
                CommitEntryStatus::Committed => None,
                CommitEntryStatus::Rejected(e) => {
                    let (code, detail) = encode_engine_error(e);
                    Some((code.to_string(), detail))
                }
            },
        })
        .collect();
    to_json(&stored)
}

pub(crate) fn decode_commit_recovery(raw: &str) -> EngineResult<Vec<EntryRecovery>> {
    let stored: Vec<StoredEntryRecovery> =
        serde_json::from_str(raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    stored
        .into_iter()
        .map(|s| {
            let additional_consumed_input_ids = s
                .additional_consumed_input_ids
                .into_iter()
                .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
            let lifecycle_item_ids = s
                .lifecycle_item_ids
                .into_iter()
                .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
            let status = match s.rejected {
                None => CommitEntryStatus::Committed,
                Some((code, detail)) => {
                    CommitEntryStatus::Rejected(decode_engine_error(&code, detail))
                }
            };
            Ok(EntryRecovery {
                consumed_input_id: ItemId::new(s.consumed_input_id)
                    .map_err(|e| EngineError::Storage(e.to_string()))?,
                additional_consumed_input_ids,
                instance: s.instance,
                side_record_keys: s.side_record_keys,
                lifecycle_item_ids,
                status,
            })
        })
        .collect()
}

/// Commit-path twin of [`check_request_idempotency`]: same retained-request-id table + replay / conflict /
/// expired classification, but the stored `response_payload` is the rich per-entry outcome vector (encoded
/// via [`encode_commit_outcomes`]) rather than a flat id list. A live record with an equal fingerprint
/// REPLAYS the prior outcomes; a different fingerprint is `RequestIdConflict`; an expired/absent record
/// returns `None` (proceed fresh, deleting the stale row).
pub(crate) fn check_commit_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let prior: Option<(Vec<u8>, String, i64)> = st(tx
        .query_row(
            "SELECT request_fingerprint, response_payload, expires_at \
             FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let Some((prior_fingerprint, response_payload, expires_at)) = prior else {
        return Ok(None);
    };
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(decode_commit_recovery(&response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

/// Recovery/explain read of the retained commit record by `request_id`, IGNORING the body fingerprint (the
/// reader has only the id). Returns the durable recovery while the record is retained; `None` once it has
/// elapsed/been deleted. Read-only — does not delete an expired row (that is the commit path's job).
pub(crate) fn read_commit_recovery(
    conn: &Connection,
    shard: &QueueKey,
    request_id: &RequestId,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let payload: Option<String> = st(conn
        .query_row(
            "SELECT response_payload FROM fireweed_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    match payload {
        Some(raw) => Ok(Some(decode_commit_recovery(&raw)?)),
        None => Ok(None),
    }
}

/// Commit-path twin of [`record_request_idempotency`]: persist the whole-body outcome under the `commit`
/// operation so a later replay returns it verbatim with no second write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_commit_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    recovery: &[EntryRecovery],
    positions: &[CommandPosition],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO fireweed_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
          command_positions,expires_at,created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=excluded.request_fingerprint, \
          response_payload=excluded.response_payload, \
          command_positions=excluded.command_positions, \
          expires_at=excluded.expires_at",
        params![
            t,
            q,
            IDEMPOTENCY_OPERATION_COMMIT,
            request_id.as_str(),
            fingerprint,
            encode_commit_recovery(recovery)?,
            positions_to_json(positions)?,
            expires_at,
            ts_nanos(now),
        ],
    ))?;
    Ok(())
}

/// Pre-commit validation of one [`ClaimRef`] against the durable `fireweed_items` row, with rejection
/// precedence IDENTICAL to the in-memory [`fireweed_projection::ProjectionData::commit_validate`]: absent ->
/// `NotFound`, fenced -> `StaleLease`, terminal -> `Terminal`, superseded -> `Superseded`, non-leased ->
/// `Invalid`, presented-token mismatch -> `StaleLease`, expired lease (half-open: `lease_expires_at < now`)
/// -> `StaleLease`, version-fence mismatch -> `Conflict`. Nothing is mutated.
///
/// LEASE-TOKEN NOTE (flagged): the relational projection persists only the lease token **hash**
/// (`lease_token_hash`), so token authority is checked by hashing the presented token and comparing hashes —
/// whereas the in-memory family compares cleartext tokens. The accept/reject decision (and its precedence)
/// is identical; only the stored representation differs.
pub(crate) fn commit_validate_sql(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    claim_ref: &ClaimRef,
    now: UtcTimestamp,
) -> EngineResult<()> {
    /// `(lifecycle_state, fenced, superseded, lease_token_hash, lease_expires_at, item_version)`.
    type CommitRow = (String, i64, i64, Option<Vec<u8>>, Option<i64>, i64);
    let (t, q) = parts(shard);
    let row: Option<CommitRow> = st(tx
        .query_row(
            "SELECT lifecycle_state, fenced, superseded, lease_token_hash, lease_expires_at, item_version \
             FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, claim_ref.item_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional())?;
    let Some((state, fenced, superseded, lease_token_hash, lease_expires_at, item_version)) = row
    else {
        return Err(EngineError::NotFound);
    };
    let state = parse_state(&state)?;
    if fenced != 0 {
        return Err(EngineError::StaleLease);
    }
    if state.is_terminal() {
        return Err(EngineError::Terminal);
    }
    if superseded != 0 {
        return Err(EngineError::Superseded);
    }
    if state != ItemState::Leased {
        return Err(EngineError::Invalid("item is not leased"));
    }
    // Claim authority: the presented token's hash must equal the stored hash (a forged/stale token differs).
    if lease_token_hash.as_deref() != Some(lease_hash(&claim_ref.lease_token).as_slice()) {
        return Err(EngineError::StaleLease);
    }
    // The lease must be unexpired (half-open, identical to `expired_leases`: expired iff strictly before now).
    if lease_expires_at.is_some_and(|exp| exp < ts_nanos(now)) {
        return Err(EngineError::StaleLease);
    }
    // Optimistic state fence: the caller's observed version must equal the committed version.
    if item_version as u64 != claim_ref.item_version {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

pub(crate) fn ensure_item_text_column(
    conn: &Connection,
    column: &str,
    default_json: &str,
) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!(
        "ALTER TABLE fireweed_items ADD COLUMN {column} TEXT NOT NULL DEFAULT '{default_json}'"
    );
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_fields_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "fields", "{}")
}

pub(crate) fn ensure_queue_pause_drain_intake_column(conn: &Connection) -> EngineResult<()> {
    match conn.execute(
        "ALTER TABLE queues ADD COLUMN pause_drain_intake INTEGER NOT NULL DEFAULT 0",
        [],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_metadata_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "metadata", "{}")
}

pub(crate) fn ensure_item_index_fields_column(conn: &Connection) -> EngineResult<()> {
    match conn.execute("ALTER TABLE fireweed_items ADD COLUMN index_fields BLOB", []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_entity_document_column(conn: &Connection) -> EngineResult<()> {
    match conn.execute(
        "ALTER TABLE fireweed_items ADD COLUMN entity_document TEXT",
        [],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_integer_column(conn: &Connection, column: &str) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!("ALTER TABLE fireweed_items ADD COLUMN {column} INTEGER");
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_terminal_command_epoch_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_integer_column(conn, "terminal_command_epoch")
}

pub(crate) fn ensure_cohort_column(
    conn: &Connection,
    column: &str,
    definition: &str,
) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!("ALTER TABLE fireweed_cohorts ADD COLUMN {column} {definition}");
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_cohort_lifecycle_columns(conn: &Connection) -> EngineResult<()> {
    ensure_cohort_column(conn, "cohort_id", "TEXT")?;
    ensure_cohort_column(conn, "member_count", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_cohort_column(conn, "state", "TEXT NOT NULL DEFAULT 'forming'")?;
    ensure_cohort_column(conn, "cohort_created_at", "INTEGER")?;
    ensure_cohort_column(conn, "first_eligible_at", "INTEGER")?;
    ensure_cohort_column(conn, "expire_command_pos", "INTEGER")?;
    ensure_cohort_column(conn, "cohort_lease_token_hash", "BLOB")?;
    ensure_cohort_column(conn, "retention_until", "INTEGER")?;
    st(conn.execute(
        "UPDATE fireweed_cohorts SET cohort_id=group_key WHERE cohort_id IS NULL",
        [],
    ))?;
    st(conn.execute(
        "UPDATE fireweed_cohorts SET cohort_created_at=created_at WHERE cohort_created_at IS NULL",
        [],
    ))?;
    st(conn.execute(
        "UPDATE fireweed_cohorts SET member_count=(SELECT COUNT(*) FROM fireweed_items i \
         WHERE i.tenant_id=fireweed_cohorts.tenant_id AND i.queue_id=fireweed_cohorts.queue_id \
         AND i.group_key=fireweed_cohorts.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
         AND i.lifecycle_state NOT IN ('Complete','Failed'))",
        [],
    ))?;
    st(conn.execute(
        "UPDATE fireweed_cohorts SET state=CASE \
         WHEN member_count >= cohort_size THEN 'complete' ELSE 'forming' END \
         WHERE state IS NULL OR state='forming' OR state='complete'",
        [],
    ))?;
    st(conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS fireweed_cohorts_claim_idx \
             ON fireweed_cohorts (tenant_id, queue_id, state) WHERE state='complete';\
         CREATE INDEX IF NOT EXISTS fireweed_cohorts_expiry_idx \
             ON fireweed_cohorts (tenant_id, queue_id, cohort_created_at) \
             WHERE state IN ('forming','complete');",
    ))?;
    Ok(())
}

pub(crate) fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

// ---------------------------------------------------------------------------
// ADR-011 typed secondary index helpers
// ---------------------------------------------------------------------------

/// Decode a caller-supplied raw lookup byte slice into a `serde_json::Value` for re-encoding via
/// `IndexDef::index_key` / `CompoundIndexDef::index_key`. Mirrors `decode_typed_lookup_value` in
/// `fireweed_projection` — the two must stay identical so lookup keys byte-match stored keys.
pub(crate) fn decode_typed_lookup_value_rel(
    index_type: &IndexType,
    bytes: &[u8],
) -> EngineResult<JsonValue> {
    match index_type {
        IndexType::String => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(JsonValue::String(s.to_owned()))
        }
        IndexType::Datetime => {
            if let Ok(value @ JsonValue::Number(_)) = serde_json::from_slice::<JsonValue>(bytes) {
                return Ok(value);
            }
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(JsonValue::String(s.to_owned()))
        }
        IndexType::Integer | IndexType::Float => serde_json::from_slice::<JsonValue>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number")),
        IndexType::Boolean => serde_json::from_slice::<JsonValue>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON boolean")),
    }
}

/// Compute the canonical `index_key` bytes for a lookup against a named index. Roundtrips the
/// caller's raw byte slices through their declared types so the result is byte-identical to stored
/// keys regardless of how the caller encoded the lookup value.
pub(crate) fn typed_lookup_canonical_key(
    qi: &QueueIndex,
    key_values: &[Vec<u8>],
) -> EngineResult<Vec<u8>> {
    match &qi.declaration {
        IndexDeclaration::Single(def) => {
            let val = decode_typed_lookup_value_rel(&def.index_type, &key_values[0])?;
            let mut record = serde_json::Map::new();
            record.insert(def.field.clone(), val);
            def.index_key(&JsonValue::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
        IndexDeclaration::Compound(def) => {
            let mut record = serde_json::Map::new();
            for (field, bytes) in def.fields.iter().zip(key_values.iter()) {
                let val = decode_typed_lookup_value_rel(&field.index_type, bytes)?;
                record.insert(field.field.clone(), val);
            }
            def.index_key(&JsonValue::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
    }
}

/// Compute `(index_name, canonical_key_bytes)` pairs for an item's `entity_document`.
/// Returns empty when `typed_indexes` is empty or `entity` is `None` (schema-less queues).
pub(crate) fn typed_index_keys_for_entity(
    typed_indexes: &[QueueIndex],
    entity: Option<&JsonValue>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let Some(entity) = entity else {
        return Ok(vec![]);
    };
    let mut out = Vec::with_capacity(typed_indexes.len());
    for qi in typed_indexes {
        let key = match &qi.declaration {
            IndexDeclaration::Single(def) => def.index_key(entity),
            IndexDeclaration::Compound(def) => def.index_key(entity),
        };
        if let Some(k) = key.map_err(|e| EngineError::Storage(e.to_string()))? {
            out.push((qi.name.clone(), k));
        }
    }
    Ok(out)
}

/// Prefer native [`PushItem::index_fields`]; fall back to entity JSON for pre-native rows.
pub(crate) fn typed_index_keys_for_native(
    typed_indexes: &[QueueIndex],
    index_fields: &std::collections::BTreeMap<String, fireweed_core::TypedValue>,
    entity: Option<&JsonValue>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    if typed_indexes.is_empty() {
        return Ok(vec![]);
    }
    if !index_fields.is_empty() {
        let entity = fireweed_engine::index_fields::index_fields_as_entity(index_fields)?;
        return typed_index_keys_for_entity(typed_indexes, Some(&entity));
    }
    typed_index_keys_for_entity(typed_indexes, entity)
}

pub(crate) fn typed_index_keys_for_push_item(
    typed_indexes: &[QueueIndex],
    item: &PushItem,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    typed_index_keys_for_native(
        typed_indexes,
        &item.index_fields,
        item.entity_document.as_ref(),
    )
}

pub(crate) fn index_is_unique(qi: &QueueIndex) -> bool {
    match &qi.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

/// Check unique-index constraints for `keys` against existing DB rows. Returns `Conflict` if any
/// unique index already maps the same key to a *different* item. Pass `exclude_item_id = Some(id)`
/// when the item whose old rows were just deleted might still appear in DB (i.e. for UpdateFields).
pub(crate) fn check_typed_unique_conflicts(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    keys: &[(String, Vec<u8>)],
    exclude_item_id: Option<&str>,
) -> EngineResult<()> {
    for (name, key) in keys {
        let unique = typed_indexes
            .iter()
            .find(|qi| &qi.name == name)
            .map(index_is_unique)
            .unwrap_or(false);
        if !unique {
            continue;
        }
        let holder: Option<String> = match exclude_item_id {
            Some(excl) => st(tx
                .query_row(
                    "SELECT item_id FROM fireweed_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     AND item_id!=?5 LIMIT 1",
                    params![t, q, name, key.as_slice(), excl],
                    |row| row.get(0),
                )
                .optional())?,
            None => st(tx
                .query_row(
                    "SELECT item_id FROM fireweed_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     LIMIT 1",
                    params![t, q, name, key.as_slice()],
                    |row| row.get(0),
                )
                .optional())?,
        };
        if holder.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

/// Insert `fireweed_item_index` rows for one item's `(name, key)` pairs (upsert so a retry is safe).
pub(crate) fn insert_typed_index_rows(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    item_id: &str,
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    insert_typed_index_rows_batch(tx, t, q, &[(item_id.to_string(), keys.to_vec())])
}

// Item inserts already bind 10×1500 variables, so this SQLite is not the historical 999 cap.
const TYPED_INDEX_CHECK_CHUNK: usize = 1_000;
const TYPED_INDEX_INSERT_CHUNK: usize = 1_500;
type TypedIndexKey = (String, Vec<u8>);
type TypedIndexBatchItem = (String, Vec<TypedIndexKey>);

fn check_typed_unique_conflicts_batch(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    items: &[TypedIndexBatchItem],
) -> EngineResult<()> {
    let unique_names: std::collections::HashSet<&str> = typed_indexes
        .iter()
        .filter(|index| index_is_unique(index))
        .map(|index| index.name.as_str())
        .collect();
    let rows: Vec<_> = items
        .iter()
        .flat_map(|(item_id, keys)| {
            keys.iter()
                .filter(|(name, _)| unique_names.contains(name.as_str()))
                .map(move |(name, key)| (item_id, name, key))
        })
        .collect();
    for chunk in rows.chunks(TYPED_INDEX_CHECK_CHUNK) {
        let values = vec!["(?,?,?)"; chunk.len()].join(",");
        let mut parameters = Vec::with_capacity(chunk.len() * 3 + 2);
        for (item_id, name, key) in chunk {
            parameters.extend([
                Value::Text((*item_id).clone()),
                Value::Text((*name).clone()),
                Value::Blob((*key).clone()),
            ]);
        }
        parameters.extend([Value::Text(t.to_string()), Value::Text(q.to_string())]);
        let conflict: Option<i64> = st(tx
            .query_row(
                &format!(
                    "WITH incoming(item_id,index_name,index_key) AS (VALUES {values}) \
                     SELECT 1 FROM fireweed_item_index existing JOIN incoming \
                       ON existing.index_name=incoming.index_name \
                      AND existing.index_key=incoming.index_key \
                      AND existing.item_id!=incoming.item_id \
                     WHERE existing.tenant_id=? AND existing.queue_id=? LIMIT 1"
                ),
                params_from_iter(parameters.iter()),
                |row| row.get(0),
            )
            .optional())?;
        if conflict.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

fn insert_typed_index_rows_batch(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    items: &[TypedIndexBatchItem],
) -> EngineResult<()> {
    let mut rows: Vec<_> = items
        .iter()
        .flat_map(|(item_id, keys)| keys.iter().map(move |(name, key)| (item_id, name, key)))
        .collect();
    // Insert in index/key order so B-tree leaf splits stay sequential.
    rows.sort_unstable_by(|a, b| a.1.cmp(b.1).then_with(|| a.2.cmp(b.2)));
    for chunk in rows.chunks(TYPED_INDEX_INSERT_CHUNK) {
        let values = vec!["(?,?,?,?,?)"; chunk.len()].join(",");
        let mut parameters = Vec::with_capacity(chunk.len() * 5);
        let tenant = Value::Text(t.to_string());
        let queue = Value::Text(q.to_string());
        for (item_id, name, key) in chunk {
            parameters.push(tenant.clone());
            parameters.push(queue.clone());
            parameters.extend([
                Value::Text((*name).clone()),
                Value::Blob((*key).clone()),
                Value::Text((*item_id).clone()),
            ]);
        }
        st(tx.execute(
            &format!(
                "INSERT INTO fireweed_item_index \
                 (tenant_id,queue_id,index_name,index_key,item_id) VALUES {values}"
            ),
            params_from_iter(parameters.iter()),
        ))?;
    }
    Ok(())
}

/// Delete all `fireweed_item_index` rows for the given item IDs.
pub(crate) fn delete_typed_index_rows(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    item_ids: &[String],
) -> EngineResult<()> {
    for chunk in item_ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "DELETE FROM fireweed_item_index \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.to_string()), Value::Text(q.to_string())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
    }
    Ok(())
}

/// Pack and check unique conflicts, then insert index rows for all `items` in a push batch.
/// `typed_indexes` must already be resolved from the queue definition.
pub(crate) fn maintain_typed_indexes_on_insert(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    items: &[&PushItem],
) -> EngineResult<()> {
    if typed_indexes.is_empty() {
        return Ok(());
    }
    // Durable SQL rows are paid per index. Unique indexes must be in SQLite for conflict
    // checks; non-unique keys are derived from native index_fields and are not written
    // on the apply hot path (snorri-shaped 19-index batches are otherwise 19× row inserts).
    let unique_names: std::collections::HashSet<&str> = typed_indexes
        .iter()
        .filter(|index| index_is_unique(index))
        .map(|index| index.name.as_str())
        .collect();
    // Collect (item_id, keys) and enforce within-batch uniqueness in a single pass.
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    let mut item_keys: TypedIndexRows = Vec::with_capacity(items.len());
    for item in items {
        let keys = typed_index_keys_for_push_item(typed_indexes, item)?;
        let id_str = item.item_id.to_string();
        // Within-batch: detect two items in the same push sharing a unique key.
        for (name, key) in &keys {
            if unique_names.contains(name.as_str()) {
                let bk = (name.clone(), key.clone());
                if let Some(prev) = batch_unique.get(&bk) {
                    if prev != &id_str {
                        return Err(EngineError::Conflict);
                    }
                } else {
                    batch_unique.insert(bk, id_str.clone());
                }
            }
        }
        let durable: Vec<_> = keys
            .into_iter()
            .filter(|(name, _)| unique_names.contains(name.as_str()))
            .collect();
        if !durable.is_empty() {
            item_keys.push((id_str, durable));
        }
    }
    check_typed_unique_conflicts_batch(tx, t, q, typed_indexes, &item_keys)?;
    insert_typed_index_rows_batch(tx, t, q, &item_keys)?;
    Ok(())
}

/// Pre-commit (ADR-010 §5.1; `commit` has no rollback) UNIQUE typed-index validation for a push batch on
/// the relational projection: reject with [`EngineError::Conflict`] when inserting `items` would collide on
/// a UNIQUE typed index — against existing durable rows AND against another item earlier in the same batch —
/// mirroring the apply-time enforcement in [`maintain_typed_indexes_on_insert`] (identical key derivation
/// and uniqueness rule). The composed commit path stages every committed entry's pushes into ONE candidate
/// batch and validates here BEFORE the durable log append, so a within-commit duplicate unique key is caught
/// at VALIDATION and the appended batch is always appliable (no recovery poison). Mutates nothing; non-unique
/// indexes and disjoint keys pass; schema-less queues (no typed indexes) short-circuit.
pub(crate) fn validate_typed_unique_push(
    conn: &Connection,
    shard: &QueueKey,
    typed_indexes: &[QueueIndex],
    items: &[PushItem],
) -> EngineResult<()> {
    if typed_indexes.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    // Read-only unchecked transaction (dropped/rolled back — no writes): a stable snapshot for the DB-level
    // unique lookups, exactly the queries `maintain_typed_indexes_on_insert` runs at apply time.
    let tx = st(conn.unchecked_transaction())?;
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    let mut item_keys: TypedIndexRows = Vec::with_capacity(items.len());
    for item in items {
        let keys = typed_index_keys_for_push_item(typed_indexes, item)?;
        let id_str = item.item_id.to_string();
        // (a) Within-batch: two items in the SAME candidate batch (possibly from different commit entries)
        // sharing a unique key collide — this is the cross-entry duplicate apply enforces only at insert time.
        for (name, key) in &keys {
            if typed_indexes
                .iter()
                .find(|qi| &qi.name == name)
                .map(index_is_unique)
                .unwrap_or(false)
            {
                let bk = (name.clone(), key.clone());
                if let Some(prev) = batch_unique.get(&bk) {
                    if prev != &id_str {
                        return Err(EngineError::Conflict);
                    }
                } else {
                    batch_unique.insert(bk, id_str.clone());
                }
            }
        }
        item_keys.push((id_str, keys));
    }
    check_typed_unique_conflicts_batch(&tx, &t, &q, typed_indexes, &item_keys)?;
    Ok(())
}

/// Pack a timestamp as nanoseconds-since-epoch (comparable in SQL for `not_before`/expiry ordering).
/// Saturating so a far-future timestamp (> ~year 2262) clamps rather than overflow-panics; realistic
/// queue timestamps are far inside the i64-nanos range.
pub(crate) fn is_fifo_claim_scan_item(item: &PushItem) -> bool {
    item.priority.is_none()
        && item.not_before.is_none()
        && item.group_key.is_none()
        && item.cohort_size.is_none()
        && item.gate_keys.is_empty()
}

pub(crate) fn reset_claim_scan_hint(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
) {
    claim_scan_hints.remove(shard);
    claim_scan_default_fifo.insert(shard.clone(), false);
}

pub(crate) fn observe_push_for_claim_scan(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    items: &[&PushItem],
) {
    if items.iter().copied().all(is_fifo_claim_scan_item) {
        claim_scan_default_fifo.entry(shard.clone()).or_insert(true);
    } else {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
    }
}

pub(crate) fn advance_claim_scan_hint_for_ids(
    tx: &Transaction<'_>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    if item_ids.is_empty() || !claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    let ids: Vec<String> = item_ids.iter().map(|id| id.to_string()).collect();
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT COUNT(*), MAX(rowid), \
             COALESCE(SUM(CASE WHEN priority IS NOT NULL OR not_before IS NOT NULL \
             OR group_key IS NOT NULL OR cohort_size IS NOT NULL THEN 1 ELSE 0 END), 0) \
             FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let (count, chunk_max_rowid, chunk_rich): (i64, Option<i64>, i64) =
            st(tx.query_row(&sql, params_from_iter(p.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            }))?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_max_rowid {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != item_ids.len() as i64 || rich_rows > 0 {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
        return Ok(());
    }
    if let Some(rowid) = max_rowid {
        let next = rowid
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
        let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
        if next > *slot {
            *slot = next;
        }
    }
    Ok(())
}

pub(crate) fn fifo_rowid_range_for_id_strings(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[String],
    expected_state: Option<&str>,
) -> EngineResult<Option<(i64, i64)>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut min_rowid: Option<i64> = None;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT COUNT(*), MIN(rowid), MAX(rowid), \
             COALESCE(SUM(CASE WHEN priority IS NOT NULL OR not_before IS NOT NULL \
             OR group_key IS NOT NULL OR cohort_size IS NOT NULL THEN 1 ELSE 0 END), 0) \
             FROM fireweed_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let (count, chunk_min, chunk_max, chunk_rich): (i64, Option<i64>, Option<i64>, i64) =
            st(conn.query_row(&sql, params_from_iter(p.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            }))?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_min {
            min_rowid = Some(min_rowid.map_or(rowid, |current| current.min(rowid)));
        }
        if let Some(rowid) = chunk_max {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != ids.len() as i64 || rich_rows > 0 {
        return Ok(None);
    }
    let (Some(min_rowid), Some(max_rowid)) = (min_rowid, max_rowid) else {
        return Ok(None);
    };
    let range_count: i64 = if let Some(state) = expected_state {
        st(conn.query_row(
            "SELECT COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4 AND lifecycle_state=?5",
            params![t, q, min_rowid, max_rowid, state],
            |row| row.get(0),
        ))?
    } else {
        st(conn.query_row(
            "SELECT COUNT(*) FROM fireweed_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4",
            params![t, q, min_rowid, max_rowid],
            |row| row.get(0),
        ))?
    };
    if range_count == ids.len() as i64 {
        Ok(Some((min_rowid, max_rowid)))
    } else {
        Ok(None)
    }
}
