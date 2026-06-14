use pqueue_core::{
    CohortOnIncomplete, CohortPolicy, EligibilityPolicy, GateKeyPolicy, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition,
    QueueId, RecurrenceMode, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use serde_json::{json, Value};

#[derive(Debug)]
pub struct ConvertError(pub String);

pub fn ordering_mode_str(mode: OrderingMode) -> &'static str {
    match mode {
        OrderingMode::Strict => "strict",
        OrderingMode::BoundedRelaxed => "bounded_relaxed",
    }
}

pub fn str_ordering_mode(s: &str) -> Result<OrderingMode, ConvertError> {
    match s {
        "strict" => Ok(OrderingMode::Strict),
        "bounded_relaxed" => Ok(OrderingMode::BoundedRelaxed),
        _ => Err(ConvertError(format!("unknown ordering_mode: {s}"))),
    }
}

pub fn priority_model_to_json(pm: &PriorityModel) -> Value {
    json!({
        "kind": match pm.kind {
            PriorityModelKind::Timestamp => "timestamp",
            PriorityModelKind::Int64 => "int64",
            PriorityModelKind::Decimal => "decimal",
            PriorityModelKind::Text => "text",
        },
        "direction": match pm.direction {
            PriorityDirection::Ascending => "ascending",
            PriorityDirection::Descending => "descending",
        },
        "tie_breaker": match pm.tie_breaker {
            PriorityTieBreaker::CreatedSequence => "created_sequence",
            PriorityTieBreaker::ClientItemKey => "client_item_key",
            PriorityTieBreaker::ItemId => "item_id",
        },
    })
}

pub fn json_priority_model(v: &Value) -> Result<PriorityModel, ConvertError> {
    let kind = match v["kind"].as_str().unwrap_or("") {
        "timestamp" => PriorityModelKind::Timestamp,
        "int64" => PriorityModelKind::Int64,
        "decimal" => PriorityModelKind::Decimal,
        "text" => PriorityModelKind::Text,
        s => return Err(ConvertError(format!("unknown priority kind: {s}"))),
    };
    let direction = match v["direction"].as_str().unwrap_or("") {
        "ascending" => PriorityDirection::Ascending,
        "descending" => PriorityDirection::Descending,
        s => return Err(ConvertError(format!("unknown direction: {s}"))),
    };
    let tie_breaker = match v["tie_breaker"].as_str().unwrap_or("") {
        "created_sequence" => PriorityTieBreaker::CreatedSequence,
        "client_item_key" => PriorityTieBreaker::ClientItemKey,
        "item_id" => PriorityTieBreaker::ItemId,
        s => return Err(ConvertError(format!("unknown tie_breaker: {s}"))),
    };
    Ok(PriorityModel { kind, direction, tie_breaker })
}

pub fn eligibility_policy_to_json(ep: &EligibilityPolicy) -> Value {
    json!({
        "gate_keys": match ep.gate_keys {
            GateKeyPolicy::None => "none",
            GateKeyPolicy::Dynamic => "dynamic",
        },
        "max_gate_keys_per_item": ep.max_gate_keys_per_item,
        "max_gates_per_request": ep.max_gates_per_request,
    })
}

pub fn json_eligibility_policy(v: &Value) -> Result<EligibilityPolicy, ConvertError> {
    let gate_keys = match v["gate_keys"].as_str().unwrap_or("none") {
        "none" => GateKeyPolicy::None,
        "dynamic" => GateKeyPolicy::Dynamic,
        s => return Err(ConvertError(format!("unknown gate_keys: {s}"))),
    };
    Ok(EligibilityPolicy {
        metadata_blockers: Default::default(),
        gate_keys,
        max_gate_keys_per_item: v["max_gate_keys_per_item"].as_u64(),
        max_gates_per_request: v["max_gates_per_request"].as_u64(),
    })
}

pub fn retry_policy_to_json(rp: &RetryPolicy) -> Value {
    json!({ "max_attempts": rp.max_attempts })
}

pub fn json_retry_policy(v: &Value) -> Result<RetryPolicy, ConvertError> {
    let max_attempts = v["max_attempts"]
        .as_u64()
        .ok_or_else(|| ConvertError("missing max_attempts".into()))? as u32;
    Ok(RetryPolicy { max_attempts })
}

pub fn cohort_policy_to_json(cp: Option<&CohortPolicy>) -> Value {
    match cp {
        None => json!({ "enabled": false }),
        Some(c) => json!({
            "enabled": c.enabled,
            "completion_bound_ms": c.completion_bound_ms,
            "on_incomplete": c.on_incomplete.map(|oi| match oi {
                CohortOnIncomplete::ExpireCohort => "expire_cohort",
            }),
            "max_cohort_size": c.max_cohort_size,
        }),
    }
}

pub fn json_cohort_policy(v: &Value) -> Result<Option<CohortPolicy>, ConvertError> {
    if !v["enabled"].as_bool().unwrap_or(false) {
        return Ok(None);
    }
    let on_incomplete = match v["on_incomplete"].as_str() {
        Some("expire_cohort") => Some(CohortOnIncomplete::ExpireCohort),
        None => None,
        Some(s) => return Err(ConvertError(format!("unknown on_incomplete: {s}"))),
    };
    Ok(Some(CohortPolicy {
        enabled: true,
        completion_bound_ms: v["completion_bound_ms"].as_u64(),
        on_incomplete,
        max_cohort_size: v["max_cohort_size"].as_u64(),
    }))
}

pub fn recurrence_to_json(r: &RecurrencePolicy) -> Value {
    let mode = match r.mode {
        RecurrenceMode::Oneshot => "oneshot",
        RecurrenceMode::Recurring => "recurring",
    };
    let until = r.until.map(|u| json!({ "seconds": u.seconds, "nanoseconds": u.nanoseconds }));
    json!({ "mode": mode, "until": until })
}

pub fn json_recurrence(v: &Value) -> Result<RecurrencePolicy, ConvertError> {
    let mode = match v["mode"].as_str().unwrap_or("oneshot") {
        "oneshot" => RecurrenceMode::Oneshot,
        "recurring" => RecurrenceMode::Recurring,
        s => return Err(ConvertError(format!("unknown recurrence mode: {s}"))),
    };
    let until = if v["until"].is_null() {
        None
    } else {
        let u = &v["until"];
        let secs = u["seconds"].as_i64().ok_or_else(|| ConvertError("missing until.seconds".into()))?;
        let ns = u["nanoseconds"].as_u64().ok_or_else(|| ConvertError("missing until.nanoseconds".into()))? as u32;
        Some(UtcTimestamp::new(secs, ns).map_err(|e| ConvertError(e.to_string()))?)
    };
    Ok(RecurrencePolicy { mode, until })
}

pub fn row_to_definition(row: &tokio_postgres::Row) -> Result<QueueDefinition, ConvertError> {
    let tenant_id_str: String = row.get("tenant_id");
    let queue_id_str: String = row.get("queue_id");
    let pm_json: Value = row.get("priority_model");
    let om_str: String = row.get("ordering_mode");
    let group_co_residency: bool = row.get("group_co_residency");
    let progress_bound_ms: i64 = row.get("progress_bound_ms");
    let ep_json: Value = row.get("eligibility_policy");
    let request_id_retention_ms: i64 = row.get("request_id_retention_ms");
    let client_item_key_retention_ms: i64 = row.get("client_item_key_retention_ms");
    let max_lease_duration_ms: i64 = row.get("max_lease_duration_ms");
    let rp_json: Value = row.get("retry_policy");
    let max_push_batch_size: i64 = row.get("max_push_batch_size");
    let max_claim_batch_size: i64 = row.get("max_claim_batch_size");
    let max_eligible_group_size: Option<i64> = row.get("max_eligible_group_size");
    let cp_json: Value = row.get("cohort_policy");
    let rec_json: Value = row.get("recurrence_policy");
    let shard_count: i32 = row.get("shard_count");

    Ok(QueueDefinition {
        tenant_id: TenantId::new(tenant_id_str).map_err(|e| ConvertError(e.message))?,
        queue_id: QueueId::new(queue_id_str).map_err(|e| ConvertError(e.message))?,
        priority_model: json_priority_model(&pm_json)?,
        ordering_mode: str_ordering_mode(&om_str)?,
        group_co_residency,
        progress_bound_ms: progress_bound_ms as u64,
        eligibility_policy: json_eligibility_policy(&ep_json)?,
        cohort_policy: json_cohort_policy(&cp_json)?,
        recurrence: json_recurrence(&rec_json)?,
        request_id_retention_ms: request_id_retention_ms as u64,
        client_item_key_retention_ms: client_item_key_retention_ms as u64,
        max_lease_duration_ms: max_lease_duration_ms as u64,
        retry_policy: json_retry_policy(&rp_json)?,
        max_push_batch_size: max_push_batch_size as u64,
        max_claim_batch_size: max_claim_batch_size as u64,
        max_eligible_group_size: max_eligible_group_size.map(|v| v as u64),
        shard_count: shard_count as u32,
    })
}
