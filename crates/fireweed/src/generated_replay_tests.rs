//! CONTRACT-007: generated independent predictions against actual S3 × Turso.
use super::*;
use fireweed_core::{OwnerId, WorkerId};
use fireweed_engine::{
    ControlPlaneConfig, InMemoryControlPlane, OwnedSession, OwnershipOutcome, QueueControlPlane,
    acquire_and_fence,
};
use futures::FutureExt;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};

const SCHEMA: &str = "fireweed-model-replay-v1";
const BUILTIN: &str = include_str!("../../../scripts/formal/fixtures/replay-smoke.json");
static NEXT: AtomicU64 = AtomicU64::new(1);

fn digest_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn number(v: &Json) -> Result<u64, String> {
    v.as_u64()
        .ok_or_else(|| format!("expected unsigned integer, got {v}"))
}
fn string(v: &Json) -> Result<&str, String> {
    v.as_str()
        .ok_or_else(|| format!("expected string, got {v}"))
}
fn array(v: &Json) -> Result<&Vec<Json>, String> {
    v.as_array()
        .ok_or_else(|| format!("expected array, got {v}"))
}
fn token(req: usize) -> String {
    format!("model-token-{req}")
}
fn request_id(req: usize) -> String {
    format!("model-request-{req}")
}
fn token_rank(value: &str) -> Result<usize, String> {
    (0..2)
        .find(|&req| token(req) == value)
        .ok_or_else(|| format!("unknown or changed lease token {value}"))
}
fn request_rank(value: &str) -> Result<usize, String> {
    (0..2)
        .find(|&req| request_id(req) == value)
        .ok_or_else(|| format!("unknown request identity {value}"))
}
fn item_rank(ids: &[ItemId], value: ItemId) -> Result<usize, String> {
    ids.iter()
        .position(|id| *id == value)
        .ok_or_else(|| format!("unknown item ID {value}"))
}

fn validate_corpus(corpus: &Json) -> Result<(), String> {
    if corpus["schema"] != SCHEMA || corpus["graph"]["complete"] != true {
        return Err("unknown schema or incomplete graph".into());
    }
    let expected_hash = digest_hex(include_bytes!("../../../scripts/formal/replay_model.py"));
    if corpus["model_sha256"] != expected_hash {
        return Err("stale independent model source".into());
    }
    let bounds = json!({"queues":1,"items":2,"requests":2,"owners":2,"issued_sessions":3,
        "handoffs":1,"graceful_reopens":1,"logical_now_seconds":2,"lease_expiry_seconds":30,
        "max_items_per_claim":1});
    if corpus["bounds"] != bounds {
        return Err("unsupported model bounds".into());
    }
    let fingerprint = Sha256::digest(string(&corpus["fingerprint_body"])?.as_bytes());
    if number(&corpus["fingerprint"])? != u64::from_be_bytes(fingerprint[..8].try_into().unwrap()) {
        return Err("independent fixture fingerprint mismatch".into());
    }
    let states = corpus["states"]
        .as_object()
        .ok_or("missing state definitions")?;
    let initial = number(&corpus["initial"])?;
    if !states.contains_key(&initial.to_string()) {
        return Err("unknown initial model state".into());
    }
    let traces = array(&corpus["traces"])?;
    if traces.is_empty() || number(&corpus["selection"]["traces"])? != traces.len() as u64 {
        return Err("empty corpus or case-count mismatch".into());
    }
    let mut names = HashSet::new();
    for trace in traces {
        if !names.insert(string(&trace["id"])?) || array(&trace["steps"])?.is_empty() {
            return Err("duplicate trace ID or empty trace".into());
        }
        for step in array(&trace["steps"])? {
            let action = string(&step["action"])?;
            let valid = matches!(
                action,
                "Handoff"
                    | "Reopen"
                    | "Reaffirm"
                    | "ConcurrentClaims"
                    | "ConcurrentMixedFence"
                    | "DrainApply"
            ) || ["Claim:", "Replay:", "LoseResponse:", "StaleClaim:"]
                .iter()
                .any(|prefix| {
                    action
                        .strip_prefix(prefix)
                        .is_some_and(|index| matches!(index, "0" | "1"))
                });
            if !valid || array(&step["transitions"])?.is_empty() {
                return Err("unknown action or empty transitions".into());
            }
            for edge in array(&step["transitions"])? {
                for key in ["before", "after"] {
                    if !states.contains_key(&number(&edge[key])?.to_string()) {
                        return Err("unknown referenced state".into());
                    }
                }
                if !edge["result"].is_object() {
                    return Err("missing predicted result".into());
                }
            }
        }
    }
    Ok(())
}

/// Carry every matching predecessor/successor; never independently choose a
/// legal observation from an incompatible concurrency branch at a later step.
fn match_step(
    corpus: &Json,
    step: &Json,
    current: &BTreeSet<u64>,
    observed: &Json,
    result: &Json,
) -> Result<(BTreeSet<u64>, Vec<String>), String> {
    let mut following = BTreeSet::new();
    let mut witnessed = Vec::new();
    for edge in array(&step["transitions"])? {
        let before = number(&edge["before"])?;
        let after = number(&edge["after"])?;
        if current.contains(&before)
            && corpus["states"][after.to_string()]["observation"] == *observed
            && edge["result"] == *result
        {
            following.insert(after);
            witnessed.push(format!("{before}|{}|{after}", string(&step["action"])?));
        }
    }
    if following.is_empty() {
        return Err(format!(
            "no permitted model path at {}: result={result}; observation={observed}",
            step["action"]
        ));
    }
    Ok((following, witnessed))
}

struct World {
    backend: Option<DerivedObjectLogTursoBackend>,
    cp: InMemoryControlPlane,
    owners: [OwnerId; 2],
    current_owner: usize,
    sessions: Vec<OwnedSession>,
    root: std::path::PathBuf,
    namespace: String,
    queue: QueueKey,
    ids: Vec<ItemId>,
}

impl Drop for World {
    fn drop(&mut self) {
        // Disposable fixture cleanup; no projection lifecycle/rebuild is tested.
        drop(self.backend.take());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl World {
    async fn open(
        root: &std::path::Path,
        namespace: &str,
    ) -> Result<DerivedObjectLogTursoBackend, String> {
        let s3 = fireweed_objectlog::shared_s3_test_env();
        let log = ObjectLogEngineStore::open_s3_with_prefixes(
            &s3.endpoint,
            &s3.region,
            &s3.bucket,
            &s3.access_key,
            &s3.secret_key,
            format!("{namespace}/log/"),
            format!("{namespace}/meta/"),
            flush_config_from_segment(256 * 1024, 5),
        )
        .await
        .map_err(|e| e.to_string())?;
        let path = root.join("projection.db");
        let projection = open_turso_projection_async(&path)
            .await
            .map_err(|e| e.to_string())?;
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            path,
            0,
            Some(AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()),
        )
        .await
        .map_err(|e| e.to_string())
    }
    async fn new(trace: &str) -> Result<Self, String> {
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let namespace = format!("formal-replay-{}-{nonce}-{trace}", std::process::id());
        let root = std::env::temp_dir().join(&namespace);
        std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
        let backend = Self::open(&root, &namespace).await?;
        let definition = super::formal_analysis_tests::definition();
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend
            .create_queue(definition)
            .await
            .map_err(|e| e.to_string())?;
        let items = (0..2)
            .map(|i| PushSpec {
                client_item_key: Some(ClientItemKey::new(format!("item-{i}")).unwrap()),
                priority: Some(PriorityValue::Int64(i)),
                ..Default::default()
            })
            .collect();
        let ids = backend
            .push(&queue, items, UtcTimestamp::new(1, 0).unwrap(), None)
            .await
            .map_err(|e| e.to_string())?;
        if ids.len() != 2 || ids[0] == ids[1] {
            return Err("non-injective bootstrap identity map".into());
        }
        backend
            .catch_up_projection(&queue)
            .await
            .map_err(|e| e.to_string())?;
        let cp = InMemoryControlPlane::new(ControlPlaneConfig::default());
        let owners = [
            OwnerId::new("model-owner-0").unwrap(),
            OwnerId::new("model-owner-1").unwrap(),
        ];
        for owner in &owners {
            cp.register_owner(owner, Self::now())
                .map_err(|e| e.to_string())?;
        }
        let session = match acquire_and_fence(&cp, &backend, &queue, &owners[0], Self::now())
            .await
            .map_err(|e| e.to_string())?
        {
            OwnershipOutcome::Owned(session) => session,
            _ => return Err("bootstrap owner rejected".into()),
        };
        backend.async_apply.as_ref().unwrap().pause();
        Ok(Self {
            backend: Some(backend),
            cp,
            owners,
            current_owner: 0,
            sessions: vec![session],
            root,
            namespace,
            queue,
            ids,
        })
    }
    fn now() -> UtcTimestamp {
        UtcTimestamp::new(2, 0).unwrap()
    }
    fn backend(&self) -> &DerivedObjectLogTursoBackend {
        self.backend.as_ref().unwrap()
    }
    fn request(&self, req: usize, epoch: u64, replay: bool) -> ClaimRequest {
        ClaimRequest {
            shard: self.queue.clone(),
            worker_id: WorkerId::new(format!("worker-{req}")).unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new(if replay {
                format!("retry-token-{req}")
            } else {
                token(req)
            })
            .unwrap(),
            lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
            now: Self::now(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(epoch),
            request_id: Some(RequestId::new(request_id(req)).unwrap()),
        }
    }
    fn normalize_claim(&self, req: usize, claimed: Claimed) -> Result<Json, String> {
        if claimed.items.len() != 1 {
            return Err("claim did not return exactly one known item".into());
        }
        let item = &claimed.items[0];
        if item.lease_expires_at.nanoseconds != 0 {
            return Err("changed fractional returned lease expiry".into());
        }
        Ok(
            json!({"status":"claimed", "request":req, "item":item_rank(&self.ids, item.item_id)?,
            "token": token_rank(item.lease_token.as_ref().ok_or("missing returned lease token")?.as_str())?,
            "expiry": item.lease_expires_at.seconds, "version":item.item_version, "attempts":item.attempt_count}),
        )
    }
    async fn execute(&mut self, action: &str, diagnostics: &mut Vec<Json>) -> Result<Json, String> {
        let epoch = self.sessions.last().unwrap().fence_epoch;
        if let Some(index) = action
            .strip_prefix("Claim:")
            .or_else(|| action.strip_prefix("Replay:"))
        {
            let req = index.parse::<usize>().map_err(|e| e.to_string())?;
            let claimed = self
                .backend()
                .claim(self.request(req, epoch, action.starts_with("Replay:")))
                .await
                .map_err(|e| e.to_string())?;
            return self.normalize_claim(req, claimed);
        }
        if let Some(index) = action.strip_prefix("LoseResponse:") {
            return Ok(
                json!({"status":"response_lost","request":index.parse::<usize>().map_err(|e|e.to_string())?}),
            );
        }
        if let Some(index) = action.strip_prefix("StaleClaim:") {
            let session = &self.sessions[index.parse::<usize>().map_err(|e| e.to_string())?];
            let mut probe = self.request(0, session.fence_epoch, false);
            probe.request_id = Some(RequestId::new(format!("stale-probe-{index}")).unwrap());
            match self.backend().claim(probe).await {
                Err(EngineError::EpochFenced) => return Ok(json!({"status":"epoch_fenced"})),
                other => return Err(format!("superseded credential was not rejected: {other:?}")),
            }
        }
        match action {
            "ConcurrentClaims" => {
                let (a, b) = tokio::join!(
                    self.backend().claim(self.request(0, epoch, false)),
                    self.backend().claim(self.request(1, epoch, false))
                );
                Ok(json!({"status":"concurrent_claims","responses":[
                    self.normalize_claim(0,a.map_err(|e|e.to_string())?)?,
                    self.normalize_claim(1,b.map_err(|e|e.to_string())?)?]}))
            }
            "ConcurrentMixedFence" => {
                let stale = self.sessions[0].fence_epoch;
                let (current, rejected) = tokio::join!(
                    self.backend().claim(self.request(0, epoch, false)),
                    self.backend().claim(self.request(1, stale, false))
                );
                if !matches!(rejected, Err(EngineError::EpochFenced)) {
                    return Err(format!(
                        "mixed-batch stale request was not fenced: {rejected:?}"
                    ));
                }
                Ok(json!({"status":"mixed_fence", "responses":[
                    self.normalize_claim(0,current.map_err(|e|e.to_string())?)?,
                    {"status":"epoch_fenced"}]}))
            }
            "DrainApply" => {
                self.backend().async_apply.as_ref().unwrap().resume();
                let result = self.backend().catch_up_projection(&self.queue).await;
                self.backend().async_apply.as_ref().unwrap().pause();
                result.map_err(|e| e.to_string())?;
                Ok(json!({"status":"applied"}))
            }
            "Handoff" => {
                let session = self.sessions.last().unwrap();
                self.cp
                    .release_queue_lease(
                        &self.queue,
                        &self.owners[self.current_owner],
                        session.lease_epoch,
                        Self::now(),
                    )
                    .map_err(|e| e.to_string())?;
                let outcome = acquire_and_fence(
                    &self.cp,
                    self.backend(),
                    &self.queue,
                    &self.owners[1],
                    Self::now(),
                )
                .await
                .map_err(|e| e.to_string())?;
                let OwnershipOutcome::Owned(session) = outcome else {
                    return Err("handoff rejected".into());
                };
                self.current_owner = 1;
                self.sessions.push(session);
                Ok(json!({"status":"handed_off"}))
            }
            "Reaffirm" => {
                let before = self.sessions.last().unwrap();
                let outcome = acquire_and_fence(
                    &self.cp,
                    self.backend(),
                    &self.queue,
                    &self.owners[self.current_owner],
                    Self::now(),
                )
                .await
                .map_err(|e| e.to_string())?;
                let OwnershipOutcome::Owned(after) = outcome else {
                    return Err("same owner reaffirm rejected".into());
                };
                if after.fence_epoch != before.fence_epoch
                    || after.lease_epoch != before.lease_epoch
                {
                    return Err("same session self-fenced".into());
                }
                Ok(json!({"status":"reaffirmed"}))
            }
            "Reopen" => {
                let pre = self
                    .backend()
                    .projection
                    .writer_recovery_high_water(&self.queue)
                    .await
                    .map_err(|e| e.to_string())?;
                self.backend()
                    .close_and_drain()
                    .await
                    .map_err(|e| e.to_string())?;
                let post_close = self
                    .backend()
                    .projection
                    .writer_recovery_high_water(&self.queue)
                    .await
                    .map_err(|e| e.to_string())?;
                if pre != post_close {
                    return Err("paused apply advanced during graceful close".into());
                }
                drop(self.backend.take());
                self.cp = InMemoryControlPlane::new(ControlPlaneConfig::default());
                for owner in &self.owners {
                    self.cp
                        .register_owner(owner, Self::now())
                        .map_err(|e| e.to_string())?;
                }
                let backend = Self::open(&self.root, &self.namespace).await?;
                let post_open = backend
                    .projection
                    .writer_recovery_high_water(&self.queue)
                    .await
                    .map_err(|e| e.to_string())?;
                let outcome = acquire_and_fence(
                    &self.cp,
                    &backend,
                    &self.queue,
                    &self.owners[self.current_owner],
                    Self::now(),
                )
                .await
                .map_err(|e| e.to_string())?;
                let OwnershipOutcome::Owned(session) = outcome else {
                    return Err("reopened acquire rejected".into());
                };
                self.sessions.push(session);
                backend.async_apply.as_ref().unwrap().pause();
                self.backend = Some(backend);
                diagnostics.push(json!({"operation":"Reopen", "pre_close":pre, "post_close":post_close,
                    "post_open":post_open,"tail_repaired":pre!=post_open,"same_namespace":true,"same_projection_file":true}));
                Ok(json!({"status":"reopened"}))
            }
            _ => Err(format!("unsupported action {action}")),
        }
    }
    async fn observe(
        &self,
        fingerprint: u64,
        expected_applied: usize,
        diagnostics: &mut Vec<Json>,
    ) -> Result<Json, String> {
        let backend = self.backend();
        let coordinator = backend.async_apply.as_ref().unwrap();
        let before = backend
            .projection
            .writer_recovery_high_water(&self.queue)
            .await
            .map_err(|e| e.to_string())?;
        if !coordinator.snapshot(&self.queue).await.paused {
            return Err("observation requires paused apply".into());
        }
        let mut entries = Vec::new();
        let mut from = None;
        let mut exhausted = false;
        for _ in 0..4 {
            let page = AsyncLogStore::read_from(
                backend.log.as_ref(),
                self.queue.clone(),
                from.clone(),
                16,
            )
            .await
            .map_err(|e| e.to_string())?;
            if page.entries.is_empty() {
                exhausted = true;
                break;
            }
            from = page
                .next
                .or_else(|| page.entries.last().map(|(position, _)| position.clone()));
            entries.extend(page.entries);
            if entries.len() > 3 {
                return Err("extra authoritative commands".into());
            }
        }
        if !exhausted {
            return Err("truncated authoritative history".into());
        }
        if before.as_ref() != entries.get(expected_applied).map(|(position, _)| position) {
            return Err(format!(
                "projection cursor does not cover exact modeled prefix: {before:?}, prefix {expected_applied}"
            ));
        }
        let mut history = Vec::new();
        let mut bootstrap = 0;
        let mut commands = HashSet::new();
        for (offset, (position, envelope)) in entries.iter().enumerate() {
            if position.sequence != offset as u64 {
                return Err("noncontiguous authoritative offsets".into());
            }
            if position.queue != self.queue || !commands.insert(position.sequence) {
                return Err("wrong shard or duplicate durable command position".into());
            }
            match &envelope.command {
                QueueCommand::Push(push) => {
                    bootstrap += 1;
                    if push.items.len() != 2
                        || envelope.request_id.is_some()
                        || push
                            .items
                            .iter()
                            .map(|item| item.item_id)
                            .collect::<Vec<_>>()
                            != self.ids
                    {
                        return Err("changed bootstrap durable history".into());
                    }
                }
                QueueCommand::Claim(claim) => {
                    if !claim.authority_first
                        || claim.item_ids.len() != 1
                        || envelope.item_ids != claim.item_ids
                        || envelope.created_at != Self::now()
                    {
                        return Err("changed durable claim shape/time".into());
                    }
                    let req = request_rank(
                        envelope
                            .request_id
                            .as_ref()
                            .ok_or("missing durable request ID")?
                            .as_str(),
                    )?;
                    let item = item_rank(&self.ids, claim.item_ids[0])?;
                    let token_id = token_rank(claim.lease_token.as_str())?;
                    let worker = claim
                        .worker_id
                        .as_ref()
                        .ok_or("missing durable worker")?
                        .as_str();
                    let Some(RequestOutcome::BatchClaim {
                        item_ids,
                        lease_token,
                        worker_id,
                    }) = &envelope.request_outcome
                    else {
                        return Err("missing/wrong durable claim disposition".into());
                    };
                    if item_ids != &claim.item_ids
                        || lease_token != &claim.lease_token
                        || worker_id != &claim.worker_id
                        || envelope.request_fingerprint != Some(fingerprint)
                        || claim.lease_expires_at.nanoseconds != 0
                    {
                        return Err("durable receipt/body/lease mismatch".into());
                    }
                    history.push(json!({"operation":"claim","request":req,"item":item,
                        "epoch":position.backend_epoch,"token":token_id,"worker":worker,
                        "expiry":claim.lease_expires_at.seconds,"fingerprint":envelope.request_fingerprint}));
                }
                other => return Err(format!("unexpected durable command {other:?}")),
            }
        }
        if bootstrap != 1 {
            return Err("missing or duplicate bootstrap push".into());
        }
        let params = || {
            vec![
                self.queue.tenant_id.as_str().to_owned().into(),
                self.queue.queue_id.as_str().to_owned().into(),
            ]
        };
        let item_rows = backend.projection.query(
            "SELECT i.item_id,i.lifecycle_state,b.lease_token,i.worker_id,i.lease_expires_at,i.item_version,i.retry_count,i.superseded \
             FROM fireweed_items i LEFT JOIN fireweed_lease_bearers b ON b.tenant_id=i.tenant_id AND b.queue_id=i.queue_id AND b.item_id=i.item_id \
             WHERE i.tenant_id=?1 AND i.queue_id=?2", params()).await.map_err(|e|e.to_string())?;
        if item_rows.len() != 2 {
            return Err("missing/extra projection rows".into());
        }
        let mut rows = vec![Json::Null; 2];
        for row in item_rows {
            let v = row.values;
            let id =
                ItemId::new(v[0].as_text().ok_or("bad SQL item ID")?).map_err(|e| e.to_string())?;
            let rank = item_rank(&self.ids, id)?;
            if !rows[rank].is_null() || v[7].as_integer() != Some(&0) {
                return Err("duplicate or superseded projection item".into());
            }
            let state = match v[1].as_text().map(String::as_str) {
                Some("Pending") => "pending",
                Some("Leased") => "leased",
                _ => return Err("unexpected lifecycle state".into()),
            };
            let token_id = if v[2].is_null() {
                None
            } else {
                Some(token_rank(v[2].as_text().ok_or("bad SQL bearer")?)?)
            };
            let worker = if v[3].is_null() {
                None
            } else {
                Some(v[3].as_text().ok_or("bad SQL worker")?.as_str())
            };
            let expiry = if v[4].is_null() {
                None
            } else {
                let ns = *v[4].as_integer().ok_or("bad SQL lease expiry")?;
                if ns % 1_000_000_000 != 0 {
                    return Err("changed fractional lease expiry".into());
                }
                Some(ns / 1_000_000_000)
            };
            rows[rank] = json!({"item":rank,"state":state,"token":token_id,"worker":worker,"expiry":expiry,
                "version":v[5].as_integer().ok_or("bad SQL version")?,"attempts":v[6].as_integer().ok_or("bad SQL attempts")?});
        }
        let receipt_rows = backend.projection.query(
            "SELECT operation,request_id,hex(request_fingerprint),response_payload,expires_at \
             FROM fireweed_request_idempotency WHERE tenant_id=?1 AND queue_id=?2 ORDER BY request_id", params())
             .await.map_err(|e|e.to_string())?;
        let mut receipts = Vec::new();
        for row in receipt_rows {
            let v = row.values;
            let req = request_rank(v[1].as_text().ok_or("bad SQL request identity")?)?;
            let payload: Json =
                serde_json::from_str(v[3].as_text().ok_or("bad SQL response payload")?)
                    .map_err(|e| e.to_string())?;
            let items = array(&payload["item_ids"])?;
            if items.len() != 1 {
                return Err("wrong receipt item multiplicity".into());
            }
            let item = item_rank(
                &self.ids,
                ItemId::new(string(&items[0])?).map_err(|e| e.to_string())?,
            )?;
            let fingerprint = u64::from_str_radix(v[2].as_text().ok_or("bad SQL fingerprint")?, 16)
                .map_err(|e| e.to_string())?;
            let retention = *v[4].as_integer().ok_or("bad receipt retention")?;
            if retention % 1_000_000_000 != 0 {
                return Err("changed fractional receipt expiry".into());
            }
            receipts.push(json!({"operation":v[0].as_text().ok_or("bad receipt operation")?,"request":req,"item":item,
                "epoch":Json::Null,"token":token_rank(string(&payload["lease_token"])?)?,
                "worker":string(&payload["worker_id"])?,"fingerprint":fingerprint,"retention_expiry":retention/1_000_000_000}));
        }
        let cp = self.cp.lease(&self.queue).map_err(|e| e.to_string())?;
        if cp.state != fireweed_engine::LeaseState::Assigned
            || cp
                .lease_expires_at
                .is_none_or(|expiry| expiry <= Self::now())
            || cp.active_owner_id.as_ref() != Some(&self.owners[self.current_owner])
        {
            return Err("wrong serving owner".into());
        }
        let epoch = backend
            .current_epoch(&self.queue)
            .await
            .map_err(|e| e.to_string())?;
        let session = self.sessions.last().unwrap();
        if session.fence_epoch != epoch || session.lease_epoch != cp.assignment_epoch {
            return Err("session credential mapping diverged".into());
        }
        let after = backend
            .projection
            .writer_recovery_high_water(&self.queue)
            .await
            .map_err(|e| e.to_string())?;
        if before != after {
            return Err("observational read advanced deferred apply".into());
        }
        diagnostics.push(
            json!({"operation":"Observe","projection_before":before,"projection_after":after,
            "durable_positions":entries.iter().map(|(position,_)|position).collect::<Vec<_>>(),
            "apply_lag":coordinator.snapshot(&self.queue).await.apply_lag_commands}),
        );
        Ok(
            json!({"owner":self.current_owner,"assignment_epoch":cp.assignment_epoch,"storage_epoch":epoch,
            "history":history,"rows":rows,"applied_receipts":receipts}),
        )
    }
}

async fn run_trace(
    corpus: &Json,
    trace: &Json,
    observations: &mut Vec<Json>,
) -> Result<BTreeSet<String>, String> {
    let mut world = World::new(string(&trace["id"])?).await?;
    let mut diagnostics = Vec::new();
    let fingerprint = number(&corpus["fingerprint"])?;
    let initial = number(&corpus["initial"])?;
    let observed = world.observe(fingerprint, 0, &mut diagnostics).await?;
    if observed != corpus["states"][initial.to_string()]["observation"] {
        return Err(format!("bootstrap observation mismatch: {observed}"));
    }
    let mut current = BTreeSet::from([initial]);
    let mut edges = BTreeSet::new();
    for (index, step) in array(&trace["steps"])?.iter().enumerate() {
        diagnostics.clear();
        observations.push(json!({"step":index,"action":step["action"],
            "compatible_before":current,"status":"attempted"}));
        let result = match world
            .execute(string(&step["action"])?, &mut diagnostics)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                observations.last_mut().unwrap()["error"] = json!(error);
                observations.last_mut().unwrap()["diagnostics"] = json!(diagnostics);
                return Err(error);
            }
        };
        observations.last_mut().unwrap()["result"] = result.clone();
        let prefixes: BTreeSet<_> = array(&step["transitions"])? .iter()
            .filter(|edge|number(&edge["before"]).is_ok_and(|before|current.contains(&before)))
            .map(|edge| number(&corpus["states"][number(&edge["after"]).unwrap().to_string()]["model"]["applied"]))
            .collect::<Result<_,_>>()?;
        if prefixes.len() != 1 {
            return Err("ambiguous modeled apply frontier".into());
        }
        let observed = match world
            .observe(
                fingerprint,
                *prefixes.first().unwrap() as usize,
                &mut diagnostics,
            )
            .await
        {
            Ok(observed) => observed,
            Err(error) => {
                observations.last_mut().unwrap()["error"] = json!(error);
                observations.last_mut().unwrap()["diagnostics"] = json!(diagnostics);
                return Err(error);
            }
        };
        observations.last_mut().unwrap()["observation"] = observed.clone();
        observations.last_mut().unwrap()["diagnostics"] = json!(diagnostics);
        let (following, matched) = match_step(corpus, step, &current, &observed, &result)?;
        observations.last_mut().unwrap()["status"] = json!("PASS");
        observations.last_mut().unwrap()["compatible_after"] = json!(following);
        observations.last_mut().unwrap()["matched_edges"] = json!(matched);
        current = following;
        edges.extend(matched);
    }
    world
        .backend()
        .close_and_drain()
        .await
        .map_err(|e| e.to_string())?;
    drop(world.backend.take());
    Ok(edges)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_s3_turso_model_traces_match_every_observation() {
    let input = match std::env::var("FIREWEED_REPLAY_CORPUS") {
        Ok(path) => std::fs::read_to_string(path).expect("read explicit corpus"),
        Err(_) => BUILTIN.to_owned(),
    };
    let corpus: Json = serde_json::from_str(&input).expect("parse generated corpus");
    validate_corpus(&corpus).expect("validate corpus envelope");
    let mut report = json!({"schema":"fireweed-model-replay-evidence-v1","status":"PASS",
        "corpus_sha256":digest_hex(input.as_bytes()),"model_sha256":corpus["model_sha256"],
        "model_graph":corpus["graph"],"selection":corpus["selection"],"attempted":0,"passed":0,"cases":[],
        "attribution":"real local-S3/Turso; one-process ephemeral CP; graceful reopen; simulated client nonreceipt"});
    let mut matched = BTreeSet::new();
    for trace in array(&corpus["traces"]).unwrap() {
        let mut observations = Vec::new();
        let result = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(20),
            run_trace(&corpus, trace, &mut observations),
        ))
        .catch_unwind()
        .await;
        report["attempted"] = json!(number(&report["attempted"]).unwrap() + 1);
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("trace execution timed out".into()),
            Err(panic) => Err(panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or("trace panicked".into())),
        };
        let error = match result {
            Ok(edges) => {
                matched.extend(edges);
                None
            }
            Err(error) => Some(error),
        };
        report["cases"].as_array_mut().unwrap().push(
            json!({"trace":trace["id"],"status":if error.is_some(){"FAIL"}else{"PASS"},
            "intended_edges":trace["intended_edges"],"steps":observations,"error":error}),
        );
        if error.is_some() {
            report["status"] = json!("FAIL");
            break;
        }
        report["passed"] = json!(number(&report["passed"]).unwrap() + 1);
    }
    report["matched_edges"] = json!(matched);
    report["sut_graph_edge_exhaustive"] =
        json!(matched.len() == number(&corpus["graph"]["edges"]).unwrap() as usize);
    report["all_selected_traces_executed"] =
        json!(number(&report["passed"]).unwrap() == array(&corpus["traces"]).unwrap().len() as u64);
    if let Ok(path) = std::env::var("FIREWEED_REPLAY_EVIDENCE") {
        std::fs::write(path, serde_json::to_string_pretty(&report).unwrap() + "\n")
            .expect("write replay evidence");
    }
    assert_eq!(
        report["status"],
        "PASS",
        "{}",
        serde_json::to_string_pretty(&report).unwrap()
    );
    assert_eq!(report["all_selected_traces_executed"], true);
}

#[test]
fn generated_replay_decoder_rejects_malformed_empty_and_stale_corpora() {
    let corpus: Json = serde_json::from_str(BUILTIN).unwrap();
    validate_corpus(&corpus).unwrap();
    for field in ["schema", "model_sha256", "bounds"] {
        let mut corrupt = corpus.clone();
        corrupt[field] = Json::Null;
        assert!(validate_corpus(&corrupt).is_err(), "insensitive {field}");
    }
    let mut corrupt = corpus.clone();
    corrupt["traces"] = json!([]);
    assert!(validate_corpus(&corrupt).is_err());
    let mut corrupt = corpus.clone();
    corrupt["traces"][0]["steps"][0]["action"] = json!("Unknown");
    assert!(validate_corpus(&corrupt).is_err());
}

#[test]
fn generated_replay_oracle_rejects_corrupted_observations_and_branch_switching() {
    let corpus: Json = serde_json::from_str(BUILTIN).unwrap();
    let steps = array(&corpus["traces"][0]["steps"]).unwrap();
    let step = &steps[0];
    let edge = &step["transitions"][0];
    let current = BTreeSet::from([number(&edge["before"]).unwrap()]);
    let expected = &corpus["states"][number(&edge["after"]).unwrap().to_string()]["observation"];
    match_step(&corpus, step, &current, expected, &edge["result"]).unwrap();
    for field in [
        "history",
        "rows",
        "applied_receipts",
        "storage_epoch",
        "owner",
    ] {
        let mut corrupt = expected.clone();
        corrupt[field] = Json::Null;
        assert!(
            match_step(&corpus, step, &current, &corrupt, &edge["result"]).is_err(),
            "insensitive {field}"
        );
    }
    let concurrent = corpus["traces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|trace| trace["steps"][0]["action"] == "ConcurrentClaims")
        .unwrap();
    let first = &concurrent["steps"][0];
    let a = &first["transitions"][0];
    let b = &first["transitions"][1];
    let current = BTreeSet::from([number(&a["before"]).unwrap()]);
    let (only_a, _) = match_step(
        &corpus,
        first,
        &current,
        &corpus["states"][number(&a["after"]).unwrap().to_string()]["observation"],
        &a["result"],
    )
    .unwrap();
    // A legal next step from B cannot follow the already observed branch A.
    let continuation = corpus["traces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|trace| {
            trace["steps"][0]["action"] == "ConcurrentClaims"
                && trace["steps"][1]["action"] == "LoseResponse:0"
        })
        .unwrap();
    let next = &continuation["steps"][1];
    let alternate = array(&next["transitions"])
        .unwrap()
        .iter()
        .find(|edge| edge["before"] == b["after"])
        .unwrap();
    let alternate_observation =
        &corpus["states"][number(&alternate["after"]).unwrap().to_string()]["observation"];
    match_step(
        &corpus,
        next,
        &BTreeSet::from([number(&b["after"]).unwrap()]),
        alternate_observation,
        &alternate["result"],
    )
    .unwrap();
    assert!(
        match_step(
            &corpus,
            next,
            &only_a,
            alternate_observation,
            &alternate["result"]
        )
        .is_err()
    );
    for (field, value) in [
        ("token", json!(1)),
        ("expiry", json!(31)),
        ("worker", json!("changed")),
        ("request", json!(1)),
        ("fingerprint", json!(0)),
    ] {
        let mut changed = expected.clone();
        changed["history"][0][field] = value;
        assert!(match_step(&corpus, step, &current, &changed, &edge["result"]).is_err());
    }
    let mut missing = expected.clone();
    missing["rows"].as_array_mut().unwrap().pop();
    assert!(match_step(&corpus, step, &current, &missing, &edge["result"]).is_err());
    let mut extra = expected.clone();
    extra["rows"]
        .as_array_mut()
        .unwrap()
        .push(json!({"item":2,"state":"pending"}));
    assert!(match_step(&corpus, step, &current, &extra, &edge["result"]).is_err());
    let applied_two = corpus["traces"].as_array().unwrap().iter()
        .flat_map(|trace|trace["steps"].as_array().unwrap())
        .find(|candidate|candidate["action"]=="DrainApply" && candidate["transitions"].as_array().unwrap().iter()
            .any(|edge|corpus["states"][number(&edge["after"]).unwrap().to_string()]["model"]["applied"]==2)).unwrap();
    let two_edge = &applied_two["transitions"][0];
    let two_before = BTreeSet::from([number(&two_edge["before"]).unwrap()]);
    let mut duplicate_lease =
        corpus["states"][number(&two_edge["after"]).unwrap().to_string()]["observation"].clone();
    assert_eq!(duplicate_lease["rows"][0]["state"], "leased");
    assert_eq!(duplicate_lease["rows"][1]["state"], "leased");
    duplicate_lease["rows"][1]["token"] = duplicate_lease["rows"][0]["token"].clone();
    assert!(
        match_step(
            &corpus,
            applied_two,
            &two_before,
            &duplicate_lease,
            &two_edge["result"]
        )
        .is_err()
    );
    let replay = corpus["traces"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|trace| trace["steps"].as_array().unwrap())
        .find(|step| step["action"] == "Replay:0")
        .unwrap();
    let replay_edge = &replay["transitions"][0];
    let predecessor = BTreeSet::from([number(&replay_edge["before"]).unwrap()]);
    let replay_observed =
        &corpus["states"][number(&replay_edge["after"]).unwrap().to_string()]["observation"];
    match_step(
        &corpus,
        replay,
        &predecessor,
        replay_observed,
        &replay_edge["result"],
    )
    .unwrap();
    let mut wrong_token = replay_edge["result"].clone();
    wrong_token["token"] = json!(1);
    assert!(match_step(&corpus, replay, &predecessor, replay_observed, &wrong_token).is_err());
    // Two hidden states with the same observable result must both survive.
    let ambiguous =
        json!({"states":{"1":{"observation":{"same":true}},"2":{"observation":{"same":true}}}});
    let ambiguous_step = json!({"action":"Observe","transitions":[
        {"before":0,"after":1,"result":{"ok":true}},
        {"before":0,"after":2,"result":{"ok":true}}]});
    let (both, _) = match_step(
        &ambiguous,
        &ambiguous_step,
        &BTreeSet::from([0]),
        &json!({"same":true}),
        &json!({"ok":true}),
    )
    .unwrap();
    assert_eq!(both, BTreeSet::from([1, 2]));
    let mut duplicate = expected.clone();
    let record = duplicate["history"][0].clone();
    duplicate["history"].as_array_mut().unwrap().push(record);
    assert!(match_step(&corpus, step, &current, &duplicate, &edge["result"]).is_err());
}

#[test]
fn generated_replay_claim_tail_guard_rejects_unproven_suffixes() {
    let qdef = super::formal_analysis_tests::definition();
    let queue = QueueKey::new(qdef.tenant_id, qdef.queue_id);
    let applied = CommandPosition::new(queue.clone(), 1, 0);
    let target = CommandPosition::new(queue.clone(), 2, 2);
    let entry = |sequence, epoch, name: &str| {
        let id = ItemId::new(name).unwrap();
        (
            CommandPosition::new(queue.clone(), epoch, sequence),
            CommandEnvelope {
                command_id: fireweed_engine::CommandId::new(format!("test-{sequence}")),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: vec![id],
                checksum: CommandChecksum(0),
                created_at: World::now(),
                command: QueueCommand::Claim(fireweed_engine::ClaimCommand {
                    item_ids: vec![id],
                    lease_token: LeaseToken::new("tail-token").unwrap(),
                    lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
                    worker_id: None,
                    authority_first: true,
                }),
            },
        )
    };
    let valid = vec![entry(1, 1, "101"), entry(2, 2, "102")];
    assert_eq!(
        claim_poll_tail_exclusions(&applied, &target, valid.clone(), World::now())
            .unwrap()
            .len(),
        2
    );
    let mut variants = vec![
        vec![],
        vec![valid[0].clone()],
        vec![entry(2, 1, "101"), valid[1].clone()],
        vec![entry(1, 2, "101"), entry(2, 1, "102")],
        vec![entry(1, 1, "103"), entry(2, 2, "103")],
    ];
    let mut expired = valid.clone();
    if let QueueCommand::Claim(claim) = &mut expired[0].1.command {
        claim.lease_expires_at = World::now();
    }
    variants.push(expired);
    let mut ordinary = valid.clone();
    if let QueueCommand::Claim(claim) = &mut ordinary[0].1.command {
        claim.authority_first = false;
    }
    variants.push(ordinary);
    let mut other_queue = valid.clone();
    other_queue[0].0.queue.queue_id = QueueId::new("wrong").unwrap();
    variants.push(other_queue);
    let mut mixed = valid.clone();
    mixed[0].1.command = QueueCommand::Push(fireweed_engine::PushCommand { items: vec![] });
    variants.push(mixed);
    for entries in variants {
        assert!(claim_poll_tail_exclusions(&applied, &target, entries, World::now()).is_none());
    }
    let far = CommandPosition::new(queue, 2, 17);
    assert!(claim_poll_tail_exclusions(&applied, &far, valid, World::now()).is_none());
}
