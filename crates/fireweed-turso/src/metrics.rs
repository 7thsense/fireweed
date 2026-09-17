//! Exact lifecycle counters on the existing queue metadata row. These are
//! rebuildable projection state, updated atomically with item changes.
use std::collections::{BTreeMap, BTreeSet};

use fireweed_core::ItemId;
use fireweed_engine::{
    CommandEnvelope, CommandPosition, EngineError, EngineResult, QueueCommand, QueueKey,
};
use fireweed_relational::{RelTx, RelValue, SQLITE_BIND_CAP};

const AGGREGATES: &str = "COALESCE(SUM(i.lifecycle_state='Pending'),0),COALESCE(SUM(i.lifecycle_state='Leased'),0),COALESCE(SUM(i.lifecycle_state='Complete'),0),COALESCE(SUM(i.lifecycle_state='Failed'),0)";
pub(crate) const READ_SQL: &str = "SELECT resident_pending,resident_leased,resident_complete,resident_failed FROM queues WHERE tenant=?1 AND queue=?2";

type Counts = [i64; 4];
// None means the command can affect rows beyond explicitly addressed IDs.
type Scope = Option<BTreeSet<ItemId>>;

pub(crate) struct MetricsDelta(Vec<(QueueKey, Scope, Counts, Option<Counts>)>);

/// An optimization of the SQL transaction's counters, never a log-tail read.
/// Every position must be new and contiguous from the writer's cursor snapshot.
enum FinalEffect {
    UnresolvedClaim,
    Resolved(Option<usize>),
}

enum AfterPlan {
    FreshPush(Counts),
    FinalRows {
        rows: BTreeMap<ItemId, Option<usize>>,
        initially_pending: bool,
    },
}

fn fresh_after_plan(
    queue: &QueueKey,
    positions: &[CommandPosition],
    commands: &[CommandEnvelope],
    cursor_seeds: &std::collections::HashMap<QueueKey, i64>,
) -> Option<AfterPlan> {
    let mut next = *cursor_seeds.get(queue)?;
    let mut effects: BTreeMap<ItemId, FinalEffect> = BTreeMap::new();
    let mut pushes = false;
    let mut other_rows = false;
    let mut initially_pending = true;
    for (position, envelope) in positions.iter().zip(commands) {
        if position.queue != *queue {
            continue;
        }
        if i64::try_from(position.sequence).ok()? != next {
            return None;
        }
        next = next.checked_add(1)?;
        match &envelope.command {
            QueueCommand::Push(command) => {
                if other_rows {
                    return None;
                }
                pushes = true;
                for item in &command.items {
                    // Repeated IDs would normally fail INSERT; keep these on
                    // the existing measured-row path instead of relying on it.
                    if effects
                        .insert(item.item_id, FinalEffect::Resolved(Some(0)))
                        .is_some()
                    {
                        return None;
                    }
                }
            }
            QueueCommand::Claim(command) => {
                if pushes {
                    return None;
                }
                other_rows = true;
                for id in &command.item_ids {
                    // Authority-first SQL requires every named row to move from
                    // active Pending or the transaction fails. Conditional claims
                    // still need a later replacement/purge or measured after-state.
                    let effect = if command.authority_first {
                        FinalEffect::Resolved(Some(1))
                    } else {
                        FinalEffect::UnresolvedClaim
                    };
                    if effects.insert(*id, effect).is_none() && !command.authority_first {
                        initially_pending = false;
                    }
                }
            }
            QueueCommand::MutateItems(command) => {
                if pushes {
                    return None;
                }
                other_rows = true;
                for item in &command.items {
                    let state = match &item.action {
                        fireweed_engine::ResolvedItemMutationAction::Purge => None,
                        fireweed_engine::ResolvedItemMutationAction::Replace(values)
                        | fireweed_engine::ResolvedItemMutationAction::ReplaceKeepingPayload(
                            values,
                        ) => Some(match values.state {
                            fireweed_core::ItemState::Pending => 0,
                            fireweed_core::ItemState::Leased => 1,
                            fireweed_core::ItemState::Complete => 2,
                            fireweed_core::ItemState::Failed => 3,
                        }),
                    };
                    if effects
                        .insert(item.item_id, FinalEffect::Resolved(state))
                        .is_none()
                    {
                        initially_pending = false;
                    }
                }
            }
            QueueCommand::PurgeItems(command) => {
                if pushes {
                    return None;
                }
                other_rows = true;
                for id in &command.item_ids {
                    if effects.insert(*id, FinalEffect::Resolved(None)).is_none() {
                        initially_pending = false;
                    }
                }
            }
            // Keep every other command family on the established SQL path.
            _ => return None,
        }
    }
    if pushes {
        return Some(AfterPlan::FreshPush([
            i64::try_from(effects.len()).ok()?,
            0,
            0,
            0,
        ]));
    }
    let final_rows: Option<BTreeMap<_, _>> = effects
        .into_iter()
        .map(|(id, effect)| match effect {
            FinalEffect::UnresolvedClaim => None,
            FinalEffect::Resolved(state) => Some((id, state)),
        })
        .collect();
    Some(AfterPlan::FinalRows {
        rows: final_rows?,
        initially_pending,
    })
}

impl AfterPlan {
    fn known_before(&self, scope: &Scope) -> Option<Counts> {
        match self {
            Self::FreshPush(_) => Some([0; 4]),
            Self::FinalRows {
                rows,
                initially_pending: true,
            } => {
                let ids = scope.as_ref()?;
                if rows.len() != ids.len() || !rows.keys().eq(ids.iter()) {
                    return None;
                }
                // This inference is used only after successful guarded SQL apply;
                // replay/noncontiguous positions cannot construct a fresh plan.
                Some([i64::try_from(ids.len()).ok()?, 0, 0, 0])
            }
            Self::FinalRows { .. } => None,
        }
    }

    fn after(&self, before: Counts, scope: &Scope) -> Option<Counts> {
        match self {
            Self::FreshPush(after) => Some(*after),
            Self::FinalRows { rows, .. } => {
                let ids = scope.as_ref()?;
                if rows.len() != ids.len() || !rows.keys().eq(ids.iter()) {
                    return None;
                }
                // Purge removes all explicitly addressed rows, including
                // missing/superseded ones. Replacements preserve supersession,
                // so only infer their contribution when every row was active.
                let all_purged = rows.values().all(Option::is_none);
                let active = before.into_iter().try_fold(0i64, i64::checked_add);
                if !all_purged && active != i64::try_from(ids.len()).ok() {
                    return None;
                }
                let mut after = [0i64; 4];
                for state in rows.values().flatten() {
                    after[*state] = after[*state].checked_add(1)?;
                }
                Some(after)
            }
        }
    }
}

impl MetricsDelta {
    pub(crate) fn capture(
        tx: &impl RelTx,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
        cursor_seeds: &std::collections::HashMap<QueueKey, i64>,
    ) -> EngineResult<Self> {
        let mut scopes: BTreeMap<QueueKey, Scope> = BTreeMap::new();
        for (position, envelope) in positions.iter().zip(commands) {
            let ids: Scope = match &envelope.command {
                QueueCommand::Push(c) => Some(c.items.iter().map(|i| i.item_id).collect()),
                QueueCommand::Claim(c) => Some(c.item_ids.iter().copied().collect()),
                QueueCommand::MutateItems(c) => Some(c.items.iter().map(|i| i.item_id).collect()),
                QueueCommand::Finalize(c) => Some(c.outcomes.iter().map(|i| i.item_id).collect()),
                QueueCommand::LeaseExpired(c) => Some(c.item_ids.iter().copied().collect()),
                QueueCommand::PurgeItems(c) => Some(c.item_ids.iter().copied().collect()),
                // These may affect cohort membership or supersede an existing
                // row by key. Count the whole queue until bounded scopes exist.
                QueueCommand::CreateQueue(_)
                | QueueCommand::CohortClaim(_)
                | QueueCommand::CohortFinalize(_)
                | QueueCommand::CohortExpired(_)
                | QueueCommand::ReplacePending(_) => None,
                // Exhaustive: new command kinds require an explicit decision.
                QueueCommand::RenewLease(_)
                | QueueCommand::CohortRenewLease(_)
                | QueueCommand::ReassignLease(_)
                | QueueCommand::UpdateFields(_)
                | QueueCommand::UpdateFieldsBatch(_)
                | QueueCommand::FenceLease(_)
                | QueueCommand::UnfenceLease(_)
                | QueueCommand::PauseQueue(_)
                | QueueCommand::ResumeQueue
                | QueueCommand::SetGates(_)
                | QueueCommand::WriteSideRecords(_)
                | QueueCommand::AdvanceInstanceFence(_) => continue,
            };
            let scope = scopes
                .entry(position.queue.clone())
                .or_insert_with(|| Some(BTreeSet::new()));
            match (scope.as_mut(), ids) {
                (Some(existing), Some(ids)) => existing.extend(ids),
                (_, None) => *scope = None,
                (None, Some(_)) => {}
            }
        }
        let mut captured = Vec::with_capacity(scopes.len());
        for (queue, scope) in scopes {
            let plan = fresh_after_plan(&queue, positions, commands, cursor_seeds);
            let before = match plan.as_ref().and_then(|plan| plan.known_before(&scope)) {
                Some(before) => before,
                None => counts(tx, &queue, &scope)?,
            };
            let known_after = plan.and_then(|plan| plan.after(before, &scope));
            captured.push((queue, scope, before, known_after));
        }
        Ok(Self(captured))
    }

    pub(crate) fn apply(self, tx: &impl RelTx) -> EngineResult<()> {
        for (queue, scope, before, known_after) in self.0 {
            let after = match known_after {
                Some(after) => after,
                None => counts(tx, &queue, &scope)?,
            };
            let delta = std::array::from_fn::<_, 4, _>(|i| after[i] - before[i]);
            if delta == [0; 4] {
                continue;
            }
            let changed = tx.execute("UPDATE queues SET resident_pending=resident_pending+?3,resident_leased=resident_leased+?4,resident_complete=resident_complete+?5,resident_failed=resident_failed+?6,resident_counts_version=1 WHERE tenant=?1 AND queue=?2", &[
                queue.tenant_id.as_str().into(), queue.queue_id.as_str().into(), delta[0].into(), delta[1].into(), delta[2].into(), delta[3].into(),
            ])?;
            if changed != 1 {
                return Err(EngineError::Storage(
                    "lifecycle delta has no queue metadata row".into(),
                ));
            }
        }
        Ok(())
    }
}

fn counts(tx: &impl RelTx, queue: &QueueKey, scope: &Scope) -> EngineResult<Counts> {
    let base = vec![
        RelValue::from(queue.tenant_id.as_str()),
        RelValue::from(queue.queue_id.as_str()),
    ];
    let read = |sql: &str, params: &[RelValue]| -> EngineResult<Counts> {
        let rows = tx.query(sql, params)?;
        let row = rows
            .first()
            .ok_or_else(|| EngineError::Storage("missing lifecycle aggregate".into()))?;
        Ok([row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?])
    };
    let Some(ids) = scope else {
        return read(
            &format!(
                "SELECT {AGGREGATES} FROM fireweed_items i WHERE i.tenant_id=?1 AND i.queue_id=?2 AND i.superseded=0"
            ),
            &base,
        );
    };
    let ids: Vec<_> = ids.iter().collect();
    let mut total = [0; 4];
    for chunk in ids.chunks(SQLITE_BIND_CAP - 2) {
        let mut params = base.clone();
        params.extend(chunk.iter().map(|id| RelValue::from(id.to_string())));
        // CROSS JOIN fixes target-first access: never scan the resident queue
        // to count a bounded mutation batch.
        let sql = addressed_counts_sql(chunk.len());
        let next = read(&sql, &params)?;
        for i in 0..4 {
            total[i] += next[i];
        }
    }
    Ok(total)
}

fn addressed_counts_sql(count: usize) -> String {
    let values = (3..count + 3)
        .map(|i| format!("(?{i})"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH targets(item_id) AS (VALUES {values}) SELECT {AGGREGATES} FROM targets t CROSS JOIN fireweed_items i INDEXED BY sqlite_autoindex_fireweed_items_1 ON i.tenant_id=?1 AND i.queue_id=?2 AND i.item_id=t.item_id WHERE i.superseded=0"
    )
}

pub(crate) async fn migrate(connection: &turso::Connection) -> crate::Result<()> {
    for column in [
        "resident_pending",
        "resident_leased",
        "resident_complete",
        "resident_failed",
    ] {
        let sql = format!(
            "ALTER TABLE queues ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0 CHECK(typeof({column})='integer' AND {column}>=0)"
        );
        if let Err(error) = connection.execute(&sql, ()).await
            && !error
                .to_string()
                .to_ascii_lowercase()
                .contains("duplicate column")
        {
            return Err(error.into());
        }
    }
    if let Err(error) = connection
        .execute(
            "ALTER TABLE queues ADD COLUMN resident_counts_version INTEGER NOT NULL DEFAULT 0",
            (),
        )
        .await
        && !error
            .to_string()
            .to_ascii_lowercase()
            .contains("duplicate column")
    {
        return Err(error.into());
    }
    // One atomic backfill; a failed/interrupted migration is retried on open.
    connection.execute("UPDATE queues SET (resident_pending,resident_leased,resident_complete,resident_failed)=(SELECT COALESCE(SUM(lifecycle_state='Pending'),0),COALESCE(SUM(lifecycle_state='Leased'),0),COALESCE(SUM(lifecycle_state='Complete'),0),COALESCE(SUM(lifecycle_state='Failed'),0) FROM fireweed_items i WHERE i.tenant_id=queues.tenant AND i.queue_id=queues.queue AND i.superseded=0),resident_counts_version=1 WHERE resident_counts_version=0", ()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TursoRelational, tx::TursoRel};
    use fireweed_core::{QueueId, TenantId};
    use turso::transaction::TransactionBehavior;

    #[tokio::test]
    async fn bounded_counts_seek_the_full_item_key() {
        let store = TursoRelational::in_memory().await.unwrap();
        let rows = store
            .query(
                format!("EXPLAIN QUERY PLAN {}", addressed_counts_sql(2)),
                vec!["t".into(), "q".into(), "1".into(), "2".into()],
            )
            .await
            .unwrap();
        let details = rows
            .iter()
            .map(|r| match &r.values[3] {
                turso::Value::Text(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(
            details.iter().any(|d| d
                .starts_with("SEARCH i USING INDEX sqlite_autoindex_fireweed_items_1")
                && d.contains("tenant_id=? AND queue_id=? AND item_id=?")),
            "bounded counts must seek all three key parts: {details:?}"
        );
    }

    #[tokio::test]
    async fn deltas_follow_actual_rows_and_rollback_atomically() {
        let store = TursoRelational::in_memory().await.unwrap();
        store
            .execute(
                "INSERT INTO queues(tenant,queue,definition) VALUES('t','q','{}')",
                vec![],
            )
            .await
            .unwrap();
        let queue = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let id = ItemId::from_u64(1);
        let scope = Some(BTreeSet::from([id, ItemId::from_u64(2)]));
        let mut writer = store.writer.lock().await;
        let tx = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .unwrap();
        let hop = tx.clone();
        let q = queue.clone();
        let s = scope.clone();
        crate::tx::run_reltx_blocking(move || {
            let rel = TursoRel(&hop);
            let before = counts(&rel, &q, &s)?;
            rel.execute("INSERT INTO fireweed_items(tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority_sort,item_version,last_command_sequence,created_at,updated_at,max_attempts,created_seq) VALUES('t','q',?1,?1,'Pending',X'00',1,1,1,1,5,1)", &[id.to_string().into()])?;
            MetricsDelta(vec![(q.clone(), s.clone(), before, None)]).apply(&rel)?;
            assert_eq!(counts(&rel, &q, &None)?, [1, 0, 0, 0]);
            assert_eq!(rel.query(READ_SQL, &["t".into(), "q".into()])?[0].get::<i64>(0)?, 1);
            // Missing ID and repeated/no-op apply contribute no phantom count.
            let before = counts(&rel, &q, &s)?;
            MetricsDelta(vec![(q.clone(), s.clone(), before, None)]).apply(&rel)?;
            let before = counts(&rel, &q, &s)?;
            rel.execute("UPDATE fireweed_items SET lifecycle_state='Leased'", &[])?;
            MetricsDelta(vec![(q.clone(), s.clone(), before, None)]).apply(&rel)?;
            let actual = rel.query(READ_SQL, &["t".into(), "q".into()])?;
            assert_eq!(actual[0].get::<i64>(0)?, 0);
            assert_eq!(actual[0].get::<i64>(1)?, 1);
            Ok::<_, EngineError>(())
        }).await.unwrap();
        tx.commit().await.unwrap();
        let tx = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .unwrap();
        let hop = tx.clone();
        crate::tx::run_reltx_blocking(move || {
            let rel = TursoRel(&hop);
            let before = counts(&rel, &queue, &scope)?;
            rel.execute("DELETE FROM fireweed_items", &[])?;
            MetricsDelta(vec![(queue, scope, before, None)]).apply(&rel)?;
            assert_eq!(
                rel.query(READ_SQL, &["t".into(), "q".into()])?[0].get::<i64>(1)?,
                0
            );
            Ok::<_, EngineError>(())
        })
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        drop(writer);
        let queue = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let metrics = store.server_metrics(&queue).await.unwrap();
        assert_eq!((metrics.pending, metrics.leased), (0, 1));
        store.migrate().await.unwrap();
        let reopened = store.server_metrics(&queue).await.unwrap();
        assert_eq!(reopened.leased, 1);
    }

    fn claim_command(ids: Vec<ItemId>, authority_first: bool) -> CommandEnvelope {
        fireweed_conformance::envelope(
            QueueCommand::Claim(fireweed_engine::ClaimCommand {
                item_ids: ids.clone(),
                lease_token: fireweed_core::LeaseToken::new("metrics-claim").unwrap(),
                lease_expires_at: fireweed_conformance::ts(100),
                worker_id: None,
                authority_first,
            }),
            ids,
        )
    }

    #[test]
    fn fresh_authority_claims_avoid_metric_reads_but_ambiguous_histories_do_not() {
        struct Reads(std::cell::Cell<usize>);
        impl RelTx for Reads {
            fn execute(&self, _: &str, _: &[RelValue]) -> EngineResult<usize> {
                unreachable!()
            }
            fn query(
                &self,
                _: &str,
                _: &[RelValue],
            ) -> EngineResult<Vec<fireweed_relational::RelRow>> {
                self.0.set(self.0.get() + 1);
                Ok(vec![fireweed_relational::RelRow(vec![
                    1i64.into(),
                    0i64.into(),
                    0i64.into(),
                    0i64.into(),
                ])])
            }
        }
        let q = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let id = ItemId::from_u64(1);
        let seeds = std::collections::HashMap::from([(q.clone(), 0)]);
        let position = CommandPosition::new(q.clone(), 0, 0);
        let claim = claim_command(vec![id], true);
        let rel = Reads(std::cell::Cell::new(0));
        let delta = MetricsDelta::capture(
            &rel,
            std::slice::from_ref(&position),
            std::slice::from_ref(&claim),
            &seeds,
        )
        .unwrap();
        assert_eq!(
            rel.0.get(),
            0,
            "guarded fresh claim should need no metric SELECT"
        );
        assert_eq!(delta.0[0].2, [1, 0, 0, 0]);
        assert_eq!(delta.0[0].3, Some([0, 1, 0, 0]));
        for (positions, commands, seeds) in [
            (
                vec![position.clone()],
                vec![claim_command(vec![id], false)],
                seeds.clone(),
            ),
            (
                vec![position.clone()],
                vec![claim.clone()],
                std::collections::HashMap::from([(q.clone(), 1)]),
            ),
            (
                vec![CommandPosition::new(q.clone(), 0, 1)],
                vec![claim.clone()],
                seeds.clone(),
            ),
            (
                vec![position.clone(), CommandPosition::new(q.clone(), 0, 1)],
                vec![
                    fireweed_conformance::envelope(
                        QueueCommand::PurgeItems(fireweed_engine::PurgeItemsCommand {
                            item_ids: vec![id],
                            force: true,
                        }),
                        vec![id],
                    ),
                    claim.clone(),
                ],
                seeds.clone(),
            ),
            (
                vec![position.clone(), CommandPosition::new(q.clone(), 0, 1)],
                vec![
                    claim.clone(),
                    fireweed_conformance::envelope(QueueCommand::ResumeQueue, vec![]),
                ],
                seeds.clone(),
            ),
        ] {
            let before = rel.0.get();
            MetricsDelta::capture(&rel, &positions, &commands, &seeds).unwrap();
            assert!(
                rel.0.get() > before,
                "ambiguous/replayed prefixes must read actual rows"
            );
        }
    }

    #[tokio::test]
    async fn authority_claim_metric_inference_replays_and_rolls_back_with_rows() {
        use fireweed_conformance::{envelope, item, qdef};
        use fireweed_engine::AsyncProjectionStore;
        for invalid in ["missing", "leased", "superseded"] {
            let store = TursoRelational::in_memory().await.unwrap();
            let definition = qdef();
            let q = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            AsyncProjectionStore::ensure_shard(&store, definition)
                .await
                .unwrap();
            let items = vec![item("1", "a", 1), item("2", "b", 2), item("3", "c", 3)];
            let ids: Vec<_> = items.iter().map(|i| i.item_id).collect();
            let push = envelope(
                QueueCommand::Push(fireweed_engine::PushCommand { items }),
                ids.clone(),
            );
            AsyncProjectionStore::apply_live(
                &store,
                vec![CommandPosition::new(q.clone(), 0, 0)],
                vec![push],
            )
            .await
            .unwrap();
            if invalid == "superseded" {
                store
                    .execute(
                        "UPDATE fireweed_items SET superseded=1 WHERE item_id=?1",
                        vec![ids[2].to_string().into()],
                    )
                    .await
                    .unwrap();
                store
                    .execute(
                        "UPDATE queues SET resident_pending=resident_pending-1",
                        vec![],
                    )
                    .await
                    .unwrap();
            }
            let claim = claim_command(vec![ids[0]], true);
            let position = CommandPosition::new(q.clone(), 0, 1);
            AsyncProjectionStore::apply_live(&store, vec![position.clone()], vec![claim.clone()])
                .await
                .unwrap();
            // Replaying a committed claim must not apply its inferred delta twice.
            AsyncProjectionStore::apply_live(&store, vec![position], vec![claim])
                .await
                .unwrap();
            AsyncProjectionStore::apply_live(
                &store,
                vec![CommandPosition::new(q.clone(), 0, 2)],
                vec![claim_command(vec![ids[0]], false)],
            )
            .await
            .unwrap();
            let before = store.server_metrics(&q).await.unwrap();
            let bad_id = match invalid {
                "missing" => ItemId::from_u64(999),
                "leased" => ids[0],
                _ => ids[2],
            };
            let result = AsyncProjectionStore::apply_live(
                &store,
                vec![CommandPosition::new(q.clone(), 0, 3)],
                vec![claim_command(vec![ids[1], bad_id], true)],
            )
            .await;
            assert!(result.is_err(), "invalid {invalid} claim must fail");
            let after = store.server_metrics(&q).await.unwrap();
            assert_eq!(
                (after.pending, after.leased, after.complete, after.failed),
                (
                    before.pending,
                    before.leased,
                    before.complete,
                    before.failed
                )
            );
            // The rejected transaction must leave both the row and cursor reusable.
            AsyncProjectionStore::apply_live(
                &store,
                vec![CommandPosition::new(q.clone(), 0, 3)],
                vec![claim_command(vec![ids[1]], true)],
            )
            .await
            .unwrap();
            let rows = store.query("SELECT lifecycle_state,COUNT(*) FROM fireweed_items WHERE superseded=0 GROUP BY lifecycle_state", vec![]).await.unwrap();
            let mut actual = [0u64; 4];
            for row in rows {
                let turso::Value::Text(state) = &row.values[0] else {
                    panic!("state")
                };
                let turso::Value::Integer(count) = row.values[1] else {
                    panic!("count")
                };
                let index = match state.as_str() {
                    "Pending" => 0,
                    "Leased" => 1,
                    "Complete" => 2,
                    "Failed" => 3,
                    _ => panic!("state"),
                };
                actual[index] = count as u64;
            }
            let m = store.server_metrics(&q).await.unwrap();
            assert_eq!([m.pending, m.leased, m.complete, m.failed], actual);
            assert_eq!(m.leased, 2);
        }
    }
}
