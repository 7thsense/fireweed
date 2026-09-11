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

pub(crate) struct MetricsDelta(Vec<(QueueKey, Scope, Counts)>);

impl MetricsDelta {
    pub(crate) fn capture(
        tx: &impl RelTx,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
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
            let before = counts(tx, &queue, &scope)?;
            captured.push((queue, scope, before));
        }
        Ok(Self(captured))
    }

    pub(crate) fn apply(self, tx: &impl RelTx) -> EngineResult<()> {
        for (queue, scope, before) in self.0 {
            let after = counts(tx, &queue, &scope)?;
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
        if let Err(error) = connection.execute(&sql, ()).await {
            if !error
                .to_string()
                .to_ascii_lowercase()
                .contains("duplicate column")
            {
                return Err(error.into());
            }
        }
    }
    if let Err(error) = connection
        .execute(
            "ALTER TABLE queues ADD COLUMN resident_counts_version INTEGER NOT NULL DEFAULT 0",
            (),
        )
        .await
    {
        if !error
            .to_string()
            .to_ascii_lowercase()
            .contains("duplicate column")
        {
            return Err(error.into());
        }
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
            MetricsDelta(vec![(q.clone(), s.clone(), before)]).apply(&rel)?;
            assert_eq!(counts(&rel, &q, &None)?, [1, 0, 0, 0]);
            assert_eq!(rel.query(READ_SQL, &["t".into(), "q".into()])?[0].get::<i64>(0)?, 1);
            // Missing ID and repeated/no-op apply contribute no phantom count.
            let before = counts(&rel, &q, &s)?;
            MetricsDelta(vec![(q.clone(), s.clone(), before)]).apply(&rel)?;
            let before = counts(&rel, &q, &s)?;
            rel.execute("UPDATE fireweed_items SET lifecycle_state='Leased'", &[])?;
            MetricsDelta(vec![(q.clone(), s.clone(), before)]).apply(&rel)?;
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
            MetricsDelta(vec![(queue, scope, before)]).apply(&rel)?;
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
}
