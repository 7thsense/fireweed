#![cfg(feature = "local")]

//! Native SQL diagnostic only. These fixture timings are not workflow qualification.
use fireweed_conformance::{envelope, item, qdef};
use fireweed_engine::{AsyncProjectionStore, CommandPosition, PushCommand, QueueCommand, QueueKey};
use fireweed_turso::TursoRelational;
use std::time::Instant;
use turso::Value;

// Frozen pre-join statement: keep this control independent of production changes.
fn baseline_sql(row_count: usize) -> String {
    let values_sql = vec!["(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; row_count].join(",");
    format!(
        "WITH incoming(item_id,state,priority,priority_sort,not_before,eligible_since,fields,metadata,\
             entity_document,index_fields,new_version,terminal_at,terminal_epoch,expected_version,claimed_here,keeps_payload) \
             AS (VALUES {values_sql}) UPDATE fireweed_items SET \
             lifecycle_state=incoming.state,priority=incoming.priority,priority_sort=incoming.priority_sort,\
             not_before=incoming.not_before,eligible_since=incoming.eligible_since,\
             payload=CASE WHEN incoming.keeps_payload THEN fireweed_items.payload ELSE NULL END,\
             fields=incoming.fields,metadata=incoming.metadata,entity_document=incoming.entity_document,\
             index_fields=incoming.index_fields,lease_token_hash=NULL,lease_expires_at=NULL,worker_id=NULL,fenced=0,\
             item_version=incoming.new_version,terminal_at=incoming.terminal_at,terminal_command_epoch=incoming.terminal_epoch,\
             updated_at=?,last_command_sequence=?,retry_count=retry_count+incoming.claimed_here \
             FROM incoming WHERE fireweed_items.rowid IN ( \
             SELECT target.rowid FROM incoming CROSS JOIN fireweed_items target \
             INDEXED BY sqlite_autoindex_fireweed_items_1 \
             ON target.tenant_id=? AND target.queue_id=? AND target.item_id=incoming.item_id) \
             AND fireweed_items.item_id=incoming.item_id \
             AND fireweed_items.item_version=incoming.expected_version \
             AND (incoming.claimed_here=0 OR (lifecycle_state='Pending' AND superseded=0))"
    )
}

fn sql(shape: &str, count: usize) -> String {
    let base = baseline_sql(count);
    if shape == "baseline" {
        return base;
    }
    let (prefix, _) = base
        .split_once("FROM incoming WHERE fireweed_items.rowid IN (")
        .unwrap();
    let guard = base
        .split_once("AND fireweed_items.item_id=incoming.item_id")
        .unwrap()
        .1;
    let join = match shape {
        "scalar" => {
            "FROM incoming WHERE fireweed_items.rowid=(SELECT target.rowid FROM fireweed_items target INDEXED BY sqlite_autoindex_fireweed_items_1 WHERE target.tenant_id=? AND target.queue_id=? AND target.item_id=incoming.item_id)"
        }
        "joined" => {
            "FROM incoming CROSS JOIN fireweed_items target INDEXED BY sqlite_autoindex_fireweed_items_1 ON target.tenant_id=? AND target.queue_id=? AND target.item_id=incoming.item_id WHERE fireweed_items.rowid=target.rowid"
        }
        _ => unreachable!(),
    };
    format!("{prefix}{join} AND fireweed_items.item_id=incoming.item_id{guard}")
        .replace(
            "retry_count=retry_count+",
            "retry_count=fireweed_items.retry_count+",
        )
        .replace(
            "(lifecycle_state='Pending' AND superseded=0)",
            "(fireweed_items.lifecycle_state='Pending' AND fireweed_items.superseded=0)",
        )
}

#[tokio::test]
#[ignore = "explicit native SQL timing diagnostic; not a workflow or capacity gate"]
async fn compare_replacement_shapes_and_statement_sizes() {
    const N: usize = 1000;
    for (shape, batch) in [
        ("baseline", 56),
        ("scalar", 56),
        ("joined", 56),
        ("baseline", 112),
        ("scalar", 112),
        ("joined", 112),
        ("baseline", 224),
        ("scalar", 224),
        ("joined", 224),
        ("baseline", 56),
    ] {
        let store = TursoRelational::in_memory().await.unwrap();
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let priority_sort = fireweed_relational::elig_sort(&None, &definition.priority_model);
        AsyncProjectionStore::ensure_shard(&store, definition)
            .await
            .unwrap();
        let pushed = (1000..1000 + N)
            .map(|n| item(&n.to_string(), &format!("key-{n}"), 1))
            .collect::<Vec<_>>();
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 0)],
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: pushed.clone(),
                }),
                pushed.iter().map(|p| p.item_id).collect(),
            )],
        )
        .await
        .unwrap();
        let query = sql(shape, batch);
        let plan = store
            .query(
                format!("EXPLAIN QUERY PLAN {query}"),
                vec![Value::Null; 16 * batch + 4],
            )
            .await;
        let plan = match plan {
            Ok(rows) => rows,
            Err(e) => {
                eprintln!("shape={shape} batch={batch} plan_error={e}");
                continue;
            }
        };
        eprintln!(
            "shape={shape} batch={batch} plan={:?}",
            plan.iter().map(|r| &r.values[3]).collect::<Vec<_>>()
        );
        let mut measured = 0.0;
        for round in 0..9 {
            let start = Instant::now();
            store.execute("BEGIN IMMEDIATE", vec![]).await.unwrap();
            for (chunk_index, chunk) in pushed.chunks(batch).enumerate() {
                let mut params = Vec::with_capacity(chunk.len() * 16 + 4);
                for (offset, row) in chunk.iter().enumerate() {
                    let terminal = (chunk_index * batch + offset + round) % 3 == 0;
                    params.extend([
                        Value::Text(row.item_id.to_string()),
                        Value::Text(if terminal { "Complete" } else { "Pending" }.into()),
                        Value::Null,
                        Value::Blob(priority_sort.clone()),
                        Value::Null,
                        Value::Integer(1),
                        Value::Text("{}".into()),
                        Value::Text(format!(
                            "{{\"round\":{round},\"padding\":\"{}\"}}",
                            "x".repeat(300)
                        )),
                        Value::Null,
                        Value::Null,
                        Value::Integer(round as i64 + 2),
                        if terminal {
                            Value::Integer(1)
                        } else {
                            Value::Null
                        },
                        if terminal {
                            Value::Integer(0)
                        } else {
                            Value::Null
                        },
                        Value::Integer(round as i64 + 1),
                        Value::Integer(0),
                        Value::Integer(1),
                    ]);
                }
                params.extend([
                    Value::Integer(1),
                    Value::Integer(round as i64 + 1),
                    Value::Text(shard.tenant_id.as_str().into()),
                    Value::Text(shard.queue_id.as_str().into()),
                ]);
                let changed = store
                    .execute(sql(shape, chunk.len()), params)
                    .await
                    .unwrap();
                assert_eq!(changed, chunk.len() as u64);
            }
            store.execute("COMMIT", vec![]).await.unwrap();
            if round > 0 {
                measured += start.elapsed().as_secs_f64();
            }
        }
        let versions = store
            .query(
                "SELECT MIN(item_version),MAX(item_version),COUNT(*) FROM fireweed_items",
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(
            versions[0].values,
            vec![
                Value::Integer(10),
                Value::Integer(10),
                Value::Integer(N as i64)
            ]
        );
        eprintln!(
            "shape={shape} batch={batch} updates={} measured_s={measured:.6} updates_per_s={:.2}",
            N * 8,
            N as f64 * 8.0 / measured
        );
    }
}
