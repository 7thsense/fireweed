use fireweed_turso::{TursoConfig, TursoRelational};
use turso::Value;

// The index must support both ordinary expiry and the broader pending-lease
// view. Fenced and cohort leases are visible to the latter, not the former.
#[tokio::test]
async fn lease_index_migration_preserves_scoped_expiry_and_pending_reads() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("projection.db");
    let store = TursoRelational::open(TursoConfig::local(&path).with_log_backed_projection())
        .await
        .unwrap();
    store
        .execute(
            "DROP INDEX IF EXISTS fireweed_items_leased_scope_idx",
            vec![],
        )
        .await
        .unwrap();
    for sql in [
        "CREATE INDEX IF NOT EXISTS fireweed_items_expired_lease_idx ON fireweed_items(tenant_id,queue_id,lease_expires_at,item_id) WHERE lifecycle_state='Leased' AND cohort_size IS NULL AND fenced=0 AND superseded=0",
        "CREATE INDEX IF NOT EXISTS fireweed_items_global_expired_lease_idx ON fireweed_items(lease_expires_at,tenant_id,queue_id,item_id) WHERE lifecycle_state='Leased'",
    ] {
        store.execute(sql, vec![]).await.unwrap();
    }
    for (id, tenant, queue, state, expiry, cohort, fenced, superseded) in [
        (1, "a", "a", "Leased", 9, None, 0, 0),
        (2, "a", "a", "Leased", 10, None, 0, 0),
        (3, "a", "a", "Leased", 8, None, 1, 0),
        (4, "a", "a", "Leased", 8, Some(2), 0, 0),
        (5, "a", "a", "Leased", 8, None, 0, 1),
        (6, "a", "a", "Pending", 8, None, 0, 0),
        (7, "b", "a", "Leased", 8, None, 0, 0),
        (8, "a", "b", "Leased", 8, None, 0, 0),
    ] {
        store.execute("INSERT INTO fireweed_items(tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority_sort,item_version,last_command_sequence,created_at,updated_at,max_attempts,created_seq,lease_expires_at,cohort_size,fenced,superseded) VALUES(?1,?2,?3,?3,?4,X'00',1,1,0,0,3,?5,?6,?7,?8,?9)",
            vec![tenant.into(),queue.into(),id.to_string().into(),state.into(),
                 Value::Integer(id),Value::Integer(expiry),cohort.map(Value::Integer).unwrap_or(Value::Null),
                 Value::Integer(fenced),Value::Integer(superseded)]).await.unwrap();
    }
    let image = store
        .query(
            "SELECT * FROM fireweed_items ORDER BY tenant_id,queue_id,item_id",
            vec![],
        )
        .await
        .unwrap();
    for _ in 0..2 {
        let schema = store.migrate().await.unwrap();
        assert!(
            schema
                .indexes
                .iter()
                .any(|name| name == "fireweed_items_leased_scope_idx")
        );
        assert!(
            !schema
                .indexes
                .iter()
                .any(|name| name == "fireweed_items_expired_lease_idx"
                    || name == "fireweed_items_global_expired_lease_idx")
        );
        assert_eq!(
            image,
            store
                .query(
                    "SELECT * FROM fireweed_items ORDER BY tenant_id,queue_id,item_id",
                    vec![]
                )
                .await
                .unwrap()
        );
    }
    for (sql, expected) in [
        (
            "SELECT item_id FROM fireweed_items WHERE tenant_id='a' AND queue_id='a' AND lifecycle_state='Leased' AND cohort_size IS NULL AND fenced=0 AND superseded=0 AND lease_expires_at IS NOT NULL AND lease_expires_at<10 ORDER BY item_id LIMIT 10",
            vec!["1"],
        ),
        (
            "SELECT item_id,lease_expires_at,retry_count FROM fireweed_items WHERE tenant_id='a' AND queue_id='a' AND lifecycle_state='Leased' AND superseded=0 ORDER BY item_id",
            vec!["1", "2", "3", "4"],
        ),
    ] {
        let rows = store.query(sql, vec![]).await.unwrap();
        let ids: Vec<_> = rows
            .iter()
            .map(|r| match &r.values[0] {
                Value::Text(id) => id.as_str(),
                other => panic!("unexpected ID {other:?}"),
            })
            .collect();
        assert_eq!(ids, expected);
        let plan = format!(
            "{:?}",
            store
                .query(format!("EXPLAIN QUERY PLAN {sql}"), vec![])
                .await
                .unwrap()
        );
        assert!(
            plan.contains("fireweed_items_leased_scope_idx"),
            "query={sql}; plan={plan}"
        );
    }
    drop(store);
    let reopened = TursoRelational::open(TursoConfig::local(path).with_log_backed_projection())
        .await
        .unwrap();
    assert_eq!(
        image,
        reopened
            .query(
                "SELECT * FROM fireweed_items ORDER BY tenant_id,queue_id,item_id",
                vec![]
            )
            .await
            .unwrap()
    );
}
