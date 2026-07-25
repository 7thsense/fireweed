//! C9/C6 (epic pqueue-2201fd37, bead pqueue-593007ee): the `fireweed_side_records` / `fireweed_instance_fences`
//! tables on `PostgresRelationalBackend`. Proves `WriteSideRecords`/`AdvanceInstanceFence` commands persist
//! and survive a reconnect.
//!
//! Scope note: `CommitTransitionPort::commit_transition` (the Snorri-facing orchestration) and
//! `RecoveryReadPort` (`side_record`/`instance_fence` reads) are NOT wired on this backend yet — both are
//! separate follow-on beads, and `hot_projection_queries.rs` already asserts they stay `Unavailable`. This
//! test instead drives the two commands directly through the typed raw-commit seam and reads the persisted rows back with a
//! raw SQL query against the same schema (no read port exists yet to exercise).
//!
//! ENV-GATED on `FIREWEED_PG_TEST_URL` (a live database). Without it this prints a LOUD skip and returns —
//! a green run is then VISIBLY partial, never a hidden pass. To run live:
//!   docker run -d --name fireweed-pg -p 5433:5432 -e POSTGRES_PASSWORD=fireweed postgres:16
//!   FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5433/postgres \
//!     cargo test -p fireweed-postgres --test relational_side_records_and_instance_fences

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use fireweed_conformance::{envelope, qdef, qkey};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, Backend, ControlPlaneStore, QueueCommand, QueueKey, SideRecord,
    WriteSideRecordsCommand,
};
use fireweed_postgres::{PostgresConnectConfig, PostgresRelationalBackend, connect};

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_side_fence_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// Apply one command through the atomic unit of work (append + apply), stamping the current durable epoch —
/// the same seam every write port (`PushPort`, `SetGatesPort`, ...) is built on, minus the port wrapper.
async fn commit(backend: &PostgresRelationalBackend, shard: &QueueKey, command: QueueCommand) {
    let epoch = backend.current_epoch(shard).await.expect("current epoch");
    let env = envelope(command, vec![]);
    backend
        .commit_raw(fireweed_engine::RawCommitRequest::new(
            shard.clone(),
            vec![env],
            epoch,
        ))
        .await
        .expect("commit side-record/instance-fence command");
}

fn read_side_record(url: &str, schema: &str, key: &[u8]) -> Option<Vec<u8>> {
    let mut client = connect(PostgresConnectConfig::new(url)).expect("connect");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set search_path");
    client
        .query_opt(
            "SELECT payload FROM fireweed_side_records \
             WHERE tenant_id=$1 AND queue_id=$2 AND key=$3",
            &[
                &qkey().tenant_id.as_str().to_string(),
                &qkey().queue_id.as_str().to_string(),
                &key,
            ],
        )
        .expect("query fireweed_side_records")
        .map(|row| row.get(0))
}

fn read_instance_fence(url: &str, schema: &str, instance_key: &[u8]) -> Option<i64> {
    let mut client = connect(PostgresConnectConfig::new(url)).expect("connect");
    client
        .batch_execute(&format!("SET search_path TO {schema};"))
        .expect("set search_path");
    client
        .query_opt(
            "SELECT fence FROM fireweed_instance_fences \
             WHERE tenant_id=$1 AND queue_id=$2 AND instance_key=$3",
            &[
                &qkey().tenant_id.as_str().to_string(),
                &qkey().queue_id.as_str().to_string(),
                &instance_key,
            ],
        )
        .expect("query fireweed_instance_fences")
        .map(|row| row.get(0))
}

#[test]
fn write_side_records_and_advance_instance_fence_persist_and_survive_reconnect() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES SIDE-RECORD/INSTANCE-FENCE SKIPPED (write_side_records_and_advance_instance_fence_persist_and_survive_reconnect) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema();
    let shard = qkey();

    futures::executor::block_on(async {
        let backend =
            PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect postgres");
        backend.create_queue(qdef()).await.expect("create queue");

        commit(
            &backend,
            &shard,
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![
                    SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::from_static(b"superseded-in-batch"),
                    },
                    SideRecord {
                        key: b"state/run-2".to_vec(),
                        payload: Bytes::from_static(b"second-record"),
                    },
                    SideRecord {
                        key: b"state/run-1".to_vec(),
                        payload: Bytes::from_static(b"opaque-bytes"),
                    },
                ],
            }),
        )
        .await;
        commit(
            &backend,
            &shard,
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance/a".to_vec(),
                expected: 0,
                next: 7,
            }),
        )
        .await;
    });

    // Persisted immediately (queried through a fresh, independent connection — not the writer's own).
    assert_eq!(
        read_side_record(&url, &schema, b"state/run-1"),
        Some(b"opaque-bytes".to_vec())
    );
    assert_eq!(
        read_side_record(&url, &schema, b"state/run-2"),
        Some(b"second-record".to_vec()),
        "one vector statement must persist every distinct side-record key"
    );
    assert_eq!(read_instance_fence(&url, &schema, b"instance/a"), Some(7));

    // Reconnect to the SAME schema (drops the in-process handle, reopens against durable postgres state) —
    // both rows must still be there, and a repeat upsert of the fence must still advance/overwrite in place.
    futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema)
            .expect("reconnect postgres");
        commit(
            &backend,
            &shard,
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance/a".to_vec(),
                expected: 7,
                next: 8,
            }),
        )
        .await;
    });

    assert_eq!(
        read_side_record(&url, &schema, b"state/run-1"),
        Some(b"opaque-bytes".to_vec()),
        "side record must survive reconnect"
    );
    assert_eq!(
        read_instance_fence(&url, &schema, b"instance/a"),
        Some(8),
        "instance fence must survive reconnect and accept a post-reconnect advance"
    );
}
