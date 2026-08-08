//! ad-hoc probe: SqliteLog::append fsync cost alone, file-backed, 2000-envelope batches.
use std::time::Instant;

use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{ClientItemKey, ItemId};
use fireweed_engine::{
    CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore, LogStore, PushCommand,
    PushItem, QueueCommand,
};
use fireweed_sqlite::SqliteLog;

fn envelope(i: usize, offset: usize) -> CommandEnvelope {
    let id = format!("cost-{offset}-{i}");
    let item_id = ItemId::mint(0, 0, (offset + i) as u32);
    let item = PushItem {
        client_item_key: ClientItemKey::new(id.clone()).unwrap(),
        item_id,
        priority: None,
        not_before: None,
        group_key: None,
        max_attempts: 3,
        payload: None,
        fields: Default::default(),
        metadata: Default::default(),
        cohort_size: None,
        gate_keys: vec![],
        entity_document: None,
    };
    CommandEnvelope {
        command_id: CommandId::new(&id),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids: vec![item_id],
        command: QueueCommand::Push(PushCommand { items: vec![item] }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

fn append_batch(log: &mut SqliteLog, n: usize, offset: usize) {
    let envs: Vec<CommandEnvelope> = (0..n).map(|i| envelope(i, offset)).collect();
    log.append(&shard(), &envs, 0).expect("append");
}

fn run(mut log: SqliteLog, label: &str) {
    log.create_or_read_definition(&qdef())
        .expect("create_queue");
    log.ensure_shard(&shard()).expect("ensure_shard");

    for wave in 0..5 {
        append_batch(&mut log, 2_000, wave * 2_000);
    }

    let t10 = Instant::now();
    append_batch(&mut log, 2_000, 10_000);
    let ms10 = t10.elapsed().as_secs_f64() * 1000.0;

    for wave in 0..9 {
        append_batch(&mut log, 2_000, 12_000 + wave * 2_000);
    }

    let t30 = Instant::now();
    append_batch(&mut log, 2_000, 30_000);
    let ms30 = t30.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "{label}: @10k {ms10:.1} ms ({:.4} ms/item); @30k {ms30:.1} ms ({:.4} ms/item)",
        ms10 / 2000.0,
        ms30 / 2000.0,
    );
}

fn main() {
    run(SqliteLog::in_memory().expect("mem"), "log in_memory");

    let path =
        std::env::temp_dir().join(format!("push-floor-probe3-{}.sqlite", std::process::id()));
    let path_str = path.to_str().unwrap().to_owned();
    let _ = std::fs::remove_file(&path);
    run(
        SqliteLog::open(&path_str).expect("file"),
        "log file (synchronous=FULL)",
    );
    let _ = std::fs::remove_file(&path);
}
