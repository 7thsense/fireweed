use fireweed_conformance::{qdef, qkey};
use fireweed_engine::{
    ControlPlaneStore, HistoricalProjectionRead, ProjectionRead, ProjectionStore, PushPort,
    PushSpec, assemble_async_log_replay,
};
use fireweed_projection::{InMemoryProjection, MemoryLog};

#[test]
fn read_as_of_reconstructs_prior_state() {
    let backend = assemble_async_log_replay(MemoryLog::new(), InMemoryProjection::new(), 0)
        .expect("assemble");
    let shard = qkey();

    futures::executor::block_on(backend.create_queue(qdef())).unwrap();

    let first_ids = futures::executor::block_on(backend.push(
        &shard,
        vec![PushSpec::default()],
        fireweed_conformance::ts(0),
        None,
    ))
    .unwrap();
    let expected = futures::executor::block_on(backend.peek(&shard, 10)).unwrap();
    let position = futures::executor::block_on(backend.current_position(&shard)).unwrap();

    let _second_ids = futures::executor::block_on(backend.push(
        &shard,
        vec![PushSpec::default()],
        fireweed_conformance::ts(1),
        None,
    ))
    .unwrap();

    let query_shard = shard.clone();
    let as_of: Vec<fireweed_engine::ItemView> = futures::executor::block_on(backend.read_as_of(
        &shard,
        position,
        move |projection: &InMemoryProjection| projection.peek(&query_shard, 10),
    ))
    .unwrap();

    assert_eq!(as_of.len(), expected.len());
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].item_id, expected[0].item_id);
    assert_eq!(as_of[0].item_id, first_ids[0]);
}
