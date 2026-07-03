use pqueue_conformance::{qdef, qkey};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, HistoricalProjectionRead, InProcessControlPlane,
    ProjectionRead, ProjectionStore, PushPort, PushSpec,
};
use pqueue_projection::{InMemoryProjection, MemoryLog};

#[test]
fn read_as_of_reconstructs_prior_state() {
    let backend = ComposedBackend::new(
        MemoryLog::new(),
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    );
    let shard = qkey();

    futures::executor::block_on(backend.create_queue(qdef())).unwrap();

    let first_ids = futures::executor::block_on(backend.push(
        &shard,
        vec![PushSpec::default()],
        pqueue_conformance::ts(0),
        None,
    ))
    .unwrap();
    let expected = futures::executor::block_on(backend.peek(&shard, 10)).unwrap();
    let position = futures::executor::block_on(backend.current_position(&shard)).unwrap();

    let _second_ids = futures::executor::block_on(backend.push(
        &shard,
        vec![PushSpec::default()],
        pqueue_conformance::ts(1),
        None,
    ))
    .unwrap();

    let as_of: Vec<pqueue_engine::ItemView> = futures::executor::block_on(backend.read_as_of(
        &shard,
        position,
        |projection: &InMemoryProjection| projection.peek(&shard, 10),
    ))
    .unwrap();

    assert_eq!(as_of.len(), expected.len());
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].item_id, expected[0].item_id);
    assert_eq!(as_of[0].item_id, first_ids[0]);
}
