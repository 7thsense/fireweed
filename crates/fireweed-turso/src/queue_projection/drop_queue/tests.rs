use super::super::tests::{address, bootstrap, inspect, seed};
use super::*;
use fireweed_engine::queue_admission::QueueAdmission;

async fn count(projection: &Arc<QueueProjection>, table: &str) -> i64 {
    let rows = inspect(
        projection,
        &format!("SELECT COUNT(*) FROM {table}"),
        vec![],
        1,
    )
    .await;
    integer(&rows[0]).unwrap()
}

#[tokio::test]
async fn open_discards_files_left_by_an_earlier_process() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection.db");
    let job = Arc::new(
        QueueAdmission::new(1, 64 << 20)
            .unwrap()
            .reserve(0)
            .unwrap(),
    );
    let workers = ScratchWorkers::new(1, 2).unwrap();
    let (projection, _definition) = bootstrap(&path, &job, &workers).await;
    assert_eq!(count(&projection, "q7_queues").await, 1);
    drop(projection);
    let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
    assert_eq!(count(&reopened, "q7_queues").await, 0);
    workers.close_and_drain().await.unwrap();
}

#[tokio::test]
async fn drop_queue_removes_every_row_of_that_queue() {
    let directory = tempfile::tempdir().unwrap();
    let job = Arc::new(
        QueueAdmission::new(1, 64 << 20)
            .unwrap()
            .reserve(0)
            .unwrap(),
    );
    let workers = ScratchWorkers::new(1, 2).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("projection.db"), &job, &workers).await;
    seed(&projection, &definition, &job, &workers, false).await;
    for table in ["q7_queues", "q7_records", "q7_bodies", "q7_requests"] {
        assert!(count(&projection, table).await > 0, "{table} seeded");
    }
    projection
        .drop_queue(address(), &job, &workers)
        .await
        .unwrap();
    for table in [
        "q7_queues",
        "q7_records",
        "q7_bodies",
        "q7_fields",
        "q7_additional",
        "q7_indexes",
        "q7_requests",
    ] {
        assert_eq!(count(&projection, table).await, 0, "{table} emptied");
    }
    // Dropping an absent queue is a no-op.
    projection
        .drop_queue(address(), &job, &workers)
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
}
