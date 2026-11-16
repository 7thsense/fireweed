use super::*;

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
    let rows = |projection: &Arc<QueueProjection>| {
        let projection = projection.clone();
        async move {
            let row = inspect(&projection, "SELECT COUNT(*) FROM q7_queues", vec![], 1).await;
            integer(&row[0]).unwrap()
        }
    };
    assert_eq!(rows(&projection).await, 1);
    drop(projection);
    let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
    assert_eq!(rows(&reopened).await, 0);
    workers.close_and_drain().await.unwrap();
}
