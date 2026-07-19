use pqueue_sqlite::{SqliteRelationalBackend, composed_sqlite_backend_in_memory};

#[tokio::test]
async fn composed_sqlite_filtered_lifecycle_metrics_conformance() {
    pqueue_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(|| {
        composed_sqlite_backend_in_memory().expect("open composed sqlite")
    })
    .await;
}

#[tokio::test]
async fn relational_sqlite_filtered_lifecycle_metrics_conformance() {
    pqueue_conformance::scenarios::filtered_lifecycle_metrics_are_exact_and_read_only(|| {
        SqliteRelationalBackend::in_memory().expect("open relational sqlite")
    })
    .await;
}
