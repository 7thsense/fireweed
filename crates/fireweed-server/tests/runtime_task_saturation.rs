use fireweed_core::TenantId;
use fireweed_engine::{ControlPlaneStore, EngineError};
use fireweed_memory::composed_memory_backend;
use fireweed_server::PostgresWholeOperationAdapter;

#[tokio::test(flavor = "current_thread")]
async fn blocking_dispatch_returns_backpressure_when_runtime_task_slots_are_saturated() {
    let (_, _, prior_task_limit) = fireweed_resp::runtime_task_resource_counts();
    let (_, _, prior_connection_limit) = fireweed_resp::connection_resource_counts();
    fireweed_resp::set_max_live_connections(usize::MAX);
    fireweed_resp::set_max_runtime_tasks(1);

    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let holder = fireweed_resp::spawn_governed(async move {
        let _ = wait.await;
    });
    let adapter = PostgresWholeOperationAdapter::new(composed_memory_backend());
    let tenant = TenantId::new("tenant").unwrap();
    let error = adapter.list_queues(&tenant).await.unwrap_err();
    assert!(matches!(
        error,
        EngineError::Backpressure {
            resource: "runtime task slots"
        }
    ));

    release.send(()).unwrap();
    holder.await.unwrap();
    fireweed_resp::set_max_runtime_tasks(prior_task_limit);
    fireweed_resp::set_max_live_connections(prior_connection_limit);
}
