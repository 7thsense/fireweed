//! Test utilities for pqueue-kafka integration tests.

use crate::handler::metadata::BrokerMeta;
use crate::router::{KafkaStore, RouterState, SharedRouterState};
use crate::server::ProducerServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// A running pqueue-kafka test server.
pub struct TestProducerServer {
    pub addr: SocketAddr,
    pub state: SharedRouterState,
    store: Option<Arc<KafkaStore>>,
}

impl TestProducerServer {
    /// Start a test server without persistent storage (wire-only, backward-compatible).
    pub async fn start(queues: Vec<String>) -> Self {
        Self::start_inner(queues, None).await
    }

    /// Start a test server with in-process pqueue storage.
    ///
    /// Produce records are durably enqueued before the Produce response is sent.
    /// Use `store()` to inspect enqueued items after a produce.
    pub async fn start_with_store(queues: Vec<String>) -> Self {
        let store = Arc::new(KafkaStore::new());
        Self::start_inner(queues, Some(Arc::clone(&store))).await
    }

    async fn start_inner(queues: Vec<String>, store: Option<Arc<KafkaStore>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = BrokerMeta {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: addr.port() as i32,
            cluster_id: "pqueue-kafka-test".to_string(),
        };

        let state: SharedRouterState = Arc::new(RwLock::new(RouterState {
            queues,
            broker,
            store: store.clone(),
        }));
        let server = ProducerServer::new(state.clone());

        tokio::spawn(async move {
            let _ = server.run_with_listener(listener, None).await;
        });

        Self { addr, state, store }
    }

    pub fn bootstrap_servers(&self) -> String {
        format!("127.0.0.1:{}", self.addr.port())
    }

    /// Returns the pqueue storage backing this server, if any.
    pub fn store(&self) -> Option<&Arc<KafkaStore>> {
        self.store.as_ref()
    }
}
