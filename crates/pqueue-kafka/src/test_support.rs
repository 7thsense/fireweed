//! Test utilities for pqueue-kafka integration tests.

use crate::handler::metadata::BrokerMeta;
use crate::router::{RouterState, SharedRouterState};
use crate::server::ProducerServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// A running pqueue-kafka test server.
pub struct TestProducerServer {
    pub addr: SocketAddr,
    pub state: SharedRouterState,
}

impl TestProducerServer {
    /// Start a test server on a random port with the given queue names.
    pub async fn start(queues: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = BrokerMeta {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: addr.port() as i32,
            cluster_id: "pqueue-kafka-test".to_string(),
        };

        let state: SharedRouterState = Arc::new(RwLock::new(RouterState { queues, broker }));
        let server = ProducerServer::new(state.clone());

        tokio::spawn(async move {
            let _ = server.run_with_listener(listener, None).await;
        });

        Self { addr, state }
    }

    pub fn bootstrap_servers(&self) -> String {
        format!("127.0.0.1:{}", self.addr.port())
    }
}
