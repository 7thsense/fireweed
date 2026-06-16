//! TCP wire server for pqueue-kafka (producer-only).
//!
//! Delegates TCP accept, frame parsing, and response writing to heimq-wire
//! WireServer. PqueueFrameHandler wraps the router state and implements the
//! FrameHandler trait: route → persist → return response bytes.

use crate::router::{RouterError, SharedRouterState, route};
use async_trait::async_trait;
use bytes::Bytes;
use heimq_wire::{FrameError, FrameHandler, WireServer};

pub struct PqueueFrameHandler {
    state: SharedRouterState,
}

impl PqueueFrameHandler {
    pub fn new(state: SharedRouterState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl FrameHandler for PqueueFrameHandler {
    async fn handle(&self, frame: Bytes) -> Result<Bytes, FrameError> {
        let (route_result, store) = {
            let st = self.state.read().await;
            let store = st.store.clone();
            (route(&frame, &st), store)
        };
        let (response, batches) = route_result.map_err(|e| FrameError::Protocol(e.to_string()))?;
        if !batches.is_empty() {
            if let Some(s) = &store {
                s.persist(batches)
                    .await
                    .map_err(|e: RouterError| FrameError::Storage(e.to_string()))?;
            }
        }
        Ok(response)
    }
}

pub struct ProducerServer {
    inner: WireServer<PqueueFrameHandler>,
}

impl ProducerServer {
    pub fn new(state: SharedRouterState) -> Self {
        Self {
            inner: WireServer::new(PqueueFrameHandler::new(state)),
        }
    }

    pub async fn run(&self, addr: &str) -> Result<(), heimq_wire::WireError> {
        self.inner.run(addr).await
    }

    pub async fn run_with_listener(
        &self,
        listener: tokio::net::TcpListener,
        max_connections: Option<usize>,
    ) -> Result<(), heimq_wire::WireError> {
        self.inner
            .run_with_listener(listener, max_connections)
            .await
    }
}
