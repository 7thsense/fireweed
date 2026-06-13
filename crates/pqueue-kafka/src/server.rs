//! TCP wire server for pqueue-kafka (producer-only).
//!
//! Reuses the WIRE-001 framing pattern from heimq: reader/writer split with a
//! bounded channel and consecutive-error limit.

use crate::router::{route, RouterError, SharedRouterState};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;
const CHANNEL_DEPTH: usize = 64;
const MAX_CONSECUTIVE_ERRORS: usize = 10;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("router: {0}")]
    Router(#[from] RouterError),
}

pub struct ProducerServer {
    state: SharedRouterState,
}

impl ProducerServer {
    pub fn new(state: SharedRouterState) -> Self {
        Self { state }
    }

    pub async fn run(&self, addr: &str) -> Result<(), ServerError> {
        let listener = TcpListener::bind(addr).await?;
        info!(addr = %listener.local_addr()?, "pqueue-kafka producer server listening");
        self.run_with_listener(listener, None).await
    }

    pub async fn run_with_listener(
        &self,
        listener: TcpListener,
        max_connections: Option<usize>,
    ) -> Result<(), ServerError> {
        let mut served = 0usize;
        loop {
            match listener.accept().await {
                Ok((socket, peer)) => {
                    let _ = socket.set_nodelay(true);
                    let state = self.state.clone();
                    debug!(peer = %peer, "accepted connection");
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, peer, state).await {
                            debug!(peer = %peer, error = %e, "connection error");
                        }
                    });
                    served += 1;
                    if max_connections.is_some_and(|limit| served >= limit) {
                        break;
                    }
                }
                Err(e) => {
                    error!(error = %e, "accept error");
                }
            }
        }
        Ok(())
    }
}

async fn handle_connection(
    stream: TcpStream,
    _peer: SocketAddr,
    state: SharedRouterState,
) -> Result<(), ServerError> {
    let (read_half, write_half) = tokio::io::split(stream);
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_DEPTH);
    let reader_handle = tokio::spawn(run_reader(read_half, tx));
    let writer_result = run_writer(write_half, rx, state).await;
    reader_handle.abort();
    writer_result
}

async fn run_reader<R: AsyncRead + Unpin + Send + 'static>(
    mut stream: R,
    tx: tokio::sync::mpsc::Sender<Bytes>,
) -> Result<(), ServerError> {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(());
            }
            return Err(ServerError::Protocol("connection closed with pending data".into()));
        }
        while buf.len() >= 4 {
            let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if msg_len > MAX_FRAME_BYTES {
                return Err(ServerError::Protocol(format!(
                    "frame size {} exceeds max_frame_bytes {}",
                    msg_len, MAX_FRAME_BYTES
                )));
            }
            if buf.len() < 4 + msg_len {
                break;
            }
            buf.advance(4);
            let frame = buf.split_to(msg_len).freeze();
            if tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
    }
}

async fn run_writer<W: AsyncWrite + Unpin>(
    mut stream: W,
    mut rx: tokio::sync::mpsc::Receiver<Bytes>,
    state: SharedRouterState,
) -> Result<(), ServerError> {
    let mut consecutive_errors = 0usize;
    while let Some(frame) = rx.recv().await {
        let (route_result, store) = {
            let st = state.read().await;
            let store = st.store.clone();
            (route(&frame, &st), store)
        };
        match route_result {
            Ok((response, batches)) => {
                // Persist produce batches before acking (ack-after-store).
                if !batches.is_empty() {
                    if let Some(s) = &store {
                        if let Err(e) = s.persist(batches).await {
                            warn!(error = %e, "produce storage error");
                        }
                    }
                }
                stream.write_all(&response).await?;
                consecutive_errors = 0;
            }
            Err(e) => {
                warn!(error = %e, "request routing error");
                if let Some(corr_id) = peek_correlation_id(&frame) {
                    let error_frame = make_error_frame(corr_id, 10);
                    let _ = stream.write_all(&error_frame).await;
                    consecutive_errors += 1;
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        return Err(ServerError::Router(e));
                    }
                } else {
                    return Err(ServerError::Router(e));
                }
            }
        }
    }
    Ok(())
}

fn peek_correlation_id(msg: &[u8]) -> Option<i32> {
    if msg.len() >= 8 {
        Some(i32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]]))
    } else {
        None
    }
}

fn make_error_frame(correlation_id: i32, error_code: i16) -> Bytes {
    let mut body = BytesMut::with_capacity(6);
    body.put_i32(correlation_id);
    body.put_i16(error_code);
    let mut frame = BytesMut::with_capacity(10);
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame.freeze()
}
