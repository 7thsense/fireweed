//! A counting HTTP/1.1 reverse proxy on loopback, between the object-log S3 client
//! and the real S3 endpoint. `QueueNativeLog` builds its client internally, so the
//! wire is the only place to count requests by kind. Requests are forwarded with
//! their headers untouched (`Host` included), so SigV4 still verifies upstream.
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug)]
#[allow(dead_code)] // `start` and `end` are kept for ad-hoc timing inspection.
pub(super) struct Recorded {
    /// `LIST` for a GET of a bucket (a listing); otherwise the HTTP method.
    pub(super) method: String,
    pub(super) uri: String,
    pub(super) if_match: bool,
    pub(super) if_none_match: bool,
    pub(super) status: u16,
    pub(super) start: Instant,
    pub(super) end: Instant,
}
#[derive(Clone, Debug, Default)]
pub(super) struct Window {
    pub(super) requests: Vec<Recorded>,
    pub(super) round_trips: u64,
}
impl Window {
    pub(super) fn count(&self, method: &str) -> usize {
        self.requests.iter().filter(|r| r.method == method).count()
    }
    /// PUTs that carry `If-Match`: the compare-and-swap of the head, which is the
    /// publish. The content-addressed pages and data ahead of it are create-only
    /// (`If-None-Match`), counted by `create_only_puts`.
    pub(super) fn cas_puts(&self) -> usize {
        self.requests
            .iter()
            .filter(|r| r.method == "PUT" && r.if_match)
            .count()
    }
    pub(super) fn create_only_puts(&self) -> usize {
        self.requests
            .iter()
            .filter(|r| r.method == "PUT" && r.if_none_match)
            .count()
    }
    pub(super) fn other(&self) -> usize {
        self.requests
            .iter()
            .filter(|r| !matches!(r.method.as_str(), "PUT" | "GET" | "HEAD" | "LIST"))
            .count()
    }
}
#[derive(Default)]
struct State {
    window: Window,
    in_flight: u64,
}
pub(super) struct CountingProxy {
    pub(super) endpoint: String,
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for CountingProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl CountingProxy {
    /// Listen on 127.0.0.1:0 and forward to `upstream` (`http://host:port`).
    pub(super) async fn start(upstream: &str) -> Self {
        let upstream = upstream
            .strip_prefix("http://")
            .expect("the proxy forwards plain http")
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(client, upstream.clone(), shared.clone()));
            }
        });
        Self {
            endpoint,
            state,
            task,
        }
    }
    /// What has been recorded since the last `reset`.
    pub(super) fn snapshot(&self) -> Window {
        self.state.lock().unwrap().window.clone()
    }
    /// Start a new window. Requests still in flight count toward it when they end,
    /// but not as a round trip.
    pub(super) fn reset(&self) {
        self.state.lock().unwrap().window = Window::default();
    }
}
/// Ends a request when dropped, which happens once its response body is done.
struct Flight {
    state: Arc<Mutex<State>>,
    method: String,
    uri: String,
    if_match: bool,
    if_none_match: bool,
    status: u16,
    start: Instant,
}
impl Flight {
    fn begin(state: &Arc<Mutex<State>>, head: &hyper::http::request::Parts) -> Self {
        let mut guard = state.lock().unwrap();
        if guard.in_flight == 0 {
            guard.window.round_trips += 1;
        }
        guard.in_flight += 1;
        Self {
            state: state.clone(),
            method: kind(&head.method, head.uri.path_and_query().map(|p| p.as_str())),
            uri: head
                .uri
                .path_and_query()
                .map(|p| p.as_str().to_owned())
                .unwrap_or_default(),
            if_match: head.headers.contains_key("if-match"),
            if_none_match: head.headers.contains_key("if-none-match"),
            status: 502,
            start: Instant::now(),
        }
    }
}
impl Drop for Flight {
    fn drop(&mut self) {
        let mut guard = self.state.lock().unwrap();
        guard.in_flight -= 1;
        guard.window.requests.push(Recorded {
            method: std::mem::take(&mut self.method),
            uri: std::mem::take(&mut self.uri),
            if_match: self.if_match,
            if_none_match: self.if_none_match,
            status: self.status,
            start: self.start,
            end: Instant::now(),
        });
    }
}
struct Counted {
    inner: Incoming,
    _flight: Flight,
}
impl Body for Counted {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}
async fn serve(client: TcpStream, upstream: String, state: Arc<Mutex<State>>) {
    let Ok(server) = TcpStream::connect(&upstream).await else {
        return;
    };
    let _ = server.set_nodelay(true);
    let _ = client.set_nodelay(true);
    let Ok((sender, connection)) =
        hyper::client::conn::http1::handshake(TokioIo::new(server)).await
    else {
        return;
    };
    tokio::spawn(connection);
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    let service = service_fn(move |request: Request<Incoming>| {
        let (sender, state) = (sender.clone(), state.clone());
        async move {
            let (head, body) = request.into_parts();
            let mut flight = Flight::begin(&state, &head);
            let uri = head
                .uri
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/")
                .to_owned();
            let mut forwarded = Request::builder().method(head.method).uri(uri);
            // Same headers, same order, `Host` included: they are signed.
            *forwarded.headers_mut().unwrap() = head.headers;
            let forwarded = forwarded.body(body).unwrap();
            let sent = {
                let mut sender = sender.lock().await;
                match sender.ready().await {
                    Ok(()) => sender.send_request(forwarded),
                    Err(_) => return Ok::<_, Infallible>(bad_gateway()),
                }
            };
            match sent.await {
                Ok(response) => {
                    let (parts, body) = response.into_parts();
                    flight.status = parts.status.as_u16();
                    Ok(Response::from_parts(
                        parts,
                        Counted {
                            inner: body,
                            _flight: flight,
                        }
                        .boxed(),
                    ))
                }
                Err(_) => Ok(bad_gateway()),
            }
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(client), service)
        .await;
}
/// A GET with `list-type` in its query, or of a bucket itself, is a listing.
fn kind(method: &hyper::Method, target: Option<&str>) -> String {
    let target = target.unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let bucket_level = path.trim_matches('/').split('/').count() <= 1;
    if method == hyper::Method::GET && (query.contains("list-type") || bucket_level) {
        "LIST".to_owned()
    } else {
        method.to_string()
    }
}
type Out = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;
fn bad_gateway() -> Response<Out> {
    Response::builder()
        .status(502)
        .body(
            http_body_util::Empty::<Bytes>::new()
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}
