//! Gate real SDK HTTP reads while retaining signed requests and provider data.
use super::*;
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
struct Gate {
    entered: tokio::sync::oneshot::Sender<Vec<u8>>,
    release: std::sync::mpsc::Receiver<()>,
    path_fragment: Option<&'static [u8]>,
}
struct JoinAll {
    children: Vec<thread::JoinHandle<()>>,
    failed: Arc<AtomicBool>,
    joined: Arc<AtomicUsize>,
}
impl Drop for JoinAll {
    fn drop(&mut self) {
        for child in self.children.drain(..) {
            if child.join().is_err() {
                self.failed.store(true, Ordering::Release);
            }
            self.joined.fetch_add(1, Ordering::Release);
        }
    }
}
fn register(socket: &TcpStream, sockets: &Mutex<Vec<TcpStream>>, stop: &AtomicBool) -> bool {
    let mut owned = sockets.lock().unwrap_or_else(|error| error.into_inner());
    if stop.load(Ordering::Acquire) {
        let _ = socket.shutdown(Shutdown::Both);
        return false;
    }
    owned.push(socket.try_clone().unwrap());
    true
}
pub(super) struct Proxy {
    endpoint: String,
    armed: Arc<Mutex<Option<Gate>>>,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    worker: Option<thread::JoinHandle<()>>,
    failed: Arc<AtomicBool>,
    joined: Arc<AtomicUsize>,
    spawned: Arc<AtomicUsize>,
}
impl Proxy {
    pub(super) fn new(endpoint: &str) -> Self {
        let upstream = endpoint
            .strip_prefix("http://")
            .expect("loopback HTTP fixture")
            .trim_end_matches('/')
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let armed: Arc<Mutex<Option<Gate>>> = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::<TcpStream>::new()));
        let (gate, stopping, owned) = (armed.clone(), stop.clone(), sockets.clone());
        let failed = Arc::new(AtomicBool::new(false));
        let joined = Arc::new(AtomicUsize::new(0));
        let spawned = Arc::new(AtomicUsize::new(0));
        let (failures, joins, count) = (failed.clone(), joined.clone(), spawned.clone());
        let worker = thread::spawn(move || {
            // This guard also joins every child if the accept worker unwinds.
            let mut children = JoinAll {
                children: Vec::new(),
                failed: failures,
                joined: joins,
            };
            while !stopping.load(Ordering::Acquire) {
                let (mut client, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("proxy accept: {error}"),
                };
                client
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                client
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                if !register(&client, &owned, &stopping) {
                    break;
                }
                let (gate, owned, stopping) = (gate.clone(), owned.clone(), stopping.clone());
                count.fetch_add(1, Ordering::Release);
                children.children.push(thread::spawn(move || {
                    let mut raw = Vec::new();
                    let mut chunk = [0; 4096];
                    let end = loop {
                        let count = match client.read(&mut chunk) {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        raw.extend_from_slice(&chunk[..count]);
                        assert!(raw.len() <= 65536, "bounded fixture header");
                        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            break end;
                        }
                    };
                    if raw.starts_with(b"GET ") || raw.starts_with(b"HEAD ") {
                        let pending = {
                            let mut armed = gate.lock().unwrap();
                            let line = raw[..end].split(|b| *b == b'\r').next().unwrap();
                            if armed.as_ref().is_some_and(|gate| {
                                gate.path_fragment.is_none_or(|fragment| {
                                    line.windows(fragment.len()).any(|bytes| bytes == fragment)
                                })
                            }) {
                                armed.take()
                            } else {
                                None
                            }
                        };
                        if let Some(gate) = pending {
                            let _ = gate
                                .entered
                                .send(raw[..end].split(|b| *b == b'\r').next().unwrap().to_vec());
                            if gate.release.recv_timeout(Duration::from_secs(20)).is_err() {
                                return;
                            }
                        }
                    }
                    let mut remote =
                        TcpStream::connect_timeout(&upstream, Duration::from_secs(5)).unwrap();
                    remote
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    remote
                        .set_write_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    if !register(&remote, &owned, &stopping) {
                        return;
                    }
                    // Connection is hop-by-hop and is not part of these SigV4
                    // signed headers. Keep Host/path/body bytes unchanged.
                    let header = std::str::from_utf8(&raw[..end]).unwrap();
                    let lower = header.to_ascii_lowercase();
                    if let Some((_, rest)) = lower.split_once("signedheaders=") {
                        let names = rest.split([',', ' ']).next().unwrap();
                        assert!(!names.split(';').any(|name| name == "connection"));
                    }
                    let mut forwarded = String::new();
                    for line in header.split("\r\n") {
                        if !line
                            .split_once(':')
                            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("connection"))
                        {
                            forwarded.push_str(line);
                            forwarded.push_str("\r\n");
                        }
                    }
                    forwarded.push_str("Connection: close\r\n\r\n");
                    if remote.write_all(forwarded.as_bytes()).is_err()
                        || remote.write_all(&raw[end + 4..]).is_err()
                    {
                        return;
                    }
                    let (mut response, mut destination) =
                        (remote.try_clone().unwrap(), client.try_clone().unwrap());
                    let reply = thread::spawn(move || {
                        let _ = std::io::copy(&mut response, &mut destination);
                        let _ = response.shutdown(Shutdown::Both);
                        let _ = destination.shutdown(Shutdown::Both);
                    });
                    let _ = std::io::copy(&mut client, &mut remote);
                    let _ = remote.shutdown(Shutdown::Both);
                    let _ = client.shutdown(Shutdown::Both);
                    reply.join().unwrap();
                }));
            }
            drop(children);
        });
        Self {
            endpoint,
            armed,
            stop,
            sockets,
            worker: Some(worker),
            failed,
            joined,
            spawned,
        }
    }
    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn arm(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<Vec<u8>>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.arm_matching(None)
    }
    pub(super) fn arm_matching(
        &self,
        path_fragment: Option<&'static [u8]>,
    ) -> (
        tokio::sync::oneshot::Receiver<Vec<u8>>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        assert!(
            self.armed
                .lock()
                .unwrap()
                .replace(Gate {
                    entered,
                    release: resume,
                    path_fragment,
                })
                .is_none()
        );
        (started, release)
    }
}

#[tokio::test]
async fn pending_s3_source_record_cancellation_retains_frame_credit_and_spent_work() {
    use object_log::BlobStore;
    for disabled in [false, true] {
        let env = crate::shared_s3_test_env();
        let proxy = Proxy::new(&env.endpoint);
        let prefix = format!(
            "pending-root-record/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut limits = options(1 << 20);
        limits.operations = 1;
        let log = Arc::new(
            QueueNativeLog::s3(
                QueueNativeS3Connection {
                    endpoint: &proxy.endpoint,
                    region: &env.region,
                    bucket: &env.bucket,
                    access_key: &env.access_key,
                    secret_key: &env.secret_key,
                },
                prefix.clone(),
                limits,
                QueueNativeS3TransportOptions {
                    disable_payload_signing: disabled,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (source, expected, req) = super::record::seeded(&log, &job, "queue").await;
        let source = Arc::new(source);
        let before = source.head();
        let budget = super::record::ledger(
            &job,
            2,
            2 * req.hydration_reads(),
            2 * req.hydration_bytes(),
        );
        let baseline = admission.usage().unwrap();
        let guard = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let pending = (
            baseline.0,
            baseline.1
                + 65536
                + guard
                + 4 * req.frame_bytes()
                + 4096
                + guard
                + ownership::physical_owner_bytes::<4>(),
        );
        // Skip index/identity reads and stop precisely at the full native frame.
        let (started, release) = proxy.arm_matching(Some(b"/coupled-data/"));
        let caller = {
            let (source, log, budget, job) =
                (source.clone(), log.clone(), budget.clone(), job.clone());
            tokio::spawn(async move {
                source
                    .read_source_record(&log, 0, super::record::record_limits(), &budget, &job)
                    .await
            })
        };
        let request = tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        assert!(request.starts_with(b"GET ") || request.starts_with(b"HEAD "));
        let line = std::str::from_utf8(&request).unwrap();
        let target = line
            .split_ascii_whitespace()
            .nth(1)
            .unwrap()
            .split('?')
            .next()
            .unwrap();
        assert!(
            target.starts_with(&format!("/{}/{prefix}/namespaces/candidates/", env.bucket)),
            "{line}"
        );
        let (_, digest) = target.split_once("/coupled-data/").unwrap();
        assert!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{line}"
        );
        assert_eq!(admission.usage().unwrap(), pending);
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (1, req.hydration_reads(), req.hydration_bytes())
        );
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            admission.usage().unwrap(),
            pending,
            "pending physical frame read retains metadata, whole-frame and owner-set credit"
        );
        release.send(()).unwrap();
        // Single-operation capacity is the physical-retirement barrier.
        assert_eq!(log.capture(&source.namespace).await.unwrap().head(), before);
        assert_eq!(admission.usage().unwrap(), baseline);
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (1, req.hydration_reads(), req.hydration_bytes()),
            "cancellation never refunds work"
        );
        let read = source
            .read_source_record(&log, 0, super::record::record_limits(), &budget, &job)
            .await
            .unwrap();
        assert_eq!(read.bytes(), expected.bytes());
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (left.semantic.pages, left.semantic.hydrated_bytes),
            (work().pages, work().hydrated_bytes)
        );
        drop((read, source, expected, budget));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
        proxy.close();
        let cleanup = object_log::S3BlobStore::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
        );
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}
impl Proxy {
    fn close_inner(&mut self) -> bool {
        let Some(worker) = self.worker.take() else {
            return true;
        };
        self.stop.store(true, Ordering::Release);
        self.armed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        for socket in self
            .sockets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
        {
            let _ = socket.shutdown(Shutdown::Both);
        }
        let worker_ok = worker.join().is_ok();
        worker_ok
            && !self.failed.load(Ordering::Acquire)
            && self.joined.load(Ordering::Acquire) == self.spawned.load(Ordering::Acquire)
    }
    fn close(mut self) {
        assert!(
            self.close_inner(),
            "owned proxy worker failed after all children joined"
        );
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        let clean = self.close_inner();
        if !thread::panicking() {
            assert!(clean, "owned proxy cleanup failed");
        }
    }
}
#[test]
fn proxy_join_guard_joins_remaining_children_after_the_first_panics() {
    let failed = Arc::new(AtomicBool::new(false));
    let joined = Arc::new(AtomicUsize::new(0));
    let second_done = Arc::new(AtomicBool::new(false));
    let done = second_done.clone();
    let children = JoinAll {
        children: vec![
            thread::spawn(|| panic!("injected first proxy child failure")),
            thread::spawn(move || done.store(true, Ordering::Release)),
        ],
        failed: failed.clone(),
        joined: joined.clone(),
    };
    drop(children);
    assert!(failed.load(Ordering::Acquire));
    assert_eq!(joined.load(Ordering::Acquire), 2);
    assert!(second_done.load(Ordering::Acquire));
}

#[tokio::test]
async fn pending_s3_scan_cancellation_keeps_input_output_credit_until_physical_completion() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let proxy = Proxy::new(&env.endpoint);
        let prefix = format!(
            "pending-root-scan/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut limits = options(1 << 20);
        limits.operations = 1;
        let log = Arc::new(
            QueueNativeLog::s3(
                QueueNativeS3Connection {
                    endpoint: &proxy.endpoint,
                    region: &env.region,
                    bucket: &env.bucket,
                    access_key: &env.access_key,
                    secret_key: &env.secret_key,
                },
                prefix.clone(),
                limits,
                QueueNativeS3TransportOptions {
                    disable_payload_signing: disabled,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (first, record) = bundle(&log, "queue", &job).await;
        let publication = first
            .prepare_with_edits(
                &log,
                &record,
                "seed",
                &[
                    QueueNativeSemanticEdit {
                        key: b"a",
                        value: Some(b"first"),
                    },
                    QueueNativeSemanticEdit {
                        key: b"b",
                        value: Some(b"second"),
                    },
                ],
                edits_limit(),
                &job,
            )
            .await
            .unwrap();
        publication.submit(&log).await.unwrap();
        let current = Arc::new(fresh(&log, &first, "queue", &job).await);
        let baseline = admission.usage().unwrap();
        let page = current
            .as_source()
            .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
            .await
            .unwrap();
        let input = page.continuation().unwrap();
        drop(page);
        let (started, release) = proxy.arm();
        let caller = {
            let (log, current, job) = (log.clone(), current.clone(), job.clone());
            tokio::spawn(async move {
                current
                    .as_source()
                    .semantic_scan_next(&log, &input, scan_limit(), &job)
                    .await
            })
        };
        let request = tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        assert!(request.starts_with(b"GET ") || request.starts_with(b"HEAD "));
        let line = std::str::from_utf8(&request).unwrap();
        let target = line.split_ascii_whitespace().nth(1).unwrap();
        assert!(target.starts_with(&format!("/{}/{prefix}/namespaces/candidates/", env.bucket)));
        assert!(
            target.contains("/pages/"),
            "gated immutable-page request: {line}"
        );
        let pending = admission.usage().unwrap();
        assert!(pending.1 > baseline.1);
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            admission.usage().unwrap(),
            pending,
            "pending HTTP owns all input/output credit"
        );
        release.send(()).unwrap();
        // A single SDK slot makes this an explicit physical-completion barrier.
        log.capture(&current.source.namespace).await.unwrap();
        assert_eq!(admission.usage().unwrap(), baseline);
        let page = current
            .as_source()
            .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
            .await
            .unwrap();
        assert_eq!(page.rows().next().unwrap().key(), b"a");
        let token = page.continuation().unwrap();
        let replay = current
            .as_source()
            .semantic_scan_next(&log, &token, scan_limit(), &job)
            .await
            .unwrap();
        assert_eq!(replay.rows().next().unwrap().key(), b"b");
        drop((page, token, replay, publication, current, first, record));
        log.shutdown().await.unwrap();
        drop(log);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        proxy.close();
        let cleanup = object_log::S3BlobStore::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
        );
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}

#[tokio::test]
async fn pending_s3_capture_cancellation_keeps_sole_namespace_and_identity_until_retirement() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let proxy = Proxy::new(&env.endpoint);
        let prefix = format!(
            "pending-root-capture/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut limits = options(1 << 20);
        limits.operations = 1;
        let log = Arc::new(
            QueueNativeLog::s3(
                QueueNativeS3Connection {
                    endpoint: &proxy.endpoint,
                    region: &env.region,
                    bucket: &env.bucket,
                    access_key: &env.access_key,
                    secret_key: &env.secret_key,
                },
                prefix.clone(),
                limits,
                QueueNativeS3TransportOptions {
                    disable_payload_signing: disabled,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (first, record) = bundle(&log, "queue", &job).await;
        let before = first.as_source().head();
        let baseline = admission.usage().unwrap();
        let namespace = first
            .as_source()
            .retained_namespace(
                &QueueAddress {
                    tenant: "tenant".into(),
                    queue: "queue".into(),
                },
                &job,
            )
            .unwrap();
        let namespace_bytes = namespace._charge.admitted_bytes();
        let identity_bytes = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let expected_pending = (
            baseline.0,
            baseline.1 + namespace_bytes + identity_bytes + ownership::physical_owner_bytes::<2>(),
        );
        let (started, release) = proxy.arm();
        // This newly retained namespace has no other Root owner in the caller.
        let caller = {
            let (log, job) = (log.clone(), job.clone());
            tokio::spawn(async move { log.capture_source(namespace, &job).await })
        };
        let request = tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        assert!(request.starts_with(b"GET ") || request.starts_with(b"HEAD "));
        let line = std::str::from_utf8(&request).unwrap();
        let target = line.split_ascii_whitespace().nth(1).unwrap();
        // capture_known first revalidates the immutable namespace binding,
        // before reading the candidate head. The gate stops that first read.
        let binding_prefix = format!("/{}/{prefix}/namespaces/bindings/", env.bucket);
        let binding = target
            .split('?')
            .next()
            .unwrap()
            .strip_prefix(&binding_prefix)
            .unwrap_or_else(|| panic!("gated binding request: {line}"));
        assert!(
            binding.len() == 64 && binding.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "gated binding request: {line}"
        );
        assert_eq!(admission.usage().unwrap(), expected_pending);
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            admission.usage().unwrap(),
            expected_pending,
            "blocked physical capture owns namespace, identity and owner-set credit"
        );
        release.send(()).unwrap();
        // The one SDK operation slot is an explicit physical-retirement barrier.
        let after = log.capture(&first.source.namespace).await.unwrap().head();
        assert_eq!(after, before, "capture must not fence or append");
        assert_eq!(admission.usage().unwrap(), baseline);
        drop((first, record));
        log.shutdown().await.unwrap();
        drop(log);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        proxy.close();
        let cleanup = object_log::S3BlobStore::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
        );
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}
