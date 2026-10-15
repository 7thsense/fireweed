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
struct Proxy {
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
    fn new(endpoint: &str) -> Self {
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
                        let pending = gate.lock().unwrap().take();
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
    fn arm(
        &self,
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
                    release: resume
                })
                .is_none()
        );
        (started, release)
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
        log.capture(&current.namespace).await.unwrap();
        assert_eq!(admission.usage().unwrap(), baseline);
        let page = current
            .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
            .await
            .unwrap();
        assert_eq!(page.rows().next().unwrap().key(), b"a");
        let token = page.continuation().unwrap();
        let replay = current
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
