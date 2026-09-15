//! End-to-end smoke for the `object-log` diagnostics / produce-consume CLI.
#![cfg(feature = "cli")]

use bytes::Bytes;
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer, PartitionKey,
};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_object-log"))
}

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.into())
}

#[tokio::test]
async fn cli_list_inspect_fetch_against_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(root));
    let seq = Arc::new(
        ManifestSequencer::open(Arc::clone(&blob), "_manifest/")
            .await
            .unwrap(),
    );
    let engine = LogEngine::new(
        Arc::clone(&blob),
        Arc::clone(&seq),
        FlushConfig {
            max_batches: 1,
            linger: Duration::from_secs(3600),
            budget: object_log::BudgetConfig {
                enabled: false,
                ..Default::default()
            },
            ..FlushConfig::default()
        },
        "log/",
    );
    engine
        .produce(
            pk("events-0"),
            Bytes::from_static(b"hello"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    drop(engine);
    drop(seq);

    let list = Command::new(bin())
        .args(["list", "--root"])
        .arg(root)
        .args(["--prefix", "log/"])
        .output()
        .expect("run list");
    assert!(
        list.status.success(),
        "list stderr={}",
        String::from_utf8_lossy(&list.stderr)
    );
    let list_out = String::from_utf8_lossy(&list.stdout);
    assert!(list_out.contains("log/"), "list stdout={list_out}");

    let inspect = Command::new(bin())
        .args(["inspect", "--root"])
        .arg(root)
        .args(["--manifest-prefix", "_manifest/", "--json", "--summary"])
        .output()
        .expect("run inspect");
    assert!(
        inspect.status.success(),
        "inspect stderr={}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    let body = String::from_utf8_lossy(&inspect.stdout);
    assert!(body.contains("events-0"), "inspect={body}");

    let fetch = Command::new(bin())
        .args(["fetch", "--root"])
        .arg(root)
        .args(["--partition", "events-0", "--text"])
        .output()
        .expect("run fetch");
    assert!(
        fetch.status.success(),
        "fetch stderr={}",
        String::from_utf8_lossy(&fetch.stderr)
    );
    assert!(
        String::from_utf8_lossy(&fetch.stdout).contains("hello"),
        "fetch={}",
        String::from_utf8_lossy(&fetch.stdout)
    );
}

#[test]
fn cli_produce_consume_lines_roundtrip_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let input = root.join("in.txt");
    std::fs::write(&input, "alpha\nbeta\ngamma\n").unwrap();

    let produce = Command::new(bin())
        .args(["produce", "--root"])
        .arg(root)
        .args([
            "--partition",
            "demo",
            "--lines",
            "--linger-ms",
            "0",
            "--acks",
        ])
        .arg(&input)
        .output()
        .expect("produce");
    assert!(
        produce.status.success(),
        "produce stderr={}",
        String::from_utf8_lossy(&produce.stderr)
    );
    let acks = String::from_utf8_lossy(&produce.stdout);
    assert_eq!(acks.lines().count(), 3, "acks={acks}");

    let consume = Command::new(bin())
        .args(["consume", "--root"])
        .arg(root)
        .args(["--partition", "demo", "--lines"])
        .output()
        .expect("consume");
    assert!(
        consume.status.success(),
        "consume stderr={}",
        String::from_utf8_lossy(&consume.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&consume.stdout),
        "alpha\nbeta\ngamma\n"
    );
}

#[test]
fn cli_roundtrip_memory_lines() {
    let out = Command::new(bin())
        .args([
            "roundtrip",
            "--memory",
            "--partition",
            "rt",
            "--lines",
            "--linger-ms",
            "0",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"one\ntwo\nthree\n")?;
            child.wait_with_output()
        })
        .expect("roundtrip");
    assert!(
        out.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "one\ntwo\nthree\n");
}

#[test]
fn cli_produce_files_consume_framed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let a = root.join("a.bin");
    let b = root.join("b.bin");
    std::fs::write(&a, b"hello").unwrap();
    std::fs::write(&b, b"world!").unwrap();

    let produce = Command::new(bin())
        .args(["produce", "--root"])
        .arg(root)
        .args(["--partition", "bin", "--linger-ms", "0"])
        .arg(&a)
        .arg(&b)
        .output()
        .expect("produce files");
    assert!(
        produce.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&produce.stderr)
    );

    let consume = Command::new(bin())
        .args(["consume", "--root"])
        .arg(root)
        .args(["--partition", "bin", "--framed"])
        .output()
        .expect("consume framed");
    assert!(
        consume.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&consume.stderr)
    );
    let bytes = consume.stdout;
    // two frames: len=5 "hello", len=6 "world!"
    assert_eq!(&bytes[0..8], &5u64.to_be_bytes());
    assert_eq!(&bytes[8..13], b"hello");
    assert_eq!(&bytes[13..21], &6u64.to_be_bytes());
    assert_eq!(&bytes[21..27], b"world!");
}
