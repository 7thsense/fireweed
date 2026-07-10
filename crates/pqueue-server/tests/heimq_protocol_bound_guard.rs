//! SECURITY REGRESSION GUARD (beads pqueue-301c32cc, pqueue-ab54ecad).
//!
//! `pqueue-server` boots the embedded Kafka-compatible surface (`heimq::server::Server` +
//! `fjord-broker`), so it decodes Kafka wire frames from untrusted clients, and it encodes
//! change-record batches for the in-process fjord log append.
//!
//! Upstream `kafka-protocol` (all published versions) pre-allocates `Vec::with_capacity(n)` from an
//! attacker-controlled `Array`/`CompactArray` count field BEFORE reading any element — a
//! remotely-triggerable OOM DoS. heimq **vendored** a bounded fork as the `heimq-protocol` crate
//! (`Vec::with_capacity((n as usize).min(buf.remaining()))`), and pqueue + fjord now use it exclusively.
//! The upstream `kafka-protocol` crate must NOT appear in the dependency graph.
//!
//! This guard asserts, against the lockfile:
//!   1. the upstream `kafka-protocol` crate is absent (no `cargo update` / dep bump can silently
//!      reintroduce the unbounded decoder), and
//!   2. `heimq-protocol` is present and resolves from heimq's git source (the vendored bounded codec).
//!
//! Negative-verified: reintroducing an upstream `kafka-protocol` dependency, or replacing
//! `heimq-protocol` with the crates.io codec, fails this test.

use std::path::PathBuf;

fn workspace_lockfile() -> PathBuf {
    // crates/pqueue-server/ -> crates/ -> <workspace root>
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("Cargo.lock")
}

/// The `source` value of every `[[package]]` block whose `name` equals `target`
/// (empty string for a workspace/path member with no `source` line).
fn package_sources(lock: &str, target: &str) -> Vec<String> {
    let want = format!("name = \"{target}\"");
    let mut out = Vec::new();
    let mut in_block = false;
    let mut src = String::new();
    for line in lock.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            if in_block {
                out.push(std::mem::take(&mut src));
            }
            in_block = false;
            src.clear();
            continue;
        }
        if line == want {
            in_block = true;
            continue;
        }
        if in_block {
            if let Some(rest) = line.strip_prefix("source = ") {
                src = rest.trim_matches('"').to_string();
            }
        }
    }
    if in_block {
        out.push(src);
    }
    out
}

#[test]
fn upstream_kafka_protocol_is_absent_from_the_lock() {
    let lock_path = workspace_lockfile();
    let lock = std::fs::read_to_string(&lock_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", lock_path.display()));

    let kp = package_sources(&lock, "kafka-protocol");
    assert!(
        kp.is_empty(),
        "SECURITY REGRESSION: the upstream `kafka-protocol` crate is back in Cargo.lock ({kp:?}). \
         It is UNBOUNDED on Array/CompactArray decode (remotely-triggerable OOM DoS). The Kafka codec \
         MUST be provided exclusively by heimq's vendored `heimq-protocol`. See bead pqueue-301c32cc."
    );
}

#[test]
fn heimq_protocol_vendored_codec_is_present() {
    let lock_path = workspace_lockfile();
    let lock = std::fs::read_to_string(&lock_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", lock_path.display()));

    let hp = package_sources(&lock, "heimq-protocol");
    assert!(
        !hp.is_empty(),
        "the vendored bounded Kafka codec `heimq-protocol` must be present in Cargo.lock — it is what \
         encodes/decodes the fjord change-record and wire batches with a bounded pre-allocation"
    );
    assert!(
        hp.iter().all(|s| s.contains("github.com/easel/heimq")),
        "heimq-protocol must resolve from heimq's git source (the vendored bounded fork), not elsewhere: \
         {hp:?}"
    );
}
