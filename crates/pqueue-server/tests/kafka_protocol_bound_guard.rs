//! SECURITY REGRESSION GUARD (bead pqueue-301c32cc).
//!
//! `pqueue-server` boots the embedded Kafka-compatible surface (`heimq::server::Server` +
//! `fjord-broker`), so it decodes Kafka wire frames from untrusted clients.
//!
//! Upstream `kafka-protocol` (ALL versions 0.15–0.17) pre-allocates
//! `Vec::with_capacity(n)` from an attacker-controlled `Array`/`CompactArray` count field
//! BEFORE reading a single element — a remotely-triggerable OOM DoS (a small request with a
//! huge array-count causes a multi-GB allocation).
//!
//! heimq authored a bounded fork that clamps the pre-allocation to the remaining buffer:
//! `Vec::with_capacity((n as usize).min(buf.remaining()))`. heimq applies it via
//! `[patch.crates-io]` — but **cargo only honors `[patch]` from the top-level workspace root**,
//! so heimq's patch does NOT propagate to dependents. pqueue must carry the patch itself
//! (see the workspace root `Cargo.toml`).
//!
//! This guard asserts the lockfile actually resolves `kafka-protocol` to the bounded fork, so
//! a `cargo update`, a dependency bump, or an accidentally-dropped `[patch]` cannot silently
//! reintroduce the DoS.
//!
//! When heimq lands the bound on `kafka-protocol >= 0.17` (which also drops the unmaintained
//! `paste` dep, RUSTSEC-2024-0436), update the expected source here — see bead pqueue-ab54ecad.

use std::path::PathBuf;

/// The git fork carrying the bounded Array/CompactArray decode.
const BOUNDED_FORK_URL: &str = "git+https://github.com/easel/kafka-protocol-rs";
const BOUNDED_FORK_BRANCH: &str = "branch=heimq-bound-array-capacity";

fn workspace_lockfile() -> PathBuf {
    // crates/pqueue-server/ -> crates/ -> <workspace root>
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("Cargo.lock")
}

/// Extract the `source` line of the `[[package]]` block named `kafka-protocol`.
fn kafka_protocol_source(lock: &str) -> Option<String> {
    let mut in_block = false;
    for line in lock.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            in_block = false;
            continue;
        }
        if line == r#"name = "kafka-protocol""# {
            in_block = true;
            continue;
        }
        if in_block {
            if let Some(rest) = line.strip_prefix("source = ") {
                return Some(rest.trim_matches('"').to_string());
            }
            // `source` always precedes the next section within a package block; if we hit
            // `dependencies` first the package has no source (a path/workspace member).
            if line.starts_with("dependencies") {
                return None;
            }
        }
    }
    None
}

#[test]
fn kafka_protocol_resolves_to_the_bounded_fork_not_crates_io() {
    let lock_path = workspace_lockfile();
    let lock = std::fs::read_to_string(&lock_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", lock_path.display()));

    let source = kafka_protocol_source(&lock).expect(
        "kafka-protocol must appear in Cargo.lock with a `source` — pqueue-server decodes \
         untrusted Kafka frames and MUST use the bounded fork",
    );

    assert!(
        source.starts_with(BOUNDED_FORK_URL),
        "SECURITY REGRESSION: kafka-protocol resolves to `{source}`, not the bounded fork \
         `{BOUNDED_FORK_URL}`. Upstream kafka-protocol pre-allocates from an attacker-controlled \
         array count (remotely-triggerable OOM DoS). Restore the `[patch.crates-io]` entry in the \
         workspace root Cargo.toml. See bead pqueue-301c32cc."
    );
    assert!(
        source.contains(BOUNDED_FORK_BRANCH),
        "SECURITY REGRESSION: kafka-protocol resolves to `{source}`, which is not the \
         `heimq-bound-array-capacity` branch carrying the bounded Array/CompactArray decode."
    );
    assert!(
        !source.contains("registry+"),
        "SECURITY REGRESSION: kafka-protocol resolves to the crates.io registry (`{source}`), \
         which is UNBOUNDED in every published version (0.15-0.17)."
    );
}

#[test]
fn workspace_root_carries_the_kafka_protocol_patch() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("Cargo.toml");
    let manifest =
        std::fs::read_to_string(&root).unwrap_or_else(|e| panic!("read {}: {e}", root.display()));

    assert!(
        manifest.contains("[patch.crates-io]"),
        "the workspace root Cargo.toml must carry a `[patch.crates-io]` section — cargo only \
         honors `[patch]` at the top-level root, so heimq's bounded kafka-protocol fork does not \
         propagate to pqueue without it"
    );
    assert!(
        manifest.contains("kafka-protocol") && manifest.contains("heimq-bound-array-capacity"),
        "the workspace root `[patch.crates-io]` must pin kafka-protocol to the bounded fork \
         (easel/kafka-protocol-rs @ heimq-bound-array-capacity)"
    );
}
