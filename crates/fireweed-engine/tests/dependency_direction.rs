//! Hexagonal dependency-direction gate (plan §1 + §6 DoD): the DOMAIN crates (`fireweed-core`,
//! `fireweed-engine`) must NOT depend on any adapter (driven or driving) or the composition root. Adapters
//! depend inward on the domain, never the reverse. This reads the manifests at compile time, so a
//! forbidden dependency edge fails this test.

/// The `[dependencies]` (+ build/dev) span of a Cargo manifest, as a single string to scan. Crude but
/// sufficient: we only need to detect whether an adapter crate name appears as a dependency.
fn dep_lines(manifest: &str) -> String {
    let mut out = String::new();
    let mut in_deps = false;
    for line in manifest.lines() {
        let t = line.trim_start();
        if t.starts_with('[') {
            in_deps = t.contains("dependencies");
            continue;
        }
        if in_deps {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// True if `manifest` declares a dependency on a crate named exactly `crate_name` (matches the
/// `crate_name = ...` or `crate_name.workspace` dependency-line forms, not a substring like
/// `fireweed-core` matching `pqueue`).
fn depends_on(manifest: &str, crate_name: &str) -> bool {
    dep_lines(manifest).lines().any(|line| {
        let l = line.trim_start();
        l.strip_prefix(crate_name)
            .is_some_and(|rest| rest.starts_with([' ', '=', '.']))
    })
}

const ADAPTERS: &[&str] = &[
    "fireweed-memory",
    "fireweed-sqlite",
    "fireweed-postgres",
    "fireweed-objectlog",
    "fireweed-resp",
    "fireweed-server",
    "pqueue", // the library facade (a driving adapter)
];

#[test]
fn domain_crates_do_not_depend_on_adapters() {
    let engine = include_str!("../Cargo.toml");
    let core = include_str!("../../fireweed-core/Cargo.toml");

    for adapter in ADAPTERS {
        assert!(
            !depends_on(engine, adapter),
            "fireweed-engine must not depend on adapter `{adapter}` (hexagonal direction: adapters depend \
             on the domain, never the reverse)"
        );
        assert!(
            !depends_on(core, adapter),
            "fireweed-core must not depend on adapter `{adapter}`"
        );
    }
    // The innermost crate (core) must not depend even on the engine.
    assert!(
        !depends_on(core, "fireweed-engine"),
        "fireweed-core is the innermost domain crate and must depend on nothing pqueue-*"
    );
}
