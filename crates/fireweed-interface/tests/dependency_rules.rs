//! API-006 crate rules 1 and 5, checked from the manifests.

fn dependencies(manifest: &str) -> Vec<String> {
    let mut in_deps = false;
    let mut out = vec![];
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_deps = line == "[dependencies]";
            continue;
        }
        if in_deps && let Some((name, _)) = line.split_once('=') {
            out.push(name.trim().to_owned());
        }
    }
    out
}

#[test]
fn interface_depends_on_no_fireweed_crate() {
    let deps = dependencies(include_str!("../Cargo.toml"));
    assert!(!deps.is_empty());
    let bad: Vec<_> = deps.iter().filter(|d| d.starts_with("fireweed")).collect();
    assert!(bad.is_empty(), "fireweed-interface depends on {bad:?}");
}

#[test]
fn conformance_depends_on_no_implementation() {
    let deps = dependencies(include_str!(
        "../../fireweed-interface-conformance/Cargo.toml"
    ));
    let bad: Vec<_> = deps
        .iter()
        .filter(|d| d.starts_with("fireweed") && *d != "fireweed-interface")
        .collect();
    assert!(
        bad.is_empty(),
        "fireweed-interface-conformance depends on {bad:?}"
    );
}
