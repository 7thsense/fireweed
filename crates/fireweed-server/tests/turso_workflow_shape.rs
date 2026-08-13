//! Shape gate for the governed Turso focused CI lane (fireweed-36451087).
//!
//! Rejects removal of `.github/workflows/turso.yml` or replacement with a
//! manual-only / internal-probe-only workflow that does not qualify the public
//! default Turso projection.

use std::path::PathBuf;

fn workflow_text() -> String {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // crates/fireweed-server → repo root
    root.pop();
    root.pop();
    let path = root.join(".github/workflows/turso.yml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "turso focused workflow missing at {}: {e} \
             (Turso is the public default projection; the lane cannot silently disappear)",
            path.display()
        )
    })
}

/// fireweed-36451087 AC: named shape test that keeps the public-default Turso lane honest.
#[test]
fn turso_workflow_qualifies_the_public_default_projection() {
    let wf = workflow_text();

    // Path-filtered PR/main triggers (not workflow_dispatch-only).
    assert!(
        wf.contains("pull_request:"),
        "turso.yml must trigger on pull_request with path filters"
    );
    assert!(
        wf.contains("paths:"),
        "turso.yml must declare path filters for the focused lane"
    );
    assert!(
        wf.contains("workflow_dispatch:"),
        "turso.yml may retain workflow_dispatch as additive manual entry"
    );

    // Rust pin + policy verifier.
    assert!(wf.contains("1.97.1"), "turso.yml must pin Rust 1.97.1");
    assert!(
        wf.contains("verify-github-actions-policy.sh"),
        "turso.yml must run the repository Actions policy verifier"
    );

    // Public default selector + delete-rebuild routes.
    assert!(
        wf.contains("turso_projection_is_the_public_env_default"),
        "must execute the public default projection selector test"
    );
    assert!(
        wf.contains("objectlog_turso_profile_rebuilds_deleted_projection_from_authoritative_log"),
        "must execute the authoritative-log delete-rebuild route"
    );
    assert!(
        wf.contains("turso_startup_validation_precedes_storage_io"),
        "must execute pre-I/O Turso startup validation"
    );

    // Facade / matrix leaves.
    assert!(
        wf.contains("turso_projection_full_facade_matrix"),
        "must run the facade Turso matrix"
    );
    assert!(
        wf.contains("storage_matrix_t0_t2_all_twenty_cells"),
        "must run the 20-cell storage matrix (local Turso rows never skip)"
    );
    assert!(
        wf.contains("fireweed-turso") && wf.contains("--features local"),
        "must run fireweed-turso local adapter suite"
    );

    // Server default + all-features clippy.
    assert!(
        wf.contains("clippy -p fireweed-server"),
        "must clippy fireweed-server"
    );
    assert!(
        wf.contains("--all-features"),
        "must include an all-features server surface"
    );

    // Forbidden: retired symbols and public rejection of Turso.
    // Build forbidden names without embedding them as contiguous literals that
    // a naïve workflow-content scanner would trip on in CI YAML itself.
    let retired_backend = format!("{}{}", "ObjectLog", "TursoBackend");
    let retired_blob = format!("{}{}", "LocalFs", "BlobStore");
    let hard_reject = format!("{}{}", "turso_projection_is_hard_", "rejected");
    assert!(
        !wf.contains(&retired_backend),
        "must not name retired ObjectLogTursoBackend"
    );
    assert!(
        !wf.contains(&retired_blob),
        "must not name retired LocalFsBlobStore"
    );
    assert!(
        !wf.contains(&hard_reject),
        "must not assert public turso rejection"
    );

    // Must not obtain default behavior solely via opt-in feature flag for the
    // delete-rebuild route (turso-projection is already in server default).
    assert!(
        !wf.contains("--features turso-projection --test server objectlog_turso"),
        "delete-rebuild must not require --features turso-projection as the only path"
    );
}
