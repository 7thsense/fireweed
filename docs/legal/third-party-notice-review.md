# Third-party notice review

Review date: 2026-07-23

This review covers the dependencies resolved by `Cargo.lock` for Fireweed
Queue 0.19.6. It supports the licensing decision in
[ADR-021](../helix/02-design/adr/ADR-021-open-source-license-and-contribution-policy.md).

## Method

The review used `cargo metadata --format-version 1` to enumerate resolved
packages and their source directories, inspected package license metadata and
license files, and searched each package root for files named `NOTICE*`,
`ATTRIBUTION*`, and `THIRD-PARTY*`. The workspace's allowed dependency
licenses were also checked with `cargo deny check licenses`.

One resolved package ships notice material:

- `shuttle 0.8.1` includes `NOTICE` and `THIRD-PARTY`. It is a transitive
  dependency of `turso_core 0.7.0`, reached through the workspace's
  `pqueue-turso` package. The upstream notice identifies Amazon as Shuttle's
  copyright holder and identifies code derived from Tokio, Loom, and the Rust
  standard library.
- `webpki-roots 1.0.8` contains Mozilla root-certificate data derived from the
  Common CA Database under `CDLA-Permissive-2.0`. Section 2.1 requires the
  agreement text to accompany shared data. It is a runtime dependency of
  `pqueue-objectlog` and therefore of the server distribution.
- `cfg_block 0.1.1` uses an Apache-2.0 `license-file` rather than a manifest
  license expression. Its Trevor Gross copyright notice is preserved in
  `NOTICE`. `cargo deny` reports this packaging shape as `no-license-field`.

At the review date, `cargo deny check licenses` also reports
`CDLA-Permissive-2.0` as not yet present in the repository allowlist. That is a
policy-gate finding, not missing provenance: the exact license is identified
and reproduced in `NOTICE`. The allowlist must be reconciled before public
release.

No other resolved package root contained one of the searched notice or
attribution filenames. This is a point-in-time source audit, not a permanent
waiver: release preparation must repeat it after dependency changes and must
carry license or attribution files required by dependencies that are actually
redistributed.

## Decision

A repository `NOTICE` file is required because Fireweed Queue distributes
Shuttle and the `webpki-roots` data. The repository notice preserves Shuttle's
upstream notice, its Tokio, Loom, and Rust attributions, the `cfg_block`
copyright notice, and the full CDLA-Permissive-2.0 agreement required for
sharing the certificate data. There is no separate Fireweed-specific
attribution requirement; the project notice uses the collective wording
"Fireweed Queue contributors" as required by ADR-021.

Release archives, containers, and other redistributions that include the
affected dependency graph must include `NOTICE` alongside `LICENSE-MIT` and
`LICENSE-APACHE`. Maintainers must rerun this review when `Cargo.lock` changes.
