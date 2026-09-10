# Pinned Turso checkpoint-control backport

The five crates here are the published Turso 0.7.2 sources. Only `turso_core`
contains behavior changes; see [its patch and provenance](turso_core/FIREWEED.md).
The four binding/support crates change only their dependency paths, so the same
core reaches Fireweed when consumed from another workspace. A root-only Cargo
patch would silently disappear for downstream users, which is not acceptable.
Each crate retains its upstream metadata and includes the upstream MIT license.

`fireweed-turso` directly depends on these paths. No consumer-side Cargo override
or Snorri source change is required. These are excluded from Fireweed's workspace
member tests. Native checkpoint behavior is tested through the public Turso API
in `crates/fireweed-turso/tests/checkpoint_policy.rs`, and through Fireweed's
public workflow/recovery suites. Replace this bundle with a tested upstream
release once equivalent effective checkpoint configuration is available.
