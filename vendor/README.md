# Pinned Turso performance backports

The five Turso crates here derive from the published 0.7.2 sources. The core
performance backports are described in [their provenance](turso_core/FIREWEED.md).
The `turso` binding also retains the transaction rollback correction exercised
by Fireweed's failed-apply recovery tests. Its sync SDK is optional: ordinary
local projections do not need the remote synchronization dependency tree.

Manifests route dependencies through this bundle and remove dependencies unused
by the retained code. The SDK deliberately keeps `parking_lot` with `send_guard`:
that feature makes core guards usable in the SDK's Send futures even without a
direct SDK import. Core retains `antithesis_sdk` for generated assertion macros.
Each `FIREWEED.patch` is regenerated against its published crate archive;
independent Cargo lockfile refreshes and this documentation are tracked separately.
A root-only Cargo patch would disappear for downstream consumers.
Each crate retains its upstream metadata and includes the upstream MIT license.

`fireweed-turso` directly depends on these paths. No consumer-side Cargo override
or Snorri source change is required. These are excluded from Fireweed's workspace
member tests. Native checkpoint behavior is tested through the public Turso API
in `crates/fireweed-turso/tests/checkpoint_policy.rs`, and through Fireweed's
public workflow/recovery suites. Replace this bundle with a tested upstream
release once equivalent effective checkpoint configuration, retained-WAL retry,
and cache accounting fixes are available. Native tests are also run directly with:

```sh
cargo test --manifest-path vendor/turso_core/Cargo.toml --no-default-features --features fs,uuid --lib storage::page_cache::tests --target-dir target/core-cache-tests -- --test-threads=1
cargo test --manifest-path vendor/turso_core/Cargo.toml --no-default-features --features fs,uuid --lib storage::wal::test --target-dir target/core-cache-tests -- --test-threads=1
```
