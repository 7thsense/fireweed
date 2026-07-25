---
ddx:
  id: public-artifact-topology
  depends_on:
    - adr-023-pre-release-fireweed-namespace-cutover
    - adr-engine-enforced-coordination-and-encapsulated-library-surface
  status: accepted
---

# Fireweed public artifact topology

This document defines the target publication boundary for the first Fireweed
preview, `v0.20.0`. ADR-023 makes these Fireweed coordinates the only supported
coordinates across manifests, release automation, storage, and wire surfaces.

## Classification rules

- **publishable** packages are public registry products with a supported API.
- **repository-only** packages build shipped products or implement the public
  facade, but are not independently published or supported as APIs.
- **experimental** packages are evaluation surfaces with no compatibility
  promise and are not published.
- **private** packages are maintainer test, evidence, or release tools and are
  not product artifacts.

ADR-009 makes `fireweed` the only public Cargo package. In particular, a
runtime component can be public and supported as a binary or container without
its implementation package becoming a public Rust API.

## Workspace package inventory

The `Current package` column is the machine key consumed by
`scripts/verify-public-artifact-topology.sh`. Every package returned in the
root workspace by `cargo metadata --no-deps --format-version 1` must occur
exactly once between the inventory markers. After the core rename, current and
target package names are intentionally identical.

<!-- markdownlint-disable MD013 -->
<!-- workspace-package-inventory:start -->
| Current package | Target package | Class | Registry | Publish order | Feature policy | Rationale |
| --- | --- | --- | --- | ---: | --- | --- |
| fireweed | fireweed | publishable | crates.io | 1 | default = memory, SQLite, object log; minimal = no default features; supported focused builds = sqlite or objectlog; memory is development-only; postgres is deferred | The sole supported Rust facade and constructor surface. |
| fireweed-core | fireweed-core | repository-only | - | - | no public feature contract | Domain types are exposed only through the facade. |
| fireweed-engine | fireweed-engine | repository-only | - | - | no public feature contract | Raw ports and coordination internals must not become an external construction surface. |
| fireweed-projection | fireweed-projection | repository-only | - | - | no public feature contract | Shared projection implementation, supported only through shipped profiles. |
| fireweed-relational | fireweed-relational | repository-only | - | - | no public feature contract | Driver-neutral relational implementation shared by internal adapters. |
| fireweed-memory | fireweed-memory | repository-only | - | - | always built for the default facade; development-only durability | Reference adapter used by the facade, tests, and local evaluation. |
| fireweed-sqlite | fireweed-sqlite | repository-only | - | - | facade feature sqlite | Internal adapter for supported SQLite profiles. |
| fireweed-objectlog | fireweed-objectlog | repository-only | - | - | facade feature objectlog | Internal adapter for supported object-log profiles. |
| fireweed-postgres | fireweed-postgres | repository-only | - | - | facade feature postgres; tls implies postgres; both deferred | Wired adapter outside the preview support boundary. |
| fireweed-resp | fireweed-resp | repository-only | - | - | no independent features; shipped through the service | Supported protocol adapter, not a standalone Cargo API. |
| fireweed-server | fireweed-server | repository-only | - | - | default env-config; postgres, tls, external-kafka, and turso-projection remain opt-in | Composition package for the shipped service binary and container. |
| fireweed-turso | fireweed-turso | experimental | - | - | local only; no default features | Feature-gated evaluation adapter with no compatibility promise. |
| fireweed-conformance | fireweed-conformance | private | - | - | test-only | Maintainer backend contract suite. |
| fireweed-release | fireweed-release | private | - | - | release tooling only | Maintainer verification-ledger and evidence tools. |
| fireweed-loadgen | fireweed-loadgen | private | - | - | evidence workload only | In-cluster release-evidence generator, not an operator command. |
| fireweed-sim-support | fireweed-sim-support | private | - | - | test-only | Deterministic simulation support with no product API. |
<!-- workspace-package-inventory:end -->
<!-- markdownlint-enable MD013 -->

The independent `fireweed-bench` workspace and the Turso compatibility probe do
not appear in this table because they are not root workspace members. Their
target names are `fireweed-bench` and `fireweed-turso-compat-probe`; both are
private evidence tools and are never registry publications.

## Cargo publication order and gate

The only public registry operation is publishing `fireweed` to crates.io, so
its publication order is `1`. No other workspace package has a registry order.

The current `fireweed` manifest still depends on unversioned workspace paths.
Renaming those dependencies without making them registry-resolvable would make
a crates.io package unusable. Before the first publish, the separate packaging
implementation must make the uploaded `fireweed` package self-contained or
otherwise remove non-registry runtime dependencies, then prove both:

```sh
cargo package -p fireweed
cargo publish -p fireweed --dry-run
```

Publishing internal engine or adapter crates to work around that closure gate
is not allowed: it would violate ADR-009's encapsulation boundary.

The facade's minimum feature contract is:

- `--no-default-features` builds the engine-facing facade without an adapter;
- `--no-default-features --features sqlite` builds the supported embedded
  SQLite profile without object-log or Postgres dependencies;
- `--no-default-features --features objectlog` builds the supported object-log
  profile without SQLite or Postgres dependencies;
- default features provide memory, SQLite, and object-log constructors;
- `postgres` and server-only `tls`, `external-kafka`, and `turso-projection`
  remain outside the preview-supported facade contract.

## Non-Cargo release artifacts

These are the only supported release coordinates. ADR-023 forbids retired-name
aliases, compatibility binaries, and dual-published artifacts.

<!-- markdownlint-disable MD013 -->
| Artifact | Target coordinate | Built from | Publication and feature policy |
| --- | --- | --- | --- |
| Service container | `ghcr.io/<owner>/fireweed-service:0.20.0` and `:sha-<commit>` | `fireweed-service` plus the private ledger verifier | GHCR, Linux amd64; default service features, with a separately identified `tls` build when released. |
| Binary archive | `fireweed-0.20.0-x86_64-linux.tar.gz` | `fireweed-service`, `fireweed-verify-ledger` | GitHub Release; Linux amd64 is the preview target. |
| Helm chart | `fireweed-queue-0.20.0.tgz` | `charts/fireweed-queue` target path | GitHub Release; chart and application versions are overridden to `0.20.0` at packaging. |
| Checksums | `SHA256SUMS` | Every downloadable release asset | GitHub Release; generated again after the final artifact is added. |
| Image evidence | `fireweed-service-image.txt` | Immutable GHCR digest and both tags | GitHub Release; must identify the exact source commit. |
| Chart evidence | `fireweed-helm-chart.txt` | Packaged chart name, version, and digest | GitHub Release; must identify `v0.20.0`. |
| Source archive | GitHub-generated `v0.20.0` source archives | Tagged repository tree | GitHub Release; immutable audit identifiers may remain only where rewriting would falsify history. |
<!-- markdownlint-enable MD013 -->

`Dockerfile`, `Dockerfile.prebuilt`, `Dockerfile.e2`, `docker-compose.yml`, the
load generator, benchmark binaries, compatibility probe, and remaining
`fireweed-release` verifier binaries are build or evidence inputs. They are not
separate public artifacts unless a later topology decision promotes them.

## Validation

Run the topology guard from the repository root:

```sh
scripts/verify-public-artifact-topology.sh
```

It compares the marked table with Cargo metadata, rejects omissions, unknown
entries, duplicate current or target names, invalid classes, incomplete
publish policy, and any public package set other than the sole facade. Its
self-test proves both omission and duplicate detection:

```sh
scripts/verify-public-artifact-topology-test.sh
```
