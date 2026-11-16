# Fireweed Queue

Fireweed Queue is a priority work queue for Rust applications,
with leased delivery, retries, scheduling, and durable storage compositions.

## Overview

Use Fireweed when work must be selected by priority or eligibility rather than
read once in append order. Workers claim a bounded batch under a lease, then
complete, retry, release, or fail its items. Queue definitions can add delayed
eligibility, gates, groups, cohorts, typed fields, and secondary indexes.

Fireweed is an embedded Rust library. The `Queue` API
([API-007](docs/helix/02-design/contracts/API-007-queue-claim-commit-contract.md))
in the `fireweed` crate is the only public entry point: `Queue::postgres`,
`Queue::s3_turso`, and `Queue::ephemeral_disk`.

## Status

Fireweed Queue `v0.32.0` is a source preview. Fireweed names are authoritative
across packages, configuration, and storage identifiers. The
[namespace policy](docs/helix/02-design/adr/ADR-023-pre-release-fireweed-namespace-cutover.md)
defines this one-way pre-release cutover.

The preview includes the embedded Rust API, local development backends, and
documented object-log/Postgres compositions. Public
preview does not mean that every compiled storage pairing is production-ready.
Before relying on it, review the
[v0.22.0 public-preview checklist](docs/helix/05-deploy/public-preview-checklist.md),
run the applicable smoke and recovery checks, and plan capacity, credentials,
monitoring, backups, and failure recovery for your environment. The ephemeral
on-disk queue used below is disposable and development-only.

## Quickstart

### Prerequisites

- Git;
- [rustup](https://rustup.rs/) (the repository selects Rust 1.99.0 from
  `rust-toolchain.toml`);
- a C toolchain and CMake for native crypto dependencies.

Clone the repository and run the embedded lifecycle example:

```sh
git clone https://github.com/7thsense/fireweed.git
cd fireweed
cargo run -p fireweed --example queue_lifecycle
```

`Queue::ephemeral_disk` is a throwaway development queue; see the `fireweed`
crate documentation (`cargo doc -p fireweed --no-deps --open`) for construction
and the claim/commit lifecycle.

## Architecture

Fireweed separates queue semantics from storage:

| Layer | Responsibility |
| --- | --- |
| Rust library | Embedded queue API |
| Engine | Queue validation, leases, lifecycle transitions, idempotency, and fencing |
| Control plane | Queue definitions, ownership, and multi-replica coordination |
| Command log | Authoritative accepted history |
| Projection | Rebuildable item, eligibility, lease, and query state |

The public storage cell is an object-log published to S3, a Turso projection,
and async log acknowledgement. The log engine lives in `fireweed-objectlog`:
filesystem publication is its local adapter, and S3 publication is the public
adapter. Development and tests use `Queue::ephemeral_disk`. There is no second public
projection and no storage matrix.

## Documentation

- [Product microsite](docs/site/index.html) — openable marketing, concepts,
  examples from real tests, and API guides.
  Brand voice and visual system: [docs/site/DESIGN.md](docs/site/DESIGN.md).
  Deployed site: <https://7thsense.github.io/fireweed/> (GitHub Pages).
- [Choosing a priority queue instead of a stream](docs/helix/01-frame/guides/choosing-fireweed.md)
  explains the workload boundary.
- [Queue API source](crates/fireweed/src/queue.rs) covers construction and the
  ten queue operations.
- [Embedded lifecycle example](crates/fireweed/examples/queue_lifecycle.rs)
  provisions a queue, inserts work, claims by priority, commits outcomes, and
  schedules retries. The production open is S3 × Turso.
- [v0.32.0 release notes](docs/releases/v0.32.0.md) make `Queue` the only public
  API, remove the server, RESP and image packaging, and refuse old persisted data.
- [v0.31.32 release notes](docs/releases/v0.31.32.md) move axon-esf to
  v0.4.2, fjord to v0.1.5, and base64 to 0.23.
- [v0.31.31 release notes](docs/releases/v0.31.31.md) record the single
  public cell: S3 object-log × Turso × async acknowledgement.
- [v0.31.30 release notes](docs/releases/v0.31.30.md) cover the s3 × Turso
  capacity path, disk MinIO, and log-only projection rebuild.
- [v0.30.0 identity note](docs/releases/v0.30.0.md) reserves package identity
  `0.30.0` ahead of candidate-source freeze (not a cut tag by itself).
- [v0.29.2 release notes](docs/releases/v0.29.2.md) (historical) describe Snorri
  validate-before-apply fixes and E3 TP-003 emitter scaffold.
- [v0.23.2 release notes](docs/releases/v0.23.2.md) (historical) describe an earlier
  public 5×3 storage matrix. Current support is the single S3 × Turso cell in ADR-024.
- [v0.23.0 release notes](docs/releases/v0.23.0.md) (historical) describe the native-S3
  authority cutover and provider-neutral E3 runner. Current S3 publication authority is
  NativeConditionalWrite only; provider brands are not product SKUs.
- [v0.22.0 release notes](docs/releases/v0.22.0.md) describe request-id push
  Fresh/Replayed disposition for Snorri create/enqueue counters.
- [v0.21.0 release notes](docs/releases/v0.21.0.md) describe a
  superseded construction surface (historical).
- [v0.20.0 release notes](docs/releases/v0.20.0.md) record the Fireweed rename.

For source development, the standard local gates are:

Cargo defaults to 4 build jobs through [`.cargo/config.toml`](.cargo/config.toml)
to prevent compile and link storms on high-core-count machines. On GNU Linux,
the tracked linker wrapper uses PATH-resolved `clang` with `mold` when both are
available and falls back to the PATH-resolved system `cc` linker otherwise, so
ordinary CI does not require a Homebrew installation. Set `CARGO_BUILD_JOBS`
for a one-off narrower or wider build, for example
`CARGO_BUILD_JOBS=2 cargo test --workspace`.

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Some Postgres, S3, Kubernetes, and release-evidence checks need explicitly
provisioned services; their linked guides name the required environment.

## Contributing

Issues are welcome for bugs, feature requests, documentation problems, usage
questions, and interoperability reports. Search existing documentation and issues
first, then include the Fireweed version, storage configuration, expected
result, actual result, and a minimal reproduction.

Pull requests, patches, and other code contributions are not accepted. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the issues-only policy and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for project discussions.

## Security

Do not report vulnerabilities, exploits, secrets, or sensitive logs in a public
issue. Follow [SECURITY.md](SECURITY.md) to use GitHub private vulnerability
reporting. Security response is best-effort and does not include a guaranteed
timeline or bug bounty.

## Support

Support is provided through public issues on a best-effort basis, without an
SLA, guaranteed response, or promise of a fix. Read [SUPPORT.md](SUPPORT.md) for
the reporting checklist and support boundary. Security reports must use the
private channel above.

## License

Fireweed Queue is available under either the
[Apache License, Version 2.0](LICENSE-APACHE) or the [MIT license](LICENSE-MIT),
at your option (`MIT OR Apache-2.0`).
