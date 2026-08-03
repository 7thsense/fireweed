# Fireweed Queue

Fireweed Queue is a priority work queue for Rust applications and Redis clients,
with leased delivery, retries, scheduling, and durable storage compositions.

## Overview

Use Fireweed when work must be selected by priority or eligibility rather than
read once in append order. Workers claim a bounded batch under a lease, then
complete, retry, release, or fail its items. Queue definitions can add delayed
eligibility, gates, groups, cohorts, typed fields, and secondary indexes.

Fireweed has two public entry points:

- the `fireweed` Rust library facade for embedding a queue in an application;
- the `fireweed-service` RESP server, which supports the tested Redis Streams
  worker path (`XADD`, `XREADGROUP`, `XACK`, and related commands).

## Status

Fireweed Queue `v0.22.0` is a public preview. Fireweed names are authoritative
across packages, binaries, configuration, storage identifiers, wire extensions,
container images, and the Helm chart. The
[namespace policy](docs/helix/02-design/adr/ADR-023-pre-release-fireweed-namespace-cutover.md)
defines this one-way pre-release cutover.

The preview includes the embedded Rust API, the RESP service, local development
backends, and documented object-log/Postgres deployment compositions. Public
preview does not mean that every compiled storage pairing is production-ready.
Before deploying, review the
[deployment readiness contract](docs/helix/04-build/DEPLOYMENT-READINESS.md),
the [v0.22.0 public-preview checklist](docs/helix/05-deploy/public-preview-checklist.md),
run the applicable smoke and recovery checks, and plan capacity, credentials,
monitoring, backups, and failure recovery for your environment. The memory
configuration used below is disposable and development-only.

## Quickstart

### Prerequisites

- Git;
- [rustup](https://rustup.rs/) (the repository selects Rust 1.92.0 from
  `rust-toolchain.toml`);
- a C toolchain and CMake for native crypto dependencies; and
- `redis-cli` for this RESP example.

Clone the repository. In the first terminal, start a single-process,
in-memory service with the default example queue made explicit:

```sh
git clone https://github.com/7thsense/fireweed.git
cd fireweed

FIREWEED_LISTEN_ADDR=127.0.0.1:8080 \
FIREWEED_LOG_BACKEND=memory \
FIREWEED_PROJECTION_BACKEND=memory \
FIREWEED_BOOTSTRAP_QUEUES=t1:q1 \
cargo run -p fireweed-server --bin fireweed-service
```

The service prints `fireweed-service 0.22.0 listening on 127.0.0.1:8080` after
the queue is ready. In a second terminal, push, claim, and complete one item:

```sh
redis-cli -p 8080 PING

ITEM_ID="$(redis-cli --raw -p 8080 \
  XADD t1:q1 '*' priority 10 payload send-email)"
printf 'pushed %s\n' "$ITEM_ID"

redis-cli --raw -p 8080 \
  XREADGROUP GROUP workers worker-1 COUNT 1 STREAMS t1:q1 '>'

redis-cli -p 8080 XACK t1:q1 workers "$ITEM_ID"
redis-cli -p 8080 XLEN t1:q1
```

`PING` returns `PONG`; `XREADGROUP` returns the same item ID and its fields;
`XACK` returns `1`; and the final `XLEN` returns `0`. Stop the development
service with `Ctrl-C`.

For fuller **Python RESP** queue-management examples (documented scenarios + e2e
evidence, optional performance suite), see
[`examples/python-resp/`](examples/python-resp/README.md):

```sh
./examples/python-resp/scripts/start_dev_service.sh   # terminal 1; bootstrap demo:work
cd examples/python-resp && python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python run_e2e.py                    # functional
PERF_N=10000 python run_perf.py      # performance smoke
```

To smoke-test the Fireweed binary without starting a listener:

```sh
cargo run -p fireweed-server --bin fireweed-service -- --help
```

## Architecture

Fireweed separates queue semantics from storage and transport:

| Layer | Responsibility |
| --- | --- |
| Rust facade / RESP front | Embedded API or Redis-compatible worker commands |
| Engine | Queue validation, leases, lifecycle transitions, idempotency, and fencing |
| Control plane | Queue definitions, ownership, and multi-replica coordination |
| Command log | Authoritative accepted history |
| Projection | Rebuildable item, eligibility, lease, and query state |

Log and projection storage are independent axes. Local development can use
memory or SQLite. Durable deployments can compose an object log or Postgres log
with the projections documented in the operator guide. Unsupported pairings
fail at startup instead of silently selecting another backend.

## Documentation

- [Product microsite](docs/site/index.html) — openable marketing, concepts,
  examples from real tests, API guides, and the operator deploy console.
  Brand voice and visual system: [docs/site/DESIGN.md](docs/site/DESIGN.md).
  Deployed site: <https://7thsense.github.io/fireweed/> (GitHub Pages).
- [Choosing a priority queue instead of a stream](docs/helix/01-frame/guides/choosing-fireweed.md)
  explains the workload boundary.
- [Rust facade source and crate documentation](crates/fireweed/src/lib.rs)
  covers embedded construction and worker lifecycle verbs.
- [Embedded workflow example](crates/fireweed/examples/scheduler_boundary.rs)
  composes queue templates, grouped discovery, stateless dispersion, bounded
  multi-queue claims, and worker finalization over durable relational SQLite.
- [Container runtime contract](docs/deployment/container-runtime-contract.md)
  lists runtime settings and storage profiles.
- [Operator deployment guide](docs/deployment/operator-guide.md) covers Helm,
  storage axes, upgrades, and verification.
- [Release artifact verification](docs/deployment/operator-release-artifacts.md)
  covers images, charts, archives, and checksums.
- [Operator deploy console](docs/site/deploy/index.html) (also linked from the
  legacy [docs/operator](docs/operator/index.html) shim).
- [v0.29.2 release notes](docs/releases/v0.29.2.md) describe Snorri validate-before-apply
  fixes, Garage authority matrix, and E3 TP-003 emitter scaffold.
- [v0.23.2 release notes](docs/releases/v0.23.2.md) describe the completed
  public 5×3 storage matrix.
- [v0.23.0 release notes](docs/releases/v0.23.0.md) describe the native-S3
  authority cutover and provider-neutral E3 runner.
- [v0.22.0 release notes](docs/releases/v0.22.0.md) describe request-id push
  Fresh/Replayed disposition for Snorri create/enqueue counters.
- [v0.21.0 release notes](docs/releases/v0.21.0.md) describe the complete
  backend-opaque facade and durability matrix.
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
