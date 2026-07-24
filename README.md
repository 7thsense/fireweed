# Fireweed Queue

## Documentation

- [Operator microsite](docs/operator/index.html) is a static, openable
  first-screen console for install commands, storage backend choice, release
  artifact links, and production-readiness status.
- [Operator deployment guide](docs/deployment/operator-guide.md) covers
  `helm install`, upgrade, uninstall, values, log and projection storage axes,
  object-log storage, `kind` smoke tests, release
  artifacts, troubleshooting, and known production gaps.
- [Operator release artifacts](docs/deployment/operator-release-artifacts.md)
  states where to obtain published images, Helm chart packages, binary
  archives, checksums, and the commands to verify them before deployment.
- [Production deployment readiness](docs/helix/04-build/DEPLOYMENT-READINESS.md)
  defines the Helm, kind, storage-axis, and object-log release-readiness
  contract.
- [Container image and runtime config contract](docs/deployment/container-runtime-contract.md)
  defines the `fireweed-service` image entrypoint, environment/config keys, health
  endpoint/port, and storage backend settings consumed by Helm.
- [Choosing pqueue instead of a stream](docs/helix/01-frame/guides/choosing-pqueue.md)
  explains when to use pqueue's mutable-priority leased work queue model instead
  of an immutable sequential stream.
- [Scheduler and router boundary](docs/helix/01-frame/guides/scheduler-router-boundary.md)
  explains how to keep downstream capacity admission outside pqueue while using
  pqueue leases and `max_items` correctly.

## Release Artifacts

Fireweed Queue's first public-preview release is `v0.20.0`. The workspace
packages are release-synchronized at `0.20.0`. The Fireweed package, binary,
image, and chart names below are authoritative; old deployment coordinates are
not published as aliases. The Helm chart's source defaults use independent
versioning, while release packaging
overrides its chart and application versions to `0.20.0`.

The `v0.20.0` release provides:

- container image `ghcr.io/<owner>/fireweed-service:0.20.0` plus
  `ghcr.io/<owner>/fireweed-service:sha-<commit>`;
- Helm chart package `fireweed-queue-0.20.0.tgz`;
- binary archives `fireweed-0.20.0-<target-triple>.tar.gz`;
- `SHA256SUMS`;
- release evidence files `fireweed-service-image.txt` and
  `fireweed-queue-helm-chart.txt`.

Operators should download the GitHub Release assets, verify `SHA256SUMS`, and
compare the image tag digest against `fireweed-service-image.txt` before
deployment. See
[operator release artifacts](docs/deployment/operator-release-artifacts.md) for
the exact commands.

For local development, build and smoke-check the service image:

```sh
docker build -t fireweed-service:dev .
docker run --rm fireweed-service:dev --help
```

See the
[container runtime config contract](docs/deployment/container-runtime-contract.md)
for the full environment, health-probe, and storage backend contract.

## License

Fireweed Queue is licensed under either the
[Apache License, Version 2.0](LICENSE-APACHE) or the [MIT license](LICENSE-MIT),
at your option (`MIT OR Apache-2.0`). See [CONTRIBUTING.md](CONTRIBUTING.md) for
the project's issues-only contribution policy.

## Building from source

The workspace pins its toolchain in `rust-toolchain.toml` — **Rust 1.92.0**
(with `clippy` and `rustfmt`). With `rustup` installed, the pinned toolchain is
selected automatically; a clean build needs no extra system libraries:

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Notes:

- `cargo build --workspace` builds with the pinned 1.92.0 toolchain and requires
  **no libcurl/librdkafka** — the change-log surface produces in-process to the
  embedded fjord broker (ADR-014); the optional external-Kafka producer is a
  pure-Rust (`rskafka`) path behind the default-off `external-kafka` feature.
- Some transitive dependencies (via `heimq`) build native crypto through
  `aws-lc-sys`, which needs a C toolchain and **cmake** available on `PATH`.
- The workspace depends on the sibling projects `fjord`, `heimq`, and
  `object-log`; CI checks them out alongside the repo (see
  `.github/workflows/ci.yml`).
- Postgres and S3/object-log integration tests are env-gated
  (`PQUEUE_PG_TEST_URL`, `PQUEUE_S3_TEST_*`) and skip loudly when unset.
