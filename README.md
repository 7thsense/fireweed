# pqueue

## Documentation

- [Operator microsite](docs/operator/index.html) is a static, openable
  first-screen console for install commands, backend profile choice, release
  artifact links, and production-readiness status.
- [Operator deployment guide](docs/deployment/operator-guide.md) covers
  `helm install`, upgrade, uninstall, values, `postgres_native`,
  `object_log_sqlite_projection`, S3/MinIO, `kind` smoke tests, release
  artifacts, troubleshooting, and known production gaps.
- [Operator release artifacts](docs/deployment/operator-release-artifacts.md)
  states where to obtain published images, Helm chart packages, binary
  archives, checksums, and the commands to verify them before deployment.
- [Production deployment readiness](docs/helix/04-build/DEPLOYMENT-READINESS.md)
  defines the Helm, kind, `postgres_native`,
  `object_log_sqlite_projection`, MinIO, and S3/object-log release-readiness
  contract.
- [Container image and runtime config contract](docs/deployment/container-runtime-contract.md)
  defines the `pqueue-service` image entrypoint, environment/config keys, health
  endpoint/port, and backend-profile settings consumed by Helm.

## Release Artifacts

Published releases provide:

- container image `ghcr.io/<owner>/pqueue-service:<version>` plus
  `ghcr.io/<owner>/pqueue-service:sha-<commit>`;
- Helm chart package `pqueue-<version>.tgz`;
- binary archives `pqueue-<version>-<target-triple>.tar.gz`;
- `SHA256SUMS`;
- release evidence files `pqueue-service-image.txt` and
  `pqueue-helm-chart.txt`.

Operators should download the GitHub Release assets, verify `SHA256SUMS`, and
compare the image tag digest against `pqueue-service-image.txt` before
deployment. See
[operator release artifacts](docs/deployment/operator-release-artifacts.md) for
the exact commands.

For local development, build and smoke-check the service image:

```sh
docker build -t pqueue:dev .
docker run --rm pqueue:dev --help
```

See the
[container runtime config contract](docs/deployment/container-runtime-contract.md)
for the full environment, health-probe, and backend-profile contract.
