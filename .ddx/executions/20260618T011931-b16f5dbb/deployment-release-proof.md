# Deployment Release Proof

## Local Gate

- command: `bash scripts/ci/deployment-release-gate.sh`
- exit status: `0`
- status: `passed_with_local_environment_skip`
- chart package: `target/deployment-release-gate/release-dist/pqueue-0.2.3.tgz`
- chart evidence: `target/deployment-release-gate/release-dist/pqueue-helm-chart.txt`
- checksums: `target/deployment-release-gate/release-dist/SHA256SUMS`

The local skip was limited to the disposable kind backend matrix because the
local kind Kubernetes API did not become reachable. CI tag runs remain strict
and must prove both `postgres_native` and `object_log_sqlite_projection`.

The deployment gate intentionally reports image evidence as unavailable before
the release workflow publishes the container image. Final image coordinates and
digest are produced by `scripts/release/write-container-image-evidence.sh` after
the release image is pushed.

## Release-Readiness Children

`ddx bead list --where parent=pqueue-3546063c --no-update-check` showed every
deployment-readiness child closed except the current release bead
`pqueue-90b11cab`.

Closed children:

- `pqueue-0f1e8ba6` - deployment docs and microsite
- `pqueue-3c600abe` - postgres_native kind proof
- `pqueue-4c08f11b` - Helm chart skeleton and backend values schema
- `pqueue-4d588c37` - deployment release proof gate and CI matrix
- `pqueue-611b6645` - container image and runtime config contract
- `pqueue-728222f1` - GitHub Actions image and chart publication
- `pqueue-881aeea8` - production deployment readiness contract
- `pqueue-c3dd271c` - S3-compatible object-log runtime adapter and MinIO kind proof
- `pqueue-c4a93050` - kind Helm integration harness
- `pqueue-ce2ade85` - object_log_sqlite_projection kind proof
- `pqueue-d923bbdf` - Helm static validation gate
