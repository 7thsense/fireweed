# Deployment Release Proof

- status: `passed_with_local_environment_skip`
- exit status: `0`
- commit: `243c1f3d6da49d2e7ac10f060368c4cf85edcf3e`
- chart: `pqueue` `0.2.0`
- image tag: `unavailable`
- image digest: `unavailable`

## Commands

- `bash scripts/ci/release-gate.sh` -> `0`
- `bash scripts/ci/helm-gate.sh` -> `0`
- `bash scripts/release/package-helm-chart.sh --version 0.2.0 --destination /Users/erik/Projects/.ddx-exec-wt/.execute-bead-wt-pqueue-90b11cab-20260618T011931-b16f5dbb/target/deployment-release-gate/release-dist --chart-dir charts/pqueue` -> `0`
- `validate docs/microsite` -> `0`
- `bash scripts/ci/kind-helm-test.sh --backend postgres_native` -> `1`

## Backend Profiles

- `postgres_native`: `skipped_local_environment` (local kind Kubernetes API did not become reachable)
- `object_log_sqlite_projection`: `skipped_local_environment` (local kind Kubernetes API did not become reachable)

## Supporting Artifacts

- `target/deployment-release-gate/package-helm-chart.out`: chart packaging command output (present)
- `target/deployment-release-gate/release-dist/pqueue-0.2.0.tgz`: Helm chart package (present)
- `target/deployment-release-gate/release-dist/pqueue-helm-chart.txt`: Helm chart evidence (present)
- `target/deployment-release-gate/release-dist/SHA256SUMS`: release distribution checksums (present)
- `target/deployment-release-gate/kind-postgres_native.out`: kind Helm test output for postgres_native (present)

## Local Environment Skip

The local skip applies only to the kind backend matrix; CI matrix proof still requires successful kind runs.
- local kind Kubernetes API did not become reachable
