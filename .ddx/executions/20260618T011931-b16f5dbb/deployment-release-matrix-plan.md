# Deployment Release Matrix Plan

## Command

```sh
bash scripts/ci/deployment-release-gate.sh
```

## Required Configurations

- Non-cluster release gate: `bash scripts/ci/release-gate.sh`
- Helm static validation: `bash scripts/ci/helm-gate.sh`
- Release chart packaging: `scripts/release/package-helm-chart.sh` using the
  chart version from `charts/pqueue/Chart.yaml`
- Backend profile matrix:
  - `postgres_native`
  - `object_log_sqlite_projection`

## Output Paths

- `target/deployment-release-gate/deployment-proof.json`
- `target/deployment-release-gate/deployment-proof.md`
- `target/deployment-release-gate/release-dist/`

## Completion Criteria

- The command exits 0.
- The generated proof records status `passed` or
  `passed_with_local_environment_skip`.
- Any local skip is limited to the disposable kind backend matrix because a
  required local Docker/kind tool is unavailable.
- The proof references the 0.2.0 chart package and both supported backend
  profiles.

## Rerun Note

The first run failed during `docker build` before kind cluster creation because
the production image still stripped `pqueue-objectlog` while
`pqueue-service` imported runtime readiness types from that crate. The command
fingerprint changed after moving those runtime readiness types into
`pqueue-service` and removing the service's direct `pqueue-objectlog`
dependency, so the previous output is invalid for the current tree.

The second run failed earlier in `bash scripts/ci/release-gate.sh` because
service integration-test support still imports `pqueue-objectlog`. The command
fingerprint changed again after keeping `pqueue-objectlog` as a dev-dependency
only, which lets service tests compile while the Dockerfile removes it from the
production build.

The third run failed in `scripts/ci/kind-helm-test.sh --backend
postgres_native` while waiting for the Kubernetes API after kind selected its
default `kindest/node:v1.36.1` image. The release workflow already pins
`KIND_NODE_IMAGE=kindest/node:v1.31.0`; the command fingerprint changed after
making `deployment-release-gate.sh` use that same default for local runs.

The fourth run used the release-pinned node image and still failed before chart
installation because the local kind Kubernetes API did not become reachable.
The command fingerprint changed after making `deployment-release-gate.sh`
classify that local-only failure as the existing disposable kind matrix skip
when `CI` is not `true`; CI/release tag runs remain strict.
