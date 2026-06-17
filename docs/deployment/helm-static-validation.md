# Helm static validation gate

`scripts/ci/helm-gate.sh` is the deterministic static validation gate for the
pqueue Helm chart (`charts/pqueue`). It runs the cheap, cluster-free checks that
must pass before the expensive `kind` install smoke tests are attempted, so that
chart schema, template, and Kubernetes API errors are caught early in both local
development and GitHub Actions.

## What it does

For each supported backend profile (`postgres_native` and
`object_log_sqlite_projection`) the gate:

1. **`helm lint --strict`** — validates the chart and the merged values against
   `charts/pqueue/values.schema.json`.
2. **`helm template`** — renders the chart to manifests using the profile's CI
   values file.
3. **`kubeconform`** — validates the rendered manifests against the pinned
   Kubernetes API schema set (`-strict`, fails on unknown fields).

The gate performs **no** cluster operations. `kind` cluster creation and runtime
smoke tests are intentionally out of scope and live in a separate gate.

## CI values

The profile inputs are checked-in test values, merged over
`charts/pqueue/values.yaml`:

- `charts/pqueue/ci/postgres-native-values.yaml`
- `charts/pqueue/ci/object-log-sqlite-projection-values.yaml`

## Prerequisites

- **helm** (v3.8+ or v4) on `PATH`.
- **kubeconform** — used to schema-validate rendered manifests. If it is not
  already on `PATH`, the gate auto-installs the pinned version (see below).
- **curl** and **tar** — only required when kubeconform must be auto-installed.
- **Network access to github.com** — for the kubeconform binary download (when
  auto-installing) and for fetching the Kubernetes JSON schemas kubeconform
  validates against.

## Pinned versions

The validator version is pinned in `scripts/ci/helm-gate.sh`:

- `KUBECONFORM_VERSION` — currently `v0.6.7`. When kubeconform is auto-installed,
  the release tarball is downloaded and its SHA-256 is verified against the
  checksum table embedded in the script (covering linux/darwin × amd64/arm64).
  An already-installed `kubeconform` on `PATH` is used as-is and is *not*
  version-checked, so local toolchains are respected.
- `KUBERNETES_VERSION` — currently `1.31.0`. Rendered manifests are validated
  against this Kubernetes API schema set.

To bump kubeconform, change `KUBECONFORM_VERSION` and update the
`KUBECONFORM_SHA256` table from the corresponding release `CHECKSUMS` file.

Auto-installed binaries are cached under `target/helm-gate/bin/` (git-ignored),
so repeated local runs do not re-download.

## Running

```sh
bash scripts/ci/helm-gate.sh
```

The script exits non-zero on the first lint, template, or schema-validation
failure.
