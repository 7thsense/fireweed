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
3. **Rendered contract assertions** — checks that each profile renders only the
   expected runtime contract. The object-log profile must include object-store
   endpoint/bucket/region/segment values, Postgres and object-store Secret refs,
   SQLite projection path, and the projection PVC/volume mount. The
   postgres-native profile must not render object-log-only env vars, PVCs, or
   volume mounts. Both profiles reject local CI fixture credentials in rendered
   manifests, and chart defaults are checked for those credentials before any
   profile render.
4. **`kubeconform`** — validates the rendered manifests against the pinned
   Kubernetes API schema set (`-strict`, fails on unknown fields).

The gate performs **no** cluster operations. `kind` cluster creation and runtime
smoke tests are intentionally out of scope and live in a separate gate.

## CI values

The profile inputs are checked-in test values, merged over
`charts/pqueue/values.yaml`:

- `charts/pqueue/ci/postgres-native-values.yaml`
- `charts/pqueue/ci/object-log-sqlite-projection-values.yaml`

For `object_log_sqlite_projection`, the static gate preserves the runtime
surface consumed by `pqueue-service`:

| Helm value | Rendered runtime key or object |
|------------|--------------------------------|
| `backend.profile` | `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection` |
| `backend.postgres.existingSecret` | Secret reference `pqueue-postgres` |
| `backend.postgres.databaseUrlKey` | Secret key `database-url` for `PQUEUE_POSTGRES_DATABASE_URL` |
| `backend.objectLog.endpoint` | `PQUEUE_OBJECT_LOG_ENDPOINT=http://minio:9000` |
| `backend.objectLog.bucket` | `PQUEUE_OBJECT_LOG_BUCKET=pqueue-object-log` |
| `backend.objectLog.region` | `PQUEUE_OBJECT_LOG_REGION=us-east-1` |
| `backend.objectLog.segmentMaxCommands` | `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS=1024` |
| `backend.objectLog.existingSecret` | Secret reference `pqueue-object-log` |
| `backend.objectLog.accessKeyIdKey` | Secret key `access-key-id` for `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID` |
| `backend.objectLog.secretAccessKeyKey` | Secret key `secret-access-key` for `PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY` |
| `backend.sqliteProjection.mountPath` | `PQUEUE_SQLITE_PROJECTION_DIR=/var/lib/pqueue/projection` and matching volume mount |
| `backend.shardCount.min` / `backend.shardCount.max` | `PQUEUE_SHARD_COUNT_MIN` / `PQUEUE_SHARD_COUNT_MAX` |
| `persistence.enabled=true` | SQLite projection PVC and `sqlite-projection` volume |
| `persistence.existingClaim` | Existing SQLite projection PVC claim name override |
| `persistence.accessModes`, `persistence.size`, `persistence.storageClass` | SQLite projection PVC access modes, storage request, and optional class |

The static gate proves Helm renders those names and rejects fixture credentials
in rendered manifests. Runtime proof still belongs to the kind gate:
`bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection`.

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

Pair this static gate with the object-log runtime and kind proof commands:

```sh
cargo test -p pqueue-objectlog -- --nocapture
cargo test -p pqueue-service --test container_runtime_contract_tests -- --nocapture
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```
