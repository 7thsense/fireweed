# Mandatory S3-compatible CI job requirements

Governing bar: [`docs/helix/04-build/storage-matrix-completion-brief.md`](../../docs/helix/04-build/storage-matrix-completion-brief.md) §2
(“required jobs for product-claimed cells **must not skip** when fixtures are missing”).

Public matrix cells with log axis `s3` are Class A product cells:

| Cell | T0 construct (no network) | T1–T3 live lifecycle | T4 Helm |
|------|---------------------------|----------------------|---------|
| `s3×memory` | always | when S3 fixture present | `charts/fireweed-queue/ci/s3-memory-values.yaml` |
| `s3×sqlite` | always | when S3 fixture present | `charts/fireweed-queue/ci/s3-sqlite-values.yaml` |
| `s3×postgres` | always (spec + composition root) | when S3 **and** Postgres fixtures present | `charts/fireweed-queue/ci/s3-postgres-values.yaml` |

## Required job shape

A **required** storage-matrix / product CI job that claims the s3 axis **must**:

1. **Provision an S3-compatible service** before tests (MinIO, Garage, or cloud S3).
   - Disposable MinIO via docker is acceptable for unit/integration lanes
     (see `crates/fireweed-server/tests/production_s3_object_log_config.rs`).
   - Kind/deploy lanes may use the in-cluster MinIO fixture under
     `scripts/ci/kind/object-log.yaml`.
2. **Create a writable bucket** and export:

   | Variable | Required | Default if unset in tests |
   |----------|----------|---------------------------|
   | `FIREWEED_S3_TEST_ENDPOINT` | **yes** (job must set) | — (tests skip without it) |
   | `FIREWEED_S3_TEST_BUCKET` | recommended | `fireweed` / `fireweed-test` |
   | `FIREWEED_S3_TEST_REGION` | optional | `us-east-1` |
   | `FIREWEED_S3_TEST_ACCESS_KEY` | recommended | `minioadmin` |
   | `FIREWEED_S3_TEST_SECRET_KEY` | recommended | `minioadmin` |

3. **Not treat skip as pass** for the gate job. Local developer runs without MinIO may
   `eprintln!` skip; the **required** CI job must fail the matrix if s3 cells did not run.
4. **Native create-only**: the endpoint must support S3 create-only (`If-None-Match: *`
   or equivalent). Fireweed probes this on product open
   (`open_blob_store_with_native_create_only`). MinIO and Garage satisfy this; do not
   claim s3 cells against an S3 implementation that lacks it.
5. For `s3×postgres`, also provision Postgres and set `FIREWEED_PG_TEST_URL`, building
   with `--features postgres`.

## Suggested cargo commands (CI)

```bash
# Unit construction + T4/Helm linkage (no S3 required)
cargo test -p fireweed-server --lib s3_object_log

# Live T1–T3 for s3×memory and s3×sqlite (S3 required; no skip)
export FIREWEED_S3_TEST_ENDPOINT="http://<minio-ip>:9000"
export FIREWEED_S3_TEST_BUCKET=fireweed-test
export FIREWEED_S3_TEST_ACCESS_KEY=minioadmin
export FIREWEED_S3_TEST_SECRET_KEY=minioadmin
cargo test -p fireweed-server --lib s3_object_log
cargo test -p fireweed --test storage_matrix_t0_t2

# Live s3×postgres (+ Postgres)
export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:5432/fireweed
cargo test -p fireweed-server --features postgres --lib s3_object_log
```

## Related artifacts

| Artifact | Role |
|----------|------|
| `cargo test -p fireweed-server --lib s3_object_log` | T0 unit + env-gated T1–T3 + T4 values contract |
| `cargo test -p fireweed --test storage_matrix_t0_t2` | Table-driven 15-cell harness including s3 three cells |
| `scripts/ci/helm-gate.sh` | Renders `s3-memory` / `s3-sqlite` / `s3-postgres` (+ shared multi-replica profiles) |
| `docs/perf/evidence/tp003-ac-txn-matrix-s3-storage-pairs.jsonl` | TP-003 / request_id evidence linkage for s3 pairs |
| `docs/helix/04-build/storage-matrix-conformance-classes.md` §3 | Broader CI evidence layout |
