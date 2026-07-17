# TP-002 E3 — live object-log projection matrix over MinIO

**Date:** 2026-07-16. **Result:** PASS. **Evidence:**
`docs/perf/evidence/tp002-e3-objectlog-minio-release.jsonl`.

The release harness ran both committed object-log projection variants at the same four commit-latency
bounds. Both emitted rows are `scale=release`, `evidence_tier=release`, `bars_met=true`, and cite E3.

## Command

Start a fresh MinIO instance with a tmpfs data volume:

```bash
docker run -d --name pqe3-minio \
  --tmpfs /data:rw,size=8g \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
IP=$(docker inspect pqe3-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
PQUEUE_S3_TEST_ENDPOINT="http://$IP:9000" scripts/perf/tp002-e3-minio.sh
```

The wrapper fixes the release workload at 10,000,000 resident items, 100,000 single-item acknowledgement
pushes per bound, acknowledgement concurrency 384, load batch 1,000, load concurrency 8, and seed 0.

## Topology and hardware

- One local Rust test process drove one MinIO container over live HTTP/S3 at its bridge IP.
- MinIO `RELEASE.2025-09-07T16-13-09Z` stored object data on an 8 GiB tmpfs mounted at `/data`.
- Docker server 29.1.3 ran under Linux 6.6.87.2 WSL2.
- Host: AMD Ryzen 9 5950X, 16 cores / 32 threads, 94 GiB RAM.
- No cluster orchestration or published loopback port was used.

Total test wall time was 3,332.60 seconds (55m32.60s).

## Ack throughput and latency

All throughput measurements exceed the 2,777.78 items/s floor. All p50/p95/p99 measurements are within
the recorded acknowledgement budget.

| profile | bound | throughput/s | p50 ms | p95 ms | p99 ms | budget ms | result |
|---|---:|---:|---:|---:|---:|---:|---|
| object_log_inmemory_projection | 1 ms | 13,355.620 | 27.563 | 47.283 | 54.764 | 751.25 | PASS |
| object_log_inmemory_projection | 5 ms | 13,908.435 | 23.687 | 42.016 | 49.582 | 756.25 | PASS |
| object_log_inmemory_projection | 20 ms | 13,594.685 | 25.285 | 43.789 | 49.500 | 775.00 | PASS |
| object_log_inmemory_projection | 100 ms | 15,134.630 | 24.878 | 27.664 | 29.946 | 875.00 | PASS |
| object_log_sqlite_projection | 1 ms | 10,625.001 | 26.271 | 73.161 | 170.049 | 751.25 | PASS |
| object_log_sqlite_projection | 5 ms | 8,289.433 | 27.147 | 76.520 | 255.308 | 756.25 | PASS |
| object_log_sqlite_projection | 20 ms | 10,319.998 | 27.349 | 64.002 | 178.945 | 775.00 | PASS |
| object_log_sqlite_projection | 100 ms | 10,747.161 | 26.187 | 61.323 | 127.071 | 875.00 | PASS |

## Recovery

The SQLite projection loaded 10,000,000 resident items in 10,000 commands, reopened from snapshot high-water
sequence 10,000, replayed zero tail commands, and restored exactly 10,000,000 pending items in 4,333.887 ms.
The recovery bar passed.

## Exclusions and claim boundary

- `object_log_inmemory_projection` records `recovery_excluded=true` because it does not expose the SQLite
  reopen telemetry seam. Its four acknowledgement-bound rows remain release evidence.
- Tmpfs isolates object-log protocol and projection performance from Docker overlay-disk contention while
  preserving live HTTP/S3 semantics. This evidence does **not** prove object-store host durability, MinIO
  host restart, or provisioned production storage.
- This local MinIO row does not claim cloud-provider-specific S3 IAM, TLS, or operational certification.

## Cost-model continuity

E3 segment and object counters continue to feed the separate directional cost model in
`docs/perf/tp002-e3-cost-model.md`. This matrix does not alter cost-model policy or pricing inputs.
