# TP-002 E3 — live object-log projection matrix over MinIO

**Status:** PREPARED; a new release run is required. There is no current E3 PASS artifact.

The 2026-07-16 run is retained only as
`docs/perf/evidence/tp002-e3-objectlog-minio-historical-invalid.jsonl`. It is invalid under the current
contract because it used host-speed thresholds, excluded in-memory recovery, allowed a zero SQLite tail,
and lacks production replay/resource and measured-byte provenance. It must not be consumed as release
evidence. This preparation change deliberately does not fabricate replacement measurements.

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

The historical run took 3,332.60 seconds (55m32.60s); that duration is not a gate or current evidence.

## Ack throughput and latency

Throughput and p50/p95/p99 latency are topology-bound capacity observations,
not release thresholds. Release judgment uses exact committed work, valid
distribution ordering, logically identical interleaved recorder controls, and
fixed resource bounds; it does not require a quiet or specially selected host.

Historical observations only (invalid as current evidence):

| profile | bound | throughput/s | p50 ms | p95 ms | p99 ms | old budget ms | status |
|---|---:|---:|---:|---:|---:|---:|---|
| object_log_inmemory_projection | 1 ms | 13,355.620 | 27.563 | 47.283 | 54.764 | 751.25 | historical-invalid |
| object_log_inmemory_projection | 5 ms | 13,908.435 | 23.687 | 42.016 | 49.582 | 756.25 | historical-invalid |
| object_log_inmemory_projection | 20 ms | 13,594.685 | 25.285 | 43.789 | 49.500 | 775.00 | historical-invalid |
| object_log_inmemory_projection | 100 ms | 15,134.630 | 24.878 | 27.664 | 29.946 | 875.00 | historical-invalid |
| object_log_sqlite_projection | 1 ms | 10,625.001 | 26.271 | 73.161 | 170.049 | 751.25 | historical-invalid |
| object_log_sqlite_projection | 5 ms | 8,289.433 | 27.147 | 76.520 | 255.308 | 756.25 | historical-invalid |
| object_log_sqlite_projection | 20 ms | 10,319.998 | 27.349 | 64.002 | 178.945 | 775.00 | historical-invalid |
| object_log_sqlite_projection | 100 ms | 10,747.161 | 26.187 | 61.323 | 127.071 | 875.00 | historical-invalid |

## Recovery

Both committed profiles must recover the exact 10,000,000-item identity,
ordering, lifecycle, payload, and field state. SQLite uses snapshot high-water
plus a bounded tail; the in-memory projection performs an exact durable-log
genesis replay. Verification reads at most 512 items per batch, uses eight load
tasks, replays at most 256 commands per production page, caps object-list pages
at 1,000 keys, and bounds concurrent object-store requests independently of
resident cardinality. Replay progress and peak work/page counts come from the
production recovery loops. Recovery wall time is capacity evidence only.
The in-memory profile loads 10,000 commands. The SQLite profile loads 9,999,999
items in 10,000 commands, then commits one deliberately unacknowledged tail
command, so its governed recovery command count is 10,001. Concurrent loader
scheduling does not define queue order: the proof validates that the complete
authoritative order is a duplicate-free permutation of the verified live state
and that its page-for-page digest is identical before and after recovery.

## Exclusions and claim boundary

- Tmpfs isolates object-log protocol and projection performance from Docker overlay-disk contention while
  preserving live HTTP/S3 semantics. This evidence does **not** prove object-store host durability, MinIO
  host restart, or provisioned production storage.
- This local MinIO row does not claim cloud-provider-specific S3 IAM, TLS, or operational certification.

## Cost-model continuity

E3 measured request counts and request/response bytes feed every profile/bound
cost row. Rows record requests per billion, bytes per billion, USD per billion,
the exact source revision, and the governed price-bundle revision used for the
`postgres_native` comparison. The semantic verifier recomputes these values and
rejects missing counters or stale price provenance.
