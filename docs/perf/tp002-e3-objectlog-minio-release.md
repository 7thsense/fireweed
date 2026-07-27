# TP-002 E3 — live object-log projection matrix over MinIO

**Status:** PREPARED; the exact 10M recovery proof is now encoded in the E3 test harness, but a new release run is still required to stamp a PASS artifact.

The 2026-07-16 run is retained only as
`docs/perf/evidence/tp002-e3-objectlog-minio-historical-invalid.jsonl`. It is invalid under the current
contract because it used host-speed thresholds, excluded in-memory recovery, allowed a zero SQLite tail,
and lacks production replay/resource and measured-byte provenance. It must not be consumed as release
evidence. This preparation change deliberately does not fabricate replacement measurements.

## Command

Start a fresh MinIO instance with a tmpfs data volume:

```bash
docker run -d --name fireweed-e3-minio \
  --tmpfs /data:rw,size=8g \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
IP=$(docker inspect fireweed-e3-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
FIREWEED_S3_TEST_ENDPOINT="http://$IP:9000" scripts/perf/tp002-e3-minio.sh
```

The wrapper fixes the release workload at 10,000,000 resident items, 100,000 single-item acknowledgement
pushes per bound, acknowledgement concurrency 384, load batch 1,000, load concurrency 8, an 896 KiB
recovery-load segment target, and seed 0. Before the load starts, the harness canonically serializes the
first eight commands and fails closed unless the smallest four exceed the segment target by at least 10%,
the smallest three remain below it, each command remains below the target, and the full wave's conservative
byte-admission charge is at most
half the 16 MiB per-queue cap. These are byte-shape checks, not elapsed-time or host-performance gates.

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
production recovery loops. Recovery wall time is capacity evidence only. The
bulk load must be dominated by size-triggered seals, may use at most one latency
seal for the final partial segment, may not use forced or rollover seals, and
must reconcile the sum of all group-commit batch sizes exactly to committed
commands.
The in-memory profile loads 10,000 commands. The SQLite profile loads 9,999,999
items in 10,000 commands, then commits one deliberately unacknowledged tail
command, so its governed recovery command count is 10,001. Concurrent loader
scheduling does not define queue order: the proof validates that the complete
authoritative order is a duplicate-free permutation of the verified live state
and that its page-for-page digest is identical before and after recovery.
The exact snapshot-tail, genesis, inexact-command-range, and checksum-drift
checks are enforced by `TestE3RecoveryExactSnapshotTailReplay`,
`TestE3RecoveryExactGenesisReplay`,
`TestE3RecoveryRejectsInexactCommandRange`, and
`TestE3RecoveryRejectsChecksumDrift` in
`crates/fireweed-server/tests/performance_object_log_e3_live_tests.rs`.

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

## Acceptance mapping

The release path is fail-closed before a new ledger is accepted:

1. The cost builder emits all eight profile/bound rows from recorder request and byte counters, including
   aggregate and per-operation requests/bytes/USD per billion plus exact price and source revisions.
2. The optimizer consumes those measured densities and the fixed `postgres_native` price/workload bundle;
   elapsed host speed is capacity context only.
3. Recovery fingerprints canonical complete live state and authoritative order, requires exact 10M counts,
   monotonic production replay progress, bounded queues/pages, and successful production segment/record/frame
   checksum validation for both snapshot-tail and genesis replay.
4. Each bound requires five seeded, alternating, same-run recorder-control blocks with matching complete-state
   fingerprints and median degradation no greater than 1.02.
5. Semantic validators reject smoke rows, incomplete matrices, missing or altered counters, stale provenance,
   non-exact/checksum-unverified recovery, quiet-host deferral, and host-speed gates.
6. Focused tests, formatting, and warning-denied clippy are the code gates. The only remaining evidence gate is
   the coordinated live 10M MinIO run and validation of its generated release ledger; PREPARED is not PASS.
