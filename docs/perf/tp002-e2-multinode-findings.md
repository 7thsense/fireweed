# TP-002 E2 — multi-node object_log_sqlite_projection: harness + blocking finding

**Bead:** `pqueue-b5af53fb` (live multi-node E2 scale-out benchmark). **Status: OPEN** — harness landed,
release evidence blocked on a backend throughput ceiling (below). **Date:** 2026-06-28.

## Delivered
`crates/pqueue-bench/tests/performance_multi_node_object_log_e2_tests.rs` (commit `32e7cf9`): spins up N
independent `pqueue-service` owner nodes on the real `object_log_sqlite_projection` backend, distributes
disjoint queues, drives a concurrent RESP push+claim+ack workload at owner counts 2/4/8, measures aggregate
+ worst-per-queue throughput, proves one-owner-per-queue (`XLEN` int on the owner, `-ERR no such queue`
elsewhere), and hard-fails unless the bars hold — emitting `evidence_tier=release` ledger rows only on a
pass. The harness is correct; what follows is why it cannot emit a release row on the current infra+backend.

## Finding 1 — the environment kills *sustained loopback* traffic (topology, solved)
The harness as first written spawns nodes as **host processes** on `127.0.0.1:<port>`. In this
sandbox/orbstack environment, a sustained loopback (`127.0.0.1`) RESP connection is terminated with **signal
16 (exit 144)** once traffic is sustained (a quick connect survives; a benchmark load does not). This is NOT
a harness or server defect:
- A quick `PING`/`XADD`/`XREADGROUP` over loopback succeeds; a 20k-item drive is killed.
- The 36-minute postgres E0/E1 run (`docs/perf/tp002-e0e1-postgres-release-10m.md`) and the MinIO S3 substrate
  test both sustained traffic for their full duration **over a container-bridge IP** (`192.168.215.x`) without
  a kill.
**Resolution:** run the nodes as **containers on the docker bridge** (kind/docker) and drive them by container
IP, not loopback. Confirmed: a containerized node sustained the full benchmark load with `status=running`.

## Finding 2 — the object-log backend is below the E2 per-queue floor (blocking)
Single-queue push throughput of `object_log_sqlite_projection` (`pqueue-service`, `LocalObjectLog` +
sqlite projection), measured directly:

| storage | single-queue push | vs E0/E2 floor (2,778/s) |
|---|---|---|
| container overlayfs (default) | **~30 items/s** | 0.01× — catastrophic |
| tmpfs (RAM, no fsync — best case) | **~1,821 items/s** | 0.66× — still under floor |

Root cause: `LocalObjectLog` writes **one object file per command** (plus a sqlite projection apply). Even
with disk/fsync removed (tmpfs), the per-command file + apply overhead caps a single queue at ~1.8k/s —
**below the 2,777.78/s E2 per-queue floor**, with no headroom. On real disk/overlay it is ~30/s. The E2
headline (worst-per-queue ≥ floor *and* 8-owner ≥ 3.5× 2-owner) is therefore unreachable with this backend
regardless of node count or infra.

## Completion path (concrete)
The file-per-object cost is exactly what the segmented group-commit substrate from bead `pqueue-58b42354`
(`crates/pqueue-objectlog/src/segmented.rs`) eliminates — it batches many commands into one object. To
unblock E2:
1. **Wire `SegmentedObjectLog` into `pqueue-service`'s `object_log_sqlite_projection` backend** (replace/flag
   `LocalObjectLog`; thread segment-size + `segment_max_latency_ms` config). *This is a backend feature, a
   separate bead from the E2 benchmark.*
2. Run the E2 nodes as **bridge containers with a fast volume** (tmpfs or NVMe), driven by container IP
   (Finding 1).
3. Re-run `PQUEUE_E2_MULTINODE=1 cargo test --manifest-path crates/pqueue-bench/Cargo.toml --test performance_multi_node_object_log_e2_tests`
   (updated for the container topology) and emit the release E2 rows on a pass.

No release row was emitted and no evidence was faked. The in-process owner-independence smoke
(`performance_cross_queue_scale_out_tests`) clears 3.72× at 8/2 on these 12 cores, so the *scaling* property
holds where the backend write path is fast enough — the gap is the single-node write ceiling above.
