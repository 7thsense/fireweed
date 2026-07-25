# TP-002 E2 — multi-node `object_log_sqlite_projection` cross-queue scale-out: RELEASE evidence (kind)

**Beads:** `pqueue-a983b5e2` (run the provisioned multi-node E2 evidence) and `pqueue-b5af53fb` (the
release-tier multi-node E2 harness). **Status: BOTH CLOSED — release bars met, reproducibly, on a kind
cluster.** **Date:** 2026-06-29. Branch `build/queue-as-unit-of-sharding` (HEAD gate-green).

This is the ADR-008 headline (the queue is the unit of sharding; horizontal scale comes from distributing
queues across INDEPENDENT, shared-nothing owner nodes). It replaces the in-process memory smoke evidence
(`performance_cross_queue_scale_out_tests`, which is explicitly NOT the headline) and supersedes the
earlier raw-docker characterization in `tp002-e2-multinode-findings.md` (the co-located-box ceiling — now
resolved by CPU-limited pods + a lean separated load generator).

## Why kind passes where raw-docker did not (the design)

The prior raw-docker E2 missed the bars because a single fat ~64-thread HOST load driver stole cores from
the 8 co-located server containers, **unmanaged**, starving each owner below the 2,777.78/s per-queue floor;
the run-to-run numbers coin-flipped right at the floor edge with no reproducible margin. The backend was
already fast single-node (post Fix A/B: ~15,000/s ingest single-node, ~2,500/s per core), so the wall was
total CPU/scheduler contention on the shared box, not the backend.

kind enforces two fixes that turn that ceiling into a reproducible pass:

1. **CPU-LIMITED server pods.** Each owner pod gets a guaranteed CPU slice (request `1000m`, limit `1300m`)
   enforced by the kubelet cgroup inside the kind node. Because the per-pod limit is **constant** across
   owner counts, per-pod throughput is constant — so aggregate scales **linearly** 2 → 4 → 8 and the 8/2
   ratio lands near **4×** by construction (well clear of the 3.5× bar), instead of collapsing as the box
   saturated.
2. **A LEAN, SEPARATED, IN-CLUSTER load generator.** The load is a single in-cluster `Job` (`fireweed-loadgen
   run`) with a **bounded** CPU limit (`2000m`), driving the workload **pod → pod over Service ClusterIP**.
   Bounding the driver's CPU frees the cores the old fat host driver was stealing, so each server pod gets
   enough CPU (~1.2–1.3 effective cores) to clear the per-queue floor. Driving pod→pod also sidesteps this
   sandbox's host→pod sustained-loopback signal-16 kill (cluster-network traffic is immune); only the kind
   kubeconfig is repointed at the control-plane **bridge IP** for the same reason.

## Topology

- **Server pods (per owner):** the `fireweed:e2` image (`Dockerfile.e2` — production `fireweed-service` +
  `fireweed-loadgen` in one image) as an independent `Deployment(replicas=1)` + `Service`, in
  `object_log_sqlite_projection` **segmented group-commit** mode (`FIREWEED_LOG_BACKEND=objectlog`,
  `FIREWEED_PROJECTION_BACKEND=sqlite`, `FIREWEED_OBJECT_LOG_MODE=segmented`), with the object-log root + sqlite
  projection on an **`emptyDir medium: Memory`** tmpfs (`/data`), a distinct `FIREWEED_NODE_ID`, a **disjoint**
  `FIREWEED_BOOTSTRAP_QUEUES` set (one queue per owner; ownership disjoint by construction), and tuning
  `FIREWEED_SEGMENT_TARGET_BYTES=262144`, `FIREWEED_SEGMENT_MAX_LATENCY_MS=1`, `FIREWEED_WORKER_THREADS=2`. CPU
  **request `1000m` / limit `1300m`**, memory request `256Mi` / limit `1Gi`, `imagePullPolicy: Never`, TCP
  readiness probe on `8080`.
- **Load generator:** an in-cluster `Job` (`fireweed-loadgen run`, CPU **limit `2000m`** / request `1000m`)
  reading its `RunSpec` from a `ConfigMap`-mounted `spec.json`. It speaks raw RESP over `std::net::TcpStream`
  to each owner Service (`fireweed-o<idx>.<ns>.svc.cluster.local:8080`), drives the workload — pipelined `XADD`
  ingest, `XREADGROUP >` claim, `XACK` finalize — at owner counts 2/4/8 with `8` concurrent connections per
  queue, `pipe=1000`, `batch=1000`, `12000` items per queue, and prints one measured `RESULT {json}` line
  per owner count.
- **Cluster:** kind `kindest/node:v1.36.1`, single node, 12 cores. At 8 owners: `8×1300m` server limit +
  `2000m` load limit fits the box; requests (`8×1000m + 1000m = 9` cores) schedule comfortably.

### Robustness: a single warm-up + bounded retry on transient epoch flaps (load-side only, server untouched)

Under 8 CPU-limited pods a server's owner lease can momentarily flap, the next write re-acquires at a bumped
epoch, and in-flight commands cached at the old epoch are briefly fenced (`-ERR fireweed epoch_stale`). The
generator handles this exactly as a real client would, **without changing any server code**: it (a) warms up
each queue with one serial write + drain to establish durable ownership before the timed phases, and (b)
bounded-retries a transiently-fenced `XADD`/`XREADGROUP`/`XACK`. The retry cost stays INSIDE the timed
window, so throughput stays honest, and the final exact-count drain assertion (`num_queues × items_per_queue`
finalized) fails loudly on any miscount — it can never silently inflate a pass.

## The three sweeps (every value measured; release-tier rows emitted only because all four bars held)

Per-owner-count aggregates (items/s) and the worst single-queue rate in each phase, owners 2 / 4 / 8:

| sweep | ingest agg 2→4→8 | 8/2 ingest ratio (bar ≥3.5×) | worst ingest/q (floor 2,778) | worst claim+finalize/q | one-owner | verdict |
|---|---|---|---|---|---|---|
| 1 | 6,516 → 12,955 → 25,783 | **3.96×** | **3,223** | 27,010 | 56 conf. | **PASS** |
| 2 | 6,404 → 12,813 → 24,931 | **3.89×** | **3,117** | 25,201 | 56 conf. | **PASS** |
| 3 | 6,014 → 12,862 → 25,496 | **4.24×** | **3,007** | 26,502 | 56 conf. | **PASS** |

- **(1) ingest aggregate strictly non-decreasing 2→4→8:** PASS every sweep.
- **(2) 8-owner ingest aggregate ≥ 3.5× the 2-owner:** 3.89–4.24× — PASS every sweep, ~11–21% margin.
- **(3) worst per-queue ≥ 2,777.78/s for BOTH ingest AND claim+finalize:** worst ingest 3,007–3,223/s
  (~8–16% margin); worst claim+finalize 25,201–27,010/s (≈9× the floor) — PASS every sweep.
- **(4) one-owner-per-queue:** 56 cross-node `-ERR no such queue` confirmations at 8 owners every sweep
  (each of the 8 queues answers `XLEN` with an integer only on its owner and is unknown on the other 7) —
  PASS every sweep.

3/3 consecutive full sweeps pass all four bars with consistent margin — the reproducible margin a release
row must carry, not a lucky run at the floor edge.

## Evidence + exact commands

Release-tier rows (one per sweep), `backend_profile="object_log_sqlite_projection"`,
`scale="release"`, `evidence_tier="release"`, `measurements.tp002_evidence_ids=["E2"]`:
`docs/perf/evidence/tp002-e2-multinode-kind-release.jsonl`. The governed authority is exactly three rows,
one each for unique sweeps 1, 2, and 3. Every row is strict-validated by `fireweed_release`, and the verifier
rejects missing, duplicate, extra, mixed-revision, or mixed-configuration sweeps. Validation also happens
at emit time (the generator's `emit-row` refuses to write a release row unless every bar holds).

```sh
# Build the harness image (service + loadgen), create the kind cluster, run 3 full 2/4/8 sweeps, and
# emit one release-tier E2 ledger row per sweep (smoke-tier + non-zero exit if any bar misses):
bash scripts/perf/tp002-e2-kind.sh
# Tunables via env (defaults shown): SERVER_CPU_LIMIT=1300m SERVER_CPU_REQUEST=1000m
#   LOADGEN_CPU_LIMIT=2000m WORKER_THREADS=2 SEG_LATENCY_MS=1 SEG_TARGET_BYTES=262144
#   ITEMS_PER_QUEUE=12000 CONNS_PER_QUEUE=8 PIPE=1000 BATCH=1000 QUEUES_PER_OWNER=1 SWEEPS=3
# Teardown (the harness leaves the cluster up for inspection; delete it + the image when done):
kind delete cluster --name fireweed-e2 && docker rmi fireweed:e2
```

Harness sources: `scripts/perf/tp002-e2-kind.sh` (orchestrator + manifest generation),
`crates/fireweed-loadgen/` (the in-cluster RESP load generator + `emit-row` ledger emitter), `Dockerfile.e2`
(the service + loadgen image).
