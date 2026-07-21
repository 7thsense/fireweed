# TP-002 E0/E1 — postgres_native single-deployment RELEASE evidence (10M resident)

**Bead:** `pqueue-9b2a374e` (external/provisioned half of `pqueue-d3371502`).
**Date:** 2026-06-28.
**Commit:** `425bb406852c82bf389359201bea786c0e2896c1`.
**Suite:** `crates/pqueue-server/tests/performance_single_deployment_baseline_tests.rs`
(`PostgresRelationalBackend`, TD-002 `postgres_native`).
The current E1 workload sends API-001 `BatchUpdate` windows of 1, 100, and 1000 items through one
set-oriented PostgreSQL transaction per window; it does not emulate batching with scalar update calls.
**Historical result:** this run predates the current explicit progress-bound and
persisted-definition evidence fields. Its rates and percentiles remain useful
capacity observations, but fresh evidence from the current harness is required
for release authority. Wall-clock **36m12s**.

## Command

```
PQUEUE_PERF_ENV=1 PQUEUE_E1_RESIDENT=10000000 PQUEUE_E1_FULL=1 \
  PQUEUE_E0E1_PROGRESS_BOUND_MS=<positive-workload-declaration> \
  PQUEUE_PG_TEST_URL=postgres://postgres:pq@<instance>:5432/postgres \
  cargo test -p pqueue-server --features postgres --test performance_single_deployment_baseline_tests --release -- --nocapture
```

## Provisioned instance (local-equivalent sizing)

A containerized PostgreSQL standing in for a provisioned perf instance. Local-equivalent sizing is recorded
per the bead (no cloud instance class / region / IOPS — this is a dev-host provisioned container).

| Dimension | Value |
|---|---|
| Host | OrbStack Linux VM on macOS — **12 vCPU, 94 GiB RAM**, NVMe-backed (`/` 319 GiB free), `rustc 1.92.0` |
| Engine | `postgres:16` (Docker), `--shm-size=2g` |
| Connection | OrbStack bridge `192.168.215.11:5432`, single synchronous client (`futures::executor::block_on`, one thread) |
| Storage tuning | `synchronous_commit=off`, `shared_buffers=4GB`, `effective_cache_size=12GB`, `max_wal_size=8GB`, `checkpoint_timeout=30min`, `work_mem=64MB`, `max_connections=50` |
| **Autovacuum tuning (load-bearing — see below)** | `autovacuum_naptime=1s`, `autovacuum_vacuum_cost_delay=0`, `autovacuum_vacuum_cost_limit=10000`, `autovacuum_vacuum_scale_factor=0.0`, `autovacuum_vacuum_threshold=2000`, `autovacuum_max_workers=4`, `autovacuum_work_mem=256MB` |

The declared progress bound is a queue product contract for this workload, not
a machine-speed target. The harness persists it in the queue definition and
requires every measured accepted-to-claim interval and discovery age to remain
within it. Fixed latency buckets, rates, and percentiles are report-only
capacity data and never require a quiet or specially selected host.

## Historical capacity observations

| Evidence | Metric | Observed |
|---|---|---|
| **E0** | ingest | **20,431 items/s** |
| **E0** | claim+finalize | **6,145 items/s** |
| **E1** | worst op p99 | **416.2 ms** |

E1 per-op p95/p99 (ms), batch sizes 1 / 100 / 1000 — full row in
`docs/perf/evidence/tp002-e1-postgres-release-10m.jsonl`:

| op | b1 p95/p99 | b100 p95/p99 | b1000 p95/p99 |
|---|---|---|---|
| push | 207.3 / 213.1 | 211.5 / **416.2** | 44.4 / 239.9 |
| claim | 209.7 / 214.3 | 212.8 / 215.1 | 238.2 / 252.6 |
| finalize | 206.5 / 211.9 | 8.9 / 215.8 | 240.1 / 245.6 |

## Operational finding: claim-index MVCC bloat under a full-backlog drain

The first attempts on a default-config postgres fell below the historical capacity comparison and got *slower* as the
backlog grew (900 items/s at 50k resident, trending to sub-100/s at 10M). Root cause, confirmed with
`EXPLAIN (ANALYZE, BUFFERS)`:

- The claim selection uses the partial index `pqueue_items_claim_idx (tenant_id, queue_id, priority_sort,
  created_seq) WHERE lifecycle_state = 'Pending'`.
- The drain claims in `priority_sort` order, so the **front** of that index fills with dead index tuples
  (rows whose current version is `Leased`/`Complete`) that have not yet been vacuumed.
- Each subsequent claim must traverse the growing dead prefix to reach live `Pending` rows. Measured: a
  single 500-item claim went from **1,007 buffers / 0.55 ms** at full backlog to **46,694 buffers / 9.3 ms**
  after the low-priority front was drained — i.e. O(dead-prefix) per claim → O(N²) over the drain.

**Resolution (instance configuration, not a code change):** aggressive autovacuum (above) keeps the partial
`Pending` index clean during the drain. This lifted claim+finalize from 900/s → **10,963/s** at 50k and held
**6,145/s** across the full 10M drain. This is standard practice for
a high-churn queue-on-postgres table and is the operational requirement this evidence establishes: **a
provisioned `postgres_native` deployment must tune autovacuum aggressively on `pqueue_items`.**

Note this is a **single-threaded capacity observation**. The claim path is `… FOR UPDATE SKIP LOCKED` precisely so a real
owner runs concurrent claimers; the per-queue throughput under concurrency is a multiple of the number above.

## Ledger rows (release-tier, strict-valid)

Emitted by the suite to `target/pqueue-ledger/` (the CI gate points `$PQUEUE_LEDGER_DIR` at a collection
dir); committed copies for durable record:

- `docs/perf/evidence/tp002-e0-postgres-release-10m.jsonl` — `tp002_evidence_ids=["E0"]`
- `docs/perf/evidence/tp002-e1-postgres-release-10m.jsonl` — `tp002_evidence_ids=["E1"]`

These historical rows do not contain the current explicit/persisted progress-bound
contract and therefore cannot satisfy the current release validator. A fresh
exact-revision E0/E1 run must replace them before release.
