# TP-002 E3 — live object_log_sqlite_projection over S3 (MinIO) RELEASE evidence

**Bead:** `pqueue-f9e15a8b`. **Date:** 2026-06-29. **Commit:** `aed308d`.
**Suite:** `crates/pqueue-server/tests/performance_object_log_e3_live_tests.rs` (the real
`SegmentedObjectLogSqliteBackend` — group-commit ack-after-seal + snapshot-tail recovery — over a real
S3-compatible endpoint via `S3BlobStore`).
**Result:** all three E3 bars met under `PQUEUE_PERF_ENV=1` at the 10M-resident release shape. The emitted
row is `scale=release`, `evidence_tier=release`, `tp002_evidence_ids=["E3"]`. Wall-clock **26m32s**.

## Command

```
PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 PQUEUE_E3_LOAD_CONCURRENCY=16 \
  PQUEUE_S3_TEST_ENDPOINT="http://<minio-ip>:9000" \
  cargo test -p pqueue-server --release --test performance_object_log_e3_live_tests -- --nocapture
```

Endpoint: MinIO (`minio/minio server /data`) reached by container bridge IP (this host cannot reach docker
*published* ports; sustained bridge-IP traffic survives — loopback would be killed by signal 16).

## Results

**Bar 1 — ≥2 segment sizes** (group-commit counters per config):

| config | target_bytes | max_latency_ms | segments_sealed | objects_put | mean batch | max batch |
|---|---|---|---|---|---|---|
| latency-dominant | 8,388,608 | 50 | 50 | 100 | 41.0 | 64 |
| size-dominant | 4,096 | 1,000 | 34 | 68 | 60.2 | 64 |

**Bar 2 — group-commit ack latency p95/p99 vs `segment_max_latency_ms` (+ stated seal-cost slack):**

| config | ack p95 | ack p99 | bar | verdict |
|---|---|---|---|---|
| latency (cap 50ms) | 484.7 ms | 735.8 ms | ≤ 812.5 ms | PASS |
| size (cap 1000ms) | 457.5 ms | 475.3 ms | ≤ 2000 ms | PASS |

**Bar 3 — 10M-item snapshot-tail recovery within the recovery-window budget:**

| metric | value |
|---|---|
| resident loaded | 10,000,000 |
| recovery resumed at high-water (`start_seq`) | 10,000 (NOT genesis 0) |
| tail commands replayed | 0 (≪ budget 1,000,000) |
| snapshot used | true |
| recovery wall-clock | **5.09 s** |
| pending after recovery | 10,000,000 (== pre-restart) |

Recovery rebuilds the 10M-item SQLite projection from the persisted snapshot + bounded tail in ~5 s — no
full-genesis replay (bead `pqueue-8a76daad`), at true release scale.

## Ledger row

`docs/perf/evidence/tp002-e3-objectlog-minio-release.jsonl` — `backend_profile="object_log_sqlite_projection"`,
`scale="release"`, `evidence_tier="release"`, `measurements.tp002_evidence_ids=["E3"]`. Self-validated strict
on emit. The in-process segment-counter smoke row stays smoke-tier.

A real S3 list-pagination bug (recovery past 1000 manifest objects — ListObjectsV2 single-page limit) was
found and fixed during this run (commit `aed308d`).

## Cost half of E3

The `segments_sealed` / `objects_put` / `commands` counts above feed the E3 **cost model** — the release
evidence for ADR-001's directional `$/command` claim. The model scales these measured object/segment counts
to a billion commands, prices them against cited S3/DB inputs, and compares against the `postgres_native`
high-volume baseline (`docs/perf/tp002-e0e1-postgres-release-10m.md`): at the documented baseline
`object_log_sqlite_projection` is **$292.80/B-commands vs $984.04/B-commands** for `postgres_native`
(3.36x cheaper), with full breakdown, cited prices, and a sensitivity/crossover table in
`docs/perf/tp002-e3-cost-model.md`. The calculator is `pqueue_release::cost` (fixture-tested); regenerate the
artifact + smoke-tier cost-model ledger row with:

```
cargo run -p pqueue-release --bin pqueue-cost-model -- \
  --out docs/perf/tp002-e3-cost-model.md --ledger docs/perf/evidence/tp002-e3-cost-model.jsonl
```
