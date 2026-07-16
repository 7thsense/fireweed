# TP-002 E3 - live object-log projection matrix over S3 (MinIO) RELEASE evidence

**Harness:** [`crates/pqueue-server/tests/performance_object_log_e3_live_tests.rs`](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-42ad32c4-20260716T222915-15050fce/crates/pqueue-server/tests/performance_object_log_e3_live_tests.rs)

**Wrapper:** [`scripts/perf/tp002-e3-minio.sh`](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-42ad32c4-20260716T222915-15050fce/scripts/perf/tp002-e3-minio.sh)

This harness now runs both governed object-log projection variants at the same release workload:

- `object_log_inmemory_projection`
- `object_log_sqlite_projection`

Each profile is measured at four commit-latency bounds: `1ms`, `5ms`, `20ms`, and `100ms`.

## Command

```bash
PQUEUE_PERF_ENV=1 PQUEUE_E3_RESIDENT=10000000 \
  PQUEUE_S3_TEST_ENDPOINT="http://<minio-ip>:9000" \
  scripts/perf/tp002-e3-minio.sh
```

## Topology

- One MinIO container reachable over the bridge IP.
- One `cargo test` process driving the E3 matrix harness.
- Two backend profiles, each running the same 1/5/20/100ms bound set.
- Single deployment, release resident shape `PQUEUE_E3_RESIDENT=10000000`.

## Hardware

- Same host class and bridge-IP MinIO setup used for the original E3 evidence run.
- The matrix harness is still a local test process; no cluster orchestration is involved.

## Seed

- `0`

## Duration

- Historical single-profile E3 run: `26m32s`.
- The new two-profile matrix harness has not been rerun in this workspace, so the updated wall-clock should be recorded by the next MinIO evidence pass.

## Exclusions

- `object_log_inmemory_projection` records `recovery_excluded=true` because it does not expose the SQLite reopen telemetry seam used to capture snapshot-tail recovery stats.
- No other exclusions are intended.

## Evidence Shape

Each emitted row is expected to be:

- `scale=release`
- `evidence_tier=release` when the release workload and bars are met
- `bars_met=true`
- `tp002_evidence_ids=["E3"]`
- throughput at or above `2777.78 items/s`
- `ack_p50_ms`, `ack_p95_ms`, and `ack_p99_ms` within the configured bound

The harness emits one ledger row per backend profile, with the four bound measurements embedded under `bound_1ms_*`, `bound_5ms_*`, `bound_20ms_*`, and `bound_100ms_*`.
