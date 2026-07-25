# TP-002 E2 — live owner failover over shared object-log storage

The release evidence passed. A three-replica fireweed deployment retained all four visible items across active-owner deletion, advanced ownership from epoch 2 to epoch 3, rejected the stale writer before mutation, and admitted no double lease or corrupt write.

## Reproduction

```bash
FIREWEED_E2_SEED=2002 bash scripts/perf/tp002-e2-failover-kind.sh
```

The harness builds the exact committed source, creates a disposable kind cluster, installs the shared-S3 Helm profile, executes the fault schedule, runs the live fencing and snapshot-tail seams, emits JSON only after every assertion passes, and validates that JSON with `fireweed-verify-e2-failover`.

`FIREWEED_TEST_COORDINATION_TIMEOUT_SECS` is an opt-in operational deadlock watchdog
around the complete paused-write handoff seam. It is unset by default, accepts a
canonical positive integer up to 86,400 seconds, and is not a latency, throughput,
quiet-host, or host-speed acceptance bar. Expiry is reported as retryable
`infrastructure_indeterminate` at the last named seam stage, aborts the harness,
and cannot emit release evidence.

## Environment

- Source: `b04bd03a2c077c7b7b7c300c7275d3b934a9ddb8`
- Chart SHA-256: `80d3dae21301f3819377b0c277b8fa31a3467026b491f3c37bbf6cc8b3de5610`
- Runtime image: `fireweed:e2-failover`, image ID `sha256:a6e3aa6699500c8334019f5308b95b10761191a7a5b97c97d3e0ad0163d891b3`
- PostgreSQL: `postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20`
- MinIO: `minio/minio:latest@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
- Topology: three fireweed pods, shared MinIO object log, PostgreSQL ownership, and one SQLite projection per pod
- Host: Linux 6.6.87.2-microsoft-standard-WSL2 x86_64, 32 CPUs, 101244276736 memory bytes
- Seed: `2002`
- Duration: `823644ms`

## Fault and observations

The harness sent one request to a non-owner, observed exactly one `MOVED`, retried once against the selected owner, then pushed three additional items. With four items visible, it deleted the active owner pod and waited for a distinct Kubernetes UID at a strictly greater PostgreSQL assignment epoch.

| Assertion | Result |
| --- | --- |
| Owner transition | `5685d1d7-6f06-42f0-bc3b-67bb105a0fa9` at epoch 2 → `2c4e4621-9f6f-4f2f-b357-2a63289f3a4c` at epoch 3 |
| Visible state | 4 before → 4 after |
| Redirect and retry | 1 `MOVED`, 1 successful retry |
| Lost work | 0 |
| Double leases | 0 |
| Corrupt writes | 0 |
| Epoch-stale append | Rejected before mutation |
| Snapshot-tail takeover | Snapshot used, positive high-water, positive tail replay, exact state recovered before serving |

The snapshot-tail result comes from the live `greater_epoch_owner_hydrates_snapshot_tail_before_serving` seam. It creates a durable standby snapshot, leaves a committed tail behind it, performs the epoch-2 takeover, and directly asserts snapshot use, positive `start_seq`, positive `tail_replayed`, exact four-item visibility, stale-epoch fencing, and exclusive claims. The separate paused-append race also passed against the same PostgreSQL and MinIO services.

## Scope

This row proves E2 failover correctness for the `object_log_sqlite_projection` release profile. It excludes density, throughput, and managed-cloud S3/PostgreSQL behavior; those performance concerns remain in the separate E3 lane.

Machine-readable evidence: [`evidence/tp002-e2-objectlog-sqlite-failover-kind.json`](evidence/tp002-e2-objectlog-sqlite-failover-kind.json).
