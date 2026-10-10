---
ddx:
  id: build-simple-queue-shared-benchmarks
  depends_on:
    - api-queue-claim-commit-contract
    - build-simple-queue-postgres-refactor
  status: draft
---

# Shared settled-work benchmark lane

The queue_benchmark workload in fireweed-conformance depends only on QueueStore.
Physical fixtures provide construction and provenance; they must call the identical
workload, including verification. No adapter-private insertion, claim or settlement
shortcut is permitted. PostgreSQL currently has the first runnable fixture.

## Current reference workload

A deterministic seeded generator creates source work plus an equal number of
foreign items. Filtered priority claims settle each source with an atomic state
insert and one emitted next item; a second filtered pass settles all emitted work.
Every claimed body, unique ordinal, committed outcome and state value is checked. Exact
receipt replay must match; final stats must show 2N completed, N foreign pending,
zero leased and zero failed. State query pages must cover exactly N distinct
values without leaking work or losing progress. Correctness failure produces no
accepted throughput report.

Provision, insertion, measured lifecycle and post-lifecycle verification are
reported separately. The useful numerator is 2N durably acknowledged settled work
items. Lifecycle elapsed includes claims, state/fanout commits and consumer/oracle
work; it does not subtract append/apply debt or count emitted-but-unsettled work.
The JSON rate is explicitly acknowledged lifecycle throughput; physical drain/debt
is unmeasured. The timer stops before verification and shutdown. No arbitrary
backend is qualified by this rate without separate apply/drain evidence.

The workload records per-operation nanosecond samples and nearest-rank p50/p95/p99.
Claim and commit samples aggregate source and next phases; claims include the
terminal empty probes. Source commits carry state and fanout, while next commits
carry settlement only. Every claim is checked against the same deterministic
priority/insertion-tie oracle.
Small samples, especially the single stats observation, are smoke evidence only.

The first fixture uses one sequential client, a two-connection pool, batch 32,
256-byte work bodies, 512-byte state, seed 0x5eed and an explicit 64-source-record
warmup in a separate queue. Default measured N is 256; the environment may set
FIREWEED_API007_BENCH_RECORDS. Its fixed operational clock isolates deterministic
lease/due semantics; wall time measures execution. It does not exercise aging,
lease-expiry pressure or the numeric progress obligation. Foreign rows stay live
for the full run. Database state is retained in unique queue namespaces; repeated
runs grow the fixture, which must be controlled for comparisons.

## Running and interpreting

Run the ignored shared_settled_work_reference test in fireweed-postgres/tests/
queue_benchmark.rs with FIREWEED_API007_PG_TEST_URL pointing to an isolated native
PostgreSQL fixture. The URL is not printed. Output includes backend, source commit,
dirty-tree marker, host, compiler, build profile and workload parameters. Dirty-tree
or debug runs are development smoke checks, not publishable comparisons.

Do not infer a throughput target from a smoke run. Full qualification still needs
both stores at the same committed source, optimized profile, host/topology,
concurrency and workload distributions, plus warmup/setup/recovery separation,
10M resident queues, cold/hot density, active overload/progress, settled debt/drain,
RSS, storage and S3 request/cost evidence. Database version and crash-safety settings
must be captured with controlled fixture configuration. S3/Turso wiring and those
additional workload lanes remain required before comparative claims or activation.
