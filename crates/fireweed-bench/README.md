# API-007 internal reference runner

`fireweed-api007-reference` runs the same bounded workload through `QueueStore`
on PostgreSQL, local object-log/Turso, and S3/Turso. It is an internal reference
runner. These runs do not establish TP-005 acceptance, public S3 factory readiness,
hard RAM bounds, remote storage cost, or the 10M campaign.

Run one backend/repetition per process, sequentially on an otherwise idle host:

```sh
cargo run --manifest-path crates/fireweed-bench/Cargo.toml --locked --release \
  --bin fireweed-api007-reference -- \
  postgres /tmp/fw-reference-pg-unique /tmp/fw-reference-pg-unique.json \
  256 32 24301 256 512
```

Arguments are backend (`postgres`, `local`, `s3`), fresh absolute root, fresh
absolute artifact path, source record count, batch size, seed, body bytes, and
state bytes, with an optional `default` or `zero-linger` profile. Root and artifact must have existing parents and distinct resolved
destinations. Use fresh paths for each repetition.

PostgreSQL requires `FIREWEED_API007_PG_TEST_URL` for an isolated fixture database.
S3 requires `FIREWEED_S3_TEST_ENDPOINT`, `FIREWEED_S3_TEST_REGION`,
`FIREWEED_S3_TEST_BUCKET`, `FIREWEED_S3_TEST_ACCESS_KEY`, and
`FIREWEED_S3_TEST_SECRET_KEY`, pointing to an isolated fixture bucket. The runner
creates a fresh native prefix and queue; it retains data for inspection.

For N source records, the workload also seeds N foreign records, atomically
settles each source with workflow state and one new record, and settles all N
new records. It verifies exact filtered priority/tie order, positional identities
and versions, original receipt replay, exact state contents, and final stats:
2N completed, N foreign pending, no leased or failed records.

All backends use a fixed operational clock, the same interface limits, and the
same runtime capacities. Reports distinguish provision, insertion, lifecycle,
verification, and drain. The settled denominator continuously includes lifecycle,
verification, and successful owned shutdown. Failures retain evidence and attempt
shutdown; workload, shutdown, and provenance failures have no completed settlement
rate. Artifact delivery failure is reported separately after a measured run.

Latency evidence is a bounded histogram with explicit percentile bounds, without
raw latency samples. RSS is the client process peak; PostgreSQL/RustFS service
memory is unmeasured. Artifacts bind the running executable hash and compare the
listed benchmark/common sources and manifests/locks with their compiled bytes.
HEAD and the dirty patch identify the remaining backend source snapshot. Source
changes invalidate the report. Artifact-write failure preserves stdout evidence.

The optional `zero-linger` diagnostic changes only native `FlushConfig.linger`
to zero for both local and S3. It retains the budget controller and every other
setting; PostgreSQL behavior is unchanged. Compare profiles with the same rebuilt
executable, workload, and capacities on fresh roots, sequentially. The experiment
isolates configured linger; it does not remove budget, durability, or recovery
waiting and does not change production defaults.

Native recovery diagnostics cover the interval after construction through owned
shutdown, including warmup and verification. They count admitted framed records
and bytes, excluding failed allocations and provider requests. Elapsed time sums
recovery-task durations, including waits, and may exceed wall time with overlap.
Counters have fixed storage; overflow or unsuccessful drain suppresses numerical
deltas. PostgreSQL reports these native-only diagnostics as unavailable.
