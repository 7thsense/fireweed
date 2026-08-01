# TP-002 E3 — live object-log projection matrix over S3-compatible storage

**Status:** PARTIAL (exact 10M recovery contracts recorded; full cost/ack E3 ledger still open).

### LogEngine port (fireweed-3aaa3ebc)

The E3 harness no longer uses the retired FWSG `SegmentedObjectLog*` facades. It opens:

- `ObjectLogEngineStore::open_s3` + `AsyncObjectLogMemoryBackend` (genesis replay)
- `ObjectLogEngineStore::open_s3` + `AsyncObjectLogSqliteBackend` (snapshot high-water + tail)

Product open records `RecoveryStats` (`start_seq`, `tail_replayed`, `snapshot_used`, page/resource
counters). SQLite recovery resumes from `recovery_high_water`; in-memory always genesis-replays.
The SQLite crash tail uses `RawCommitFault::AfterAppendBeforeApply` (append durable, apply suppressed).

### Exact 10M recovery evidence (live MinIO, 2026-08-01)

Host: `fireweed-e3-minio-16g` (16 GiB tmpfs `/data`), release binary, `FIREWEED_E3_RESIDENT=10000000`.

| Contract | Result | Notes |
|----------|--------|-------|
| Snapshot-tail (`TestE3RecoveryExactSnapshotTailReplay`) | **PASS** | `snapshot_used=true`, `start_seq=10000`, `tail_replayed=1`, `total=10001`, recovery ~9.2 s, wall ~3213 s (`/tmp/fireweed-e3-10m-snapshot.log`) |
| Genesis (`TestE3RecoveryExactGenesisReplay`) | **PASS** (re-run after log-pagination fix) | `snapshot_used=false`, full 10M commands, recovery ~117 s, wall ~10473 s |
| Offline reject inexact range / checksum drift | **PASS** | No S3 required |

**Pagination fix:** `ObjectLogEngineStore::read_from` always returns a next cursor on non-empty pages so
a 4 MiB fetch that returns fewer than `limit` entries cannot truncate multi-million genesis replay.

**Residual before full E3 ledger PASS:** governed `scripts/perf/tp002-e3-s3.sh` cost/ack matrix and
semantic verifier packaging (sibling beads under `pqueue-820565a9` / `pqueue-c4e5f691`). Store PUT/GET/LIST
cost linkage remains best-effort under object-log 0.2 `MediaOpStats`.

The 2026-07-16 run is retained only as
`docs/perf/evidence/tp002-e3-objectlog-minio-historical-invalid.jsonl`. It is invalid under the current
contract because it used host-speed thresholds, excluded in-memory recovery, allowed a zero SQLite tail,
and lacks production replay/resource and measured-byte provenance. It must not be consumed as release
evidence. This preparation change deliberately does not fabricate replacement measurements.

## Provider-neutral command

The governed producer is `scripts/perf/tp002-e3-s3.sh`. It accepts an endpoint, signing region, isolated
bucket, credentials, a stable topology ID and description, an explicit durability claim, and an authority
mode. Credentials are supplied by the operator and are never stored in evidence. The operator must also
provide a provider-safety adapter: this is the only provider-specific component, and is the authority that
authenticates with the supplied credentials and performs the remote preflight/cleanup operations.

```bash
FIREWEED_S3_TEST_ENDPOINT=https://s3.example.invalid \
FIREWEED_S3_TEST_REGION=us-east-1 \
FIREWEED_S3_TEST_BUCKET=isolated-fireweed-e3 \
FIREWEED_S3_TEST_ACCESS_KEY=... \
FIREWEED_S3_TEST_SECRET_KEY=... \
FIREWEED_E3_STORAGE_TOPOLOGY_ID=provider-topology-1 \
FIREWEED_E3_STORAGE_TOPOLOGY='operator-verified isolated S3 topology; host durability excluded' \
FIREWEED_E3_STORAGE_DURABILITY_CLAIM=excluded \
FIREWEED_E3_AUTHORITY_MODE=native-create-only \
FIREWEED_E3_S3_BUCKET_MODE=create \
FIREWEED_E3_S3_BUCKET_ACK=isolated-fireweed-e3 \
FIREWEED_E3_RUN_ID=20260728-release-001 \
FIREWEED_E3_EVIDENCE_DIR=/absolute/path/to/new-empty-e3-evidence-dir \
FIREWEED_E3_S3_PROVIDER_IDENTITY=example-s3-control-plane \
FIREWEED_E3_S3_PROVIDER_ADAPTER=/absolute/path/to/example-s3-safety-adapter \
scripts/perf/tp002-e3-s3.sh
```

`FIREWEED_E3_S3_BUCKET_MODE` is exactly `create` or `preexisting`. `create` asks the adapter to create or
verify the named isolated bucket; `preexisting` requires an already provisioned, exclusive bucket. In both
cases `FIREWEED_E3_S3_BUCKET_ACK` must exactly equal `FIREWEED_S3_TEST_BUCKET`, so a cleanup invocation
cannot silently retarget another bucket. The current Rust E3 harness has no object-key-prefix setting, so a
`preexisting` bucket must be dedicated to this run; the generic runner never performs bucket-root cleanup or
bucket deletion.

`FIREWEED_E3_RUN_ID` is a unique 8--64-character token. The runner derives its own control prefix as
`fireweed-e3-control/v1/<commit12>/<run-id>/`; it is deliberately distinct from E3 test keys. The evidence
directory must already exist and be empty. The runner derives the fencing, transaction, ledger, contract,
and composition-fingerprint paths under it, preventing stale or caller-selected artifact paths from being
accepted. The recorded SHA-256 composition fingerprint covers non-secret run composition only; credentials,
the Postgres DSN, and adapter output are excluded.

The adapter is an executable invoked as:

```text
adapter capabilities|create-bucket|prefix-empty|nonce-write-read|nonce-validate|cleanup-prefix \
  --provider-identity <declared-token> --endpoint <endpoint> --region <region> \
  --bucket <acknowledged-bucket> --bucket-mode <mode> --bucket-ack <same-bucket> \
  --run-id <run-id> --run-prefix <derived-prefix> --nonce <fresh-nonce>
```

The runner calls `create-bucket` only in `create` mode. `capabilities` must verify the declared control-plane
identity and that the adapter can safely list, write/read, and delete exactly the supplied prefix.
`prefix-empty` must reject a nonempty supplied prefix; `nonce-write-read` and `nonce-validate` prove that a
fresh nonce belongs to this run. `cleanup-prefix` is called only after the generated E3 contract has
semantically recomputed the ledger, TP-003 transaction matrix, and executed fencing proof. It must list and
delete only the exact nonempty `--run-prefix`, then relist it empty; it must not delete a bucket, bucket root,
or sibling prefix. On every failed preflight, test, freshness, provenance, or semantic-verification path the
runner deliberately skips cleanup, retaining the provider namespace for investigation. Adapter stdout/stderr
is suppressed by the wrapper; adapters must not emit credentials.

`native-create-only` is the only supported authority mode. The endpoint must
provide atomic conditional object creation; the run fails closed when it does
not, rather than adding another storage authority.

## Local MinIO convenience profile

Start a fresh MinIO instance with a tmpfs data volume:

```bash
docker run -d --name fireweed-e3-minio \
  --tmpfs /data:rw,size=16g \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
IP=$(docker inspect fireweed-e3-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
FIREWEED_S3_TEST_ENDPOINT="http://$IP:9000" scripts/perf/tp002-e3-minio.sh
```

The MinIO script only verifies its container/tmpfs topology and supplies a local profile to the generic S3
wrapper. It is not the governed release producer. Supply the same run directory, bucket lifecycle
declaration, identity, and MinIO-capable safety adapter shown above when using it. The generic wrapper fixes the release workload at
10,000,000 resident items, 100,000 single-item acknowledgement
pushes per bound, acknowledgement concurrency 384, load batch 1,000, load concurrency 8, an 896 KiB
recovery-load segment target, and seed 0. Before the load starts, the harness canonically serializes the
first eight commands and fails closed unless the smallest four exceed the segment target by at least 10%,
the smallest three remain below it, each command remains below the target, and the full wave's conservative
byte-admission charge is at most
half the 16 MiB per-queue cap. These are byte-shape checks, not elapsed-time or host-performance gates.
The local convenience profile requires a 16 GiB MinIO `/data` tmpfs: the exact 10M workload exhausted the
prior 8 GiB topology with MinIO HTTP 507 (`XMinioStorageFull`) before evidence could be accepted. This local
capacity correction does not constrain another S3-compatible provider topology, add a host-speed gate, or
make a durability claim.

## Historical local topology and hardware

- One local Rust test process drove one MinIO container over live HTTP/S3 at its bridge IP.
- The invalid historical run used MinIO `RELEASE.2025-09-07T16-13-09Z` on an 8 GiB `/data` tmpfs; the fresh
  governed release run uses a 16 GiB tmpfs after the exact workload exhausted the former capacity.
- Docker server 29.1.3 ran under Linux 6.6.87.2 WSL2.
- Host: AMD Ryzen 9 5950X, 16 cores / 32 threads, 94 GiB RAM.
- No cluster orchestration or published loopback port was used.

The historical run took 3,332.60 seconds (55m32.60s); that duration is not a gate or current evidence.

## Ack throughput and latency

Throughput and p50/p95/p99 latency are topology-bound capacity observations,
not release thresholds. Release judgment uses exact committed work, valid
distribution ordering, five alternating same-run recorder-control blocks with a
stable schedule/fingerprint check, and fixed resource bounds; it does not
require a quiet or specially selected host.

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
   fingerprints and a stable recorder-control schedule/fingerprint check.
5. Semantic validators reject smoke rows, incomplete matrices, missing or altered counters, stale provenance,
   non-exact/checksum-unverified recovery, quiet-host deferral, and host-speed gates.
6. Focused tests, formatting, and warning-denied clippy are the code gates. The only remaining evidence gate is
   a coordinated live 10M run through the generic S3 wrapper and validation of its generated release ledger;
   PREPARED is not PASS.
