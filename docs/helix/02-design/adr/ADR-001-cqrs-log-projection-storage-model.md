---
ddx:
  id: adr-cqrs-log-projection-storage-model
  depends_on:
    - product-vision
    - prd
    - concerns
  review:
    self_hash: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
    deps:
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      product-vision: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    reviewed_at: "2026-07-20T00:01:21Z"
---

# ADR-001: CQRS Log Projection Storage Model

## Context

fireweed must provide durable priority queue semantics without making application
nodes durable authorities, requiring node-to-node discovery, or embedding a
cluster consensus algorithm. It must support batch push, priority update, batch
claim, lease renewal, finalize, retry, failure, progress bounds, and recovery at
10M-item queue scale while preserving exact outcomes, queue-global progress,
and bounded shared resources under concurrent load (TP-002 E0). These targets are delivered
in two committed v1 envelopes: a single-deployment envelope (delivered by
`postgres_native`), and a horizontal envelope (delivered by **cross-queue
scale-out** — distributing whole queues across nodes via control-plane-lease
per-queue ownership over a log-backed second backend, `object_log_sqlite_projection`,
and/or independent deployments). Both are substantiated by recorded benchmark
evidence (TP-002).

The preferred deployment shape separates a low-rate control plane from a
horizontally scalable data plane:

- The control plane may be centralized and transactional. It owns tenant,
  queue, shard, placement, epoch, and backend configuration metadata.
- Postgres is the preferred control-plane storage backend across operating
  modes.
- The data plane owns hot queue operations. It must scale by tenant, queue, and
  shard, and must not require a centralized coordinator in the hot path.
- Application nodes are stateless or rebuildable. They may hold local hot state,
  but acknowledged queue state must survive node loss.

The core storage question is where durable acknowledgement happens. Users have
different latency and cost requirements. A delivery system such as Seventh Sense
can often accept batched commit latency if the system preserves durability,
progress, batching, and concurrency correctness. Other users may pay for a fast
log backend to achieve lower commit latency for small batches.

## Decision Drivers

- Avoid node discovery, leader election, Raft/Paxos, ZooKeeper, and etcd inside
  fireweed.
- Keep the queue data plane horizontally scalable by tenant, queue, and shard.
- Prefer Postgres for control-plane metadata, assignment, epoch, and backend
  configuration across all data-plane storage profiles.
- Let users choose a latency/cost profile for durable commits.
- Preserve a simple correctness model: no acknowledged command is lost after
  node failure, and no successful response is externally visible as anything
  other than committed queue state.
- Keep local SQLite useful for priority scans and claim performance without
  making local disk the source of truth.
- Make Postgres-native operation a first-class mode for small deployments and
  teams with a strong managed Postgres provider.
- Bound replay cost through snapshots and log retention.
- Require object-storage log backends to batch commands into durable segments so
  they preserve a reasonable cost profile, with a configurable commit-latency
  bound that directly controls the latency/cost tradeoff.
- Support multiple durable log implementations, including Postgres, Kafka,
  S3-compatible object storage, DynamoDB, Aurora, and Redpanda-like systems.

## Considered Options

### Option 1: Postgres/Transactional Database as Queue Authority

Postgres, Aurora, or DynamoDB own durable writes, claim concurrency, leases,
finalization, and fencing directly. In the Postgres-native mode, fireweed uses
Postgres both as the durable command log and as the operational queue store
accessed through ordinary Postgres connections.

This is expected to be the optimal usage pattern at small scale when the user
has a good Postgres provider: it minimizes moving parts, gives familiar
transactional semantics, supports low-latency small commits, and avoids
operating a separate log system. At larger scale, it risks turning a database
into the central data plane unless queue/shard placement maps to independent
database partitions, tables, schemas, or clusters.

### Option 2: Kafka/Redpanda-Style Durable Log

A partitioned streaming log owns durable append and replay. fireweed projects log
commands into a local execution index and uses the log partition model as the
data-plane serialization boundary.

This fits high-throughput append workloads and avoids fireweed-owned consensus,
but claim/finalize semantics still need a carefully designed projection,
checkpoint, and compaction model.

### Option 3: S3-Compatible Object Log with Batched Commits

fireweed buffers command batches, writes sealed immutable log segments to object
storage, commits a manifest entry, acknowledges commands in the committed
segment, and projects the commands into SQLite. SQLite snapshots are also stored
in object storage so old log segments can expire after a safe recovery window.

This gives the strongest cost story when clients can send large batches and
accept commit latency. Single-command object writes are not a viable production
S3 profile; the required tradeoff is lower cost in exchange for batched commit
latency. Small, latency-sensitive commits should use a faster log backend.

### Option 4: Local SQLite WAL as Authority

Each data-plane node stores commands and projection state in local SQLite, then
ships WAL or snapshots elsewhere asynchronously.

This is fast, but it fails the durability requirement unless acknowledgement is
delayed until the durable external log boundary is reached. Local SQLite can be
the projection, not the only authority.

## Decision

fireweed will use a CQRS-style log projection storage model.

The durable command log is the source of truth. Local SQLite is the first
candidate projection store for priority ordering, eligibility scans, leases,
batch claim, and replay catch-up. SQLite snapshots may be written to object
storage and used to bound replay time, but snapshots do not replace the command
log until the snapshot is committed and the corresponding log-retention window
is safe to expire.

Every queue state transition is represented as a command in the durable log,
including:

- enqueue
- batch enqueue
- priority update
- metadata or eligibility update
- claim
- lease renewal
- complete
- retry
- rearm (recurring re-arm: release lease, set caller-supplied next eligibility,
  record effective eligible instant = max(commit_time, not_before), set optional
  priority, reset per-cycle retry counter, do not count as attempt; record
  effective not_before/eligible_since/priority/version for replay)
- purge (targeted in-band removal of an item regardless of lifecycle state;
  deletes the item row, records a terminal command position + tombstone; force
  variant invalidates an active lease)
- fail
- release
- repair or administrative state transition

fireweed acknowledges a command only after the configured `LogStore` durability
profile says the command is committed and the operation's accepted effects are
visible through the serving projection or equivalent response barrier. The
chosen backend defines the latency and cost tradeoff; it does not define a
weaker API contract:

- A Postgres-native backend can combine the command log, operational queue
  indexes, leases, idempotency state, and control-plane metadata in one managed
  Postgres deployment for small-scale or provider-backed usage.
- A fast log backend can commit small batches at lower latency and higher cost.
- An object-log backend must commit command segments, not individual commands,
  and trades lower cost for higher acknowledgement latency.
- A transactional backend can combine log, claim, and lease authority, but must
  still be deployable without centralizing the whole data plane.

Every backend profile MUST preserve API-001's external transaction contract:
success means durable and visible; structured rejection means no committed effect
for the rejected scope; and unknown outcomes are resolved by `request_id`
without duplicate state-machine transitions. Local projections, segment buffers,
manifest publication, and replay are internal mechanisms only.

The storage API must be capability-based rather than one flat generic store:

| Capability | Responsibility |
|------------|----------------|
| `LogStore` | Append and read durable command records by tenant/queue. |
| `ProjectionStore` | Maintain local query state optimized for priority claim and lease operations. A **family** of implementations — an in-memory log-replay projection and a relational / DB-resident projection — held identical by the conformance suite (ADR-008), not a single shared implementation. See ADR-013 for the single-source-of-truth amendment and durability Class A vs Class B (memory log). |
| `SnapshotStore` | Persist projection snapshots at durable log positions and support bounded replay. |
| `ControlPlaneStore` | Store queue metadata, queue assignment, backend configuration, epochs, placement, and queue-owner leases (TD-003). |

Postgres is the preferred implementation for `ControlPlaneStore` across all
operating modes. The control plane is a **pluggable seam**: a backend-specific
control plane (e.g. an object-store implementation enabling a no-Postgres
object-log deployment) may be supported later, but it must justify why Postgres
is not sufficient for low-rate metadata, assignment, epoch, and configuration
state — the object-store candidate is deferred pending a CAS-atomicity spike
(ADR-008).

The core implementation must not assume that all backends provide the same
latency, concurrency, retention, or transaction semantics. Backend adapters must
advertise their durability boundary, batching behavior, replay contract,
retention behavior, and supported concurrency model.

### Postgres-Native Operation Mode

Postgres-native mode is a first-class operating profile, not just a generic
adapter. It is intended for small deployments, early adoption, self-hosters, and
teams whose infrastructure already includes a strong managed Postgres provider.

In this mode:

- `LogStore` is an append-only Postgres command table or partitioned table set.
- `ProjectionStore` is a Postgres operational schema for priority indexes,
  eligibility, leases, idempotency, and finalization state.
- `ControlPlaneStore` uses Postgres and may share the same Postgres deployment
  at small scale.
- SQLite projection is optional rather than required.
- Queue placement (one owner per queue, ADR-008) and optional internal item-table
  partitioning exist in the schema so the deployment can migrate later to
  object-log, Kafka/Redpanda, or local SQLite projection modes.

This mode trades maximum horizontal scale for simplicity, low operational
overhead, low-latency small commits, and provider-managed durability. It remains
valid only while the selected Postgres deployment can meet the queue's
throughput, contention, retention, and noisy-neighbor targets.

### S3/Object-Log Commit Model

For S3-compatible object storage, the intended model is group commit:

1. Buffer commands per tenant/queue/shard until a size or time threshold.
2. Seal a segment with checksums and monotonic command positions.
3. Write the segment to object storage.
4. Commit a manifest entry or equivalent durable segment pointer.
5. Treat the manifest commit as the durable boundary that makes commands
   eligible for acknowledgement.
6. Apply committed commands to the local SQLite or in-memory projection, or
   otherwise construct the operation's response from committed state.
7. Acknowledge a command only after that command's accepted effects are durable
   and externally visible to later reads, claims, idempotency replay, and
   recovery.
8. Periodically snapshot SQLite to object storage at a committed log position.
9. Expire log segments only after a valid snapshot and recovery window cover
   those positions.

This design makes S3 viable for cost-optimized workloads that can send large
client batches and tolerate batched acknowledgement latency. S3 adapters should
reject or strongly discourage production configurations that write one object
per command; that shape has poor request cost, poor object-count behavior, and
does not use S3's economics correctly.

The object-log profile exposes a commit-latency bound (implemented by segment
time/size thresholds such as `segment_max_latency_ms`) so operators can choose
the point on the latency/cost curve. Lower latency bounds create more segments
and object-store requests; higher bounds improve batch density at the cost of
mutation latency. This knob is never a correctness knob.

### Scale Claim Scoping

Per the cross-document Scale-Claim Rule, every scale claim references an evidence
record naming deployment shape + workload envelope + substantiating artifact.
This ADR's backend menu maps to two **delivered v1** envelopes:

- **Single-deployment envelope** — delivered by `postgres_native` (TD-002). Its
  ceiling is that of a well-tuned single-Postgres `SKIP LOCKED` priority queue;
  v1's advantage here is durable queue semantics. Evidence: TP-002 E1 against
  E0's portable correctness, progress, and resource contract. Rates and
  percentiles describe the measured topology's capacity.
- **Horizontal envelope** — delivered by **cross-queue scale-out**: distributing
  whole queues across nodes via per-queue ownership (TD-003) over the
  **`object_log_sqlite_projection`** second backend (TD-004) and/or independent
  `postgres_native` deployments. A single deployment alone MUST NOT be cited as
  evidence for this envelope. Evidence: TP-002 E2 (cross-queue scale-out) and E3
  (object-log latency/cost + recovery).

| Claim | Substantiated by (committed v1) | Evidence record |
|-------|--------------------------------|-----------------|
| Single-deployment exact outcomes and queue-global progress under load with bounded shared resources | `postgres_native` (TD-002) | E1 vs E0 |
| Write/claim load scales beyond one deployment by distributing queues across nodes while exact outcomes, per-queue progress, fencing, and bounded shared resources are preserved | cross-queue placement + per-queue ownership (TD-003) + object-log backend (TD-004) | E2 |
| Per-queue progress bound holds on the queue's single owner | per-queue oldest-eligible tracking (TD-003); queue-local, no cross-shard aggregation | E1 |
| Lower $/command + bounded recovery at high volume | `object_log_sqlite_projection` and local-projection object-log variants using group commit + projection rebuild (TD-004) | E3 |

### Napkin Cost Comparison

These numbers are directional. They use public us-east-1 pricing observed while
drafting this ADR and intentionally exclude data transfer, support plans,
PrivateLink, backups beyond stated retention, compression differences, and
operator labor.

Baseline workload:

- 1 billion durable queue commands per month.
- 1 KiB encoded command record.
- Roughly 1 TiB logical log ingest per month.
- One durable append per command before batching.
- 30-day month.
- Object-log snapshots allow old command segments to expire, so object-log
  storage cost depends on recovery window rather than total historical ingest.

Pricing inputs:

Source URLs:

- AWS S3 pricing: https://aws.amazon.com/s3/pricing/
- AWS MSK pricing: https://aws.amazon.com/msk/pricing/
- AWS DynamoDB pricing: https://aws.amazon.com/dynamodb/pricing/on-demand/
- AWS Aurora pricing: https://aws.amazon.com/rds/aurora/pricing/
- AWS public offer files:
  https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/

| Service | Public input used |
|---------|-------------------|
| S3 Standard | $0.023/GB-month, $0.005/1K PUT/COPY/POST/LIST, $0.0004/1K GET. Source: AWS S3 pricing page and AWS AmazonS3 offer file, publication `2026-05-28T22:27:23Z`. |
| MSK Provisioned | `kafka.m7g.large` at $0.204/hour, three brokers about $441/month; storage $0.10/GB-month. Source: AWS MSK pricing page and AWS AmazonMSK offer file, publication `2026-04-22T04:05:53Z`. |
| MSK Express | `express.m7g.large` at $0.408/hour, three brokers about $881/month; $0.01/GB ingest; storage $0.10/GB-month. Source: AWS MSK pricing page and AWS AmazonMSK offer file, publication `2026-04-22T04:05:53Z`. |
| DynamoDB on-demand | $0.625/million 1 KiB write request units, transactional writes 2x, storage $0.25/GB-month beyond free tier. Source: AWS DynamoDB pricing page and AWS AmazonDynamoDB offer file, publication `2025-08-28T15:38:19Z`. |
| Aurora PostgreSQL | `db.r7g.large` at $0.276/hour standard or $0.359/hour I/O-Optimized; standard storage $0.10/GB-month; standard I/O $0.20/million; I/O-Optimized storage $0.225/GB-month. Source: AWS Aurora pricing page and AWS AmazonRDS offer file, publication `2026-06-05T18:53:43Z`. |
| Redpanda self-managed proxy | Three EC2 `i4i.large` nodes at $0.172/hour each, about $372/month before Redpanda licensing/support/ops; larger nodes scale linearly. Source: AWS AmazonEC2 offer file, publication `2026-06-04T22:00:57Z`. |

Directional monthly cost for the baseline:

| Backend | Approximate Monthly Cost | What Dominates | Read |
|---------|--------------------------|----------------|------|
| S3 object log, 1-command objects | ~$5,000 in PUTs + storage | Request count | Non-starter for production; useful only as a contrast case or development fallback. |
| S3 object log, 1 MiB segments | ~$10 in PUT/manifest requests + $1-$25 storage window | Commit latency, not dollars | Best cost profile if clients and server use large batches. |
| S3 object log, 16 MiB segments | <$2 in PUT/manifest requests + storage window | Commit latency, not dollars | Very cheap, but requires larger batches and tolerates slower ack. |
| Postgres-native managed provider | Provider-dependent; often one database bill and no separate log cluster | Managed database compute/I/O | Expected best default at small scale because it minimizes operational surface and handles small commits well. |
| DynamoDB on-demand | ~$625 non-transactional writes; ~$1,250 transactional writes; + up to ~$250/TiB storage retained | Per-command write units | Good low-latency authority, but steady high write volume is meaningfully more expensive than batched S3. |
| MSK provisioned | ~$441 broker floor + storage | Fixed cluster floor | Good fast log option once traffic justifies the cluster; inefficient for small deployments. |
| MSK Express | ~$881 broker floor + ~$10/TiB ingest + storage | Fixed cluster floor | Higher floor; may buy operational/performance characteristics rather than raw cost efficiency. |
| Redpanda self-managed proxy | ~$372 for 3 `i4i.large` nodes before license/support/ops | Compute and operations | Potentially cheaper fixed floor than MSK, but fireweed users inherit more operational responsibility unless managed Redpanda is used. |
| Aurora PostgreSQL standard | ~$397 for two `db.r7g.large` instances + ~$100/TiB storage + ~$200 per billion I/Os per I/O touched | Compute plus I/O | Strong transactional semantics; must shard carefully to avoid central data-plane bottleneck. |
| Aurora PostgreSQL I/O-Optimized | ~$517 for two `db.r7g.large` instances + ~$225/TiB storage | Compute plus storage | More predictable for I/O-heavy logs, but fixed cost and centralized write limits still matter. |

Interpretation:

- S3 is the cost floor only when commands are batched into segments and
  acknowledgement latency can include group-commit time.
- Postgres-native mode is likely the best small-scale default when a good
  Postgres provider is available: one familiar system, real transactions, and
  no separate broker or object-log machinery.
- DynamoDB and Aurora are simpler correctness authorities for low-latency
  writes, but their per-command or I/O economics matter at billions of
  transitions.
- MSK and Redpanda are attractive when fireweed needs a fast append log with high
  throughput and replay, and the fixed cluster cost is acceptable.
- Seventh Sense-like workloads may be a strong fit for S3 object-log durability
  if producers send large batches and business latency tolerates batched
  acknowledgement.

## Consequences

Positive:

- fireweed avoids implementing cluster consensus and avoids node-to-node ownership
  negotiation in the data plane.
- The durability model is explicit: acknowledged state is in the durable log.
- Users can choose latency/cost tradeoffs by selecting a backend and batch
  profile.
- Postgres-native mode gives a simple, credible small-scale deployment path.
- A Postgres control plane (the preferred, pluggable `ControlPlaneStore`) gives
  every data-plane mode one consistent place for queue metadata, queue assignment,
  backend configuration, and epoch/fencing state.
- SQLite remains useful for fast local claims without becoming an unrecoverable
  authority.
- Snapshots let object-log deployments expire old command segments and keep
  storage cost tied to the recovery window.

Negative:

- The storage API is more complex than a single database adapter.
- Postgres-native mode can hide scaling limits if used past one deployment's
  ceiling; v1 mitigates this by committing the cross-queue placement mechanism
  (per-queue ownership, TD-003) and the `object_log_sqlite_projection` second
  backend (TD-004), each with a
  benchmark gate, so the horizontal envelope is delivered and tested rather than
  assumed.
- Every backend needs conformance tests for durability, replay, idempotency,
  ordering, batch commit, and crash recovery.
- S3/object-log deployments require careful client and server batching to avoid
  high acknowledgement latency.
- Transactional backends can accidentally become centralized data planes unless
  shard placement is part of the design.
- Projection bugs can cause temporary execution errors even if the command log
  remains correct.

Follow-up design work:

- Define the `LogStore`, `ProjectionStore`, `SnapshotStore`, and
  `ControlPlaneStore` traits.
- Define the Postgres-native adapter as an explicit reference backend for the
  first technical design pass.
- Define the Postgres `ControlPlaneStore` schema as the preferred cross-backend
  control-plane implementation.
- Define command record schema, idempotency keys, checksums, command positions,
  and shard epoch fields.

Committed v1 design artifacts (no longer open spikes):

- **TD-002** — `postgres_native` reference backend (single-deployment envelope).
- **TD-003** — queue ownership (the queue is the unit of sharding, ADR-008):
  deterministic per-queue assignment from the control plane (target vs active
  owner), storage-backed per-queue leases, monotonic epoch fencing durably bound
  to the log before a new lease is usable, queue rebalance, graceful drain, and
  recovery. Progress is per-queue/local (no cross-shard aggregation). No
  ZooKeeper/etcd/embedded consensus. This substantiates the cross-queue
  horizontal-scale direction without an external coordinator.
- **TD-004** — `object_log_sqlite_projection` backend: group-commit sealed
  segments, manifest commit (conditional/CAS) with fencing against the current
  control-plane epoch, in-flight claim reservation, SQLite projection, periodic
  snapshot to object storage, and bounded replay. Validates the object-log commit
  model in this ADR.
- **Object-log group-commit latency/cost across segment sizes** and **SQLite
  projection rebuild time at 10M-item shard scale** are now benchmark exit
  criteria inside TD-004 / TP-002 E3, not open spikes.
- **Operator-contract dependency**: shard placement/rebalance/drain (TD-003) and
  backend migration require the separate operator contracts API-001 calls out.
  TD-003 specifies the *mechanism* and its automated control-plane assignment;
  the operator-facing administrative surface (manual rebalance, drain commands) is
  the only piece that remains an operator-contract follow-up and MUST NOT block
  the automated v1 mechanism.
- Compare `postgres_native`, `object_log_sqlite_projection` (and later
  Kafka/DynamoDB) with the same conformance suite (TD-001).

## Status

Accepted as the initial storage architecture direction. Backend selection and
implementation details remain subject to technical spikes.
