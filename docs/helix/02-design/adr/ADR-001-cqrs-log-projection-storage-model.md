---
ddx:
  id: adr-cqrs-log-projection-storage-model
  depends_on:
    - product-vision
    - prd
    - concerns
    - api-fireweed-kernel-contract
  status: accepted
  review:
    self_hash: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
    deps:
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      product-vision: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    reviewed_at: "2026-07-20T00:01:21Z"
---

# ADR-001: CQRS log and projection storage model

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-008, ADR-013, ADR-016, ADR-017, ADR-024, ADR-025, TD-017, TD-018 | High |

## Context

- Every `Ok` from a mutating op means its effects survive process loss (API-006 DUR1). Application nodes must not be the durable authority, and Fireweed must not need node-to-node discovery, leader election or an embedded consensus protocol (Raft, Paxos, ZooKeeper, etcd).
- Claims need an ordered index over each stage that costs O(log n + k + b) (API-006 C9). A log alone cannot serve that.
- Object storage is the cheapest durable store per byte, but it charges per request. One object per command is unaffordable at production volume, so commands must be committed in groups.
- API-006 says nothing about how an implementation stores data (its §8). This ADR fixes the storage model of the production implementation, S3 object log × Turso (ADR-024).

## Decision

1. **The durable command log is the record.** Every change to a queue that a caller requests (a definition, a write unit, a claim, a force-clear, a close) is a command in the log. ADR-013 states the authority rule that follows from this.
2. **The log is an object log on S3, committed in groups.** The log engine buffers commands for up to the linger L (default 20 ms), packs them into immutable data objects, and publishes them with a conditional write of the partition's head document (ADR-008, TD-018). A command is durable once its data object and the head update have both landed.
3. **L is a latency and cost setting, never a correctness setting.** A shorter linger commits sooner and issues more requests; a longer one batches more commands per request. One object per command is not a production configuration.
4. **The serving projection is derived.** A Turso file per partition holds what the ops read and claim from: the stage-ordered claim index, the claims table, the fields and the declared indexes (ADR-016, TD-017). It applies committed commands in log order, and it can be deleted and rebuilt from the log.
5. **Acknowledgment follows the durable commit.** An op returns `Ok` only after every command it reports is durable in the log (DUR1). The implementation meets API-006's visibility rules (V1–V3) while the projection applies behind the log; TD-016 and TD-017 design how.
6. **A control plane owns placement, never data.** It assigns each queue its partition and holds each partition's owner lease (ADR-008). It is off the per-command path. A single-node deployment uses an in-process control plane; a multi-node deployment uses an external control-plane store. Epochs and the append fence live in the log, not in the control plane (ADR-013).
7. **Replay is from genesis until snapshots are measured.** A projection rebuild replays the log from its start. Snapshots with log truncation (TD-018, proposal P1) would bound replay time and storage and would change the rebuild source to "latest snapshot plus retained tail". They are adopted only after measurement and an owner decision.

## S3 cost model

Inputs: S3 Standard in us-east-1 at $0.005 per 1,000 PUTs, $0.0004 per 1,000 GETs and $0.023 per GB-month (AWS S3 pricing page and AmazonS3 offer file, publication 2026-05-28T22:27:23Z), and 730 h per month. One PUT per second, sustained, costs $13.14 a month.

- **Requests follow commits, not commands.** Each commit costs at least two PUTs: a data object and a head update. A busy partition commits at most once per linger, so it costs about 2/L PUT/s however many commands and queues it carries: about 100 PUT/s ($1,314 a month) at L = 20 ms, and 20 PUT/s ($263 a month) at 100 ms (modelled in TD-018, not measured).
- **One object per command is the contrast case.** 10^9 commands a month would cost about $5,000 in PUTs alone.
- **Storage grows with ingest until truncation exists.** 1 TiB of commands retained costs about $23.55 a month, and without P1 every month's ingest is retained.

TD-018 carries the full per-partition PUT and GET model and the measurements that replace it.

## Alternatives

| Option | Evaluation |
|--------|------------|
| A transactional database (PostgreSQL, Aurora, DynamoDB) as the queue authority | Rejected for the production implementation. The database becomes the central data plane, and a shared engine seam bent to fit it (ADR-025). A PostgreSQL implementation may later implement `Store` directly behind the contract (ADR-007); ADR-025 commits to none. |
| A Kafka or Redpanda log | Rejected. A broker cluster has a fixed cost floor and its own operations, and Fireweed exposes no change-log stream that would use it. |
| Local SQLite or Turso WAL as the authority | Rejected. Losing the node's disk loses acknowledged writes. Local files serve only as the derived projection. |
| One object per command | Rejected. Request cost grows with command rate instead of commit rate. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | No consensus protocol and no node-to-node ownership negotiation. Fencing uses conditional writes on the log and control-plane leases. |
| Positive | The durability rule is simple: acknowledged means durable in the object log. |
| Positive | The projection is disposable: losing a Turso file costs a rebuild, never data. |
| Positive | Request cost follows the commit rate, so packing many queues into one partition lowers the cost per queue (TD-018). |
| Negative | Commit latency includes the linger and the S3 round trips. A lone low-rate queue still pays about two PUTs per command. |
| Negative | Rebuild replays from genesis, and log storage grows without bound until snapshots and truncation are measured and accepted. |
| Negative | A projection bug can serve wrong results until the projection is rebuilt, even though the log is correct. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
- AWS S3 pricing: https://aws.amazon.com/s3/pricing/
