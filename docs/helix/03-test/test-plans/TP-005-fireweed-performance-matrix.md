---
ddx:
  id: tp-fireweed-performance-matrix
  depends_on: [architecture, api-queue-claim-commit-contract, tp-fireweed-facade-and-snorri-acceptance, tp-scale-substantiation, contract-construction-config, contract-diagnostics]
  status: accepted
  review:
    self_hash: af91841838128efbd8aec308c20b127aef0fbc4f50e328b6a210674b9ba73080
    deps:
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-orthogonal-log-projection-composition: 5e35283d3ad0cc38c61d57aac7a63ce7c5fc8028bc8ff5f51a2bb4c28a1f13e6
      api-fireweed-rust-facade: 26104ab47a5ecfa0f2fea739303d599d3a414461770f73e48a87a14dd48cba37
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
      tp-fireweed-facade-and-snorri-acceptance: b0646721bd9f5ec8bdcdf1698ff07fa03fe167f0bcd8991d57c35fcbb79493cc
      tp-scale-substantiation: 23f20e8dab88330e4ddd165a0d2230151b7ef0f99ca16c016671558ed5719686
    reviewed_at: "2026-08-04T04:50:53Z"
---

# TP-005: Comparable settled queue benchmarks

## Scope and prerequisites

Compare PostgreSQL and S3/object-log with stock Turso 0.8.2 through the identical API-007 facade and one Snorri adapter. Each backend must first pass the complete TP-004 public-path suite and backend durability/fault tests with zero skips. Internal primitives, legacy APIs, ACK-only measurements and debug 10,000-row runs do not qualify.

The Architecture, PRD and TP-002 own correctness, numeric progress, 10M resident scale, 1,000-cold-plus-hot density and bounded-resource requirements. This benchmark supplies matched measurements; it cannot silently waive those gates or turn a historical throughput observation into a universal pass bar.

The [first-principles performance model](../../00-discover/first-principles-performance-model.md)
owns the applicable numeric capacity targets and host/commit envelopes. Qualify
its local headline and backend-specific matrix rows separately from the Snorri
10,000-recipient/second campaign gate (12,500 stretch). Report each applicable
row's measured result against its declared target; neither a passing consumer
campaign nor correctness progress watchdog replaces primitive qualification.

## Matched run design

Use controlled operator-selected hardware, release builds, identical useful-work counts, seeds, payload/body/state distributions, selectors, queue definitions, batching, concurrency, lease durations and semantics. Declare host, storage/provider, latency/topology, toolchain, dependency revisions and all effective limits. Randomize/interleave backend order and repeat independent runs; retain raw samples and controls. Conventional CI may compile and verify evidence or run explicitly non-authoritative smoke checks; authoritative timing requires the declared host envelope.

Measure insert, natively filtered priority claim, atomic commit with workflow state and next work, state/query/admin and exact statistics. Include homogeneous and heterogeneous batches, rejected atomic groups, retries, replay, sparse/dense field filters, wide selectors, hot/cold queues and real consumer transitions. Report each operation's p50/p95/p99, throughput, bytes and useful completed work. State/body coverage and byte limits must match; a faster reduced response is a different workload.

## Settlement and correctness

A measurement ends only when all submitted useful work reaches its declared terminal state, every acknowledged mutation is covered by successful reads/claims, projection debt is drained, and owned workers/uploads/scratch are terminal. Record admission wait, publication, projection/recovery, response, settlement and drain intervals separately. Include any debt left at ACK in total settled time and cost. An ambiguous outcome must resolve through its identity before final counts.

Require exact inserted/claimed/committed/state/new-work counts, no simultaneous leases, no lost or duplicated work, exact priority/progress oracle outcomes, immutable request identities, and all cold queues making bounded monotonic progress. Record any failure; do not drop failed or slow runs from summaries.

## Required campaigns

- Ten million resident writable, claimable and observable work records, followed by exact reopen/recovery verification.
- One thousand cold queues plus a designated hot queue using bounded shared admission, native-worker, connection and memory pools.
- Numeric progress under sustained load and sparse/dense filtering, including ageing and scheduled/expired lease eligibility.
- Density and cost curves varying batch size, object-log segment/flush policy, payload/state coverage and concurrency while preserving the same contract.
- Recovery and provider/process failure runs with settlement, exact counts, tail replay and original-genesis/fencing checks.

Full manifest, projection, SQL VM/planner/cache, SDK buffers and runtime memory contribute to RSS. Bounded application scratch alone does not prove total memory bounds. Record peak/steady RSS, CPU, disk/network bytes, connections, object requests, log/projection debt, storage growth and provider costs per useful completed work. Publish declared limit consumption and same-run degradation as resident work, load and queue count increase.

## Evidence and acceptance

Write runtime JSONL, raw samples, provider attestations and diagnostic captures into run-owned target artifacts or the declared evidence store, outside active Helix specifications. Include immutable source/build pins, effective configuration, repetition/order, seed, timestamps, receiver/bridge versions, redaction/loss status and verifiable hashes. Verify positive examples and rejection controls for incomplete settlement, mismatched useful work, stale reads, missing counts, lost samples and exceeded limits.

Qualification requires comparable complete settled runs and all PRD/TP-002 correctness/progress/resource gates. Report observed capacity with its host envelope. Never infer a speed advantage from ACK latency, an unsupported constructor or historical filesystem/Turso results.
