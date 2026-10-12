---
ddx:
  id: td-object-log-turso-projection
  depends_on: [architecture, api-queue-claim-commit-contract, td-storage-architecture-backend-contracts, contract-construction-config, contract-durable-queue-records, contract-formal-queue-semantics]
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: concerns}
  status: accepted
  review:
    self_hash: d05ba185fd6e0be01855a97f39abbce04a486adf4a6f2c69f14e236befecc25f
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      adr-turso-derived-projection: b93a1a9c4ba242940b86878551dddd35f9aa4e399357417c620e66f5ab2a7b67
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      concerns: d00e29334f99ed2fe3c9151bacb107255a3d7add89606949e409eb6614382d6c
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    reviewed_at: "2026-08-04T04:50:53Z"
---

# TD-010: S3 authority and stock-Turso queue projection

## Scope

Implement the same API-007 contract and acceptance tests as PostgreSQL. Follow
Architecture/TD-001 and durable-record, construction and Snorri Contracts.
Stock Turso 0.8.2 is required; a custom fork does not qualify this backend.

## Technical Approach

Acquire authoritative partition state/fence with matching epoch and committed
head. Bind full namespace and exact original genesis/definition/secret. Native
conditional publication checks that epoch and expected head atomically. Uploads,
hints, old epoch documents and a projection watermark do not confer authority.

The durable checksummed record contains the resolved mutation and immutable
result. ACK implies authoritative publication, not merely upload. Stock Turso
applies work/state/fields/indexes/receipts and the external cursor in one native
transaction. Cold recovery validates the exact contiguous history and prefix;
missing known/corrupt authority fails closed, never as destructive empty rebuild.

Native reads/claims must cover acknowledged authority and compare recovered
cursor, insertion sequence and historical epoch in their snapshot. Filtered
candidate selection uses native indexes before ordering/limits, with full key/type
checks. Wide property predicates use fixed-depth relational intersection, not an
unbounded recursive SQL expression or a client-side queue scan. Metadata-only
operations preserve opaque payloads. Claim publication uses shared plan_claim and
an exact ordered logical receipt; replay validates current whole membership.

## Component Changes

Object-log integration owns namespace/head/fence/bounded reads/owned publication.
Turso integration owns native snapshot/query/apply transactions. Core/engine own
resolved codecs and semantic planners; spool owns bounded whole-worker execution
and temporary streaming. Facade owns composition and the mandatory public port.
Apply Architecture's boundary checker; no public vendor or transaction handles.
Native log construction accepts an explicit provisioning-operation capacity and
validates it before directory creation, SDK construction or S3 probes. The facade
factory must supply that capacity alongside its other process resource settings;
legacy constructor defaults do not establish public S3/Turso qualification.
Provisioning retries retain the verified namespace and wait asynchronously for
an initializing authority. The configured delay permits at most seven waits whose
sum is bounded by the pre-position timeout; physical I/O duration is a separate bound.

## API/Interface Design

API-007 owns consumer guarantees; durable-record Contract owns lineage and wire
compatibility; construction Contract owns resource/clock configuration. Original
fingerprint formats, definition pins and outcome ordinals remain unchanged.

## Failure and Resource Ownership

Once dispatched, the whole physical owner retains inputs, aliases, scratch and
output admission until terminal cleanup. Native errors and poll panics await
rollback/autocommit or poison before release/reuse. Caller cancellation cannot
cancel accepted work or release its reservation. Worker completion and final
reservation drain are separate shutdown obligations.

Native conditional publication distinguishes known rejection from ambiguity;
a late reply/deadline cannot prove no effect. Resolve original identity against
fresh authority. Claim/commit ownership uses a fresh operational clock at the
linearization point; explicit classification timestamps do not confer leases.

Full authoritative index/manifest memory and rewrite growth must be bounded and
admitted independently of record payloads. Wrapper byte charges do not qualify
SDK manifests, parser/VM/page-cache residency or process RSS. No published capacity
claim may hide those resources, apply debt or unbounded per-queue actors.

## Testing

Run the identical concrete-handle suite without optional operations or missing
fixture passes. Verify typed/wide native selection, complete state/new-work
atomicity, retained/rejected regrouping, preview, renewal, whole-claim expiry and
original full identity. Real endpoint/native database gates include paused apply,
process loss, stale fencing, missing/corrupt authority, unknown responses,
rollback/poison/cancellation and exact genesis rebuild.

## Formal Specification

CONTRACT-006 and TP-006 require revised bounded properties/correspondence and
positive/negative witnesses. Provider atomic conditional publication and physical
durability remain explicit assumptions exercised by real tests; model checking
cannot certify them or prove universal Rust refinement.

## Diagnostic Wiring

Use caller-owned tracing/export and sanitized adapter-boundary outcomes per
Diagnostics Contract. Report apply/drain debt, bounded resources and loss without
logging raw request values, secrets or bodies. Export failure cannot weaken ACK.

## Performance and Migration

Qualify matched settled release workloads against PostgreSQL, including native
rare-filter density, wide keys/fields, 10M hot work, 1000 cold+hot queues, numeric
progress, final drain, RSS and S3 request/storage cost. ACK-only rate is not useful
completed throughput. Existing wire/body versions require checked migration and
actual consumer conformance; snapshots/truncation are separately qualified and
not shortcuts around original genesis or missing authority.
