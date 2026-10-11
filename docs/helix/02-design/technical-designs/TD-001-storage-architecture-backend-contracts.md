---
ddx:
  id: td-storage-architecture-backend-contracts
  depends_on: [architecture, api-queue-claim-commit-contract, contract-construction-config, contract-diagnostics, contract-durable-queue-records]
  status: accepted
  review:
    self_hash: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-auth-tenancy-and-storage-isolation: 1d4296498a187d4d4c3bb4a4e37647f7193036c3a20ab2a8b66154a45937ece2
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
      adr-granularity-mapping-and-claim-domain: c44740df25ed32569e86f4c3459ed07839d33325367cdafc49fb1317deab606d
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      adr-orthogonal-log-projection-composition: 5e35283d3ad0cc38c61d57aac7a63ce7c5fc8028bc8ff5f51a2bb4c28a1f13e6
      adr-queue-as-shard-unit-and-projection-families: 64a7c7b0e2e5f4caa2c7d775b84c87a9a1e4484ae3df9dccbe3d145d22681a7e
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      concerns: d00e29334f99ed2fe3c9151bacb107255a3d7add89606949e409eb6614382d6c
      orthogonal-storage-matrix-brief: 170b24ca9791ae59d5ab7d2f986b0dce93fce092d4d5da7dd896b50ffd67eaae
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
    reviewed_at: "2026-08-04T04:50:53Z"
---

This is the target design and its acceptance contract. The public PostgreSQL path is implemented; S3/Turso assembly and the single Snorri adapter cutover remain pending. Invalid configuration before all I/O and explicit bounded capacity settings remain implementation and negative-test gates, including checked admission/scratch multipliers before PostgreSQL connection or DDL. Existing code is not qualified merely by this specification.


# TD-001: Queue storage orchestration

## Scope

Apply Architecture's module map to API-007 and the construction, diagnostics,
Snorri integration and durable-record Contracts. This technical design describes
local orchestration; Architecture owns the durable system boundaries.

## Technical Approach

The concrete facade delegates the same mandatory operations to either durable
adapter. Shared core/planners validate schemas, fingerprints, current lease/CAS,
field/body intent, uniqueness and group results. Adapters acquire authoritative
queue serialization and execute resolved effects with their native mechanisms.
Business rules and the Snorri adapter are not duplicated by backend.

PostgreSQL uses native async transactions and indexed filtered row locking.
S3 publishes the resolved unit and immutable result before ACK; stock Turso
atomically applies records, state, fields/indexes, receipts and the external
cursor. Successful reads and claims cover acknowledged authority. Internal apply
lag cannot cause a stale empty claim or consumer projection polling.

## Component Changes

Core owns DTOs/definitions/validation; engine owns pure planning and bounded
codecs/admission; spool owns owned workers and temporary streaming; adapters own
native storage/SDK translation; facade owns construction. Follow Architecture's
allowed/forbidden edges and exact legacy exceptions. Run
`python3 scripts/ci/check_queue_boundaries.py --controls` before feature gates.
Rust visibility and semantic review also check public mutable state, vendor DTO
leakage, invariant bypass and duplicate policy.

## API/Interface Design

API-007 owns ordinary and auxiliary operations. Construction Contract owns all
configuration/default/error precedence; Diagnostics Contract owns event and
capture/query surfaces. This TD does not redefine their fields or signatures.

## Data Model Changes

Work and typed state are first-class records. Keyed version high-water survives
deletion. Full identities and unsigned counters remain exact. Field/body
Preserve/Replace/Edit is resolved separately from partial read knowledge.
Request domains/fingerprints and expired tombstones remain durable. Resolved
administration and compact positional/histogram results share the commit unit.

## Integration Points

PostgreSQL SQL stays in its adapter; Turso statements stay in its adapter; S3
namespace/head/fence/read/append stays in the object-log integration. Hosts own
clock, credentials, process configuration, maintenance and telemetry exporters.
No storage control handle appears in a workflow commit group.

## Security

Full tenant/queue comparisons protect every read/write; digests alone never
identify authority. Parameterize external values. Redact credentials/tokens and
opaque bodies before all diagnostic sinks. Fail closed on corrupt or missing
known authority; a recovery error never authorizes an empty replacement.

## Performance

Preserve native filtered priority/schedule/age order with full insertion-sequence
ties. Key/value/field width does not create a hidden identifier, bind or match cap.
Admission covers payload and structural scratch before copies and aliases.
Retained jobs outlive caller cancellation; physical terminal cleanup and final
reservation drain remain distinct. Native planner/cache residency and full log
metadata must be bounded and measured separately from logical admission.

## Testing

Both concrete constructors run the same mandatory public suite. Include atomic
state/new work, retained/rejected regrouping, renewal/current replay, whole-claim
expiry, wide selectors, preview, collision/ABA, bounds and shutdown. Native
rollback/poison, process loss, ownership races, unknown replies and exact cold
rebuild require real backing stores. TP-004/TP-005/TP-006 own acceptance.

## Formal Specification

CONTRACT-006 governs lease, fence, replay, atomic group, fresh observation, clock,
ABA and budget properties. Revised models require reachable positive/recovery
witnesses, targeted negative controls and actual code/test correspondence under
recorded bounds/assumptions. Superseded stale-empty/identity-reuse models provide
no assurance for the changed binding. Routine structural document changes do
not require a solver; critical protocol readiness requires executable analysis.

## Diagnostic Wiring

Adapters report sanitized outcomes once at their boundary; facade instrumentation
must not duplicate them. Host tracing/export and runner capture follow Diagnostics
Contract. Telemetry failure cannot change durable results or release queue ownership.

## Migration & Rollback

Qualify original definitions, schema/body pins, fingerprint/outcome bytes and
native reopen before cutover. Keep one Snorri adapter throughout. Retire legacy
consumer surfaces only after both complete backends pass; preserve Git history,
not obsolete governing contracts or chronological run output in HELIX.
