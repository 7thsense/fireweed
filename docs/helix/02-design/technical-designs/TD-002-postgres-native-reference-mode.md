---
ddx:
  id: td-postgres-native-reference-mode
  depends_on: [architecture, api-queue-claim-commit-contract, td-storage-architecture-backend-contracts, contract-construction-config, contract-durable-queue-records]
  review:
    self_hash: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:26Z"
---

# TD-002: Native PostgreSQL queue baseline

## Scope

Implement API-007 as the durable reference backend and baseline for the identical
S3/Turso suite. Follow Architecture and TD-001; storage choice does not weaken
consumer semantics or introduce a second Snorri implementation.

## Technical Approach

Use native asynchronous PostgreSQL transactions with queue authority/definition
checks. Native indexes intersect typed field/key predicates before priority,
schedule, eligible-age and insertion-sequence order. Claim leases only that
matching bounded prefix; concurrent claims cannot grant two live credentials.

A commit group validates current leases, versions, workflow/state CAS and final
uniqueness, then atomically persists work/state/new work and its immutable result.
Independent group identities survive regrouping; later groups observe prior
successful effects. Any infrastructure failure rolls back the enclosing native
unit rather than fabricating partial acknowledged results.

## Component Changes

`fireweed-postgres/src/queue_store.rs` and owned submodules own SQL/execution.
Core and engine own shared policy/codecs; facade owns construction. The native
queue path has no object-store/Turso runtime requirement. Architecture records
one exact legacy PostgreSQL→object-log composition dependency to remove at cutover;
no new import location or SDK edge is allowed. Run the real boundary checker.

## API/Interface Design

Use API-007 and the construction, durable-record and Snorri Contracts. Both
backends share DTOs, limits, fingerprint/outcome semantics and conformance.
PostgreSQL driver/session types remain private.

## Data Model Changes

Persist original definitions, work/state, typed declared/additional fields,
indexes, request fingerprints/results and expired identities. Numeric columns
retain full unsigned versions/sequences and exact signed timestamps. Key version
high-water survives deletion; complete names/values disambiguate all hash probes.
Metadata-only operations preserve omitted bodies/fields without hydration.

## Security and Diagnostics

Bind complete tenant/queue scope in every storage operation. Typed configuration
validates before connection/fence effects and displays secrets redacted. Gate
corrupt fixed-width authority and large values before materialization/admission.
The host owns telemetry export; follow the Diagnostics Contract without SQL-value
or payload logging.

## Testing

Run real PostgreSQL concrete-handle mandatory conformance, native selective/wide
selector/index tests and malformed authority checks. Verify renewal returns
current versions/expiry, partial settlement expires whole replay, empty results
remain stable, preview has no writes/identity and state/new work is atomic.
Writer kill, server process loss, durable reopen, lost replies and cancelled owned
operations require physical fixtures. Missing environment is an unexecuted gate.

## Formal Specification

Map SQL authority, transaction, receipt and native lease seams to CONTRACT-006
properties and TP-006. Positive witnesses and deliberately weakened guards must
have bounded replayable results; no model-only or non-durable mock qualification.

## Performance and Migration

Use the same settled workload generator, limits, durability, payload/state sizes,
filter density and concurrency as S3/Turso. Publish useful rate, latency, RSS,
drain and cost under TP-005. PostgreSQL is a correctness and efficiency baseline,
not a license for a weaker second implementation. Retire duplicate legacy queue
orchestration after the single-adapter migration passes.
