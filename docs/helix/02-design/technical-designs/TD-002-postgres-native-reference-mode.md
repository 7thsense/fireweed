---
ddx:
  id: td-postgres-native-reference-mode
  depends_on: [architecture, api-queue-claim-commit-contract, td-storage-architecture-backend-contracts, contract-construction-config, contract-durable-queue-records]
  review:
    self_hash: 80778e507024702cedc72601827f3baa40e639f1207421d7ad8e676882f5e2dc
    deps:
      api-queue-claim-commit-contract: d7981b6ff3af923cf6d6d6fa3065d12186867ec47ef09613ac2c030277b332a1
      architecture: 0fb7e5ddb84e59f909e455b130450962d98e5be6ab0306ea4add9e0cbb192060
      contract-construction-config: f332fd8165cabf9c37ccf6a5d42ae34fc9f6dd50152202ab9178d81bff69df62
      contract-durable-queue-records: 0df994edd33ee1d243dbe2b725caa20d1c398eefe136494c99101b5b24e79475
      td-storage-architecture-backend-contracts: 5cc10b490a657e03c427507f9eee6d522698655edf0dc68b9841edbe77ba4615
    reviewed_at: "2026-11-10T14:29:59Z"
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

### Stored schema version

Every PostgreSQL DDL family (queue store, relational projection, relational
command log, relational queue metrics, control plane, postgres backend, compose
log) is a named component with a version constant in
`crates/fireweed-postgres/src/schema_version.rs`. Each PostgreSQL schema holds
one `fireweed_schema_version (component TEXT PRIMARY KEY, version BIGINT NOT
NULL)` table. At open, under an advisory lock and in one transaction, a
component with no tables and no row is created and stamped together; a row equal
to the build's version continues; a different row, or component tables with no
row (a database from before the check), fail the open before any component table
is touched. The error names the component, the found version or `none`, and the
expected version, and says the database must be recreated; for the queue store it
is `QueueError::Invalid`, elsewhere the site's storage error. There is no
migration or upgrade path.

To change a component's DDL, bump its `*_SCHEMA_VERSION` and update that
component's pin (version and SHA-256 of the DDL text) in the
`ddl_digests_pin_component_versions` test; the test fails on any DDL edit
without both.

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
