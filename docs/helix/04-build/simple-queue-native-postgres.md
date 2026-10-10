---
ddx:
  id: build-simple-queue-native-postgres
  depends_on:
    - api-queue-claim-commit-contract
    - build-simple-queue-binding-v1
  status: accepted
---

# Native PostgreSQL API-007 implementation, iteration 7

`PostgresQueueStore` implements every mandatory QueueStore operation with native
async PostgreSQL calls. It uses the shared contract, planner, ordered-key codec,
owned admission and memory accounting; it does not copy domain transitions or
use the legacy blocking/log-to-memory PostgreSQL backend. Existing production
facades remain on their current implementation until cutover qualification.

## Atomicity and ownership

A queue row lock protects record/receipt identity, uniqueness and version/sequence
authority across independent clients. A commit envelope currently publishes one
SQL transaction containing the final combined delta and each group's individual
receipt. Groups execute in request order; business rejection does not undo valid
siblings. Retained receipts are resolved before loading current records and replay
independently under rebatching. Infrastructure failure before durable publication
can leave no group committed; this packaging adds no public transaction session.

Owned Tokio jobs retain connections, admission and buffer charges after caller
cancellation. Shutdown stops admission, drains terminal jobs, closes connections
and joins drivers. Session synchronous_commit is forced on; fsync/full_page_writes
must already be on. No cluster settings are changed.

## Storage and bounded materialization

Shared tables keep opaque bodies separate from claim metadata. Fixed-width hash
probes and bounded prefixes accompany complete-value comparisons; no key/name
length cap or database collation changes the logical domain. Versions include
retained deletion tombstones. Declared fields have typed predicate indexes;
composite indexes retain full memcomparable sort keys and sparse unique semantics.

Claims select only matching due work, including expired leases, before leasing.
They do not reclaim the whole queue or filter leased records in a consumer.
Response-size preflight selects a prefix before loading opaque values or large
keys. Replay verifies every original live lease before payload sizing. Queries
use authenticated scoped keyset cursors and include collection names in prefix
budgets; only the final required cursor's full sort key is loaded. Stats aggregate
in SQL and hydrate bounded representative values without copying schema names.

The engine's queue_memory module shares schema/filter/container charges, worst-case
index amplification, repeated names/identities and borrowed persisted-structure
scans. Physical stores measure loaded sizes before allocation. Positional state
and receipt results are sized through borrowed references before duplicate clones.
Schema-independent wire-byte/count/declared-response-limit preflight runs before
admission; schema/type and other mutation semantic checks run under the atomic
boundary. This does not promise every semantic error precedes saturation.

## Evidence and remaining gates

The mandatory fireweed-conformance queue_contract scenarios have no capability
skips. PostgreSQL runs the same public calls and assertions intended for S3/Turso:
provision drift, immutable insert identity, exact filtered priority claims, atomic
state/new-work settlement, independent rejected receipts/rebatching, ordered
shared-key groups, sparse multi-column stats, every field type/predicate/range,
missing values, signed extrema/zero, due times, insertion ties, state pagination,
long keys/index/field/collection names, selective returns, lease replay precedence,
exact expiry, administrative controls and competing independent clients.

Adapter fault setup additionally holds a SQL lock, aborts an admitted caller,
drains storage, and reopens the complete state/work/receipt group. This is a
cancellation/reopen proof. A separate subprocess fixture now kills an acknowledged
writer without shutdown and starts a fresh verifier process. The shared scenario
checks the original acknowledged receipt and exact replay, completed source, state
value and emitted next work together. The readiness payload is published by atomic
rename only after the writer has received successful commit acknowledgement. This
proves writer-process loss; abrupt database/server crash remains a separate gate.

This iteration is a functional native adapter slice. It does not qualify matched
throughput, 10M indexed query paths, numeric progress bounds, forced hash collisions,
full legacy persisted-data migration, server/PG crash recovery, or S3/Turso parity.
Full Snorri/Cayce facade migration and run-wide gates/membership/control mapping
still require verification against actual consumers. Production activation and
legacy-code removal remain gated by those tests and comparable settled-work
benchmarks. Historical API-006 measurements do not qualify this source.
