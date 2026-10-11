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
must already be on. No cluster settings are changed. Schema opening serializes constructors with an
advisory lock and fences existing queue operations at the queues table before
checking dependent tables/indexes. This transient global opening barrier avoids
DDL/write lock inversion; it is not a throughput qualification or an online
schema migration claim.

## Storage and bounded materialization

Shared tables keep opaque bodies separate from claim metadata. Fixed-width hash
probes and bounded prefixes accompany complete-value comparisons; no key/name
length cap or database collation changes the logical domain. Insert vectors batch-load prospective unique-tuple authority before ordered
per-item planning, including old tuple-release amplification for pending upserts.
Versions include retained deletion tombstones. Declared fields have typed predicate indexes;
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
proves writer-process loss. The isolated queue_server_crash fixture additionally
verifies acknowledged-transaction recovery after PostgreSQL immediate shutdown
and restart with WAL recovery evidence; see [durability qualification](simple-queue-postgres-durability.md).
This is a private stock PostgreSQL 16.15 fixture, not power-loss or storage-corruption
qualification.

A native current-thread fixture now forces all discovery hashes to the same
32-byte value. Shared contract, ordered uniqueness, typed predicate and long-value
scenarios still pass across distinct tenant/name queues, and reopening resolves
their original state by full identity. Semantic fingerprints, lease hashing and
cursor cryptography retain their normal algorithms. This validates collision
disambiguation, not adversarial storage corruption or S3/Turso parity.

This iteration is a functional native adapter slice. It does not qualify matched
throughput, 10M indexed query paths, numeric progress bounds,
full legacy persisted-data migration, power-loss/storage-corruption recovery,
or S3/Turso parity.
Full Snorri/Cayce facade migration and run-wide gates/membership/control mapping
still require verification against actual consumers. Production activation and
legacy-code removal remain gated by those tests and comparable settled-work
benchmarks. Historical API-006 measurements do not qualify this source.

## Per-group physical visibility — iteration 34

Ordinary commit groups now load, plan and apply in order inside the same existing
fenced SQL transaction. Each fresh group publishes its data and immutable receipt
before the next group's native reads. The local insertion sequence advances from
that group's resolved delta. Retained replay skips current record materialization,
physical writes and sequence changes. Business rejection persists its positional
receipt without applying candidate effects. The outer commit remains the only
acknowledgement boundary; no public transaction handle is introduced.

This ordering is necessary for future native administrative selectors to observe
prior successful groups. It replaces end-of-envelope coalescing for ordinary
commits with per-group physical writes. That structural tradeoff has no performance
claim; comparable settled benchmarks must measure the final implementation. Insert
vector packing remains unchanged. Whole administrative selection must still freeze
one group's complete initial ownership before any of its mutations are applied.

A queue-scoped test-only hook reads physical SQL records and receipts after every
fresh group. The fixture covers a stale CAS rejection between successes, unique
ownership release/reuse, deletion/reinsertion, exact highwater and insertion sequence,
reverse retained replay and the next fresh insert. A real SQL division-by-zero after
three groups forces transaction failure before acknowledgement; all unacknowledged
records, state, receipts and queue sequence roll back, and exact retry succeeds.
This is separate from caller cancellation: the existing caller-drop test confirms
that admitted owned jobs continue, drain and reopen with their complete receipt.

Native indexed administration, result/publication byte qualification and preview
remain pending. This iteration changes ordering within the existing ordinary native
commit path; it activates no administrative interface or alternate backend.

## Private native control selector preparation — iteration 37

The private administrative SQL compiler resolves first-clause ownership in the
server over active work records, ordered by full key and physical identity. It
shares the ordinary adapter's typed equality/range and exact field-name SQL.
Hash probes retain full identity comparisons. Stored and effective lifecycle,
active lease, attempts, availability, missing fields, keys and nested boolean
selectors match the common evaluator at explicit operator time. Preconditions do
not change first-clause ownership.

A flat byte-array pool keeps protocol parameter count fixed at three: queue,
bytes and operator time. Wide n-ary boolean expressions are balanced for server
parsing/planning. SQL and parameter allocation is charged before construction,
including expression-copy and container-header scratch, and held until drop.
These conservative bounds are not a performance qualification.

Two actual PostgreSQL fixtures, including forced discovery-hash collisions, compare
native selection against the shared evaluator across all leaf types and nested
logic at times before, exactly at and after lease expiry. They cover signed-minimum and signed-maximum
availability boundaries, sparse fields, typed range isolation, 80 KiB field names,
40,000 membership values, a 10,000-term boolean selector, 5,007 selected records,
first ownership despite false preconditions and pre-build capacity failure.
All ten native queue fixtures and strict PostgreSQL library/test Clippy pass.

The compiler is privately staged; its entry point and output carry a temporary
non-test dead-code expectation explaining the activation gate. No public control
input is ignored or accepted by an unsupported path. Streaming one-row hydration,
fenced exhaustion, durable complete-result receipts/replay, exact publication
limits and preview remain required before public administration activation.

## Private fenced row provider — iteration 38

The private provider declares a non-scrollable server cursor over the native
first-owner selection and fetches one metadata row at a time. Key length is read
before variable key/record construction is charged. It reuses the ordinary native
record loader for one selected record, loads opaque bodies only for explicit body
comparison, freezes exact prefix edits through the shared resolver, and loads
prospective unique owner/nonowner authority under the same outer queue fence.
Active-lease deletion uses the common eligible unique tuple codec rather than the
ordinary delete planner's lease rejection. Identity amplification is charged
before native tuple generation.

Same-job buffer guards transfer into one streamed hydration guard without releasing
or reacquiring admitted capacity; different jobs cannot merge. Native loaded payloads
now drop before their guards on error paths. The shared asynchronous accumulator
consumes the cursor to verified exhaustion; successful CLOSE precedes its fresh
ordinary ownership clock and final CAS/uniqueness checks. The caller retains the
outer transaction and queue-row fence through publication or rollback. Any error or cancellation of this owner future requires rollback of the entire
containing transaction; callers must not continue sibling groups in that transaction.
An open/failed cursor is never a partial success.

Actual normal and forced-collision fixtures stream 5,001 bulk no-ops with constant
retained client admission, compact results and no retained work/probe images. They
check omitted megabyte bodies, minimal changed metadata, fresh-clock stale-lease
rollback, zero matches and cursor reuse. Another physical fixture checks initial
owner conflict, free-tuple sibling collision, prefix removal before explicit writes,
explicit empty-body replacement, and deletion of a lease acquired through the public
claim path. A real SQL division-by-zero during the second row, after an earlier
accepted proposal, returns unknown with no final clock/result and releases planning
charges; the outer transaction rolls back.

This remains private planning qualification. It fetches metadata individually and
hydrates complete fields for one selected row; selective dynamic-field hydration,
batched metadata and server execution/resource measurements remain performance gates.
It is not a full-queue client-filtering path. Durable complete administrative receipts,
exact result/publication limits, preview, public activation, S3/Turso and matched
benchmarks remain outstanding.

Native physical fixtures share a schema whose store-open path deliberately takes
a global DDL fence. They now share a test-only fixture mutex, preventing long
selection transactions from blocking unrelated fencing/cancellation fixtures.
Concurrency within each fixture remains enabled and timeouts remain unchanged.
