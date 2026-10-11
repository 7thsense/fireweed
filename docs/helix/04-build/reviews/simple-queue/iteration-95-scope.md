# Iteration 95: native filtered ordered claim candidates

Add an admitted stock-Turso candidate reader under the shared Deferred snapshot
owner. Replay and selection share schema/address/recovered-progress checks.
Native key probes and typed field-index relational intersections enforce the full
filter before priority/schedule/progress ordering and max-record selection. No
leases or receipts change and no broad work is claimed for client filtering.

Equality/membership compare discovery hashes and complete ordered keys. Ranges
use exact type floors, bounded prefixes and full inclusive/exclusive keys.
Additional names require full-name equality. A single bound byte pool represents
wide key/value sets as native VALUES relations, avoiding parameter-per-value
limits. Field probes have stable logical predicate IDs; four fixed declared/
additional equality/range branches intersect matches with COUNT(DISTINCT id),
which preserves duplicate-IN semantics and conjunctive field requirements.
Probe-first CROSS JOIN and explicit native index hints enforce indexed seeks.
Actual EXPLAIN QUERY PLAN controls verify all four equal/range indexes. Pool, statement and result headers are preadmitted from request structure
and actual bytes. Candidate key lengths are checked before fetch; private output
keys retain their reservation guards. Business errors survive the physical owner.

The progress deadline matches the existing PostgreSQL policy with i128
subtraction, avoiding stock SQLite numeric promotion/overflow. Ordering preserves
full unsigned insertion sequence blobs. The explicit timestamp is an internal
primitive; public integration must sample a fresh operational clock.

Physical tests cover equality, membership, typed/inclusive/exclusive ranges,
additional integer/float isolation, priority direction, conjunctions, 10,001-key
and 10,001-value filters, forced key-discovery collision, no lease/receipt/cursor
mutations and exact-expiry candidates. Full native regression and strict checks
are required before Astra-reviewed landing.

This selects ordered identities for subsequent admitted record hydration and
resolved durable claim publication. It is not a QueueStore activation, a live
fence, a completed claim-response budget check or a performance qualification.
Full public operations, bounded manifest storage, provider tests, Snorri adapter
and settled comparable benchmarks remain outstanding.

A 5,001-field balanced boolean query still overflowed the stock parser stack.
No coredump was available; gdb identified recursive parse_nexpr_list/parse_expr
calls in turso_parser, and available memory ruled out OOM. The replacement uses
flat fifteen-column VALUES rows and four fixed UNION branches: expression depth
is independent of field count, without increasing the worker stack or imposing
an interface cap. The earlier PostgreSQL balanced emitter is now shared unchanged;
Turso only uses it for the fixed-width outer conjunction.

Revised adapter-owned scratch admission includes 2,048 bytes per probe plus
actual request bytes and result headers before construction, covering row
containers and overlapping VALUES/CTE/final statement buffers. Native planner,
VM and page-cache residency remain separately unqualified. Physical controls
include all-match/one-wrong-leaf 5,001-field conjunctions; a single mixed declared
and additional equality/IN/range request with duplicate IN values; four native
SEARCH index plans; schedule order, unsigned-MAX sequence ties, overdue age
precedence and an exact threshold below i64::MIN. These are correctness/debug
fixtures, not settled release performance or density qualification.
