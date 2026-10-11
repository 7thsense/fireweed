# Iteration 82: one canonical relational queue representation

PostgreSQL now imports shared queue_storage_layout helpers from the engine.
Strict Postcard encoding/complete-consumption decoding, lifecycle tags, schema
collection/field/index ordinals, full-value SHA discovery and 256-byte discovery
prefixes have one implementation for PostgreSQL and the upcoming Turso projector.

Persisted mappings remain unchanged: work collection zero; BTreeMap-ordered
state collections one onward; declared fields/indexes zero onward; additional
field sentinel minus one; lifecycle tags zero through three. Checked ordinal
conversions and corrupt negative collection IDs fail closed, including i32::MIN,
which could previously panic on subtraction. Literal mapping and trailing-byte
corruption tests guard this representation boundary.

SQL, row hydration/admission, full-value verification, physical row IDs,
transactions and collision injection stay in the PostgreSQL adapter. Strict
decode still requires caller pre-admission; it is not a memory bound. There is
no second business planner or public transaction interface. Wire serializers
and queue interface constraints are unchanged.

Validation uses shared engine tests, strict engine-library/PostgreSQL-all-target
Clippy, concrete postgres+turso facade compilation, and real PostgreSQL physical,
mandatory public conformance, writer-process loss and private-server immediate
crash/WAL recovery checks. No Turso backend qualification is claimed here.

Final checks passed: 464 engine tests; 10 PostgreSQL queue-store checks, three
mandatory public contract/reopen scenarios, parent writer-loss, private-server
crash/WAL recovery, and concrete facade conformance. The ordinal fixture uses
and validates a production-valid additional-field namespace. Strict Clippy and
both concrete backend feature paths compile successfully.
