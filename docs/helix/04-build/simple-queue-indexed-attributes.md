---
ddx:
  id: build-simple-queue-indexed-attributes
  depends_on:
    - build-simple-queue-consumer-integration-binding
    - build-simple-queue-json-equality
  status: accepted
---

# Indexed additional attributes and definition persistence

A collection may provision additional_field_prefixes, explicit literal namespaces
ending in slash. Every admitted full field name under them has an automatic typed
index. Declared fields remain strict and take precedence. Additional values retain
their own scalar type; equality never coerces and range bounds must agree in type.
This is an immutable schema policy, not an optional backend capability.

PostgreSQL stores additional names as complete BYTEA names with name hashes and
full typed sort values. Hash/prefix candidates always undergo complete-name/value
comparison. No full unbounded name is used as a B-tree uniqueness key. Queue
fencing plus complete logical comparisons protect identity. Record replacement
and changed-field persistence regenerate both declared and additional rows in the
same transaction, removing obsolete attributes and their indexes. Deleted work
retains its non-reused version tombstone while removing attributes/body.

Native filters, selective claims, state reads, work/state query pages and grouped
stats include the new attributes. Mixed-type ranges constrain the scalar type tag
as well as bounds; mixed-type groups are distinct and order by missing then codec
type (integer, float, boolean, text, bytes) and within-type value. Group hydration
returns requested ordinal/value pairs, avoiding repeated large field-name copies.
Payload/header preflight includes additional names and values before loading them;
commit materialization additionally accounts complete sort keys and containers.

Definitions now use an explicit FWQD4 frame behind a sentinel that cannot begin a
valid legacy postcard collection length. Legacy unframed definitions decode through
the original field/index layout with no additional namespaces; their field and
state collection ordinals remain unchanged. Both formats have borrowed structural
header measurement before owned decoding. Unknown versions/trailing bytes and
invalid stored schemas fail closed. Provision compares semantic definitions and
charges an existing definition before materialization; format alone is not drift.
Adding namespaces to an existing definition is still explicit definition drift,
requiring a checked migration rather than silently enabling writes.

Ordinary request fingerprints and retained receipts keep their existing v3
encoding. Query cursor scopes include the current definition shape; a legacy
cursor scope is not treated as a new-schema continuation. Fresh queries reopen
old work/state/receipts without resetting names, versions or lease authority.

The mandatory shared scenario covers literal route/~tier names, exact JSON equality
bytes, null versus missing, mixed-type equality/ranges/groups, long NUL/UTF-8 names,
selective claims with bounded returns, atomic attribute removal/state insert and
pending replacement and version-fenced deletion cleanup. The same scenario runs with all PostgreSQL discovery
hashes forced equal. A real database fixture rewrites a populated definition to
its original unframed shape, reopens it, reprovisions identically and verifies work,
state and immutable receipt preservation.

This is the generic attribute-storage slice. Canonical JSON namespace pins still
need enforcement by the consuming Snorri mapping before query/write activation.
Administrative intents, preview, numeric bucket stats, S3/Turso parity, legacy
API-001 data migration and the full performance/progress lanes remain required.
