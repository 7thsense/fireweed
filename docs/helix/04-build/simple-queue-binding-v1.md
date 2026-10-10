---
ddx:
  id: build-simple-queue-binding-v1
  depends_on:
    - api-queue-claim-commit-contract
  status: accepted
---

# API-007 Rust binding, iteration 2

The pure `fireweed_core::queue_contract` module owns DTOs, declared schema/filter
validation, canonical byte admission, and operation fingerprints. The mandatory
object-safe `fireweed_engine::queue_contract::QueueStore` port has no default
unsupported operations. Its owned requests allow adapters to own complete jobs
across cancellation. The existing interface is not yet replaced or qualified.

Queue provisioning retains max_attempts: RetryAt at the attempt bound fails
the item; otherwise its supplied retry time is the eligible-age anchor (main
Retry semantics, distinct from Rearm). Times are signed UTC microseconds. Lease renewal duration is 1 ms through 1 h.
Record versions start at one and are per-key durable non-reused versions.
Fields are typed integer, finite float, boolean, UTF-8 text or bytes. Negative
floating zero is rejected at validation to ensure one canonical representation;
NaN/infinity are invalid. Ranges have explicit inclusive/exclusive bounds;
absent fields do not match and all text/byte ordering is bytewise. Absent
index/group fields sort first ascending and last descending; SQL NULL defaults
must not alter this shared comparator. No database
collation or cross-type coercion changes these comparisons. Identifiers are nonblank, at most 256 UTF-8 bytes.

Canonical encoding is postcard's fixed DTO representation and sorted BTreeMap
keys, streamed for hashing/counting without a second full-body buffer. Fingerprints
are SHA-256, version/domain separated; group identity includes queue and full body
but excludes batch placement. Schema/DTO encoding changes require a new codec
version plus legacy readers. Insert/claim/commit identity namespaces are distinct.

Insert is a batch of independent insert/upsert outcomes retained as one immutable
request receipt; retry replays that complete vector, and a changed body conflicts.
It cannot split request identity implicitly. Existing work key insert-only rejects;
upsert of leased work rejects and may not steal authority. Commit work_inserts
are insert-only; replacing work uses an explicit versioned work update.
UnclaimedUpdate requires Version, never Absent; creation is work_inserts.
RetryAt and patch.available_at may agree but contradictory times reject before
effects; adapters cannot invent a precedence rule.
Insert and commit admission conservatively budget the entire positional response before
effects, including maximum encoded versions and rejection diagnostics (capped
at 256 bytes). Rejected commit outcomes are durably retained. RequestConflict/RequestExpired
are positional group outcomes, never a known envelope rejection after other
groups have committed. Shared-key first executions are
ordered; no whole-batch rollback. Duplicate writes inside one group are invalid.

Claims select only pending due work with explicit filtered priority/progress order.
If the first candidate cannot fit the requested response bytes, claim returns
TooLarge before leasing anything. Query also returns TooLarge rather than an
empty page/nonadvancing cursor when its first record cannot fit.
A replay keeps original selected keys and lease identities, renders committed
current records, and respects the original response-byte limit. If updated values
exceed it, return TooLarge without changing leases/identity; caller may query the
selected keys or settle/release known credentials. Partial settlement expires
claim replay. Empty replay stays empty, never quietly reclaims newly inserted work.

Query is a bounded keyset page in declared index order, with key as the final tie
break; cursor binds queue, source, index, filter, direction and schema generation.
Page count/byte limits may change; query identity may not. A stale schema cursor
fails explicitly. Inter-page concurrent mutations may move records: no snapshot
is promised. An unchanged dataset drains exactly with no omissions/duplicates.
Every collection has a reserved $key index for addressed recovery reads. All
declared fields automatically receive a typed index, including fields not in
a composite query index; conjunctions use storage-side index intersection.
No backend may accept a predicate and implement it by broadly claiming work
or scanning/hydrating the entire queue. Composite indexes govern query order;
automatic field indexes supply predicates, including unrelated predicate fields.
Provisioning and receipt reads use shared count/byte-validated request DTOs.

Default common limits: 4096 record mutations/group, 64 groups/request, 128 MiB
canonical group and response, 256 MiB request, 1 MiB work body, 64 MiB state value.
These cover current 512 effects + 512 awaits and 1 MiB transition output. They
are admission defaults, not a claim that accumulated instance state is bounded
by a transition. Snorri default hot history is 50 MiB; 64 MiB is provisional
headroom, not measured qualification. Before enabling a backend/Snorri binding,
serialize representative maximum instance checkpoints and all retained sections,
record actual sizes and increase the common configured state/group/response bound
if needed. The exact same limits are used by both qualification/benchmark lanes.
Full states are never truncated or silently broken into non-atomic writes.

Implementation tests in this iteration cover envelope/schema rejection, legal
shared-key groups, duplicate identities, fanout/megabyte state, stable per-group
fingerprints through rebatching, complete claim selection fingerprints, canonical
map/domain encoding, count/byte boundaries and state/work separation. Runtime
receipts, ABA version allocation, cursor continuation, admission ownership and
storage semantics are subsequent iteration obligations, not claimed implemented.
