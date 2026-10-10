# Astra Ultra iteration 7 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE after corrections. Base dcc7040b.
Scope: native PostgreSQL functional adapter, shared schema-independent preflight
and memory accounting, shared physical conformance scenarios, and scope document.

Queue-scoped SQL fencing protects identity resolution, versions and unique
ownership across independent handles. Owned jobs retain admission through caller
cancellation; the held-lock cancellation/drain/reopen test proves state, work and
receipt complete together. Group receipts remain independent and immutable under
rebatching; business rejection preserves valid siblings in ordered execution.

Review corrected sparse grouping order, replay expiry precedence after payload
growth, collection-name page-prefix accounting, and schema/name/index/receipt
allocation gaps. Borrowed structure scans precede owned decoding; tuple expansion
and repeated identities are charged before planning; external owner key lengths
precede full-key loading. Shared helpers live in engine queue_memory. PostgreSQL
physical claim receipt encoding remains in its adapter. Schema-independent byte,
count and declared-limit rejection precedes admission; other semantic validation
is not claimed to precede saturation.

Final regression cases cover typed equality/membership/all range bounds, missing
values, signed integer/float extrema and zero signs, bytes/NUL/UTF-8, priority and
scheduled order, insertion ties, explicit future eligibility, state pagination,
sparse multi-column stats, ordered valid/stale/valid groups and reversed replay,
partial settlement/expiry versus grown payloads, long names/keys/index values,
selective returns, active administrative changes, and two independent claimers.

Root verified 23 core contract and 57 engine queue tests, both shared PostgreSQL
runners, three native unit/fault tests, strict Clippy for four affected library
packages, TLS compilation, formatting and diff checks. Reviewer inspected final
source/documentation and independently checked the diff. No remaining blockers
for this functional slice. The review does not qualify performance/progress,
forced hash collision tests, abrupt crash recovery, legacy migration, S3/Turso
parity, Snorri/Cayce facade migration or production activation. These remain gates
in the native PostgreSQL scope document and governing refactor plan.
