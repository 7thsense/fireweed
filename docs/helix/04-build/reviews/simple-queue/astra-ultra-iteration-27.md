# Astra Ultra review — iteration 27

Model: gpt-6-astra, Ultra. Base: 1441b789.

APPROVE, scoped to the FWQP1 resolved mutation/ordinary receipt codec and exact
validation. Fixed widths, explicit frozen tags and strict identity ordering are
independent of evolving serde layouts. Full borrowed structural scanning rejects
unknown versions/tags, malformed lengths/UTF-8/scalars, sequence/lease/highwater
shape and trailing bytes. The 489-byte rich frame length and SHA256 are pinned.
The compiler binds original raw definition bytes, predecessor/hwm authority,
exact command maps and current commit/insert receipt domain/id/v3 fingerprint.
Encoded bytes remain private with their preallocation charge.

Review found and resolved command/view consistency gaps: the planner and compiler
now share exact Preserve/Edit/body validation, without field/body cloning. Verified
no-ops omit record commands while retaining receipts; every wire mutation advances
highwater. Absent insertion ordering and existing preserved/new sequence rules,
prior embedded identity/sequence, and success receipt version ranges are checked.
Receipt-range scratch borrows identities and is charged before allocation with a
1024-byte root/node floor plus 192 bytes/entry, released before frame allocation.
InsertOnly cannot produce Replaced. No conflict/expired response overwrites a receipt.

Verification: seven codec tests, 101 engine queue tests, three public PostgreSQL
contract/reopen/legacy scenarios, six isolated native fault/cancellation/collision
fixtures, strict core/engine/postgres/conformance library Clippy, formatting and
independent diff hygiene. Adversarial tests cover every truncation of the rich
frame, explicit byte mutations, invalid Preserve/unedited changes, sequence reuse,
receipt version999, one-entry scratch saturation, leased Edit/removal/body empty,
no-op receipt retention and packed absent create/delete highwater. Complete and
selective megabyte views encode identical small metadata frames.

No projector/store appends this frame yet. Claim/provision/admin-result bindings,
atomic external apply cursor and fresh receipt authority, logical-group durable
size rejection and physical S3/Turso qualification remain required.
