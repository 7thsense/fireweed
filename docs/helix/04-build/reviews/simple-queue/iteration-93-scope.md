# Iteration 93: native current-record claim replay

Add an internal stock-Turso replay reader using the shared Deferred snapshot
transaction lifecycle. It binds exact full address, original raw definition and
cursor secret, insertion head, historical epoch and independently recovered
external offset before reading an immutable domain1/FP3 claim receipt. Full
identity collision checks remain native; fingerprint conflict precedes expiry.
Receipt bytes are sized/admitted before fetch and structurally checked in full.

Every original full-key member must retain its token and unexpired raw lease.
All current ownership checks complete before any payload fetch. Renewal returns
current versions and expiries in original selection order; partial settlement,
deletion or expiry rejects the complete group. Creation versions only bound
current versions from below. Empty receipts remain explicit stable empty groups.

Output, receipt, keys, metadata and field structures are admitted before copies.
Selected fields and body controls are honored natively; opaque omitted values
are never fetched. Native payload lengths are checked before fetch; combined
raw payload bytes and final canonical encoded response obey the original bound.
Malformed fixed-width stored authority is bounded with SQL CASE before SDK
materialization. All operation errors/panics finish rollback or poison before
releasing the owner. Result data precedes retained charges in drop order.

This is an internal reader, not the public S3/Turso QueueStore or a live fence.
Authoritative recovery/coverage must be supplied independently. Native fresh
selection, other public operations, provider durability, bounded manifest storage
and settled comparable benchmarks remain necessary before backend qualification.

Astra review corrections: the snapshot now compares the recovered offset,
insertion sequence and historical last-record epoch exactly; the result vector
is private behind a borrowed accessor. Schema/selected names are borrowed until
presence is established, then each retained output name is counted and charged
before copying. Full-field reads validate actual stored ordinals, and corrupt
persisted names/types return OutcomeUnknown. Nested business results preserve
RequestConflict/RequestExpired/ClaimExpired through the physical scratch runner.

Physical controls cover current renewal/reopen, complete payload, four-MiB bad
hash, partial-settlement precedence over oversized remaining body, independent
same-offset sequence/epoch corruption, invalid ordinal/type, selective opaque
omission under one-MiB admission, stable empty replay across reopen/extreme time,
expired identity and conflict precedence. A tight one-MiB admission fixture keeps
128 records with absent 80-KiB declared/selected names and rejects present long
names before copying. Started-reader cancellation retains snapshot/result guards
until physical completion and the final reservation callback, then proves native
autocommit with unchanged queue cursor and receipt count.

The internal reader accepts an explicit evaluation time. Public integration must
sample the independent operational clock freshly at its linearization point
after acquiring authority/snapshot; a timestamp captured before queued execution
is not proof of current lease ownership.
