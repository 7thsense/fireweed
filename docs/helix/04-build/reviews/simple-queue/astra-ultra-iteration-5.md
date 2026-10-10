# Astra Ultra iteration 5 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base bfe4eeb5.

Plans now clone only addressed records and merge successful deltas, retaining
unrelated records and unique owners. Failed groups have no speculative effects;
unique swaps/rekeys, deletion followed by tuple reuse, and packed independent
ownership have regression coverage. Scoped buffer charges borrow the owned job
reservation and release without releasing its slot. Failed charging leaves usage
unchanged and returns OutcomeUnknown.

Opaque work/state payloads use Bytes to avoid repeatedly copying large workflow
bodies. Targeted empty/nonempty/multibyte-length tests prove this binding's
postcard wire bytes and fingerprints are unchanged from Vec<u8>, including
bidirectional decoding. This is not a claim about every Serde format.

Internal StoredWork materialization flags distinguish Preserve from an explicit
empty replacement. Incomplete fields fail before effects. Flags change the
internal delta encoding: any future durable log envelope must version that
representation and apply Preserve to existing storage, never expose an unloaded
body as a complete user record.

Root verified 22 core contract and 48 matching engine queue tests, strict core
and engine library Clippy, formatting and diff checks. Reviewer inspected final
source and tests and independently checked the diff. No blockers remain for this
shared helper iteration. The incomplete PostgreSQL draft is excluded. Physical
adapter buffer lifetimes, retained Bytes alias charging, durable Preserve handling,
parity suites, migration and performance remain implementation/qualification gates.
