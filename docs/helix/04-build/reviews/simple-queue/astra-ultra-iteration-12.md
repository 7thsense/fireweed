# Astra Ultra iteration 12 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 468a2e2d.
Scope: native PostgreSQL forced discovery-hash collisions and evidence document.

The current-thread fixture returns one constant for all physical discovery probes.
Shared contract, ordered uniqueness, typed predicate/order and long-value/cursor
scenarios exercise full-value disambiguation across tenant/name identities with
identical logical record/request names. Reopened stores resolve original state.

Production probe hashing remains SHA256. Core semantic fingerprints, lease hashes
and cursor HMAC are independent; cursor secrets derive directly from SHA256 as
before. The test-only thread-local hook reaches owned Tokio jobs on its explicit
current-thread runtime, stays active through drain/reopen, and resets on exit.

Root verified the final forced-collision fixture, strict PostgreSQL library Clippy,
formatting and diff checks. Reviewer independently inspected the complete diff
and checked formatting safety. This proves discovery collision disambiguation,
not adversarial corruption, S3/Turso parity or performance qualification.
