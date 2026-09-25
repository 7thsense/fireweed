---
ddx:
  id: us-idempotent-ingest-and-mutation
  depends_on:
    - feat-idempotent-ingest-and-mutation
    - prd
kind: user-story
---

# US: Idempotent Ingest and Mutation

As a producer engineer, I add and change records by key, retry any write safely, and resolve an unknown outcome without duplicating work.

## Acceptance

- A record's key is opaque bytes of 1..=256 B chosen upstream, and a queue holds at most one record per key, in any stage (FR-18).
- An insert with `Expect::Absent` of an existing key reports `Exists { version }` and changes nothing; `Expect::Any` upserts (FR-19).
- A write resent with the same `request_id` and body returns the recorded outcome with `replayed: true`; the same id with a different body is `RequestIdConflict` (FR-19, FR-22).
- A sparse patch changes only the stage, rank, gates and fields it names, and `Expect::Version` of a stale version reports `VersionMismatch` (FR-20).
- A lease-free write to a leased record is rejected with `Leased` (FR-20).
- Each unit of a write request applies atomically or not at all, and a rejected unit reports its failing write and the reason without affecting the other units (FR-21).
- A write that failed with `Unavailable` is resolved by resending it with its `request_id` or, without one, by reading the keys it touched (FR-22).
