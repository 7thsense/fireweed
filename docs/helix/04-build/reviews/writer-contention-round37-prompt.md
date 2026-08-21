# Adversarial review: writer-contention recovery round 37

Review the current plan, S-0 probe code, and compatibility evidence against
round 36 and the v0.31.21 source. Do not implement.

Round 36 had converged. Execution of S-0 then triggered its explicit hard
stop:

- the root adapter pins Turso 0.7.0; the standalone probe pins 0.7.2;
- all twenty-four Deferred readers preserved a committed snapshot without
  blocking under a live `IMMEDIATE` writer, then remained stable across writer
  commit while a fresh connection observed the new value;
- numeric `query_only=1`, `cache_size=-4096`, and `busy_timeout<=100 ms` read
  back correctly;
- `pragma_update("read_uncommitted", "0")` returned success, but
  `PRAGMA read_uncommitted` returned no row for every reader on both versions;
- the current v0.31.21 serving reader passes keyword strings
  `query_only="ON"` and `read_uncommitted="ON"` while discarding errors;
  Turso rejects the `query_only` keyword value, so that protection is not
  active; and
- no committed reader pool or autocommit fallback has been implemented.

The revised S-0 gate now:

1. configures twenty-four independent connections with numeric
   `query_only=1`, `cache_size=-4096`, and `busy_timeout<=100 ms`;
2. begins every Deferred transaction without issuing a SELECT;
3. opens an `IMMEDIATE` writer and changes `before` to uncommitted `after`;
4. requires each reader's first SELECT, while that writer remains live, to
   complete within 100 ms and return committed `before`;
5. commits the writer, requires every held transaction to continue returning
   `before`, and requires a fresh connection to return `after`;
6. removes the serving reader's unsupported `read_uncommitted=ON` attempt,
   configures `query_only=1` numerically, and propagates configuration failure;
7. requires exact readback only for supported settings and repeats the semantic
   sequence after Turso bumps; and
8. retains the hard stop on any semantic or supported-readback failure and the
   ban on multi-statement autocommit fallback.

Audit the whole plan, but focus on whether this sequence is a sufficient and
non-circular proof of the isolation needed by S3r/S5. Check transaction timing,
whether the first SELECT really occurs after the uncommitted writer update,
whether query-only/readback is tested on the same candidate connections,
whether the existing serving-reader correction belongs in S-0, and whether any
remaining plan clause incorrectly depends on `read_uncommitted`.

Use this exact response contract:

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |

Severity is `BLOCKING`, `WARNING`, or `NOTE`. A concern implementable inside an
existing named acceptance criterion without changing architecture or public
contract is a NOTE.

## Prior-round audit

State whether the release-driven redesign preserves or invalidates any
round-36 conclusion.

## Verdict

Exactly `APPROVE` or `REVISE`.

## Convergence

Exactly `YES` or `NO`. `YES` requires no BLOCKING and no WARNING.

## Summary

One concise paragraph.
