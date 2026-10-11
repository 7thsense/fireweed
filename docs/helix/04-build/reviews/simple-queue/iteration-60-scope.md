# Iteration 60: exact native full-publication decisions

Base: eaeb2069. Shared counting derives a successful FWQP2 floor with the full
queue/header, request ID, all ordinary identities and minimum FWAR1 result. It
also measures the full maximum zero-effect fallback. Native preparation rejects
budgets that cannot fit fallbacks and derives raw command capture allowance from
the full publication budget minus the success floor.

Preparation retains FP4 request/intent/result-budget binding and both declared
limits. Finalization sorts ordinary command vectors in place without cloning
payloads; their receipt retains producer order. The finalizer verifies prepared FP4 identity before any business rejection or
clock sample. After all business/evidence checks,
the full gate verifies scratch queue/pin/frontier/job and allowance context,
validates/counts all ordinary commands before any size decision, checks exact
complete-result bytes, then counts all
administrative command bytes and the complete bound binary receipt. It emits a
private Fits/TooLarge decision with exact full size and command count.

Canonical-writer tests prove success/fallback floors and ordinary command counts,
including long UTF-8 IDs. The actual PostgreSQL positional fixture covers full
publication N/N-1, actual capture overflow under a fallback-sized budget, changed
request rejection, fallback N-1 preflight with no new admission, CAS rejection
before both size overflows, zero prior SQL effects and unchanged existing atomic
application. Changed preparation on a rejection path fails Unknown before time;
malformed ordinary lease metadata remains Unknown despite both size overflows.

This is a private size decision, not durable/public activation. The admitted
resolved FWQP2 frame still must be prepared and checked before atomic application,
with envelope-wide fallback preflight, durable complete receipts/preview and
public commit variants. S3/Turso, Snorri and settled benchmarks remain outstanding.
