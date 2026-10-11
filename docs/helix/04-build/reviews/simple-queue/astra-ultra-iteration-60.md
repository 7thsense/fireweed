# Astra Ultra iteration 60 review

Reviewer: gpt-6-astra, Ultra; existing read-only astra_plan_review agent.
Base: eaeb2069. Decision: APPROVE after two ordering corrections.

Scope: full FWQP2 success/fallback floors, derived native raw-command allowance,
in-place ordinary command ordering and exact native Fits/TooLarge assessment.
Prepared request/intent/result-budget, queue pin/frontier/job and capture allowance
bind the decision. Producer-order ordinary receipts remain unchanged.

Astra required checking prepared FP4 at finalizer entry, before any business
rejection or operational clock sample. It also required canonical ordinary
command validation before either result/capture overflow can return TooLarge.
Both corrections are implemented. Actual PostgreSQL regressions prove changed
preparation returns Unknown with zero clock samples and a real Pending row with
stray lease hash returns Unknown despite both size limits overflowing.

Canonical writer tests verify full success/fallback floors and ordinary command
sizes, including long UTF-8 IDs. Native tests cover full N/N-1, genuine command
capture overflow at fallback-sized limits, fallback preflight without new
admission, CAS rejection before size, zero prior SQL effects and atomic application.

Astra reread all corrections and independently checked diff hygiene. Verification:
186 engine queue tests (0.35 seconds), 48 physical spool tests (6.13 seconds),
15 actual PostgreSQL queue-store tests (64.67 seconds), focused native binding/
corruption tests (0.94 seconds), strict engine/PostgreSQL libraries and spool
all-targets Clippy, formatting and diff checks.

Approval covers a private size decision. Full admitted FWQP2 frame preparation,
envelope-wide preflight, durable receipts/public commit/preview, S3/Turso parity,
Snorri cutover and settled benchmarks remain outstanding.
