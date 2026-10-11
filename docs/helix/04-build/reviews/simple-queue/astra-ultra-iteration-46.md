# Astra Ultra review — iteration 46

Model: gpt-6-astra, Ultra. Base: 415f4991.

APPROVE physical semantic evidence reduction after the complete-cursor fix.
Initial review found reduction could silently start at a partially consumed
cursor or report false flags on a second call at EOF. Reduction now rewinds
inside its poison wrapper and always validates the complete sealed image.

Strict full-key/ordinal order and ordinal-bound record integrity prevent evidence
substitution/duplication. Allocations precede decoding only after child admission;
first header ownership outlives its reducer, later payloads drop per row and old
tuples drop before new hydration. Later errors override provisional conflicts
and poison completion. The domain reducer is reused; no all-tuples map exists.

Verification: 139 engine queue tests, nineteen physical spool tests (1.23 seconds),
strict engine library/spool all-target Clippy, formatting and diff checks passed.
Astra independently checked source, final cursor correction and scope text.
The typed composite fixture spills over 24 MiB with a 2 MiB admitted-operation
memory budget; it is not an OS RSS/page-cache measurement.

Approval covers evidence-spool reduction only. Original selection EOF, owned async
drain, streamed commands, logical size/rejection receipts, publication, parity and
performance qualification remain pending.
