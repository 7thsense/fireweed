# Astra Ultra iteration 16 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 96373bab.
Scope: numeric bucket stats and demonstrated lifecycle SQL parameter fixes.
No blocking findings remain.

The typed [lower,upper) intervals preserve positional declaration order, duplicate
labels, zero bins, empty interval vectors and a final missing bucket. Nonoverlap
and finite bounds are validated; present out-of-range/different-type values are
omitted. Simultaneous field grouping returns independent dimensions under one
queue fence, not a cross product or per-bin mutable reads.

Review identified and corrected per-bin long-name duplication, overlapping filter
parameter lifetimes, uncharged repeated SQL scratch, and count validation after
admission. Full identity binds once, previous params drop before regeneration,
and checked SQL scratch is retained by the owned job before construction. The
51-bin limit precedes admission. Effective-lifecycle CASE parameters now have
explicit smallint casts in stats and query, fixing native SQL type inference.

Root verified native PostgreSQL conformance/reopen, forced hash collisions,
80 KiB NUL/UTF-8 names with 50 bins, slot/byte saturation precedence, lifecycle
expiry/query regression, core23/engine59 tests, strict Clippy, TLS, formatting and
diff checks. Astra independently reviewed final code/docs and checked the diff.

Snorri as_of classification mapping, S3/Turso parity, release qualification and
consumer activation remain outstanding. Finite-bound tightening relative to the
legacy bucket validator is explicit; numeric payload projection remains separate
from canonical JSON equality attributes.
