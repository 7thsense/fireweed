# Astra Ultra iteration 17 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base d3850e43.
Scope: shared selectors and pure per-target administrative proposals, plus
behavior-preserving extraction of the existing administrative transition helper.
No blocking findings remain.

Reviewed both new administration modules and final scope documentation. Raw stored
lifecycle remains distinct from evaluated_at-dependent lease/effective lifecycle.
Empty All/Any, first-match ownership before preconditions, field presence/null
bytes, removal-before-addition precedence, compact before metadata, highwater
validation, no-op versions and unloaded-body Preserve semantics are covered.
Existing operational-clock callers retain their behavior; no claim token is
created by operator-time classification.

Review removed an unintended Turso-specific 1000 addressed cap: the common count
limit applies, and a 1001-entry regression proves parity. Duplicate addressed
inputs remain representable for positional Invalid outcomes in the future
operation planner. Iterator stacks bound validation/evaluation scratch by depth.
Standalone validation covers count/type/structure/body constraints; full byte
admission and bounded owned-AST decoding remain explicit integration gates.

Root verified seven administrative regressions, core23 and engine65 queue tests
before the final added count regression, native PostgreSQL conformance/reopen
following extraction, strict library Clippy, formatting and diff checks. Astra
independently reviewed final source/docs and checked the diff.

Approval does not qualify whole-operation atomicity, native indexed selection,
uniqueness, bounded spooling/publication, retained receipts, true preview, S3/Turso
or consumer activation. The 5001-target helper test proves no helper match cap;
it is not whole-selector publication evidence.
