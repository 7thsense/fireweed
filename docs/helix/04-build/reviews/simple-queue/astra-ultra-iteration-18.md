# Astra Ultra iteration 18 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base a1a59feb.
Scope: shared in-memory administrative-plus-ordinary group planning oracle.
No remaining blocking findings.

Reviewed positional addressed results, compact per-clause histograms, missing and
skipped target ownership/overlap, zero-effect plans and fail-closed native first
ownership. Initial unique-owner collisions remain business UniqueConflict with
legacy Invalid mapping required in Snorri. Accepted proposals colliding on a free
tuple reject the containing group; ordinary disjoint work participates in the
final all-group check and may reuse a released tuple. State CAS, final uniqueness
and stale ordinary claims roll back every speculative administrative success.
Operator time never makes an expired claimed token valid.

The source/owner map stays immutable and uncloned. Clause actions/ASTs are borrowed
without per-target AdministrativeEntry copies; resolved patch/replacement scratch
still requires caller admission. Review clarified this resource statement and
identified the old 1000 addressed cap as Turso-only, not generic port policy.

Root verified core23 and engine74 queue tests, including15 administrative tests,
strict four-library Clippy, formatting and diff checks. Astra independently
reviewed final source/docs and checked the diff. Tests include initial-owner and
free-proposal conflicts, ordinary reuse/collision, CAS/stale-lease rollback,
missing/duplicate/overlap outcomes and compact5001-target no-op plans.

Approval does not qualify native indexed streaming, bounded decoding/publication,
immutable administrative execution receipts, preview/replay, S3/Turso, consumer
cutover or performance. The in-memory oracle must become a shared admitted
accumulator/native bounded provider before large-selector backend activation.
