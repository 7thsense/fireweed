---
ddx:
  id: review-tp005-performance-matrix
  depends_on:
    - tp-fireweed-performance-matrix
  status: accepted
---

# TP-005 performance matrix review

## Outcome

TP-005 converged after three adversarial review rounds. The final round used
two independent reviewers against the repository's public facade and returned
PASS from both, with no implementation-blocking contradictions.

## Material corrections

- Distinguished the legacy per-command local object log from segmented
  object-log compositions and prohibited misleading comparisons.
- Defined comparison classes by the actual response success barrier.
- Bound constructors, queue methods, workloads, sample counts, statistics, and
  smoke/full verifier semantics to exact executable contracts.
- Matched object-log/PostgreSQL schema derivation and hex-encoded S3 physical
  namespace behavior in the current facade.
- Required exact cleanup allowlists, dedicated service locks, pushed-source
  provenance, same-commit conformance tests, bounded fragments, checkpoints,
  and verified resume behavior.
- Populated recovery and maintenance workloads and separated async catch-up
  from timed response latency.
- Kept TP-005 host-bound and explicitly ineligible as TP-002 scale evidence.

## Review history

Round one blocked on incomplete construction, cleanup, correctness, sampling,
and evidence contracts. Round two blocked on concrete implementation mismatches
in the legacy object log, derived PostgreSQL schemas, S3 namespace encoding,
smoke thresholds, maintenance population, and runtime controls. Round three
verified those fixes against the current public constructors and methods and
returned two PASS verdicts.

## Implementation convergence

The executable implementation received a separate adversarial review after the
spec converged. Its blocking findings were corrected, including same-commit
conformance inputs, canonical measured scheduling, queue limits and content
verification, pre-authorized cleanup, resumable checkpoint semantics, secret
redaction, complete provenance, and bounded signal handling. The final review
returned PASS with no release-blocking findings. The locked benchmark library
suite, Clippy with warnings denied, shell checks, document validation, and the
six-cell smoke matrix all pass.
