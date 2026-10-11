# Iteration 85: owned full-prefix recovery composition

The facade composition layer now connects the native object-log and stock-Turso
API-007 kernels through a doc-hidden recovery pipeline. Consumer Queue remains
the existing mandatory interface; no transaction/session or readiness cache is
introduced. The pipeline is not yet a QueueStore or backend factory.

A bounded 64-task owner retains admission across sequential stage workers.
Cancellation after spawn does not interrupt fencing, reads or projection apply.
Task slots release before the final reservation; each native stage completes
without occupying an outer blocking worker, including max_workers=1.

Each operation freshly acquires the durable epoch and matching log bounds,
requires log_start=0 and a positive head, and reads the entire prefix from genesis
through the exact acquired target. Every record must be FWQL2 with contiguous
one-record offsets and historical epoch <= acquired epoch. ColdQueueHistory
validates exact address/raw definition pin, secret, epoch and insertion sequence.
Original genesis bytes apply when the projection is empty. Existing checkpoints
bind raw payload authority and must agree with sequence AND historical epoch at
the checkpoint boundary before suffix application. Ahead checkpoints, budget
failure, early EOF and concurrent cursor disagreement fail closed. Final native
progress must exactly agree with the validated history before coverage returns.

This validates prefix framing and the projection cursor binding, not an
independent scan of every cached data/index/receipt row. The full manifest index
residency/rewrite gate remains unqualified. S3/provider validation, consumer
operations, Snorri integration and matched settled benchmarks remain outstanding.

A real local-log/stock-Turso fixture exercises empty projection recovery, a
newly appended insertion suffix, repeated cached-prefix validation, too-small
record budget and caller cancellation while the only native worker is blocked.
It checks retained task slot/admission through terminal completion.

Astra-requested controls: independently mismatched insertion sequence and
historical epoch at cached cursor two reject at the prefix boundary before a
later valid suffix. An authoritative FWQL1 publication at offset one rejects
even when the equivalent checked projection already has cursor two. The queued
cancellation fixture uses one execution worker and two owned-work slots (one
blocker plus one queued acquisition), synchronized before acquire on the
current-thread test runtime; after abort/release the new suffix reaches cursor
three and final admission returns zero before worker shutdown. Too-small record
budget returns TooLarge rather than partial coverage.

Production facade Clippy with postgres is strict and passed. Legacy facade
lib-test imports produce five preexisting warnings under that feature set;
these are not suppressed or represented as a strict all-target pass.

Final rerun passed: two physical recovery fixtures (with the listed controls),
two shared ColdQueueHistory tests, strict production facade library Clippy with
postgres, and diff hygiene. The temporary limited-budget pipeline is explicitly
dropped before fixture files are removed.
