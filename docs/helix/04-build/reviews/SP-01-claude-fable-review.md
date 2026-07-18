# Claude Fable Review: SP-01 Global Buffer Byte Admission

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. The byte permit must transfer with an accepted request through coordinator and buffer ownership; caller
   cancellation after acceptance cannot release resident bytes.
2. Exact serialized size was unavailable at the proposed acquisition point. Serialization must be hoisted and
   threaded through the group-commit seam without reintroducing double serialization.
3. Runtime-neutral admission belongs in `pqueue-engine`, injected per ADR-017. Ordering and mitigation for
   byte-holding queue-gate waiters must be explicit.
4. Release semantics must cover fence, watermark self-fence, CAS loss, seal success, post-seal apply failure,
   abandon phases, close, and drain, with release at seal completion rather than seal start.
5. Existing seal-target and durable-byte counters are not superseded and must not be deleted.
6. Oversize commands need a permanent invalid-request error, distinct from retryable budget backpressure.
7. The async/cohort/purge baseline must be green and pushed before implementation.

## Incorporated Improvements

The revised plan adds the baseline gate, serialization refactor, engine placement, permit ownership/release
table, per-queue waiter mitigation, config relationships, error taxonomy, temporary frame accounting,
multi-queue contention benchmark, TP-003 link, and precise counter-retention exit criterion.

## Re-review Result

Claude Fable confirmed every blocker resolved. Non-blocking follow-ups were folded in: caller drop now
distinguishes early release from RAII release after actual removal, and the benchmark explicitly measures
serialization paid before rejection. Slice 1 must name the concrete permanent-oversize and retryable-budget
error variants and their transport mappings.
