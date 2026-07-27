# Claude Fable Review: SP-06 Targeted Ownership-Handoff Warmup

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. SP-04 excludes queue labels, so per-handoff attribution required isolated snapshot deltas/dedicated recorders.
2. Identical fault schedules implicitly depended on optional SP-02 and omitted existing scripted hooks.
3. The trigger relative to acquire/fence—the core safety/performance choice—was undefined.
4. SP-01's command budget could not be shared with read-side warmup.
5. A named replay-tail index does not exist.
6. The plan did not explain how authenticated warm bytes feed authoritative recovery.
7. Cache identity duplicated rather than reused SP-03 types.
8. Relative performance gates lacked environment, samples, avoidable definition, and absolute effect floor.
9. No queue-level cancellation seam exists; late jobs after ownership loss were unhandled.

## Incorporated Corrections

The revision defines isolated/per-arm attribution, scripted fault schedules with optional SP-02 upgrade,
post-fence hydration as the trigger, a separate SP-01-typed budget, real manifest replay objects, mutable-hint
and immutable-authentication rules, SP-03/SP-07 identity, 200-sample two-latency numeric gates, generation-
stamped cooperative jobs, and concrete reuse of existing budget/identity/recorder/LRU/dispatcher components.

## Re-review Result

Claude Fable confirmed every blocker resolved. Follow-ups now explain local owner generation versus durable
epoch, bind execution to SP-05's node-global background-work scaffold instead of the commit dispatcher, cover
v2 checksum authentication before SP-07, and freeze `progress_bound_ms=2000` before scripted measurements.
