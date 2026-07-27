# Claude Fable Review: SP-02 Deterministic Storage Simulation

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. The draft did not choose whether determinism covered a model interleaving, controlled production runtime,
   or a new simulation runtime, making the primary gate unfalsifiable.
2. Mutants had no stated injection target; mutating the model would test it against itself.
3. The focused CI cost ceiling was undefined and ignored runner disk pressure.
4. The plan did not define its relationship to TP-003's process-kill harness and AC-TXN-4.
5. Existing fault-cut vocabularies were inventoried but not made authoritative, while ambiguous store outcomes
   required by SP-03 were missing.
6. Full multi-owner handoff is ahead of shipped code and needed a model-only boundary.

## Incorporated Corrections

The revision selects deterministic operation-level simulation over real synchronous production transitions,
keeps Tokio scheduling out of scope, mechanically isolates the oracle crate, mutates only the system under
test, reconciles existing fault events, models ambiguous store outcomes, scopes fencing to shipped behavior,
adds historical and synthetic mutants, sets a 32-operation shrink target and versioned corpus, defines a
five-minute/one-GiB CI ceiling with repeat-suite integration, and binds evidence to TP-003.

## Re-review Result

Claude Fable confirmed all blockers resolved. Follow-up clarifications now exclude async coordinator scheduling
from mutation claims, keep model identifiers independent of production crates, require an explicit zero-flake
repeat-suite threshold, and describe how the fake store represents versioning and durable-effect-then-error.
