# Astra Ultra plan review

Reviewer: `gpt-6-astra`, reasoning effort `ultra`, explicitly requested by user.
Initial disposition: **REQUEST_CHANGES**, all compatible with vision/PRD.

| Finding | Folded correction |
|---|---|
| Greedy concurrency successor selection can reject valid paths or permit branch switching. | Carry the full matching model-state set; every later edge must originate in that set. |
| Selected shortest prefixes do not prove every nondeterministic SUT edge. | Record intended and actually matched edge coverage separately; never label smoke replay graph-exhaustive. |
| Two owner IDs cannot identify every incarnation across reopen/handoff. | Retain up to three issued sessions; probe both previous credentials and both operation orders. |
| Coarse observations could hide changed receipt identity/lease details or extra effects. | Injective bootstrap IDs, predetermined request/token identity, exact worker/expiry/version/attempts, history and receipt multiplicity/fingerprint checks. |
| Observation/reopen could silently drain apply and erase intended debt. | Direct committed reads with unchanged exact-prefix cursor; record pre-close/post-close/post-open frontiers and distinguish startup repair. |

These corrections were incorporated before execution. The reviewed scope is real
local-S3 × Turso with a one-process ephemeral CP, graceful reopening and simulated
client nonreceipt; cloud S3, OS process kill and whole-program proof remain excluded.
