# SS phased capacity plan — adversarial round 1

- **Axis:** harness (claude, codex)
- **Prompt:** `ss-phased-capacity-iteration-review-prompt.md`
- **Policy:** any BLOCKING stops; WARNING with 2-harness agreement treated as BLOCKING
- **Date:** 2026-08-15

## Verdicts

| Arm | Verdict |
|---|---|
| claude | **BLOCK** |
| codex | **BLOCK** |
| Aggregate | **BLOCK** the draft; **adopted** rewrite before beads |

## Agreement

| Topic | Claude | Codex | Disposition |
|---|---|---|---|
| `select_item_claim` + `metadata_equals` scans all eligible | BLOCKING | (implied by claim-filter BLOCKING) | **Adopted:** gated P4 has no predicate; I-select required before any filtered arm |
| P2/P3 claim→`BatchUpdate`→release is not a public API | BLOCKING | — | **Adopted:** pending `BatchUpdate` by P1 keys |
| I2 breaks `to_claimed` echo on `open_sqlite` | BLOCKING | BLOCKING | **Adopted:** read-time echo in-scope |
| I2 hides items from query / unique validate | BLOCKING | BLOCKING | **Adopted:** `index_fields` keying required |
| I4 lazy claim indexes vs ADR-013 / empty index | BLOCKING | BLOCKING | **Adopted:** I4 A/B deleted |
| I0 ignored-test / N=200 verify | — | BLOCKING | **Adopted:** real test, SS_N=10000 |
| N=100k as G baseline | WARNING | BLOCKING | **Adopted:** calibration only |
| Worker concurrency unspecified | BLOCKING | BLOCKING | **Adopted:** workers=1, in-flight=1 |
| G3 wall vs G1∧G2 arithmetic | BLOCKING | — | **Adopted:** per-phase gates; wall is stretch |
| I3 invalidation incomplete | WARNING | BLOCKING | **Adopted:** invalidation matrix; cache not unique authority |
| Host class undefined | WARNING | WARNING | **Adopted:** H-server or recorded exception |
| Measurement schema thinner than discover spec | WARNING | BLOCKING | **Adopted:** full per-call percentiles |
| Stop rule unfalsifiable | BLOCKING | BLOCKING | **Adopted:** Instant spans + 6-slice cap |

Disagreement: Claude treats claim-selection as the *primary* wrong layer; Codex treats I0/I2 spec gaps as primary. Both are correct. The rewrite addresses both; it does **not** put I-select on the G-path because the gated P4 no longer uses a predicate.

No second adversarial round: the BLOCKING items were specification holes, now closed in the adopted plan. A re-review is warranted only if I0’s public loop is changed again.
