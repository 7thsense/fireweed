# Later-phase pipeline plan — adversarial round 1

Harnesses: `ddx run --harness claude` and `ddx run --harness codex` against
`later-phase-speed-review-prompt.md`. Both **BLOCK**.

## Verdicts

| Harness | Verdict |
|---|---|
| Claude | BLOCK |
| Codex | BLOCK |

## BLOCKING (both)

1. **Cut 1 response/command wire.** `Updated` requires `item_id` + `client_item_key` + `item_version`. Misses have no truthful payload. `UpdateFieldsCommand.item_id` is required serde. Folded: optional `client_item_key`; `item_id == 0` means resolve-by-key at apply; response fields are advisory under `AsyncProjection` (peek fill, else `item_id=0` / `item_version=0` / key from request). `expected_item_version: Some` keeps today’s Conflict-on-peek-hit; peek-miss still logs the expected version and apply skips.
2. **Cut 1 API-001 skip-as-Updated.** Leased/terminal/mismatch must not pretend to land. Folded: peek-hit Conflict/Terminal/NotFound stay plan-time and are **not logged**. Peek-miss is apply lag, not NotFound; it is logged. `api001_batch` skip-Leased at apply only; FAC-1 leased reschedule unchanged.
3. **Class S cursor vs in-flight produce.** `last_produce` is written after append; reserve is before. Folded: SS phases are sequential — at the produce→claim boundary every produce is appended. Scope the seam as **appended-but-unapplied**. In-flight linger is pre-existing and out of this cut. Per-claim: remember the produce position we last caught up to; skip the wait while `last_produce` has not moved.
4. **Group rerank.** “Refs + summary row” cannot elect an unchanged member. Folded: **keep `refresh_group_summaries` when the representative moved.** Do not invent a wrong incremental algorithm.
5. **Claim apply rebuild.** Rebuild does not run the live lease txn. Folded: keep Claim apply lease+bearers+group-remove. Live Class S does not duplicate group maintenance.

## BLOCKING (Claude only) — accepted

6. **Deliver ≥ ingest arithmetic.** Cut 1 must **overlap** apply with produce, not dump it into P4’s wall. If apply of updates keeps up, the seam is a no-op. Inflight=8 on claim is required to match ingest’s concurrent appends; inflight=1 cannot beat a ~23 ms PUT.

## WARNINGs accepted

- P2/P3 `live_item` already `catch_up_produce`s; last_produce includes UpdateFieldsBatch; samples are outside the phase wall. No harness change.
- Do not delete the 300 ms delay as the sole “fix”; delete it because it is a sleep in apply, and judge ingest p50 separately.
- Outbox delete stays its own txn; reorder before enqueue. Fold into next lease later if still hot.
- Finalize apply must no-op if the item is no longer leased (TOCTOU on reader validate).
- P4 inflight=8 loop must not treat one empty claim as “all done.”

## Folded into

`docs/helix/04-build/later-phase-speed.md` (this round). Implementing that text, not the pre-review cuts.
