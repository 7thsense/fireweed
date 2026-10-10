---
ddx:
  id: build-generated-trace-replay-evidence
  type: implementation-evidence
  depends_on: [build-generated-trace-replay, contract-model-trace-replay, tp-formal-analysis]
  status: complete
---

# Generated trace replay execution evidence

The completed package runs independently generated predictions against real S3
object-log × Turso × AsyncProjection. The independent graph is complete within
its declared finite domains: **6,386 states / 20,913 edges**. The canonical smoke
corpus selects **91 traces** and **83 intended semantic edges**. Final execution
passed **91/91 traces**, **282 action observations**, and witnessed **119
actual model edges**. Actual edge coverage is selected coverage, not graph-wide
implementation exhaustiveness. Both concurrent serial outcomes are permitted.

[Replay report](evidence/generated-trace-replay/replay.json),
[canonical corpus](evidence/generated-trace-replay/corpus.json) and
[per-step observations](evidence/generated-trace-replay/observations.json) retain
exact source/corpus/log hashes, compatible predecessor/successor sets, actual
matched edges and physical frontier diagnostics. All four Rust replay/guard/control
tests passed. All **22 Python tests** passed, including canonical all-edge
selection and fail-closed corruption controls. The existing nine abstract models
remain complete at **21,517 states**, with **20 detected negative controls**.

## Confirmed defects and repairs

| Trigger and before result | Repair and verified result |
|---|---|
| Fresh ephemeral CP reopens with assignment1 while durable storage fence1: Reopen reused1, contrary to new-incarnation prediction2. | Shared ownership fence helper advances storage even at numeric equality for a new incarnation. Both sync/async equal-epoch restart and different-owner handoff regressions fail before and pass after. Same-session reaffirmation preserves its fence. |
| Claim0 publishes while apply is paused, then Claim1 has a remaining eligible applied row: coverage wait timed out. | Under the generation turn and exclusive selection fence, validate a complete contiguous unexpired authoritative Claim-only suffix and exclude its distinct leased IDs. Each accepted member's credential is checked independently; SELECT verifies minimum coverage on its exact serving connection. Sequential/concurrent generated traces pass with apply paused. |
| Expired PendingFence reacquired by the same owner could be mislabeled same-session and reuse storage2 for assignment2. | Reaffirmation requires unchanged owner and assignment and a non-PendingFence prior lease. Both regression APIs failed with (2,2) versus expected (2,3), then passed. Existing Assigned expiry reaffirmation remains unchanged. |

Original source-bound counterexamples and logs are retained in
[evidence manifest](evidence/generated-trace-replay/manifest.json). The
PendingFence pre-fix artifact is explicitly labeled a transcript captured from
tool output; it is not presented as a redirected subprocess log. The complete
post-fix engine log passes **283 unit + 9 integration tests**, zero ignored.

## Final qualification and review

[Conformance](evidence/generated-trace-replay/conformance.json) passes **370 tests**
across seven suites, zero ignored: engine283, Turso16, real filesystem-log/Turso2,
packed-apply33, publication/recovery22, async-debt9 and public S3/Turso receipts5.
Both this refreshed report and final replay have identical before/after source
hash maps matching the final tested source; the existing unrelated PostgreSQL edit
is captured in provenance and preserved outside the commit.

Native S3 conditional-create/update preflights pass for both isolated fixture
runs. Evidence records the RustFS binary hash and owns/terminates only its child
process. Masked fixture credentials are not retained. The source/log manifests
map original absolute run paths to retained repository artifacts without changing
original report bytes.

[Astra plan review](reviews/generated-trace-replay/astra-ultra-plan-review.md)
and [implementation review](reviews/generated-trace-replay/astra-ultra-implementation-review.md)
capture every finding and compatible proactive correction. Astra Ultra approved
the bounded package with no remaining must-fix findings; the final source-bound
replay and conformance refresh satisfy its evidence conditions.

Formatting, patch whitespace and GitHub Actions policy checks pass. CI runs
canonical generation and all Python controls, then smoke replay using pinned
RustFS, and uploads corpus/report/observation/log/CAS artifacts even on failure.
The deeper `--replay-profile edges` command is provided in
[scripts/formal/README.md](../../../scripts/formal/README.md); its corpus generation
and selection metadata are tested, but full real-S3 all-edge replay was not run.

## Reproduction and limits

```sh
python3 -m unittest discover -s scripts/formal -p 'test_*.py'
python3 scripts/formal/check.py --output target/formal-analysis.json
CARGO_INCREMENTAL=0 python3 scripts/formal/with_rustfs.py \
  --binary /absolute/path/to/rustfs --replay-profile smoke \
  --offline --output target/model-replay.json
CARGO_INCREMENTAL=0 python3 scripts/formal/with_rustfs.py \
  --binary /absolute/path/to/rustfs --offline \
  --output target/formal-conformance.json
```

The model has one queue, two items/accepted requests, two owners, up to three
issued sessions, one handoff, one graceful reopen and fixed logical time. The
claim-tail repair is bounded to16 commands /8,192 distinct IDs. Mixed, expired,
oversized or otherwise unvalidated tails retain the existing conservative
coverage wait; complete ordinary-poll compliance for those paths is not claimed.

This is one-process ephemeral-CP evidence on real local S3, graceful reopen and
simulated client nonreceipt. It does not qualify shared multi-process ownership,
OS process kill, production-cloud S3, universal Rust refinement or FR-12's numeric
deadline. Existing foundation evidence remains historical and untouched. DDx CLI
and the repository-local skill installation are unavailable; the accessible DDx
skill was read, no tracker was fabricated, and no bead completion is claimed.
