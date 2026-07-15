# Branch GC Deletion Proof + pqueue-c33c367e Release Interaction — Review Gate

- Bead: `pqueue-c2f0b050` (child 3 of 3 of `pqueue-9e8c0378`)
- Base revision: `817e828be21a58ea1bea1ae0218826b31fc42fd7`
- Governing artifacts named by this bead:
  - `docs/perf/design/manifest-compaction-hotpath.md:374` — the `ManifestHeadBlob` permanent-head contract
    (§6.1) that below-floor branch GC deletion eligibility must respect: below-floor manifest addresses stay
    occupied (never freed) as the stale-writer fence, `retention_floor_through` bounds reclamation, and the
    line-388 note records the `pqueue-c33c367e` conclusion this bead must carry into release notes.
  - `docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224` — AC-TXN-4, the object-log
    crash-point matrix, which requires 0 lost accepted items / 0 duplicate active leases / orphan segments
    ignored-or-reconciled per TD-004 across every commit cut point; branch GC deletion eligibility must not
    weaken that matrix.
  - Dependency `pqueue-8928baec` ("objectlog manifest compaction: reclaim tombstone + superseded floor
    entries below the retention floor") — **status: closed**. It is what makes physical deletion of
    below-floor manifest entries possible in the first place; this bead's release-note/review evidence
    documents the branch-GC consumer of that closed work.
  - `pqueue-c33c367e` interaction: this bead ID no longer exists in the tracker (`ddx bead show
    pqueue-c33c367e` returns `bead: not found`, re-confirmed for this bead). The conclusion to carry forward
    is the one already recorded at `docs/perf/design/manifest-compaction-hotpath.md:388`: "Owner-fence
    evaluation for `pqueue-c33c367e`: evaluate the deferred server-wiring change before any later child
    relies on it. Under the current protocol, the index-CAS fence still requires below-floor manifest
    addresses to remain occupied, so `pqueue-c33c367e` does **not** change the rollout safety envelope for
    this bead." Branch GC's deletion eligibility depends only on the persisted source-pin/branch registry and
    inherited floor/head metadata — not on the deferred server-side `fence_epoch` wiring — so
    `pqueue-c33c367e` (whether or not it ever lands) does not widen or narrow the retain-while-readable or
    final-readable-branch-deletion envelope. This conclusion, and the operational-justification condition, is
    carried into `docs/releases/v0.14.0.md`.

## Scope of the code under review

Sibling beads `pqueue-635500fb` (retain-while-readable, closed) and `pqueue-29a6c98c` (final-readable-branch
physical deletion, closed) already implemented the deletion-eligibility change in
`crates/pqueue-objectlog/src/segmented.rs`:

- `read_branch_registry` (~1900) / `live_branch_registry` (~1914) / `branch_pins_segment` (~1926) — the
  live-branch-pin proof, fail-closed (`EngineError::Storage`) on a listed-but-unfetchable registry entry.
- `expire_segments_through` (~2386) — the GC/trim entry point: skips any below-floor entry a live branch
  still pins, otherwise deletes the segment object, marks the manifest entry reclaimed, deletes the legacy
  manifest copy, and advances the durable contiguous reclamation watermark.
- `lowest_branch_pinned_below` (~2512) and `contiguous_manifest_deletion_watermark_from_entries` (~1215) —
  supporting watermark logic that never advances past a pinned or unreclaimed entry.

This bead's own scope is documentation and review evidence only — no runtime behavior changed.

## TestBranchGcDeletesBelowFloorAfterLastReadableBranchReviewRustGate

```text
$ rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture TestBranchGcDeletesBelowFloorAfterLastReadableBranch
```

Both named symbols exist on this branch (implemented by the closed sibling beads) and both pass:

```text
test TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinal ... ok
test TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed ... ok
test TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinalConservative ... ok
test TestBranchGcDeletesBelowFloorAfterLastReadableBranch ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 114 filtered out
```

Full `pqueue-objectlog` suite (lib + all integration binaries) also passes; no regression.

## TestBranchGcDeletesBelowFloorAfterLastReadableBranchReviewWorkspaceQualityGate

| Gate | Command | Result |
|---|---|---|
| fmt | `rustup run 1.92.0 cargo fmt --all --check` | clean |
| clippy | `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | clean, 0 warnings |
| go test | n/a — no `go.mod` or Go packages found under the repo root |
| lefthook | `lefthook run pre-commit` | `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found` (exit 0) — no lefthook config present in this workspace, consistent with every prior bead in this queue; recorded as an operator-required gate (config absent, not a failure) |
| pr-gate enforcing | `bash scripts/ci/pr-gate.sh --mode enforcing` | `=== pr-gate [enforcing] PASSED ===` — fmt, ledger validator tests, coverage-threshold fixtures, product-workflow-suite names, full workspace test run (sqlite/engine/conformance/etc. all green, `pqueue-engine: lines 84.96% (6523/7678)`), build-closure integrity (`pqueue-131eadfa: live closure verified`), release gate (SMOKE lane) PASSED, nightly gate passed |

## TestBranchGcDeletesBelowFloorAfterLastReadableBranchReviewGate — Codex adversarial review

`codex exec` (codex-cli `0.144.3`) was attempted directly:

```text
$ timeout 25 codex exec --sandbox read-only --skip-git-repo-check --json "Say OK" < /dev/null
Reading additional input from stdin...
```

Consistent with every prior review gate in this queue (e.g. `.ddx/executions/20260714T164924-f2b3fc7f/review-gate.md`,
`.ddx/executions/20260714T173456-d90d6e12/branch-gc-retain-while-readable.md`), `codex exec` hangs
indefinitely non-interactively in this worktree. **Classification: operator_required.**

In its place, an independent adversarial-review sub-agent was dispatched with no access to this conversation's
context — only the repo files, told explicitly to act as a critic (not a validator), to adversarially assess
whether the branch-GC deletion-eligibility code and its four proof tests actually establish the claimed
"retained while any branch is readable, physically deletable only once no branch is readable, fail-closed on
ambiguity" property, and to classify every finding as BLOCKING / NON-BLOCKING / NOTED-UNCERTAINTY with
file:line evidence. Its full, unedited output follows.

---

## Independent Reviewer Result (verbatim)

# Adversarial Code Review: Branch-GC Deletion-Eligibility Path

**Scope reviewed:** `crates/pqueue-objectlog/src/segmented.rs` — `read_branch_registry` (~1900), `live_branch_registry` (~1914), `branch_pins_segment` (~1926), `expire_segments_through` (~2386), `lowest_branch_pinned_below` (~2512), `contiguous_manifest_deletion_watermark_from_entries` (~1215), `manifest_reclamation_candidates_from_entries` (~2784), plus the branch-creation path (`branch_attempt`, ~2032) and the four named tests in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` (lines 4294–4644). All four tests were run (`cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestBranchGcDeletesBelowFloorAfterLastReadableBranch`) and pass.

## Summary of what I verified as correct

- **Fail-closed propagation is real and complete.** `read_branch_registry` (segmented.rs:1900-1911) returns `Err(EngineError::Storage(...))` the instant a listed key's `store_get` returns `None`, rather than skipping it. `branch_pins_segment` (1926-1936) and `live_branch_registry` (1914-1924) propagate that `Err` via `?` with no intervening `.ok()`/`.unwrap_or(...)`/swallow. In `expire_segments_through`, the call site `if self.branch_pins_segment(source, entry.first_seq, now_ms)? { continue; }` (line 2405) uses a bare `?`, so a registry-read failure aborts the whole function immediately with `Err`, before any `store_delete` for that entry or later entries runs. This further propagates through `compose.rs`'s `expire_segments_through(...)?` call (line 1372) up to `trim_reclaimable_segments`, with no swallow anywhere in the chain I traced (grepped every call site of `expire_segments_through`).
- **Comparison direction is correct, not inverted.** `branch_pins_segment` uses `first_seq <= meta.cut_sequence` (line 1935) — a segment is pinned iff its start is at or before the branch's cut, i.e. `cut_sequence >= first_seq`, exactly as claimed. I did not find an off-by-one: a branch cut exactly at a segment's `first_seq` correctly pins that segment (it needs at least that one record).
- **The four tests genuinely exercise fail-closed vs. normal-progress behavior and pass.** `OrphanBranchFaultStore.arm_missing_get` (2894-2925) faults only `get()` for one specific key while leaving `list()` intact — this is precisely the "listed-but-unfetchable" scenario the code defends against. The `FailClosed`/`FinalConservative` tests correctly assert `EngineError::Storage` containing `"missing branch registry entry"` and that objects survive; the `Final`/non-conservative tests correctly assert physical deletion of both segment objects and the legacy manifest copies (`manifest_key_s`, i.e. `manifest/{index}.json` — note this checks the *legacy* compat key, not `manifest_head/`, which is intentionally never freed, only overwritten with a reclaimed marker; that's consistent with the documented "manifest addresses stay occupied" CAS-fence design, not a test bug).
- **Watermark monotonicity/safety.** `contiguous_manifest_deletion_watermark_from_entries` and `manifest_reclamation_candidates_from_entries` both stop advancing at the first branch-pinned or not-yet-reclaimed entry, and the documented invariant ("W always strictly below every live entry") holds up under my trace of `read_retention_floor`/`visible_manifest_deletion_watermark`.

## Findings

### 1. NON-BLOCKING — race between concurrent `branch()` creation and `expire_segments_through` can produce a hard, non-retried failure instead of a graceful retry

`branch_attempt` (segmented.rs:2032-2251) is PIN-FIRST: it publishes the source registry entry (line 2070-2074) before reading the floor and copying segment bytes. This correctly protects **committed** branches: `expire_segments_through`'s per-entry `branch_pins_segment` check (line 2405) is evaluated fresh, immediately before each delete, so once a pin is durably published it will be seen by any subsequent check.

However, there is a window where the *check* for entry X happens before the new branch's pin is published, but the *delete* for entry X (`store_delete` at line 2415) happens after the new branch has already published its pin and is mid-copy. In that window:
- If the racing `branch_attempt`'s `store_get(seg_key)` (line 2178-2179) runs *after* the delete, it gets `None` and returns `Err(EngineError::Storage("missing segment {key}"))` via `.ok_or(...)?` (line 2179).
- This is a **generic `Err`**, not the private `BranchAttempt::FloorAdvanced` signal. `branch_with_emission`'s bounded retry loop (1997-2010) only retries on `FloorAdvanced`; a bare `EngineError` is propagated straight to the caller (line 2005 `?`), even though the underlying cause (a floor/trim race during copy) is exactly the scenario the `FloorAdvanced` retry mechanism exists to handle gracefully.

This does **not** violate the safety property in scope — rollback (`cleanup_uncommitted_branch`) correctly removes the pin and no committed/live branch ever ends up depending on a deleted object — but it is a real robustness/liveness gap: `branch()` calls racing with an active trim can spuriously fail with a `Storage` error instead of being retried like the analogous `FloorAdvanced` case. Evidence: segmented.rs:2174-2182 (copy/fetch), 1997-2023 (retry loop only matches `FloorAdvanced`), 2379(comment)/2405 (per-entry check-then-delete in the same iteration with no re-check).

### 2. NOTED-UNCERTAINTY — the branch-pin mechanism appears to be strictly more conservative than the property it's documented to enforce, and none of the four tests falsify the stronger (unnecessary-retention) reading

Tracing `branch_attempt`'s copy loop (segmented.rs:2135-2188): for every data manifest entry in `[source_floor+1 (or genesis), cut_sequence]`, it unconditionally fetches the source segment's bytes (`store_get(seg_key)`, line 2178) and re-writes them under a **new, branch-owned key** (`branch_segment_key(&branch, index, first_seq)`, line 795-800, which is always under the branch's own `shard_prefix` — distinct from source's, since `branch == source` is rejected at line 2042-2044). Every read path (`read_all`, `read_manifest`, etc.) is shard-scoped to whichever `QueueKey` is passed in; I grepped and found no code path where a branch's reads fall back to `source`'s objects. `expires_at_ms`/`branch_pins_segment` are used **only** inside the four in-scope GC functions (confirmed by grep across `segmented.rs`) — never in any read path.

Given that, once a branch has committed, it appears to be a fully self-sufficient physical copy that does not depend on `source`'s below-floor objects at all — the pin's real, load-bearing purpose (per the "CROSS-OWNER SAFETY" comment at 2046-2052) is to protect the brief **creation-time copy window**, not the branch's whole subsequent lifetime. The registry entry, however, is retained (and keeps blocking `expire_segments_through`) for the branch's *entire TTL*, which can be far longer than the copy window.

This means:
- The property is safe (never under-retains), but the four tests only demonstrate that the code *is* conservative, not that retention is *necessary* — in every multi-branch test, the wider-cut branch (`branch_b`, cut_sequence=3) always dominates and pins everything, so no test ever deletes a source object while a live branch that (per this analysis) would still need it remains. None of the tests delete a source segment out from under a still-registered-but-narrower-cut live branch to check its `read_all` still succeeds via its own copy — which would be the test that actually distinguishes "necessary for correctness" from "conservative by construction."
- Doc-comments/test names such as "`branch_b` ALONE keeps every below-floor segment retained" (test doc-comment, line 4294-4299) imply a read-dependency that, per my trace, doesn't exist post-commit. This is a documentation/narrative overclaim, not a safety bug — TTL-expired-but-undiscarded branches remain fully readable (since no read path checks `expires_at_ms`) regardless of what `branch_pins_segment` decides about `source`, so the TTL/expiry edge case the task asked me to check does not produce a hazard.

I flag this as NOTED-UNCERTAINTY rather than a firm finding because it rests on a negative claim ("no read path ever falls back to source") established by static tracing/grep rather than a runtime experiment (I did not add a test to empirically delete a source segment and confirm a live branch keeps reading, since the review is read-only and I did not want to modify the repo even transiently). If this reading is correct, the practical implication is purely a **storage-cost inefficiency** (below-floor source segments are held hostage for a branch's full TTL even though only needed for the initial copy window), not a data-loss or fail-open risk.

### 3. NON-BLOCKING — inconsistent partial-progress handling on a `branch_pins_segment` fail-closed error inside `expire_segments_through`

The loop in `expire_segments_through` (2398-2438) handles `store_delete`/`mark_manifest_entry_reclaimed`/`delete_manifest_entry`/fault errors uniformly via `error = Some(err); break;` (2411-2434), which lets execution fall through to the post-loop watermark-persist block (2449-2477) so that any entries reclaimed *before* the failure still get their watermark progress durably recorded — the function's own docstring is explicit about this ("a partial failure after some successful reclaim work should still durably record the safe prefix so a retry can resume from the last committed boundary", 2440-2442).

`branch_pins_segment`'s error (line 2405, `self.branch_pins_segment(...)?`) does not follow this pattern — it's a bare `?`, so it returns immediately, **skipping** the watermark-persist block entirely, even if earlier entries in the same pass were already successfully deleted and marked reclaimed. This is not a safety bug (an under-advanced watermark is always safe per the documented invariant, and already-reclaimed markers are idempotently re-processed on the next pass — this exact self-healing behavior is explicitly relied on elsewhere, e.g. `manifest_reclamation_candidates_from_entries`'s comment at 2827-2830 "later reclaimed entries remain eligible... within the same pass"). It is, however, an inconsistency with the function's own stated intent, and means a fail-closed registry error occurring after partial progress in the same call forces the next pass to redundantly rescan/re-touch already-completed reclaims rather than resuming past them.

## What I explicitly checked and found no problem with

- Watermark never advances past the authoritative floor entry or a still-pinned entry (`contiguous_manifest_deletion_watermark_from_entries` lines 1230-1253, `manifest_reclamation_candidates_from_entries` lines 2797-2836) — traced by hand against the "STOP at floor" / "STOP at pinned" break conditions.
- No error-swallowing between `expire_segments_through` and any caller — grepped every call site across `pqueue-objectlog` and `pqueue-engine/src/compose.rs`; all use `?`.
- `EngineError::Storage` is a distinct enum variant with no lossy `From`/mapping observed that could convert it into a non-error/success path.
- Segment/manifest deletion ordering inside `expire_segments_through` (delete object → mark reclaimed → delete legacy manifest copy) means a crash mid-pass can only leave *more* work for a retry, never a state where the manifest looks reclaimed but the object is still charged/undeleted, nor a state where the watermark advances past an undeleted object — the fault-injection seam (`FaultCutPoint::DuringSegmentExpiry`) and its associated error-capture confirm this ordering is intentional and enforced.
- `branch_registry_key`/`branch_segment_key`/`branch_metadata_key` namespacing (segmented.rs:775-808) rules out any accidental key collision between a branch's own objects and its source's.

---

## Disposition

**No BLOCKING findings.** Two NON-BLOCKING robustness/consistency observations (finding 1: a branch-creation
race can surface a spurious non-retried `Storage` error instead of transparently retrying; finding 3: a
fail-closed registry error mid-pass skips the partial-progress watermark persist that other error paths in the
same function perform) and one NOTED-UNCERTAINTY documentation-overclaim observation (finding 2: post-commit
branches appear to be fully self-sufficient physical copies, so the retained pin's real load-bearing window is
branch-creation-time, not the branch's full TTL — a storage-cost/retention-duration question, not a
correctness or fail-open risk). None of the three findings undermine the retain-while-readable,
final-readable-branch-deletion, source-pin, inherited floor/head metadata, or fail-closed properties this
bead's acceptance criteria require reviewing. No code changes are required by this review; findings 1 and 3
are candidate follow-up refinements, not blockers, and are not being actioned by this documentation-only bead
per its NON-SCOPE (no runtime behavior changes except minimal fixes required by review findings, and neither
finding is a required fix — both are already safe, just non-optimal).
