# pqueue-635500fb — retain below-floor source objects while any branch remains readable

## Root cause found

`SegmentedObjectLog::read_branch_registry` (crates/pqueue-objectlog/src/segmented.rs), the sole feed for
`live_branch_registry` → `branch_pins_segment` — which gates deletion eligibility in `expire_segments_through`,
`contiguous_manifest_deletion_watermark_from_entries`, and `lowest_branch_pinned_below` — silently `continue`d
past a branch-registry key that `store.list()` returned but whose `store.get()` unexpectedly returned `None`.
That treated a still-committed, still-readable branch as unpinned on nothing more than a storage read
inconsistency, letting `expire_segments_through` delete a below-floor source object a live branch could still
need. `gc_orphaned_branches` already treated the identical class of inconsistency (listed-but-unfetchable
registry entry) as a hard `EngineError::Storage("missing branch registry entry ...")`; the trim/deletion path
did not have the same protection.

## Fix

`read_branch_registry` now returns `EngineError::Storage("missing branch registry entry {key}")` instead of
silently skipping, matching `gc_orphaned_branches`'s existing fail-closed contract. This closes the gap for all
three downstream consumers without touching branch creation, `discard_branch`, or `gc_orphaned_branches` itself.

## Tests added (crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs)

- `TestBranchGcDeletesBelowFloorAfterLastReadableBranch` (AC1): two committed branches (`branch_a` cut@0,
  `branch_b` cut@3) from a 4-segment trimmed source. `expire_segments_through` deletes nothing while both are
  live. After discarding `branch_a`, every below-floor segment — including ones `branch_a` never needed — stays
  retained purely because `branch_b` alone can still read them (proves the OR-across-branches semantics, not
  just "protected while every branch needs it"). Only after `branch_b` is also discarded do the segments become
  reclaimable via the pre-existing trim path.
- `TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed` (AC2): one committed, live branch (large
  TTL, never discarded) whose source-pin registry key is made unfetchable via fault injection
  (`OrphanBranchFaultStore::arm_missing_get`) while `list()` still returns it. `expire_segments_through` must
  return `Err(EngineError::Storage(_))` containing "missing branch registry entry" and delete nothing; the
  below-floor segment object stays present; after the fault clears the branch still reads all 4 of its commands.

Both tests failed before the fix (the fail-closed test observed `Ok(4)` — all four below-floor segments
deleted while the branch was still live) and pass after it; verified independently by the adversarial reviewer
(see below), who reverted just the source change and re-ran the new tests to confirm they are load-bearing.

## pqueue-8928baec / pqueue-c33c367e evaluation

- `pqueue-8928baec` (durable read-horizon watermark) is the dependency this bead builds on; unaffected by this
  change — the watermark advance path (`contiguous_manifest_deletion_watermark_from_entries`) is one of the
  three fixed consumers and now shares the same fail-closed registry read as the deletion path.
- `pqueue-c33c367e` (deferred server-side `fence_epoch` wiring) does not change the conclusion: the trim path's
  deletion eligibility depends only on the persisted source-pin registry / branch metadata (a store-object
  proof), not on the deferred owner-fence wiring. Recorded in `docs/releases/v0.14.0.md` for release-note
  follow-up, consistent with how prior sibling beads (`pqueue-37a550c6`) recorded the same conclusion for
  `gc_orphaned_branches`.

## Gates (AC3/AC4/AC5)

| Gate | Command | Result |
|---|---|---|
| Objectlog focused | `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestBranchGcDeletesBelowFloorAfterLastReadableBranch -- --nocapture` | 2 passed |
| Objectlog full | `rustup run 1.92.0 cargo test -p pqueue-objectlog` | lib: 23 passed; 12 integration binaries incl. 116 in `segmented_s3_substrate_tests`; 0 failed |
| fmt | `rustup run 1.92.0 cargo fmt --all --check` | clean (after `cargo fmt --all` normalized the two new tests) |
| clippy | `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | clean, 0 warnings |
| go test | n/a — no `go.mod` or Go packages found under the repo root |
| lefthook | `lefthook run pre-commit` | `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found` (exit 0) — no lefthook config present in this workspace, same as every prior bead in this queue; recorded as an operator-required gate (config not present, not a failure) |
| pr-gate enforcing | `bash scripts/ci/pr-gate.sh --mode enforcing` | `=== pr-gate [enforcing] PASSED ===` — fmt, ledger validator tests, coverage-threshold fixtures, product-workflow-suite names, full workspace test run (sqlite/engine/conformance/etc. all green), build-closure integrity, release gate (SMOKE lane) PASSED, nightly gate passed |
| Codex adversarial review | `codex exec --sandbox read-only --skip-git-repo-check --json "Say OK" < /dev/null` under a 30s timeout | Hangs non-interactively (`Reading additional input from stdin...`, no further output) — same `operator_required` classification as the established precedent in `.ddx/executions/20260714T164924-f2b3fc7f/review-gate.md`. Substituted with an independent adversarial-review sub-agent (no conversation context, told to act as a critic) per that same precedent. |

### Independent adversarial review (substitute for Codex, verbatim verdict)

**Verdict: PASS.** No BLOCKING findings. The reviewer independently reverted the source fix and re-ran the new
tests to confirm the old code actually deleted all four below-floor segments while the branch was still live
(`Ok(4)`), then confirmed the fix + full `cargo test -p pqueue-objectlog` (295 tests, 0 failed, 1 ignored) is
green with the fix restored. It confirmed all three consumers of `branch_pins_segment` route through the fixed
`read_branch_registry` with no bypass path, and that the sibling bead's non-goal (physical deletion after the
*last* readable branch discards) is untouched.

Three NON-BLOCKING / NOTED-UNCERTAINTY items were raised, none requiring a code change in this pass:

1. **NON-BLOCKING** — when the new registry-read error fires mid-loop in `expire_segments_through`, the
   function returns immediately via `?` without persisting the deletion watermark for entries already
   legitimately deleted earlier in the same pass (unlike a `store_delete` failure, which persists partial
   progress before returning). Does not violate the retain-while-readable property (no premature deletion; a
   later pass safely re-verifies), but is an inconsistency between the two error paths in the same loop. Left
   as follow-up.
2. **NON-BLOCKING** — the new two-branch test only discards the narrower-cut branch (`branch_a`) and leaves the
   wider-cut branch (`branch_b`); it does not additionally cover discarding the *wider*-cut branch first while
   the narrower one remains. Coverage gap, not a safety gap (`branch_pins_segment` composes independently
   per-branch via `.any(...)`).
3. **NOTED-UNCERTAINTY** — the fail-closed fix trusts that a `list()`-then-`get()` inconsistency is always an
   anomaly worth erroring on; on a backend with eventual-consistent listing this could turn a transient
   condition into a hard trim failure. Noted as the same intentional "fail closed over fail open" design choice
   `gc_orphaned_branches` already makes, not a new risk introduced by this change.

Two BLOCKING findings from the precedent review-gate (`branch_attempt`'s pin-publish-vs-delete TOCTOU;
`gc_orphaned_branches`'s missing cross-instance epoch fence) are unrelated to this diff's scope — this change
does not touch `branch_attempt`, `gc_orphaned_branches`'s epoch handling, or the delete-vs-publish ordering — so
they are not re-raised here.

## Non-scope compliance

- No physical-deletion protocol change was made after the last readable branch is removed/advanced — that
  remains the pre-existing `expire_segments_through` path, only its input-safety (registry read) was fixed.
- No branch-creation behavior changed.
- No new queue semantics or user-facing API changes.
- No existing atomicity/orphan-GC/source-pin/retention-floor/fail-closed guarantee was relaxed — only tightened.
