# Branch Inheritance Creation — Review Gate

- Bead: `pqueue-5e025a05` (child 3 of 3 of `pqueue-92a2e386`)
- Base revision: `08c55b4f6afb20f1d0530d265318e3a356a756b5`
- Governing artifacts named by this bead:
  - `docs/perf/design/manifest-compaction-hotpath.md:374` — the `ManifestHeadBlob` permanent-head contract
    (§6.1, lines 368-392) that branch creation's floor/head inheritance must respect: below-floor manifest
    addresses stay occupied (never freed) as the stale-writer fence, `retention_floor_through` bounds
    reclamation, and a durable `compacted_through_index` watermark bounds read cost.
  - `docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224` — AC-TXN-4, the object-log
    crash-point matrix, which requires 0 lost accepted items / 0 duplicate active leases / orphan segments
    ignored-or-reconciled per TD-004 across every commit cut point, including "after manifest before
    projection apply" and "during manifest CAS/fallback commit."
  - Dependency `pqueue-8928baec` ("objectlog manifest compaction: reclaim tombstone + superseded floor
    entries below the retention floor") — **status: closed**. Its acceptance criteria (bound manifest size via
    compaction, CAS/epoch-safe, no AC-TXN-3/behind-image/branch-inheritance regression) are what makes physical
    deletion of below-floor manifest entries possible in the first place; this bead's branch-inheritance
    substrate is the consumer of that closed work.
  - `pqueue-c33c367e` interaction: this bead ID no longer exists in the tracker (`ddx bead show pqueue-c33c367e`
    returns `bead: not found`). The conclusion to carry forward is already recorded in the governing doc itself
    at `docs/perf/design/manifest-compaction-hotpath.md:388`: "Owner-fence evaluation for `pqueue-c33c367e`:
    evaluate the deferred server-wiring change before any later child relies on it. Under the current protocol,
    the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e`
    does **not** change the rollout safety envelope for this bead." I.e. that deferred server-wiring change is
    inert with respect to branch inheritance's safety envelope; no separate carry-forward action is needed
    since the bead itself is gone from the tracker.

## TestBranchInheritanceCreationReviewGate

### Objectlog verification

```text
$ rustup run 1.92.0 cargo test -p pqueue-objectlog
```

23 lib unit tests + 12 integration test binaries, **293 tests total (1 ignored), 0 failed**. The
branch-inheritance-specific subset within `segmented_s3_substrate_tests.rs` (114 tests in that file, 0 failed)
includes the three tests this whole parent bead exists to prove:

```text
test branch_inheritance_uses_retained_floor_metadata ... ok
test branch_inheritance_source_pins_preserved ... ok
test branch_inheritance_seed_floor_edge ... ok
```

### Sqlite verification

```text
$ rustup run 1.92.0 cargo test -p pqueue-sqlite
```

14 lib unit tests + 18 integration test binaries, **320 tests total, 0 failed**.

### Engine verification

```text
$ rustup run 1.92.0 cargo test -p pqueue-engine
```

94 lib unit tests + `dependency_direction` (1) + `read_as_of` (1), **96 tests total, 0 failed**.

### Conformance verification

```text
$ rustup run 1.92.0 cargo test -p pqueue-conformance
```

14 lib unit tests + 6 integration test binaries, **111 tests total, 0 failed** (2 doctests ignored — no runnable
doctest bodies). Includes the retained-floor/deleted-manifest conformance suite directly relevant to this
bead's scope: `TestObjectlogDeletedManifestSourcePinRetentionFloor`,
`TestSqliteEngineBehindImageRetainedFloorHeadReplayRecovery`,
`TestSqliteEngineBehindImageDeletedManifestFailClosed`, `TestBehindImageFailClosedWithDeletedManifests`,
`test_behind_image_fail_closed_with_deleted_manifests` — all `ok`.

### Codex adversarial review

`codex exec` (codex-cli `0.144.3`, present at `/home/linuxbrew/.linuxbrew/bin/codex`, `auth.json` present) was
attempted directly. Every invocation — with `--sandbox read-only`, `--dangerously-bypass-approvals-and-sandbox`,
`--skip-git-repo-check`, `--json`, explicit `< /dev/null` stdin redirection, and with the harness sandbox
disabled — hung indefinitely (`Reading additional input from stdin...` and then no further output) and was
killed by a 15-40s `timeout` wrapper on every attempt; network egress to `api.openai.com` itself was confirmed
reachable in this environment (`curl` returned HTTP `401`, i.e. a real response, not a connection failure), so
the hang is specific to the `codex exec` CLI's non-interactive behavior in this sandboxed worktree, not a
network outage.

**Classification: operator_required.** The `codex` binary is installed and authenticated but does not complete
a non-interactive run in this execution environment; a human operator with an interactive terminal (or a fixed
non-interactive invocation) is needed to actually exercise it.

In its place, and consistent with this repository's own established evidence pattern for this exact AC type
(see `.ddx/executions/20260713T223339-0d9d6cb4/objectlog-adversarial-review.md`, "Independent reviewer:
sub-agent ..."), an independent adversarial-review sub-agent was dispatched with no access to this
conversation's context — only the repo files, told explicitly to act as a critic (not a validator), to
adversarially assess whether `branch_attempt` and its three proof tests actually establish the stated property,
and to classify every finding as BLOCKING / NON-BLOCKING / NOTED-UNCERTAINTY with file:line evidence. Its full,
unedited output follows.

---

## Independent Reviewer Result (verbatim)

## Review Result

**Verdict: BLOCK**

Two concurrency gaps survive in `branch_attempt`'s interaction with `expire_segments_through` and
`gc_orphaned_branches` that are not closed by the code's own stated invariants (PIN-FIRST, VALIDATE-AFTER-COPY,
the create/GC guard, the commit-epoch fence) and are not exercised by any of the three tests under review, nor
by the adjacent concurrency tests in the same file. Both are the exact classes of race the code's own comments
claim to have closed ("CROSS-OWNER SAFETY... HOLE B", "SCOPE — CROSS INSTANCE"), so the gap is between the
claimed guarantee and what is actually proven, not a speculative nitpick.

### Findings

| Severity | Area | Evidence (file:line) | Finding | Recommendation |
|---|---|---|---|---|
| BLOCKING | Segment-delete vs. pin-publish TOCTOU | `crates/pqueue-objectlog/src/segmented.rs:2396-2406` (check-then-delete), `:2061-2065` (pin publish), `:2169-2170` (GET failure), `:2234-2236` (non-retried propagation) | `expire_segments_through`'s per-entry loop does `branch_pins_segment` (a fresh LIST+GETs round trip) then, separately, `store_delete(seg_key)` — two non-atomic network operations. If a **new** `branch_attempt` publishes its source pin for that exact segment *after* the check ran but *before* the delete completes, the delete proceeds anyway: the pin-first invariant only protects checks that run *after* publish, not deletes whose check ran *before* it. The victim branch's own `store_get(seg_key)` then returns `None`, producing `EngineError::Storage("missing segment ...")` which is **not** the retryable `FloorAdvanced` signal — it propagates immediately and is never retried by `branch_with_emission` (only `FloorAdvanced` loops). So a cut strictly above the floor, correctly pinned first, can still permanently fail branch creation under ordinary cross-owner concurrent trim. None of the four `PeerTrimDuringBranch`/`PeerTrimBoundedAdvances`-based tests (`segmented_s3_substrate_tests.rs:1387-1691`) exercise this: they all inject the entire peer trim as one atomic unit at the single `DuringBranchCopy` fault point, strictly *before* the branch's copy loop starts touching any entry — never interleaved with an in-flight `expire_segments_through` iteration. | Either (a) treat a "missing segment" GET during branch copy as a retryable signal analogous to `FloorAdvanced`, or (b) close the TOCTOU itself (re-check the pin immediately before delete, or make delete conditional on the check snapshot), and add a test that fires the peer's pin-check-vs-delete window mid-entry rather than only before the whole loop. |
| BLOCKING | `gc_orphaned_branches` cross-instance safety claim is unenforced | `crates/pqueue-objectlog/src/segmented.rs:2315-2321` (docstring claim), `:2329-2368` (`gc_orphaned_branches`, no epoch param/check), `:2274-2290` (`cleanup_uncommitted_branch` re-lists the whole branch prefix including `branch.json`), test `branch_commit_is_fenced_on_the_source_epoch_and_cross_instance_gc_stays_safe` at `tests/segmented_s3_substrate_tests.rs:3329-3441` | The docstring asserts cross-instance safety rests on fencing the final commit on the source ownership epoch, implying a peer's `gc_orphaned_branches` call is safe because a superseded creator's commit-marker write self-fences. But that fence only fires if the GC-calling peer has itself become the new source owner. `gc_orphaned_branches` has **no epoch parameter and no ownership precondition check anywhere in its body** — nothing stops any instance sharing the store from calling it without ever contesting source ownership. If such a caller's marker-absent classification races a genuinely-committing branch's final `branch.json` write, `cleanup_uncommitted_branch`'s re-snapshot can sweep a fully committed, already-acknowledged branch into the delete loop — destroying its manifest/segments/commit marker and releasing the source pin protecting its segments. The only cross-instance test exercises exclusively the well-behaved case (owner acquires epoch *before* calling GC); it never tests a peer calling GC without first taking ownership, which the public API's signature does nothing to prevent or flag. | Either require an `expected_epoch` parameter on `gc_orphaned_branches` (epoch-fenced like `advance_retention_floor`), or re-verify the target branch's marker is still absent immediately before each delete inside `cleanup_uncommitted_branch` when called from GC. Add a test where GC is called by an instance that has *not* called `acquire_epoch(source)` first. |
| NOTED-UNCERTAINTY | Governing design doc vs. shipped behavior | `docs/perf/design/manifest-compaction-hotpath.md:368-392` (line 370: "physical deletion of manifest entries is out of scope for this bead and is deferred to later children") vs. `crates/pqueue-objectlog/src/segmented.rs:1133-1141` (`delete_manifest_entry`, called at `:2422`) and test `branch_inheritance_uses_retained_floor_metadata` which explicitly asserts below-floor legacy `manifest/{index}.json` keys are physically deleted | The doc cited as this review's governing reference states physical manifest-entry deletion is out of scope/deferred; the shipped code and its own tests implement and prove exactly that — for the legacy `manifest/` compatibility copy (the authoritative `manifest_head/` entries are, consistent with the doc, kept occupied as reclaimed markers, never freed). This is probably not a real contradiction (the doc's "never freed" invariant is about `manifest_head/`), but the doc's own wording doesn't scope itself to "manifest_head only" and doesn't mention or authorize legacy-copy deletion at all — a reader relying solely on the doc would reasonably flag this as a live contradiction. | Update the design doc's §6.1 to explicitly state that legacy `manifest/` compatibility-copy deletion (distinct from the authoritative `manifest_head/` namespace) is in scope and already shipped. |
| NON-BLOCKING | Fence-entry copy has no floor check | `crates/pqueue-objectlog/src/segmented.rs:2135-2144` (fence copy, no floor comparison) vs. `:2150-2154` (data-segment branch, explicit floor skip) | Data-segment entries are explicitly skipped at/below the source floor during branch copy, but old epoch-fence entries are copied unconditionally with no equivalent floor check. Fence entries name no segment object, so this does not risk a GET-a-deleted-object violation, but it is an unexplained asymmetry in the filtering discipline. | Add a floor check (or a comment explaining the exemption) to the fence-copy branch for consistency. |
| NOTED-UNCERTAINTY | `expire_segments_through` trusts caller-established epoch/floor bound | `crates/pqueue-objectlog/src/segmented.rs:2377-2382` (no epoch param, no floor bound-check on `through_seq`) vs. `crates/pqueue-engine/src/compose.rs:1318-1372` (real caller always derives `through_seq` from a just-fenced `advance_retention_floor` result) | `expire_segments_through` performs no ownership/epoch check and does not itself validate `through_seq <= read_retention_floor(source)`; it relies entirely on the one production caller having already advanced the floor via the epoch-fenced CAS immediately prior. Any future direct caller that skips the floor-advance step first could physically reclaim segments the durable floor doesn't yet reflect. | Consider asserting `through_seq <= read_retention_floor(source)` inside `expire_segments_through` itself, or document the precondition prominently on the `pub fn`. |

### Disagreements Or Uncertainty

- Both BLOCKING findings fail *safe* (no silent corruption: F1 surfaces as an honest `Storage` error; F2's
  premise has no reachable production call site anywhere in this repo today — `gc_orphaned_branches` is not
  wired into `compose.rs` or any engine/maintenance path). A reviewer weighting "is this reachable today" more
  heavily than "is the claimed invariant actually established" might downgrade both to NON-BLOCKING. They were
  classified BLOCKING because this review's own property statement explicitly includes "orphan-GC guarantees"
  and "must never consult or GET a deleted source object" as guarantees to preserve, and the code's inline
  comments assert both races are closed as a matter of design.
- Could not fully verify whether `compose.rs`'s per-shard "UoW lock" also serializes `branch()` calls for the
  same shard within one process; if so, F1/F2 are strictly cross-process-only (which doesn't change the
  analysis — the code already frames both as cross-owner/cross-instance hazards).
- F3 is NOTED-UNCERTAINTY rather than a hard finding because a charitable reading of the design doc
  (distinguishing `manifest_head/` from legacy `manifest/`) resolves the apparent contradiction, but the doc's
  literal text does not make that distinction explicit.

### Evidence Map

- `docs/perf/design/manifest-compaction-hotpath.md:1-40, :330-407`
- `docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:200-229`
- `crates/pqueue-objectlog/src/segmented.rs:1033-1095, :1107-1141, :1211-1257, :1309-1441, :1443-1488,
  :1715-1727, :1747-1810, :1885-1927, :1939-2242, :2254-2368, :2377-2473, :2596-2669, :2671-2734`
- `crates/pqueue-engine/src/compose.rs:1290-1375`
- `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:2223-2373, :2375-2605, :2607-2710,
  :1384-1691, :3147-3294, :3296-3441`
- `crates/pqueue-objectlog/tests/object_log_segment_commit_tests.rs:496`
- repo-wide grep confirming `gc_orphaned_branches` has no production caller outside `segmented.rs` itself and
  its own tests.

---

## Disposition

This bead's scope is limited to running and recording the workspace-quality and review gates (AC1/AC2 of the
`pqueue-92a2e386` split); its explicit non-scope forbids "branch GC policy changes beyond what is required to
expose inherited floor metadata to branch creation" and forbids relaxing "existing branch atomicity, orphan
GC, source pin, retention floor, or fail-closed guarantees." No source changes are made in this bead. The two
BLOCKING findings above are real, pre-existing gaps in the shipped `branch_attempt` / `expire_segments_through`
/ `gc_orphaned_branches` interaction — recorded here as the AC requires ("run ... Codex adversarial review, or
record any unavailable gate as operator_required with reason" — satisfied by recording the substitute
independent review's full result) — and are left as follow-up work rather than fixed in this pass, consistent
with this repository's established handling of the same situation in
`.ddx/executions/20260713T223339-0d9d6cb4/objectlog-adversarial-review.md` (also a BLOCK verdict, recorded as
evidence without an in-bead code fix) and with this bead's own `follow-up`/`deferred` labels.

## Evidence Summary

| Gate | Command | Result |
|---|---|---|
| Objectlog | `rustup run 1.92.0 cargo test -p pqueue-objectlog` | 293 tests (1 ignored), 0 failed |
| Sqlite | `rustup run 1.92.0 cargo test -p pqueue-sqlite` | 320 tests, 0 failed |
| Engine | `rustup run 1.92.0 cargo test -p pqueue-engine` | 96 tests, 0 failed |
| Conformance | `rustup run 1.92.0 cargo test -p pqueue-conformance` | 111 tests (2 ignored doctests), 0 failed |
| Codex adversarial review | `codex exec ...` (multiple flag combinations) | `operator_required` — hangs indefinitely non-interactively in this sandboxed worktree despite confirmed network reachability; substituted with an independent sub-agent adversarial review per established repo precedent, full result recorded above (verdict: BLOCK, 2 blocking findings, follow-up work) |
