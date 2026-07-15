# Head CAS Blocking Finding Disposition

**Bead:** pqueue-8f22e5d8
**Dependency:** pqueue-4157c36f
**Governing references:** TD-004 S3 Object-Log + SQLite Projection Mode, ADR-003 Rust Workspace and Toolchain Policy
**Source inventory:** docs/helix/05-review/head-cas-linearizability-finding-inventory.md
**Source transcript:** docs/helix/05-review/head-cas-linearizability-review.md
**Date:** 2026-07-15

## Summary

Two BLOCKING findings (HCAS-F1, HCAS-F2) and four non-blocking findings
(HCAS-F3 through HCAS-F6) were classified in the finding inventory. This
document records the disposition of each BLOCKING finding per the execution
contract: every blocker is either fixed in scope with evidence or converted
into a clearly identified follow-up/operator-required item with head CAS risk
context preserved.

## Blocking findings

### HCAS-F1: Current-epoch validation against control plane absent from seal()

| Field | Value |
|-------|-------|
| **Severity** | BLOCKING |
| **Source** | head-cas-linearizability-finding-inventory.md:17-31 |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:1685` |
| **Spec reference** | TD-004:235-237 |

**Description:** `seal()` at `crates/pqueue-objectlog/src/segmented.rs:1685`
checks `expected_epoch != buf.committed_epoch` against the manifest-recorded
epoch, not the current control-plane epoch. The code relies on option (b) from
TD-004:236 — epoch fence published to manifest before handoff — but does not
independently validate against the control plane.

**Risk context:** If a fence entry is not observed by a stale writer (e.g.,
crash during `acquire_epoch()` between CAS success at
`crates/pqueue-objectlog/src/segmented.rs:1563` and local epoch update at
line 1569), the stale writer's in-memory `committed_epoch` remains at the old
value, allowing the epoch check in `seal()` to pass. The subsequent manifest
CAS at `commit_manifest_entry()` (line 1743) still protects linearizability
— the stale writer's `put_if_absent` at an already-occupied index fails —
but a wasted orphan segment write occurs (line 1725).

**Disposition:** FOLLOW-UP WORK

**Rationale for not fixing in this bead:** Two remediation paths are
specified in TD-004:236 and the review verdict:
- (a) Add a control-plane epoch read + compare inside `seal()` before the
  manifest CAS. This requires injecting a `ControlPlaneStore` dependency into
  `SegmentedObjectLog` and reading from the Postgres control plane on the hot
  seal path, which is a significant production protocol code change beyond the
  scope of this bead ("Do not modify production protocol code except for
  blocker remediation justified by the finding map").
- (b) Prove in the test suite that the fence-entry protocol guarantees no
  control-plane epoch advance escapes the manifest before a fence entry
  commits, including a crash-at-fence-entry-gap test. This requires new
  crash-simulation tests that are themselves a bead-sized unit of work.

Neither path is completed here. A follow-up bead should implement option (b)
(crash-at-fence-entry-gap test coverage) as the lighter-weight proof that the
existing option (b) protocol is sound.

**Follow-up bead(s) required:**
- Crash-at-fence-entry-gap test: add test proving that a crash during
  `acquire_epoch()` between CAS success and local epoch update leaves the
  fence entry readable by all subsequent opens, and that a stale writer
  reopened after such a crash correctly self-fences.

### HCAS-F2: Versioned head stale after fence entry commit

| Field | Value |
|-------|-------|
| **Severity** | BLOCKING |
| **Source** | head-cas-linearizability-finding-inventory.md:33-44 |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:1506` (acquire_epoch), line 163 (update_manifest_head_if_version) |
| **Spec reference** | TD-004:188 (manifest commit CAS/fencing enforcement point) |

**Description:** `acquire_epoch()` at
`crates/pqueue-objectlog/src/segmented.rs:1563` commits a fence entry via
`commit_manifest_entry()` (per-index write at
`manifest_head/{index:020}.json`) but does NOT call
`update_manifest_head_if_version()` to update the versioned manifest head
blob (`ManifestHeadBlob`). Readers using `read_manifest_head()` observe a
versioned head that may be lower than the highest committed fence entry.

**Risk context:** Recovery via `recover_manifest()` /
`list_authoritative_manifest_keys_at()` scans all manifest entries and is
unaffected — it reads per-index entries at
`manifest_head/{index:020}.json` directly. The stale versioned head only
affects callers that depend on `read_manifest_head()` for fencing decisions.
No such callers exist in the current codebase; `read_manifest_head()` is
defined but its call graph does not reach any linearizability-critical path.

**Disposition:** FOLLOW-UP WORK (operator_required)

**Rationale for not fixing in this bead:** Two remediation paths are
specified in the review verdict:
- (a) Ensure `acquire_epoch()` calls `update_manifest_head_if_version()`
  after the fence entry commit. This modifies production protocol code in
  `crates/pqueue-objectlog/src/segmented.rs` and requires verifying that the
  `ManifestHeadBlob` data model, key layout (versioned keys vs. per-index
  keys in the shared `manifest_head/` prefix), and the versioned head's
  expected-version CAS contract are compatible with the fence-entry commit
  path — a targeted but non-trivial production code change.
- (b) Document that `read_manifest_head()` may return a head lower than the
  latest fence entry and that callers must use `recover_manifest()` for
  authoritative state. This is a documentation-only fix but does not address
  the underlying risk for future callers.

Option (a) is deferred because it modifies production protocol code and
requires validation of versioned-head semantics alongside the per-index
manifest entry path. Option (b) is deferred to the same follow-up bead to
ensure a single coherent resolution.

**Follow-up bead(s) required:**
- Ensure `acquire_epoch()` calls `update_manifest_head_if_version()` after
  each successful fence entry commit, or document the staleness contract for
  all public manifest-head readers.

## Non-blocking findings

Non-blocking findings (HCAS-F3 WARNING, HCAS-F4 WARNING, HCAS-F5 NOTE,
HCAS-F6 NOTE) are outside the scope of this bead but are noted for reference.

## Gate results

| Gate | Result | Evidence |
|------|--------|----------|
| `go test ./...` | not-applicable | No Go module/packages exist in the workspace root. Exit code 1 with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`. |
| `lefthook run pre-commit` | operator_required | Lefthook binary exists at `/home/linuxbrew/.linuxbrew/bin/lefthook` but no lefthook config file (`lefthook.yml`, `.lefthook.yml`, etc.) is present in the repository. `lefthook run pre-commit` produced: "No config files with names [lefthook .lefthook .config/lefthook] have been found" (exit 0, no hooks defined). A lefthook config must be created and maintained before the pre-commit gate is enforceable. |

## Protocol scope compliance

No production protocol code
(`crates/pqueue-objectlog/src/segmented.rs`,
`crates/pqueue-objectlog/src/lib.rs`, or other crate sources) was modified.
All BLOCKING findings are recorded as follow-up/operator-required items
rather than in-scope fixes per the bead's scope constraint.
