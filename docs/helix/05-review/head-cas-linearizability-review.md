# Head CAS Linearizability — Adversarial Review Transcript

**Review ID:** `pqueue-1b1fb5ec` · **Date:** 2026-07-15
**Governing Spec:** TD-004 S3 Object-Log + SQLite Projection Mode
**Governing ADR:** ADR-003 Rust Workspace and Toolchain Policy
**Dependency:** `pqueue-4157c36f`

## Target

Head CAS (compare-and-set) linearizability in the segmented object-log substrate:
manifest commit as the CAS/fencing enforcement point
(TD-004:188), documented conditional-write primitives (TD-004:218),
and the provider-agnostic contract boundary (TD-004:730).

## Scope

- Manifest entry CAS via `put_if_absent` (create-only conditional PUT).
- Epoch fencing: current-epoch validation, fence entry publication, stale-writer rejection.
- BlobStore CAS trait contract and implementations (`S3BlobStore`, `InMemoryBlobStore`, `LocalFsBlobStore`).
- Reclaim-time fence and retention-floor CAS.
- Postgres-manifest-pointer fallback CAS.
- Versioned manifest head CAS.

**Explicitly out of scope:** provider-specific AWS S3 certification (TD-004:730–735),
end-to-end hybrid/async projection semantics, performance/latency measurement,
and non-CAS durability paths (segment object writes, snapshot writes).

## Evidence Reviewed

- `crates/fireweed-objectlog/src/segmented.rs` — `BlobStore` trait, `seal()`,
  `acquire_epoch()`, `commit_manifest_entry()`, `recover_manifest()`,
  `advance_retention_floor()`, `ManifestEntry`, `ManifestHeadBlob`,
  `VersionedHead`, `update_manifest_head_if_version()`.
- `crates/fireweed-objectlog/src/lib.rs` — local-filesystem epoch CAS
  (`with_epoch_lock`, `write_epoch_object`).
- `crates/fireweed-objectlog/src/compose_log.rs` — `append`, `acquire_epoch`,
  `advance_retention_floor` passthroughs.
- `crates/fireweed-objectlog/tests/object_log_segment_commit_tests.rs` — CAS/fencing
  concurrency tests.
- `crates/fireweed-objectlog/tests/segmented_s3_substrate_tests.rs` — stale-epoch
  CAS tests, `assert_manifest_head_cas_contract`.
- `crates/fireweed-objectlog/tests/composed_group_commit.rs` — hybrid force-seal
  and stale-epoch fencing.
- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
  — lines 188, 218, 730, §§"Manifest Commit and Epoch Fencing" and
  "Object-Store Capability Requirements".

## Findings

| # | Severity | Area | Finding |
|---|----------|------|---------|
| F1 | BLOCKING | Epoch fencing: current-epoch validation | `seal()` at `segmented.rs:1662` checks `expected_epoch == buf.committed_epoch` against the manifest-recorded epoch, not the **current control-plane epoch** (Postgres `assignment_epoch`). TD-004:235–237 requires manifest commit to validate against the control-plane epoch, not merely the manifest-recorded epoch. The code relies on option (b) — epoch fence published to manifest before handoff via `acquire_epoch()` — but does not independently validate against the control plane. If a fence entry is lost (e.g., crash during `acquire_epoch` after CAS success but before the fence entry is read back by all nodes), or if a stale writer's lease clock skew allows it to present `expected_epoch` matching the stale `buf.committed_epoch` while the control plane has already advanced, the old-epoch writer's CAS succeeds. The stale-writer tests (`segment_manifest_cas_fences_concurrent_writers`) cover the two-thread race case but not the **lost-fence-entry-after-crash** scenario: a crash in `acquire_epoch` between `commit_manifest_entry` returning `Ok(true)` and the local `committed_epoch` update. After reopen, the fence entry exists in the manifest (readable by recovery) but the crashed writer's in-memory state is gone; a concurrent writer that never observed the fence entry could still race at the old epoch. |
| F2 | WARNING | Manifest entry CAS: no reader-visible atomicity between per-index write and versioned head | `commit_manifest_entry()` writes to `manifest_head/{index:020}.json` via `put_if_absent`. The versioned manifest head (`ManifestHeadBlob`, updated by `update_manifest_head_if_version()`) is a *separate* object. A reader using `read_manifest_head()` (which reads the versioned head) can observe a head index lower than the highest committed manifest entry if the versioned head update races with or is skipped after the per-index write succeeds. `acquire_epoch()` calls `commit_manifest_entry()` (which writes a per-index fence entry) but does NOT appear to call `update_manifest_head_if_version()`, leaving the versioned head stale relative to the latest fence entry. Recovery uses `recover_manifest()` which scans all `manifest_head/` objects and is not affected. Downstream readers that depend on `read_manifest_head()` for fencing decisions could observe a stale head after a fence entry commit. |
| F3 | WARNING | BlobStore CAS contracts: `put_if_absent` error semantics differ across impls | `BlobStore::put_if_absent` returns `Result<bool>` where `Ok(true)` means created and `Ok(false)` means already exists. The S3 implementation (`S3BlobStore:3415`) maps HTTP 409/412 to `Ok(false)`. The in-memory implementation (`InMemoryBlobStore`) uses a `Mutex<BTreeMap>` compare-and-swap returning `Ok(false)` on collision. The local-fs implementation (`LocalFsBlobStore`) uses `OpenOptions::new().create_new(true)` which returns `Ok(false)` on `EEXIST`. However, the local-fs implementation does not document that `create_new(true)` is NOT atomic on FUSE, NFS re-export, or overlay filesystems (common in containerized dev/test). A false `Ok(true)` from a non-atomic local-fs `create_new` would let a stale writer win a CAS it should have lost. While `LocalFsBlobStore` is test-only, the test suite that validates CAS linearizability (`object_log_segment_commit_tests`) runs on `LocalFsBlobStore` by default and may produce false passes on non-atomic filesystems. |
| F4 | WARNING | Reclaim-time fence relies on undelatable object addresses | `seal()` at `segmented.rs:1668–1684` checks a cached `manifest_deletion_watermark` and self-fences if the cached index was reclaimed. The mitigation is that the `manifest_head/{index}.json` address is **never freed** (retained as zero-byte marker). This is a correct invariant per `mark_manifest_entry_reclaimed()` at `segmented.rs:1122`. However, the watermark is cached from `recover_manifest()` at open time and is **never refreshed** during a long-lived writer session. If a concurrent writer (same epoch, transient split-brain due to network partition healing) advances the watermark during this session, the stale writer's cached watermark remains below the true horizon, and the recover-time fence check at lines 1670–1676 passes, wrongly permitting a seal attempt at a reclaimed index. The subsequent per-index `put_if_absent` at the reclaimed index would still fail (the address is retained), so the CAS ultimately rejects the stale write — but the epoch check was bypassed, and the writer wasted a segment write that becomes an orphan. Documented behavior prevents address reuse, so this is a correctness-preserving waste, not a data-safety hole. |
| F5 | NOTE | Postgres-manifest-pointer fallback epoch validation gap | The Postgres-manifest-pointer fallback (TD-004:220) uses a transactional CAS in the control plane. `commit_manifest_entry()` does not appear to have a fallback path that routes to Postgres — the fallback appears to be a deployment-level configuration that selects a different `BlobStore` implementation or wraps the control-plane CAS. If the fallback is implemented at the `BlobStore` level, the `put_if_absent` contract must carry the `assignment_epoch` through to the row-level CAS. No test in the reviewed set exercises a mock control-plane CAS via the `BlobStore` trait, so the fallback's epoch-atomicity boundary is untested at the unit level. |
| F6 | NOTE | `acquire_epoch` retry budget is unbounded in pathological S3 latency | `acquire_epoch()` at `segmented.rs:1506–1551` retries up to 16 times on CAS collision. Each retry re-reads the manifest tail. A sustained collision storm (e.g., multiple concurrent reassignments or a tight acquire loop) could exhaust the budget and leave the queue without an epoch holder. The bounded retry does not propagate backpressure to the caller. |

## Verdict: REQUEST_CHANGES

### BLOCKING findings to resolve

1. **F1 — Current-epoch validation against control plane** (TD-004:235). The epoch-on-commit
   guard against control-plane advancement without a prior fence entry is not independently
   validated. Resolve by either: (a) adding a control-plane epoch read + compare inside
   `seal()` before the manifest CAS (option (a) per TD-004:236), or (b) proving in the
   test suite that the acquire_epoch + fence-entry protocol guarantees that no window
   exists where a control-plane epoch advance can escape the manifest before a fence entry
   commits — and that a crash during `acquire_epoch()` between CAS and in-memory epoch
   update leaves the fence entry readable by all subsequent opens (test: crash-at-fence-entry-gap).

2. **F2 — Versioned head stale after fence entry** may confuse readers that depend on
   `read_manifest_head()` for fencing decisions. Either ensure `acquire_epoch()` also
   calls `update_manifest_head_if_version()` after the fence entry commit, or document
   that `read_manifest_head()` may return a head lower than the latest fence entry and
   that callers must use `recover_manifest()` for authoritative state.

3. **F3 — Non-atomic create_new on FUSE/overlay in LocalFsBlobStore.** Document the
   atomicity assumptions of `create_new(true)` and add a CI check that `put_if_absent`
   tests on `InMemoryBlobStore` (which is fully atomic) run in addition to the local-fs
   tests, so a false pass on a non-atomic filesystem cannot hide a CAS regression.

## Summary

The head CAS linearizability implementation uses `put_if_absent` at deterministic
monotonic keys as its core linearization point — a well-understood and correct pattern.
Every manifest entry, epoch fence, and retention-floor advance commits through the same
CAS path, and the address-retention invariant preserves the collision property across
reclamation cycles. The six findings identify two BLOCKING gaps (control-plane epoch
validation independence and versioned-head stale-read after fence), two WARNING gaps
(divergent CAS atomicity across blob-store impls and watermark staleness during a
writer session), and two NOTE observations (fallback test coverage and retry budget
pathology). Findings F1–F3 must be resolved before this review can be called complete;
the architecture is sound and the OPEN items are narrow, targeted gaps.
