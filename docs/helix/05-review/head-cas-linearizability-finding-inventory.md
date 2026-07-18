# Head CAS Linearizability — Finding Inventory

**Inventory ID:** `pqueue-1b1fb5ec-inventory`
**Source transcript:** `docs/helix/05-review/head-cas-linearizability-review.md`
**Last updated:** 2026-07-15
**Status:** CLASSIFIED

## Scope statement

This inventory covers only head CAS linearizability findings tied to TD-004
manifest commit CAS/fencing and conditional-write requirements (TD-004:188,
TD-004:218). Provider-specific AWS S3 certification (TD-004:730–735) is
explicitly excluded.

## Inventory entries

### F1: Current-epoch validation against control plane absent from seal()

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F1` |
| **Severity** | BLOCKING |
| **Disposition** | discharged by SP-03 PendingFence linearization evidence |
| **Evidence** | CP `PendingFence` is non-serving. The non-skipping `pending_fence_gap_has_one_safe_old_prefix_then_fences_stale_retry` and live Postgres/S3 `pending_fence_gap_linearizes_old_commit_before_storage_fence_then_rejects_stale_retry` pause after the CP reservation but before storage fencing: old routing is unavailable; one already-admitted old-epoch prefix may linearize while no new-epoch operation can serve; after storage fence+reset-and-replay hydration+confirm, its stale retry is rejected and the new owner recovers the prefix. `hcas_f1_f2_crash_after_fence_head_reopens_fenced_and_keeps_prefix_invisible` covers crash after head publication. |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F1 |
| **Area** | Epoch fencing: current-epoch validation |
| **Governing spec** | TD-004:235–237 (Manifest Commit and Epoch Fencing) |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:1662` |
| **Context** | `seal()` checks `expected_epoch == buf.committed_epoch` against the manifest-recorded epoch, not the current control-plane epoch. TD-004:235 requires "the writer's `assignment_epoch` equals the epoch currently authoritative in the control plane." The code relies on option (b) — epoch fence published to manifest before handoff via `acquire_epoch()` — but does not independently validate against the control plane. If a fence entry is lost (crash during `acquire_epoch` after CAS success but before local epoch update), a stale writer could present `expected_epoch` matching the stale `buf.committed_epoch` while the control plane has already advanced. |
| **Proposed disposition** | Discharged for safety: PendingFence is reservation, not serving handoff. The storage fence is the linearization boundary between old-epoch and new-epoch writes; no new-epoch response precedes it. |

### F2: Versioned head stale after fence entry commit

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F2` |
| **Severity** | BLOCKING |
| **Disposition** | resolved by SP-03 slice 0 |
| **Evidence** | Authoritative data, floor, and fence publication share the versioned authority-head CAS; immutable candidates become visible only when named by that head. Exact-key reread resolves ambiguous creates. Non-empty legacy queues fail closed rather than being adopted concurrently. The HCAS-F1/F2 crash test proves reopened head visibility. |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F2 |
| **Area** | Manifest entry CAS: reader-visible atomicity |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs` — `acquire_epoch()` at 1506, `commit_manifest_entry()` at 1096, `read_manifest_head()` at 134, `update_manifest_head_if_version()` at 163 |
| **Context** | `commit_manifest_entry()` writes a per-index manifest entry via `put_if_absent` at `manifest_head/{index:020}.json`. The versioned manifest head (`ManifestHeadBlob`, updated by `update_manifest_head_if_version()`) is a separate object. `acquire_epoch()` commits a fence entry via `commit_manifest_entry()` but does NOT appear to call `update_manifest_head_if_version()`, leaving the versioned head stale relative to the latest fence entry. Readers using `read_manifest_head()` observe an index lower than the highest committed fence entry. Recovery uses `recover_manifest()` (scans all `manifest_head/` objects) and is unaffected. |
| **Proposed disposition** | Closed on the migration surface: the authority head is the single publication point; legacy compatibility remains a separate explicit protocol and cannot be concurrently migrated. |

### F3: Non-atomic create_new on FUSE/overlay in LocalFsBlobStore

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F3` |
| **Severity** | WARNING |
| **Disposition** | non-blocking |
| **Evidence** | `docs/helix/05-review/head-cas-linearizability-review.md:54` §Findings row F3 — review transcript documents `create_new(true)` non-atomicity on FUSE/overlay; `crates/pqueue-objectlog/src/segmented.rs:60-65` — `BlobStore::put_if_absent` trait contract; `LocalFsBlobStore` impl — `OpenOptions::new().create_new(true)` used for local-fs CAS |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F3 |
| **Area** | BlobStore CAS contract portability |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:60–65` (BlobStore::put_if_absent), `LocalFsBlobStore` impl (local-fs atomic create via `OpenOptions::new().create_new(true)`) |
| **Context** | `BlobStore::put_if_absent` returns `Ok(true)` on create, `Ok(false)` on collision. The local-fs implementation uses `create_new(true)` which is NOT atomic on FUSE, NFS re-export, or overlay filesystems (common in containerized CI/dev). A false `Ok(true)` from a non-atomic local-fs `create_new` would let a stale writer win a CAS it should have lost. Default test suite runs on `LocalFsBlobStore` and may produce false passes on non-atomic filesystems. |
| **Proposed disposition** | Document atomicity assumptions of `create_new(true)`. Add CI check that `put_if_absent` tests on `InMemoryBlobStore` run alongside local-fs tests. |

### F4: Watermark staleness during stale-writer session bypasses early fence

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F4` |
| **Severity** | WARNING |
| **Disposition** | non-blocking |
| **Evidence** | `docs/helix/05-review/head-cas-linearizability-review.md:55` §Findings row F4 documents watermark staleness bypassing the early reclaim fence. `stale_writer_below_horizon_is_fenced_by_retained_head_address` and the shared stale-writer substrate fixture prove the exact footprint: stale seal returns `EpochFenced`, retained head remains occupied, freeable legacy mirror remains absent, no manifest entry becomes visible, and at most one unreachable segment object is left. |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F4 |
| **Area** | Reclaim-time fence caching |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:1668–1684` (reclaim-time fence in seal()) |
| **Context** | `seal()` checks its in-memory `manifest_deletion_watermark` and self-fences if the target index was already known reclaimed. A concurrent trim can advance marker authority after that cache was loaded. The stale seal may therefore write one content-addressed segment before its create-only publication loses at the retained manifest-head/authority address. The segment is unreachable, the legacy compatibility mirror remains freeable, and the operation returns `EpochFenced`; this is bounded correctness-preserving waste, not a data-safety hole. Avoiding it would add an object-store read/LIST to the successful steady-state seal path. |
| **Proposed disposition** | Either refresh the watermark periodically during a long-lived writer session, or add telemetry for orphan segment writes to detect the stale-watermark pattern in production. |

### F5: Postgres-manifest-pointer fallback epoch-atomicity untested at BlobStore trait level

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F5` |
| **Severity** | NOTE |
| **Disposition** | non-blocking |
| **Evidence** | `docs/helix/05-review/head-cas-linearizability-review.md:56` §Findings row F5 — review transcript documents untested Postgres-manifest-pointer fallback epoch-atomicity boundary; `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:220` — Postgres-manifest-pointer fallback normative rule requiring epoch-atomic CAS in control plane |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F5 |
| **Area** | Postgres-manifest-pointer fallback |
| **Governing spec** | TD-004:220 (Postgres-manifest-pointer fallback) |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs` — `commit_manifest_entry()` has no visible fallback path routing to Postgres |
| **Context** | The Postgres-manifest-pointer fallback uses a transactional CAS in the control plane. If implemented at the `BlobStore` level, `put_if_absent` must carry `assignment_epoch` through to the row-level CAS. No test in the reviewed set exercises a mock control-plane CAS via the `BlobStore` trait, so the fallback's epoch-atomicity boundary is untested at the unit level. |
| **Proposed disposition** | Add a unit-level `BlobStore` test that validates the fallback's epoch-atomicity boundary yields the same linearizability guarantees as the object-store CAS. |

### F6: acquire_epoch retry budget exhaustion under sustained CAS collision

| Field | Value |
|-------|-------|
| **ID** | `HCAS-F6` |
| **Severity** | NOTE |
| **Disposition** | non-blocking |
| **Evidence** | `docs/helix/05-review/head-cas-linearizability-review.md:57` §Findings row F6 — review transcript documents unbounded retry budget pathology; `crates/pqueue-objectlog/src/segmented.rs:1506-1551` — `acquire_epoch()` bounded retry loop with 16-retry budget |
| **Source** | `docs/helix/05-review/head-cas-linearizability-review.md` §Findings, row F6 |
| **Area** | Epoch acquisition retry policy |
| **Code location** | `crates/pqueue-objectlog/src/segmented.rs:1506–1551` (acquire_epoch bounded retry loop) |
| **Context** | `acquire_epoch()` retries up to 16 times on CAS collision, each retry re-reading the manifest tail. A sustained collision storm (multiple concurrent reassignments, tight acquire loop) could exhaust the budget and leave a queue without an epoch holder. The bounded retry does not propagate backpressure to the caller. A queue stuck without an epoch holder cannot serve any mutating operations until operator intervention or a retry from the orchestration layer. |
| **Proposed disposition** | Document the retry budget in the `acquire_epoch` contract. Consider an exponential-backoff retry with a configurable cap, and surface retry-exhaustion as a metrics event. |

## Index

| Entry ID | Severity | Status | Disposition |
|----------|----------|--------|-------------|
| HCAS-F1 | BLOCKING | DISCHARGED | SP-03 PendingFence linearization |
| HCAS-F2 | BLOCKING | RESOLVED | SP-03 slice 0 |
| HCAS-F3 | WARNING | CLASSIFIED | non-blocking |
| HCAS-F4 | WARNING | CLASSIFIED | non-blocking |
| HCAS-F5 | NOTE | CLASSIFIED | non-blocking |
| HCAS-F6 | NOTE | CLASSIFIED | non-blocking |

## Evidence mapping

Each entry's source field points to the persisted transcript at
`docs/helix/05-review/head-cas-linearizability-review.md` with the specific
§Findings table row that contains the full context. No entry in this inventory
references provider-specific AWS S3 certification, deployment hardening, or
other out-of-scope topics (per TD-004:730–735 scope boundary).
