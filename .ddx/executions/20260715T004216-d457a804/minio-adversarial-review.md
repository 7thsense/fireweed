# Adversarial Review: MinIO Semantics vs Objectlog S3-Compatible Protocol

## Metadata

- **Bead:** `pqueue-1aab0a55`
- **Reviewer:** Codex (adversarial review agent)
- **Date:** 2026-07-14
- **Governing references:** TD-004 S3 Object-Log + SQLite Projection Mode, ADR-003 Rust Workspace and Toolchain Policy, dependency `pqueue-4157c36f`
- **Bundle:** `.ddx/executions/20260715T004216-d457a804/`

## Review Prompt / Context

This adversarial review evaluates the final implemented objectlog protocol against MinIO-specific S3-compatible semantics. The protocol is implemented in `crates/pqueue-objectlog/src/segmented.rs` as the `SegmentedObjectLog<S: BlobStore>` substrate and its `S3BlobStore` concrete adapter for S3-compatible endpoints.

The governing TD-004 specification requires:
- **TD-004:218** — conditional-write (CAS) as a required backend capability
- **TD-004:188** — manifest commit as the CAS/fencing enforcement point (step 4 of the group-commit pipeline)
- **TD-004:730-735** — provider-specific live S3 hardening scoped to deployment certification

The review covers:
1. S3-compatible conditional writes (CAS) via `put_if_absent`
2. Manifest CAS/fencing behavior under MinIO semantics
3. MinIO-specific divergence that could undermine append safety, manifest CAS/fencing, or local evidence claims

### Files reviewed

| File | Lines | Role |
|------|-------|------|
| `crates/pqueue-objectlog/src/segmented.rs` | 3272-3563 | `S3BlobStore` implementation — SigV4 signing, `put_if_absent` CAS, ListObjectsV2 pagination |
| `crates/pqueue-objectlog/src/segmented.rs` | 57-228 | `BlobStore` trait — the S3 seam |
| `crates/pqueue-objectlog/src/segmented.rs` | 1107-1121 | `commit_manifest_entry` — the manifest CAS |
| `crates/pqueue-objectlog/src/segmented.rs` | 1539-1577 | `acquire_epoch` — epoch fence publication to manifest |
| `crates/pqueue-objectlog/src/segmented.rs` | 1653-1786 | `seal` — epoch check + segment write + manifest CAS |
| `crates/pqueue-objectlog/src/segmented.rs` | 1348-1420 | `recover_manifest` — manifest recovery from S3 |
| `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` | 2765-2859 | Live MinIO integration test |
| `docs/perf/tp002-e3-objectlog-minio-release.md` | 1-74 | MinIO release evidence |
| `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md` | 214-241 | Object-Store Capability Requirements + Manifest Commit and Epoch Fencing |
| `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md` | 1-156 | Rust workspace and toolchain policies |

## Reviewer Findings

### Finding 1: CAS primitive — `If-None-Match: *` (create-only PUT) [NO-BLOCKER]

**Source:** `segmented.rs:3441-3453`

The `S3BlobStore::put_if_absent` uses `If-None-Match: *` to implement create-only conditional PUT:

```rust
fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
    let extra = vec![("If-None-Match".to_string(), "*".to_string())];
    let (status, resp) = self.request("PUT", &self.object_path(key), &[], body, &extra)?;
    match status {
        200 | 204 => Ok(true),
        409 | 412 => Ok(false),  // CAS lost
        _ => Err(...)
    }
}
```

**Assessment:** `If-None-Match: *` is the correct CAS primitive for both MinIO and AWS S3. MinIO returns HTTP 412 (Precondition Failed) when the object exists; AWS S3 returns 409 (Conflict). The implementation handles both status codes, so the CAS path is portable. The manifest CAS therefore works correctly on MinIO.

The existing live integration test `segmented_object_log_commits_through_minio` (`segmented_s3_substrate_tests.rs:2769`) exercises this path end-to-end: it writes a probe key, confirms `put_if_absent` returns `false` for the existing key (CAS collision), and then runs the full seal/manifest-commit/fence pipeline against MinIO.

### Finding 2: Manifest CAS fencing — epoch fence before segment write [NO-BLOCKER]

**Source:** `segmented.rs:1653-1786`

The seal path (step 4 of TD-004) checks the epoch BEFORE writing the segment object:

```rust
if expected_epoch != buf.committed_epoch {
    buf.buffered_bytes = 0;  // discard buffer
    return Err(EngineError::EpochFenced);
}
```

The segment is only written after the epoch check passes. The manifest CAS is then performed via `put_if_absent` at the next manifest index. A stale writer whose epoch is outdated is fenced before any I/O.

**Assessment:** The epoch-first-then-CAS ordering satisfies TD-004's safety invariant — a fenced writer creates no orphan segment and no dangling manifest entry. The manifest CAS at the next index prevents two writers at the same epoch from both extending the log (TD-004 manifest tail CAS rule). MinIO's create-only `If-None-Match: *` provides the necessary atomicity for this CAS.

### Finding 3: Epoch fence publication (acquire_epoch) [NO-BLOCKER]

**Source:** `segmented.rs:1539-1577`

`acquire_epoch` publishes a fence entry to the manifest head via create-only CAS (TD-004 implementation (b): epoch fence published to manifest before handoff):

```rust
let entry = ManifestEntry { index: next_index, epoch: new_epoch, fence: true, ... };
if self.commit_manifest_entry(shard, &entry, true)? {
    buf.committed_epoch = new_epoch;
    return Ok(new_epoch);
}
```

The committed fence at `manifest_head/{next_index:020}.json` is IMMUTABLE — no subsequent writer can overwrite it. A stale-epoch writer that tries to seal next will call `recover_manifest`, observe the higher epoch in the fence entry, and self-fence with `EpochFenced`.

**Assessment:** The bounded retry loop (16 attempts) handles concurrent acquirers. The create-only CAS guarantees linearizable fence publication. This is correct on MinIO — the fence entry survives any subsequent failure because manifest head entries are immutable once committed.

### Finding 4: Recovery manifest consistency with ListObjectsV2 on MinIO [NO-BLOCKER, DEFERRED TO CERTIFICATION]

**Source:** `segmented.rs:3479-3563` (list pagination), TD-004:730-735

Recovery lists manifest entries via `list_authoritative_manifest_keys_at`, which for S3 uses `ListObjectsV2`. The implementation correctly handles pagination via continuation tokens and `StartAfter` for ranged lists.

**Analysis:** MinIO single-node deployments provide strong read-after-write consistency for both GET and LIST operations, so recovery listing is consistent. However, distributed MinIO (erasure-coded mode) provides eventual consistency for LIST operations — a newly committed manifest entry might not appear in a subsequent ListObjectsV2 call.

**Assessment:** Single-node MinIO is strongly consistent, so this is not a blocker for the current integration. Per TD-004:730-735, provider-specific hardening against distributed/live S3 endpoints is a deployment certification activity, not a v1 blocker. The code's correct pagination handling (continuation tokens, `StartAfter`) makes it compatible with eventual-consistency semantics when the deployment certifies against it. Tagged for the deployment certification tracking issue.

### Finding 5: No retry/backoff on transient S3 errors [NO-BLOCKER, NOTE]

**Source:** `segmented.rs:3428-3463`

`S3BlobStore` opens a fresh TCP connection per request (`Connection: close`) and does not implement retry with exponential backoff for transient failures (e.g., HTTP 503 SlowDown, connection timeouts). If any S3 request fails at the transport or HTTP level, the error propagates to the caller immediately.

**Assessment:** This is acceptable for the current scope. The test/MinIO environment is local or container-bridged with negligible transient failures. Production deployments against a remote S3-compatible endpoint may encounter transient errors; adding retry logic with backoff would improve robustness. Tagged for deployment hardening.

### Finding 6: Segment object key collision safety [NO-BLOCKER]

**Source:** `segmented.rs:1722-1724`

Segment object keys include a process-ID and attempt counter suffix:
```
seg_attempt/e{epoch:020}/i{index:020}/s{first_seq:020}-{pid}-{attempt}.seg
```

This ensures idempotent segment writes at deterministic `first_seq`-based keys. The epoch+fence check before writing (Finding 2) prevents a stale writer from ever reaching the segment write, so cross-epoch collisions cannot occur. Same-epoch collisions are impossible because the index allocation is monotonic and lock-protected.

**Assessment:** No collision risk. The process-ID + counter suffix is defense-in-depth against intra-process collisions. MinIO's PUT handling is sufficient for idempotent writes.

### Finding 7: `LocalFsBlobStore` O_EXCL CAS — parity with S3 `If-None-Match: *` [NO-BLOCKER]

**Source:** `segmented.rs:399-412`

The local filesystem store uses `O_EXCL` (`create_new(true)`) for its `put_if_absent`, which is the kernel-level equivalent of S3's `If-None-Match: *`. The `InMemoryBlobStore` uses a mutex-guarded `BTreeMap::contains_key` check.

**Assessment:** All three `BlobStore` implementations provide equivalent CAS semantics. Unit/integration tests that pass on `InMemoryBlobStore` or `LocalFsBlobStore` exercise the same CAS contract that `S3BlobStore` exercises against MinIO. The MinIO integration test (`segmented_object_log_commits_through_minio`) proves the substrate produces the same behavior against a live MinIO endpoint.

### Finding 8: No explicit `If-Match` (ETag-based CAS) — append-only model suffices [NO-BLOCKER]

**Assessment:** The substrate uses create-only PUT (index-based) rather than ETag-based compare-and-swap (`If-Match`). This is the correct design for the append-only manifest model per TD-004 — the manifest is an immutable series of versioned objects, not a single mutable object. MinIO and S3 both support `If-None-Match: *`, and neither requires `If-Match` for the append-only path. An ETag-based CAS would be needed only if the manifest were a single mutable object (which TD-004 explicitly rejects in favor of append-only versioned keys).

## Conclusion

**No blockers found.** The objectlog protocol's S3-compatible assumptions are valid for MinIO (single-node deployment). Key findings:

| # | Area | Severity | Summary |
|---|------|----------|---------|
| 1 | CAS primitive | OK | `If-None-Match: *` works on MinIO; both 409/412 handled |
| 2 | Manifest CAS fencing | OK | Epoch checked before segment write; CAS is create-only at next index |
| 3 | Epoch fence publication | OK | Fence committed as create-only manifest entry; stale writers self-fence |
| 4 | List consistency (distributed mode) | DEFERRED | Single-node MinIO is strongly consistent; distributed eventual consistency is a deployment certification concern per TD-004:730-735 |
| 5 | Transient error handling | NOTE | No retry/backoff — acceptable for local deployment; consider for production |
| 6 | Segment key collisions | OK | Deterministic keys + epoch guard → no collision risk |
| 7 | CAS parity across stores | OK | O_EXCL, mutex-BTreeMap, and If-None-Match provide equivalent contract |
| 8 | No If-Match CAS needed | OK | Append-only model needs only create-only CAS, which is correct per TD-004 |

The existing live MinIO integration test (`segmented_object_log_commits_through_minio`), the TP-002 E3 release evidence (`docs/perf/tp002-e3-objectlog-minio-release.md`), and the full conformance suite (`segmented_s3_substrate_tests.rs`) provide comprehensive coverage of the MinIO integration.

All items deferred to deployment certification are tracked in TD-004:730-735 and do not block the current v1 profile.
