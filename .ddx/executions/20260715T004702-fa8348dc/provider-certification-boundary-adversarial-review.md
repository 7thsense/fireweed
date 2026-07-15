# Adversarial Review: Provider-Certification Boundary — Local S3-Compatible Evidence vs AWS S3 Certification Claims

**Bead:** `pqueue-67f9aa56`
**Review type:** Codex adversarial review
**Reviewed:** 2026-07-14
**Reviewer:** automated Codex analysis session
**Governing references:** TD-004 S3 Object-Log + SQLite Projection Mode, ADR-003 Rust Workspace and Toolchain Policy, dependency `pqueue-4157c36f`
**Bundle:** `.ddx/executions/20260715T004702-fa8348dc/`

---

## Review Prompt / Context

Conduct an adversarial review of the final implemented objectlog protocol (pqueue v0.8.x workspace) focused on the **provider-certification boundary** — whether the repository evidence, MinIO/local gates, protocol documentation, and code correctly:

1. **Define the boundary between local S3-compatible evidence** (MinIO, `LocalFsBlobStore`, `InMemoryBlobStore` tests, and the `BlobStore` trait) and **provider-specific AWS S3 certification claims** (which per TD-004:730-735 remain a deployment certification activity).
2. **Avoid incorrectly claiming provider-specific AWS S3 certification** through documentation, release evidence, test labels, code comments, or README claims.
3. **Honestly characterize what the existing test and evidence trail proves** — MinIO single-node semantics, not AWS S3 production semantics.
4. **Document the gaps** between MinIO/local gate coverage and what an AWS S3 deployment certification would require.

### Governing boundary (TD-004:730-735)

> Provider-specific hardening against a live cloud S3 endpoint remains a deployment certification activity unless a future bead adds a concrete S3 adapter and credentials-backed acceptance run. That future activity must not be cited as a blocker for the current v1 profile unless the release claims provider-specific S3 support rather than S3-compatible semantics through the freestanding object-log.

### Source documents reviewed

| Document | Lines | Role |
|----------|-------|------|
| `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md` | 856 | Governing specification |
| `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md` | 156 | Workspace and toolchain policy |
| `crates/pqueue-objectlog/src/segmented.rs` | 5121 | Core segmented object-log implementation |
| `crates/pqueue-objectlog/src/lib.rs` | 3254 | LocalObjectLog implementation |
| `crates/pqueue-objectlog/src/compose_log.rs` | 395 | Object-log composition backend |
| `docs/perf/tp002-e3-objectlog-minio-release.md` | 74 | MinIO release evidence |
| `docs/perf/tp002-e3-cost-model.md` | 107 | Cost model using AWS S3 pricing |
| `docs/perf/design/manifest-compaction-hotpath.md` | 380 | Manifest compaction performance design |
| `docs/perf/tp002-hybrid-async-gates.md` | 100 | Hybrid async release gates |
| `docs/perf/tp002-objectlog-hybrid-evidence.md` | 140 | Hybrid object-log evidence |

### Test artifacts reviewed

| File | Lines | Role |
|------|-------|------|
| `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` | 4300 | S3 substrate tests including MinIO integration |
| `crates/pqueue-objectlog/tests/object_log_segment_commit_tests.rs` | 778 | Segment commit unit tests |
| `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs` | 1110 | Commit and recovery tests |
| `crates/pqueue-objectlog/tests/composed_group_commit.rs` | 407 | Group commit composition tests |
| `crates/pqueue/tests/product_validation_tests.rs` | ~3200 | Product-level validation |
| `crates/pqueue-server/tests/performance_object_log_e3_live_tests.rs` | ~500 | Live E3 performance tests |

---

## Review Findings

### Finding 1: [NO-BLOCKER] `S3BlobStore` is a hand-rolled HTTP client — not production-grade for AWS S3

**Location:** `segmented.rs:3270-3564`

The `S3BlobStore` is documented as "dependency-light" and is a hand-rolled SigV4-signing HTTP/1.1 client over `TcpStream`. Key production gaps:

| Gap | Detail | Code evidence |
|-----|--------|---------------|
| **No TLS** | Only supports `http://` endpoints; rejects `https://` at construction | `segmented.rs:3296`: `strip_prefix("http://")` |
| **No connection pooling** | Every request opens a fresh `TcpStream` with `Connection: close` | `segmented.rs:3409` |
| **No retry/backoff** | Transient failures (503, connection reset) propagate immediately | Full request path: `segmented.rs:3409-3414` |
| **No DNS load balancing** | Single `host:port` target, no round-robin or failover | `S3BlobStore` fields `host`, `port` |
| **No timeouts** | `TcpStream::connect` and `read_to_end` have no explicit timeout | `segmented.rs:3409-3414` |

**Assessment:** The implementation is fit for purpose — testing against MinIO in a local/container environment. The `segmented.rs:3270-3274` doc-comment explicitly states: "Targets MinIO / any S3-compatible store over `http://host:port`". This is **not** a claim of AWS S3 production readiness. The doc-comment honestly describes the scope.

**Boundary status:** Correctly bounded. No claim of AWS S3 production suitability.

---

### Finding 2: [NO-BLOCKER] TD-004:730-735 certification boundary is clearly stated but not consistently referenced

**Location:** `TD-004:730-735`

The governing specification states:

> Provider-specific hardening against a live cloud S3 endpoint remains a deployment certification activity unless a future bead adds a concrete S3 adapter and credentials-backed acceptance run.

This is a **single occurrence** in the 856-line document. The phrase is unambiguous but has limited surface area. Other references in TD-004 discuss "S3-compatible object store" (used 7 times throughout) without consistently noting that the compatibility claim is limited to MinIO/local testing:

- Line 40: "S3-compatible object store" (scope definition)
- Line 138: "S3-compatible object log with group-commit sealed segments" (LogStore definition)
- Line 142: "S3-compatible object storage holding SQLite snapshots" (SnapshotStore definition)
- Line 637: "that produces the S3 cost floor" (cost tradeoff section)

**Risk:** A reader unfamiliar with the certification boundary might read "S3-compatible" and assume certified AWS S3 support. The term "compatible" is standard industry terminology for MinIO/etc, but without the certification caveat being visible alongside each usage, an unwary reader could over-interpret.

**Boundary status:** Acceptable. The single authoritative boundary statement at line 730-735 is clear. The non-scope section (line 93-110) also clarifies that operator repair, migration, and backend-migration APIs are out of scope. A reasonable implementer or deployer reading the full document would encounter the caveat. However, the boundary would benefit from a forward-reference from the scope section (line 40) to the certification limitation (line 730).

---

### Finding 3: [NO-BLOCKER] MinIO release evidence accurately characterizes scope

**Location:** `docs/perf/tp002-e3-objectlog-minio-release.md`

The E3 release evidence document:
- Title: "TP-002 E3 — live object_log_sqlite_projection over S3 (MinIO) RELEASE evidence"
- States "Endpoint: MinIO" explicitly
- Documents the exact MinIO version (`minio/minio server /data`)
- Reports only MinIO-measured metrics (segments sealed, ack latency, recovery time)

**Assessment:** The evidence document correctly labels the endpoint as MinIO and does not claim AWS S3 testing. The metrics reported (objects PUT, LIST pagination) are implementation-dependent and would differ for AWS S3 (different network latency, different pricing). The cost model (`tp002-e3-cost-model.md`) explicitly notes the distinction at line 104: "The COUNTS (objects/command, segments/command) are storage-implementation-independent; only the prices assume AWS S3."

**Boundary status:** Correctly bounded.

---

### Finding 4: [NO-BLOCKER] MinIO integration test is env-gated and accurately named

**Location:** `segmented_s3_substrate_tests.rs:2765-2849`

The live MinIO test `segmented_object_log_commits_through_minio`:
- Is gated on `PQUEUE_S3_TEST_ENDPOINT` environment variable
- Loudly SKIPS with a message telling the user to set the variable
- Documents MinIO setup steps in the module-level doc comment
- The queue ID includes `minio-` prefix for namespace isolation

**Assessment:** The test is correctly scoped, env-gated, and labeled. No claim of AWS S3 testing. The SKIP message could be improved to clarify that the test targets MinIO, not AWS S3 — but the test name itself is explicit ("commits_through_minio").

**Boundary status:** Correctly bounded.

---

### Finding 5: [NO-BLOCKER] `BlobStore` trait is S3-seam-compatible but has no AWS S3-specific implementation

**Location:** `segmented.rs:57-228`

The `BlobStore` trait defines exactly the primitives S3 needs and nothing more:
- `put` — unconditional PUT
- `put_if_absent` — conditional create-only PUT (CAS)
- `get` — GET
- `delete` — DELETE
- `list` / `list_from` — LIST with pagination

**Assessment:** The trait is sufficient to implement a production-grade AWS S3 adapter (using `hyper`/`reqwest`, TLS, connection pooling, retry). The existing `S3BlobStore` is a valid reference implementation that exercises the API surface against MinIO. A deployment certification for AWS S3 would need to implement an `AwsS3BlobStore` with proper TLS, timeouts, connection pooling, and retry.

**Boundary status:** Correctly designed — the seam supports future AWS S3 adaptation without changing the substrate.

---

### Finding 6: [NO-BLOCKER] Manifest compaction performance design references "modern S3" without qualification to MinIO vs AWS

**Location:** `docs/perf/design/manifest-compaction-hotpath.md:305-307`

> Modern S3 is strongly read-after-write and strongly list-consistent, so a correctly written recovery can skip below-horizon enumeration with confidence the server is not hiding a truncated stale tail.

**Assessment:** This statement is cited as a design rationale for the durable read-horizon watermark, not as a testing claim. The design document correctly identifies that "eventual-consistency-only S3-compatible stores would violate the tail-visible requirement" (line 307-310). This is a **correct design assumption** for AWS S3 (which provides strong read-after-write consistency for PUT of new objects since December 2020). MinIO single-node also provides strong consistency. The note about eventual-consistency stores is a correct caveat.

**Boundary status:** Acceptable. The design assumption is consistent with both AWS S3 and single-node MinIO semantics.

---

### Finding 7: [NO-BLOCKER] `S3BlobStore` listing uses `StartAfter` — compatible with modern S3

**Location:** `segmented.rs:3479-3563`

The `list_from_with_request_count` method uses `StartAfter` (ListObjectsV2), which is the natively bounded range-list primitive. This works correctly on:
- AWS S3 (strongly consistent since Dec 2020)
- MinIO single-node (strongly consistent)
- MinIO distributed (eventually consistent for LIST)

**Assessment:** The implementation is compatible with AWS S3 semantics. The `StartAfter` parameter is an S3 API feature, so the code path that exercises it would work against real S3 without modification. Only the underlying transport (TLS, connection management, retry) would need upgrading.

**Boundary status:** Compatible — the logical S3 API usage is correct for both MinIO and AWS S3.

---

### Finding 8: [NO-BLOCKER] Cost model uses AWS S3 pricing but correctly attributes source

**Location:** `docs/perf/tp002-e3-cost-model.md:47-104`

The cost model:
- Uses "AWS S3 pricing (AmazonS3 offer file pub. 2026-05-28)" as the price source
- Labels the entire analysis as "object_log_sqlite_projection vs postgres_native"
- Includes a MinIO-vs-real-S3 note (line 104): "The E3 counts were measured against MinIO; the prices are real-S3 US-East-1. The COUNTS (objects/command, segments/command) are storage-implementation-independent; only the prices assume AWS S3."

**Assessment:** The note at line 104 is accurate and explicit. The counts (objects per command, segments per seal) are a function of the group-commit algorithm and `SegmentConfig`, not the storage endpoint. A reader seeing "S3 pricing" would reasonably infer AWS S3, and the note clarifies this is pricing only — the operational characteristics were MinIO-measured.

**Boundary status:** Correctly bounded. The note is present, but could be more prominently placed near the top of the document (it is currently near the bottom, in a "Sensitivity" subsection).

---

### Finding 9: [NO-BLOCKER] `S3BlobStore` CAS uses `If-None-Match: *` — portable between MinIO and AWS S3

**Location:** `segmented.rs:3441-3454`

The `put_if_absent` implementation uses `If-None-Match: *` and handles both `409` (Conflict, AWS S3) and `412` (Precondition Failed, MinIO) responses.

**Assessment:** The CAS primitive is portable between MinIO and AWS S3. The dual response code handling is evidence that the developer considered both endpoints. This is the one code path that explicitly accounts for AWS S3 behavior.

**Boundary status:** Correctly scoped — works on both MinIO and AWS S3.

---

### Finding 10: [NO-BLOCKER] No `README.md`, release notes, or crate-level docs claim AWS S3 certification

**Location:** Workspace root `README.md`, `crates/pqueue-objectlog/Cargo.toml`, crate doc comments

I reviewed the workspace README (`README.md`) and `pqueue-objectlog` crate documentation for any claims of AWS S3 support. The README describes the project as a "high-performance queue engine" and does not mention S3, MinIO, or cloud provider specifics in any certification sense. The `pqueue-objectlog` crate-level docs (`segmented.rs:1-32`) describe it as "Segmented S3 object-log group-commit substrate" and "S3-compatible object store."

**Assessment:** No claims of AWS S3 certification exist in marketing or documentation materials.

**Boundary status:** Correctly bounded.

---

### Finding 11: [NO-BLOCKER] `Connection: close` and `http://` limitation is explicitly documented

**Location:** `segmented.rs:3285-3296`, `segmented.rs:3333-3335`, `segmented.rs:3407`

The `S3BlobStore` constructor rejects non-`http://` endpoints, and the request method adds `Connection: close`. These are documented:
- Line 3285: "`endpoint` is `http://host:port` (the orbstack container IP form)"
- Line 3333: "Sign and send one HTTP/1.1 request over a fresh `Connection: close` TCP stream"

**Assessment:** The limitations are honestly documented. No reader would mistake this for an AWS S3 production client. An operator deploying against AWS S3 would need to build (or reuse) a proper S3 adapter with TLS.

**Boundary status:** Correctly bounded. The transport limitations are visible and documented.

---

### Finding 12: [NOTE] Cost model MinIO-vs-S3 note would be more visible near the document top

**Location:** `docs/perf/tp002-e3-cost-model.md:104`

The note "MinIO vs real S3" is at line 104, in the "Sensitivity" section near the end of the document. A reader who skims the title and cost figures without reaching the sensitivity section could incorrectly assume all measurements were taken against AWS S3.

**Assessment:** This is a documentation clarity concern, not a blocker. The note is present and accurate. Moving it to a prominent position (e.g., before the results table) would reduce the risk of misinterpretation.

**Suggested follow-up:** Reorder the cost model so the "MinIO vs real S3" caveat appears before or alongside the cost summary, not only in the sensitivity section.

---

### Finding 13: [NO-BLOCKER] No `request_id` or `client_item_key` expiry semantics depend on provider-specific S3 behavior

**Location:** `TD-004:568-601`, `segmented.rs:5079-5121`

The retention and expiry rules (TD-004 §Retention and Expiry) are defined in terms of logical command sequences, not storage-provider-specific behaviors. The expiry frontier computation uses only the committed snapshot, manifest tail, request-id retention, item-key retention, and SQLite lag — all of which are implementation-internal metrics, not S3 API features.

**Assessment:** The retention/expiry model is provider-agnostic. No part of the protocol semantics depends on provider-specific S3 behavior.

**Boundary status:** Provider-agnostic by design.

---

### Finding 14: [NO-BLOCKER] `S3BlobStore` has no IAM or STS integration

**Location:** `segmented.rs:3275-3426`

The `S3BlobStore` uses static `access_key` + `secret_key` credentials with AWS SigV4 signing. There is no support for:
- IAM instance profiles
- STS temporary credentials
- AssumeRole
- Credential rotation
- VPC endpoints

**Assessment:** This is consistent with the MinIO/test-only scope. An AWS S3 production deployment would need one or more of these mechanisms, but the `BlobStore` trait does not require them — they are transport concerns that a production `S3BlobStore` implementation would handle.

**Boundary status:** Correctly out of scope for the current profile.

---

### Finding 15: [NO-BLOCKER] Test environment variables use `PQUEUE_S3_*` prefix — neutral naming

**Location:** `segmented_s3_substrate_tests.rs:2770-2782`

The env variables `PQUEUE_S3_TEST_ENDPOINT`, `PQUEUE_S3_TEST_BUCKET`, `PQUEUE_S3_TEST_ACCESS_KEY`, `PQUEUE_S3_TEST_SECRET_KEY` use the generic "S3" prefix rather than "MINIO". This is neutral — the same variables could be used for an AWS S3 integration test in the future. The test documentation (module-level doc comment) clarifies the MinIO scope.

**Assessment:** Acceptable naming. The `S3` prefix is standard for S3-compatible endpoints and does not claim AWS certification.

**Boundary status:** Correctly neutral.

---

## Summary of Findings

| # | Finding | Severity | Boundary status |
|---|---------|----------|-----------------|
| 1 | `S3BlobStore` is hand-rolled, no TLS, no retry | No-blocker | Correctly bounded — doc-comment explicitly targets MinIO |
| 2 | TD-004:730-735 certification boundary is clear but single-occurrence | No-blocker | Acceptable — unambiguous, but could benefit from forward-reference from scope section |
| 3 | MinIO release evidence accurately characterizes scope | No-blocker | Correctly bounded — endpoint explicitly labeled as MinIO |
| 4 | MinIO integration test is env-gated and accurately named | No-blocker | Correctly bounded |
| 5 | `BlobStore` trait is sufficient for future AWS S3 adapter | No-blocker | Correctly designed — seam supports future adaptation |
| 6 | Manifest compaction design references "modern S3" correctly | No-blocker | Consistent with both AWS S3 and MinIO semantics |
| 7 | `StartAfter` LIST usage is compatible with both MinIO and AWS S3 | No-blocker | Portable |
| 8 | Cost model uses AWS S3 pricing with MinIO-vs-S3 note | No-blocker | Correctly bounded; note could be more prominent |
| 9 | `If-None-Match: *` CAS handles both 409 (AWS) and 412 (MinIO) | No-blocker | Portable — explicit AWS S3 compatibility |
| 10 | No README/docs claim AWS S3 certification | No-blocker | Clean marketing/documentation boundary |
| 11 | `Connection: close` and `http://` limitation is explicitly documented | No-blocker | Correctly bounded |
| 12 | Cost model MinIO-vs-S3 note position (near end) | NOTE | Documentation clarity concern; suggest moving earlier |
| 13 | Retention/expiry model is provider-agnostic | No-blocker | Independent of S3 API specifics |
| 14 | No IAM/STS credential support | No-blocker | Consistently out of scope for v1 profile |
| 15 | Env var naming is S3-generic, test docs clarify MinIO scope | No-blocker | Correctly neutral naming |

## Blocker Conclusion

**No blockers found.** The repository evidence, MinIO/local gates, and protocol documentation correctly avoid claiming provider-specific AWS S3 certification. The boundary between what is tested/proven locally (MinIO single-node) and what would require deployment certification (AWS S3 production) is honestly documented in:

1. **TD-004:730-735** — explicit boundary statement limiting provider-specific hardening to deployment certification
2. **`segmented.rs:3270-3274`** — `S3BlobStore` doc-comment targets MinIO
3. **`segmented.rs:3285-3296`** — endpoint format documented as `http://`
4. **`segmented_s3_substrate_tests.rs` module doc** — MinIO setup instructions and env-gating
5. **`docs/perf/tp002-e3-objectlog-minio-release.md:1`** — title explicitly names MinIO
6. **`docs/perf/tp002-e3-cost-model.md:104`** — MinIO-vs-S3 measurement note

### What the existing evidence proves

| Layer | Proven by | Scope |
|-------|-----------|-------|
| `InMemoryBlobStore` CAS | Unit tests in `segmented_s3_substrate_tests.rs` | No network, mutex-guarded |
| `LocalFsBlobStore` CAS | Unit tests in `segmented_s3_substrate_tests.rs` | Filesystem O_EXCL |
| `S3BlobStore` CAS against MinIO | `segmented_object_log_commits_through_minio` | Single-node MinIO, `http://`, no TLS |
| Manifest fencing protocol | Full conformance suite + MinIO integration | Epoch fence + create-only CAS |
| Group-commit pipeline at scale | TP-002 E3 release evidence (MinIO, 10M items) | MinIO-specific latency/cost |
| Recovery from object-log tail | E3 recovery measurement (MinIO, 5.09s for 10M) | MinIO-specific performance |
| Cost model | `pqueue-release` cost calculator | MinIO counts, AWS S3 pricing |

### What deployment certification would need to test against AWS S3

1. **Transport layer**: TLS termination, connection pooling, keep-alive, retry with backoff, timeouts, DNS resolution, VPC endpoints
2. **IAM/STS**: Credential management, temporary credentials, IAM policies, instance profiles
3. **Consistency characterization**: Verify read-after-write, list-after-write consistency under AWS S3's stated consistency model
4. **Performance profile**: Measure ack latency, recovery time, and LIST pagination costs with AWS S3's network characteristics
5. **Error handling**: Map AWS S3-specific error codes (SlowDown, RequestTimeout, InternalError) to retry behavior
6. **Request cost verification**: Validate the cost model against actual AWS S3 billing

All of these are deployment certification activities per TD-004:730-735 and are correctly excluded from the current v1 profile claims.

## Review Context Metadata

- **Review tool:** Manual Codex adversarial review session
- **Governing docs:** TD-004 (f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0), ADR-003 (7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb)
- **Prior beads:** pqueue-4157c36f (epoch-fencing bead), pqueue-8928baec (durable read-horizon bead)
- **Scope:** Provider-certification boundary between local S3-compatible evidence and AWS S3 certification
- **Non-scope:** Rust release matrix beyond local gates, production protocol code changes, actual AWS S3 certification
