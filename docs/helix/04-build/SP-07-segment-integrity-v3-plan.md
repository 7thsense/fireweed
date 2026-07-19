---
ddx:
  id: build-sp07-segment-integrity-v3
  depends_on: [build-sp02-deterministic-storage-simulation, build-sp03-sequenced-metadata-boundary, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 8186965f01064cec092a73c0496971d809e1a8c3740d39acfd0c34bc9893be11
    deps:
      build-sp02-deterministic-storage-simulation: 822bd8ebd2a9e07bdec818a12eb2ea8c21a2feca965422830eae41a839a407c8
      build-sp03-sequenced-metadata-boundary: 6634c5fd29d1980929354abc206f44a274102462a3fb210f9a4842a8e985e280
      td-s3-object-log-sqlite-projection-mode: a88fb07f8275de066ab5f7a65f815e2da511774a164a20b464ebabf0a6e9d369
    reviewed_at: "2026-07-19T03:37:52Z"
---

# Implementation Plan: SP-07 Segment Integrity Version 3

## Scope

Introduce a new explicitly versioned segment/record format. Use Castagnoli CRC32C (`crc32c` crate,
`"123456789" -> 0xE3069283`) for fast corruption detection and reuse SHA-256 for cryptographic content identity. Continue reading
legacy v2/FNV segments and never rewrite committed objects in place.

V3 record payloads remain canonical JSON, matching the bytes v2 actually writes despite stale postcard comments.
Additive JSON fields retain serde compatibility; frame/version dispatch never relies on serde defaults. V3
frames carry length + CRC32C per record and a full-frame CRC32C trailer covering header and records. SHA-256
identity is over the complete stored object bytes including the CRC trailer, is stored in the manifest/key,
and is view-independent for branch-visible prefixes.

## Shared Constraints

- The manifest entry is dispatch authority. Legacy data entries without integrity metadata mean v2; v3 data
  entries carry `entry_kind: data`, `segment_format`, `frame_crc32c`, `content_sha256`, and algorithm IDs. Fence/floor/watermark
  entries are explicitly non-data and exempt. The segment self-version must match the manifest before decode.
  Serde defaults remain valid for additive JSON manifest/envelope fields, but never choose a frame version.
- CRC32C covers every segment byte except its own trailer value. Per-record frame CRC32C replaces TD-004's
  fictional envelope-checksum requirement; `CommandEnvelope.checksum` remains legacy/application metadata and
  is not the v3 integrity authority. SHA-256 unifies content identity with the existing content-addressed key
  digest, avoiding a second cryptographic hash.
- BlobStore GET has already materialized object bytes. Decode validates lengths/counts with hard bounds and
  preflights every record before secondary command-vector allocation for both v2 and v3, then checksum,
  structured payload, and contiguous positions. It cross-checks epoch, first sequence, record count/last
  sequence, format, and integrity metadata against the manifest for both versions. Typed errors identify stage
  plus manifest index and a queue-scoped opaque locator, never an object key.
- New writers emit v3 only after mixed-version recovery tests pass. Readers remain v2+v3 for the support window.

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 1 | Amend TD-004 and TP-003 with JSON framing, algorithms, canonical bytes, manifest authority/fields, per-record checksum disposition, AC mapping, rollout and evidence | format review and golden spec vectors |
| 2 | Correct stale postcard comments; isolate manifest-authoritative dispatch and bounded decoder while preserving v2 semantics | v2 goldens: cohorts, data, fence/floor/marker, shared-authority key |
| 3 | Implement v3 encoder/decoder and algorithm wrappers | standard CRC32C/SHA-256 vectors; bit-flip/truncation/length fuzz |
| 4 | Extend manifest/read/replay for arbitrarily interleaved v2/v3 logs and idempotent content identity | restart, snapshot tail, branch pin, GC, duplicate PUT tests |
| 5 | Ship runtime writer-format control with v2 default in release N; after soak, flip default in release N+1 | migration and deterministic crash matrix |
| 6 | Benchmark encode/decode/replay and object metadata/single-hash overhead before any default flip | roadmap performance bars; bounded allocations |

## Issue Decomposition

Decoder compatibility lands before the writer switch. Keep algorithm code behind narrow integrity types rather
than spreading digest choices through segment/manifest logic. Fuzzing may be a focused optional job with path
filters; registered format fuzz targets run in TP-003's per-PR tier without expanding the general matrix.
Map crash cuts to existing AC-TXN-4/AC-HYB-4 events and record command/environment/fixtures/results in the
TP-003 ledger. If SP-02 stops negatively, hand-written interleavings plus the process-kill fault harness cover
the same crash matrix.

This iteration excludes the file-based one-command-per-object reference log and snapshot checksum format.
FNV remains only in the v2 read path during the support window. New typed integrity variants replace
load-bearing string matching where applicable and feed SP-04 counters.

## Validation Plan

- [x] Golden v2 bytes decode to the exact historical command envelopes.
- [x] Golden v3 vectors are stable across architectures and releases.
- [x] Every single-bit mutation anywhere in the v3 object is detected by CRC32C/structure; identity mismatch fails closed.
- [x] Manifest/segment epoch, first-sequence, count/last-sequence, version, checksum, and identity mismatches fail closed for v2/v3 as applicable.
- [x] Arbitrarily interleaved v2/v3/data/fence/floor/watermark replay, retention, branch reads, and recovery preserve positions and results.
- [x] Oversized/truncated/malicious lengths fail before large allocation in both decoders.

## Release-N Performance Evidence

Deterministic accounting pins v3 storage overhead at four bytes per record plus one four-byte frame trailer
relative to v2; both formats retain the same 21-byte header and four-byte count. Admission charges the exact
format-specific retained-record plus temporary-frame peak before coordinator registration. The ignored manual
harness exercises identical 256-record input through both encoders and reports elapsed time without entering
ordinary CI:

```text
cargo test -p pqueue-objectlog segment_v2_v3_integrity_overhead_manual_benchmark -- --ignored --nocapture
```

Release N keeps v2 as the default. An interleaved same-run v2/v3 encode/decode/replay comparison under
representative load and soak evidence are required before the release-N+1 default flip, not before reader
compatibility lands. No extra GitHub Actions runner is introduced for this evidence.

## Risks and Rollbacks

The critical risk is unreadable durable history. Release N ships v2+v3 readers with the v2 writer default;
release N+1 may flip only after N soaks. After the first v3 commit, binary rollback below N is unsupported until
all v3 objects age out. Runtime writer rollback may emit v2 after v3, so arbitrary interleaving is mandatory.
Committed objects are never downgraded or rewritten in place.

`ManifestEntry.epoch` is logical authority while `segment_epoch` is the immutable physical header epoch.
New branch copies preserve `segment_epoch`; a narrow historical-v2 committed-branch exemption accepts an
absent field and uses the header epoch because committed manifests cannot be rewritten.

## Exit Criteria

Release N defaults writers to v2 while its supported readers recover v2 and v3 exactly. Integrity errors are
typed and observable, fuzz/golden/mixed-version suites pass, and measured encode/decode/replay evidence records
the overhead. Release N+1 may flip the default to v3 after soak and performance review; that flip is not an
exit condition for this release-N compatibility iteration.
