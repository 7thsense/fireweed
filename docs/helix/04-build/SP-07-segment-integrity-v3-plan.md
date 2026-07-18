---
ddx:
  id: build-sp07-segment-integrity-v3
  depends_on: [build-sp02-deterministic-storage-simulation, build-sp03-sequenced-metadata-boundary, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: df04e0cb83b702b1bca3fc1ee1b6446cb9176b7741ec633e4b0002f11be4efb9
    deps:
      build-sp02-deterministic-storage-simulation: a7e7545464a051bd23046ffbb1b0f04fece7c450eb071e593a82084a08ed66ff
      build-sp03-sequenced-metadata-boundary: c212bb092c036690b331e446a3b53ee8d5d5ae47eb6237524d038b6e7fdb53db
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
    reviewed_at: "2026-07-18T16:20:32Z"
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
  entries carry `segment_format`, `frame_crc32c`, `content_sha256`, and algorithm IDs. Fence/floor/watermark
  entries are explicitly non-data and exempt. The segment self-version must match the manifest before decode.
  Serde defaults remain valid for additive JSON manifest/envelope fields, but never choose a frame version.
- CRC32C covers every segment byte except its own trailer value. Per-record frame CRC32C replaces TD-004's
  fictional envelope-checksum requirement; `CommandEnvelope.checksum` remains legacy/application metadata and
  is not the v3 integrity authority. SHA-256 unifies content identity with the existing content-addressed key
  digest, avoiding a second cryptographic hash.
- Decode validates lengths/counts with hard bounds before allocation for both v2 and v3, then checksum,
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

- [ ] Golden v2 bytes decode to the exact historical command envelopes.
- [ ] Golden v3 vectors are stable across architectures and releases.
- [ ] Every single-bit mutation anywhere in the v3 object is detected by CRC32C/structure; identity mismatch fails closed.
- [ ] Manifest/segment epoch, first-sequence, count/last-sequence, version, checksum, and identity mismatches fail closed for v2/v3 as applicable.
- [ ] Arbitrarily interleaved v2/v3/data/fence/floor/watermark replay, retention, branch reads, and recovery preserve positions and results.
- [ ] Oversized/truncated/malicious lengths fail before large allocation in both decoders.

## Risks and Rollbacks

The critical risk is unreadable durable history. Release N ships v2+v3 readers with the v2 writer default;
release N+1 may flip only after N soaks. After the first v3 commit, binary rollback below N is unsupported until
all v3 objects age out. Runtime writer rollback may emit v2 after v3, so arbitrary interleaving is mandatory.
Committed objects are never downgraded or rewritten in place.

## Exit Criteria

The default writer emits v3, supported readers recover v2 and v3 exactly, integrity errors are typed and
observable, fuzz/golden/mixed-version suites pass, and performance stays within the stated bars.
