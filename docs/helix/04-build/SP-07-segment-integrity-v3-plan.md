---
ddx:
  id: build-sp07-segment-integrity-v3
  depends_on: [build-sp02-deterministic-storage-simulation, build-sp03-sequenced-metadata-boundary, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
---

# SP-07: Current Segment Integrity Format

Fireweed has one pre-release object-log format. There is no format negotiation, rollout selector, fallback
decoder, or in-place adoption path.

Each segment uses the `FWSG` header, current version byte, physical segment epoch, first sequence, record
count, and repeated `length + CRC32C + canonical JSON` records. A frame CRC32C covers the header and records;
SHA-256 covers the complete stored object including that trailer. The manifest is the authority for the
version, algorithms, epoch, sequence range, CRC, digest, and canonical content-addressed object key.

Current data entries require `entry_kind: data`, `segment_epoch`, the current `segment_format`,
`frame_crc32c`, `content_sha256`, and the exact algorithm identifiers. Control entries contain no segment
integrity fields. Unknown fields, missing fields, unknown versions, non-current algorithms, identity
mismatches, and retired durable namespaces fail closed with typed corruption.

Validation gates:

- literal current frame and manifest goldens;
- standard CRC32C and SHA-256 vectors;
- every single-bit mutation, truncation, hostile length, count, epoch, sequence, and key mismatch fails closed;
- replay, restart, branching, retention, and recovery-index paging preserve exact positions;
- retired frame and manifest fixtures are rejection tests only;
- no public or environment format-selection API exists.
