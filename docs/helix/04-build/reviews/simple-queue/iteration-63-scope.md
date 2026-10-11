# Iteration 63: full-address framing and authoritative-log recovery prerequisite

Base: 46d8c418. Shared FWQADDR1 framing consists of eight magic bytes, then
little-endian u64 length and exact UTF-8 bytes for tenant and queue. The borrowed
whole parser rejects unknown version, invalid identifiers/UTF-8, incomplete or
overflowing lengths and trailing bytes. Identifiers retain the existing policy
without normalization or new size limits. Canonical transport is fwq7- followed
by unpadded URL-safe Base64 of the full frame. Binary and transport literals are
independently pinned. Callers pre-admit peak raw framing plus Base64 output on
encode, and decoded bytes plus canonical re-encoding scratch on decode.

The real local object-log fixture uses a separate temporary root and an opaque
transport bridge. Interleaved delimiter/slash/Unicode addresses have distinct
partitions, epochs, high-waters and emission cursors. After dropping original
owners, cold reopen verifies exact initial/tail/extra payloads and positions,
reading from genesis through actual empty EOF with one-entry pages. A first
5 MiB payload exceeds the adapter's 4 MiB fetch budget. Pinned object-log 0.3.6
already includes the first batch regardless of budget; no dependency change.

Legacy partition encoding and metadata roots are unchanged. This is not an
API-007 projector, constructor, migration or S3 durability qualification.
Existing ManifestSequencer filename encoding and metadata key expansion still
limit full transport names physically. Before activation, implement compact
collision-checked physical indirection with full original-address verification
for every authority metadata/log identity, and checked old-address migration.
The long-address codec test proves framing only; it does not qualify those
addresses in the current physical log layout. No identifier cap is introduced.
