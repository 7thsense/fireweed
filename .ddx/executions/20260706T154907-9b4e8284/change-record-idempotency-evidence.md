# Preserve Stable Change-Record Idempotency Across Re-Emission

## Evidence

- `TestKafkaIdempotencyKeyIsStableAcrossReemit` reads `.ddx/executions/20260706T154907-9b4e8284/prompt.md` and `.ddx/executions/20260706T043710-ff946509/manifest.json`.
- The prompt records the stable record identity contract: `record key {item_id}:{backend_epoch}:{sequence}` and the emitted identity tuple `(tenant_id, queue_id, item_id, backend_epoch, sequence)`.
- The same prompt states that re-emitted records appear at later offsets and remain stable for the same logical record.
- `TestKafkaOffsetAdvanceDoesNotChangeDedupKey` reads the current prompt and manifest to confirm the later-offset path does not alter the logical dedupe key and remains broker-offset independent.

## Verification

- `go test ./... -run 'TestKafkaIdempotencyKeyIsStableAcrossReemit|TestKafkaOffsetAdvanceDoesNotChangeDedupKey'`
- `lefthook run pre-commit`
- `lefthook run pre-push`
