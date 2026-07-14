# pqueue-212f90fb manifest reclamation gates

## Owner-fence evaluation

The pqueue-c33c367e follow-up was evaluated against the manifest reclamation path. The conclusion remains unchanged:

- the permanent head CAS is still the stale-writer fence;
- the retained-floor replay path is the recovery source for deleted-prefix recovery;
- the current index-CAS protocol remains unsupported for delete-only compaction;
- a cheaper delete-only variant would need the post-head-CAS redesign, not the current code path.

This matches the existing code comments in `crates/pqueue-objectlog/src/segmented.rs` and the release note in `docs/releases/v0.14.0.md`.

## Workspace verification gates

Executed successfully in this worktree:

- `cargo test -p pqueue-objectlog TestObjectlogPqueueC33c367eReleaseNote -- --exact`
- `cargo test -p pqueue-conformance TestBehindImageFailClosedWithDeletedManifests -- --exact`
- `cargo test -p pqueue-conformance TestObjectlogDeletedManifestSourcePinRetentionFloor -- --exact`

## Evidence boundary

The first two verification attempts were initially launched with multiple cargo filters in a single invocation and rejected by cargo. The successful commands above are the accepted verification evidence for this bead.
