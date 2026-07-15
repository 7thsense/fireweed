# Stale-Writer Adversarial Review

Bead: `pqueue-fcb6a893`

Review question: can a stale writer extend the object-log durable log, observe an ack after reassignment, or survive the fence/recovery path without being rejected?

## Sources Reviewed

- [TD-004 S3 Object-Log + SQLite Projection Mode](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L184)
- [TD-004 S3 Object-Log + SQLite Projection Mode](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L223)
- [TD-004 S3 Object-Log + SQLite Projection Mode](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L730)
- [crates/pqueue-objectlog/src/segmented.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/src/segmented.rs#L1081)
- [crates/pqueue-objectlog/src/segmented.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/src/segmented.rs#L1591)
- [crates/pqueue-objectlog/src/segmented.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/src/segmented.rs#L1640)
- [crates/pqueue-objectlog/src/segmented.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/src/segmented.rs#L2618)
- [crates/pqueue-objectlog/tests/durability.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/tests/durability.rs#L123)
- [crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs#L850)
- [crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs#L1213)

## Checks Run

- `cargo test -p pqueue-objectlog --test durability local_object_log_rejects_stale_expected_epoch_before_append -- --nocapture`
- `cargo test -p pqueue-objectlog --test object_log_commit_recovery_tests TestManifestDeletionWatermarkPersistsAndRecoversMetadata -- --nocapture`
- `cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests retention_floor_advance_is_epoch_fenced_manifest_cas_against_a_superseded_owner -- --nocapture`

All three checks passed.

## Finding Map

| Classification | Finding | Evidence | Disposition |
|---|---|---|---|
| Blocking | None identified | TD-004 requires current-epoch rejection on manifest commit and fenced-writer rollback; the implementation and tests match that contract. | None |
| Non-blocking | None identified | The stale-writer guard is enforced before segment write in `seal`, and CAS-loss recovery fences on `observed_epoch > expected_epoch`. | None |
| Duplicate | None identified | No duplicate stale-writer defect surfaced during the review. | None |
| Out of scope | Provider-specific live S3 hardening is deferred to deployment certification, not this review. | [TD-004](/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fcb6a893-20260714T144305-f5e28d3f/docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L730) | Out of scope |

## Verdict

No stale-writer blocking defects found.

The protocol-level fence is supported by:

- the manifest-CAS helper and ack boundary in `crates/pqueue-objectlog/src/segmented.rs:1081-1094` and `:1640-1667`
- the pre-segment stale-epoch rejection path in `crates/pqueue-objectlog/src/segmented.rs:1591-1620`
- the durable-horizon readback used for reclamation and recovery in `crates/pqueue-objectlog/src/segmented.rs:2618-2626`
- the stale-epoch append rejection test in `crates/pqueue-objectlog/tests/durability.rs:123-139`
- the persisted-fence recovery test in `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs:850-886`
- the superseded-owner trim rejection test in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:1213-1245`
