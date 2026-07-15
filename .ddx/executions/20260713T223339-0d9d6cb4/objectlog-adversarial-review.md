# Objectlog Adversarial Review

Bead: `pqueue-5209e6b5`
Bundle: `.ddx/executions/20260713T223339-0d9d6cb4`

## Review Inputs

- Governing design: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- Governing policy: `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`
- Review scope: branch pins, partial expiry, and deletion safety
- Independent reviewer: sub-agent `019f5d9e-bf7a-7773-a5ae-c9f3df6985dd`

## Review Result

Verdict: `BLOCK`

### Findings

| Severity | Area | Evidence | Finding | Recommendation |
|---|---|---|---|---|
| BLOCKING | expiry deletion | TD-004 `188-192` and `570-574` require segment deletion only after a committed snapshot covers the segment range and `log_recovery_window_ms` has elapsed. The surfaced branch-pin tests (`crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:602-705`, `:941-1068`, `:4439-4515`) prove live-pin and watermark behavior, but the delete path shown in `crates/pqueue-objectlog/src/segmented.rs:2322-2389` gates on `through_seq`, `now_ms`, and `branch_pins_segment` and does not evidence the snapshot/recovery-window predicate. | The evidence surface does not prove the required snapshot-coverage plus recovery-window gate for segment expiry. | Add an explicit test that a segment remains undeletable until a covering committed snapshot exists and `log_recovery_window_ms` has elapsed, and wire the delete frontier to that predicate. |
| BLOCKING | retention frontier | TD-004 `576-595` defines the hybrid-async deletion frontier as the minimum of snapshot coverage, manifest-tail lineage, request_id replay, client_item_key retention, and `sqlite_high_water` lag. The cited evidence surface covers branch-pin retention and contiguous-prefix watermark behavior (`crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:602-705`, `:941-1068`, `:4439-4515`) but does not show the other four frontier terms being enforced before deletion. | The retention safety story is incomplete for the full expiry frontier. | Add frontier-specific tests for request-id retention, client-item-key tombstones, manifest-tail lineage, and `sqlite_high_water` lag/health, then state which component owns the final frontier computation. |

### Disagreements Or Uncertainty

- Snapshot-gate ownership: it is possible the snapshot/recovery-window check lives in a higher-level retention planner not included in the cited code, but nothing in the reviewed delete path or tests proves that it is enforced before physical delete.
- Branch-pin TTL semantics: the tests show TTL release behavior, but TD-004 does not spell out whether TTL is an intentional lease-expiry policy or only an orphan-GC mechanism.

### Evidence Map

- Branch pins block expiry while live and release on discard or TTL expiry:
  - `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:602-705`
  - `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:941-1068`
  - `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:4439-4515`
- Retention-floor / watermark logic:
  - `crates/pqueue-objectlog/src/segmented.rs:2541-2810`
  - `crates/pqueue-objectlog/src/segmented.rs:2859-2895`
- Snapshot persistence exists, but the review did not find a proof that snapshot coverage is a required delete precondition:
  - `crates/pqueue-objectlog/src/segmented.rs:2911-2992`
  - `crates/pqueue-objectlog/tests/object_log_segment_commit_tests.rs:448-466`

## Gate Results

- `go test ./...`
  - Result: `not-applicable`
  - Evidence: the workspace has no `go.mod` or Go packages. The command exited with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`.
- `lefthook run pre-commit`
  - Result: `operator_required`
  - Evidence: the workspace reported `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "<worktree>"`.

## Transcript / Result

Independent reviewer result:

```json
{"verdict":"BLOCK","findings":[{"severity":"BLOCKING","area":"expiry deletion","evidence":"TD-004 lines 188-192 and 570-574 require deletion only after a committed snapshot covers the segment range and `log_recovery_window_ms` has elapsed; the surfaced branch-pin tests (`crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:602-705`, `:941-1068`, `:4439-4508`) only exercise pin/live-branch and watermark behavior, and the shown delete path (`crates/pqueue-objectlog/src/segmented.rs:2335-2372`) gates reclamation on `through_seq`, `now_ms`, and `branch_pins_segment` but does not evidence the snapshot/recovery-window predicate.","finding":"The evidence surface does not prove the required snapshot-coverage plus recovery-window gate for segment expiry. That leaves a real blocker because an implementation can satisfy every cited branch-pin test while still deleting data before a covering snapshot exists or before the recovery window has elapsed.","recommendation":"Add an explicit test that a segment remains undeletable until a covering committed snapshot exists and `log_recovery_window_ms` has elapsed, and wire the delete frontier to that predicate in the documented protocol."},{"severity":"BLOCKING","area":"retention frontier","evidence":"TD-004 lines 576-595 define the hybrid-async deletion frontier as the minimum of five inputs: snapshot coverage, manifest-tail lineage, request_id replay, client_item_key retention, and `sqlite_high_water` lag. The cited evidence surface only covers branch-pin retention and contiguous-prefix watermark behavior (`branch_pins_parent_segments_against_expiry`, `branch_pin_ttl_expiry_releases_manifest_reclamation`, `retention_floor_trim_respects_branch_pins_and_rejects_below_floor_cuts`, `TestManifestDeletionWatermarkContiguousPrefixOnly`, `TestBranchPinReleaseEnablesManifestReclaim`); it does not show the other four frontier terms being enforced before deletion.","finding":"The retention safety story is incomplete. A competent implementer could make a different, still-plausible choice for any of the missing frontier terms and still pass the surfaced branch-pin tests, which means the protocol is not yet pinned down enough to certify safe expiry deletion.","recommendation":"Add frontier-specific tests for request_id retention, client_item_key tombstones, manifest-tail lineage, and the `sqlite_high_water` lag/health cases, then state which component owns the final frontier computation."}],"disagreements_or_uncertainty":[{"topic":"snapshot-gate ownership","reason":"It is possible the snapshot/recovery-window check lives in a higher-level retention planner not included in the surfaced code, but nothing in the cited tests or the shown `expire_segments_through` path proves that the gate is enforced before physical delete."},{"topic":"branch-pin TTL semantics","reason":"The tests assert TTL expiry can release a live pin, but the TD-004 excerpt does not spell out whether TTL is an intentional lease-expiry policy or only an orphan-GC mechanism, so different implementers could treat a still-readable branch differently."}],"summary":"Branch-pin safety itself is well covered by the cited tests, including live-pin blocking, TTL release, rollback safety, and contiguous-prefix watermark behavior. The blocker is that the evidence surface does not prove the full expiry contract in TD-004: snapshot coverage plus recovery-window delay is not exercised, and the hybrid-async deletion frontier is only partially covered. As written, a reviewer cannot rule out premature deletion under a different but still reasonable implementation of the missing frontier terms."}
```

