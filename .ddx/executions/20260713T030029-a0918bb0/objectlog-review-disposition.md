# Objectlog Review Disposition

Bead: `pqueue-03cd5412`

## Blocking finding

The review blocker is the delete-only compaction path. The design note records that freeing a below-floor manifest index is unsafe because a stale writer can later win `put_if_absent` at that reused address and false-ack a phantom entry. Relevant evidence:

- [`docs/perf/design/manifest-compaction-hotpath.md:210`](docs/perf/design/manifest-compaction-hotpath.md#L210)
- [`docs/perf/design/manifest-compaction-hotpath.md:365-373`](docs/perf/design/manifest-compaction-hotpath.md#L365)
- [`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:188`](docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L188)
- [`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:218`](docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L218)
- [`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:570`](docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L570)
- [`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:730`](docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md#L730)

## Disposition

This bead does not change protocol code. The blocker is mapped to the durable follow-up chain that preserves the parent dependency `pqueue-4157c36f` and the governing references:

- `pqueue-4157c36f`
- `pqueue-ddfbccc8`
- `pqueue-9eb4d830`

Those beads hold the head-CAS redesign and bounded-object-count follow-up work that the review note says must precede any delete-only protocol change.

## Gates

- `go test ./...`
  - Result: not applicable at the repo root. The workspace has no root Go module/packages.
  - Output: `pattern ./...: directory prefix . does not contain main module or its selected dependencies`
- `lefthook run pre-commit`
  - Result: operator_required / missing local config.
  - Output: `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-03cd5412-20260713T030029-a0918bb0"`
- `bash scripts/ci/pr-gate.sh --mode enforcing`
  - Result: operator_required fallback; command could not complete locally.
  - Last failing command/output:
    - `cargo +1.92.0 fmt --all --check`
    - `error: no such command: \`+1.92.0\``
    - `help: invoke \`cargo\` through \`rustup\` to handle \`+toolchain\` directives`

## Notes

The design note recommends `RANGE-LIST + watermark` as the safe read-cost fix and explicitly states that full object-count bounding is a separate, larger redesign. This report records the blocker disposition only.
