# Owner-Fence Impact

Bead: `pqueue-96686a97`

## Verdict

`pqueue-c33c367e` owner-fence wiring does not change the watermark design.

The governing doc now states that the current index-CAS protocol must keep below-floor manifest
addresses occupied, so the permanent head remains the stale-writer fence and `compacted_through_index`
remains a read-cost helper rather than an ownership fence.

## Governing References

- `docs/perf/design/manifest-compaction-hotpath.md:370`
- `docs/perf/design/manifest-compaction-hotpath.md:374`

## Gate Results

- `go test ./...` was run from the repository root and failed as not applicable because there is no
  Go module or package layout at this root.
- `lefthook run pre-commit` was run and reported that no lefthook config files were present in this
  repository root, so the gate is an operator-required missing-tooling/config failure.
