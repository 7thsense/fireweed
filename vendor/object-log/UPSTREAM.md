# Pinned upstream source

Source: https://github.com/7thsense/object-log, tag v0.3.1,
commit dcd37c0e7de3afa26e671245faf41ee513c984ec.

Cargo.toml, README, licenses, src and tests originate from that commit.
Fireweed patches this package locally to evaluate combining already
completed ordered data uploads into one durable manifest publication. No
storage-format migration or relaxed durability barrier is intended.

The sibling object-log checkout and Cargo's source cache are not modified.
Local implementation changes must be recorded below and covered by ordering,
error, durability and recovery tests before workflow measurements.

## Local changes

- Remove the unused `proptest` development dependency. Check in the standalone
  test lockfile so `--locked` tests reproduce the refreshed dependency graph.

- Retain the earliest failed PUT/manifest enqueue position for cumulative
  flush barriers. Barriers before that position can succeed; covering barriers
  return the original failure even if later appends succeed. Producer error
  handling, offset assignment and on-disk formats are unchanged.
- Fireweed depends directly on this vendored path. Cargo workspace patches do
  not propagate to dependent workspaces; the benchmark and public-consumer
  fixtures must resolve the same log implementation as the product. Standalone
  log tests use a checked-in standalone lockfile and a separate target directory.

- Opted-in built-in sequencers can commit a bounded contiguous ready success
  prefix of uploaded objects in one manifest. The configured maximum in-flight
  upload count bounds each group; no extra linger is introduced. Unfinished or
  failed PUTs stop a group. Custom sequencers default to one-object commits.
- Group members retain their object locations, responder durability levels,
  enqueue ordering and byte accounting. Fallback media-op accounting counts
  each data object; debug PUT duration is the largest member observation.
- Tests cover grouping bounds, failure boundaries, multiple partitions,
  acknowledgements while manifest publication is gated, manifest failure,
  replayed exact locations, and concurrent public produces followed by reopen.

- With concurrent uploads enabled, one blocking commit job owns the ordered
  ready group while the dispatcher continues admitting and polling uploads.
  Aggregate byte admission remains charged through commit completion; shutdown
  drains uploads and the committer. A panicked committer closes admission and
  rejects outstanding barriers. Single-flight configurations retain inline commit.
- A gated regression failed before this change because a blocked first manifest
  prevented three later data uploads. It now passes without early sequenced
  acknowledgement. Additional tests cover release of admission bytes with no
  further upload queued, draining buffered work on shutdown and commit panic.

- Manifest publication uses a separate mutation-order mutex. Planning and
  publishing the durable index take the index mutex briefly; serialization and
  durable blob publication do not. Committed reads can proceed with the previous
  index, and failed publication exposes no new entries. Retention mutations
  acquire the mutation-order mutex before the index mutex, preserving ordering.
- The gated reader regression fails on the prior code and passes for successful
  and failed publication. Tests also cover concurrent direct commits, offset
  assignment and retention while publication is blocked.

## Reproduce focused tests

From the Fireweed repository root, use the checked-in standalone test lockfile
and keep its build artifacts separate:

```sh
cargo test --locked --manifest-path vendor/object-log/Cargo.toml --target-dir target/object-log-tests --lib --test engine --test manifest --test blob --test sequencer_conformance --test perf_budget
```

These library tests supplement the public Fireweed workflow and filesystem
recovery tests; their memory-store rates are not capacity qualification.
