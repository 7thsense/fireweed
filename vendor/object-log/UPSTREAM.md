# Pinned upstream source

Source: https://github.com/7thsense/object-log, tag v0.3.1,
commit dcd37c0e7de3afa26e671245faf41ee513c984ec.

Cargo.toml, README, licenses, src and tests are copied unchanged from that
commit. Fireweed patches this package locally to evaluate combining already
completed ordered data uploads into one durable manifest publication. No
storage-format migration or relaxed durability barrier is intended.

The sibling object-log checkout and Cargo's source cache are not modified.
Local implementation changes must be recorded below and covered by ordering,
error, durability and recovery tests before workflow measurements.

## Local changes

- Retain the earliest failed PUT/manifest enqueue position for cumulative
  flush barriers. Barriers before that position can succeed; covering barriers
  return the original failure even if later appends succeed. Producer error
  handling, offset assignment and on-disk formats are unchanged.
- Root Cargo.lock changes only the object-log source from its pinned Git entry
  to this path. Standalone tests use an ignored local lockfile and a separate
  target directory to avoid changing Fireweed build artifacts.

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

## Reproduce focused tests

From the Fireweed repository root, seed the ignored standalone test lockfile
from the archived lock and keep its build artifacts separate:

```sh
cp docs/helix/04-build/evidence/workflow-capacity/fireweed-object-log-test-Cargo.lock vendor/object-log/Cargo.lock
cargo test --locked --manifest-path vendor/object-log/Cargo.toml --target-dir target/object-log-tests --lib --test engine --test manifest --test blob --test sequencer_conformance --test perf_budget
```

These library tests supplement the public Fireweed workflow and filesystem
recovery tests; their memory-store rates are not capacity qualification.
