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
