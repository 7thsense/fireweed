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
