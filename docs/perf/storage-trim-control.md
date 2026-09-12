# Proposed one-time storage control

The current six-cycle source-aligned campaign control completes 6,000,000
recipients at 7,911.78/sec. Every correctness, progress, due-time, RSS, database
and WAL gate passes; overall and later-cycle throughput fail. All 32 DB/log pairs
share one Kingston OM8PCP3512F-AB NVMe through the encrypted root filesystem.

Read-only observations: the NVMe supports discard (2 TiB maximum request), the
root crypt mapping advertises 0-byte discard support, the Btrfs mount has no
discard option, and `fstrim.timer` is disabled and inactive. The 7,911/sec run
writes 26.23 GiB at the host device, averaging 35.50 MiB/s. Device reclamation
is a plausible contributor; no causal attribution or speedup is established.

The runtime helper and its mocked tests are archived for review as
[helper source](../helix/04-build/evidence/workflow-capacity/fireweed-trim-once.py.txt)
and [control-flow tests](../helix/04-build/evidence/workflow-capacity/test_fireweed_trim_once.py.txt).
The exact helper SHA256 is
`06d5971fbfd03d05c967292eb40bc172b4681e9e7095ff3e941fb4a589e002f9`.

## Exact proposed action

After explicit approval, run the reviewed helper:

    pkexec /usr/bin/python3 /tmp/fireweed-trim-once.py --apply

It verifies the root filesystem is Btrfs on the expected encrypted `root`
mapping, verifies LUKS1/2 on `/dev/nvme0n1p2`, reads and preserves known active
performance flags, and refuses unknown flags or a running Fireweed benchmark.
It temporarily refreshes `root` with those flags plus `--allow-discards`, runs
`fstrim --verbose /`, then restores and verifies the original activation flags
in a finally block, including on refresh/trim failure. It does not use
`--persistent`, alter boot configuration, enable a timer, or perform raw-device
discard. Authentication belongs in the desktop authorization prompt; no keys
or passwords are requested by the helper or logged.

This intentionally discards filesystem-free extents. Allowing discard reveals
free-block allocation information on an encrypted device; restoring the runtime
flag cannot undo that disclosure or make discarded old free-space data recoverable.
That host-level policy choice needs approval beyond repository optimization.

Six mocked control-flow tests pass: read-only planning, preservation of every
known flag, restoration after trim failure, restoration after ambiguous refresh
failure, refusal of unknown/already-changed policy, and visible restore failure.
These tests do not prove the host's firmware behavior. See the exact helper and
`/tmp/fireweed-trim-helper-tests.log` for review.

After restoration, repeat the exact clean `1fe89a44` one-worker, 1,000-row,
six-cycle campaign on binary SHA256
`bb7ee7df61f0b3306bbe22fd8bfe696ab648522e295a6041773f80029ff9d080`.
Measure throughput, host bytes, CPU, all qualification gates and discard counters.
Keep this host-maintenance comparison separate from backend-code improvements.
No maintenance has been performed yet.

Sources:
- [Kernel dm-crypt documentation](https://docs.kernel.org/admin-guide/device-mapper/dm-crypt.html)
- [Kingston garbage-collection discussion](https://www.kingston.com/en/blog/servers-and-data-centers/garbage-collection)
- Installed cryptsetup-refresh(8), dated 2026-07-21; source description and hash
  captured in `/tmp/fireweed-storage-discard-review.json`.
