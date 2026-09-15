# Local machine diagnosis — 2026-09-15

The running kernel has a strong match to a known Btrfs writeback regression.
Resolve this before treating further performance measurements as a clean
hardware baseline. This is not yet proof that the defect causes the entire
38–45 MiB/sec sustained-write slowdown.

## Evidence

- Running kernel and installed package: `7.2.3-arch1-3` / `7.2.3.arch1-3`.
- Kernel log capture contains **86,083 identical Btrfs errors**, beginning
  September 10 at 02:50:50, still recurring September 15:
  `root 257 ino 8721967 folio 178978816 is fixup with an empty fixup bitmap`.
- Inode 8721967 is
  `target/debug/deps/differential-ccf77c21858d741e`, a 286,924,184-byte build
  artifact. Its modification time is September 10 at 02:50:50.660631409 -0400,
  matching the first logged error to the second. The file was preserved.
- The project is on a Kingston OM8PCP3512F-AB NVMe, through LUKS and Btrfs
  with zstd compression. The Samsung SATA drive is unmounted and unused.
- PCIe link reads 8.0 GT/s ×4, matching the advertised maximum in sysfs.
  Runtime power control reads `on`; AC is connected.
- Btrfs device write/read/flush/corruption/generation error counters all read
  zero. Estimated free space is approximately 325 GiB. These counters do not
  rule out the observed software defect or establish drive health.
- No D-state processes were present at the process snapshot. The earlier
  sequential test was sampled waiting in `balance_dirty_pages`.

The upstream [7.2 backport discussion](https://lore-kernel.gnuweeb.org/linux-btrfs/20260902-daily-reply-0003-btrfs-writeprotect-7-2@kernel.org/T/)
reports this exact error with linking on compressed Btrfs and explains the
missing write protection during writeback. The
[official Linux 7.2.4 changelog](https://cdn.kernel.org/pub/linux/kernel/v7.x/ChangeLog-7.2.4)
includes `074c715e0b498891c09fe7f11e1cd9d7a04699bd`,
**btrfs: write-protect folios during data writeback**. Thus a kernel containing
that fix is the next corrective test. Matching symptoms and version are strong
evidence, but not a controlled before/after performance result. The installed
Arch package's downstream patch contents have not been independently audited.

## Next steps

1. Obtain administrator access for read-only controller diagnostics. Utilities
   are currently absent. Install through `omarchy pkg add nvme-cli smartmontools`,
   then capture `sudo nvme smart-log /dev/nvme0 -o json`,
   `sudo nvme error-log /dev/nvme0 --log-entries=16 -o json`, and
   `sudo smartctl -x /dev/nvme0`. Preserve health and thermal transition counters
   before additional load. Do not infer throttling from temperature alone.
2. Use the supported distribution update path to install a kernel containing
   the fix, verifying the actual package version before rebooting. Upstream
   7.2.4 contains it; the locally cached Omarchy repository still offers
   7.2.3.arch1-3. A live mirror metadata request returned HTTP 403, so fixed
   package availability is not established. Do not blindly assume an update
   command will install the fix or switch to an unverified LTS version.
3. After a coordinated reboot, verify the running kernel, inspect new-boot
   filesystem errors, and rebuild the affected disposable test artifact.
4. Repeat the same bounded sequential test with time-resolved bandwidth,
   device latency, temperature, and controller counters; then rerun the
   unchanged eight-cycle Fireweed campaign. Keep workload correctness,
   durability, and performance gates intact. A fixed kernel may remove this
   error without resolving every throughput bottleneck.

Administrator authentication is unavailable to this agent session:
`sudo` requires a password, and `pkexec` could not open an authentication
terminal. No packages were installed. No kernel, filesystem, power, or SSD
settings were changed, and no reboot or filesystem repair was attempted.
Additional heavy write benchmarks were paused upon finding the errors.

Raw evidence: `../helix/04-build/evidence/workflow-capacity/fireweed-local-kernel-storage-review.log.gz`
and `../helix/04-build/evidence/workflow-capacity/fireweed-local-kernel-diagnosis.json`.

## After booting Linux 7.2.6

The running kernel and installed headers now read `7.2.6-arch2-1`, and NVIDIA
DKMS 610.57.04 reports installed for that kernel. The new boot's kernel journal
had no Btrfs error entries before or during the sequential retests.

Using the exact archived scripts, the 8 GiB direct/NOCOW test measured
**75.62 MiB/sec in 108.33 seconds**; the 8 GiB normal buffered test measured
**51.44 MiB/sec in 159.25 seconds**, including 3.47 seconds of final fdatasync.
Both verified the first/last blocks and removed their private files. These
improve on the preceding 44.99/38.51 MiB/sec pair but still reproduce low
sustained bandwidth. Kernel replacement and reboot changed multiple conditions,
including drive temperature/cache state; this does not isolate a kernel-caused
speedup. The absence of the prior errors does not resolve the remaining
storage-performance diagnosis.

Raw reports are `fireweed-kernel726-direct.json` and
`fireweed-kernel726-buffered.json` in the workflow-capacity evidence directory.
The existing CLI retains SHA256
`be319723be6ee3e617fe0e3694045a589d86bb60dfa8f612357d7f24f5d9a030`.
An unchanged 64-store, two-worker, eight-cycle stretch campaign was started
under tag `kernel726-s64-w2-eight`; its outcome must be inspected before any
qualification claim. No throughput gate or application code was changed.
