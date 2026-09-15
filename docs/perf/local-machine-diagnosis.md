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

The eight-cycle campaign has now completed: child exit 0, **9,971.46 complete
recipients/sec**, 802.76 seconds process wall time, **1.04243 CPU-ms/recipient**,
and peak RSS **16.89 GiB**. All non-throughput qualification checks passed;
the slowest cycle was **6,902.51/sec**. Both the sustained 10k and 12.5k goals
remain unmet. Host-wide writes were 24.64 GiB (31.50 MiB/sec), with 58.28 ms
mean completed write-request latency. No Btrfs errors were observed in the new
boot's journal after the run. Fixing the recurring kernel error was insufficient
to meet the performance target; it must not be presented as the sole cause.

The archived `fireweed-kernel726-campaign-summary.json` lists every failed gate.
Raw campaign, monitor, and provenance reports are archived with the same tag.
The successful run's projection root is retained for further inspection; its
path is recorded in the summary. The application's automatic successful-run
log cleanup remains unchanged. Future code experiments should compare against
this fixed-kernel baseline with the same original-row workflow and gates.

## Expected performance and independent dd comparison — 2026-09-15

The approximately 50–70 MB/sec long-write results remain unexplained. They
should not be accepted as the SSD's normal capability or attributed to Btrfs
merely because the filesystem is Btrfs. We did identify a Btrfs correctness
regression, but the fixed kernel still exhibits slow sustained writes.

Published exact-model results provide a useful sanity check:
[PassMark](https://www.harddrivebenchmark.net/hdd.php?hdd=KINGSTON+OM8PCP3512F-AB)
reports approximately 626–627 MB/sec sequential writes (the fetched page and
search index differed slightly), while
[Novabench](https://novabench.com/parts/storage/kingston-om8pcp3512f-ab)
reports 930 MB/sec. These are aggregated benchmarks, not guaranteed sustained
8 GiB incompressible dd results on encrypted Btrfs. Novabench's current
[methodology](https://novabench.com/docs/benchmarks/storage-benchmark)
uses 4 MB asynchronous sequential writes at queue depth eight; our probes
submit one userspace write at a time. Request splitting below userspace still
produces multiple outstanding block requests. No verified manufacturer
steady-state write specification for this exact OEM model/firmware was found.

Btrfs's [mount documentation](https://btrfs.readthedocs.io/en/latest/ch-mount-options.html)
says NOCOW also disables data checksums and compression; its
[compression documentation](https://btrfs.readthedocs.io/en/latest/Compression.html)
distinguishes direct-write fallback for checksummed files from the direct path
for files without checksums. Our fresh NOCOW/direct probe remained slow, so
data-checksum CPU cost, data COW, or zstd compression alone cannot explain the
full slowdown. NOCOW does not bypass Btrfs metadata allocation or dm-crypt.

The [kernel dm-crypt documentation](https://www.kernel.org/doc/html/latest/admin-guide/device-mapper/dm-crypt.html)
describes cases where workqueue/write-submission scheduling hurts performance.
That is a mechanism worth distinguishing in a trace, not evidence that changing
those options would fix this machine. No encryption or filesystem settings
were changed. Btrfs is therefore still a candidate, but is not isolated as the
cause of the remaining slowdown.

[Kingston's NAND explanation](https://www.kingston.com/en/blog/pc-performance/difference-between-slc-mlc-tlc-3d-nand)
describes fast pseudo-SLC caching in consumer SSDs ahead of slower dense flash.
It makes cache exhaustion/internal housekeeping a plausible hypothesis for a
burst-to-sustained collapse, not a diagnosis of this drive. Its exact NAND
configuration and sustained cache-exhausted rate have not been established.
The NVMe interface alone does not guarantee a 150 MB/sec minimum under all
write conditions. That threshold is nevertheless a reasonable diagnostic
expectation to investigate here, especially given the exact-model benchmark
results and this machine's measured burst headroom.

The dd test used an 8 GiB incompressible source prepared on tmpfs before timing,
16 MiB writes, and conv=fdatasync. Direct/NOCOW ran first, then normal buffered
writes, then the exact archived Python direct and buffered probes. No write
tests or Fireweed benchmarks overlapped. Only a lightweight device-counter
sampler ran alongside dd. Owned scratch data was verified at its first/last
blocks and removed after each successful test; random-source generation and
verification were excluded from timing.

The direct dd test measured 67.09 MiB/sec (70.35 MB/sec), versus physical-device
counters of 67.32 MiB/sec. Buffered dd measured 47.91 MiB/sec (50.24 MB/sec),
versus device counters of 48.29 MiB/sec. Mean completed device write-request
latencies were 130.08 and 245.83 ms. These corroborate a real storage-path delay
without Python or Fireweed in the write loop. Device counters are host-wide,
and their window extends slightly beyond the timed dd process.

Direct dd's first 10.7 sampled seconds averaged 274.8 MiB/sec at the device;
the following 29.0 seconds averaged 29.2 MiB/sec, and the remaining 82.4 seconds
53.7 MiB/sec. Thus even within one unchanged command the service rate collapses.
NVMe temperature rose from 29.85 to 59.85 C during direct dd and from 59.85 to
69.85 C during buffered dd. Temperatures alone neither prove nor rule out
controller throttling; before/after controller transition counters are needed.

At 150 MB/sec, 8 GiB would take 57.27 seconds. Actual dd wall times were 122.11
and 170.97 seconds: 2.13× and 2.99× that duration. The larger ~10× discrepancy
is against published several-hundred-MB/sec benchmark results, whose workload
and queue depth differ. MB and MiB are explicitly distinguished here.

Next discrimination should measure request latency at both dm-crypt and NVMe
boundaries, alongside controller thermal counters, followed if needed by the
same workload on an independent filesystem on the same hardware. A loopback
ext4 file hosted on Btrfs does not remove Btrfs from the stack and cannot serve
as the decisive filesystem control. An alternate-filesystem control needs a
properly reserved scratch area; the existing OS/BitLocker partitions are not
scratch space. No raw-device write or repartitioning was performed.

The final Python repeats completed at **51.21 MiB/sec direct** (159.97 seconds,
0.853 CPU-seconds) and **43.51 MiB/sec buffered** (188.28 seconds,
3.008 CPU-seconds). These reproduce slow writes using the exact earlier
scripts. The initial post-reboot 75.62/51.44 pair was faster; the repeat shows
substantial state/order variation and does not support assigning a precise
performance improvement to the kernel fix. Python user execution consumed
0.0124 and 1.2977 seconds respectively, so interpreter execution is not a
credible dominant cause of the observed minutes of elapsed time.

Evidence: `fireweed-dd-recheck.py`, `fireweed-dd-recheck-results.json.gz`,
`fireweed-python-after-dd-{direct,buffered}.json`, and
`fireweed-dd-python-comparison.json` in the workflow-capacity evidence directory.
All four new write tests are complete; no benchmark remains running.

### NVMe command trace after terminal authentication (2026-09-15)

One additional, sequential 8 GiB Python direct/NOCOW test completed on kernel
7.2.6 with a private ftrace instance recording NVMe command setup and completion.
No other benchmark ran concurrently. The workload ran as the ordinary user;
root privileges were used for tracing and SMART reads. The instance was removed
on completion. No filesystem, encryption, power, or SSD setting was changed.

The test took **122.016 seconds, 67.138 MiB/sec**, including final fdatasync.
Python consumed **0.846 CPU-seconds**. Successive 1 GiB segments achieved
**917.7, 510.5, 41.3, 43.8, 57.6, 61.2, 54.4, and 59.1 MiB/sec**.
Thus the same file and write loop demonstrate both the expected fast burst
and the subsequent sustained slowdown.

All 134,934 trace events paired into 67,467 commands without missing events,
duplicate command IDs in flight, retries, error statuses, or buffer overruns.
The 67,150 write commands accounted for 8,615,579,648 bytes, versus the
benchmark's 8,589,934,592 bytes; tracing includes filesystem/background traffic.
NVMe setup-to-completion write latency was **119.97 ms mean, 86.67 ms median,
281.25 ms p95, 495.81 ms p99, and 6.331 seconds maximum**. At least one write
was outstanding across a union of **120.920 seconds** of the 122.333-second
collector window. This is overlapping elapsed time, not the sum of request
latencies; it is also not an exact attribution of the benchmark's timed window.

This establishes substantial delay after NVMe command setup, rather than merely
inferring a device limitation from low application throughput. It does **not**
measure flash service time alone: driver queueing, submission and completion
handling remain included. It does not prove an SSD throughput ceiling or fully
exonerate Btrfs/dm-crypt, which still determine the incoming request pattern.
Python execution and compression/checksumming of this NOCOW file cannot explain
the observed long NVMe command latencies. Cache exhaustion/internal maintenance
is consistent with the burst-to-slow transition, but is not yet a proven cause.

Temperature rose from approximately 40 to 62 C. Media errors remained zero;
thermal-management transition count/time remained **4 / 3 seconds** before and
after. There is no new reported thermal-management activity during this test.

Evidence in the workflow-capacity directory: `fireweed-nvme-trace.py` (collector),
`fireweed-nvme-trace-raw.tar.gz` (trace, formats, benchmark and SMART reports),
`fireweed-nvme-trace-summary.json`, and `fireweed-nvme-trace-analyze.py` (replay
analysis with pairing and loss assertions). The test is complete. A useful next
discriminator is the same sustained workload through an independent filesystem
or OS on this controller, followed by a different drive if necessary. Repeating
the same Python/dd comparison or changing TRIM does not answer that question.
