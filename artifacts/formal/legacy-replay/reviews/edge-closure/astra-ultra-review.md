# Astra Ultra review — complete bounded edge closure, 2026-10-11

Requested reviewer: gpt-6-astra, ultra reasoning. Final disposition: **APPROVE
exact source and complete bounded backend edge closure; no corrections requested.**

The reviewer independently reconstructed all 20,920 cases / 153,819 steps and
verified exact set equality with all 20,913 graph edges. Zero remain unwitnessed.
A separate independent audit verifies the earliest concrete case/step witness for
all 3,918 previously missing edges: 26 ConcurrentClaims transitions and 3,892
downstream transitions, all from reverse schedules.

Verified all 399 current/before/after source hashes, unchanged independent model,
7,837 actual pairs of overlapping backend calls (3,919 forward / 3,918 reverse),
10,064 repaired-tail and 8,318 retained-state reopens, six exact Rust tests with
zero ignored, native CAS, binary identity, artifact hashes, disk capacity/private
paths/cleanup and separately hashed CI wiring. Maximum trace duration was 4,838ms
under the unchanged 20-second bound; Cargo runtime was 3,480.14 seconds.

Approach feedback incorporated before qualification:

- Derive reverse selection from the independent model; keep full oracle alternatives
  and compatible predecessors. Use fresh combined qualification at one frozen source.
- Measure actual first poll, Pending and Ready events; reject sequential/immediate
  impostors, wrong order and duplicate completions. Do not infer admission order
  from polling order or serialize calls to manufacture concurrency.
- Require per-case observed schedule-path equality and reconstruct aggregate coverage
  from per-step witnesses. Reject fabricated coverage and exhaustiveness flags.
- Reconcile case/base/schedule identities and require complete graph set equality.
- Move private S3/projection fixtures to an inode-rich filesystem after the incomplete
  ENOSPC attempt; keep every S3 world through final qualification. Add inode/byte
  capacity controls, dynamic-inode handling, device snapshots and confirmed cleanup.
- Make the qualified 96-case schedule-smoke command with eight worlds the CI gate;
  bind workflow YAML separately and distinguish local validation from hosted CI.

No production admission hook was needed. The resource-failed attempt remains
unqualified and supplies no final physical witnesses. No vision/PRD requirement,
independent model, normalizer, oracle or timeout was relaxed.

Retained [full audit](../../evidence/edge-closure/independent-final-audit.json),
[full audit script](../../evidence/edge-closure/independent-scheduled-audit.py),
[closure audit](../../evidence/edge-closure/independent-closure-index-audit.json), and
[closure audit script](../../evidence/edge-closure/independent-closure-index-audit.py).
Approval covers the declared finite backend fixture; broader facade/API-007,
multi-process, process-kill, cloud-S3 and universal refinement claims remain outside it.
