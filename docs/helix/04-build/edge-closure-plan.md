---
ddx:
  id: build-edge-closure
  type: implementation-plan
  depends_on: [build-all-edge-replay-evidence, contract-model-trace-replay, tp-formal-analysis]
  status: complete
---

# Witness the complete bounded S3/Turso replay graph

Astra Ultra approved a fresh combined replay, subject to actual overlap telemetry,
independent canonical selection, unchanged oracle alternatives, and exact witnessed
edge-set equality. Preserve all prior evidence and the isolated task branch.

1. Derive forward paths and greedy reverse additions solely from the complete model.
   The 17,002 original prefixes plus 3,918 reverse cases yield 20,920 cases and
   153,819 steps. Missing edges comprise 26 concurrent transitions and 3,892 downstream
   transitions. Keep all model alternatives and compatible predecessor propagation.
2. Use biased first polling of actual backend futures; measure first poll, Pending
   and Ready. Require genuine overlap, declared polling order, exactly one completion
   per request and normalize responses by request identity. Scheduling is stimulus,
   not evidence of realized order. If insufficient, use only a test-only per-world
   admission rendezvous preserving real storage decisions and both live invocations.
3. Pass schedule-smoke and controls for sequential impostors, invalid schedules,
   missing cases and branch switching. Bind base trace, schedule and case identity
   across corpus, progress and final observations.
4. Freeze source, run every case on native-CAS local RustFS and actual Turso with
   eight worlds and unchanged 20-second per-trace deadline. Require actual witnessed
   edge set to equal the independently rebuilt 20,913-edge graph. Counts alone fail.
5. Independently audit physical history, frontier/reopen behavior, source and artifact
   hashes with Astra Ultra. Fix any findings and rerun affected qualification.
6. Update Helix definitions/evidence, retain exact-byte artifacts and reviews, commit
   completed work preserving history. No tracker creation or external publishing.

Existing limits remain: backend-level composition, one-process ephemeral CP, graceful
reopen and simulated client nonreceipt; no cloud-S3, facade/API-007, process-kill,
universal refinement or production numerical deadline qualification is implied.

## Operational interruption and correction

The first expanded run completed 20,645 cases (20,637 PASS, eight unqualified
failures) before terminal observation-file creation failed with ENOSPC. Detailed
observations and the final report were unavailable. /tmp has 1,048,576 total inodes
and about 604,000 free after task-owned RustFS cleanup despite abundant free bytes;
retained S3 worlds exceeded the available fixture inode headroom. Preserve exact
progress, Cargo log, corpus, preflight and source-bound diagnostic. This run is
not a product safety counterexample and cannot supply final edge qualification.

Astra approved moving the private fixture to the disk filesystem with about
780 million available inodes, retaining all S3 data through qualification. Add
capacity regression controls and record placement, headroom and confirmed cleanup.
Restart smoke and all 20,920 cases on the corrected frozen wrapper/spec source;
Rust interpreter/model/oracle/20s deadline remain unchanged. CI uses the already
qualified schedule-smoke command with eight worlds to retain both request orders.

## Completed acceptance

The corrected disk-backed run passed every 20,920 case and 153,819 step and
witnessed the exact complete 20,913-edge graph, zero remaining. Astra Ultra
approved the exact tested source and independently reconstructed the closure
index for all 3,918 prior missing edges. All six Rust tests and 27 Python controls
passed; actual fixture cleanup and separately hashed CI wiring were verified.
See [qualification evidence](edge-closure-evidence.md) and
[final review](reviews/edge-closure/astra-ultra-review.md).
