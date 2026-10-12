---
ddx:
  id: build-all-edge-replay
  type: implementation-plan
  depends_on: [contract-model-trace-replay, tp-formal-analysis, build-generated-trace-replay]
  status: complete
---

# Full real-S3 all-edge corpus replay

Execute the complete canonical edges profile:17,002 prefixes,127,208 operation
steps, all20,913 modeled edges intended,6,386 complete reachable model states.
Qualify committed base7a6cb860 in an isolated checkout on
`codex/fireweed-all-edge-replay`; preserve the five unrelated dirty queue files
in the primary checkout. No tracker mutation is requested or claimed.

1. Review deep execution/isolation/coverage with explicitly requested Astra Ultra.
2. Add bounded independent-world concurrency, flushed progress and strict terminal
   corpus/action reconciliation; preserve20s timeout and every model oracle.
3. Pass eight-world smoke, then execute every full-corpus trace on isolated native
   CAS RustFS and actual Turso. Stop launching after failure and drain owned work.
4. Repair confirmed product/harness issues proactively with independent regression
   evidence, rerun affected gates and preserve source-bound counterexamples.
5. Independently audit every final observed model path, progress/case/action counts,
   coverage and artifact/source hashes with Astra Ultra. Record all feedback.
6. Retain compressed exact-byte evidence, update Helix qualification/limits, commit
   only task files and preserve audit history. No remote merge/publish is requested.

Full corpus completion is distinct from actual all-edge exhaustiveness: one
concurrent-prefix execution realizes only one serial order. Both independent
predictions remain permitted; no expected branch is forced from SUT results.

Prelaunch Astra review approved the parallel loop and exact eight-world smoke:
91 traces,282 passing steps,119 matched edges, stable source and verified hashes.
The initial full launch hit task disk quota during corpus writing before any Rust
trace started. Recovery omitted historical evidence from this isolated checkout
and removed only task-owned inactive compiler libraries after restarting Rust.
The qualified run is source-frozen and retains its own fixture/process lifetime.


## Fixture scaling correction

Astra discovered a real harness defect during the first source-frozen deep run:
noncanonical `/log/` and `/meta/` suffixes made the production generation-prefix
resolver return an empty prefix, causing whole-bucket LIST on every open/reopen.
At equal five-step length, retained-world accumulation grew median duration from
4.76s to8.53s. The incomplete run was stopped before the20s bound was exceeded;
no full PASS was claimed. Exact partial progress and source-bound diagnostics are
retained, including the final post-signal progress reconciliation.

The permanent correction uses the product's canonical `/fwlog/` and `/fwmeta/`
suffixes. A regression calls the production resolver through the actual fixture
factory and checks the world namespace parent; the old pair demonstrates the
empty-prefix failure. Model, normalization, oracle and20s deadline are unchanged.
The corrected eight-world smoke passes91/91 and allfive Rust controls. Entire
canonical full-corpus qualification restarts on that corrected frozen source.

## Completion

The corrected full run passed all 17,002 traces and 127,208 steps. Astra Ultra
independently approved the exact tested source and evidence. Actual coverage is
16,995 of 20,913 intended edges, with 3,918 unwitnessed alternatives; the graph
is not edge-exhaustive. See [qualification evidence](all-edge-replay-evidence.md)
and [final review](reviews/all-edge-replay/astra-ultra-review.md).

## Integration scope

These historical records were moved outside active Helix documents when merged
with the updated API-007 specs. Reproduce their qualification at the recorded
source revision and hash-identified edits. Their complete legacy graph coverage
does not establish current API-007/public-facade correspondence or readiness.
