---
ddx:
  id: build-all-edge-replay-evidence
  type: evidence
  depends_on: [build-all-edge-replay, contract-model-trace-replay, tp-formal-analysis]
  status: complete
---

# Full S3/Turso model corpus qualification

The complete canonical edges profile passed on 2026-10-11: **17,002 traces,
127,208 action steps**, eight independent worlds, all five Rust controls passed
with zero ignored. Cargo test execution took 2,843.32 seconds (47m23s). Maximum
trace duration was 5.012s; the unchanged per-trace bound is 20s.

The model graph contains 6,386 complete reachable states and 20,913 edges.
All selected prefixes executed, but actual matched coverage is **16,995 edges**;
**3,918 intended edges remain unwitnessed**. A concurrent prefix permits alternate
serial outcomes, and one execution realizes only one. All 3,919 ConcurrentClaims
observations chose [0,1]. This is full selected-corpus qualification, not actual
all-edge exhaustiveness or universal implementation refinement.

The fixture used real local RustFS 1.0.0, native conditional S3 writes and actual
Turso. Native CAS create/update/race preflight passed. This is direct backend-level
replay of the S3/Turso composition used by public Fireweed at tested revision
7a6cb8606fb80b531b1d9245743ae880646b8a1b, plus hash-identified task edits.
All 399 source hashes were stable before/after execution and independently audited.
The final commit contains those tested edits; its parent is the recorded tested HEAD.
The five unrelated queue edits in the primary checkout were excluded and preserved.

## Confirmed correction and feedback

The first deep run exposed a fixture scaling defect: `/log/` and `/meta/` made the
production generation-prefix resolver return an empty prefix. Every reopen then
listed the accumulated bucket. The task stopped its owned run and retained the
incomplete progress and diagnostics; this is not a product safety counterexample.
Canonical `/fwlog/` and `/fwmeta/` now scope generation scans to each world. A fifth
Rust control exercises the actual fixture factory and production resolver, including
the old empty-prefix behavior. No model oracle or timeout was relaxed.

An independent comparison of identical five-step trace IDs 1600–1799 found median
latency fell from 8,527ms to 1,164.5ms, and maximum from 13,011ms to 1,463ms.
The latency audit records a byte-prefix hash of the growing corrected progress file;
that snapshot is distinct from the final complete progress hash.

Astra Ultra reviewed the plan and concurrency design, diagnosed the namespace
scaling problem, clarified intended versus witnessed coverage, and independently
approved the complete corrected run. Its final audit reconstructs all transitions,
compatible model paths, physical applied-prefix/reopen frontiers and cumulative
progress. It verifies 8,648 repaired-tail and 6,362 retained-state reopens, exact
Rust test names, CAS evidence, binary identity, artifact hashes and current source.
See [review](reviews/all-edge-replay/astra-ultra-review.md).

Additional checks passed: corrected smoke (91 traces, 282 steps), 22 Python controls,
nine complete models (21,517 states) with 20 detected negative controls, CI policy,
Rust formatting and whitespace checks. DDx doctor/stale could not run because the
DDx CLI was absent; no tracker operations were claimed.

## Retained evidence and limits

[Manifest](evidence/all-edge-replay/manifest.json) binds 23 retained artifacts to
original bytes and SHA-256 hashes. Deterministic gzip preserves the exact original
report, corpus, observations, progress and Cargo log bytes; every compressed and
decompressed hash was checked after capture. The manifest also retains smoke,
formal/control checks and incomplete-run diagnostics.

- [Full report](evidence/all-edge-replay/report.json.gz)
- [Canonical corpus](evidence/all-edge-replay/corpus.json.gz)
- [Observations](evidence/all-edge-replay/observations.json.gz)
- [Progress](evidence/all-edge-replay/progress.jsonl.gz)
- [Independent audit](evidence/all-edge-replay/independent-final-audit.json)
- [Audit script](evidence/all-edge-replay/independent-final-audit.py)
- [Audit execution](evidence/all-edge-replay/independent-final-audit.log)

The audit script preserves the exact precommit execution procedure: replay paths
refer to the original artifacts and it requires the tested base HEAD. To reproduce,
use a checkout of that base with the recorded source edits and extract artifacts to
the report paths; committing the same source does not change its hash identity.

Qualification covers a one-process ephemeral control plane, graceful reopen and
simulated client nonreceipt. Production-cloud S3, facade routing, the API-007 queue
pipeline, process-kill recovery, shared multi-process control planes, universal
refinement and numerical production deadlines remain outside this evidence.
