---
ddx:
  id: build-edge-closure-evidence
  type: evidence
  depends_on: [build-edge-closure, contract-model-trace-replay, tp-formal-analysis]
  status: complete
---

# Complete witnessed bounded S3/Turso graph

The fresh disk-backed replay passed **20,920 cases and 153,819 action steps**.
Actual witnessed edges set-equal the independent complete graph: **20,913 of
20,913 edges**, **zero unwitnessed**, across 6,386 model states. All 3,918 edges
missing from the prior qualification now have concrete case/step witnesses in
[the closure index](evidence/edge-closure/closed-edge-witness-index.json.gz).
The index binds the old report, current report, current observations and model
hashes; the prior incomplete resource run contributes no qualification evidence.

All six Rust tests passed, zero failed or ignored, in 3,480.14 seconds (58m00s).
Eight independent worlds retained the unchanged 20-second per-trace deadline;
maximum trace duration was 4.838 seconds. The native local RustFS 1.0.0/Turso
fixture passed conditional create, conditional update and concurrent CAS race
preflight. RustFS binary SHA-256 is
`222eedc3d9baabf6516702d9fbf230270c3ca49b50f562d3461c96e2cc6ae6ad`.

## Independent schedule selection and actual witnesses

Canonical action-sequence dedup previously collapsed both concurrency histories.
The model alone selects the 17,002 canonical forward cases plus 3,918 greedy
reverse additions, covering 26 missing concurrent transitions and 3,892 downstream
transitions. Unique case/base-trace/schedule identities are reconciled across corpus,
progress and observations. The independent model, all successor alternatives,
normalization and compatible-predecessor oracle remain unchanged.

Biased polling changes stimulus only. Actual backend futures record first poll,
first Pending and completion for each request; both must be Pending before either
completes, with exactly one of each event per request. Responses stay in request
identity order. No production admission hook, serialized-call impostor or forced
model result is used. Observed per-case edge sets must realize the independently
predicted schedule path. The aggregate is reconstructed from per-step witnesses
and must set-equal the independently rebuilt graph, rather than trusting counts
or an exhaustiveness flag.

## Resource failure repaired without changing the oracle

The first expanded attempt completed 20,645 cases, including 20,637 reported PASS
and eight unqualified failures. Terminal observation-file creation failed with
ENOSPC; detailed observations and the final report were unavailable. Retained
progress, Cargo log, corpus, native-CAS preflight and source-bound diagnostic
capture the incomplete attempt. Individual failed-case causes cannot be audited
from progress alone; no product safety counterexample or passing qualification
is claimed.

The /tmp tmpfs has a 1,048,576-inode ceiling and about 604,000 free inodes after
owned-fixture cleanup, despite abundant byte space. Astra independently confirmed
the inode-headroom diagnosis and approved a disk-backed private fixture instead
of deleting S3 objects between cases. The corrected wrapper places sibling `s3/`
and `worlds/` directories under one private root on the chosen disk parent and
passes `worlds/` as child TMPDIR. It preflights conservative inode/byte headroom,
handles filesystems reporting no fixed inode total, retains filesystem/device
snapshots, terminates only its owned server and confirms private-root removal.

The qualified run reserved a budget of 1,347,072 inodes and 1,505,230,848 bytes
against 779,867,765 available disk inodes. All S3 worlds remained available until
final qualification; cleanup then completed. Filesystem snapshots include unrelated
activity and are not exact per-fixture allocation measurements. The capacity
budget is operational headroom, not a formal footprint theorem.

## Regression gates and source identity

Disk smoke passed 96 cases, 296 steps and 128 edges, with five witnesses per
concurrency order. CI now uses `schedule-smoke --workers 8`, preserving all 91
canonical cases and adding five reverse cases. [CI wiring](evidence/edge-closure/ci-wiring.json)
separately binds the workflow hash because workflow YAML is outside the execution
source-hash set. The local command and workflow policy passed; hosted GitHub Actions
execution is not claimed.

Additional checks passed: 27 Python controls, nine complete abstract models with
21,517 states and 20 detected negative controls, Rust formatting, CI policy and
whitespace checks. New controls reject missing or malformed schedules, omitted
reverse cases, sequential/immediate-ready concurrency impostors, wrong order,
duplicate events, fabricated aggregate coverage, forged exhaustiveness, unrealized
schedules, and low inode headroom despite free bytes. Existing branch-switching,
decoder, token/receipt and tail controls remain active.

The tested HEAD is `0e098f9515ba930729c4275022a7f87d6e4c594a`, plus hash-identified
task edits. All 399 execution-source hashes were stable before/after replay; the
final commit preserves those tested bytes and records that HEAD as its parent.
The primary checkout and its independent work were preserved. DDx doctor/doc stale
were unavailable because the CLI was absent; no tracker mutation is claimed.

## Review, retained artifacts and scope

Astra Ultra independently **APPROVED the exact source and complete bounded edge
closure**, with no remaining corrections. It reconstructed every model path and
physical frontier, verified 7,837 actual overlap witnesses (3,919 forward and 3,918
reverse), 10,064 repaired-tail and 8,318 retained-state reopens, and all source,
artifact, CAS, binary, cleanup and CI wiring checks. A separate audit independently
reconstructed and verified every one of the 3,918 closure-index entries. See
[final review](reviews/edge-closure/astra-ultra-review.md),
[full execution audit](evidence/edge-closure/independent-final-audit.json) and
[closure audit](evidence/edge-closure/independent-closure-index-audit.json).

[Manifest](evidence/edge-closure/manifest.json) binds 35 retained artifacts; all retained
and decompressed original hashes were verified after capture. Deterministic gzip binds artifacts
to exact original bytes and SHA-256 hashes, with reproducible independent audit
and index scripts. Extract artifacts to the paths recorded in their reports to
reproduce precommit checks; the auditor requires the recorded tested HEAD with
its hash-identified edits. Committing those bytes changes HEAD identity, not the
qualified source contents.

This is complete witnessed coverage of the declared finite backend model and
fixture. It exercises the S3/Turso composition at the tested revision, a one-process
ephemeral control plane, graceful reopen and simulated client nonreceipt. Facade
routing, API-007 queue execution, shared multi-process control planes, process-kill
recovery, production-cloud S3, universal Rust refinement and numeric production
deadlines remain outside this evidence.
