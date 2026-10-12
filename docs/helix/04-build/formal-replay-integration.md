---
ddx:
  id: build-formal-replay-integration
  type: implementation-note
  depends_on: [contract-model-trace-replay, tp-formal-analysis, architecture]
  status: complete
---

# Legacy replay integration into current main

Preserve the current API-007 public-facade correspondence, fresh operational-clock,
expiration, group atomicity and acknowledged-read requirements. The paused-apply,
ephemeral-CP interpreter remains a legacy regression fixture. Its scheduled
coverage obligations require actual overlapping calls, compatible predecessor
paths, canonical selection and exact graph set equality; these obligations do
not qualify current API-007 readiness.

Historical qualification payloads and reviews are retained outside active Helix
in [legacy artifacts](../../../artifacts/formal/legacy-replay/edge-closure-evidence.md),
with their original source-bound hashes and revision scope. Both original branch
commits remain in merge history. Run future graphs, captures and review journals
in run-owned target/evidence storage.

Integration acceptance passed on the resolved main tree: Python controls,
queue-module boundaries and their negative/compiler controls, workflow policy,
Rust formatting/whitespace and real-S3/Turso schedule smoke, with six Rust tests
and zero ignored. The smoke retains both request-order stimuli and inode-rich
private fixture placement without changing current production semantics.
See [run-owned integration manifest](../../../artifacts/formal/legacy-replay/integration/manifest.json).
