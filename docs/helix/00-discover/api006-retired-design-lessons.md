---
ddx:
  id: discover-api006-retired-design-lessons
  status: archived
---

# Lessons retained from the retired API-006 design

The owner retired this interface direction on 2026-10-10. API-007/ADR-026 govern.
This inventory retains evidence rather than reviving the proposed contract.
Source: `rescue/sindri-2026-10-01/wip-main` at
`1b65a5b731ddea052c1c5b77d9916732c09c8aae`. Before deletion, preserve that commit
under `archive/api006-design-20261010`; the archive is historical, not a work queue.

## Evidence to carry forward

| Source | Lesson | Use in the simple interface |
|---|---|---|
| SPIKE-002 record-key storage | Per-key indexed lookup avoids unhinted key lookup/IN-list cliffs; compact queue/key representation reduces row footprint | Measure actual claim/read/commit SQL plans; enforce declared index use; rerun on stock Turso |
| SPIKE-003 record-field storage | Large values and index maintenance change update cost | Keep state/body hydration off selection; measure write amplification and fanout |
| SPIKE-004 claim scale | Narrow indexed claim selection and separate lease metadata can reduce expiry/renewal work | Prototype only when identical lease/ordering guarantees pass; no compulsory batch-token redesign |
| SPIKE-005 value-size boundary | Inline large fields and statement forms can sharply increase cost | Shared explicit byte admission; size-spectrum benchmarks; preserve Snorri valid outputs |
| SPIKE-006 field layout | Hot/cold row separation and projected hydration matter; per-queue physical schemas have large creation/open costs | Use shared tables keyed by queue; compare realistic filtered claims with large state/body |
| TD-017 | Lean claim rows, field-selective reads, deterministic log-carried tokens/versions, cached statements and bounded writer resources | Retain as implementation hypotheses with independent qualification |
| API-006 conformance | Stable requirement ids, canonical limits, lost-response hook, reopen and mandatory suite | One public conformance suite for PostgreSQL and S3/Turso, plus process-kill/multi-process gates |
| API-006 operator split | Consumer work must not force-clear another holder's lease | Explicit maintenance owner, no consumer lease-theft shortcut |
| TD-018 measurement plan | Shared publication cost grows with queue count; measure PUT/GET/latency/index size before redesign | Benchmark current S3 commit economics; do not implement speculative partitions/truncation |

TD-017 itself labels its spike host WSL2, 32 cores, load averages 5–43. Relative
comparisons within a spike are evidence; absolute rates are directional. Spikes
used the then-current engine/layout, not the recently qualified stock Turso pin.
No number here is a performance promise for current main.

## Designs intentionally not carried forward

API-006's stage lifecycle, WholeGroups consumer protocol, mandatory per-claim token,
64 KiB state-field ceiling, elimination of instance fencing, quiescent-only progress,
and 24-hour minimum receipt retention do not establish current Snorri requirements.
The old API-003 rewrite also changed await-consumption state-version semantics.
The accepted simple contract keeps business invariants and solves storage internals.

TD-018 partitions, HeadSequencer, snapshots, archive export and log truncation
remain proposals. Genesis rebuild and current ownership fencing are retained.
API-006/ADR-025 contract deletions are not imported. Original research is reachable
from the archive, with source/range context, without leaving an active design branch.
