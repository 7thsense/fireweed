---
ddx:
  id: storage-matrix-final-implementation-inventory
  depends_on:
    - storage-matrix-completion-brief
    - storage-matrix-composition-inventory
    - orthogonal-storage-matrix-brief
    - public-preview-boundary
  status: accepted
  review:
    self_hash: d3afef13284624c64743ff1f79e60bb68ae7716afb92f2174bbdd670e23a5cd9
    deps:
      orthogonal-storage-matrix-brief: 3e6dda6559c43fb47179240e3aa0b32e280c93ef1dca15177e37c5f7289134c4
      public-preview-boundary: 5ba43c1229b88bb13dcced736ff7adfd3346d68ad0af1f3cd771e3b1e2b4f906
      storage-matrix-completion-brief: 16a37c5b1c592108039bb5cfa176503112fc8509e1ab3334861643e7866c390f
      storage-matrix-composition-inventory: 15969794ab423d33b0acece7ffc53e6bf04158c5f80869d6ef6bfb66cd7a239f
    reviewed_at: "2026-08-07T11:25:30Z"
---

# Storage matrix final implementation inventory (P19)

**Bead**: `fireweed-7cb65c7e` (plan key P19)  
**As-of**: 2026-08-07  
**Product law**: 5×4 log × projection matrix; Turso is the public default projection.

## 1. Public matrix

| Log \ Projection | `memory` | `sqlite` | `turso` (default) | `postgres` |
|------------------|----------|----------|-------------------|------------|
| `memory` | Class B | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A | Class A |

- Sole full-matrix entry: `Fireweed::open` / `open_async(StorageConfig)`.
- Server env defaults: projection `turso` at `FIREWEED_TURSO_PROJECTION_PATH`.
- Helm defaults: `storage.log.backend=filesystem`, `storage.projection.backend=turso`.
- Response barriers: `Strict` (all cells); `AsyncProjection` on filesystem/S3 object-log cells.
- Hard-rejected public names: `objectlog`, `inmemory`, `hybrid`, `hybrid-strict`, `hybrid-async`.

## 2. Composition map

Canonical cell wire-up:
[storage-matrix-composition-inventory.md](./storage-matrix-composition-inventory.md).

Key surfaces:

| Surface | Path | Role |
|---------|------|------|
| Facade open | `crates/fireweed/src/lib.rs` | `open` / `open_async` + convenience sugar |
| Server env | `crates/fireweed-server/src/env_config.rs` | Public parse bijection; Turso default |
| Server compose | `crates/fireweed-server/src/lib.rs` | Match arms for 20 cells |
| Chart values | `charts/fireweed-queue/values.yaml` | Public axes + Turso path |
| Chart schema | `charts/fireweed-queue/values.schema.json` | Rejects hybrid/inmemory/objectlog |
| Operator contract | `docs/deployment/container-runtime-contract.md` | Runtime axes |
| Operator guide | `docs/deployment/operator-guide.md` | Helm/operator runbook |
| Preview boundary | `docs/helix/00-discover/public-preview-boundary.md` | External support claim |
| Product brief | `docs/helix/02-design/orthogonal-storage-matrix-brief.md` | Product law |
| Completion brief | `docs/helix/04-build/storage-matrix-completion-brief.md` | Zero-gap program |

## 3. Hybrid retirement and preserved historical evidence

| Artifact | Treatment |
|----------|-----------|
| Public Hybrid selectors | Retired; hard-rejected on env/Helm |
| Hybrid build plans | Marked **Superseded** under P19 |
| `docs/perf/tp002-objectlog-hybrid-evidence.md` | Immutable historical provenance companion (**do not rewrite**) |
| `docs/perf/evidence/performance_object_log_hybrid_smoke.jsonl` | Immutable historical JSONL |
| `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_{100k,1m,10m}.jsonl` | Immutable historical JSONLs |

Checksums recorded at P19 close (sha256):

```text
cce674c238f7e083991d8ed4b8236ec5e98156fc03bb930e31d535c790cd8584  docs/perf/tp002-objectlog-hybrid-evidence.md
c0339a51b783c7f148030d6413ce44fd0b0fb17a72ad93ae4a2602f673a6560f  docs/perf/evidence/performance_object_log_hybrid_smoke.jsonl
f67a6c09f3e31d1f9616d084ff4c7ea308f9001395caa820db3d7893e6ba9942  docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_100k.jsonl
c01ce88c1421bb2629b342281cdee77423d17ce48624c6a8b1fb9a8d9496215f  docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl
fdc1ee40cbcf50ae5b0f2f7b118b089adca813148e5941ada0943c33b0dcb2dc  docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_1m.jsonl
```

## 4. S3 / Garage public-identity disposition

| Path | Classification | Owner |
|------|----------------|-------|
| `README.md` | Current source prose — Garage residue removed under P19 | P19 |
| `docs/releases/v0.23.0.md`, `v0.23.1.md`, `v0.29.0.md` | Immutable published history (Garage claims) | P17a |
| `docs/releases/v0.14.0.md` | Immutable inert `.ddx` hyperlink provenance | P17a |
| `crates/fireweed-server/tests/production_s3_object_log_config.rs` | Live S3 provenance | P4s |

Current product claim: **NativeConditionalWrite** S3 authority only. Provider brands
are not product SKUs. Current site/link generation must not dereference historical
`.ddx` targets as current proof (P17a residue check remains complete).

## 5. Public site / operator surfaces (P19 exclusive ownership)

| Path | Disposition |
|------|-------------|
| `docs/site/**` | Rendered product microsite; 5×4 + Turso default |
| `scripts/site/**` | Site generators and link/provenance gates |
| Microsite gate | `bash scripts/ci/microsite-gate.sh` |
| Public example harness skip semantics | Example-only / non-governing; documented on support page |

## 6. Governing-doc semantic inspect (P19)

Inspected before stamp (content reconciled to final composition where drifted):

| Doc | Result |
|------|--------|
| TP-001 | Updated 15→20 cell language |
| TD-001 | Already 5×4 / Turso default |
| TD-003 | No public Hybrid/Turso-experimental conflict found |
| TD-004 | Hybrid sections are retired lineage; public barriers Strict/AsyncProjection |
| TD-008 | Updated 5×3→5×4 wording |
| TD-010 | Turso default; accepted |
| API-002 | No residual Hybrid product claim found |
| BUILD-001 | Hybrid “ships” claim retired under P19 |
| ADR-012 / ADR-015 / ADR-016 / ADR-017 / ADR-020 | Orthogonal composition / async / Turso / namespace — leave content; stamp as needed after edits |
| public-preview-boundary | Rewritten to 20-cell Turso default |
| storage-matrix-composition-inventory | Header + summary → 5×4 final |

## 7. Verifier commands (P19 acceptance)

```sh
git diff --check
ddx doc validate
ddx doc stale
bash scripts/ci/microsite-gate.sh
# optional health:
# ddx doctor
# python3 scripts/site/render_site.py
# python3 scripts/site/extract_examples.py
```
