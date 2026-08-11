# Commit batch amortization ladder (fireweed-110c25bc)

Host: local workstation (linux), 2026-08-11.  
Profile: `cargo test -p fireweed --features sqlite --test sqlite_commit_batch_linearity --release -- --nocapture`  
Clock: `ManualClock`. Temp file SQLite DBs under `/tmp`.

## Product bar

Until write payload approaches SSD/ZFS-aligned durable units, **ms/entry must be
monotone non-increasing** as entries/commit grows (amortization). Flat linear-in-entries
is a defect; **rising** per-entry cost is critical. Software share driven toward
~0.1 ms/entry at the amortizing plateau. Regression gates: ratio ≤1.05 (5% noise).

## Post-fix ladder — `open_sqlite` (log=sqlite × projection=memory)

Gates green. Fixed total 1024 (pow2) or 2000 (500/1000).

### finalize + lifecycle (plain)

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.147 |
| 512 | 0.039 |
| 500 | 0.031 |
| 1000 | 0.024 |

- ratio 512/64 = **0.26** · ratio 1000/500 = **0.79**

### finalize + lifecycle (unique typed index)

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.149 |
| 512 | 0.040 |
| 500 | 0.037 |
| 1000 | 0.032 |

- ratio 512/64 = **0.27** · ratio 1000/500 = **0.87**

### finalize + side_record + instance_fence

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.144 |
| 512 | 0.038 |
| 500 | 0.038 |
| 1000 | 0.027 |

- ratio 512/64 = **0.27** · ratio 1000/500 = **0.72**
- absolute @512 = **0.038 ms/entry** (bar ≤0.25; under ~0.1 software target)

### multi typed-index (≥8 indexes, fireweed-346a8d9b)

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.161 |
| 512 | 0.057 |

- ratio 512/64 = **0.35**

## `open_sqlite_relational` (unified) — bulk commit_transition

After fireweed-6e651ac5: coalesce accepted side/fence/lifecycle/finalize into O(1) command
groups per commit (was 3–4 applies/entry). Absolute cost remains higher than log×memory
(per-entry claim validate SELECTs + durable item rows); amortization is flat-to-mild.

### finalize + lifecycle (plain)

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.327 |
| 512 | 0.312 |
| 500 | 0.436 |
| 1000 | 0.418 |

### finalize + side_record + instance_fence

| entries/commit | ms/entry |
|---:|---:|
| 64 | 0.236 |
| 512 | 0.217 |
| 500 | 0.345 |
| 1000 | 0.321 |

## Interpretation

1. **Log-replay (`open_sqlite`)** meets the product contract: ms/entry falls with batch size
   through 64→512 and 500→1000; software share ~0.03–0.04 ms/entry at large batches.
2. **Regression gates** encode amortization (≤1.05), not the old 2.5× "almost linear" tolerance.
3. **Relational** is still ~10× slower absolute; bulk apply removes inverted O(command) tax for
   side+fence and large lifecycle commits but claim-validate remains O(n) SELECTs. Callers
   needing 10k+ tps should prefer `open_sqlite` (log×memory) or dual-store log-replay cells
   when projection durability mid-commit is not required; snorri re-measure tracked on
   fireweed-38213c74.

## Software vs fsync

- `open_sqlite` log: `PRAGMA synchronous=FULL` — one fsync per commit batch (amortizes).
- Probe uses ManualClock (no timer sleep). Host filesystem noise appears in wall times;
  ratios are stable across runs.

## HOLD (fireweed-6bfe48ca) — 2026-08-11

Snorri same-day ladder at main `ed311dff` (post bulk-apply): **regression** vs v0.31.2:

| workers | tps (landing) | tps (v0.31.2) | ratio |
|---:|---:|---:|---:|
| 1 | 988 | 2,069 | 0.48× |
| 4 | 2,247 | 3,395 | 0.66× |
| 8 | 1,540 | 3,692 | 0.42× |

`durable_queue_commit` wall: 124.2 s (was 37.2 s). Shape: 19 typed indexes, entity docs, ~2.3 KB payloads, 500-entry batches.

**Action:** reverse bulk-apply coalescing on relational `commit_transition` (reinstated per-entry apply). Keep amortization gates on `open_sqlite` (lean shapes still green). Do not tag v0.31.3 until snorri ladder ≥ v0.31.2 baseline at w=8.

## Snorri-shaped probe (post-revert, fireweed-6bfe48ca)

Host: local workstation, release profile, ManualClock.
Shape: 19 typed indexes (1 unique), ~2.3 KB payload on input + lifecycle entity docs.
Command: `sqlite_commit_snorri_shaped_ladder_probe`.

### open_sqlite

| entries/commit | ms/entry |
|---:|---:|
| 64 | 1.006 |
| 500 | 0.272 |
| 512 | 0.202 |

ratio 512/64 = **0.20** (amortizing). Absolute @500 ≈0.27 ms (software target ~0.1 still open).

### open_sqlite_relational

| entries/commit | ms/entry |
|---:|---:|
| 64 | 1.006 |
| 500 | 0.783 |
| 512 | 0.762 |

ratio 512/64 = **0.76**. Absolute closer to snorri's historical 0.93 ms/entry.

**Do not tag v0.31.3** until snorri w=8 ladder ≥ v0.31.2 baseline (3,692 tps) on a post-revert tip.

## Snorri-shaped regression gate (fireweed-d8ceee81)

`sqlite_commit_snorri_shaped_ladder_probe` was a print-only ladder (no assertions), so a
re-landing of relational bulk-apply coalescing on `commit_transition` would reproduce the HOLD
regression above (`durable_queue_commit` inflation at 500-entry batches, w=8 tps 3,692 → 1,540)
and still exit 0 — the same evidence gap that let fireweed-346a8d9b close on lean-shape data that
did not transfer to this shape. The probe now asserts, host-independently, over the shape above:

| open kind | ratio 500/64 must be ≤ | ratio 512/64 must be ≤ | measured band (post-revert) |
|---|---:|---:|---|
| `open_sqlite` | 1.05 | 1.05 | 0.272/1.006 ≈ 0.27, 0.202/1.006 = **0.20** |
| `open_sqlite_relational` | 1.05 | 1.05 | 0.783/1.006 ≈ 0.78, 0.762/1.006 = **0.76** |

Mechanism defended against: per-entry cost *rising* with batch size on the relational path
(bulk-apply coalescing turning `commit_transition` into fewer, larger, more expensive command
groups instead of amortizing) — the exact inversion signature snorri's real w=1/4/8 ladder caught
and this synthetic probe originally missed. `sqlite_commit_snorri_shape_rejects_batch_inversion`
feeds `assert_snorri_amortizes` a synthetic inverted ladder and asserts it panics, proving the
gate can actually fail rather than silently passing forever.

No absolute ms/entry floor is asserted here (host-dependent); absolute software-floor gates stay
on the lean `open_sqlite` shapes only (e.g. finalize+side+fence @512 ≤0.25 ms/entry).
