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
