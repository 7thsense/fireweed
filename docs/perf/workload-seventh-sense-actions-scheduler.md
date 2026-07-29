# Workload profile — Seventh Sense actions / scheduler / jobs (RESP black box)

**Status:** active profile for black-box validation  
**Interface:** Fireweed RESP worker surface only (stock Streams + `FW.*` live reads)  
**Harness:** `examples/python-resp` suite `ss`  
**Related:** TP-002 E0 (correctness / progress / resources); API-001 / TD-006 (RESP limits)

## Product intent

Seventh Sense materializes durable work-state for:

| Logical table | Role |
|---------------|------|
| **jobs** | Job registry / metadata rows (point lookup + update) |
| **actions** | Executable work units (insert, claim, complete) |
| **scheduled_actions** | Future work with due time (insert, reschedule, become claimable when due) |

Target product shape (not yet a portable release SLA): **millions of resident
rows**, **insert / mutate / query**, **sub-second** client latency on a named
topology. This profile defines a **black-box** workload that exercises the
subset of that shape available over RESP, records capacity, and enforces
correctness always.

## RESP mapping (black box)

Bootstrap three queues (tenant `ss`):

| Table | Stream key | RESP usage |
|-------|------------|------------|
| jobs | `ss:jobs` | `XADD` insert/replace by `client_item_key`; `FW.HGETALL` / `FW.MGET` point read; `XLEN` depth |
| actions | `ss:actions` | `XADD` insert; `XREADGROUP >` claim; `XACK` complete; `FW.HGETALL` status |
| scheduled_actions | `ss:scheduled` | `XADD` with `not_before` + `priority`; re-`XADD` same key to reschedule; claim when due |

### Explicitly not on RESP (out of this black box)

- Post-start queue create / change `progress_bound_ms` / `max_claim_batch_size`
- Filtered / cohort / group claim
- Finalize: fail, retry, release, rearm (library-only)
- Rich metrics (`oldest_eligible_age_ms`, failed counts, etc.)
- `request_id` replay on stock Streams commands
- Indexed `range_scan` / `query_index` (library hot projection)

Those remain Rust-facade / future RESP extensions. The black box **must not**
pretend they exist.

## Topology declaration (every run)

Evidence **must** record:

| Field | Default smoke | Full-scale intent |
|-------|---------------|-------------------|
| `log` × `projection` | memory × memory (dev) | Class A cell under test (e.g. filesystem×sqlite or postgres×postgres) |
| Listen addr | `127.0.0.1:8080` | same |
| Bootstrap queues | `ss:jobs,ss:actions,ss:scheduled` | same |
| `max_claim_batch_size` | bootstrap default **100** | raise only via product config when available |
| Resident target `SS_N` | **5_000** | **1_000_000** (or 10_000_000 when provisioned) |
| Pipeline batch | 1_000 | 1_000 |
| Claim COUNT request | 100 | 100 (capped by bootstrap) |

## Operation mix (one profile cycle)

Phases run **sequentially** on one RESP client (black box; no library API):

1. **Jobs seed** — insert `SS_N / 100` (min 50) job rows into `ss:jobs` with fields
   `job_id`, `name`, `state=open`.
2. **Scheduled seed** — insert `SS_N` rows into `ss:scheduled` with:
   - `client_item_key` = action id  
   - `job_id` field → owning job  
   - `not_before` / `priority` = past-due band so work is immediately eligible  
   - `payload` = small JSON-ish string  
3. **Point query** — `FW.HGETALL` on a fixed sample of job keys and action keys
   (correctness + latency samples).
4. **Reschedule mutate** — re-`XADD` same keys for `SS_N / 2` scheduled rows
   (pending replace); re-query a sample.
5. **Scheduler drain** — loop `XREADGROUP` on `ss:scheduled` + `XACK` until empty
   or timeout; optional copy-to-actions: for each claimed scheduled item,
   `XADD` to `ss:actions` before complete (models “fire scheduled → runnable”).
6. **Actions drain** — claim/complete remaining `ss:actions` (if any).
7. **Depth check** — `XLEN` on all three queues; assert scheduled residual = 0
   (or document intentional residual under timeout).

## Pass / fail bars

### Always (correctness) — hard

| Check | Rule |
|-------|------|
| Connect | `PING` succeeds |
| Insert counts | All `XADD` in seed phases succeed |
| Point read | Sampled keys return expected `client_item_key` / fields after insert |
| Mutate | After reschedule, sample shows updated `payload` / `not_before` |
| Drain | Claimed ids are a subset of inserted action ids; no double-complete error |
| Terminal depth | After full drain, `XLEN(ss:scheduled)` and pending-eligible work = 0 |
| No crash | Service still responds to `PING` and `XLEN` |

### Latency (capacity + optional SLO)

Latency samples are **client wall time** for RESP round-trips.

| Metric | Smoke default (`SS_N=5000`) | Strict (`SS_STRICT=1`) |
|--------|----------------------------|-------------------------|
| `FW.HGETALL` p95 | **report**; soft fail if p95 ≥ **1000 ms** | p95 &lt; **100 ms** |
| Pipeline `XADD` batch (1000) p95 | **report**; soft fail if p95 ≥ **1000 ms** | p95 &lt; **500 ms** |
| `XREADGROUP`+`XACK` chunk p95 | **report**; soft fail if p95 ≥ **1000 ms** | p95 &lt; **500 ms** |

Soft fail = test **fails** in default profile so regressions are visible, but
evidence JSON always records raw percentiles for TP-002-style capacity
publishing. Strict mode is for a named topology only.

**Sub-second product goal:** default soft bars encode “sub-second on this
black-box path under smoke load.” They are **not** a substitute for TP-002 E1/E2
release stamps on Class A storage.

### Progress proxy (RESP limitation)

Server `progress_bound_ms` is not readable over stock RESP. The black box
records **accepted→claim wall time** for the first successfully claimed item
after scheduled seed completes as a client-side progress proxy, and reports
whether the full drain finished within `SS_DRAIN_TIMEOUT_S` (default 120s
smoke / 3600s full).

## Evidence artifact

Directory: `target/python-resp-ss/<UTC>/`

| File | Content |
|------|---------|
| `summary.json` | Topology env, `SS_N`, phase timings, percentiles, pass/fail |
| `SS01_black_box.json` / `.log` | Scenario detail + transcript |

## Commands

```sh
# Terminal A — three bootstrap queues (from repo root)
FIREWEED_BOOTSTRAP_QUEUES=ss:jobs,ss:actions,ss:scheduled \
  ./examples/python-resp/scripts/start_ss_service.sh

# Terminal B — smoke black box (sub-second soft SLOs)
cd examples/python-resp
SS_N=5000 python run_e2e.py --suite ss

# Larger capacity observation (still RESP black box)
SS_N=100000 python run_e2e.py --suite ss

# Strict latency (only on a known quiet topology)
SS_STRICT=1 SS_N=5000 python run_e2e.py --suite ss
```

## Relationship to TP-002

| TP-002 concern | This profile |
|----------------|--------------|
| E0 correctness / drain exactness | Hard asserts |
| E0 progress_bound | Client drain timeout + first-claim latency proxy only |
| E1 10M single-deployment | Optional-only; not this suite |
| E2 density (1000 queues) | Three queues only; not density |
| E3 recovery 10M | Not this suite |
| Capacity p50/p95/p99 | Recorded in evidence JSON |

## Exit criteria for “profile implemented”

1. Doc checked in (this file).  
2. `examples/python-resp` suite `ss` runs against bootstrap queues above.  
3. Smoke `SS_N=5000` green on in-memory service.  
4. README points operators at the suite and RESP gaps.
