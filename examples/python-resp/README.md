# Python RESP examples and e2e

Minimal, heavily commented **queue-management** scenarios for Fireweed’s RESP
worker surface (`redis-py` → `fireweed-service`).

Each scenario is both:

1. **Documentation** — intent, wire fields, and RESP limits in the module docstring.
2. **E2E test** — hard assertions with captured logs and JSON evidence.

A separate **performance** suite measures large pipelined insert, bulk pending
update, chunked claim/complete, and status latency. Numbers are
**host- and profile-bound**, not product SLAs.

## Prerequisites

- Fireweed built with the repo toolchain (`cargo build -p fireweed-server`)
- Python 3.10+
- `redis` package (`pip install -r requirements.txt`)

## Start a local service

```sh
# from repository root
./examples/python-resp/scripts/start_dev_service.sh
```

This starts an in-memory service with bootstrap queue **`demo:work`**:

```text
FIREWEED_LISTEN_ADDR=127.0.0.1:8080
FIREWEED_LOG_BACKEND=memory
FIREWEED_PROJECTION_BACKEND=inmemory
FIREWEED_BOOTSTRAP_QUEUES=demo:work
```

## Run functional e2e

```sh
cd examples/python-resp
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python run_e2e.py
```

Evidence: `target/python-resp-e2e/<timestamp>/` (repo root).

| ID | Title |
|----|--------|
| `01_connect` | Connect and address a queue |
| `02_batch_insert` | Batch insert work items |
| `03_batch_update_pending` | Update pending items (reschedule) |
| `04_claim_before_due` | Empty claim before eligibility |
| `05_claim_due_batch` | Claim due batch in priority order |
| `06_complete_and_status` | Complete work and read status |
| `07_idempotent_upsert` | Idempotent pending upsert |
| `08_lease_renew` | Renew a held lease |
| `09_lease_reclaim` | Reclaim after expiry (`--full`, ~60s) |

```sh
python run_e2e.py --scenario 05_claim_due_batch
python run_e2e.py --full   # includes lease reclaim wait
```

## Seventh Sense black box (actions / scheduler / jobs)

Profile: [`docs/perf/workload-seventh-sense-actions-scheduler.md`](../../docs/perf/workload-seventh-sense-actions-scheduler.md)

Three bootstrap queues model the product tables over RESP only:

```sh
# Terminal A
./examples/python-resp/scripts/start_ss_service.sh

# Terminal B — smoke (default SS_N=5000, sub-second soft latency bars)
cd examples/python-resp
SS_N=5000 python run_e2e.py --suite ss
```

| Queue | Table |
|-------|--------|
| `ss:jobs` | jobs registry |
| `ss:actions` | executable actions |
| `ss:scheduled` | scheduled_actions (due / reschedule) |

| ID | Title |
|----|--------|
| `SS01_black_box` | Multi-queue jobs/scheduled/actions smoke + soft latency bars |
| `SS02_lifecycle` | **Parameterized lifecycle framework** (same loop for any scale) |

Evidence: `target/python-resp-ss/<timestamp>/`.

### Lifecycle framework (`lib/workflow.py`)

Reusable RESP-only loop — **identical shape** for smoke and scale demos:

```text
for cycle in 1..y:
    insert N records
    mutate floor(N / x) of them
    get status
then:
    claim batches of size Z, complete + status, until queue empty
```

| Knob | Env | Meaning |
|------|-----|---------|
| **N** | `WF_N` / `SS_N` | Inserts per cycle |
| **x** | `WF_MUTATE_DIVISOR` / `SS_X` | Mutate `N/x` keys (default `2` → half) |
| **y** | `WF_CYCLES` / `SS_Y` | Insert/mutate/status iterations |
| **Z** | `WF_CLAIM_CHUNK` / `SS_Z` | Claim chunk size (bootstrap often caps at **100**) |
| pipeline | `WF_PIPELINE` | XADD pipeline batch |
| queue | `WF_QUEUE` | Stream key (SS02 default `ss:actions`) |

```sh
# Smoke: 5k insert, mutate half, 1 cycle, drain Z=100
WF_N=5000 WF_MUTATE_DIVISOR=2 WF_CYCLES=1 WF_CLAIM_CHUNK=100 \
  python run_e2e.py --suite ss --scenario SS02_lifecycle

# Multi-cycle demo: 2k × 3 cycles, mutate 1/4, mid-drain status every 5 chunks
WF_N=2000 SS_X=4 SS_Y=3 SS_Z=100 WF_STATUS_EVERY_CHUNKS=5 \
  python run_e2e.py --suite ss --scenario SS02_lifecycle

# Scale observation (still the same workflow)
WF_N=100000 WF_CYCLES=1 WF_CLAIM_CHUNK=100 \
  python run_e2e.py --suite ss --scenario SS02_lifecycle
```

Primitives (`insert_records`, `mutate_records`, `status_snapshot`, `drain_queue`,
`run_lifecycle`) live in `lib/workflow.py` so perf/SS scenarios can share one path.
## Run performance e2e

```sh
# smoke (recommended first)
PERF_N=10000 python run_perf.py

# full-scale (needs RAM; start a fresh service for clean XLEN)
PERF_N=1000000 python run_perf.py
```

| ID | Intent |
|----|--------|
| `P01_insert_1m` | Pipelined bulk insert (`PERF_N`, default 1e6) |
| `P02_update_half` | Pending replace on ~N/2 keys |
| `P03_claim_complete_chunks` | Claim/complete loops (`PERF_CLAIM_COUNT`, often capped at **100** by bootstrap) |
| `P04_status_under_load` | `XLEN` / `XINFO` latency with a large live set |
| `P05_end_to_end_pipeline` | Insert → update half → status → drain rollup |

Evidence: `target/python-resp-perf/<timestamp>/`.

### Claim chunk size

Bootstrap queues use `max_claim_batch_size = 100`. Requesting `COUNT 1000` is
capped by the server. Evidence records `claim_count_requested` vs
`claim_count_effective_max`.

## Queue management → RESP

| Step | RESP |
|------|------|
| Batch insert | Pipeline `XADD` |
| Pending update / reschedule | Re-`XADD` same `client_item_key` |
| Next due batch | `XREADGROUP … COUNT n … >` (priority + `not_before`) |
| Complete | `XACK` (complete only) |
| Status | `XLEN`, `XINFO STREAM`, `XPENDING` |
| Live read by key | `FW.HGETALL` / `FW.MGET` / `FW.HMGET` |
| Renew lease | `XCLAIM` with `consumer = lease_token` |
| Reclaim | `XAUTOCLAIM` after lease expiry |

### Not on RESP (use the Rust facade)

- Queue create/configure after start  
- Filtered / group / cohort claim  
- Finalize: fail, retry, release, rearm  
- Rich metrics (`pending`/`complete`/`failed`/`oldest_eligible_age_ms`)  
- `request_id` replay on stock Streams commands  

## Layout

```text
examples/python-resp/
  run_e2e.py / run_perf.py
  harness/          # runner, capture, context
  lib/resp.py       # thin helpers (not a full SDK)
  lib/workflow.py   # parameterized insert/mutate/status × y + drain Z
  scenarios/        # functional (docs + e2e)
  scenarios/perf/   # performance e2e
  scenarios/ss/     # Seventh Sense black box + lifecycle
  scripts/start_dev_service.sh
```

## Design notes

- Scenarios stay **minimal** and **commented**; they are the primary docs.
- Transcripts in `*.log` should read as worked examples.
- Performance results are **not** universal capacity claims.
