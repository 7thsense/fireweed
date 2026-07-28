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
  scenarios/        # functional (docs + e2e)
  scenarios/perf/   # performance e2e
  scripts/start_dev_service.sh
```

## Design notes

- Scenarios stay **minimal** and **commented**; they are the primary docs.
- Transcripts in `*.log` should read as worked examples.
- Performance results are **not** universal capacity claims.
