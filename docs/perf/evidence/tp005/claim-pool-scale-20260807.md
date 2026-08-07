# fireweed-66d64e91 — claim pool scale evidence

- host: 32 logical CPUs, ~94 GiB RAM
- live Postgres: `127.0.0.1:55432` (`FIREWEED_PG_TEST_URL`)
- corpus: 4_000 pending items, claim batch 32
- binary: `cargo test -p fireweed-postgres --test claim_pool_scale`

## Result (same-queue multi-writer)

After removing the long-held `relational_cursor … FOR UPDATE` from the item-level claim
path (lease under `FOR UPDATE SKIP LOCKED` first, then CAS-allocate `next_seq`):

| posture | workers | claim_pool | wall ms | items/s |
|---|---:|---:|---:|---:|
| single connection | 1 | 0 | 5158 | 775 |
| claim pool | 4 | 4 | 2012 | 1988 |

**Speedup: 2.56×** (bar was ≥1.25×). Correctness suite `claim_pool_contended` remains green
(partition under SKIP LOCKED, every item claimed exactly once).

## API

- `PostgresRelationalBackend::connect_*_with_claim_pool`
- `PostgresRuntimeConfig::claim_pool_size` / `StorageConfig` wiring via open path
- `FOR UPDATE SKIP LOCKED` claim CTE unchanged; cursor is no longer locked for the whole claim

## Residual

Snorri end-to-end remeasure at the consumer pin remains external validation (recorded by snorri).
