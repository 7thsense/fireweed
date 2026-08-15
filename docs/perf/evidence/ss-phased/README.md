# SS phased capacity evidence

Harness: `crates/fireweed/tests/ss_phased_capacity.rs`  
Plan: `docs/helix/04-build/ss-phased-capacity-iteration-plan.md`

Two log axes. **Do not mix rows across tables in `ladder.md`.**

## Production log axis — object log (`filesystem--memory`)

Filesystem object log (same protocol as S3) × in-memory projection. `open_objectlog`.
This is the production deployment log. It is not a SQLite command log.

```sh
# smoke
SS_CELL=objectlog SS_N=10000 cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture

# declared-host capacity
SS_CELL=objectlog SS_N=1000000 SS_PUSH_BATCH=1000 SS_CLAIM_BATCH=1000 SS_EVIDENCE=1 \
  cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

`SS_SQLITE_SYNC` is rejected on this cell.

## Calibration only — SQLite command log (`sqlite--memory`)

Not the production log. Do not quote these rates as object-storage capacity.

```sh
SS_CELL=sqlite SS_N=10000 cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture

SS_CELL=sqlite SS_SQLITE_SYNC=off SS_N=1000000 SS_PUSH_BATCH=1000 SS_CLAIM_BATCH=1000 \
  cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture
```

`ladder.md` is the same-host before/after log. Do not compare N=10k or N=100k rows to G1–G5.
`SS_LOG_DIR` is the parent directory for either cell's scratch path.
