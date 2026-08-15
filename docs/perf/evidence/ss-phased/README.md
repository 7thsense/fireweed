# SS phased capacity evidence

Harness: `crates/fireweed/tests/ss_phased_capacity.rs`  
Plan: `docs/helix/04-build/ss-phased-capacity-iteration-plan.md`

```sh
# smoke (CI / slice ratchet)
SS_N=10000 cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture

# declared-host capacity (I1 / G-gates)
SS_N=1000000 SS_EVIDENCE=1 cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture
```

`ladder.md` is the same-host before/after log. Do not compare N=10k or N=100k rows to G1–G5.

SQLite log sync (default `full` = Class A fsync-per-commit):

```sh
# process-crash usually OK, power-loss may lose the WAL tail
SS_N=1000000 SS_PUSH_BATCH=1000 SS_CLAIM_BATCH=1000 SS_SQLITE_SYNC=off \
  cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture

# middle setting
SS_SQLITE_SYNC=normal ...
```

`SS_LOG_DIR=/dev/shm` isolates CPU from disk. `SS_SQLITE_SYNC=off` on a real path is the on-disk throughput cell.
