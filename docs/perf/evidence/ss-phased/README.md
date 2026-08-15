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
