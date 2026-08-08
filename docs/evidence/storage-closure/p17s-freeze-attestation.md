# P17s candidate-source freeze attestation

## Identity

| Field | Value |
|---|---|
| V | 0.30.1 |
| S | `23bb355043c2d7c0bc2e28c6491592aecc75e841` |
| source_ref | `refs/heads/release-source/v0.30.1` |
| remote | origin (`https://github.com/7thsense/fireweed.git`) |
| frozen_at_utc | 2026-08-08T07:46:09Z |

## Equality proof

- local HEAD = `23bb355043c2d7c0bc2e28c6491592aecc75e841`
- local `refs/heads/release-source/v0.30.1` = `23bb355043c2d7c0bc2e28c6491592aecc75e841`
- remote `refs/heads/release-source/v0.30.1` = `23bb355043c2d7c0bc2e28c6491592aecc75e841`

## Source predicate

```
ddx_inventory_root=/home/erik/Projects/fireweed
verify-source-predicate: ddx_tracked_count=1732 ddx_untracked_count=0
tracked_ignore_ok admin_untracked=5011 build_untracked=346112
local_global_exclude_masking_has_no_authority=true
admin_roots_ok roots=.ddx/ proofs=tracked_rule,contained_path,no_source_authority,no_evidence_authority,no_s_bound_reader
build_cache_roots_ok roots=target/,scripts/site/node_modules/,__pycache__/,examples/python-resp/.venv/ proofs=tracked_rule,untracked_or_symlink_contained,tracked_lock_and_config_identity,no_credentials,no_governing_input,no_promoted_evidence_authority
no_s_bound_reader_ok administrative=.ddx/
verify-source-predicate: ok mode=source source=23bb355043c2d7c0bc2e28c6491592aecc75e841 ref=refs/heads/release-source/v0.30.1
```

## Operator inventory (non-product)

- `.ddx/**` tracked blob name-set digest (operator-local, not product authority): `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- administrative roots: `.ddx/` (no-reader proof in predicate log)
- build/cache roots: `target/`, `scripts/site/node_modules/`, `__pycache__/`, `examples/python-resp/.venv/`
- Local/global excludes have no policy authority (predicate: `local_global_exclude_masking_has_no_authority=true`)

## Commands

```sh
git rev-parse HEAD
git rev-parse refs/heads/release-source/v0.30.1
git ls-remote origin refs/heads/release-source/v0.30.1
bash scripts/release/verify-source-predicate.sh --mode source \
  --source-root . --expected-source 23bb355043c2d7c0bc2e28c6491592aecc75e841 \
  --expected-remote origin --expected-ref refs/heads/release-source/v0.30.1
bash scripts/release/verify-release-identity.sh --version 0.30.1
```

## Non-goals

- Did not create tag `v0.30.1` (reserved until product-ready cut)
- Did not promote evidence E
- Storage campaign continues at P17 class regeneration against frozen S
