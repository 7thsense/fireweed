# P18 storage evidence promotion

| Field | Value |
|---|---|
| S | `23bb355043c2d7c0bc2e28c6491592aecc75e841` |
| E | `a57e85b3373163bbf1049fb49dd932cb459a1629` |
| source_ref | `refs/heads/release-source/v0.30.1` |
| campaign | storage |
| allowlist | `docs/evidence/storage-closure/p18-storage-allowlist.json` |
| promoter | `scripts/release/promote-governed-evidence.sh` |
| promoted_at_utc | 2026-08-08T09:53:20Z |

## Proofs

- Parent of E is S: `23bb355043c2d7c0bc2e28c6491592aecc75e841`
- E message records Measured-source / Source-ref / Campaign=storage
- Dual-root reader: source tooling at S; promoted evidence only from E allowlist

## Commands

```sh
bash scripts/release/promote-governed-evidence.sh \
  --source-root <S-checkout> \
  --expected-source 23bb355043c2d7c0bc2e28c6491592aecc75e841 \
  --expected-remote origin \
  --expected-ref refs/heads/release-source/v0.30.1 \
  --campaign storage \
  --bundle-root <external-bundle> \
  --allowlist docs/evidence/storage-closure/p18-storage-allowlist.json \
  --promotion-root <empty-external-dir>
```
