# P6s — Provider-neutral Snorri S3 durability acceptance

| Field | Value |
| --- | --- |
| Plan key | `P6s` |
| Bead | `fireweed-2886078a` |
| Capability ID | `SNORRI-S3-DURABILITY-ACCEPTANCE` |
| Machine-readable attestation | [`p6s-s3-durability-attestation.json`](./p6s-s3-durability-attestation.json) |
| Ledger fixture | `scripts/ci/fixtures/snorri/p6s-s3-durability.json` |

## What this proves

TP-004 live S3 semantic IDs on a P1s-attested provider-neutral endpoint (not Garage/`eldir` as implicit provisioning):

| ID | Cells | Evidence |
| --- | --- | --- |
| `SNORRI-REOPEN` | `s3--memory`, `s3--sqlite`, `s3--postgres` | Class A round-trip reopen |
| `SNORRI-PROJECTION-REBUILD` | `s3--sqlite`, `s3--postgres` | `projection_control` verify/delete/rebuild; same item/`request_id` image |
| `SNORRI-RETRY-ONCE` | all three S3 projection rows | `push_with_request_id` Fresh → Replayed; conflict fails; survives reopen |

Unsupported negative retained: `s3--memory` has no disposable `projection_control`.

## Execute

```bash
# Requires P1s secrets under /tmp/fireweed-s3-secrets and isolated Postgres.
bash scripts/ci/snorri-s3-durability-acceptance.sh

# Fireweed harness only:
P6S_SKIP_SNORRI=1 bash scripts/ci/snorri-s3-durability-acceptance.sh

# External Snorri re-run (after provider-neutral migration landed):
SNORRI_CHECKOUT=/path/to/snorri bash scripts/ci/snorri-s3-durability-acceptance.sh
# or directly:
eval "$(bash scripts/ci/snorri-runner-preflight.sh --export-env)"
cd /path/to/snorri && bash scripts/test-s3-live-mutations.sh
```

## Pinning

Snorri path/git pin for release acceptance should reference the Fireweed SHA recorded in the attestation (`fireweed_sha`) and the Snorri commit that migrated `SNORRI_S3_*` fixtures (`snorri.sha`).
