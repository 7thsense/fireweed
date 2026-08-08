# Million-cycle production evidence (TP-005)

- tier: production (1M insert / 500k modify / 1M read+verify)
- cells_passed_with_json: 15 / 20
- freeze S: `23bb355043c2d7c0bc2e28c6491592aecc75e841`

## Results

| cell | insert | modify | read | reopen | artifact |
|---|---:|---:|---:|---|---|
| memory--memory | 5.0s | 1.8s | 0.7s | True | `prior-partial` |
| memory--sqlite | 19.0s | 139.0s | 2.3s | True | `prior-partial` |
| memory--postgres | 101.5s | 853.9s | 7.5s | True | `memory-postgres-production-20260808T080901Z.json` |
| memory--turso | — | — | — | — | residual |
| sqlite--memory | 27.8s | 14.9s | 0.6s | True | `sqlite-memory-production-20260808T080011Z.json` |
| sqlite--sqlite | 32.0s | 164.9s | 2.5s | True | `sqlite-sqlite-production-20260808T080105Z.json` |
| sqlite--postgres | 88.5s | 751.4s | 7.5s | True | `sqlite-postgres-production-20260808T082605Z.json` |
| sqlite--turso | — | — | — | — | residual |
| filesystem--memory | 63.5s | 38.5s | 0.8s | True | `filesystem-memory-production-20260808T080441Z.json` |
| filesystem--sqlite | 62.4s | 40.2s | 0.9s | True | `filesystem-sqlite-production-20260808T080651Z.json` |
| filesystem--postgres | 106.3s | 780.2s | 0.6s | True | `filesystem-postgres-production-20260808T084107Z.json` |
| filesystem--turso | — | — | — | — | residual |
| postgres--memory | 14.4s | 12.0s | 1.0s | True | `postgres-memory-production-20260808T085706Z.json` |
| postgres--sqlite | 26.2s | 138.4s | 2.3s | True | `postgres-sqlite-production-20260808T085746Z.json` |
| postgres--postgres | 82.9s | 47.5s | 7.5s | True | `postgres-postgres-production-20260808T090047Z.json` |
| postgres--turso | — | — | — | — | residual |
| s3--memory | 49.9s | 23.2s | 0.6s | True | `s3-memory-production-20260808T090317Z.json` |
| s3--sqlite | 69.6s | 49.4s | 0.8s | True | `s3-sqlite-production-20260808T090539Z.json` |
| s3--postgres | 135.8s | 856.6s | 0.6s | True | `s3-postgres-production-20260808T090849Z.json` |
| s3--turso | — | — | — | — | residual |

Passed: 15. Residual: 5 (mostly Turso if non-turso complete).

