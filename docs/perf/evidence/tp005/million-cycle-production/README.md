# Million-cycle production evidence (TP-005)

- tier: production (1M insert / 500k modify / 1M read+verify)
- archived_at_utc: 2026-08-08T09:37:40Z
- HEAD: `3ab9eff853f1c03f6454c4ca49360929a590acbb`
- freeze S: `23bb355043c2d7c0bc2e28c6491592aecc75e841`

## Results

| cell | status | insert | modify | read | reopen |
|---|---|---:|---:|---:|---|
| memory--memory | PASS (prior) | 5.0s | 1.8s | 0.7s | true |
| memory--sqlite | PASS (prior) | 19.0s | 139.0s | 2.3s | true |
| sqlite--memory | PASS | 27.8s | 14.9s | 0.6s | true |
| sqlite--sqlite | PASS | 32.0s | 164.9s | 2.5s | true |
| filesystem--memory | PASS | 63.5s | 38.5s | 0.8s | true |
| filesystem--sqlite | PASS | 62.4s | 40.2s | 0.9s | true |
| memory--postgres | PASS | 101.5s | 853.9s | 7.5s | true |

## Residual

- Turso projection cells (memory|sqlite|filesystem|postgres|s3 × turso) still running or deferred:
  Turso 1M-item cycles are physics-slow on this host (earlier memory--turso >25m without completion).
- JSON artifacts: `docs/perf/evidence/tp005/million-cycle-production/*.json`
