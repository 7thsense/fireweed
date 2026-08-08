# Million-cycle production evidence (TP-005)

- tier: production (1M insert / 500k modify / 1M read+verify)
- cells_passed: 7 / 20 matrix
- freeze S: `23bb355043c2d7c0bc2e28c6491592aecc75e841`

## Results

| cell | status | insert | modify | read | reopen |
|---|---|---:|---:|---:|---|
| filesystem--memory | PASS | 63.5s | 38.5s | 0.8s | true |
| filesystem--sqlite | PASS | 62.4s | 40.2s | 0.9s | true |
| memory--memory | PASS (prior) | 5.0s | 1.8s | 0.7s | true |
| memory--postgres | PASS | 101.5s | 853.9s | 7.5s | true |
| memory--sqlite | PASS (prior) | 19.0s | 139.0s | 2.3s | true |
| sqlite--memory | PASS | 27.8s | 14.9s | 0.6s | true |
| sqlite--sqlite | PASS | 32.0s | 164.9s | 2.5s | true |

## Residual / not yet archived

- `filesystem--postgres`
- `filesystem--turso`
- `memory--turso`
- `postgres--memory`
- `postgres--postgres`
- `postgres--sqlite`
- `postgres--turso`
- `s3--memory`
- `s3--postgres`
- `s3--sqlite`
- `s3--turso`
- `sqlite--postgres`
- `sqlite--turso`

JSON under this directory.
