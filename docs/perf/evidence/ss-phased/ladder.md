# SS phased capacity ladder

Same-host before/after only. G-gates require N=1,000,000. N=10k/100k are not comparable to G-rows.

Host for rows below: WSL2 `sindri`, AMD Ryzen 9 5950X 16C/32T, 94 GiB RAM, workspace on `/dev/sdd` ext4 virtual disk (~39 GiB free, `df` 100%). **Not** H-server NVMe+PLP. loadavg during baseline ~4.8–5.8. rustc 1.97.1.

| utc | sha | N | note | p1_items_s | p2_items_s | p3_items_s | p4_items_s | wall_s | p1_p99_ms | p2_p99_ms | p3_p99_ms | p4_claim_p99_ms | residual |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-15 | 7faa3588 | 10000 | smoke | 21020 | 15317 | 21808 | 14550 | 2.28 | 25.22 | 22.73 | 15.58 | 10.26 | 0 |
| 2026-08-15 | 7faa3588 | 1000000 | baseline (run 1; WSL virt disk) | 12188 | 7721 | 8365 | 8514 | 448.6 | 41.59 | 78.64 | 76.39 | 12.42 | 0 |

Gates (H-server): G4 P1≥80k, G2 P2≥40k, G3 P3≥40k, G1 P4≥50k. This host’s N=1M row is **below all four**. Long pole: P2 then P3/P4.

Evidence: `docs/perf/evidence/ss-phased/1786755686/summary.json`.
