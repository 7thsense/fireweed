# TP-002 — object-log granularity PUT and payload-storage sensitivity

This document is GENERATED from explicit workload assumptions. It is modelled sensitivity, not measured release evidence. Regenerate it with:

```
cargo run -p fireweed-release --bin fireweed-cost-model -- --granularity-only --out docs/perf/tp002-objectlog-granularity-economics.md
```

## Workload-driven object granularity

This table is **fixed-batch, regular-arrival sensitivity**, not measured release evidence or a universal prediction. It models the real downstream primitive explicitly: `commands/segment = min(batch * ceil(target bytes / batch bytes), batch * ceil(commands arriving inside latency bound / batch))`. This admits target overshoot by a whole downstream batch and assumes a due flush wins ties at the exact deadline. Real arrival and batch distributions must come from E3 counters. The production defaults are `FIREWEED_SEGMENT_TARGET_BYTES=262144` and `FIREWEED_SEGMENT_MAX_LATENCY_MS=20`. Steady successful non-genesis PUT amplification is derived from the current authority-head algorithm: segment + manifest candidate + versioned head + one copy-on-write node per recovery-index level + one retirement marker, or `5 + resulting index height` on an ordinary post-genesis append. A root-height transition reuses the old root and omits that retirement marker. The calculator integrates fanout-64 height transitions from each scenario's starting lifetime entry count across all per-queue seals in the billing window; it does not hold height constant. Queue initialization, fences, retries, and maintenance remain measured-only terms. Storage bytes are uncompressed command payload and exclude framing and metadata overhead; measured E3 primitive and byte counters remain authoritative for releases. Queue count is explicit because independent queues cannot share a segment; fleet request and byte totals are the per-queue shape multiplied by active queues.

| Scenario | active queues | cmd/s/queue | input batch | encoded bytes/cmd | target | bound | starting index entries | ending height | avg PUT/seal | trigger | cmd/segment | mean segment | fill | PUT requests/month | PUT $/month | PUT $/B commands | ingress GB/month | retained payload 24h GB | payload storage $/month |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| default, low-rate scalar input | 1 | 10 | 1 | 1024 | 262144 | 20 ms | 0 | 4 | 8.35 | latency | 1 | 1024 B | 0.4% | 219476475 | $1097.38 | $41757.32 | 26.9 | 0.9 | $0.02 |
| PRD density: 1000 queues at 10 cmd/s each | 1000 | 10 | 1 | 1024 | 262144 | 20 ms | 0 | 4 | 8.35 | latency | 1 | 1024 B | 0.4% | 219476475000 | $1097382.38 | $41757.32 | 26910.7 | 884.7 | $20.35 |
| default, sustained; 100-command downstream batches | 1 | 1000 | 100 | 1024 | 262144 | 20 ms | 0 | 4 | 8.35 | latency | 100 | 102400 B | 39.1% | 219476475 | $1097.38 | $417.57 | 2691.1 | 88.5 | $2.03 |
| default, hot; 1000-command downstream batches | 1 | 20000 | 1000 | 1024 | 262144 | 20 ms | 0 | 4 | 8.68 | size | 1000 | 1024000 B | 390.6% | 455996475 | $2279.98 | $43.38 | 53821.4 | 1769.5 | $40.70 |
| default, 16 KiB commands; 100-command batches | 1 | 1000 | 100 | 16384 | 262144 | 20 ms | 0 | 4 | 8.35 | size | 100 | 1638400 B | 625.0% | 219476475 | $1097.38 | $417.57 | 43057.2 | 1415.6 | $32.56 |
| 100 ms bound; 100-command batches | 1 | 1000 | 100 | 1024 | 262144 | 100 ms | 0 | 4 | 8.35 | latency | 100 | 102400 B | 39.1% | 219476475 | $1097.38 | $417.57 | 2691.1 | 88.5 | $2.03 |
| 8 MiB target; 1000-command batches | 1 | 20000 | 1000 | 1024 | 8388608 | 100 ms | 0 | 4 | 8.35 | latency | 2000 | 2048000 B | 24.4% | 219476475 | $1097.38 | $20.88 | 53821.4 | 1769.5 | $40.70 |
| hot scalar input; fresh queue | 1 | 20000 | 1 | 1024 | 262144 | 20 ms | 0 | 4 | 8.92 | size | 256 | 262144 B | 100.0% | 1830768975 | $9153.84 | $174.16 | 53821.4 | 1769.5 | $40.70 |
| hot scalar input; aged queue | 1 | 20000 | 1 | 1024 | 262144 | 20 ms | 16777216 | 4 | 9.00 | size | 256 | 262144 B | 100.0% | 1847812499 | $9239.06 | $175.78 | 53821.4 | 1769.5 | $40.70 |

**Interpretation:** granularity optimization means allowing more commands to share each segment object while respecting the operator-selected commit-latency bound. At low arrival rates the latency bound correctly wins and may produce one-command segments; the table makes that cost visible instead of pretending every queue fills its byte target. At high rates or with larger commands, the byte target wins. A large downstream primitive may overshoot the soft byte target; that is visible rather than hidden. Changing the target, bound, or downstream batch is an economic/latency decision, never a durability change. The full TP-002 E3 cost model—not this sensitivity table—adds measured metadata bytes, retries, GET/LIST/DELETE, recovery, and compute.

## Price provenance

- ADR-001 Napkin Cost Comparison, US-East-1: AWS S3 pricing (AmazonS3 offer file pub. 2026-05-28); Aurora PostgreSQL db.r7g.large standard $0.276/hr + $0.10/GB-mo storage (AmazonRDS offer file pub. 2026-06-05); EC2 i4i.large $0.172/hr (AmazonEC2 offer file pub. 2026-06-04)
- AWS EBS io2 provisioned-IOPS first tier $0.065/IOPS-month (AWS EBS pricing page, accessed 2026-06-29) — NOT cited by ADR-001; stated as the one non-ADR price input

