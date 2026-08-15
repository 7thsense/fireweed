### Findings

| Severity | Area | Finding |
|---|---|---|
| BLOCKING | I0 harness execution | I0 allows an ignored test, but the verify command omits `--ignored`, so it can pass without running the harness. It also verifies `SS_N=200` while smoke is defined as `N=10,000`. Evidence: `docs/helix/04-build/ss-phased-capacity-iteration-plan.md:42,84,95`. |
| BLOCKING | I0 workload shape | The harness does not pin stub/profile payload sizes or state that batch construction is inside the timed phase. The benchmark requires ~400-600 B ingest, ~1.0-1.5 KiB after enrich, and construction inside the clock. Evidence: `ss-phased-capacity-iteration-plan.md:87`; `seventh-sense-phased-capacity-benchmark.md:82-85,112-113`. |
| BLOCKING | Claim filter path | Phase representation is ambiguous: I0 says typed indexes include `phase`, while the benchmark says claim with `metadata_equals`; I4 assumes SS does not use `claim_by_query`. Agents can benchmark different selection paths. Evidence: `ss-phased-capacity-iteration-plan.md:86-87,151`; `seventh-sense-phased-capacity-benchmark.md:93-100`. |
| BLOCKING | Worker-loop honesty | G1-G3 do not define worker concurrency or in-flight batch policy. The model says in-flight batches and one-queue serialization dominate capacity, so single-threaded vs concurrent workers will produce incomparable results. Evidence: `ss-phased-capacity-iteration-plan.md:26,83-90`; `first-principles-performance-model.md:159-160,196-203`. |
| BLOCKING | G4 correctness | G4/I0 require counts and residual eligible, but omit sampled reads after P2/P3 proving profile blob and delivery timestamp were written. Counts can pass with a broken update body. Evidence: `ss-phased-capacity-iteration-plan.md:36,95-96`; `seventh-sense-phased-capacity-benchmark.md:199-205`. |
| BLOCKING | I1 baseline | I1 permits `N=100k` if 1M is not provisioned, but G1-G3 are explicitly `N=1,000,000` and smoke cannot satisfy them. A 100k row cannot anchor G1-G3. Evidence: `ss-phased-capacity-iteration-plan.md:26,42,105-107`. |
| BLOCKING | Measurement schema | The ladder schema records only P2/P4 claim p99, but the benchmark requires per-phase batch p50/p95/p99 and mutation/item rates. This loses the evidence needed to distinguish latency regressions from throughput wins. Evidence: `ss-phased-capacity-iteration-plan.md:186-192`; `seventh-sense-phased-capacity-benchmark.md:120,130-131,143,152,160-170`. |
| BLOCKING | I2 lazy echo | I2 says read-time echo preserves the v0.31.0 contract, but current claim rendering returns stored `entity_document` directly; if `insert_pending` stops rehydrating, compact records claim as `None`. Evidence: `ss-phased-capacity-iteration-plan.md:116`; `crates/fireweed-projection/src/lib.rs:249-266,3945-3951`. |
| BLOCKING | I2 query compatibility | Lazy echo names claim/render only, but query paths currently skip records with no stored entity. `select_claim_by_query` and range scan require `rec.entity_document.as_ref()`, so native-index-only records can become invisible. Evidence: `crates/fireweed-engine/src/index_fields.rs:353-357`; `crates/fireweed-projection/src/lib.rs:4612-4616,4716-4721`. |
| BLOCKING | Snapshot compat | I2 does not define snapshot image behavior after write-time rehydration stops. `ProjectionImageItem` persists `entity_document`, export clones it, but import currently rehydrates from `index_fields`, making export/import non-idempotent unless specified. Evidence: `ss-phased-capacity-iteration-plan.md:114-123`; `crates/fireweed-projection/src/lib.rs:117-119,139-155,172-194`. |
| BLOCKING | I3 cached keys | I3 scopes cached keys to insert/transition/remove, but key maintenance also happens in update, replace, and supersede paths. Without an invalidation matrix, cached keys can leak stale index rows. Evidence: `ss-phased-capacity-iteration-plan.md:132-139`; `crates/fireweed-projection/src/lib.rs:2484-2519,2546-2629,2791-2796,3832-3835`. |
| BLOCKING | I4 claim indexes | I4 proposes lazy `claim_indexes`, but current `select_claim_by_query` treats a missing claim index as empty. The plan needs explicit built/stale/rebuild semantics or first filtered claims can falsely return no work. Evidence: `ss-phased-capacity-iteration-plan.md:149-157`; `crates/fireweed-projection/src/lib.rs:1667-1713,4592-4594`. |
| BLOCKING | Unique indexes | The plan says unique behavior is out of scope and unique checks stay on `indexes`, but does not state cached keys are derived-only and never authoritative for precommit validation. Agents can accidentally couple stale cached keys to unique occupancy. Evidence: `ss-phased-capacity-iteration-plan.md:140,204`; `crates/fireweed-projection/src/lib.rs:4316-4337,4353-4376`. |
| BLOCKING | Stop rule | Blocked stop depends on an undefined “profile of N=10k” and “remaining time outside `ProjectionData` apply,” while the measurement schema has no profile fields and the plan says 10k does not satisfy G1-G3. Evidence: `ss-phased-capacity-iteration-plan.md:42,186-192,213`. |
| WARNING | Host bar | G1-G3 are absolute floors on an undefined “declared quiet host.” The governing model says absolute rates are topology-bound and distinguishes H-server from consumer NVMe. Evidence: `ss-phased-capacity-iteration-plan.md:26,40`; `first-principles-performance-model.md:36-39,95-115`. |
| WARNING | I5 dense fields | I5 does not define missing-field representation, declaration-order versioning, or snapshot form. Current hot record and snapshot are `BTreeMap<String, TypedValue>` shaped. Evidence: `ss-phased-capacity-iteration-plan.md:166-176`; `crates/fireweed-projection/src/lib.rs:77-81,117-119,1923-1938`. |
| NOTE | Layer choice | I2-I4 do target real current hot-path costs: insert rehydrates entity docs, inserts both index maps, and transition recomputes keys. Evidence: `ss-phased-capacity-iteration-plan.md:55-63`; `crates/fireweed-projection/src/lib.rs:1970-2012,2125-2147`. |

### Verdict: BLOCK

### Summary

The plan is not ready to file as beads because I0 and I2 leave cold agents with incompatible interpretations of the benchmark and public echo/query behavior. G1-G3 are closer to an honest Seventh Sense bar than the 13k probe, but they are still under-specified on worker concurrency, host class, and measurement fields. The highest rework risk is I2/I4: lazy entity echo and lazy claim indexes both touch public selection/query behavior, not just internal apply cost.

Required edits before beads are filed:

1. Make I0 non-ignored or fix the command with `--ignored`, and require `SS_N=10_000` smoke plus `N=1_000_000` capacity.
2. Pin payload sizes, phase field carrier, claim predicate, worker concurrency, in-flight batch policy, and timed scope.
3. Expand G4 to include sampled reads after P2/P3 and duplicate-lease detection methodology.
4. Remove `N=100k` as a G1-G3 baseline substitute; mark it calibration only.
5. Expand ladder schema to include every benchmark-required phase latency and mutation-rate column.
6. Rewrite I2 with explicit read-time echo requirements for claim, claim_by_query, range scan/query, snapshots, and import/export.
7. Rewrite I3/I4 with cache invalidation and claim-index built/stale/rebuild invariants, including unique-index source-of-truth rules.
8. Define the profiling command/output for the blocked stop rule, or allow baseline profiling to redirect before I2-I5 land.