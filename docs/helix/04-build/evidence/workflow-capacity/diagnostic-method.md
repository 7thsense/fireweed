# Fireweed performance review experiments

Review of checkout 1c8a2c4f on 2026-09-09. No tracked files changed.

Turso CLI reports 0.7.2, matching Cargo.lock. Binary downloaded from the upstream v0.7.2 GitHub release, Linux x86_64 archive. Tests here are diagnostic, not deployment capacity evidence.

SQL fixtures use the exact RELATIONAL_SCHEMA from crates/fireweed-relational/src/schema.rs. Seeded through Python sqlite3 with tenant t, queue q, ascending text item IDs, keys key-0000001 onward, Pending state, priority_sort x'00', item_version 1, max_attempts 3, created_seq equal to numeric ID, other required numeric fields zero. Optional payload/metadata remain empty/default. Turso executes the measured updates. Temporary database files live on /tmp (tmpfs). Some measurements overlapped a Rust build; no absolute hardware throughput claim is intended.

query-plans.json includes SQL and observed Turso plans. SQLite's planner on the same 100k fixture chooses full client-key seeks for uniform_update and varied_update; Turso chooses the pending-group index constrained only by tenant/queue.

ladder.json and *-timing.txt record 5 repetitions of a 100-row update within one transaction at 10k and 1M rows, with synchronous=OFF and cache_size=-131072. Current versus forced_key differ only by INDEXED BY fireweed_items_active_key. CLI wall includes startup/close. The 100k control used 20 repetitions, repeated twice: current wall 1.8231/1.8404s; forced-key wall 0.0235/0.0222s. This repeatedly touches the same narrow key range and does not model random updates, payload enrichment, object-log durability, or the facade. Forcing the key index without a narrow range still gives a tenant/queue-only scan for IN; it is not a general fix.

rust-profile-result.json records the existing release-mode Rust qualification test failure and phase counters. This is independent evidence that SQL-write cost, rather than transaction commit, dominates that test.
