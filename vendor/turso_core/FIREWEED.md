# Fireweed checkpoint-control backport

This is the published `turso_core 0.7.2` source, with the limited patch in
[FIREWEED.patch](FIREWEED.patch). Original crate checksum:
`7a833cc3bf8d4e6c101c504fa470f8ab4270c2202ff2591b61b2e373b4f20d9b`.
Upstream source revision: `046e9cbf67d22491e8ecc941ec2891b02a9f3cad` (`core/`).
The upstream MIT license is included in [LICENSE.md](LICENSE.md).

The backport implements setting and querying `PRAGMA wal_autocheckpoint` for the
main database's ordinary native WAL on each connection. Its default remains 1,000 frames, and each connection owns
its limit. Zero (including a negative input normalized to zero) disables automatic
checkpoints; explicit and shutdown checkpoints remain available. Unsupported WAL
implementations fail explicitly when configuration is requested. Native lock, synchronization, safe-frame backfill and WAL restart semantics are
preserved. The destination-write ordering optimization is described below.

The automatic trigger counts total retained WAL frames, including already
backfilled frames, and fires at or above the configured limit. Subtracting
backfilled frames lets a reader-forced partial checkpoint defer its retry for
another full budget while the WAL keeps growing. Retrying passive checkpoints
after subsequent commits lets a released reader's remaining frames be backfilled
and the next writer restart the WAL. This follows the documented
[SQLite auto-checkpoint trigger](https://www.sqlite.org/c3ref/wal_autocheckpoint.html).
The native regression holds a real reader snapshot through partial backfill,
checks its contents, and verifies automatic catch-up and restart after release.

The SQL handler and pragma-list entry are implemented in the core translator to
avoid also forking the parser's public enum. Table-valued pragma syntax is not
added by this patch. Changing the limit invalidates prepared pragma readbacks.

Fireweed's log-backed projection selects its measured limit explicitly and checks
readback. This is not a durability setting and must not be used to disable the
authoritative log's synchronization. The vendored crate is excluded from the
Fireweed workspace member list; its sources otherwise retain the published crate
layout. Remove this override when an upstream release supplies equivalent tested
checkpoint control. Do not silently replace the pinned sources during upgrades.

The patch also reconciles the page cache's tracked evictable count after WAL
commit clears dirty flags. Without reconciliation, later allocations can scan
the entire cache repeatedly despite having clean pages available. Recounting
once per commit preserves the existing spill decision and eviction safety
checks. The fallback count stops once the required number of pages is found.
A native cache regression test covers dirty-to-clean commit reconciliation.

Checkpoint batches now consume latest-safe frames in destination page order.
Previously, sorting by WAL frame number scattered adjacent database pages across
512-page batches, preventing vectored writes when updating a reused database.
A native integration regression updates 2,048 existing pages in interleaved order:
the old path issued 1,973 destination writes; page ordering issued eight. The test
checks both a large pager cache and forced WAL reads, then truncates the WAL,
reopens and independently verifies every persisted value by count and checksum.
This favors destination write locality over WAL read locality; performance on
cold storage beyond the tested working set is not established. Safe frame
selection, bounded batch sizes, reader guards and sync publication are unchanged.

Freed pages that already require a WAL write now clear their unused usable bytes.
Free-list trunks retain their header/pointers; clean free-list leaves are not
made dirty, avoiding writes of otherwise untouched overflow pages. Existing
`add_dirty` captures undo state and invalidates spill tags before clearing. WAL
frames, synchronization, reserved codec bytes and reader snapshots are preserved.
This makes obsolete body bytes compressible on the measured filesystem; it does
not omit frames or promise secure deletion. A native VFS regression covers both
900-byte and overflow bodies with large and spilling caches, savepoint rollback,
pinned old readers, checkpoint/reopen, and reuse of every freed page.

UPDATE now checks a partial index's new-row predicate before evaluating the new
key expressions, applying key affinities, or constructing its index record.
Rows outside the predicate (including NULL) never use that key in constraint
checking or insertion. Old-entry deletion remains separately guarded by the
old-row predicate. This also fixes erroneous evaluation errors for unused keys,
such as `abs(-9223372036854775808)` on a row outside an expression index.
The native regression covers predicate transitions, uniqueness rollback and
index integrity, with the same case checked against SQLite.
