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
implementations fail explicitly when configuration is requested. Native writes,
locks, synchronization, checkpoint backfill and WAL restart are unchanged.

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
