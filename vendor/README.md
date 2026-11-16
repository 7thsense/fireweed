# Upstream dependencies

Fireweed uses the published Turso 0.8.2 packages from crates.io. No Turso engine
or SDK fork is vendored. Adapter behavior must use upstream-supported APIs;
application recovery and checkpoint scheduling belong in Fireweed.

`FIREWEED_VENDOR.json` preserves provenance of the retired 0.7.2 fork. It does
not describe the current dependency or qualify upstream tests.
