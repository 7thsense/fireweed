# Upstream dependencies

Fireweed uses the published Turso 0.8.2 packages from crates.io. No Turso engine
or SDK fork is vendored. Adapter behavior must use upstream-supported APIs;
application recovery and checkpoint scheduling belong in Fireweed.
