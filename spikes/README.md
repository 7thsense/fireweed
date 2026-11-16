# Historical storage experiments

These standalone experiments and their recorded results were recovered from
`93c80ee8` on `rescue/sindri-2026-10-01/main`. They document the measurements
behind SPIKE-002 through SPIKE-006. They are outside the Fireweed product
workspace and its release test suite.

The experiment manifests target the vendored Turso 0.7.2 tree that existed at
the recorded revision. To reproduce the measurements, use an isolated worktree
at that revision with its original dependencies and the commands in each spike.
Current Fireweed uses a later Turso release. The recorded 10M/100M experiments
are historical capacity evidence; they do not qualify the current release.

API-006 and ADR-025 are retained as superseded design records. API-007 and
ADR-026 govern the current queue contract.
