# Astra Ultra iteration 10 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base aa7d01c0.
Scope: shared single-client acknowledged-lifecycle workload and PostgreSQL fixture.

The useful numerator counts completed source plus completed emitted work only.
Foreign records remain pending. Both phases use the same typed filtered claims,
priority/insertion-tie oracle, payload checks and per-group commit interface.
Source commits include atomic state/fanout; emitted-work commits settle only.
Final stats, exact receipt replay and paginated state values/counts verify effects.

Review required precise metric scope: lifecycle timing stops before verification
and shutdown; physical drain/debt is unmeasured. The rate is explicitly named
acknowledged_settled_per_second. Aggregated source/next latency samples and terminal
empty claim probes are disclosed. Fixed operational clock, debug/source-dirty
runs, single-client/small-sample stats and retained fixture growth are not full
qualification. Source/host/compiler/profile metadata accompany fixture output.

Root verified the default 256-source workload and partial 65-source batch lane,
strict fixture Clippy, formatting and diff checks. Approval does not qualify
matched S3/Turso rates, active progress, 10M/density, debt/RSS/storage/cost, or legacy
migration. The identical workload must be wired to S3/Turso before comparison.
