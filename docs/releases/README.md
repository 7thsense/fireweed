# Release tag evidence gate

Every pqueue tag must pass two distinct evidence lanes before artifacts are
published.

`scripts/ci/release-gate.sh` first creates a clean ledger and runs the current
smoke suites. Fresh smoke-tier E2 and E3 rows are required. The same command then
validates the governed TP-002 composite at
`target/tp002-release/composite-contract.json`. Exact-commit evidence producers stage
these files outside Git because a commit cannot contain a file that embeds that
same commit's not-yet-computed SHA. The composite names separate E0, E1, E2
cross-owner, E2 density, E2 failover/routing, and E3 authorities. No directory
scan or generic `bars_met=true` row can satisfy a missing semantic authority.

The tag workflow then verifies `target/tp002-release/attestation.json`. It
verifies that the resolved tag points to `GITHUB_SHA`, that the attestation
names that same tag and commit, and that all evidence and input digests match
before packaging or publication begins.

The governed E3 authority is portable across hosts: exact command/recovery
counts, progress, bounded resources, batching, and same-run comparisons are
release criteria. Quiet-host requirements and absolute throughput or latency
thresholds are rejected. Wall-clock results may be reported for capacity
planning, but they do not decide the release verdict. The configured
`progress_bound_ms` is different: it is the queue's logical liveness contract
under load, not a host-performance bar, and remains release-significant.

Versioned files in this directory describe already-cut releases. Fireweed Queue
v0.20.0 is the first renamed public preview release; v0.19.6 and earlier retain
the pqueue identity as immutable release and audit history under ADR-020. This
file defines the gate applied to future tags.

## Public version sources

Before changing version values for a release, inventory every public version
source from the repository root:

```bash
bash scripts/release/list-public-version-sources.sh
```

The command reports the Cargo workspace version, README artifact coordinates,
Helm chart `version` and `appVersion`, Helm packaging inputs and evidence names,
and existing files under `docs/releases`. ADR-020 sets `v0.20.0` as the first
Fireweed-branded release. Cargo and published artifact coordinates are
release-synchronized to that target. The chart file's development defaults are
independently versioned, while `package-helm-chart.sh` overrides both chart
versions with the release version when it builds the published package.

Historical release-note files remain immutable. Add `docs/releases/v0.20.0.md`
when that release is cut; do not rename earlier notes or artifacts. Any new
public version source must be added to the inventory command before release.

## Exact-revision composite staging

`scripts/release/build-governed-evidence-bundle.sh` is the local source of
truth for staging release evidence. It accepts only explicitly named producer
outputs, requires the requested revision to equal checked-out `HEAD`, copies
them into a fresh directory, writes `composite-contract.json`, and immediately
dispatches the E0/E1, E2 cross-owner, exact-profile density, failover/routing,
and E3 semantic validators.

The E3 handoff is deliberately a directory hook (`--e3-source-dir`). The
strengthened E3 producer owns the measured ledger, TP-003 rows, and fencing
evidence in that directory. The stager copies only those three named inputs;
unlisted files beside them cannot enter the governed bundle. It invokes
`pqueue-build-e3-contract` to recompute cost rows and build the exact-revision
contract without weakening it.
When tag and review timestamps are supplied, the stager adds and verifies
`attestation.json`, using `pqueue_release::attestation::digest_path`, then
archives the fixed `tp002-release/` root as `<exact-head>.tar.gz` plus
`<exact-head>.tar.gz.sha256` beside it. Sorted paths and normalized tar/gzip
metadata make identical inputs byte-identical. Existing output, archive, or
sidecar paths fail closed instead of substituting stale evidence. The tag
workflow explicitly acquires that archive before installing toolchains or
starting heavy validation. A fresh checkout never assumes untracked `target/`
files exist.

```bash
revision=$(git rev-parse HEAD)
bash scripts/release/build-governed-evidence-bundle.sh \
  --source-dir target/tp002-producer-output \
  --e3-source-dir target/tp002-e3-producer-output \
  --out target/tp002-release \
  --revision "$revision" \
  --tag vX.Y.Z \
  --produced-at 2026-07-20T00:00:00Z \
  --reviewed-at 2026-07-20T00:05:00Z
```
