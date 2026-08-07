# Release tag evidence gate

Fireweed release qualification has a local smoke-evidence lane and a
tag-publishing governed-evidence lane; neither permits a missing semantic
authority to be substituted.

The default local invocation of `scripts/ci/release-gate.sh` first creates a
clean ledger and runs the current smoke suites. Fresh smoke-tier E2 and E3 rows
are required. The same command then validates the governed TP-002 composite at
`target/tp002-release/composite-contract.json`. Exact-commit evidence producers first stage
these files in an explicit run-owned directory outside the repository because a commit cannot
contain a file that embeds that same commit's not-yet-computed SHA. The tag workflow verifies and
extracts the promoted archive at `target/tp002-release`. The composite names separate E0, E1, E2
cross-owner, E2 density, E2 failover/routing, and E3 authorities. No directory
scan or generic `bars_met=true` row can satisfy a missing semantic authority.
The tag workflow invokes `release-gate.sh --governed-performance-only`: it runs
the functional release checks and exact-revision composite verifier, but does
not rerun scaled local smoke workloads on a GitHub runner. Its required
performance authority is the acquired, semantic-verified governed archive.

The tag workflow (`push.tags: ['v*']`, plus `workflow_dispatch` tag reruns)
checks out evidence commit `E` (tag peel) and measured source `S` from `E`'s
promotion metadata into physically separate directories. It never treats ambient
`GITHUB_SHA` as measured source. S-authored tooling verifies
`target/tp002-release/attestation.json` (materialized from `E` into a run-owned
root) against tag `vV` and commit `S`, and all evidence and input digests must
match before packaging or publication begins. Immutable container tags use
`sha-${S}`.

The governed E3 authority is portable across hosts: exact command/recovery
counts, progress, bounded resources, batching, and same-run comparisons are
release criteria. Quiet-host requirements and absolute throughput or latency
thresholds are rejected. Wall-clock results may be reported for capacity
planning, but they do not decide the release verdict. The configured
`progress_bound_ms` is different: it is the queue's logical liveness contract
under load, not a host-performance bar, and remains release-significant.

Versioned files in this directory describe release candidates and already-cut
releases. Once a release is cut, its note is immutable. Fireweed Queue v0.22.0
is the current release candidate. v0.20.0 is the first renamed public
preview release; v0.19.6 and earlier retain
the retired identity as immutable release and audit history under ADR-023. This
file defines the gate applied to future tags.

## Source-preview dry run

Run the complete local public-release validation manifest and retain its
revision/tool evidence before preparing source artifacts:

```bash
python3 scripts/ci/public-release-gate.py \
  --manifest scripts/ci/public-release-gates.json \
  --evidence target/public-release-gate.json
```

The versioned manifest preserves each constituent's native output and stops at
the first failure. CI invokes the same entrypoint with the bounded CI manifest;
both evidence files record the exact Git revision, manifest version, command
status, and available Git, Rust, Cargo, Python, cargo-deny, and gitleaks
versions. Registry package publication and deployment lint remain outside the
v0.21 source-only boundary; the manifest verifies the declared source package,
SBOM, checksums, and provenance without publishing them.

The v0.21 public-preview boundary ships an immutable Git tag and
GitHub-generated source archives. crates.io, GHCR, binary, and Helm publication
remain deferred. Prepare the exact local verification bundle without publishing:

```bash
revision="$(git rev-parse HEAD)"
bash scripts/release/build-source-preview-artifacts.sh \
  --out target/source-preview-dist \
  --version 0.22.0 \
  --revision "$revision" \
  --builder "local:$(id -un)"
bash scripts/release/verify-source-preview-artifacts.sh \
  --dist target/source-preview-dist \
  --version 0.22.0 \
  --revision "$revision"
```

The exact set is a deterministic source archive, SPDX 2.3 package SBOM,
revision-and-builder provenance statement, and `SHA256SUMS`. The verifier checks
the archive path boundary, every checksum, SBOM revision binding, provenance
subjects, source revision, builder, and dry-run invocation. No artifact is
signed and no SLSA level is claimed; checksum and provenance verification are
the documented preview mechanism until signing is separately authorized.

## Public version sources

Before changing version values for a release, inventory every public version
source from the repository root:

```bash
bash scripts/release/list-public-version-sources.sh
```

The command reports the Cargo workspace version, README artifact coordinates,
Helm chart `version` and `appVersion`, Helm packaging inputs and evidence names,
and existing files under `docs/releases`. ADR-023 sets `v0.20.0` as the first
Fireweed-branded release. Cargo and published artifact coordinates are
release-synchronized to that target. The chart file's development defaults are
independently versioned, while `package-helm-chart.sh` overrides both chart
versions with the release version when it builds the published package.

Historical release-note files remain immutable. Add one new file matching each
release tag; do not rename earlier notes or artifacts. Any new
public version source must be added to the inventory command before release.

## Exact-revision composite staging

`scripts/release/build-governed-evidence-bundle.sh` is the local source of
truth for staging release evidence. It accepts only explicitly named producer
outputs, requires the requested revision to equal checked-out `HEAD`, copies
them into a fresh external run-owned directory, writes `composite-contract.json`, and immediately
dispatches the E0/E1, E2 cross-owner, exact-profile density, failover/routing,
and E3 semantic validators.

The E3 handoff is deliberately a directory hook (`--e3-source-dir`). The
strengthened E3 producer owns the measured ledger, TP-003 rows, and fencing
evidence in that directory. The stager copies only those three named inputs;
unlisted files beside them cannot enter the governed bundle. It invokes
`fireweed-build-e3-contract` to recompute cost rows and build the exact-revision
contract without weakening it.
When tag and review timestamps are supplied, the stager adds and verifies
`attestation.json`, using `fireweed_release::attestation::digest_path`, then
archives the fixed `tp002-release/` root as `<exact-head>.tar.gz` plus
`<exact-head>.tar.gz.sha256` beside it. Sorted paths and normalized tar/gzip
metadata make identical inputs byte-identical. Existing output, archive, or
sidecar paths fail closed instead of substituting stale evidence. The tag
workflow explicitly acquires that archive before installing toolchains or
starting heavy validation. A fresh checkout never assumes untracked `target/`
files exist. Repository-owned output paths are rejected; `target/tp002-release`
is only the tag workflow's verified promotion/extraction location.

```bash
revision=$(git rev-parse HEAD)
run_root=$(mktemp -d)
bash scripts/release/build-governed-evidence-bundle.sh \
  --source-dir "$run_root/tp002-producer-output" \
  --e3-source-dir "$run_root/tp002-e3-producer-output" \
  --out "$run_root/tp002-release" \
  --revision "$revision" \
  --tag vX.Y.Z \
  --produced-at 2026-07-20T00:00:00Z \
  --reviewed-at 2026-07-20T00:05:00Z
```
