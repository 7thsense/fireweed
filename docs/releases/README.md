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

The tag workflow then runs the governed gate in `exact-tag` mode with
`target/tp002-release/attestation.json`. It verifies that the
resolved tag points to `GITHUB_SHA`, that the attestation names that same tag and
commit, and that all evidence and input digests match before packaging or
publication begins.

The governed E3 authority is portable across hosts: exact command/recovery
counts, progress, bounded resources, batching, and same-run comparisons are
release criteria. Quiet-host requirements and absolute throughput or latency
thresholds are rejected. Wall-clock results may be reported for capacity
planning, but they do not decide the release verdict.

Versioned files in this directory describe already-cut releases. This file
defines the gate applied to future tags.

## Exact-revision composite staging

`scripts/release/build-governed-evidence-bundle.sh` is the local source of
truth for staging release evidence. It accepts only explicitly named producer
outputs, requires the requested revision to equal checked-out `HEAD`, copies
them into a fresh directory, writes `composite-contract.json`, and immediately
dispatches the E0/E1, E2 cross-owner, exact-profile density, failover/routing,
and E3 semantic validators.

The E3 handoff is deliberately a directory hook (`--e3-source-dir`). The
strengthened E3 producer owns that directory and its `e3-contract.json`; the
stager does not reconstruct or weaken it. After staging, the release producer
adds `attestation.json`, using `pqueue_release::attestation::digest_path`, then
archives the directory as `<exact-head>.tar.gz` plus its SHA-256 sidecar. The
tag workflow explicitly acquires that archive before installing toolchains or
starting heavy validation. A fresh checkout never assumes untracked `target/`
files exist.

```bash
revision=$(git rev-parse HEAD)
bash scripts/release/build-governed-evidence-bundle.sh \
  --source-dir target/tp002-producer-output \
  --e3-source-dir target/tp002-e3-producer-output \
  --out target/tp002-release \
  --revision "$revision"
```
