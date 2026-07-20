# Release tag evidence gate

Every pqueue tag must pass two distinct evidence lanes before artifacts are
published.

`scripts/ci/release-gate.sh` first creates a clean ledger and runs the current
smoke suites. Fresh smoke-tier E2 and E3 rows are required. The same command then
validates the governed TP-002 manifest at
`target/tp002-release/manifest.json` and the source-bound E3 contract at
`target/tp002-release/e3-contract.json`. Exact-commit evidence producers stage
these files outside Git because a commit cannot contain a file that embeds that
same commit's not-yet-computed SHA. The manifest names one exact
authority for each of E0, E1, E2, and E3. No directory scan, neighboring TP-003
JSONL, or unlisted replacement row can satisfy a missing authority.

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
