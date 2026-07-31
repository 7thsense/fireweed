---
ddx:
  id: public-preview-checklist
  type: deployment-checklist
  flow: helix
  status: released
  links:
    - kind: informed_by
      to: public-preview-boundary
    - kind: informed_by
      to: adr-021-open-source-license-and-contribution-policy
    - kind: informed_by
      to: production-deployment-readiness
    - kind: informed_by
      to: storage-matrix-completion-brief
---

# Fireweed v0.24.0 Public Preview Checklist

## Release Scope

- Component: Fireweed Queue source repository and Rust embedding facade
- Version: `v0.24.0`
- Release time: 2026-07-29 01:05:54 UTC
- Repository: <https://github.com/7thsense/fireweed> (public; `main` default branch)
- Publication: annotated Git tag and GitHub source release, both complete
- Contribution policy: issues accepted; pull requests, patches, and other code
  contributions not accepted
- Deferred publication: crates.io and GHCR
- Release owner: project maintainer
- Rollback owner: project maintainer
- Approvers: technical lead and release owner
- Supporting artifacts: [release notes](../../releases/v0.24.0.md),
  [public preview boundary](../00-discover/public-preview-boundary.md),
  [deployment readiness contract](../04-build/DEPLOYMENT-READINESS.md),
  [storage matrix completion brief](../04-build/storage-matrix-completion-brief.md), and
  [ADR-021](../02-design/adr/ADR-021-open-source-license-and-contribution-policy.md)

## Pre-Deploy Checks

Unchecked rows are required evidence placeholders. They are not passing
evidence and must not be changed to pass without recording the exact command,
revision, result, and environment where applicable.

| Area | Check | Evidence or Command | Status |
|------|-------|---------------------|--------|
| Repository | Public repository is `7thsense/fireweed`; issues enabled | `gh repo view 7thsense/fireweed --json nameWithOwner,visibility,hasIssuesEnabled,defaultBranchRef` | Confirmed 2026-07-29 |
| Identity | Current Fireweed namespace gate passes | `bash scripts/verify-public-identity.sh` | Required at tag SHA |
| Format | Formatting gate passes | `cargo fmt --all --check` | Passed at release prep (`d16b97bc` lineage / tag `211376db`) |
| Lint | Workspace clippy gate passes | `cargo clippy --workspace --all-targets -- -D warnings` | Passed at `d16b97bc` and carried into `v0.24.0` |
| Storage matrix | Full 15-cell matrix gate is bound on the release path | `scripts/ci/storage-matrix-gate.sh` wired into release/deployment release-gate (commit `212915ce`) | Passed wiring on `main` at tag |
| Version | Cargo, docs, tag, and release-note versions agree | `workspace.package.version=0.24.0`; `docs/releases/v0.24.0.md`; tag `v0.24.0` | Passed at `211376db` |
| Public surface | No legacy product SKU names on public axes | `scripts/ci/assert-no-legacy-storage-product-names.sh` | Required by storage-matrix gate |
| Quickstart | Anonymous public clone / operator quickstart unblocked | bead `pqueue-f44eac58` closed on main | Passed pre-tag |

## Rollout Plan

| Stage | Action | Exit Condition |
|-------|--------|----------------|
| Local candidate | Finish required gates at one immutable commit | Format, clippy, storage-matrix wiring, version identity green |
| Public source release | Push validated `main`, create annotated `v0.24.0`, GitHub Release from `docs/releases/v0.24.0.md` | Remote branch, tag, and release resolve to the validated commit |
| Scale evidence (separate) | Exact 10M recovery + density tracks remain governed beads | Not a blocker for source preview publication |

crates.io publication, GHCR publication, and a production multi-region deployment
are not rollout stages for v0.24.0.

## Verification Checks

| Signal or Check | Expected Result | Evidence or Command | Status |
|-----------------|-----------------|---------------------|--------|
| Git tag | Annotated `v0.24.0` resolves to the validated commit | Tag peels to `211376db954730817e0e13168cf4ff33da705958` | Passed 2026-07-29 |
| GitHub release | Non-draft source release exists for `v0.24.0` | <https://github.com/7thsense/fireweed/releases/tag/v0.24.0> | Passed 2026-07-29 |
| origin/main identity | Tagged commit equals `origin/main` | Release workflow identity check | Passed at push |
| Registry boundary | No crates.io or GHCR artifact is claimed | Release notes defer registry publication | Passed 2026-07-29 |

## Rollback Triggers

| Trigger | Threshold or Condition | Immediate Action | Owner |
|---------|------------------------|------------------|-------|
| Validation regression | Any required local or live-backend row fails or skips | Hold the release; fix on `main`; cut a forward patch | Project maintainer |
| Tag mismatch | Tag, release, or consumer lockfile resolves to a different commit | Do not move the tag; withdraw if needed; cut a new patch version | Project maintainer |
| Public API defect | Tagged facade exposes backend-specific construction or drops a matrix cell | Mark release unsupported; forward-fix patch; never retag | Project maintainer |
| Durability defect | Committed mutation lost, rejection durable, or request replay diverges | Mark affected cells unsupported; forward fix | Project maintainer |

Rollback is forward-only. Published tags are immutable.

## Support and Deferred Claims

- Support is best-effort through public issues and has no SLA.
- v0.24.0 does not claim production multi-region readiness, capacity leadership,
  provider certification, or universal performance bounds.
- Class B (`memory` log) carries a **semantic durability** disclaimer only; it is
  a supported matrix row, not an incomplete product family (see public-preview-boundary).
- Release-tier 10M recovery PASS and 1,000-queue density live evidence remain
  separate governed tracks when not closed on this tag.
- crates.io packaging and GHCR images remain deferred until separately authorized.

## Go or No-Go Decision

- Decision: **Go for the Fireweed source release**
- Decision time: 2026-07-29
- Reason: complete public 5×3 matrix surface from v0.23.2, plus post-matrix CI
  wiring, clippy green after authority demotion, and quickstart unblock land on
  `v0.24.0`; registry publication remains deliberately deferred
- Follow-up owner: project maintainer (E3 recovery PASS stamp, density durable runner)
