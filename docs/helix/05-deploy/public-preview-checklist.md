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
---

# Fireweed v0.21.0 Public Preview Checklist

## Release Scope

- Component: Fireweed Queue source repository and Rust embedding facade
- Version: `v0.21.0`
- Release time: 2026-07-26 06:04:35 UTC
- Repository: <https://github.com/telepathdata/fireweed> (public; `main` is the
  default branch; GitHub issues are enabled)
- Publication: annotated Git tag and GitHub source release, both complete
- Contribution policy: issues accepted; pull requests, patches, and other code
  contributions not accepted
- Deferred publication: crates.io and GHCR
- Release owner: project maintainer
- Rollback owner: project maintainer
- Approvers: technical lead and release owner
- Supporting artifacts: [release notes](../../releases/v0.21.0.md),
  [public preview boundary](../00-discover/public-preview-boundary.md),
  [deployment readiness contract](../04-build/DEPLOYMENT-READINESS.md), and
  [ADR-021](../02-design/adr/ADR-021-open-source-license-and-contribution-policy.md)

## Pre-Deploy Checks

Unchecked rows are required evidence placeholders. They are not passing
evidence and must not be changed to pass without recording the exact command,
revision, result, and environment where applicable.

| Area | Check | Evidence or Command | Status |
|------|-------|---------------------|--------|
| Repository | Public repository is `telepathdata/fireweed`; issues enabled | `gh repo view telepathdata/fireweed --json nameWithOwner,visibility,hasIssuesEnabled,defaultBranchRef` | Confirmed 2026-07-25 |
| Repository redirect | The immediately previous GitHub coordinate resolves to the immutable released source | Anonymous `git ls-remote` returned `5b2cf59b29c0652af9e8513ea2e6de5e93201474`; the exact historical coordinate is retained in the cutover bead | Confirmed 2026-07-26 |
| Hosting controls | Record repository automation and branch policy rather than assuming protection | Repository Actions API reports `enabled=false`; `main` has no GitHub branch-protection rule; release validation and publication were therefore performed locally/manual | Recorded 2026-07-26 |
| Policy | README, `CONTRIBUTING.md`, support, security, and ADR-021 agree on issues-only contributions | `rg -n 'Issues are welcome|Pull requests.*not accepted|issues-only' README.md CONTRIBUTING.md SUPPORT.md SECURITY.md docs/helix/02-design/adr/ADR-021-open-source-license-and-contribution-policy.md` | Passed at `51152e1d` (2026-07-25) |
| Identity | Current Fireweed namespace gate passes | `bash scripts/verify-public-identity.sh` | Passed at `51152e1d` (2026-07-25) |
| Format | Formatting and whitespace gates pass | `cargo fmt --all --check && git diff --check` | Passed at `51152e1d` (2026-07-25) |
| Build | Workspace compiles with the release toolchain | `cargo check --locked --workspace --all-targets --all-features` | Passed at `51152e1d` (2026-07-25) |
| Lint | Workspace clippy gate passes | `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | Passed at `51152e1d` (2026-07-25) |
| Function | Complete non-performance workspace functionality passes | Workspace sweep, then affected-package and explicitly enumerated non-performance server-target reruns | Passed at `51152e1d` (2026-07-25) |
| Facade | Public constructor, mutation, and durability matrices pass | `cargo test --locked -p fireweed --all-features` plus the 13-cell external matrix | Passed at `51152e1d` (2026-07-25) |
| PostgreSQL | PostgreSQL-backed matrix executes with no skips | `FIREWEED_PG_TEST_URL=<test-dsn> bash scripts/ci/record-postgres-transaction-evidence.sh` plus the live workspace suite | Passed against Niflheim PostgreSQL at `51152e1d` (2026-07-25) |
| Object storage | S3-compatible object-log matrix executes against Garage on eldir with no skips | Public external matrix plus `objectlog_shared_ownership` using PostgreSQL publication authority | Passed against Garage: external 13/13 and ownership 9/9 at `51152e1d` (2026-07-25) |
| Version | Cargo, docs, tag, and release-note versions agree | `bash scripts/release/list-public-version-sources.sh v0.21.0` | Passed at `51152e1d` (2026-07-25) |

## Rollout Plan

| Stage | Action | Exit Condition |
|-------|--------|----------------|
| Local candidate | Finish all pre-deploy rows at one immutable commit | Every required row records pass evidence; zero skips in conditional backend rows |
| Public source release | Push validated `main`, create annotated `v0.21.0`, and create the GitHub Release from `docs/releases/v0.21.0.md` | Remote branch, tag, and release resolve to the validated commit |
| Downstream integration | Pin Snorri to the public tag and rerun its release matrix | Snorri lockfile records the tag revision and all required tests pass |

crates.io publication, GHCR publication, and a production deployment are not
rollout stages for v0.21.0.

## Verification Checks

| Signal or Check | Expected Result | Evidence or Command | Status |
|-----------------|-----------------|---------------------|--------|
| Git tag | Annotated `v0.21.0` resolves to the validated commit | Tag object `cc07218`; peeled commit `5b2cf59b29c0652af9e8513ea2e6de5e93201474` | Passed 2026-07-26 |
| GitHub release | Non-draft, non-prerelease source release exists for `v0.21.0` | <https://github.com/telepathdata/fireweed/releases/tag/v0.21.0> | Passed 2026-07-26 |
| Public clone | Clean clone resolves and builds the facade from the tag | Anonymous depth-one tag clone; `cargo check --locked -p fireweed` | Passed 2026-07-26 |
| Snorri consumption | Snorri resolves only public `fireweed` at the tag revision | Snorri `Cargo.lock`; clean-checkout all-feature workspace check; live PostgreSQL/Garage matrix; Snorri `v0.11.0` at `b37e46410287b563ca692666bca2032a81cb9e3b`; Fast CI run `30191358058` passed in 56 seconds | Passed 2026-07-26 |
| Registry boundary | No crates.io or GHCR artifact is claimed or required | Fireweed and Snorri GitHub releases have zero uploaded assets and their release notes defer registry publication | Passed 2026-07-26 |

## Rollback Triggers

| Trigger | Threshold or Condition | Immediate Action | Owner |
|---------|------------------------|------------------|-------|
| Validation regression | Any required local or live-backend row fails or skips | Hold the release; fix on `main`; rerun the entire affected matrix | Project maintainer |
| Tag mismatch | Tag, release, or Snorri lockfile resolves to a different commit | Do not move the tag; withdraw the GitHub Release if created and cut a new patch version after correction | Project maintainer |
| Public API defect | The tagged facade cannot satisfy Snorri or exposes backend-specific construction details | Mark the release unsupported, document the defect, and cut a forward-fix patch; never retag | Project maintainer |
| Durability defect | Committed mutation is lost, rejected mutation has durable effect, or request replay diverges | Mark affected profiles unsupported, publish an issue/advisory as appropriate, and cut a forward fix | Project maintainer |

Rollback is forward-only. Published tags are immutable; rollback never means
moving or deleting a tag to substitute different source.

## Support and Deferred Claims

- Support is best-effort through public issues and has no SLA or guaranteed fix
  timeline. Security reports use the private channel in `SECURITY.md`.
- v0.21.0 does not claim production readiness, hosted availability,
  multi-region failover, capacity leadership, provider certification, or
  universal performance bounds.
- Memory is development-only. Experimental and deferred profiles retain the
  classifications in `public-preview-boundary.md` even when their tests pass.
- Performance results are host- and configuration-bound evidence. They do not
  replace complete functionality and durability gates.
- crates.io packaging, internal-crate publication, GHCR images, and associated
  registry support remain deferred until separately authorized and verified.

## Go or No-Go Decision

- Decision: **Go for the Fireweed source release**
- Decision time: 2026-07-25
- Reason: local functionality, facade, live PostgreSQL, and live Garage gates
  passed; registry publication remains deliberately deferred
- Post-publication condition: verify the immutable tag and clean public clone,
  then pin and validate Snorri before cutting its release
- Follow-up owner: project maintainer
