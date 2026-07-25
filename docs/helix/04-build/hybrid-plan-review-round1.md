## Target

Review `docs/helix/04-build/hybrid-sqlite-inmemory-projection-plan.md`.

## Governing Artifacts And Code

- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `docs/helix/02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md`
- `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- `crates/fireweed-engine/src/compose.rs`
- `crates/fireweed-projection/src/compose_impls.rs`
- `crates/fireweed-sqlite/src/relational.rs`
- `crates/fireweed-objectlog/src/compose_log.rs`
- `crates/fireweed-server/src/lib.rs`
- `crates/fireweed-server/src/env_config.rs`

## Review Question

You are a critic, not a validator. Find BLOCKING issues in the plan before it
is broken into DDx beads and implemented. Focus on ambiguities, missing
contracts, incorrect recovery assumptions, transaction-integrity holes,
performance risks under multi-million-member campaigns, missing tests, or any
scope split that would cause agents to build the wrong thing.

A BLOCKING finding is anything that would cause implementation rework, a
migration hazard, or a spec gap that agents will interpret differently. Do not
balance criticism with praise.

## Output Contract

Produce findings as:

### Findings

| Severity | Area | Finding |
|---|---|---|
| BLOCKING | <area> | <specific issue with evidence> |
| WARNING  | <area> | <specific issue with evidence> |
| NOTE     | <area> | <observation with evidence> |

### Verdict: APPROVE | REQUEST_CHANGES | BLOCK

### Summary

2-4 sentences. Cite the specific section, file, or missing contract that caused
each finding.
