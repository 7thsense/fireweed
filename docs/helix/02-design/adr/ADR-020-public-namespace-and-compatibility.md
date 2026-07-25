---
ddx:
  id: adr-020-public-namespace-and-compatibility
  depends_on:
    - name-and-positioning-analysis
  links:
    - {kind: informed_by, to: name-and-positioning-analysis}
  status: superseded
---

# ADR-020: Public namespace and compatibility policy for Fireweed Queue

| Date | Status | Deciders | Related |
|------|--------|----------|---------|
| 2026-07-23 | Superseded by ADR-023 | Project maintainers | name-and-positioning-analysis |

> Superseded on 2026-07-25 by ADR-023. Fireweed is pre-release; the compatibility period and retained
> runtime/persistence namespaces specified here do not apply.

## Context

The project has already selected Fireweed Queue as the public identity. The
current repository, package, binary, environment, and artifact names still use
the `pqueue` stem, so the rename must be treated as a deliberate migration
policy rather than a loose brand refresh.

The naming contract for the public namespace is fixed:

- display name: Fireweed Queue
- short name: Fireweed
- repository stem: `fireweed`
- CLI stem: `fireweed`
- Rust stem: `fireweed`
- environment prefix: `FIREWEED_`
- abbreviation: `FWQ`
- first renamed release: `v0.20.0`

That rename has to coexist with existing releases, persisted data, wire-facing
protocols, downstream consumers, and audit history. The migration therefore
needs two outputs:

1. a stable decision on which surfaces are public renames versus retained
   compatibility points; and
2. a machine-readable inventory that lets later implementation work proceed
   surface by surface.

This ADR is about namespace control only. It does not change queue semantics,
storage behavior, or release content beyond the name classification needed for
the migration.

## Decision

Adopt Fireweed Queue as the public namespace and publish the first renamed
release as `v0.20.0`.

Public-facing renames are the target coordinates listed in
[public-namespace-migration.yaml](../public-namespace-migration.yaml). Current
`pqueue` names remain compatibility aliases until the cutover release lands.

The migration policy is:

- public brand and packaging coordinates rename to Fireweed values;
- current `pqueue` package, binary, CLI, and environment names remain callable
  as compatibility aliases until `v0.20.0`;
- persisted data names and wire identifiers stay stable when changing them
  would rewrite durable state or break on-disk / on-wire compatibility;
- historical release, audit, and branch identifiers stay in history and are not
  mechanically rewritten;
- git history rewriting is forbidden for this migration path. Do not use
  rebase-based rewrites, filter-based rewrites, or commit amendment to relabel
  already-published execution history.

The public rename is therefore additive from the consumer point of view, not a
history rewrite.

## Compatibility matrix

| Surface | Current | Policy | Target / note |
|---|---|---|---|
| Display name | `pqueue` / `pqueue-service` era branding | Rename-now | `Fireweed Queue` |
| Short name | `pqueue` | Rename-now | `Fireweed` |
| Repository | `telepathdata/7thsense-pqueue` | Rename-now | `fireweed` |
| Rust / CLI stem | `pqueue` | Rename-now | `fireweed` |
| Environment prefix | `PQUEUE_` | Temporary compatibility alias | `FIREWEED_` |
| Cargo packages | `pqueue*` | Temporary compatibility alias | `fireweed*` package names |
| Rust crate paths | `crates/pqueue*`, `tools/pqueue*` | Temporary compatibility alias | `crates/fireweed*`, `tools/fireweed*` |
| Binaries | `pqueue-service`, `pqueue-loadgen`, `pqueue-verify-*` | Temporary compatibility alias | `fireweed`, `fireweed-service`, `fireweed-loadgen`, etc. |
| CLI entrypoints | `pqueue`, `pqueue-service` | Temporary compatibility alias | `fireweed`, `fireweed-service` |
| Config and chart names | `pqueue` chart/package names | Temporary compatibility alias | `fireweed-queue` chart/package names |
| Protocol and on-disk identifiers | `PQUEUE_*`, `pqueue_*`, `/var/lib/pqueue/*` | Intentionally retained persistence / wire identifier | Keep until a separate data-migration ADR changes them |
| Services and images | `pqueue` service names, `pqueue-service` image names | Temporary compatibility alias | `fireweed` service names, `fireweed-service` image names |
| Repository URLs | current `pqueue` remote | Historical allowlist or rename-now, depending on the URL | New `fireweed` remote becomes authoritative |
| Downstream consumers | CI, scripts, docs, release packaging | Temporary compatibility alias | Update to `fireweed` coordinates as each consumer migrates |
| Audit identifiers | release tags, bead IDs, historical docs | Historical allowlist | Keep as-is for traceability |

The compatibility matrix is intentionally split so that public renames are not
confused with durable identifiers that should stay stable.

## Migration order

1. Reserve the new public coordinates: repository, package namespace, image
   namespace, chart namespace, and CLI stem.
2. Ship the first renamed release as `v0.20.0` with Fireweed branding and
   compatibility aliases still accepting the old `pqueue` names.
3. Update downstream consumers in priority order:
   - release packaging and artifact naming;
   - container images and Helm release coordinates;
   - CLI and library entrypoints;
   - environment-variable documentation and runtime wrappers;
   - external docs and examples.
4. Leave persisted data names and wire identifiers alone unless a later
   migration ADR explicitly authorizes a data rewrite.
5. Retire compatibility aliases only after the renamed release and the
   follow-up migration inventory confirm no remaining required consumer.

## Rollback

Rollback is a release rollback, not a history rewrite.

- If the rename introduces a blocking issue before `v0.20.0` ships, keep the
  old `pqueue` namespace as the active public surface and defer the cutover.
- If `v0.20.0` ships with a naming defect, cut a follow-up release that restores
  the previous public coordinates or reintroduces the missing compatibility
  alias.
- Do not rewrite published git history, published tags, or release artifacts to
  fake a rollback.
- Do not change persisted data or wire identifiers during rollback unless the
  separate storage migration that owns those identifiers is also being reversed.

## Consequences

The rename gives the project a more specific public identity and aligns the
repository, CLI, and Rust namespace with the product name.

The cost is a temporary dual-name period. Operators, release automation, and
integration tests will need to tolerate both the old `pqueue` coordinates and
the new Fireweed ones until the migration completes.

The retained persistence and wire identifiers reduce migration risk because the
rename does not force a rewrite of stored data or protocol contracts.

The no-history-rewrite rule preserves auditability and avoids breaking the
existing execution trail, but it also means the old namespace remains visible in
past release artifacts and git history.
