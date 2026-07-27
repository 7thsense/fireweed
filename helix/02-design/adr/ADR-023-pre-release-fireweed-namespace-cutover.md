---
ddx:
  id: adr-023-pre-release-fireweed-namespace-cutover
  links:
    - {kind: informed_by, to: name-and-positioning-analysis}
    - {kind: supersedes, to: adr-020-public-namespace-and-compatibility}
    - {kind: enables, to: adr-concrete-fireweed-facade-and-optional-controls}
  status: accepted
---

# ADR-023: Pre-release Fireweed namespace cutover

| Date | Status | Deciders | Related | Confidence |
| --- | --- | --- | --- | --- |
| 2026-07-25 | Accepted | Project maintainers | ADR-020, ADR-022, API-005 | High |

## Context

| Aspect | Description |
| --- | --- |
| Problem | ADR-020 preserved `pqueue` compatibility aliases, persisted names, paths, and wire identifiers. Those exceptions repeatedly reintroduced the retired name into current Fireweed work. |
| Current State | Fireweed has not had a public stable release. There is no supported installed base or persisted format that requires a dual-name migration period. |
| Requirements | One Fireweed identity; a backend-opaque `Fireweed` Rust interface; fresh test schemas and storage namespaces; immutable execution history remains auditable. |
| Decision Drivers | Pre-release freedom to break formats, the cost of a permanent compatibility surface, and the need for a residue gate that cannot hide current runtime names. |

## Decision

We will make a hard pre-release cutover from the retired `pqueue` identity to Fireweed.

Current executable and generated surfaces use Fireweed names only:

- Rust crates, types, modules, features, binaries, commands, package metadata, repository coordinates, images, services, charts, and release artifacts;
- environment variables, configuration keys, telemetry, error text, HTTP headers, RESP extensions, and other wire-visible identifiers;
- SQL tables, indexes, triggers, functions, schemas, migration objects, object-store prefixes, filesystem paths, and generated evidence directories;
- test fixtures, temporary paths, database schemas, buckets, examples, and active operator documentation.

No deprecated alias, fallback read, compatibility binary, dual-write, migration shim, or old-name format is shipped. Fresh schemas and storage roots are required for this pre-release cutover.

The only permitted retired-name occurrences are:

1. immutable DDx bead IDs and execution/audit paths whose identifiers already use the `pqueue-<hex>` form;
2. historical evidence quoted as a historical record, where rewriting would falsify that record; and
3. negative tests that name a retired API or namespace solely to prove it is rejected.

Those exceptions do not authorize executable aliases, accepted configuration, current examples, or persisted runtime identifiers. The identity verifier uses narrow path-and-pattern exceptions for them; it must not allow an entire source, documentation, chart, workflow, or script subtree.

## Alternatives

| Option | Pros | Cons | Evaluation |
| --- | --- | --- | --- |
| Preserve ADR-020 compatibility through `v0.20.0` | Lower migration risk for existing installations | Invents an installed-base constraint before release; doubles the supported namespace; masks residue | Rejected |
| Rename public packaging but retain storage and wire identifiers | Smaller immediate diff | Leaves the retired identity in every deployment and schema; requires a later migration | Rejected |
| Hard pre-release cutover | One coherent identity and one testable contract | Existing development databases and local paths must be recreated | Selected |

## Consequences

| Type | Impact |
| --- | --- |
| Positive | Every supported Fireweed surface uses the same name; no compatibility layer can leak into the external interface. |
| Negative | Existing development-only databases, object namespaces, environment files, and scripts using retired names stop working. |
| Mitigation | Tests create isolated fresh schemas/namespaces; operator documentation states the clean-start requirement; no migration code is added. |
| Neutral | Immutable bead IDs and historical evidence continue to contain the retired token as audit identifiers, not product names. |

## Risks

| Risk | Prob | Impact | Mitigation |
| --- | --- | --- | --- |
| Partial SQL rename compiles but fails at runtime | M | H | Rename definitions, queries, triggers, probes, and tests atomically; run the complete SQLite, PostgreSQL, Turso, and object-log durability matrices. |
| Broad replacement corrupts bead IDs or historical evidence | M | H | Replace explicit runtime token classes; never globally replace `pqueue-`; retain exact `pqueue-[0-9a-f]{8}` audit IDs. |
| Allowlist continues masking current residue | M | H | Reduce it to exact immutable-history and negative-test paths; make every other retired token fail CI. |

## Validation

| Success Metric | Review Trigger |
| --- | --- |
| The identity residue verifier reports zero unapproved retired-name occurrences | Any new allowlist entry covering executable/runtime code or a broad subtree |
| Public crate-boundary tests reject retired Rust symbols and constructors | A downstream crate can compile using a retired symbol |
| Fresh SQLite, PostgreSQL, object-log, and Turso schemas pass functional and close/reopen durability tests | Any backend requires an old table, path, prefix, or environment variable |
| Fireweed and Snorri release workflows consume only `FIREWEED_*` and Fireweed coordinates | A release path accepts or emits an old-name alias |

## Supersession

- **Supersedes**: ADR-020
- **Superseded by**: None

## Concern Impact

- **No concern impact**: this changes namespace and compatibility policy, not the selected architecture concerns.

## References

- `docs/helix/00-discover/name-and-positioning-analysis.md`
- `docs/helix/02-design/public-namespace-migration.yaml`
- `docs/helix/02-design/contracts/API-005-fireweed-rust-facade.md`
- `docs/helix/03-test/test-plans/TP-004-fireweed-facade-and-snorri-acceptance.md`
