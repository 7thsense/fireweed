---
ddx:
  id: tp-fireweed-facade-and-snorri-acceptance
  depends_on:
    - api-fireweed-rust-facade
    - adr-020-public-namespace-and-compatibility
  status: accepted
---

# TP-004: Fireweed facade and Snorri acceptance

## Testing strategy

This plan verifies the Rust facade, its first downstream consumer, and
per-constructor behavioral parity. Existing backend and recovery suites remain
supporting evidence; they do not substitute for running one capability-complete
suite through every public constructor.

**Goals**: exact API-005 signature/export closure; concrete-handle parity;
full operation-family parity across every supported constructor;
Snorri compilation and semantic acceptance against the same Fireweed revision;
tagged GitHub-source consumption evidence for the release candidate
**Out of scope**: non-Cargo
release artifacts, and runtime-hardening work not exposed by API-005
**Traceability source**: ADR-022 and API-005

| Level | Coverage target | Priority |
| --- | --- | --- |
| Contract compile | 100% of API-005 constructors, Snorri methods, named types, and forbidden legacy Rust names | P0 |
| Fireweed integration | The same capability-complete operation suite succeeds through every supported constructor | P0 |
| Million-cycle parity | Insert 1,000,000, batch-update 500,000, and read/verify 1,000,000 through every supported constructor | P0 |
| Downstream integration | All five Snorri feature combinations compile against one concrete type | P0 |
| Garage integration | SQLite and PostgreSQL projection lifecycle plus retry/idempotency run against Garage on `eldir` without skips | P0 |
| Published-source consumption | Snorri resolves the tagged public `telepathdata/fireweed` repository rather than a workspace-internal crate | P0 |
| Existing backend suites | No regression in the selected profile's current semantic tests | P0 |

## Test data

Fireweed integration tests use unique temporary SQLite/object-log paths and
synthetic queue/item identifiers. PostgreSQL tests use the existing
`PQUEUE_PG_TEST_URL` gate and unique schemas. No production or customer data is
required.

## Coverage requirements

| Metric | Target | Minimum | Enforcement |
| --- | --- | --- | --- |
| API-005 Snorri method signatures | 100% compile referenced | 100% | downstream compile fixture blocks |
| API-005 Snorri named types | 100% importable from `fireweed` | 100% | single-package fixture blocks |
| Construction-only composition boundary | 100% of enabled constructors | 100% | public-API and compile-fail tests block |
| Per-constructor operation parity | Every inherent `Fireweed` method family succeeds; zero construction-dependent `Unavailable` results | 100% | shared conformance matrix blocks |
| Legacy Rust facade/config names | 0 externally constructible | 0 | compile-fail fixture blocks |

## Fireweed gates

1. A downstream fixture depends only on package `fireweed`.
2. The fixture constructs every enabled profile and assigns each result to the
   same concrete `Fireweed` type.
3. The fixture names every API-005 input/output type required by Snorri without
   depending on `fireweed-core` or `fireweed-engine`.
4. Compile-fail fixtures reject attempts to construct with a raw backend or use
   `Pqueue`, `Pqueue::new`, `EmbeddedPqueue`, `EmbeddedHandle`, `LibBackend`,
   or any `Embedded*` configuration name. ADR-020 package aliases do not exempt
   these Rust symbols.
5. One shared suite creates a queue, appends, claims, mutates, commits, queries,
   and finalizes through every supported constructor. It MUST include
   `batch_update` and `live_items`; `Unavailable`, a skip, or substituting a
   different method is failure. Merely receiving `Ok` is not evidence: every
   operation asserts its returned value and an independent observable
   postcondition. Empty claim results may not skip finalize/renew/reassign
   assertions; empty query, discovery, index, mutation, commit, or recovery
   results fail when seeded data requires a non-empty result. Gate coverage
   uses an item with real gate membership and proves both blocked and unblocked
   claim behavior.
6. Plain profiles return `None` from `projection_control`. Object-log plus
   disposable-projection profiles return `Some` and retain existing lifecycle
   verification/delete/rebuild tests.
7. No constructor result exposes backend/projection identity, configuration,
   a backend discriminator, or a downcast. Capability assertions remain
   separate and queue-scoped.
8. New contract tests are required under `crates/fireweed/tests/` for the
   supported-API compile fixture, compile-fail fixture, opaque-composition
   boundary, and full forwarding closure. Until those files exist and pass,
   this plan makes no claim that the gate is implemented.
9. Run:

```sh
cargo fmt --all -- --check
cargo check -p fireweed --no-default-features
cargo check -p fireweed --no-default-features --features memory
cargo check -p fireweed --no-default-features --features sqlite
cargo check -p fireweed --no-default-features --features objectlog
cargo test -p fireweed --all-features
scripts/verify-public-crate-boundary.sh
scripts/verify-public-artifact-topology.sh
git diff --check
```

10. Only after the complete shared functional suite and every durability row
    pass with zero skips and zero construction-dependent `Unavailable`
    results, run the TP-005 `million-cycle-v1` gate. A constructor that has
    not passed those prerequisites is ineligible for performance testing, not
    a slower or reduced benchmark cell. Each eligible constructor must insert
    1,000,000 keyed items in at most 9 seconds, batch-update 500,000 in at most
    9 seconds, and read plus verify 1,000,000 in at most 9 seconds. Durable
    constructors additionally close, reopen, and verify the exact final image
    outside timed phases. No row may skip an operation or report
    `Unavailable`.

Crates.io package closure is a separate follow-up. The v0.20 GitHub release
MUST NOT publish repository-only internal crates merely to make the facade's
current path dependencies registry-resolvable.

Environment-gated PostgreSQL tests remain explicitly reported rather than
silently counted as passing when no database is available.

The public-API closure comparison normalizes away the root-type rename and the
explicit API-005 exclusions, then fails on any other removed public method or
named DTO. Merely compiling the Snorri slice is insufficient.

## Snorri gates

The sibling Snorri checkout is the acceptance client. Its migration removes
the `pqueue` and `pqueue-core` dependencies, adds only `fireweed`, replaces
`PqueueStateStore<B>` with a non-generic store holding `Arc<Fireweed>`, and
deletes its `Plain`/`Embedded` handle enum.

Before running any Snorri command, the acceptance record MUST prove dependency
identity. Pre-release testing uses a path dependency on
`../fireweed/crates/fireweed` or an exact Fireweed commit revision. Release
testing uses the exact v0.20 tag from the public `telepathdata/fireweed`
repository. `cargo tree` and `Cargo.lock` evidence MUST show package `fireweed`
at the intended path, revision, or tagged git source; the old
`7thsense-pqueue` source is a hard failure even if compilation succeeds from
cache.

Compile each feature profile independently before `--all-features`:

```sh
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features memory
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features sqlite
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features postgres
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features objectlog,sqlite
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features objectlog,postgres
cargo check --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --all-features
```

Required local semantic tests cover memory conformance, SQLite commit and
reopen, SQLite hot queries, and object-log/SQLite verify-delete-rebuild and
worker reassignment. PostgreSQL and object-log/PostgreSQL tests run when the
documented test database variable is available and are otherwise recorded as
not run.

```sh
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features memory,conformance -- --list | rg '^tests::pqueue_memory_state_store_conformance_executes: test$'
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features memory,conformance tests::pqueue_memory_state_store_conformance_executes -- --exact --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features sqlite sqlite_public_facade_commits_authoritative_transition -- --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features sqlite hot_projection_sqlite_visibility_business_cases -- --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features objectlog,sqlite objectlog_sqlite_delete_and_rehydrate -- --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features objectlog,sqlite objectlog_sqlite_worker_reassignment_recovers_deleted_projection -- --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features postgres postgres_public_facade_is_env_gated_and_capability_checked -- --nocapture
cargo test --manifest-path ../snorri/Cargo.toml -p snorri-pqueue --no-default-features --features objectlog,postgres objectlog_postgres_delete_and_rehydrate -- --nocapture
```

Each targeted command MUST report at least one executed test. A successful
process with `running 0 tests` is a failed gate, including when a feature gate
or a non-exact filter silently excludes the named test.

The release acceptance record additionally runs the real S3-compatible matrix
on host `eldir`, where Garage and PostgreSQL are provisioned. Working SSH
access to `eldir` is a prerequisite; inability to reach the host blocks this
P0 row rather than converting it to a local skip. The runner exports
`SNORRI_S3_TEST_ENDPOINT`, `SNORRI_S3_TEST_BUCKET`,
`SNORRI_S3_TEST_REGION`, `SNORRI_S3_TEST_ACCESS_KEY`,
`SNORRI_S3_TEST_SECRET_KEY`, and `SNORRI_FIREWEED_POSTGRES_URL` from host-managed
secrets. Values MUST NOT be copied into the repository or logs. From the Snorri
checkout pinned to the accepted Fireweed revision, run:

```sh
cargo test -p snorri-embed --no-default-features --features objectlog-s3,postgres --test garage_objectlog -- --nocapture
```

The tests `garage_round_trip_reopen_and_rebuild`,
`garage_postgres_round_trip_delete_and_rebuild`, and
`garage_retry_push_is_applied_exactly_once` MUST each execute and pass. A
missing-variable `SKIP` line or `running 0 tests` is a failed P0 row, not
passing evidence.

## Acceptance criteria layer allocation

| Requirement source | Primary layer | Blocking evidence |
| --- | --- | --- |
| ADR-022 concrete ownership | Contract compile | One non-generic `Fireweed`; forbidden legacy names fail to compile |
| API-005 full facade closure | Fireweed integration | Normalized public-API comparison plus the complete shared suite on every constructor |
| API-005 per-constructor parity | Fireweed integration + local performance | Zero `Unavailable` results; exact million-cycle outcomes and phase ceilings for every constructor |
| API-005 Snorri slice | Downstream integration | Independent feature checks and semantic tests above |
| API-005 projection control | Fireweed + Snorri object-log integration | Borrowed control verify/delete/rebuild and reassignment tests |
| API-005 opaque composition | Contract compile | Construction selects composition; the live facade cannot disclose it |
| Garage durability acceptance | Snorri on `eldir` | All three named Garage tests execute without skips and pass |
| Published facade consumption | GitHub release + downstream integration | Public tag/release exists; post-publication lockfile resolves the exact `telepathdata/fireweed` tag |

## Infrastructure and implementation order

1. Add compile-success/compile-fail and opaque-composition tests before
   completing facade forwarding; these tests define the supported closure.
2. Add the zero-skip shared functional and durability matrix.
3. Complete all `Fireweed` forwarding and backend implementations until that
   matrix is green.
4. Run Fireweed profile tests and boundary scripts.
5. Run TP-005 performance gates only for the already-green matrix.
6. Migrate Snorri against the exact local commit and run independent feature
   checks before semantic tests.
7. Run the no-skip Garage matrix on `eldir` against the release candidate.
8. Publish the GitHub tag/release, then repeat dependency identity and the full
   Snorri gates against that public tag.

Local developer runs may report PostgreSQL rows as `not run` when the URL is
absent. Release acceptance requires both Garage and PostgreSQL on `eldir`; no
environment-gated row may be skipped there.

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Snorri resolves its old git pin | High | Record `cargo tree` and lockfile source before accepting results |
| Only the Snorri slice is forwarded | High | Full normalized public-API closure blocks independently |
| A method exists on `Fireweed` but only some constructors install it | Critical | Shared per-constructor suite and million-cycle gate treat `Unavailable` as failure |
| Compile-fail and compatibility policy disagree | High | API-005 makes legacy Rust types unavailable; ADR-020 aliases are package-only |
| PostgreSQL is silently skipped | Medium | Report as `not run`; never count as a pass |
| Garage credentials leak into evidence | High | Source host-managed secrets and reject credential values in logs |

The concrete facade, sibling Snorri migration, supported-surface fixture, and
compile-fail fixtures are implemented. Release acceptance still requires the
exact-revision `eldir` Garage run and post-publication tagged-source repeat;
local or path-based success does not substitute for either row.

## Build handoff

**Priority**: contract fixtures → forwarding closure → Fireweed matrix → Snorri
matrix → `eldir` Garage matrix → public GitHub tag/release → tagged-source repeat
**Blocking gate**: every P0 row above passes against one recorded Fireweed
revision with zero phantom test claims and no unreviewed supported-API removal.

## Failure policy

- A Fireweed regression is fixed in Fireweed before changing Snorri around it.
- A missing domain re-export is fixed at the Fireweed facade; Snorri must not
  restore a direct internal-crate dependency.
- Backend-specific behavior must not reintroduce a generic parameter into the
  downstream store.
- Operational hardening findings become focused follow-up beads. They block
  this release only when they invalidate an operation already exposed by
  API-005 or its existing safety guarantees.
