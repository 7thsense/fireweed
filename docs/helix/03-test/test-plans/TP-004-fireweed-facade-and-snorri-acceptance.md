---
ddx:
  id: tp-fireweed-facade-and-snorri-acceptance
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - api-workload-integration-profiles
    - adr-023-pre-release-fireweed-namespace-cutover
    - tp-verification-acceptance-criteria
  status: accepted
---

# TP-004: Fireweed public surface and consumer acceptance

## Scope

This plan verifies two things that the contract suite cannot:

- **The public surface.** The `fireweed` package exposes the kernel records
  contract and nothing else: `Fireweed` wraps `Arc<dyn Store>` and delegates
  1:1 to the eight `Store` ops, beside the S3×Turso constructor and its admin
  handle (ADR-025 §1, API-006 §1 and §7).
- **Consumer acceptance.** Snorri, the first consumer, runs on the kernel
  contract as API-003 maps it, and it resolves Fireweed from the intended
  source.

The contract itself is verified by `fireweed-contract-suite` (TP-003 AC-KC-1
to AC-KC-3); this plan builds on it and never re-proves it. Snorri tests its
own workflows, not the contract.

| Level | Coverage target | Priority |
| --- | --- | --- |
| Contract suite prerequisite | AC-KC-1 to AC-KC-3 pass on the revision under test | P0 |
| Public surface | 100% of the public items of `fireweed` trace to API-006 or to the S3×Turso constructor and admin handle | P0 |
| Operator separation | no `Store` value converts into an `Operator` (OP1); Snorri never references the `Operator` trait, `force_clear` or the admin handle (OP4, TP-003 AC-KC-4) | P0 |
| Snorri acceptance | every Snorri semantic id below executes and passes on S3×Turso | P0 |
| Live provider | the Snorri durability ids run on a provisioned runner with an attested S3 provider and zero skips | P0 |
| Published-source consumption | Snorri resolves the tagged public `7thsense/fireweed` repository | P0 |

## Surface gates

1. A downstream fixture depends on no Fireweed package other than `fireweed`
   and `fireweed-contract`, and names every API-006 type it uses through them.
2. The fixture calls the S3×Turso constructor in `fireweed`, which returns the
   `Fireweed` handle together with `TursoAdmin` (ADR-009, ADR-024).
3. A normalized public-API comparison of `fireweed` fails on any public item
   that is not one of the eight ops, an API-006 type, the S3×Turso constructor
   and its configuration, or the admin handle. No backend type, capability
   flag, log position, storage selector or alias is reachable from a consumer
   (ADR-025, Validation).
4. Compile-fail fixtures prove that no `Store` or `Fireweed` value converts
   into an `Operator` (OP1), and that the names ADR-023's cutover removed do
   not resolve; ADR-023 permits no alias.
5. A forwarding test wraps a recording `Store` in `Fireweed` and checks that
   each `Fireweed` method calls the matching `Store` op once, with the same
   arguments, and returns its result unchanged.
6. Neither `Fireweed` nor `Store` exposes backend identity, a backend
   discriminator or a downcast (API-006 §8, CS4).
7. Projection control lives on the admin handle. Tests that verify, delete or
   rebuild the projection call it; they never unlink the Turso files and
   reopen instead.
8. Run:

```sh
cargo fmt --all -- --check
cargo test -p fireweed --all-features
scripts/verify-public-crate-boundary.sh
scripts/verify-public-artifact-topology.sh
git diff --check
```

Each gate's test must exist and pass before this plan counts the gate as met.

## Snorri gates

The sibling Snorri checkout is the acceptance client. It depends on `fireweed`
only, holds an `Arc<Fireweed>`, and uses the eight ops. The kernel has no
side records, fences, record-kind filters or operator helpers on the data
plane, so a Snorri build that compiles against `fireweed` uses none of them.

**Dependency identity.** Before any Snorri command runs, the acceptance record
proves which Fireweed it built against. Pre-release testing uses a path
dependency on `../fireweed/crates/fireweed` or an exact Fireweed commit.
Release testing uses the exact release tag of the public `7thsense/fireweed`
repository. `cargo tree` and `Cargo.lock` show `fireweed` at the intended
path, commit or tag; any other source fails the gate, even when the build
succeeds from a cache.

**Operator ban.** The TP-003 AC-KC-4 guard runs over Snorri. Snorri cancels
and repairs without the operator: it blocks gates and writes by key, and
retries a write that gets `Leased` once the holder settles or the lease
expires.

**Semantic ids.** Required Snorri evidence is keyed by stable ids, not test
file names. The acceptance record maps each id to the Snorri test binary and
filter that ran it at the accepted revision.

| ID | Required semantics |
| --- | --- |
| `SNORRI-LIFECYCLE` | Snorri's workflow lifecycle on S3×Turso, as API-003 maps it: each workflow instance is a group; record kinds are stages, and every claim names its stage; each transition settles its input with `Release(t)` and writes the per-transition history into fields of the settled record; the instance record lives in a stage that nothing claims and is updated with `Expect::Version`; awaits are read by key. |
| `SNORRI-EXCLUSIVITY` | Snorri claims an instance's stages only with `WholeGroups`, so two workers never hold records of one instance at once, across stages (C6a). |
| `SNORRI-MANIFEST` | The shard manifest lives in queue metadata: one metadata CAS bootstraps it, `read{queue: true}` reads it fresh, and a concurrent bootstrap loses with `MetadataVersionMismatch` (W13, R4). |
| `SNORRI-CANCEL` | `CancelRun` with no operator (API-003 P13, §3.5): a gate op in each shard queue blocks the run's gate, so no claim takes the run's records; each Free record of the run in a work stage moves by key to its kind's cancelled stage; a move that gets `Leased` is retried after the holder settles or the lease expires; at the end no record of the run is left in a work stage. |
| `SNORRI-REOPEN` | After a restart over the same durable state, Snorri resumes with every acknowledged transition, instance record and manifest intact (DUR1). |
| `SNORRI-RETRY-ONCE` | A lost response resent with the same `request_id` returns the recorded outcome with `replayed: true` and applies once; a conflicting body gets `RequestIdConflict` (RQ2). |

Each targeted command reports at least one executed test. A successful process
with `running 0 tests` is a failed gate, including when a feature gate or an
inexact filter silently excludes the named test.

**Live provider.** The release record also runs `SNORRI-REOPEN` and
`SNORRI-RETRY-ONCE` on a provisioned qualification runner against a live S3
provider. The runner attests, before construction and without secrets, its
identity and topology, and the provider's version, region, native atomic
conditional create and update, consistency contract, TLS mode and
bucket-ownership acknowledgement. The contract depends on those capabilities,
not on a provider brand or host. The runner exports
`SNORRI_S3_TEST_ENDPOINT`, `SNORRI_S3_TEST_BUCKET`, `SNORRI_S3_TEST_REGION`,
`SNORRI_S3_TEST_ACCESS_KEY` and `SNORRI_S3_TEST_SECRET_KEY` from host-managed
secrets; their values never enter the repository or logs. A missing
variable, an unattested provider, `running 0 tests` or an unmapped id fails
the P0 row; it is never a passing skip. Developer runs may report the live rows
as `not run`, which is never qualification evidence.

## Seventh Sense

Seventh Sense is written in Scala, so its own cutover needs a transport over
`Store`, which the contract leaves for later (API-006 §8, ADR-025 §4). Until
that transport exists, its acceptance is the in-process product workflows
that model its shapes (TP-003 AC-E2E-1 to AC-E2E-4 and AC-E2E-9).

## Acceptance criteria layer allocation

| Requirement source | Primary layer | Blocking evidence |
| --- | --- | --- |
| API-006 conformance | Contract suite | TP-003 AC-KC-1 to AC-KC-3 on the revision under test |
| ADR-025 §1 public surface | Contract compile and public-API comparison | Surface gates 1 to 6 |
| API-006 OP1 and OP4 | Compile-fail fixture and CI guard | Surface gate 4; TP-003 AC-KC-4 over Snorri |
| API-003 Snorri profile | Downstream integration | The six Snorri semantic ids |
| DUR1 and RQ2 on a live provider | Provisioned runner | `SNORRI-REOPEN` and `SNORRI-RETRY-ONCE` against attested S3 with zero skips |
| Published source | Release and downstream integration | The public tag exists, and Snorri's lockfile resolves it |

## Implementation order

1. Add the compile-success, compile-fail, public-API comparison and
   forwarding tests. They define the supported surface.
2. Pass the contract suite on the revision (TP-003 AC-KC-1 to AC-KC-3).
3. Run the surface gates and boundary scripts.
4. Migrate Snorri against the exact local commit and run its semantic ids.
5. Run the live-provider rows on the attested qualification runner against
   the release candidate.
6. Publish the tag, then repeat dependency identity and every Snorri gate
   against that public tag.

TP-005 timing runs only on a revision that has passed the contract suite
(TP-005 "Functional prerequisite").

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Snorri resolves an old Fireweed source | High | Record `cargo tree` and the lockfile source before accepting results |
| A consumer-shaped method is added to `Fireweed` | High | The public-API comparison fails on any item that does not trace to API-006; a consumer need becomes a clause change (ADR-025 §4) |
| Snorri reaches force-clear | High | The AC-KC-4 guard over Snorri; operator-free cancel (`SNORRI-CANCEL`) |
| A live row is skipped silently | Medium | Report it as `not run`; never count it as a pass |
| Provider credentials leak into evidence | High | Runner-managed secrets; credential values rejected in logs |

## Failure policy

- A Fireweed defect is fixed in Fireweed, not worked around in Snorri.
- A consumer need becomes a contract change only through a clause change in
  API-006, plus an ADR when it changes the model; it is never a
  consumer-shaped method on `Fireweed` (ADR-025 §4).
- Snorri never restores a dependency on an internal Fireweed crate.
- Operational hardening findings become focused beads. They block acceptance
  only when they break an API-006 guarantee.
