---
ddx:
  id: adr-concrete-fireweed-facade-and-optional-controls
  depends_on:
    - api-native-client-interface
    - adr-engine-enforced-coordination-and-encapsulated-library-surface
    - adr-020-public-namespace-and-compatibility
  status: accepted
---

# ADR-022: Concrete Fireweed facade and optional controls

| Date | Status | Deciders | Related | Confidence |
| --- | --- | --- | --- | --- |
| 2026-07-24 | Accepted | Project maintainers | API-001, API-004, API-005, ADR-009, ADR-020 | High |

## Context

The renamed crate still exposes `Pqueue<B>`, `EmbeddedPqueue<B>`, and the
`LibBackend` bound. A downstream application therefore carries Fireweed's
backend choice through its own public types. Snorri demonstrates the cost: its
state-store adapter is generic over `B: LibBackend`, keeps a local
`Plain`/`Embedded` enum, imports a second core crate, and duplicates projection
lifecycle types solely to normalize Fireweed's construction results.

The previous runtime-composition review mixed four different questions:

1. the supported Rust facade;
2. optional runtime controls;
3. backend and lifecycle implementation correctness; and
4. downstream migration and release evidence.

Only the first two determine the external interface. Storage recovery,
concurrency, observability, and operational policy remain important, but they
must be reviewed as implementation subjects rather than expanding the facade
until it becomes an operations framework.

## Decision

### One concrete handle

The supported Rust entry point is a concrete, non-generic `Fireweed` type.
Callers do not name, provide, or retain a backend type. `Pqueue`,
`EmbeddedPqueue`, `LibBackend`, raw backend constructors, and `Pqueue::new` are
not part of the supported external surface.

`Fireweed` retains the native queue operations as inherent methods. Renaming
the handle must not also force Snorri or another consumer to import traits just
to call `push`, `claim`, `commit`, query, or mutation operations. Public traits
may mirror coherent portions of the inherent interface later for generic
libraries, but those traits are not runtime capability switches and are not a
release prerequisite.

The v0.20 supported surface is the existing public queue facade, not only the
methods exercised by Snorri. API-005 identifies the Snorri slice separately so
it can be delivered first without being mistaken for the complete facade.

The implementation may keep a generic internal handle while migrating
first-party code. Runtime erasure occurs above the storage ports through a
private, object-safe adapter over the high-level queue operations. The storage
ports themselves remain non-object-safe and are not redesigned for this work.

### Optional controls describe optional authority, not ordinary projection use

Memory, SQLite, and object-log profiles all use projections to serve queue
reads. The optional value therefore cannot mean "this runtime has a
projection." What is optional is authority to perform destructive or recovery
maintenance on a disposable projection.

`Fireweed` exposes that authority through:

```rust
pub fn projection_control(&self) -> Option<ProjectionControl<'_>>;
```

`ProjectionControl` is a borrowed view and exposes only capability inspection,
verification, deletion, and rebuild. Append, claim, commit, update, and query
operations remain on `Fireweed`. The borrowed lifetime prevents projection
maintenance from becoming a detached lifecycle owner after its `Fireweed`
instance is gone. `Arc<Fireweed>` remains sufficient for applications such as
Snorri: a control borrow may cross an `.await`, but cannot outlive the
`Fireweed` value owned by the `Arc`.

Queue-scoped support remains authoritative at the point where it already
varies: `commit_capabilities(&QueueKey)` and
`hot_projection_capabilities(&QueueKey)`. A runtime descriptor may report
construction facts such as authority, projection class, response barrier, and
coordination mode, but it must not duplicate those queue-scoped capability
booleans.

`projection_control().is_some()` reports owned maintenance authority only. It
does not report whether queue reads use a projection and does not replace
`hot_projection_capabilities`.

### Configuration names describe roles

The `Embedded*` prefix is retired from the supported interface. Those values
describe an authoritative object log, a disposable projection, response
barrier, segmentation, and recovery policy; they do not describe whether the
library is embedded in a host process.

The first usable build preserves the role-correct feature-gated `open_*` free
functions while changing every return type to `Fireweed`. The
`open_embedded*` functions and `Embedded*` configuration values are replaced by
the exact role-named object-log surface in API-005. A unified
`Fireweed::open(config)` may follow without changing the handle or operation
surface. Synchronous and asynchronous construction remain distinct where
PostgreSQL initialization cannot safely block inside a Tokio runtime.

### Review and delivery boundaries

The work is reviewed in four bounded packets:

| Packet | Governing question | Release role |
| --- | --- | --- |
| Rust facade | Is one concrete `Fireweed` sufficient for the full supported native queue surface? | Blocking |
| Optional controls | Does `ProjectionControl` expose only supported maintenance authority with correct ownership? | Blocking for object-log profiles |
| Snorri migration | Can Snorri remove backend generics and its direct core dependency and pass its backend matrix? | Blocking acceptance client |
| Runtime hardening | Are maintenance concurrency, recovery, fault injection, and observability complete? | Separate beads; blocking only when an exposed operation lacks its existing safety contract |

Historical review rounds that combine these packets are discovery input, not a
governing interface specification.

## Alternatives

| Option | Pros | Cons | Evaluation |
| --- | --- | --- | --- |
| Keep `Pqueue<B>` and `EmbeddedPqueue<B>` public | No internal dispatch layer; no first-party migration | Every consumer carries Fireweed backend types and normalizes profile-specific wrappers | Rejected: preserves the consumer problem this decision exists to remove |
| Erase the storage ports behind `dyn LibBackend` | One internal representation | Existing RPITIT and associated projection types make the storage ports non-object-safe | Rejected: would turn an interface refactor into a storage-port redesign |
| Expose one concrete `Fireweed` through a private high-level dispatch seam | Backend choice is private; operation semantics remain at the existing facade | Requires explicit forwarding coverage and a public-API closure gate | Selected: solves downstream encapsulation without redesigning storage ports |
| Put queue operations on optional component objects | Makes composition visible in the type graph | Forces capability branching for ordinary queue use and confuses projection maintenance with queue authority | Rejected: optional controls are narrower than the queue interface |

## Consequences

- Snorri can store `Arc<Fireweed>` and delete its generic backend parameter and
  `Plain`/`Embedded` normalization enum.
- The `fireweed` crate must re-export every domain type appearing in its public
  signatures, including `WorkerId`; consumers do not depend on `fireweed-core`.
- Existing queue verbs and semantics remain stable during the rename.
- `read_as_of<T, F>` cannot cross the erased boundary because its callback
  names a backend-associated projection type. A backend-neutral history/query
  contract must replace it before it becomes part of the supported facade.
- `batch_update` remains an API-001 operation. Its facade method returns the
  structured unsupported result on profiles without the capability; internal
  conditional dispatch must not leak a backend bound.

## Risks

| Risk | Prob | Impact | Mitigation |
| --- | --- | --- | --- |
| The erased facade forwards only the Snorri slice and silently drops existing public methods | M | H | API-005 names the full method closure; TP-004 compares a normalized public-API baseline and compiles representative calls from every family |
| Legacy Rust symbols become a second supported interface | M | H | Make them unavailable to external crates; ADR-020 package/name aliases do not authorize legacy Rust facade or configuration types |
| Backend identity leaks back into consumer control flow | L | H | Backend/projection choice is accepted only during construction; the live facade exposes behavior and optional controls, not composition introspection |
| Projection maintenance is detached from the owning runtime | L | H | Return a borrowed `ProjectionControl<'_>`; do not expose an owned cloneable lifecycle handle |

## Validation

| Success metric | Review trigger |
| --- | --- |
| Snorri stores `Arc<Fireweed>` with no `LibBackend` parameter or direct internal-crate dependency | A supported Snorri backend profile cannot compile or activate through the concrete facade |
| Every pre-refactor supported facade family remains callable on `Fireweed`, except the explicit `read_as_of` exclusion | The public-API closure diff removes an unreviewed method or named DTO |
| Projection maintenance is available only as a borrow from its owning `Fireweed` | A caller can clone or retain maintenance ownership after dropping `Fireweed` |
| Queue-scoped capability checks remain authoritative | Backend identity or projection-control presence is used to authorize commit or hot-query behavior |

## Superseded guidance

This ADR supersedes ADR-009 only where ADR-009 rejects a concrete runtime
handle and accepts `Pqueue<impl LibBackend>` as the consumer-facing type. It
does not change ADR-009's coordination, fencing, publication-topology, or
authorization decisions.

## Concern impact

No concern selection changes. This decision tightens the existing Rust library
boundary and does not override a practice recorded in
`docs/helix/01-frame/concerns.md`.

## References

- `docs/helix/01-frame/prd.md` — one transaction contract without exposing the
  selected storage implementation.
- `docs/helix/00-discover/public-preview-boundary.md` — supported profile and
  publishability boundary.
- `docs/helix/02-design/contracts/API-005-fireweed-rust-facade.md` — exact Rust
  binding contract.
- `docs/helix/03-test/test-plans/TP-004-fireweed-facade-and-snorri-acceptance.md`
  — executable acceptance gates.
- `../snorri/crates/snorri-pqueue/src/lib.rs` — acceptance-client inventory.
