---
ddx:
  id: adr-queue-template-and-exact-ensure
  depends_on:
    - api-native-client-interface
    - adr-queue-as-shard-unit-and-projection-families
    - build-foqs-inspired-interface-and-boundary
  links:
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: governs, to: build-foqs-inspired-interface-and-boundary}
  status: accepted
---

# ADR-018: Queue templates and exact ensure are a Rust-library additive contract

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-23 | Accepted | Project owner | API-001, ADR-008, B-004, B-010, B-013, B-014, B-015, B-016 | High |

## Context

API-001 already defines `CreateQueue` as the native control-plane operation: a queue is created from a full
`QueueDefinition`, the response carries the stored effective definition and a `created` boolean, and repeated
creation with an incompatible definition fails with `queue-definition-conflict`. That contract remains the
transport-neutral surface for direct queue creation.

The Rust library needs one additive convenience for applications that create many similarly configured
queues: callers should be able to define one queue configuration template, resolve it for a concrete
`QueueKey`, atomically create the queue if absent, and then prove that an existing queue's stored effective
definition is exactly the resolved desired definition. The convenience must not add a template registry,
loosen create semantics, hide backend races, or make push/claim implicitly create queues.

Current backend create compatibility is narrower than full `QueueDefinition` equality in some planes. Exact
ensure therefore cannot trust "compatible create" as proof that every stored field matches the caller's
desired definition. This ADR records the Rust-library contract that B-004 implements and the atomic-create
prerequisites that B-010 and B-013 through B-016 must satisfy.

## Decision

### 1. QueueTemplate is caller-owned and non-durable

`QueueTemplate` is a public Rust-library type owned entirely by the caller. It is not a server resource, not
a durable registry entry, not a control-plane table row, and not persisted into `QueueDefinition`.

A template contains:

| Field | Rule |
|-------|------|
| Keyless queue specification | The complete `CreateQueue` shape except `tenant_id` and `queue_id`. Construction must be based on an exhaustive destructure of `CreateQueue`, so adding a create field fails compilation until the template mapping is updated. |
| Pinned `QueueCreationPolicy` | The policy used to validate and resolve the template. The Rust library resolves this policy caller-side; servers and backends do not reinterpret or supply it for templates. |
| Optional `template_name` | Caller diagnostic only. It does not participate in equality, storage identity, queue routing, or authorization. |
| Optional `template_revision` | Caller diagnostic only. It is returned in ensure diagnostics but is not stored as provenance. |

Template identity is the keyless queue specification plus the pinned `QueueCreationPolicy`. The optional
name and revision are diagnostics, not identity. Prototype `QueueKey`, `TenantId`, or `QueueId` values used
while building a template are discarded before the template is stored in memory and must not affect template
identity or diagnostics.

### 2. Resolution injects a QueueKey and validates through CreateQueue

Resolving a template for a `QueueKey` produces a concrete desired `QueueDefinition` by:

1. injecting the key's `tenant_id` and `queue_id` into a `CreateQueue`;
2. applying the template's pinned `QueueCreationPolicy`;
3. calling `CreateQueue::validate`; and
4. returning the resulting effective `QueueDefinition`.

Validation failures are caller-visible `EnsureQueueError` values; they are not backend calls. Any schema or
index type needed to build a template through the public facade must either be re-exported by `fireweed` or
documented as a required direct dependency.

Because `QueueDefinition` includes decimal priority/index-related values that implement `PartialEq` but not
`Eq`, exact ensure is defined in terms of `PartialEq` structural equality. Tests must assert the ordinary
reflexive case used by fireweed definitions, including `resolved == resolved.clone()`, and must avoid values
whose comparison would make that assertion false.

### 3. EnsureQueue is an explicit Rust-library operation

The Rust library exposes:

```rust
Fireweed::ensure_queue(&QueueKey, &QueueTemplate) -> Result<EnsureQueueOutcome, EnsureQueueError>
```

`ensure_queue` is explicit. No `push`, `claim`, `ack`, `nack`, `renew`, `discover`, metrics, query, or other
data-plane path may call it implicitly. Direct `create_queue` remains available and remains governed by
API-001; this ADR does not change direct create compatibility or claim that direct create has exact-drift
behavior.

The operation sequence is:

1. resolve the template into the desired effective `QueueDefinition`;
2. call backend `CreateQueue` atomically with that desired definition;
3. obtain the authoritative stored effective `QueueDefinition`;
4. compare the full desired and stored definitions with `QueueDefinition::eq`;
5. return success only when the definitions are exactly equal.

The library must not perform a check-then-create sequence. The atomic create-or-read property belongs to the
backend family named below.

### 4. EnsureQueueOutcome is facade-local

`EnsureQueueOutcome` is a `fireweed` facade-local result type, distinct from engine and wire response types.
It carries:

| Field | Meaning |
|-------|---------|
| `created: bool` | `true` if this ensure call created the queue; `false` if it observed an existing exact match. |
| `definition: QueueDefinition` | The authoritative stored effective definition returned or read after create. |
| `template_name: Option<String>` | Non-durable diagnostic copied from the template. |
| `template_revision: Option<String>` | Non-durable diagnostic copied from the template. |

The `created` flag is required because fireweed has no rollback/delete primitive. A conflict after a
successful create still means the queue may now exist and callers need to know whether this operation
created it.

### 5. EnsureQueueError is facade-local and typed

`EnsureQueueError` is also local to the Rust facade. It does not replace `EngineError`, `CreateQueueError`,
RESP errors, or API-001 error codes. It has at least these variants:

| Variant | Carries | Rule |
|---------|---------|------|
| `Validation` | template diagnostics and the `CreateQueueError` or validation message | Returned when resolving the template fails before backend effects. |
| `Backend` | template diagnostics and the backend error | Returned for backend failures that are not definition drift/conflict. |
| `DefinitionConflict` | `created`, desired `QueueDefinition`, stored `QueueDefinition`, `template_name`, `template_revision` | Returned when the stored effective definition differs from the resolved desired definition, regardless of which field differs. |

If backend `CreateQueue` returns its legacy `QueueDefinitionConflict` error instead of a stored definition,
`ensure_queue` must read the authoritative stored definition for the `QueueKey` before returning
`EnsureQueueError::DefinitionConflict`. If the follow-up read itself fails, the error is a backend failure
with template diagnostics because the facade cannot prove the stored definition.

### 6. Drift behavior is exact and field-complete

Exact ensure rejects every stored/desired `QueueDefinition` difference, including fields current create
compatibility may ignore:

| Drift class | Required behavior |
|-------------|-------------------|
| Priority model, ordering mode, rank error, progress bound | Return `DefinitionConflict` with both definitions. |
| Eligibility, gate caps from `QueueCreationPolicy`, cohort, recurrence, retry, retention, lease, and batch limits | Return `DefinitionConflict` with both definitions. |
| Secondary indexes, typed indexes, entity schema, and change-record emission | Return `DefinitionConflict` with both definitions. |
| Any future `QueueDefinition` field | Fail compilation in drift tests until the field is covered by exact equality and fixture mutation coverage. |

Runtime code must use the single direct `QueueDefinition::eq` comparison. Tests may use helpers to mutate
every current field, but those helpers must not become a second production comparison implementation.

### 7. Reopen and legacy-definition semantics

Templates are not persisted as provenance. On durable reopen, the stored `QueueDefinition` is the only
authority. Applying the same template to the same key after reopen must return `created=false` and the exact
stored definition when all fields match.

A queue created by an older build, a pre-template code path, or another control plane may differ after
default rehydration. `ensure_queue` must return the documented exact `DefinitionConflict`; it must not
silently normalize the stored definition, delete/recreate the queue, mutate durable formats, or accept
partial compatibility as success.

The remedies are intentionally narrow:

1. use a new `queue_id`; or
2. align the caller's template, including its pinned `QueueCreationPolicy`, to the stored effective
   definition.

No delete/recreate remedy is promised by this ADR.

### 8. Cross-plane policy drift is caller-visible

Ensuring a queue created through another plane requires the exact creation policy that plane used to produce
the stored effective definition. In particular, defaults supplied by `QueueCreationPolicy` are part of the
resolved desired definition. If one plane uses different defaults for gate caps or future policy-derived
fields, a later Rust-library ensure with a different pinned policy must return `DefinitionConflict` carrying
both resolved definitions.

This is caller-side policy resolution by design. The backend stores only the effective `QueueDefinition`; it
does not store template names, revisions, or policy provenance.

### 9. Backend-family atomic-create prerequisites

Exact ensure is only correct when the backend create path is atomic create-or-read for its ownership scope:
one creator wins, compatible losers read the winning stored effective definition, incompatible losers return
conflict without overwriting the winner, and every durable path returns the decoded authoritative definition
rather than echoing caller input.

The dependency split is:

| Bead | Required prerequisite |
|------|-----------------------|
| B-010 | In-process control planes and blocking wrappers are atomic within one process. Durable SQLite authority is excluded here and belongs to B-016. |
| B-013 | Object-log create is atomic across supported concurrent handles and reopens through decoded storage. |
| B-014 | PostgreSQL native and relational create are atomic through live cross-process races, including losing-handle immediate use. |
| B-015 | Server-owned SQLite, Turso, and object-log composition planes meet their documented ownership-scope create/read conformance. |
| B-016 | Embedded SQLite catalogs use durable queue definitions for `open_sqlite` and `open_sqlite_relational`, with two-handle races and every-non-default-field reopen checks. |

B-004 may implement the facade operation only against backend families whose relevant prerequisite has
landed or is otherwise gated out of the tested constructor set.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Persist template provenance | Could explain why a queue exists | Adds a registry/control-plane subsystem, creates migration and deletion semantics, and duplicates `QueueDefinition` authority | Rejected |
| Treat backend compatible create as ensure success | Minimal implementation | Misses drift in fields outside the current compatibility check | Rejected |
| Let push/claim implicitly create queues from a default template | Ergonomic for demos | Hides configuration errors and changes data-plane failure modes | Rejected |
| Resolve `QueueCreationPolicy` in the server/backend | Centralizes defaults | Makes the Rust template non-portable across planes and hides cross-plane drift | Rejected |
| **Caller-owned template plus exact post-create comparison** | Explicit, additive, and testable without durable template state | Requires atomic create-or-read per backend family | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Callers can create dynamic queues cheaply without copying full queue definitions at every call site. |
| Positive | Exact drift is surfaced with actionable desired/stored definitions instead of a generic conflict. |
| Positive | The Rust facade gains diagnostics without changing engine errors, RESP mappings, API-001, or durable formats. |
| Negative | Template users must pin and preserve their creation policy; policy-default changes are observable drift. |
| Negative | Ensure correctness depends on backend-specific atomic-create work sequenced in B-010 and B-013 through B-016. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| A backend echoes caller input instead of reading the stored winner | M | H | Dependent beads require authoritative decoded definitions and reopen/race tests. |
| A future `QueueDefinition` field is not covered by drift tests | M | H | Exhaustive mutation helper plus direct `QueueDefinition::eq` in runtime. |
| Diagnostics are mistaken for durable provenance | M | M | Keep template name/revision facade-local and absent from stored definitions. |
| Direct `create_queue` users expect exact ensure semantics | M | M | Document direct create as API-001-compatible but separate from exact ensure. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| ADR-018 is linked into the document graph and validates/audits cleanly | `ddx doc validate` or `ddx doc audit` fails. |
| B-004 facade tests prove same template/same key returns `created=false`, distinct keys resolve identically except for key, and every current field drifts exactly | Any drift passes silently or test comparison duplicates production logic. |
| Durable reopen tests create every non-default field, reopen, read the stored definition, and ensure again | Reopen changes or normalizes the effective definition. |
| Cross-plane policy tests return typed conflicts with desired and stored definitions | Policy-default drift is accepted or only returns a generic backend error. |
| No data-plane operation calls `ensure_queue` | Push, claim, discovery, or finalize creates a queue implicitly. |

## Supersession

- **Supersedes**: None.
- **Superseded by**: None.

## Concern Impact

- `api-contract`: API-001 direct `CreateQueue` remains unchanged; exact ensure is an additive Rust-library
  contract.
- `resilience`: atomic create-or-read is a prerequisite, not an assumption hidden inside the facade.
- `operability`: conflict diagnostics include desired/stored definitions and non-durable template
  diagnostics so callers can choose a new `queue_id` or align their template.

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/04-build/foqs-inspired-interface-and-boundary-plan.md`
