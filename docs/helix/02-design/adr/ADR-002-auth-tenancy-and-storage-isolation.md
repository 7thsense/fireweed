---
ddx:
  id: adr-auth-tenancy-and-storage-isolation
  depends_on:
    - prd
    - concerns
    - api-fireweed-kernel-contract
    - adr-queue-as-shard-unit-and-projection-families
  status: accepted
  review:
    self_hash: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
    deps:
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
    reviewed_at: "2026-07-20T00:01:22Z"
---

# ADR-002: Auth, tenancy and storage isolation

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-008, ADR-009, ADR-017, ADR-025, TD-017 | High |

## Context

- Fireweed is an in-process library. Its interface is API-006's `Store`, and there is no network front; a transport over `Store` may be built later under its own contract (ADR-025).
- Many tenants' queues can share one process, one partition and one Turso file (ADR-008). Tenants must not read, change or starve each other's queues.
- The product must stay provider-neutral: it must not embed a signup flow, an identity provider or a consumer's account model.

## Decision

1. **Tenant and queue are namespaces.** Every op addresses one queue by `QueueKey { tenant, queue }` (API-006 §4.0). A tenant groups queues for authorization, admission budgets and operations. It is not a placement unit: one partition holds many tenants' queues, and one tenant's queues may span partitions (ADR-008).
2. **`Store` performs no authentication and no authorization.** The embedding host resolves its principal and authorizes the tenant, the queue and the op before it calls `Store`.
   - A multi-tenant host is deny-by-default. It authorizes before any call and never reveals whether a queue it refuses exists.
   - A trusted single-tenant embedder may use one fixed principal.
   - A future network transport brings its own authentication and authorization as part of its contract. It adds no semantics to `Store`.
3. **Privileged handles stay separate.** The admin handle (for S3×Turso, `TursoAdmin`) carries the `Operator`, projection control and queue placement (API-006 §7). A host never hands it to a data-plane principal, and CI bans it in Snorri, the workload and loadgen crates and the contract suite (OP4).
4. **Queues are isolated by contract.** An op on one queue never observes or changes another queue's records, claims, gates, metadata or definition, and a token is valid only on the queue whose claim created it (API-006 ISO1). Storage keys every row by queue: the Turso projection uses a per-queue integer surrogate, and every statement binds it (TD-017).
5. **Noisy neighbors are bounded by shared resources.** A node runs at least 1,000 concurrently active cold queues plus one hot queue (PRD) without per-queue resource growth.
   - Per-queue background work (lease reclaim, request-id expiry, projection apply and the purge of closed queues) runs on bounded, shared per-node pools and sweepers, never one task, loop or connection per queue.
   - A node-wide byte budget and an optional uniform per-tenant budget bound the mutations in flight; exhaustion is `Busy` (ADR-017).
   - Enforcing a caller's downstream rate limits is not Fireweed's job. Callers pace their own claims with `max_records`, claim cadence, rank and gates.
6. **Security rules.**
   - Lease tokens are stored only as hashes, and a claim has no holder identity: whoever presents the token is the holder (API-006 L1).
   - Field values and queue metadata are caller data. They are validated against the declared types and limits (F1, F2) and are not written to logs or traces by default.
   - Every storage statement is parameterized.
   - Mutating ops emit trace identifiers without their field values.

## Alternatives

| Option | Evaluation |
|--------|------------|
| An `Authorizer` and permission model inside the library | Rejected. `Store` is in-process, so the host already owns the trust boundary; an in-library policy engine would duplicate it and still need the host's principal. |
| Tenant as the placement or ownership unit | Rejected. Placement is per partition and mapped by the control plane (ADR-008); a tenant-to-partition rule would stop the control plane packing queues by load. |
| A dedicated projection file per tenant | Rejected as the default. Queues are cheap and share a partition's file; separating a heavy tenant is a placement choice on the admin handle. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | The queue semantics are independent of any identity provider, and embedding stays simple. |
| Positive | Isolation between queues is a contract clause with conformance scenarios, not a convention of one backend. |
| Negative | A multi-tenant host must authorize every call itself; Fireweed cannot enforce a policy it never sees. |
| Negative | Shared files make isolation depend on every statement binding its queue; tests must check it. |

## Required tests

- ISO1: an op on one queue observes and changes nothing of another queue, and a token used on another queue gets `StaleLease` (API-006 CS6).
- Every Turso statement on a queue's rows binds the queue surrogate (TD-017 performance and statement-form checks).
- A hot queue does not stop 1,000 cold queues on the same node from claiming (TP-002 E2).

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/adr/ADR-009-engine-enforced-coordination-and-encapsulated-library-surface.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
