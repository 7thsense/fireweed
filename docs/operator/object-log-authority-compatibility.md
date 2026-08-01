# Object-log authority compatibility

Fireweed’s public object-log authority is **`NativeConditionalWrite`** only
(create-only / `If-None-Match: *` or equivalent). Multi-writer publication
and fencing assume the blob store **rejects** a second create of the same key
when the precondition is set. Endpoints that accept but ignore the
precondition would silently lose fencing and are rejected at open.

## Compatibility matrix

| Endpoint class | Conditional create (`If-None-Match: *` / put-if-absent) | Fireweed `NativeConditionalWrite` |
|----------------|----------------------------------------------------------|-------------------------------------|
| **AWS S3** | Enforced (HTTP 412 on conflict) | Supported |
| **MinIO** (recent) | Enforced when configured as S3-compatible create-only | Supported (verify on your build) |
| **Filesystem local blob** | Enforced via O_EXCL / create-new | Supported |
| **Garage v2.2.0** | **Not enforced** — second conditional PUT returns **200** | **Unsupported** — open fails closed |
| **Other S3-compatible** | Operator-verified | Supported only if probe proves rejection |

**Garage (as of v2.2.0):** execution-verified 2026-08-01 (`fireweed-2aefefbb` /
snorri-a1b67264). Garage’s S3 docs do not claim conditional-write support.
Until Garage enforces create-only preconditions, use MinIO/AWS/filesystem for
object-log authority, or keep single-writer non-shared topologies only where
the product path does not require multi-writer CAS.

There is **no** second public authority mode in the product matrix (historical
Postgres-pointer fallbacks were demoted). Multi-replica shared S3 still
requires a control plane (Postgres) for owner fencing; that does not replace
native conditional create on the object store.

## Open-time probe

On open, Fireweed (or the LogEngine/S3 stack it embeds) proves create-only
semantics before accepting the endpoint as authority. Failure is fail-closed
with [`EngineError::Unavailable`] / `Storage` carrying a message that names:

1. That **native conditional create** was required  
2. That the **precondition was not enforced** (or the probe could not prove it)  
3. The **endpoint class** when known (e.g. S3-compatible URL)

Operators should not treat bare `Unavailable` on open as a transient network
error when the message references conditional create / `If-None-Match`.

## Poisoned projection (illegal-lifecycle residual)

If a process still holds an object-log namespace that was written under a
pre-fix dual-stack race (validate then apply without one queue permit),
reopen may poison when the durable log contains a command `apply_transition`
rejects. **Remediation (refuse-with-remediation):**

1. Stop writers for that queue namespace.  
2. Capture the log prefix for forensics.  
3. Recreate the queue on a **new** object-log prefix/generation (do not
   attempt to partial-skip illegal commands in place — the log is
   authoritative).  
4. Rebuild projections from the new clean log.

Post-fix products (queue-local permit across claim/finalize/commit_transition
and claim_by_item_ids prepare) must not admit commands that apply rejects.

## Related

- [object-log storage generation](object-log-storage-generation.md) — FWSG vs LogEngine layout  
- [API-005](../helix/02-design/contracts/API-005-fireweed-rust-facade.md) — `ObjectLogAuthority::NativeConditionalWrite`  
- [operator guide](../deployment/operator-guide.md) — S3 log + control plane  
