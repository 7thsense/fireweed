# Object-log authority compatibility

Fireweed’s public object-log authority is **`NativeConditionalWrite`** only
(create-only / `If-None-Match: *` or equivalent). Multi-writer publication
and fencing assume the storage adapter **rejects** a second create of the same
key. Queue definitions are immutable per-queue objects; the stored first writer
is cached before a caller's complete definition is compared with it.

The current local adapter implements that operation with a synced temporary
file followed by an atomic create-only hard link. Local handles in one process
also share one LogEngine/manifest sequencer for the canonical namespace root.
The registry that shares this sequencer is open-time plumbing, not definition
authority, and it is not held around append, read, or projection I/O.
Filesystem publication runs on Tokio's blocking pool because file and directory
sync are deliberately durable, potentially slow control-plane operations; it
does not block an async runtime worker or the data-plane LogEngine.

The upstream `object-log` v0.3.1 `BlobStore` port exposes overwrite-only `put`.
Fireweed therefore owns a separate create-only path for **queue-definition**
authority on the S3 open path: `PutObject` with `If-None-Match: *`. That path
requires the endpoint to enforce the precondition (P1s-qualified MinIO does;
Garage v2.2.0 does not). Generic `open_with_blob` (custom adapters without a
create-only publisher) still fails closed rather than pretending an
unconditional read-then-put is authoritative. Multi-writer **manifest**
sequencing remains single-process (in-memory sequencer lock over unique
manifest keys); cross-process multi-writer S3 still needs control-plane fencing.

## Compatibility matrix

| Endpoint class | Conditional create boundary | Current Fireweed support |
|----------------|-----------------------------|--------------------------|
| **Filesystem local blob, one process** | Synced temp + atomic hard-link create; canonical-root handles share one sequencer | Supported, including concurrent handles and unrelated queues |
| **Filesystem local blob, multiple processes** | Definition hard-link is authoritative, but `object-log` v0.3.1 manifest sequencing is not cross-process fenced | Unsupported for concurrent writers |
| **AWS S3 / MinIO** (P1s-qualified) | Endpoint enforces HTTP 412; Fireweed issues `If-None-Match: *` for definitions | Supported for single-writer product cells (definition create-only + log append) |
| **Garage v2.2.0** | **Not enforced** — second conditional PUT returns **200** | **Unsupported** |
| **Other S3-compatible** | Must enforce create-only; multi-process multi-writer also needs fenced sequencing | Supported only when create-only is enforced; multi-writer still needs control-plane fencing |

**Garage (as of v2.2.0):** execution-verified 2026-08-01 (`fireweed-2aefefbb` /
snorri-a1b67264). Garage’s S3 docs do not claim conditional-write support.
Do not select Garage for Fireweed object-log S3. P1s attestation retains it as a
rejected candidate.

There is **no** second public authority mode in the product matrix (historical
Postgres-pointer fallbacks were demoted). Multi-replica shared S3 still
requires a control plane (Postgres) for owner fencing; that does not replace
native conditional create on the object store.

## Failure boundary

Local authority is selected from the canonical filesystem root and needs no
network probe. The S3 product open path publishes definitions with create-only
PutObject; a 412 means another writer won and the stored definition is returned
(create-or-read). Custom `open_with_blob` without a create-only publisher fails
closed on the first queue create with `EngineError::Storage` naming:

1. That **native conditional create** was required  
2. That overwrite-only `put` cannot prove the precondition
3. The required operation (`If-None-Match: *` / put-if-absent)

An ambiguous network response must be resolved by rereading the immutable
per-queue authority key; it must never be converted to `created=true` from
process-local state.

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
