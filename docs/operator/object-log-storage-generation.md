# Object-log storage generation: FWSG → LogEngine

<!-- markdownlint-disable MD013 -->

**Audience:** operators and embedders (including Snorri) that persist object-log
data across Fireweed versions.  
**Since:** v0.24.0 cut the product object-log axis to crates.io
`object-log` 0.2 (`LogEngine`) and deleted the in-tree segmented FWSG substrate.  
**Code:** `crates/fireweed-objectlog/src/storage_generation.rs`

## Summary

| Generation | Versions | On-disk markers |
|------------|----------|-----------------|
| **FWSG** (retired) | pre-v0.24 in-tree segmented substrate | Keys under `t/{tenant_hex}/q/{queue_hex}/…` (`seg_candidates/`, `manifest_head/`, `authority_*`, `*.seg` with magic `FWSG`) |
| **LogEngine** (current) | v0.24.0+ | Product prefixes `fwlog/` (data) and `fwmeta/` (catalog, epochs, high-water, sequencer manifest) |

There is **no in-place decoder** for FWSG objects under LogEngine. Opening a
prefix/root that still holds FWSG data fails closed with a **stable, matchable**
error — it is not undefined behavior and must not be treated as empty storage.

## (a) Open-time behavior on old-generation data

Every public LogEngine open path runs generation detection before constructing
the engine:

- `ObjectLogEngineStore::open_local` / `open_s3` / `open_with_blob`
- Namespace-aware helpers `open_object_log_engine_local*` / `open_object_log_engine_s3*`
- Product opens that wrap those (`AsyncObjectLog*Backend`, composed backends,
  server object-log start arms)

### Error shape

Failures return `fireweed_engine::EngineError::Storage(String)` whose message
contains one of these **stable tokens** (also exported from
`fireweed_objectlog`):

| Token | Meaning |
|-------|---------|
| `INCOMPATIBLE_OBJECT_LOG_GENERATION` | Retired FWSG layout markers found; no LogEngine product keys under the same scan prefix |
| `MIXED_OBJECT_LOG_GENERATION` | Both FWSG markers and LogEngine (`fwlog`/`fwmeta`) keys present under the same namespace |

Helpers:

- `fireweed_objectlog::is_incompatible_generation_error(&err) -> bool`
- Constants: `INCOMPATIBLE_OBJECT_LOG_GENERATION`, `MIXED_OBJECT_LOG_GENERATION`

The message also references this document path so logs point operators here.

### What is *not* guaranteed

- No silent ignore of FWSG objects.
- No partial replay of FWSG segments as LogEngine batches.
- No automatic rewrite or dual-stack open.

If detection is bypassed (custom blob layout outside product prefixes), behavior
is undefined; product paths always detect.

## (b) Migration / regeneration procedure

Fireweed does **not** ship an automated FWSG→LogEngine converter. Sanctioned
paths:

### 1. Prefer regenerate (recommended for previews and rebuildable workloads)

Object-log authority is the durable log; projections (sqlite / hybrid / postgres
relational) are rebuildable **from a LogEngine log of the same generation**.
They are **not** a bridge across generations.

1. **Drain and quiesce** writers (pause intake / stop producers).
2. **Export application state** if you must retain work-in-flight outside the
   queue (application-level dump, not Fireweed FWSG tools).
3. **Choose a new storage generation root:**
   - Local: new `FIREWEED_OBJECT_LOG_ROOT` directory (or empty the old root after
     backup).
   - S3: new key **namespace** (prefix) and/or bucket; do not reuse a prefix that
     still contains `t/…/q/…` FWSG trees.
4. **Reset rebuildable projections** for the new generation (new sqlite file /
   empty projection schema / fresh PVC). Do not point a LogEngine product at an
   FWSG-era projection expecting continuity.
5. **Start Fireweed ≥ v0.24.0** against the empty LogEngine root.
6. **Re-provision queues** (bootstrap env, create APIs, or application seed).
7. **Re-ingest** workload from the application source of truth.
8. **Verify** open succeeds and a push/claim/commit round-trip works; then retire
   the old FWSG prefix after retention policy.

### 2. Side-by-side cutover (shared S3 / multi-replica)

1. Deploy the new release with a **new** object-log namespace (or bucket).
2. Keep the pre-v0.24 release serving the FWSG prefix only until cutover.
3. Dual-running two generations against the **same** prefix is unsupported and
   will hit `MIXED_OBJECT_LOG_GENERATION` once LogEngine keys appear beside FWSG
   residue.
4. Flip traffic only after the new generation is healthy; decommission the FWSG
   prefix.

### 3. What not to do

- Do not copy FWSG `*.seg` / `manifest*` objects under `fwlog/` or `fwmeta/`.
- Do not “repair” by deleting only some FWSG keys while leaving others — open
  will still fail until the namespace is clean of FWSG markers **or** you move to
  a fresh prefix.
- Do not reuse an FWSG local root directory without wiping it first.

## (c) Detecting mixed- or old-generation storage

### Automatic (product open)

Product open is the primary detector. Match
`INCOMPATIBLE_OBJECT_LOG_GENERATION` or `MIXED_OBJECT_LOG_GENERATION` on the
`Storage` error string (or use `is_incompatible_generation_error`).

### Manual inspection (local filesystem)

Under `FIREWEED_OBJECT_LOG_ROOT` (and any hex namespace subdirectory used by
server helpers):

```sh
# FWSG tenant/queue tree (retired)
find "$FIREWEED_OBJECT_LOG_ROOT" -type f \( -path '*/t/*/q/*' -o -name '*.seg' \) | head

# FWSG segment magic
find "$FIREWEED_OBJECT_LOG_ROOT" -name '*.seg' -print0 \
  | xargs -0 -I{} sh -c 'dd if="$1" bs=4 count=1 2>/dev/null | od -An -tx1' _ {}

# LogEngine product prefixes (current)
find "$FIREWEED_OBJECT_LOG_ROOT" \( -path '*/fwlog/*' -o -path '*/fwmeta/*' \) | head
```

| Observation | Conclusion |
|-------------|------------|
| Only `t/…/q/…` / `*.seg` / `manifest_head` / `authority_*` | Old generation — open will fail with `INCOMPATIBLE_…` |
| Only `fwlog/` + `fwmeta/` | Current generation — OK |
| Both families under one root/prefix | Mixed — open will fail with `MIXED_…`; isolate or wipe |

### Manual inspection (S3-compatible)

List under the product namespace prefix (server hex-encodes the logical
namespace):

```sh
# Example with aws cli; adjust endpoint/profile
aws s3 ls "s3://$BUCKET/$NAMESPACE_PREFIX/" --recursive | head
```

Look for:

- **FWSG:** `…/t/<hex>/q/<hex>/seg_candidates/…`, `…/manifest_head/…`, `….seg`
- **LogEngine:** `…/fwlog/…`, `…/fwmeta/catalog.json`, `…/fwmeta/manifest/…`

### Embedder checklist (Snorri and peers)

1. On open failure, branch on `is_incompatible_generation_error` (or the token
   substrings) before treating the error as transient S3/disk failure.
2. Plan live-endpoint verification against **empty or LogEngine-only** prefixes
   for v0.24+.
3. Treat Garage/S3 NativeConditionalWrite tests as orthogonal to generation
   migration: prove CAS on a clean LogEngine namespace, not on FWSG residue.
   See [object-log authority compatibility](object-log-authority-compatibility.md)
   — Garage v2.2.0 does not enforce create-only preconditions and is unsupported
   for multi-writer object-log authority.

## Related release notes

- [v0.24.0](../releases/v0.24.0.md) — FWSG deletion and LogEngine cutover
- [v0.25.0](../releases/v0.25.0.md) — no further on-disk break beyond v0.24.0
- [v0.26.0](../releases/v0.26.0.md) — Snorri-facing validation; same storage generation

## Verification

```sh
cargo test -p fireweed-objectlog --lib storage_generation
```
