---
ddx:
  id: adr-typed-payload-schemas-and-indexes-via-axon-esf
  depends_on:
    - adr-per-queue-secondary-indexes
  review:
    self_hash: bc29e64f6e6f89932496a4803282d3e388bea665db6c526a92ba17fe49422347
    deps:
      adr-per-queue-secondary-indexes: cd412536c22371beb00f53e7d439cbabed2de5f357c1cf2b8659b9ab38f4c055
    reviewed_at: "2026-07-06T00:56:00Z"
---

# ADR-011: Typed Payload Schemas and Secondary Indexes via the Shared `axon-esf` ESF Vocabulary

- Status: Accepted / Implemented
- Date: 2026-06-28
- Supersedes: ADR-010's **declaration shape** (`IndexSpec { name, fields: Vec<String>, unique }` — the untyped, byte-keyed interim form). ADR-010's maintain-on-apply mechanism, per-queue scope, and in-memory-first phasing **stand**.
- Relates: ADR-008 (queue as shard unit + two projection families), ADR-009 (encapsulated library surface + structured ItemId), FAC-1 (`update_fields`), CONTRACT-007 (consumer "cayce" index/schema needs), and **axon ADR-002** (ESF = JSON Schema 2020-12 entity bodies + Axon vocabulary).
- Driver: ADR-010 shipped *untyped* indexes (values are opaque `Bytes`; keys sort lexically). The consumer surfaced the obvious question — "is everything a string?" — and the answer pulled in the long-deferred payload-schema question. We resolve both: pqueue adopts **axon's Entity Schema Format (ESF) as the single source of truth** for typed payload schemas *and* typed secondary indexes, consumed through the thin `axon-esf` leaf crate.

## 1. Context

ADR-010 Phase 1 shipped per-queue secondary indexes in the in-memory projection family
(`crates/pqueue-projection/src/lib.rs`), declared as `IndexSpec { name, fields: Vec<String>, unique }` and
keyed on opaque field **bytes** (`len‖bytes` per field). That works, but:

- field values are untyped, so a numeric field sorts lexically (`"10" < "9"`) — no typed equality/range,
- the query key is raw `&[Vec<u8>]` — the caller owns all encoding,
- the DB-resident relational family (ADR-008) can only store the key as a generic blob, not a native typed
  column with a real SQL index.

Separately, "axon" (`/Users/erik/Projects/axon`) is becoming the house **schema-first data substrate**. Its
**ESF** (axon ADR-002) is layered and — by deliberate design — keeps the entity body as **pure JSON Schema
Draft 2020-12** (it explicitly rejected `x-` extensions), with axon-specific vocabulary (links, lifecycles,
**index definitions**, named queries) living in *sibling* fields, never mixed into the entity schema. So
axon already separates exactly the layer pqueue needs (the typed entity schema + the typed index
declarations) from the graph-store machinery pqueue does not.

A spike (`SPIKE: axon-schema as an external dep`) established the feasibility and the costs:
depending on the *full* `axon-schema` crate drags `axon-core` + `axon-cypher-ast` (a Cypher AST) and — via
`jsonschema`'s default features — the `reqwest → hyper → tower-http` stack (~148 transitive crates). But the
*reusable core* is small, and `jsonschema` compiled-once validates at **~83 ns/record** (vs ~9 µs when
recompiled per call).

## 2. Decision

pqueue adopts axon's ESF vocabulary **by depending on a thin, shared leaf crate `axon-esf`** — not by
mirroring its types, and not by depending on the full `axon-schema`.

1. **Typed payload schema.** `QueueDefinition` carries an `EntitySchemaDocument` (axon ESF Layer 1 = JSON
   Schema 2020-12). At `create_queue` the queue compiles a `CompiledSchema` once; every `push` / `upsert` /
   `update_fields` validates the item's payload/fields against it **pre-commit, with format assertions on**
   (email/uuid/date-time enforced) — a violation rejects with a structured error and appends nothing.

2. **Typed secondary indexes.** The interim `IndexSpec` is replaced by axon's typed ESF Layer-4 vocabulary —
   `IndexType { String, Integer, Float, Datetime, Boolean }`, `IndexDef { field, index_type, unique }`,
   `CompoundIndexDef { fields: Vec<CompoundIndexField>, unique }` (with leftmost-prefix matching). **Index
   keys are encoded by `axon_esf::index_key`** (the canonical, order-preserving encoder — requested in axon
   bead `axon-a1c87cb1`), so pqueue's keys are **byte-identical to axon's** and cannot drift.

3. **Single source of truth.** A cayce ESF `CollectionSchema` document drives **both** axon and pqueue:
   pqueue consumes its `entity_schema` (validation) and its index declarations (indexing) from the *same*
   document. No parallel pqueue copy of either the schema or the index defs.

4. **Consumed as a git dependency**, pinned to axon release tag `v0.3.2` — **no path dependencies**
   (`axon-esf = { git = "https://github.com/DocumentDrivenDX/axon", tag = "v0.3.2" }`). This also sets
   the direction to move pqueue's other sibling path-deps (fjord/object-log/heimq) onto git pins over
   time.

5. **Named pqueue indexes wrap ESF declarations.** ESF single and compound index declarations do not carry
   pqueue's query name, so pqueue stores them as `QueueIndex { name, declaration }`. The `declaration` is
   axon's ESF type; `name` is the pqueue lookup handle used by `IndexQueryPort` and the public facade.

This supersedes ADR-010's `IndexSpec`. ADR-010's maintain-on-apply pattern, per-queue scope, unique-conflict
pre-commit validation, and in-memory-first phasing are unchanged — only the declaration/key *vocabulary*
becomes axon's, and validation gains the typed entity schema.

## 3. Rationale

- **Drift is preventable, so we prevent it.** Mirroring `IndexType`/`IndexDef` into `pqueue-core` would make
  two copies that *must* stay identical — latent drift. Worse, re-implementing key *encoding* would drift on
  the subtle parts (float/datetime sort order, nested-path resolution, missing-field rules). A shared crate
  for both the types **and** the encoder removes the seam entirely.
- **It is the cleanest possible alignment with axon.** pqueue consumes axon ESF Layer 1 (pure JSON Schema)
  + Layer 4 (typed index defs) verbatim — exactly the layers axon kept reusable. No `x-` extension hazard.
- **The dependency is thin and verified.** `axon-esf` (axon `v0.3.2`) depends on `serde` + `serde_json` +
  `jsonschema` only; the axon workspace pins `jsonschema = { default-features = false }`, and dependency
  proof below shows **no** reqwest/hyper/tower/axon-core/cypher-ast in pqueue's `pqueue-core` tree.
- **The typed model is what indexing wanted all along.** `IndexType` *is* the field typing the "is it
  strings?" question was really asking for; adopting it answers that question instead of inventing a parallel
  one.

## 4. Alternatives considered

| Option | Why rejected |
|---|---|
| **Mirror** axon's ESF type structs in `pqueue-core` | Two copies → drift; and key *encoding* would still be re-implemented (the real drift surface). |
| **DIY typed fields** in pqueue (own `FieldType`) | Reinvents schema *evolution* + validation; diverges from axon's format; no shared key encoding. |
| Depend on the **full `axon-schema`** | Drags `axon-core` + `axon-cypher-ast` + the `jsonschema` HTTP stack (~148 crates) into pqueue's lean public surface (and transitively cayce's). |
| **Plain `jsonschema` only**, ignore axon's index vocabulary | Gets typed *validation* but pqueue re-invents the index declaration + key encoding → drift from axon. |

## 5. Compatibility edges (limiting to ESF)

- **Entity/type layer: near-zero risk.** Both axon and pqueue validate the entity body with the **same
  `jsonschema` crate + Draft 2020-12**, from the same `axon-esf::CompiledSchema`. Identical validation.
- **Format assertions ON** — `format` is annotation-only by default in 2020-12; pqueue enables the format
  vocabulary to match axon, or it would accept a malformed email/uuid axon rejects.
- **Canonical value encoding** — the payload is stored/validated as canonical JSON (`serde_json::Value`), so
  values round-trip to axon; index keys come from the shared `index_key` encoder (byte-identical).
- **Non-coverage, not conflict** — axon's links / lifecycles / named-queries are *outside* pqueue's view (a
  queue is not a graph). pqueue's own lifecycle (Pending/Leased/terminal) coexists with axon's at the app
  layer; not a schema incompatibility.

## 6. Consequences

- `QueueDefinition` gains an optional `entity_schema` (ESF) + typed index declarations; existing definitions
  default both to empty (`#[serde(default)]`) — no churn for non-schema queues.
- JSON compatibility is preserved. Existing byte-oriented callers can continue using payload/field
  carriers and the legacy `IndexSpec` path; typed queues add optional `entity_document`/`entity` JSON
  carriers. `update_fields` with no replacement entity preserves the existing document, while a supplied
  JSON entity is schema-validated and rekeys typed indexes pre-commit.
- **Write-path cost:** payload validation per push/upsert/update_fields. Compile-once → ~83 ns/record;
  acceptable. (Hot-path-skippable when a queue declares no schema.)
- **Dependency surface:** pqueue's published surface re-acquires a dependency on **pre-release axon**
  (`axon-esf`), inherited transitively by cayce. Mitigated by rev-pinning, both being pre-1.0, and co-
  ownership of both repos — but it is a deliberate re-coupling (pqueue had shed sibling couplings; ADR-009).
- Sets the **no-path-deps** direction for pqueue's sibling dependencies generally.
- Relational backends store typed index rows in a side table keyed by canonical ESF bytes. SQLite and
  Postgres relational backends maintain those rows on push, update, replace-pending/upsert, and purge.
  Postgres additionally uses a partial unique SQL index for atomic unique-key enforcement across backend
  instances.

## 7. Implementation status

- **Dependency pin:** implemented with workspace dependency `axon-esf` at axon tag `v0.3.2`.
- **Entity schema storage:** `QueueDefinition.entity_schema` embeds the ESF `EntitySchemaDocument` and is
  serialized through queue definitions and command/replay paths.
- **Typed index declarations:** `QueueDefinition.typed_indexes` stores `Vec<QueueIndex>`, preserving
  pqueue names around ESF single/compound declarations.
- **Write validation:** push, request-id push, upsert/replace-pending, update-fields, and commit lifecycle
  insert paths validate typed entity documents before any append/apply side effect.
- **Index semantics:** string, integer, float, datetime, boolean, compound, sparse missing-field behavior,
  unique conflicts, update rekeying, purge cleanup, and log replay/reconnect behavior are covered in shared
  or backend-specific conformance.
- **Relational parity:** sqlite and postgres relational backends implement `IndexQueryPort`; postgres live
  verification remains env-gated by `PQUEUE_PG_TEST_URL` and skips loudly when absent.

## 8. Verification evidence

Run on 2026-06-30:

```bash
cargo test -p pqueue-conformance -- --nocapture
cargo test -p pqueue-memory adr011_ -- --nocapture
cargo test -p pqueue-sqlite --test conformance adr011_ -- --nocapture
cargo test -p pqueue-sqlite --test relational_conformance adr011_ -- --nocapture
cargo test -p pqueue-postgres --test relational_conformance adr011_ -- --nocapture
cargo test -p pqueue --test secondary_indexes -- --nocapture
cargo test --workspace -- --nocapture
```

The Postgres ADR-011 relational tests compile and report explicit `PQUEUE_PG_TEST_URL` skips when no live
database URL is present.

Dependency proof:

```bash
cargo tree -p pqueue-core | rg 'axon-core|axon-schema|axon-cypher-ast|reqwest|hyper|tower'
rg 'path = .*axon|../axon|/Users/.*/axon' Cargo.toml crates/*/Cargo.toml
```

Both commands return no matches. `cargo tree -p pqueue-core | rg axon` shows only the pinned
`axon-esf v0.3.2` dependency.

## 9. Remaining follow-up

The general path-dep to git-pin migration for other sibling dependencies remains separate work.
