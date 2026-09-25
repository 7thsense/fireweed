---
ddx:
  id: adr-typed-payload-schemas-and-indexes-via-axon-esf
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
  status: accepted
  review:
    self_hash: bc29e64f6e6f89932496a4803282d3e388bea665db6c526a92ba17fe49422347
    deps:
      adr-per-queue-secondary-indexes: cd412536c22371beb00f53e7d439cbabed2de5f357c1cf2b8659b9ab38f4c055
    reviewed_at: "2026-07-20T00:01:22Z"
---

# ADR-011: Declared typed fields, with index keys in the axon-esf encoding

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-003, ADR-025, TD-017, axon ADR-002 | High |

## Context

- Records carry typed data, and consumers look records up and count them by typed values: `scan` and `aggregate` over declared indexes (API-006 §4.6, §4.7).
- An index key must sort the same way in every implementation, and its size must be measured the same way everywhere, because the 1024 B bound on an encoded index key is a contract limit (API-006 LIM1).
- Axon is the house schema-first data substrate. Its Entity Schema Format (ESF, axon ADR-002) defines typed index declarations and a canonical, order-preserving index-key encoder, published in the thin leaf crate `axon-esf`. Reimplementing that encoding would drift from axon on its subtle parts: float order, time order and missing values.

## Decision

1. **Fields are declared, typed and the only per-record data.** A queue's definition declares at most 256 fields, each of type `Int` (i64), `Float` (finite f64), `Bool`, `Time` (µs), `Str` (UTF-8) or `Bytes` (API-006 §5.1). A write is checked against the declaration before any effect (F1, W1). There is no JSON Schema, no entity document and no separate payload.
2. **Indexes are declared with the fields.** A queue declares at most 32 indexes, each over 1..=8 keys drawn from its fields of any type and the built-ins `Stage`, `Rank` and `Group`. The key is the only uniqueness, so no index is unique (ADR-025 §2). Indexes serve `scan` and `aggregate`; claims order only by rank.
3. **Index keys use the ESF encoding.** `fireweed_contract::esf_index_key` encodes every index key with axon-esf's canonical, order-preserving encoding, and every implementation uses it verbatim. It is also the measure of the 1024 B bound on a whole encoded index key.
4. **The encoding fixes the order.** Within a type, values sort in their natural order: `Str` and `Bytes` bytewise, `Float` numerically with `-0.0` stored as `+0.0`. Across types, values sort in declaration order. `Stage` values sort by the stage's declaration position, not by name. The encoded order matches the `Value` order that API-006 §4.0 states.
5. **`fireweed-contract` extends the encoding where axon-esf stops.** It adds `Bytes` values and the `Stage`, `Rank` and `Group` built-ins, with ordering and round-trip tests, and it keeps the encoding of every type axon-esf covers byte-identical to axon's.
6. **axon-esf is a git dependency pinned to an axon release tag.** No Fireweed crate uses a path dependency on axon, and no crate depends on the full `axon-schema`.

## Alternatives

| Option | Evaluation |
|--------|------------|
| A JSON Schema entity document per queue, validated on every write | Rejected. It is a second typing concept beside declared fields, and declared types already give validation and index typing. |
| Fireweed's own index-key encoder | Rejected. It would drift from axon on float, time and missing-value order. |
| Mirroring axon's ESF types in Fireweed | Rejected. Two copies of the same vocabulary drift. |
| Depending on the full `axon-schema` crate | Rejected. It pulls `axon-core`, a Cypher AST and an HTTP client stack into the contract crate. |
| Untyped byte keys | Rejected. Numbers sort lexically, and every caller must encode keys itself. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Every implementation agrees on key order and on where the 1024 B bound falls. |
| Positive | For the types axon covers, Fireweed's index keys are byte-identical to axon's. |
| Negative | `fireweed-contract` depends on a pre-1.0 crate from another repository; the pin and co-ownership of both repositories contain the risk. |
| Negative | The `Bytes` and built-in extensions live outside axon and need their own ordering tests. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-025-kernel-records-contract.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- axon ADR-002 (ESF), in the axon repository
