# Astra Ultra iteration 14 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 3f431d27.
Scope: pure client-owned JSON equality/key encoding and its persistence note.

The versioned encoding matches the locked serde_json numeric equality models,
including exact integer/float representations and zero signs. Type/length framing,
bytewise object-name sorting and ordered arrays are injective; literal RFC6901 key
escaping does not reinterpret payload keys. No body or JSON tree is cloned.

Review clarified persistence: consumers pin the complete encoding/model in the
namespace/schema and check both indexed reads and writes. The public identity and
encode_json_equality_for reject drift until explicit migration. Per-value headers
alone are insufficient to prevent changed-model lookup misses.

Global pending minima include all unvisited ancestor/sibling members before object
sorting allocation. Iterative traversal avoids call-stack depth; the output limit
is distinct from O(max_bytes) scratch with a container/reference multiplier. Literal
number formatting is measured/streamed without numeric-string copies.

Root verified six default tests and the same six with arbitrary_precision and
preserve_order, exact header/numeric wire goldens, mixed equality corpus, namespace
pins, literal keys, deep arrays and nested-wide byte boundaries; strict core library
Clippy, formatting and diff checks passed. Reviewer independently inspected final
source/docs and checked the diff. This does not qualify physical additional-field
indexes, namespace enforcement in consumers or Snorri/S3 cutover.
