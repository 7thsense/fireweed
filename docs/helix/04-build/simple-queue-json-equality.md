---
ddx:
  id: build-simple-queue-json-equality
  depends_on:
    - build-simple-queue-consumer-integration-binding
  status: accepted
---

# Exact JSON equality attributes

The pure core queue_json helper encodes client-owned JSON values as opaque Bytes
attributes. Backends index those bytes; they do not parse workflow bodies. This
primitive does not yet implement additional-field physical storage or migrate
Snorri. It supplies the exact equality/key mapping required by that integration.

FWJ1 identifies the versioned equality encoding. A following numeric-model byte
records whether serde_json Number equality uses numeric variants or literal number
representations under arbitrary_precision. The default model distinguishes integer
from float, preserves full signed/unsigned range and normalizes floating +/-zero.
The literal model retains exact numeric spelling. Object members are sorted by
full UTF-8 key bytes; arrays retain order. Type tags, lengths and fixed numeric
bytes make the representation unambiguous. Null differs from string "null" and
from an absent indexed field. This is equality encoding, not JSON range ordering.

RFC6901 escaping applies only to the literal top-level key: tilde becomes ~0 and
slash becomes ~1. Namespace remains separate from that escaped segment; no path
traversal or Unicode normalization changes key identity. Full names and bytes,
not their digests alone, must be stored and compared by the physical index.

Encoding is iterative and limits encoded output bytes. Scratch is O(max_bytes)
with a container/reference multiplier, not a max_bytes allocator ceiling. Object sorting borrows references; bodies,
strings and trees are not cloned. A global pending-minimum budget includes all
unvisited ancestor/sibling members before allocating sorting/traversal state.
Literal number formatting is measured and streamed without copying an arbitrarily
large numeric string. The caller owns the input JSON tree and calls the helper
before submitting the resulting Bytes field to the queue's normal admission.

Tests compare every pair in a mixed JSON corpus against actual Value equality,
including extrema, exact unsigned values, float/integer representations, exponent
spellings, zero signs, nested objects/arrays and literal keys. The same tests run
with normal features and arbitrary_precision+preserve_order. A golden frame and
exact byte boundary guard encoding compatibility; a 2048-level value exercises
iterative traversal without a hidden depth cap.

The consumer pins the complete JsonEqualityEncoding in its namespace/schema and
checks it before every indexed query and write. current_json_equality_encoding
exposes that identity without encoding a dummy value; encode_json_equality_for
rejects a mismatched persisted pin. Per-value headers alone cannot detect query
misses after feature drift. Preserve the old encoding authority until migration.

Persisted attributes retain the encoding version/model. A numeric equality-model
change requires an explicit checked attribute rewrite before consumer query
activation; old bytes must not be silently reinterpreted. Both adapters and their
queries must use the identical source/features/encoding. Existing queue request
fingerprints, lease authority and native schema are unchanged by this iteration.
