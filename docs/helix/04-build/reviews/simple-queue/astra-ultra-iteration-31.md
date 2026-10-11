# Astra Ultra review — iteration 31

Model: gpt-6-astra, Ultra. Base: 358cbba8.

APPROVE flat administrative selector ownership. Checked scalar postfix operand
accounting rejects underflow and disconnected roots. Reverse-prefix traversal
uses a fixed 64-frame parent stack independent of width. Reversed child order
preserves pure All/Any truth; Not is unary. Empty folds propagate correctly, every
leaf is evaluated, and schema validation examines every predicate independently
of truth. Leaf and empty-operator depth counts consistently at the 64/65 boundary.
Clone, drop and derived serialization no longer recurse through selector trees.

Verification: 109 engine queue tests and 23 existing core contract tests pass,
including wide/deep/malformed programs, nested empty folds, postcard round trips
and 100,000-node invalid-depth ownership safety. Strict core/engine library Clippy,
formatting and independent diff hygiene pass. No blocking review findings.

This prerequisite changes the provisional selector representation only. Live
CommitGroup, fingerprints and persisted receipt formats remain unchanged. Owned
decoding still requires byte and allocation admission before deserialization.
Native indexed selection, selective coverage, durable administrative results,
preview and consumer activation remain required. The next iteration freezes the
original five-field commit identity and every retained receipt enum ordinal
before adding any administrative wire format.
