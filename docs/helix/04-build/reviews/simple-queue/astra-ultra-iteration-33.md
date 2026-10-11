# Astra Ultra review — iteration 33

Model: gpt-6-astra, Ultra. Base: a0ff7c1b.

APPROVE selective per-target administrative proposals. The checked selector helper
validates the flat selector and certifies every referenced Field/Present value,
type and absence before boolean evaluation. Before fields use the same coverage
checks; prefix removals require full enumeration. Exact edits are resolved once,
and all actually affected composite components are certified, including nonunique
indexes. Deletion requires all unique components. Changed alone carries a candidate
and exact field/body intent; equal explicit bodies Preserve after loaded comparison,
changed/empty bodies Replace, and metadata-only proposals preserve unknown bodies.

The complete-only group oracle now consumes these commands through existing
per-target initial-owner and final combined uniqueness checks. First-clause,
overlap, state CAS and administrative lifecycle/clock semantics remain unchanged.

Review corrected an impossible composite test schema by declaring its fields and
asserting schema validity, added the nonunique missing-component regression, and
removed an unnecessary request-field clone while constructing the resolved patch.
No remaining blocking findings; independent diff hygiene passed.

Verification: 114 engine queue tests, including 21 administrative tests, strict
engine library Clippy and formatting pass. New cases cover unknown predicates
behind true/false folds, certified absence, corrupt loaded types, omitted megabyte
field/body payloads with complete/selected command/result parity, sparse composites,
prefix certificates, compact before fields and equal/empty/unloaded body handling.

This remains caller-admitted pure planning. No native administrative execution,
streaming qualification, public input, durable administrative result or preview is
activated. The next native prerequisite applies each ordinary group inside the
existing single SQL transaction so later selectors can read prior group writes.
Caller cancellation must still let admitted owned jobs finish; database failures
before commit must roll back all unacknowledged effects and receipts.
