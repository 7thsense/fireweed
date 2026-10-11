# Astra Ultra review — iteration 34

Model: gpt-6-astra, Ultra. Base: 1fd35074.

APPROVE ordered physical PostgreSQL group application. Retained replay resolves
before current-record materialization and cannot rewind insertion sequence. Fresh
groups load selective physical authority after prior writes under the same queue
row fence, then plan, charge, apply and persist immutable receipts inside one outer
SQL transaction. Per-group loaded/probe/output admission remains held through
application; envelope response and retained receipt accounting remains intact.
Business rejection persists its own receipt without candidate effects.

The cfg(test), exact-queue boundary hook reads actual SQL records and receipt count
after each fresh group. Its fixture covers stale CAS rejection between successful
groups, unique rekey/reuse, deletion/reinsertion, highwater/sequence, retained reverse
replay and the next fresh insert. Actual division-by-zero after the third group
forces pre-ack database failure: every unacknowledged record, state, receipt and
queue sequence rolls back, then retry succeeds. This is separate from caller
cancellation, where admitted owned work continues to completion and drains.

Verification: all eight native queue fixtures were actually executed and pass,
including caller-drop, schema fencing, hash collisions and selective writer checks.
The public PostgreSQL shared-contract/exact-receipt-reopen fixture was actually
executed and passes (9.58 seconds). Strict PostgreSQL library and test Clippy,
formatting and independently checked diff hygiene pass. No blocking findings.

Ordinary commits sacrifice cross-group physical write coalescing so later native
SQL selection can see earlier successful groups. There is no performance claim;
matched settled benchmarks remain required. One outer publication transaction
remains; no public transaction or administrative operation is activated. Whole
administrative selection must still freeze its group's initial ownership before
applying any of that group's mutations.
