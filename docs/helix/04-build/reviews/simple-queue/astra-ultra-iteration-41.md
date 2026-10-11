# Astra Ultra review — iteration 41

Model: gpt-6-astra, Ultra. Base: 56562de2.

APPROVE frozen ordinary group/envelope size accounting. Both production size paths
(preflight and schema validation) use the same frozen five-field projection for
each ordinary group. The borrowed envelope preserves original queue framing,
sequence length and producer order without a temporary vector. No accepted-size
or identity change is introduced. Future mixed administrative/ordinary envelope
accounting must explicitly dispatch the appropriate projection per group.

Verification: 26 core contract tests including exact N/N-1 group and multi-group
boundaries, strict core library Clippy, formatting and diff checks passed. The
actual public PostgreSQL shared contract and exact receipt reopen passed in
9.80 seconds. Astra independently checked the diff and scope documentation and
found no blockers.

This is a compatibility prerequisite. Administrative activation, bounded overflow
execution, complete receipt budgets, FWQP2 and backend parity remain pending.
