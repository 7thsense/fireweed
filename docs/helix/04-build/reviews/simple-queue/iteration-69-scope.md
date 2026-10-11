# Iteration 69: admitted native queue record replay

An operation-scoped one-record plan reserves metadata/address capacity before
native bounded lookup. Each batch must cover exactly the requested offset and
contain one record. Exact payload size is exposed before I/O; hard byte bounds
reject without reading. Exact reads validate record framing and the full namespace
address. Returned bytes retain their payload admission.

A whole ordered ReadWorker container passes through bounded owned execution and
a consuming method. This resolves Astra's initial disjoint-field capture finding:
metadata/address guards cannot remain behind in a cancelled caller. Deterministic
queued and running cancellation tests retain all charges through physical worker
completion and return to zero after drain. Physical tests cover genesis bytes,
N/N-1, EOF, tiny metadata, wrong full address and invalid record counts.

EOF is the installed sequencer snapshot's EOF. The recovery owner must establish
durable freshness through refresh/fence ownership before treating it as coverage.
This iteration deliberately does not add unbounded refresh to every page.

No queue factory, first-genesis publication, semantic definition activation,
claim receipts, projection/cursor transaction, or consumer migration is activated.
Those remain required before backend equivalence or durability claims.
