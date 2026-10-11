# Astra Ultra review — iteration 30

Model: gpt-6-astra, Ultra. Base: 743eb116.

APPROVE borrowed FWQP1 consumption and logical replay evidence. One full structural
parser backs both view_publication and scan_publication; no view exposes partial
validation. Private immutable slices, scalars and counts support restartable
command/receipt/field/result iterators without payload/list allocation or per-item
rescanning from frame start. Frozen wire tags/golden bytes are unchanged.

The complete-image cfg(test) oracle checks exact queue/raw definition/sequence,
expected existence/version/highwater, every referenced initial record's identity
and sequence, field admissibility, fresh receipts and success version ranges before
all effects. Matching retained receipts cannot authorize replaying a new frame.
Late command or receipt failure leaves the logical image and receipt map unchanged.
Review hoisted authority checks across Delete and omitted no-op paths, and added
actual uncoalesced leased Preserve and work/state Delete replay (the packed
Retain/Delete/Reinsert test alone had hidden those emitted variants). Inserted,
replaced and rejected positional outcomes and rejected commit results are directly
decoded/asserted. State/work fanout, empty bodies, edits/removals, tombstones,
reinsertion and no-op receipt retention compare against planned primary records.

Verification: 107 engine queue tests, including thirteen codec/view/replay scenarios,
strict engine library Clippy, formatting and independent diff hygiene pass.

This is logical work/state/highwater/sequence/receipt evidence. The test schema has
no unique indexes; the oracle does not rebuild physical indexes or compare unique
probe caches. Its full image clone/owned maps are test-only. Stock Turso must still
qualify bounded physical rows/indexes, unique authority, and atomic data/receipts/
external log cursor publication through the existing ownership pipeline. No new
store append/projector route, claim/provision/admin wire or cursor is activated.
