# Astra Ultra review — iteration 38

Model: gpt-6-astra, Ultra. Base: a8678521.

APPROVE the private native PostgreSQL provider. A fenced non-scrollable cursor
supplies first-owned rows through one-row metadata, admitted key construction,
shared record/body hydration, exact prefix resolution and current unique probes.
Initial owners remain frozen before mutations. Active-lease deletion discovers
eligible existing unique tuples without ordinary delete's lease rejection.
Identity amplification is charged before tuple generation. Same-job guards merge
without releasing capacity; different reservations cannot merge. Native loaded
payloads drop before their guards.

Successful cursor exhaustion and CLOSE precede the shared async finalizer's fresh
ownership clock. Astra requested explicit lifecycle wording: any provider error
or cancellation of the owner future requires rollback of the entire containing
transaction; callers must not continue sibling groups even if a non-SQL error
left the transaction otherwise usable. Rustdoc and scope documentation now state
that requirement exactly. The outer queue fence persists through publication or
rollback. No public caller-cancellation guarantee is replaced by owner cancellation.

Physical normal/collision fixtures stream 5,001 no-ops with constant retained
client admission and no retained records/probes, omit preserved megabyte bodies,
prune changed metadata, reject an expired ordinary claim after selection, and
reuse an exhausted cursor. A third physical fixture verifies initial-owner and
free-sibling conflicts, exact prefix edits, explicit empty-body replacement and
active deletion after a real public claim. Real SQL division-by-zero during the
second row discards an earlier accepted proposal, returns unknown without a
final clock/result, releases planning charges and rolls back the transaction.

The initial full parallel run exposed two existing five-second fixture timeouts
from global DDL fencing blocked by long cursor transactions. A cfg(test) shared
fixture mutex isolates these fixtures; concurrency inside each fixture and its
timeouts remain unchanged. Astra reviewed the mutex and found no nested lock.
The final default-harness run passes all eleven native fixtures (31.17 seconds),
public conformance/exact-receipt reopen (7.48 seconds), 125 engine queue tests,
strict PostgreSQL library/test and engine library Clippy, formatting and diff
checks. Astra reported no remaining source blocker.

This remains private physical planning qualification. Complete fields for one
row and FETCH1 are conservative correctness shapes. Selective hydration, batching,
server resource measurements, durable complete administrative receipts/replay,
canonical size outcomes, preview, public activation, S3/Turso and matched settled
benchmarks remain required. No performance qualification or public admin stub.
