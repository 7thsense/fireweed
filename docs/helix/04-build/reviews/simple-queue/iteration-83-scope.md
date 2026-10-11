# Iteration 83: native atomic Turso queue projection

Stock Turso 0.8.2 now has an API-007 projector with canonical native queues,
work/state headers, separate bodies, typed declared/additional fields, query and
unique indexes, and immutable request receipts. Every accepted raw record must
be FWQL2. The last applied historical epoch is distinct from a live writer fence.
Exact expected external offsets apply; old offsets and gaps fail closed.

One ordinary-WAL immediate transaction applies complete borrowed FWQP1/2 data,
indexes, receipts, insertion sequence and external applied cursor. Predecessor
existence/version/highwater, raw lease shape, original definition pin and
sequence authority are checked. Final unique ownership uses full values after
all commands, allowing swaps and preserving sparse query keys. Fresh receipt
identity is immutable. Fixed FWQP outcomes are stored as such, never mislabeled
Postcard. Receipt success versions are checked against per-frame record bounds.

Metadata-only Preserve skips opaque fields/body. Edited index closure uses shared
checked-field effects; stored payload lengths and map headers are admitted before
query/materialization. Ordered eight-byte BLOBs preserve full u64 versions and
insertion sequences. Shared canonical layout/index codecs supply both adapters.

Existing owned blocking execution and block_on_owned_apply keep native VFS calls
off application workers. The whole worker retains record, writer/transaction and
guards through physical commit or awaited rollback. Failed rollback poisons the
connection. Uncached execute/query statements avoid persistent bound payloads.
WAL, FULL synchronous and 4 MiB cache settings are checked by readback.

Admitted replay transfers into the existing OwnedQueueLogRecord without copying,
releasing or reacquiring capacity; the checked ingress verifies same job,
sufficient retained bytes and complete framing. Result guards retain admission
through delivery. Physical tests cover mixed state/new work/receipt/cursor cold
reopen, late predecessor and receipt rollback, unique swap/sparse removal,
metadata-only megabyte preservation under one-MiB admission, exact unsigned MAX
CAS/order, caller cancellation during transaction and ambiguous commit recovery.

This is an atomic lower-level projector, not a complete QueueStore. The S3/Turso
factory, native filtered claim/request replay and remaining consumer operations,
full manifest residency/admission/rewrite scaling, Snorri cutover and matched
settled benchmarks remain outstanding. No backend qualification is implied.

Astra corrections: SQL CASE validates consulted authority types and fixed BLOB
lengths before returning payloads; publication cursor is positive, stored epoch
is positive/nonregressing, lease expiry is INTEGER, and live predecessor work
sequence is 1..=initial_sequence even for Delete. Poll-level unwind boundaries
cover begin, apply, commit and rollback while the whole worker and writer lock
remain held. Rollback failure/panic poisons the sole connection before release.
Administrative addressed Changed/Deleted outcomes bind versions and effect kind
to same-frame physical authority. Tests prove rollback before any follow-up SQL
or connection drop, reject four-MiB corrupt authority under one-MiB admission,
and exercise actual FWQP2 success, wrong key, wrong kind, TooLarge and exact
fingerprint/frozen-outcome recovery after reopen.

Incoming field copies share the record's retained four-times-raw allowance;
map headers have separate charges. Schema names share admitted definition
raw/header allowance. These are logical admission bounds, not an RSS proof.

Final validation: ten native projector tests, six admitted physical replay tests,
eleven queue-log codec/ingress tests, strict Turso local all-target Clippy, strict
object-log all-target Clippy, and diff hygiene passed on stock Turso 0.8.2.
