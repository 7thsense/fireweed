# Streaming exact uniqueness evidence

The private shared engine reducer validates one externally grouped full tuple.
It borrows an admitted tuple and initial owner header and retains at most two
charged claimant identities; repeated release is a boolean after exact initial
ownership validation. It owns no map of all tuples, predecessor field bags,
selector data or I/O dependencies. Discovery hashes do not enter its decisions.

Each evidence row must certify loaded owner-or-absence authority and the same
full collection/index/tuple identity. Contradictory authority, an unowned release,
collection mismatch or failed admission poisons the reducer. Callers cannot
swallow the error and finish successfully. Releases may arrive after claims;
final collision evaluation waits for successful completion of that tuple.
The driver must successfully finish every tuple/source before publication.

The reducer distinguishes accepted administrative sibling-claim conflicts from
combined ordinary/administrative conflicts. This preserves the existing planner's
business ordering: explicit overlap, administrative sibling conflict, fresh
ordinary CAS/lease rejection, final combined uniqueness, then logical overflow.
These provisional decisions alone authorize no durable result or acknowledgement.

Differential tests compare combined decisions with UniqueFieldEffects::validate
for initial ownership, releases, claim multiplicity and reversed evidence order.
Additional tests cover distinct origin decisions, late corrupt authority, poison,
missing authority, wrong full index identity, invalid release and capacity failure.
A caller-admitted megabyte tuple with 5001 repeated claims retains constant owner
admission and releases that admission when reduction finishes.

This iteration does not sort, spill, encode a durable control frame, replace the
current administrative accumulator or activate a public input. A shared physical
spill implementation still needs exact full-key external grouping, charged run
windows and bounded fan-in, a separate scratch-disk quota, sealed owned readers,
I/O failure/cleanup tests and accumulator/application integration. Full values
larger than a merge window must remain comparable without digest shortcuts.
Neither semantic publication limits nor small commands can bound old composite
index authority: a tiny Edit(a) in unique(a, large_b) is the regression that the
physical spill path must qualify. Scratch failure remains an owning-operation
Unknown and rollback; it cannot fabricate a known TooLarge receipt.

## Shared physical scratch — iteration 44

fireweed-queue-spool is one disposable physical adapter usable by both backends.
The engine stays I/O-free. Anonymous temporary files hold borrowed key/payload
bytes and two fixed-width descriptor streams. Bottom-up two-way external merge
compares collection presence, collection bytes, index bytes and full tuple bytes
lexicographically with fixed 4096-byte read windows. Producer ordinal breaks
exact-key ties deterministically; tuple grouping must ignore that ordinal.
No hash, whole-file mapping, full-key clone or in-memory run directory is used.

The operation retains a fixed memory charge before file creation. A separate
shared scratch quota reserves complete append data/descriptor growth before the
first write and the full second index before sorting. The quota measures file
bytes at the two-index peak, not filesystem blocks, metadata or page-cache usage.
Partial failed writes keep their reservation until their owned files close.
Exact append image lengths, descriptor bounds, ordinals and contiguous coverage
are validated before sealing. Merge arithmetic and result sizes are checked.

A sealed owner keeps all files and guards together, with exclusive cursor access
and repeatable reads. Opaque entries are bound to that owner; callers supply
admitted read windows. Retained entry metadata and entry containers require
caller admission as well; the primitive retains only one cursor. A late reader
error permanently poisons EOF and rewind.
Sealing proves only the successfully appended scratch image, not completeness of
the original selected source. The driver must separately reach and close source
EOF under the queue fence and preserve that fence through actual publication.
Scratch is not a durable log, acknowledgement or public transaction protocol.

Physical tests cover empty/singleton/non-power-of-two runs, 1027 rows with flat
retained admission, full-key reference parity for NUL/UTF-8/presence values, equal
prefixes beyond three windows, large payloads, stable ties and repeatable reads.
Tests also cover exact two-index peak, atomic shared-quota competition, partial
append/merge writes, malformed/truncated index/data, overflowing offsets, foreign
entries, poisoned read EOF, cleanup and admission failure before file creation.

This synchronous primitive is not wired into the live asynchronous owners.
Integration must supply an owned blocking boundary that cannot outlive its guards,
encode/reduce frozen semantic evidence, hydrate one admitted tuple for the domain
reducer, and stream application/publication without reconstructing an aggregate.
The comparator's window bound does not eliminate semantic tuple hydration.
Scratch failure remains whole-owner Unknown/rollback; semantic logical TooLarge
still requires complete-result/frame measurements and durable positional receipts.
No backend parity, power-loss durability or performance qualification is implied.

## Owned worker admission — iteration 45

A scoped buffer charge can transfer to OwnedQueueBufferCharge only with the Arc
of the exact same reservation. Transfer changes no admission counters and exposes
no unprotected parts; another-job transfer fails closed. owned_buffer reuses the
existing admission rules. The owned guard keeps both buffer capacity and the
operation's slot/base reservation alive through the last worker-held resource.

The shared physical spool now uses this owned guard and has no borrowed job
lifetime. Its files still drop before disk/memory guards. One implementation can
move intact into a blocking worker without cloning buffers or releasing/reacquiring
capacity. Tests pause a real worker holding scratch, drop the caller's reservation
Arc and close admission, then verify file/quota/slot/base/buffer ownership survives
until the worker sorts, reads and closes the files. Foreign-job conversion,
unchanged transfer counters and failed capacity acquisition are also checked.

This qualifies guard and physical-handle ownership, not a live backend blocking
scheduler or shutdown drain. Native integration must keep completion notification
and terminal ownership attached to pending workers, await their results, preserve
the queue fence and avoid unbounded task/channel accumulation. Semantic evidence
streaming and actual administrative publication remain pending.

## Physical semantic reduction — iteration 46

Frozen ScratchUniqueEvidence contains loaded initial owner-or-absence authority
and release/claim origin. Its full tuple is the external grouping key. Evidence
encoding and decoding use same-job child admission before raw payload and decoded
owner/string allocation. A protected reservation borrow cannot outlive its owned
guard or extract/release the job. Only one admitted full tuple and its first
initial-owner header survive a tuple group; later payloads drop one row at a time.
New tuple comparison reads windows against the retained key without duplicating it.

Private FWUE1 envelopes bind producer ordinal, unambiguously framed full key and
raw record bytes with SHA-256 for content integrity. Hashes never group tuples or
decide uniqueness. Unknown versions, invalid UTF-8/enums/framing, trailing bytes,
checksum errors and absent/contradictory authority fail closed. Administrative
claims must already satisfy immutable initial-owner per-row acceptance. Reader
ordering is strictly full key plus ordinal; duplicated valid descriptors and
replayed record payloads cannot silently replace evidence. EOF checks file sizes.

Reduction always rewinds and consumes the complete sealed evidence image, even
if a prior caller read part of it or a previous reduction reached EOF. Provisional
administrative/final collision flags do not skip later validation. Any semantic,
I/O or capacity error poisons replay/completion and returns whole-owner Unknown,
not partial flags or a semantic TooLarge receipt. No second uniqueness algorithm
is introduced: the physical driver uses the shared domain reducer.

Physical differential tests match the complete UniqueFieldEffects oracle,
including release after claim and repeated replay. A real typed unique(a, large_b)
fixture changes a across 48 records with 256 KiB b values: more than 24 MiB of
old/new tuple scratch reduces within a 2 MiB admitted-operation memory budget.
This is admission/loaded-buffer evidence, not an operating-system page-cache or
RSS measurement. Other tests cover late checksum/serde/read failure after a
provisional conflict, unaccepted admin claims, contradictory/missing authority,
decoder capacity cleanup, invalid version/UTF-8, ordinal binding, duplicated
valid descriptors, partial cursors and repeated full reductions.

This proves complete evidence-spool reduction only. Original selection EOF,
owned asynchronous scheduling/drain, resolved command streaming, logical overflow
and receipt/result sizing, actual publication and backend parity remain pending.
The pure complete-image planner remains the differential reference; the native
path must not reconstruct its aggregate predecessor/probe maps at finalization.

## Final-reservation drain notification — iteration 47

Terminal notification now belongs to the reservation's final owner. A small,
nonblocking, non-panicking one-shot callback is registered before Arc sharing.
The final reservation drop first releases slot/base counters and the admission
metadata lock, then signals completion. Owned child buffers release their own
capacity before dropping that reservation Arc. Duplicate callback registration
fails closed while preserving the original terminal signal. The callback is not
storage finalization, publication or a second scheduling protocol.

PostgreSQL dispatch captures only its shared completion Notify in this callback.
Its former root-task-only notification could occur before a worker's child guard
released the last slot, leaving shutdown asleep after the later release. Shutdown
still enables its notification before checking counters and closes clients only
after admission drains. The physical regression makes dispatch return while an
already-owned blocking child is paused, polls shutdown Pending with nonzero
usage, then releases the child and requires shutdown completion with zero usage.
The existing caller-drop durable commit/reopen regression remains mandatory.

This closes a live baseline shutdown wake-up gap and provides the terminal signal
needed by upcoming scratch scheduling. It does not activate administrative input,
create a blocking worker pool or prove complete selected-source publication.

## Bounded owned blocking execution — iteration 48

ScratchWorkers limits executing blocking closures and all registered executing/
waiting jobs separately. Registration happens on the first polling of run, before
its first await, and is linearized with close under one metadata mutex. Waiting
is caller-owned rather than an unbounded background task/channel queue. Captured
operation resources must own their admission; queued cancellation drops those
resources before releasing registration. A dispatched worker owns its operation,
execution permit, completion token and result delivery. Physical failures/panics
are OutcomeUnknown, including post-admission scheduler capacity failure.

Tokio can enqueue a blocking closure before thread-creation dispatch panics.
A private atomic ownership handoff cancels and drops still-unstarted work on that
failure, leaving any retained task stub empty. If a worker already took ownership,
run awaits its terminal result and drops returned resources before reporting the
error. Dispatch failure poisons new registration and already-registered future
dispatch, bounding empty stubs. Runtime cancellation drops the handoff and closes
the response channel; the caller must not retain another sender owner while
awaiting it. Worker panics are caught separately from dispatch failure.

Normal completion releases execution capacity before delivering results, avoiding
spurious next-row registration failure. Returned T resources retain their queue
reservation independently. close_and_drain rejects new registration but preserves
accepted waiters and enables notification before checking counters. It drains
terminal closure execution, not returned resources; final reservation notification
supplies that separate ownership boundary. Cancelled/repeated close remains safe.

Tests cover queued/running cancellation, actual file sorting/read/cleanup after
caller drop, two independent execution slots, accepted waiters during close,
capacity normalization, immediate next jobs, worker panic and close/register/
completion races. Injected dispatchers cover retaining a closure then panicking,
starting it then panicking, and dropping an unstarted runtime task. The first
case permits no later I/O; the second waits for completion and closes its result.

This scheduler is not wired into either backend's control path. Integration must
retain the queue fence and complete owned operation while awaiting run. Backend
shutdown must reject new queue admission, finish existing operations and drain
final reservations before closing the scheduler, so accepted queue work not yet
registered for scratch can still finish. Selected-source EOF, command streaming,
logical-size receipts, publication and backend parity remain pending.

## Sink-driven selected-row resolution — iteration 49

The new internal scan_broad_group_to_sink awaits one owned resolved row at a time.
It retains only the admitted previous key, input-sized explicit identity set and
clause histograms. selected_row, selected_clause and resolve_candidate are shared
with the existing admitted reference accumulator, so the sink path uses identical
first-clause ownership, operator-time decisions, prefix edits, dependency pruning,
unique effects and immutable initial-owner acceptance. Its source must remain
fenced through later finalization and atomic storage.

No-op, precondition and lease-only outcomes update histograms without a packet.
Each resolved-candidate packet exposes the row outcome/clause, predecessor/highwater and
consulted unique authority. Only accepted packets expose a resolved command and
accepted effects. Rejected UniqueConflict candidates still deliver their consulted
probe observations. Scratch evidence now supports an ephemeral Observe tag that
validates the exact initial owner without changing release/claim state. This does
not change any durable FWQP1 or consumer request/receipt encoding. The eventual
physical sink must write those observations separately from accepted effects.

The scan continues complete row validation and sink delivery after provisional
overlap. It never calls the aggregate accumulator's doom path and samples no
ordinary ownership clock. Source, sink, authority or admission errors return
OutcomeUnknown without a scan summary. Successful EOF certifies selected-source
and awaited-sink delivery only; it does not certify evidence reduction, ordinary
CAS/lease validation, logical publication size or publication authority. Complete
external authority reduction remains mandatory even after provisional business
rejection. Final precedence remains overlap, administrative sibling collision,
ordinary CAS/lease, combined uniqueness, then logical TooLarge.

Tests drain 5,001 accepted updates and 5,001 accepted deletes under a 1 MiB
operation-admission ceiling with constant retained driver memory, then release all
charges. A late rejected candidate reports contradictory initial ownership after
an earlier overlap and cannot escape as a business rejection. Injected source,
sink and inconsistent highwater/sequence failures yield no summary. Physical
Observe tests prove it neither claims nor releases and late contradictory
observations poison complete evidence replay. A suspended sink prevents source
advancement, and dropping that operation owner releases its row/root charges.

This iteration supplies the bounded domain driver. It does not connect the native
PostgreSQL cursor to physical command/evidence spools, activate public
administration/preview, or supply S3/Turso parity. The existing aggregate driver
remains an admitted reference adapter and must not be activated for native broad
administration.
