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
