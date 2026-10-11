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
