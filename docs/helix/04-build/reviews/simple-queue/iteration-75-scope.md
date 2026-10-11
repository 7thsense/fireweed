# Iteration 75: conditional raw queue append

Fireweed pins reviewed object-log revision 0890e206f0012450396c21167db07c3e9eced3b0.
Raw API-007 append requires the fully applied authoritative log next offset,
checks its signed representation before dispatch, and submits a sequenced native
conditional append. Insertion sequence and cached high-water hints are not this
condition. A typed head rejection is definite only before the deadline; a
post-position timeout remains ambiguous after draining a later rejection.

Both legacy reservation-disposal matches cancel a definite head rejection
without poisoning the queue. Existing fencing and ambiguity handling remain.
Physical tests establish a single head-zero genesis winner across independent
same-epoch handles, distinguish insertion sequence from log offset, and retain
all admission ownership while draining timed-out or cancelled I/O. The late
rejection regression seeds head one, submits stale head zero, delays data PUT,
and verifies ambiguity plus no second indexed record.

Validation: 102 object-log adapter tests, strict all-target adapter Clippy and
stock Turso facade feature compilation. Astra Ultra reviewed the implementation
and the final focused timeout regression before publication.

This activates no queue factory or projector. First-genesis provisioning,
operational freshness, atomic projection and cursor application, conformance and
settled backend performance qualification remain outstanding. Full manifest
residency and rewrite scaling remain explicit dependency qualification gates.
