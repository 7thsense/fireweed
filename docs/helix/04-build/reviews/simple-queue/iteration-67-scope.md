# Iteration 67: exact bounded replay dependency integration

Fireweed pins object-log revision 5f0e6ac123628646f68e6318881cfd7a359144b6,
reviewed by Astra Ultra and pushed with the preserved v0.3.6 reliability ancestry.
Cargo.lock changes only this source identity; no unrelated dependency upgrade.
Tracing and head-publication pause wrappers explicitly forward strict range reads.
The composed-wrapper regression checks an exact success and an out-of-bounds error.

This enables the bounded replay port for subsequent native queue implementation.
It does not activate raw queue replay, S3/Turso QueueStore construction, claims,
projection atomicity, consumer migration, or qualify remote durability/performance.
Full manifest residency/write scaling remains a qualification gate.
