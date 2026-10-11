# Ordinary commit durability compatibility — iteration 32

Ordinary API-007 commits retain the v3 domain and the original five-field order:
request_id, claimed_updates, state_updates, work_inserts, work_updates. Production
fingerprinting uses an explicit borrowed projection of those fields, rather than
serializing future fields added to the live group. It performs no payload copy.
An administrative extension must use an explicit new fingerprint format while
sharing the existing commit request-identity domain; adding optional controls must
not change ordinary fingerprints.

Core regressions pin original postcard bytes and concrete SHA-256 fingerprints:

- Empty five-field shape: ee3a8791dd0a46a3445d71b36d2c0b3f67cd9082b2c26aa2aae8f81cdf68ce63.
- Rich five-field shape: f6bffe65f454e14c35943a1438cd252a47bb8dbc74a5641977998ddcb13c419e.

The rich fixture includes every vector, full unsigned version range, signed time
and priority extremes, literal NUL/slash/UTF-8 names, negative zero, removal and
explicit empty body. The empty shape is an encoding fixture, not a valid admitted
zero-record commit. Literal bytes and hashes must remain frozen when the live DTO
is extended. At that point the current derived-group comparison becomes a dedicated
legacy five-field fixture; do not update its expected bytes.

Retained receipt fixtures pin GroupOutcome tags 0 through 3, Rejection tags 0
through 7, and ReceiptLookup tags 0 through 2. They decode, re-encode and agree with
conservative borrowed receipt header measurement. Every fixture rejects trailing
bytes through measured persisted decoding. Append new variants with new ordinals;
do not reorder these tags. Any new administrative result shape must extend the
borrowed measurement decoder in the same iteration as persistence.

No administrative input/result, preview, new rejection or FWQP2 format is activated
here. FWQP1 remains unchanged. Native indexed controls, per-group publication/result
admission, mixed-format durable replay and stock Turso parity remain separate gates.
