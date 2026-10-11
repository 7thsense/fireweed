# Iteration 88: thin admitted claim publication wrapper

FWQC1 wraps an existing strict FWQP1 zero-receipt command frame and an ordered
FWCR1 receipt, plus request identity/fingerprint and recorded claim timing.
No queue, mutation or projection implementation is duplicated. Existing log
kinds, FWQP1/2 and checksummed envelopes are unchanged; unknown claim kinds
remain rejected until physical dispatch is qualified.

The borrowed view completely checks framing/EOF, legal recorded lease duration,
checked expiry arithmetic, unchanged insertion sequence, zero embedded receipts
and command/entry cardinality without allocation. Cross-binding is explicitly
separate: a pre-admitted borrowed full-key map binds every ordered entry to one
canonical sorted work command in O(n log n), rejects duplicates/extras/missing
commands, requires positive live prior version and exactly one increment,
Leased metadata with matching token hash/creation version/common expiry,
nonzero attempts and legal sequence/availability, and Fields/Body Preserve.
No state, create, delete or payload mutation can pass that binding.

The writer validates the actual request against the admitted original definition
and limits, exact queue/pin, response count and expiry before admitted output
allocation. It retains output bytes/guard through complete validation. Core lease
duration validation is now doc-hidden public for internal reuse, preserving the
existing 1ms..1h rule rather than duplicating constants.

A focused fixture checks non-key selection order against sorted commands,
empty claims, request fingerprint, exact N/N-1, every truncation, trailing bytes
and structurally valid token/hash mismatch. Strict engine production Clippy and
diff hygiene pass. Native physical predecessor checks must additionally compare
preserved priority/availability/eligible age/order and attempts+1, then apply
commands/domain1 receipt/cursor in the existing transaction. No clock/selector
re-execution belongs in replay. Current live-record replay and physical activation
remain outstanding; creation versions are not replay-version predicates.

Astra correction: parsed timing, identity and fingerprint fields are private with
read-only getters; validated recorded authority cannot be mutated afterward.
FWQC1 explicitly freezes claim-domain canonical fingerprint v3 and exposes its
fixed format getter. Independent Python assembly of the canonical Postcard
request and frozen field grammar pins the 196-byte literal empty wrapper and
544-byte rich SHA-256 07be6c7890946d187d28d9ec51a3079648de8f9cc41589d75628ccbf1ddf7ca0.
The fixture now also rejects structurally valid duplicate entries, field/body
replacement, terminal and delete commands. A too-small admitted map budget
returns to its baseline without leaking or borrowing output capacity. Final
focused test, strict production engine Clippy and diff hygiene passed.

The complete engine suite also passed: 472 tests, including unchanged legacy
protocol goldens and existing shared planners.
