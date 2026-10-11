# Astra Ultra review — iteration 47

Model: gpt-6-astra, Ultra. Base: 7d1d2fc5.

APPROVE the live PostgreSQL shutdown notification correction. Terminal notification
belongs to the final reservation owner, including child-held resources. Slot/base
counters release and the admission lock exits before the one-shot callback runs.
Owned buffers release capacity before the final reservation Arc drops. Duplicate
registration cannot replace the original signal; callback internals stay private.

PostgreSQL captures only shared Notify and preserves shutdown enable-before-usage
ordering and client/permit ownership. The real fixture establishes root dispatch
return, then child-held nonzero usage and shutdown Pending; releasing that child
must wake and complete shutdown. Callback scope is a small nonblocking,
non-panicking signal, not storage finalization or scheduling.

Verification: 141 engine queue tests, all thirteen native fixtures (28.47 seconds),
public shared contract/exact receipt reopen (6.99 seconds), nineteen spool tests
(1.24 seconds), strict engine library/PostgreSQL library+tests Clippy, formatting
and diff checks passed. Existing caller-drop durable commit/reopen remains green.
Astra independently reviewed source/scope and checked the diff.

Scratch scheduling, public administration, streamed publication and parity remain
pending; this approval includes only the baseline drain correction.
