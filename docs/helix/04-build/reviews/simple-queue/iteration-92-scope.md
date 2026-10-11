# Iteration 92: one owned native transaction lifecycle

Extract the existing stock-Turso projection transaction lifecycle into a private
NativeTransactionWork runner. Publication/claim application still uses Immediate
transactions; future multi-statement readers can request Deferred snapshots.

The owner still retains input, output reservation and operation registration
through completion. Begin failures force awaited cleanup, operation poll panics
become OutcomeUnknown, all error paths prove rollback/autocommit or poison the
connection, and commit failures force cleanup before return. Test-only precommit
pauses are now inside the caught operation future. Postcommit lost-reply testing
and admitted result construction remain in the apply owner.

No public transaction concept or new consumer API is introduced. This removes
duplication before native live replay is added. Existing twelve physical native
projector tests, including rollback, panic, cancellation, lost replies, reopen,
opaque payload preservation and claim receipts, are required to pass unchanged.
