# Iteration 80: owned first-genesis provisioning

A whole consuming ProvisionTask runs across acquire, append and exact-read
stages after dispatch. Caller cancellation cannot stop later stages. Each
store admits at most 64 active protocols independently of reservation reuse;
root bytes also admit complete address/prefix/path/task headers before copying.
Physical stages retain the existing bounded workers and transport ownership.
Shutdown must drain reservations before closing workers needed by later stages.

Native acquisition supplies epoch and installed durable replay bounds. Empty
head submits genesis only at conditional offset zero. ACK must identify offset
zero and the acquired epoch, then advances the target to one. Drained ambiguous,
head-rejected or fenced attempts reacquire fresh native bounds, with eight
attempts maximum. An indexed genesis survives acceleration-hint failure and is
recovered without a second insert.

Every successful path reads offset zero and validates full address, record kind,
epoch bound and admitted definition semantics. Existing head plus missing,
corrupt or retained-away genesis fails closed. Semantic schema equality is
idempotent; conflicts return DefinitionConflict. Original persisted bytes, pin
and cursor secret are retained, including legacy encodings. Output is admitted,
operation-local genesis authority, not a fully recovered/projected queue.

Exact genesis sizing is shared with the FWQL1 writer. Hard-limit preflight
happens before fence mutation. Wire bytes and existing goldens are unchanged.
Physical tests cover independent concurrent equal/different-schema creators, cold legacy reopen,
conflict, corrupt/deleted genesis, task-slot rejection, ambiguous append recovery
and caller cancellation between stages. Full engine and adapter regressions,
strict adapter all-target/engine library Clippy and stock Turso feature compile
are the validation gates. Engine test-target Clippy has seven preexisting lint
findings in unrelated publication tests; no new production lint is suppressed.

No QueueStore factory or atomic projector is activated here. Whole-history
validation, integrity qualification, operational freshness, full manifest
admission/residency/rewrite scaling, consumer cutover and matched settled
backend benchmarks remain outstanding.

Protocol slots drop before the job/root guards, so final reservation
notification cannot race a still-held task slot on errors or abandoned results.
