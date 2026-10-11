# Astra Ultra review — iteration 48

Model: gpt-6-astra, Ultra. Base: a83291b8.

APPROVE the shared bounded scratch scheduler after dispatch-handoff correction.
Registration/close are linearized, waiting owns resources without background tasks,
and queued cancellation drops operation resources before completion accounting.
The worker owns its permit, operation, completion token and response delivery.

Initial review found pinned Tokio can enqueue a closure before thread-creation
panic. Atomic handoff now cancels unstarted work or awaits started work and drops
its result before error. Dispatch failure closes/poisons future dispatch; a late
retained stub owns no I/O. Normal dispatch releases the caller's cell reference
before awaiting, so runtime cancellation can drop the sender. Worker panics and
other physical errors normalize to Unknown. Execution completion remains distinct
from final reservation/resource completion.

Verification: all thirty physical tests passed (1.35 seconds), strict spool
all-target Clippy, formatting and diff checks passed. Injected dispatchers cover
enqueue-then-panic, start-then-panic and cancelled unstarted task; tests cover
waiting/running cancellation, real files, close races and independent slots.
Astra independently checked the corrected source and scope. Its rustdoc wording
correction distinguishes cancellation during permit waiting from after dispatch.

Approval is scheduler-only. Native control activation, selected-source EOF,
streamed commands, logical receipts and backend parity remain pending.
