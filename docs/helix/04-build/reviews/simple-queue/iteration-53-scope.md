# Iteration 53: bounded resolved administrative SQL application

Base: 03c8161e. Shared engine materialization consumes exact parsed work commands
against certified selective physical predecessors. It preserves unknown fields
and bodies and validates identity, live version/highwater, insertion sequence,
field schema, resolved view, and affected index coverage. Payload and index
amplification are admitted before copies; guards remain through SQL encoding.
No user selector, CAS, lease decision, operational clock, or version allocation
runs during application.

The private PostgreSQL driver drains the sorted command stream one admitted
packet and physical row at a time. Edit loads exact edited fields plus all
components of affected unique and nonunique indexes. Preserve/replacement/delete
load no unrelated old fields or bodies. Every command requires its existing
physical ID, preventing accidental nextval allocation. Administrative commands
preserve the initial logical queue sequence; ordinary publication follows later.
Any command/storage failure rolls back the entire outer transaction. The owner
must immediately abandon the borrowed Transaction handle on OutcomeUnknown;
it cannot continue siblings or write a rejection receipt after server rollback.
Cancelling the application owner requires dropping/rolling back its containing
transaction; public caller cancellation must leave the owned backend job running.

The new physical fixture exposed incorrect sparse ownership flags in existing
persist_delta. Both edits and complete replacements now flag unique ownership
only when every component exists, while retaining the sparse query-index row.
Old sparse flagged rows cannot match prospective complete tuples because typed
full tuple bytes encode each missing component distinctly; no repair is needed
for ownership correctness.

This remains private prerequisite qualification: no administrative ACK or
receipt is created. Logical publication/result limits, atomic ordinary result
publication, public administration/preview, S3/Turso parity, Snorri integration,
and settled comparable benchmarks remain outstanding.
