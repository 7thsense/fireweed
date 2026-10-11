# Iteration 61: shared resolved FWQP2 frame preparation

Base: 1adfa3f9. Shared spool assembly merges a verified external administrative
command source with the input-bounded ordinary command source into the existing
admitted FWQP2 builder. Two source heads and one last identity suffice; no broad
command map or full selected-row image is created. The exact pin encoder reuses
FWQP1 writing without copying the persisted definition. Verified scratch packets
expose their canonical resolved bytes while retaining their admission guards.

Shared preparation validates context, full receipt binding, every command,
complete source EOF and final frame size before returning owned bytes. Any error
poisons the joint scratch owner. Late command truncation after predecision is
caught, no frame escapes and the source cannot be reused.

PostgreSQL first obtains the fully validated size decision, precharges copied
request/intent payload and all container/node headers, then runs shared assembly
inside its owned worker. The returned frame and provisional physical bindings
retain their own job charges. Oversized groups return no prepared frame.

The actual PostgreSQL fixture checks full frame N/N-1, strict legacy-reader
rejection, FP4 binding, sorted work/state commands, original insertion sequence,
new-work sequence, ordinary producer-order receipt outputs and atomic application
only after full frame preparation. Shared physical corruption coverage checks
permanent poisoning and complete quota/admission cleanup.

Public activation still requires envelope-wide preflight, complete durable
Postcard receipts and supporting DTO/decoder/preview changes. The frame is not
standalone ACK authority. S3/Turso, Snorri cutover and settled benchmarks remain
outstanding.
