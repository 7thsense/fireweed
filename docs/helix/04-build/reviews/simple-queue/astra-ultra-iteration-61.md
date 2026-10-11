# Astra Ultra iteration 61 review

Reviewer: gpt-6-astra, Ultra; existing read-only astra_plan_review agent.
Base: 1adfa3f9. Decision: APPROVE.

Scope: shared resolved FWQP2 assembly in queue-spool/publication.rs, exact-pin
encoder delegation, verified packet bytes and owned native frame preparation.
Two source heads merge administrative/ordinary commands through the existing
builder. Every command, identity order, complete EOF, receipt binding and final
size validates before owned bytes escape. Any failure poisons joint scratch.

Native request payload and record/field/container/selector headers are charged
before cloning. The complete request is captured and dropped with its guard under
the owned worker; source/frame/provisional guards survive errors and cancellation.
The actual PostgreSQL fixture checks exact full N/N-1, FP4, command order,
producer-order ordinary outputs, insertion sequence and atomic application only
after full frame preparation. Shared physical coverage truncates a retained
command after predecision and proves Unknown, no frame, poisoning and cleanup.

Astra reviewed all source and scope and independently checked diff hygiene.
Verification: 186 engine tests (0.37 seconds), 49 physical spool tests
(6.12 seconds), actual PostgreSQL fixture (1.37 seconds), strict engine/PostgreSQL
libraries and spool all-targets Clippy, formatting and diff checks.

Approval is preparation-only. Complete durable receipts, envelope-wide preflight,
public commit/preview and supporting DTO/decoder activation, S3/Turso, Snorri and
settled benchmarks remain outstanding.
