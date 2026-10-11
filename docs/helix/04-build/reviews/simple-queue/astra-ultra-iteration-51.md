# Astra Ultra iteration 51 review

Reviewer: gpt-6-astra, Ultra; existing read-only astra_plan_review agent.
Base: 7268addf. Decision: APPROVE.

Scope: shared native PostgreSQL cursor/hydration/probe source; shared owned
administrative scratch runtime adapter; typed native preparation retaining
ordinary addressed authority; actual DB and physical lifecycle qualification.

Astra reviewed the full source, including owned_administrative.rs, dependency
changes, tests and scope note. The explicit Ready/InFlight/Sealed/Poisoned state
machine takes/restores ownership outside physical I/O mutexes. Whole-object
TakenOwner methods preserve cancellation Drop handling. Queued cancellation,
concurrent/terminal use and failures poison the sink; dispatched work keeps its
files/admission. Sealing consumes exact source EOF and returns summary/resources
together. Native source extraction preserves selection/cursor behavior, and
whole-TX rollback remains mandatory on source/sink failure.

Verification: 43 physical spool tests (1.33 seconds), including queued/dispatched
cancellation and final-reservation cleanup; 14 actual PostgreSQL queue-store
fixtures (48.26 seconds), including 5,001 accepted native changes under a 1 MiB
admission ceiling without SQL data/sequence changes and rollback after a late
source fault; strict PostgreSQL library/tests and spool all-target Clippy; cargo
fmt; git diff --check. Astra independently confirmed a clean diff check.

The fixture retains actual ordinary Loaded authority unchanged. Never-present
identities have no versions-map entry and use the planner's zero highwater;
existing tombstones retain their explicit durable highwater.

Approval excludes final combined uniqueness/ordinary clock/CAS decisions,
logical-size/result receipts, resolved SQL application, public administration/
preview, and S3/Turso parity. These remain required follow-on work.
