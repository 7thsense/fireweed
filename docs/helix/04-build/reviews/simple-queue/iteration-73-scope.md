# Iteration 73: admitted owned raw queue append

Fireweed pins the exact reviewed object-log lifetime revision 9cce82740c69ed61c3c822b30440165227e02a78.
The shared append state machine has an opt-in owned path. Preparation deadlines
drain already-started I/O without submitting a produce. Post-position deadlines
retain completion, snapshot the actual deadline phase, and remain ambiguous even
if completion later succeeds. Existing legacy append policy remains unchanged.

A whole consuming AppendWorker owns a same-job verified namespace, admission-
carrying record Bytes, engine-owned transport/copy/header lifetime, and admitted
result delivery. Only privately qualified standard local/S3 construction admits
raw writes. Arbitrary wrapped/custom constructors cannot inherit qualification.
The gate excludes persistent alias caches. Headers include complete data/meta
prefixes, diagnostic identity and local path bytes before dispatch.

A cold epoch read uses the existing bounded admitted metadata reader: actual
local size is admitted before allocation; S3 uses HEAD and exact IfMatch reads.
The legacy metadata read remains unchanged. Tests reject an oversized epoch
before allocation/append and leave its cache uninstalled.

Physical tests cover success/cold replay, definite fencing, post-position
high-water failure, memory/custom rejection, and controlled preparation/data
PUT/high-water PUT deadlines with and without caller cancellation. Final
reservation notification is separate from ACK and bounded worker drain.

This lower-level raw writer does not establish first-genesis uniqueness,
operational freshness, queue poison/recovery, claims, receipt retention or atomic
projection/cursor application. No QueueStore factory is activated. Global
manifest/index/cache residency and remote durability/performance qualification
remain gates for the completed S3/Turso backend.
