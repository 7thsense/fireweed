# Iteration 79: native fence acquisition and append authority

Pin object-log 91760a892b835243c086ce923cefb81d06769bb0. The same operation-owned
StateWorker now performs native epoch acquisition, returning the matching
post-fence replay bounds and retained namespace. Read and acquire share private
qualification, same-job validation, header admission and whole-worker ownership.
An admitted result can transfer its namespace to the next operation stage.

Native conditional append no longer reads legacy epoch documents or caches.
The durable sequencer validates epoch and head together before indexing. Legacy
append retains its serialized epoch checks. Native preparation deadlines still
drain started preparation and never submit after deadline; post-position
ambiguity remains unchanged. The preparation fixture now injects a stalled
native preparation rather than requiring a legacy metadata read.

Physical tests acquire and append genesis without any legacy epoch file/cache,
acquire a newer fence with unchanged head, and reject the superseded writer.
Oversized legacy metadata remains rejected by its bounded reader but cannot
block native publication. Queued cancellation covers both read and acquire;
acquire finishes its durable epoch change after caller cancellation while all
reservation ownership stays held through physical completion.

Full manifest memory/admission and rewrite scaling remain gates. This does not
activate the QueueStore factory, first-genesis provisioning protocol or atomic
Turso projection. Validation: complete adapter suite, strict all-target Clippy
and stock Turso facade feature compilation.
