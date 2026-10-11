# Iteration 77: owned native authority read

Pin object-log 9d64be93b701c85dc7e7ea210ca68c4144e3fb90. Fireweed consumes a
same-job, same-store verified namespace into a whole blocking StateWorker,
precharging full wrapper address/prefix/path/header capacity before dispatch.
Only privately qualified native local/S3 construction admits this path. It calls
native authoritative_partition_state directly, without epoch metadata, cached
getters or legacy refresh fallback.

The admitted result retains both namespace binding and its output guard. Queued
caller cancellation retains all operation charges until physical completion and
final reservation release. A physical local test uses independently constructed
sequencer handles, rather than the shared open_local registry, to verify fresh
fence visibility and retained result binding.

This snapshot is a point-in-time target, not exclusive ownership or a pinned
history. Bootstrap must fence then read a fresh target before validated replay;
conditional append still guards publication. Wrapper admission does not cover
full Manifest index loading or residency. Those remain qualification gates.
No QueueStore factory or projector is activated by this iteration.
