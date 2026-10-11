# Iteration 71: admission-carrying shared record payload

OwnedQueueLogRecord can transfer its complete ownership into Bytes::from_owner.
Every clone/slice retains the record's admission until the last physical alias
is dropped. This closes the gap where object-log can reply while its flush work
still holds surviving payload aliases. Exact payload bytes and all wire formats remain
unchanged. The terminal-callback regression retains only a slice after dropping
the job/original/clone, checks unchanged admission, then verifies final cleanup.

This is an ownership prerequisite, not native append activation. Stores that
retain input Bytes as persisted data (including current MemoryBlobStore) would
retain operation admission indefinitely; the native writer must initially use
local/S3 stores or a separately reviewed detachment policy. Transport metadata
and started pre-phase reads also need owned completion. Post-timeout drain and
queue poisoning/recovery remain required before native writer activation.

Copied upload buffers are a separate failure gate. Small multi-chunk S3 PUTs
can merge payloads into a new buffer; a commit-worker panic may drop original
payload owners before separately retained uploads finish. Native append still
needs an engine-owned terminal lifetime token that survives upload draining,
or equivalent proof of final physical completion. This primitive does not
solve that path.
