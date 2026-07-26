# Object-log manifest authority and compaction

Fireweed uses one current durable authority protocol. The versioned `authority_head` (or a configured
`ManifestPointerStore`) names the winning immutable `manifest_candidates` entry and the immutable forward
`recovery_index/v1` root in one fenced compare-and-swap. Readers traverse only that winning recovery index;
LIST and reverse candidate walks are not recovery authority.

Current durable namespaces are:

- `authority_head`, `authority_protocol_v1`, and `authority_initialized_v1`;
- `manifest_candidates` and `recovery_index/v1`;
- `seg_candidates` for content-addressed segment objects and `branch-seg` for branch-owned copies;
- `manifest_head/*~watermark.json` for the append-only deletion watermark;
- branch metadata, recovery pins/guards, and recovery-index garbage batches.

An empty queue is initialized with a genesis authority head. A non-empty current namespace without a head,
an incomplete head, a missing winning candidate/index node, or any retired pre-release namespace fails
closed. Fireweed does not infer or adopt another durable shape.

Retention is advance-then-delete: the fenced authority head publishes the monotonic command-sequence floor
before any eligible segment deletion. Deletion-watermark progress is delete-then-advance: each segment in the
contiguous manifest-index prefix is proven absent before an append-only watermark marker is published.
Candidate garbage collection preserves the live authority/index closure and any durable recovery pin.

Bounded maintenance carries exact captured authority (`version + body`) and revalidates it before destructive
work. A successor head, missing proof object, corrupt index, branch pin, ambiguous store result, or exhausted
request budget stops the page without advancing progress. Restart discards soft cursors and resumes from
durable authority and watermark markers.

Performance gates measure physical object-store requests. Sealing is bounded by recovery-index height and
does not scan history. Recovery pages cap commands, index fanout, buffered bytes, and segment GETs. Expiry
and garbage collection cap LIST/GET/DELETE requests and rotate durable namespaces fairly.
