# FWQP1 resolved mutation frame

Iteration 27 introduces an internal codec only. No store appends or projects this
frame yet. It covers coalesced resolved mutations and current ordinary commit and
insert receipts. Claim selection/credential receipts, provisioning/cursor secrets,
administrative whole-operation results and their before metadata require separate
reviewed wire bindings. The final per-logical-group encoded-size rejection policy
is not implemented by this codec: FrameLimit is an internal encoding error, not a
durable business rejection. Admission failure remains an infrastructure error.

All integers are fixed-width little-endian. Counts and byte lengths are u64,
priorities/timestamps are i64, attempts u32, tags u8. Text is length followed by
literal valid UTF-8, including NUL and slash. Bytes use the same framing without
text validation. There is no alignment/padding. Floats retain finite IEEE754 f64
bits, including negative zero; canonical means deterministic explicit resolved
intent, not normalization of arbitrary representations.

Field order is frozen:

1. ASCII FWQP1; tenant text; queue text; 32-byte SHA256 of exact original persisted
   queue definition; initial insertion sequence; final insertion sequence.
2. Command count; commands in strict `(collection option, key)` byte order (None
   first). Identity is collection presence tag 0/1, optional collection text, key
   text. Then expected existing-record presence 0/1, optional existing version;
   prior durable highwater; final durable highwater; effect tag 0 Delete / 1 Put.
3. Work Put: priority, available_at, insertion_sequence, lifecycle tag (0 Pending,
   1 Leased, 2 Completed, 3 Failed), eligible_since, attempts; lease expiry presence
   and optional i64; lease hash presence and optional 32 bytes. Both lease members
   are present exactly for Leased. Fields command 0 Preserve / 1 full Replace /
   2 exact Edit. Replace carries a sorted field map; Edit carries sorted full-name
   entries, each presence tag and optional typed value. Body command 0 Preserve /
   1 Replace followed by bytes. Read coverage/materialization never appears.
4. State Put: full sorted field map followed by opaque value bytes. Delete has no
   payload and retains final durable highwater. Packed absent→create→delete is
   valid when final highwater advances. Existing predecessor version is nonzero
   and equals prior highwater. Absent Put requires full field/body replacement
   for work. Every persisted mutation strictly advances final highwater. Verified existing
   no-op metadata/body/field commands are omitted entirely while receipts remain.
   Absent work requires insertion_sequence above initial_sequence; existing work
   preserves its prior sequence or receives a new one above initial_sequence.
5. Receipt count; receipts strictly sorted by `(domain, request_id)`. Domain is
   0 Insert / 2 Commit; then request identity text, fingerprint-format tag 3,
   32-byte API-007 v3 fingerprint, and result. Commit result tag 0 Committed carries
   count plus positional identity/version entries; tag 1 Rejected carries rejection.
   Insert result carries positional outcome count: 0 Inserted / 1 Replaced carry
   key text and nonzero version; 2 Rejected carries key text and rejection. Original
   item ordering is retained inside a receipt. Conflict/expired replay responses
   never become replacement receipts. Constructors bind outcomes to request
   identities/queue and fingerprint; the store still establishes fresh authority.

Field value tags: 0 Integer i64, 1 Float f64 bits, 2 Boolean presence-style 0/1,
3 Text, 4 Bytes. Field maps/edits use count followed by strictly sorted literal
name and value/presence. Rejection tags: 0 StaleLease, 1 VersionConflict,
2 AlreadyExists, 3 NotFound, 4 Leased, 5 Invalid (reason text), 6 UniqueConflict,
7 Terminal. No tags are inherited from Rust/serde enum ordinals.

The compiler requires sorted/coalesced, bijective effects/version/intent maps.
Borrowed initial highwater/state cursors advance linearly; effect lookup is binary
search and work lookup uses the existing map. It creates no command sorting container or payload clones. Success receipt bounds
use one temporary map of borrowed identities to initial/final versions, charged
before allocation at 192 bytes per entry plus a 1024-byte root/node floor and released before output
allocation. Each acknowledged success must be covered by the final version map
and lie within those bounds, including intermediate packed and no-op outcomes.
Count and encode use the same walker, including magic and receipts.
It charges final encoded bytes plus fixed frame overhead before allocating; the
private bytes and guard remain together, exposed only through a borrowed slice.
The existing source request/snapshot/delta must already belong to an admitted job.

The full zero-allocation scanner checks every field and end-of-frame, rejects
unknown version/tags, invalid UTF-8, unordered/duplicate identities, nonfinite
floats, impossible/truncated lengths/counts, bad predecessor/lease/sequence shape
and trailing bytes. It borrows current/previous names and retains no payload list
or seen-set. It proves structural validity. Projection must still verify expected
queue/raw definition, all predecessors/highwaters and immutable fresh receipts,
and apply all effects, receipts and an **external log cursor atomically**. Insertion
sequence is not that cursor. Checks must precede all mutations/cursor advancement.

Tests pin the 489-byte rich frame SHA256 to
`aaf900b5512de2f9d137a24310256c48654c89ad8c1b5766a056319b754696b2`,
covering literal names, scalar leaves, full work/state Put and commit receipt.
Complete and selective megabyte read views encode identical small metadata frames;
every proper prefix of the rich frame is rejected. Charge lifetime, frame limit,
receipt identity/queue/order, malformed lease/hwm, and absent create/delete packing
are covered. This is codec evidence, not crash replay or S3/Turso qualification.

## Borrowed replay consumption — iteration 30

view_publication validates the entire frame before returning a private-slice
PublicationView. scan_publication delegates to this exact parser; there is one
wire acceptance implementation. Frozen tags and golden bytes are unchanged.
Restartable command/receipt iterators retain only slices, counts and scalar state;
fields, edits/removals, body bytes, version vectors, positional insert outcomes
and rejection reasons remain borrowed. No payload/list allocation, sorting or
per-command scan from frame start occurs. Read materialization is never decoded
as a write. Consumers can perform independent complete authority and apply passes.

The cfg(test)-only complete-image oracle checks queue/raw schema pin, initial
sequence, expected existence/version and exact tombstone highwater, every referenced
existing record's identity/sequence, field schema/type/namespace admissibility,
fresh receipts and success-version bounds before effects. Already-retained matching
receipts cannot authorize a new apply. Omitted verified no-op receipt versions
require current durable authority. Late command/receipt failures leave the original
logical image and receipt map unchanged. The clone and owned test maps are not a
production replay implementation or bounded-memory proof.

Thirteen codec/view/replay scenarios cover frozen scalar/wire branches, complete
versus selective metadata encoding, all truncations, corrupt tags/lengths/types,
restartable lists, atomic state/new work, edits/removals/empty bodies, an actual
uncoalesced Leased Put with Preserve, work/state Delete tombstones, full reinsertion,
intermediate packed receipt versions, inserted/replaced/rejected positional results,
direct rejected-commit decoding, omitted no-op receipts and stale/late authority.

This is logical primary-record/highwater/sequence/receipt replay evidence. The
oracle schema has no unique indexes and does not rebuild physical indexes or
compare speculative unique-probe caches. Stock Turso still must qualify physical
index regeneration, unique authority, admitted bounded rows/buffers and all data+
receipt+external log cursor publication through the existing owned writer pipeline.
No physical projector or new store append route is activated by this iteration.
