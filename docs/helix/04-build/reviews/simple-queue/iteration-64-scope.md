# Iteration 64: admitted compact physical namespace authority

Base: dc8f75eb. SHA256 of strict FWQADDR1 bytes discovers a physical bucket; it
never decides full identity. Fixed ASCII q7-<64 hex digest>-<16 hex slot> components
fit the existing full-partition hex expansion even at u64::MAX. Lookup starts at
slot zero, publishes immutable candidate bytes, rereads the same slot, and validates
complete framing and bucket before equality. Only a valid different full address
advances to the next slot, without an arbitrary collision limit. Malformed/wrong-
bucket authority stops; checked slot exhaustion is an infrastructure error.
Concurrent same-address creators converge on the full verified winner.

VerifiedApi007Namespace is opaque, retaining full address Bytes, physical QueueKey,
Weak per-store authority token and owned admission. Origin mismatch or dead authority
fails before key extraction. No shard/cache/epoch mutation precedes verification.
Encoding, publication worker payloads, actual-size winner reads and retained mapping
have overlapping owned charges. Local reads open once, obtain size from that file,
admit before allocation and cap reading at size+1; truncation/growth is rejected.
S3 reads HEAD first, admit size, issue If-Match GET against that ETag, verify response
ETag/length, consume bounded chunks and reject excess/truncated bodies. There is
no range-error-as-EOF or unbounded collect path in the new authority reader.

One shared immutable publication helper serves existing queue definitions and new
namespace authorities. Legacy catalog winner selection, created flag, identity
validation and keys remain unchanged. Local publication retains synced temporary
files and atomic hard links, then fsyncs the authoritative directory for BOTH a
winner and an EEXIST observer, plus every ancestor for newly linked directories.
A payload guard moves with a whole local worker container on caller cancellation.
S3 uses existing attested create-only publication; memory uses its existing permit.

Physical tests cover long NUL/slash/Unicode addresses across reopen, forced collisions
of differently sized identities, deterministic concurrent convergence before the
first winner returns, wrong/gone origin, malformed no-skip/no-overwrite, pre-read
capacity rejection and a winner/loser fsync barrier. This does not activate API-007
S3/Turso consumers: separate factory roots, checked legacy migration, raw resolved
log binding/genesis and projector/cursor atomicity remain gates. S3 source support
is not an S3 durability qualification result.

Review correction: opaque mappings bind a per-store Arc token, rather than just
BlobStore identity. A shared blob opened under distinct physical prefixes cannot
accept a mapping from the other authority; dropping the original store invalidates
that mapping even when the shared blob survives.

Detached local read workers pre-admit a whole envelope before path/Arc capture;
publishers charge checked actual root/key multiplicity before cloning paths.
A queued-worker owner-drop test holds the blocking pool and observes charged
header bytes and slot until terminal cleanup. The mapping is operation-scoped;
storing it indefinitely would retain a reservation and block shutdown. A future
long-lived mapping cache requires separate bounded cache ownership/accounting.
Actual long-address/collision log operations cover ensure, distinct epochs, append,
high-water, emission cursors and exact cold-reopen genesis through empty EOF.
