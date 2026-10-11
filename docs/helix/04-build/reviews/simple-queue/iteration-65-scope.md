# Iteration 65: immutable genesis and resolved-record wire binding

Base: 7797f050. FWQL1 is one raw authoritative record per logical log offset:
magic, little-endian u64 writer epoch, kind byte, u64 payload length and payload.
Epoch must be nonzero; it is not insertion sequence, external log position or
projection cursor. Kind 0 genesis contains length-framed FWQADDR1 address,
length-framed exact persisted definition and 32 opaque cursor-secret bytes.
Kind 1 contains one existing complete FWQP1/FWQP2 frame. The sole shared resolved
publication reader handles both versions; old strict readers/goldens are unchanged.
No API-007 bytes are disguised as legacy QueueCommand values.

Borrowed whole-record parsing checks version/kind/lengths, complete EOF, strict
address framing, structural definition accounting and embedded publication framing.
GenesisView deliberately represents structure only: semantic use requires
validate_definition under an owned job. AdmittedQueueDefinition runs existing
strict decode_definition after byte/header admission, retaining original bytes,
validated schema and SHA256 pin. It never decodes/re-encodes to derive identity.
Genesis encoding requires this unforgeable validated handle from the same job.

Output and retained-input overlap are admitted before allocation. Owned record
bytes precede the guard; debug output reports only length and never cursor secret.
Frozen 118-byte genesis and wrapper literals are independently derived. Tests
cover full truncation/trailing/unknown/overflow validation, N/N-1 bounds, structurally
valid but semantically invalid schemas, exact legacy pin preservation, same-job
ownership, and original FWQP1/FWQP2 byte retention.

This codec establishes no genesis uniqueness, first-position authority, immutable
winner selection, fresh receipt/predecessor approval or atomic data/receipt/external
cursor publication. A concurrent provision loser must read the winning exact
schema and cursor secret rather than replace them. Native append must verify the
opaque namespace and expected epoch; projector must bind original definition and
external position, then apply atomically without selecting/reclocking commands.
Raw producer/reader integration, claim credential receipts, S3/Turso factories,
checked migration and identical conformance remain outstanding.

Review corrections: both encoders retain a private bytes-first writer container
through every fallible operation, including epoch-zero failure, before converting
to the owned record. Capacity cleanup is regression-tested. Genesis and publication
wrappers share the borrowed nonblank address policy without changing FWQP bytes.
