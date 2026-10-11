# Iteration 81: versioned complete-record integrity

FWQL2 keeps the raw record header/payload framing and appends SHA-256 over the
complete header and payload, including magic, epoch, kind and declared length.
The sole borrowed reader dispatches explicitly by version, verifies exact total
length and checksum before exposing payload, and never falls back after failure.
FWQL1 readers/writers and frozen goldens remain unchanged compatibility paths.

Checked genesis/publication writers use the same format-parametrized encoding
and sealing helpers. Every hard limit, admission and genesis preflight includes
the 32-byte trailer before allocating. New provisioning creates FWQL2 and
retains verified format in its admitted authority. Existing FWQL1 is identified
as legacy rather than silently reframed. Legacy definition bytes are separately
preserved inside checksummed records with their original pin/secret.

Tests freeze literal checked genesis/publication bytes, verify exact N/N-1
bounds, reject every single-bit genesis corruption and every single-byte
publication alteration, and reject truncation/appended bytes. Physical tests
alter a cursor-secret byte while leaving a valid shape, distinguish real FWQL1
from FWQL2 containing legacy definition bytes, and cold-reopen both explicitly.
Validation: 462 engine tests, 110 adapter tests, strict engine library/adapter
all-target Clippy, stock Turso facade feature compilation.

This detects corruption of one complete record. It does not authenticate
provenance or prevent replacement with another valid record or removal of whole
valid records. External history/epoch/pin/atomic cursor checks remain required.
The eventual factory must enforce its integrity/migration policy for EVERY
recovered record, not only genesis. No factory/projector activation or full
manifest/performance qualification is included.
