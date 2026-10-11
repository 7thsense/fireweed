# Iteration 55: complete administrative result and receipt codecs

Base: 3594d25e. FWAR1 is a canonical binary result component, not a standalone
receipt/log or publication authority. One fixed writer handles counting and
encoding. Complete validation precedes restartable borrowed item/clause/field
views; decoding retains a fixed number of 11-entry counter arrays and one transient item. Positional
duplicate keys/clause IDs and input order remain intact. Histogram kinds and
before fields have explicit canonical order. Checked totals, presence, positive
versions, before/output increments, conflict versions, raw lease/expiry shape,
finite typed fields, truncation and trailing bytes fail closed. Signed-zero bits
and opaque/text bytes survive unchanged; no clock is reevaluated.

The separate private Postcard receipt projection measures and encodes the whole
future CommitReceipt: request ID, appended outcome ordinal 4, ordinary versions
in producer order, and complete administrative result. TooLarge uses existing
Rejected ordinal 3 and appended rejection ordinal 8. Administrative Invalid diagnostics are limited to 256 UTF-8 bytes on this new
private receipt path. That maximum fallback and full long identities are counted
without allocation; legacy receipt decoding is unchanged. An independent
future derived fixture and literal/hash goldens pin that layout. Public enums,
ordinary fingerprints/envelopes, and FWQP1 are unchanged.

FWAR1 binary size and full Postcard receipt size are distinct; neither substitutes
for the other. Admitted encoding and borrowed decoding qualify multi-mebibyte
before values. This prerequisite does not provide request/source binding,
FWQP2 frames, bounded logical overflow execution, durable result ACKs, public
administration/preview, or S3/Turso parity. Those remain required alongside
Snorri integration and settled comparable benchmarks.
