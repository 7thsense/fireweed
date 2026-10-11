# Iteration 57: bounded addressed result retention

Base: ab4a0999. A shared borrowed before projection certifies all requested
fields, their types and known absence without cloning. Its seven-field Postcard
serialization matches the owned projection, including full byte values and
signed zero. Existing proposals use the same validation/projection helper.

The bounded addressed driver derives a monotone complete-success receipt floor
from the full request ID, outcome tag, ordinary output count and every full
collection/key identity with minimum positive version encoding. It counts each
borrowed item, the histogram and known list framing before result payload copies.
Once overflow is proven, it drops retained payloads then their separate admission
charges and continues every source position, proposal, uniqueness packet, overlap
check, requested-before certification and successful EOF. The summary explicitly
contains either Complete or Overflow; result() returns Option and never exposes
an incomplete result. Overflow retains the complete histogram and packet proof.

Tests cover exact full receipt N/N-1, duplicate/missing positions, truncated EOF
after overflow, late corrupt requested fields, large bytes/signed-zero/absent
borrowed serialization, and retention -> overflow -> later affordable allocation.
Receipt-floor tests include all four ordinary output vectors, long UTF-8 IDs,
full state identities and versions 1/127/128/u64::MAX.

This is bounded preparation, not public or PostgreSQL activation. The existing
unlimited wrapper remains for reference/current private native callers. The
final exact complete-receipt gate, empty scans, actual larger ordinary versions,
business-rejection precedence, publication overflow, durable receipts and preview
must be integrated before activation. S3/Turso, Snorri and settled benchmarks
remain outstanding.
