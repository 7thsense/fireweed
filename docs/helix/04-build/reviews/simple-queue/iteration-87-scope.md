# Iteration 87: logical claim replay receipt codec

FWCR1 stores ordered full keys, lease credentials and creation versions without
backend physical row IDs. A literal fixture preserves non-key selection order,
embedded NUL/UTF-8 keys, binary tokens and full u64 versions. Empty receipts are
explicit. Complete borrowed structural parsing is allocation-free and validates
count, every entry and EOF; it does not establish current ownership/request
identity or certify duplicate-key publication binding.

The writer admits complete output plus duplicate-key BTreeSet headers before
allocation/copy, rejects duplicate/invalid entries, and retains charge with the
owned output. Astra's drop-order finding is corrected: duplicate-check scratch
is destroyed before its shared guard moves into the fallible output writer.
Creation versions are replay creation evidence, not a requirement that live
renewed records still have their original version.

Two focused tests cover literal order, unsigned values, exact N/N-1 limits,
empty output, invalid/duplicate entries, impossible count, truncation and trailing
bytes. Strict engine production-library Clippy and diff hygiene are required.
No existing wire format, raw-log kind or physical replay implementation changes.
FWQL1/2 unknown claim kinds continue failing closed. FWQC1 publication binding,
atomic native claim application and live replay qualification are outstanding.

Final adversarial control covers every raw truncation prefix, invalid UTF-8,
zero version, empty token and oversized declared key/token lengths. Exact
4,380-byte admission proves failed second output and invalid-entry errors return
to the original counter baseline. Three codec tests, strict production-library
Clippy and diff hygiene passed after the drop-order correction.
