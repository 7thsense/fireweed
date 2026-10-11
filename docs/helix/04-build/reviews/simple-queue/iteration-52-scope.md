# Iteration 52: combined native administrative semantics

Base: 85644fc2. This iteration adds the two-pass uniqueness validation and
provisional ordinary planning required after the native administrative scan.
Both command integrity and all initial uniqueness observations are exhausted
before business rejection or the single fresh ownership clock. Administrative
overlap and administrative sibling-claim collisions precede ordinary CAS/lease
checks; final
combined uniqueness permits ordinary inserts to reuse administrative releases.

Two sorted evidence files are merged using full tuple bytes and source-local
ordinals, with one tuple reducer and bounded read windows. Ordinary authority
stays input-bounded; loaded payloads transfer to owned scratch work without
copying or releasing admission. Error paths drop payloads before their guards.

The actual PostgreSQL fixture prepares and validates 5,001 administrative
changes plus workflow state and next-work inserts without SQL mutations or
physical sequence allocation. This remains private provisional planning.
Logical publication/result limits, resolved application, public administration
and preview, S3/Turso parity, Snorri integration, and settled comparative
benchmarks remain outstanding.
