# Iteration 70: cold queue history authority validation

ColdQueueHistory is an admitted operation-local validator, not a projection
cursor or acknowledgement frontier. It requires exactly one semantically valid
genesis at offset zero, preserves the original definition bytes/pin and secret,
and accepts both existing resolved publication formats. Subsequent records must
match the full address, original definition identity and insertion sequence, with
contiguous offsets and nonregressing writer epochs. Any failure poisons the
operation; records cannot be skipped toward an apparent EOF.

Tests cover exact legacy genesis, both publication formats, missing/duplicate
genesis, offset gaps, epoch regression, truncation, raw identity mismatch,
semantically invalid genesis and insertion-sequence discontinuity. No golden
format or live codec changed.

This does not publish genesis, apply projection data, commit a durable cursor,
activate factories, or certify remote durability/performance. Recovery still
requires external durable freshness and whole-operation completion ownership.
