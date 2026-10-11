# Astra Ultra review — iteration 45

Model: gpt-6-astra, Ultra. Base: 8c1ddce2.

APPROVE exact-reservation owned buffer transfer and physical worker ownership.
Pointer identity must match the reservation; transfer zeroes the consumed scoped
guard without changing admission counters. owned_buffer reuses existing rules.
Private non-Clone guards hold reservation Arc until buffer capacity releases.
Spool files still drop before disk/memory guards; no duplicate physical path exists.

Verification: 138 engine queue tests and eleven spool tests passed, strict engine
library/spool all-target Clippy, formatting and diff checks passed. Astra checked
the source and scope independently. The optional worker fixture improvement now
merges [3,1,2], reads [1,2,3] and payload/EOF after caller Arc drop and admission
close; eleven spool tests and checks passed again. Astra rechecked it and approval
persists. Foreign-job/capacity cleanup and final slot/base/buffer/quota release
are covered. Live backend scheduler and shutdown/drain integration remain pending.
