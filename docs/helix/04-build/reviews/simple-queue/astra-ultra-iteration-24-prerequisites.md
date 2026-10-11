# Astra Ultra review — iteration 24 activation prerequisites

Model: gpt-6-astra, Ultra. Base: 7c66bc0f.

APPROVE prerequisites and activation design; the activated diff still requires
source review and public-path qualification before landing. Direct assertions now
cover edited additional values and affected full index keys. Remove-only Edit
contains zero Set values and deletes only its exact full name, preserving sibling
names/rows even under forced hash collisions. Both guarded physical fixtures and
strict Clippy pass. Reviewer independently verified diff hygiene.

Automatic approval review rejected source activation again, after the concrete
implementation and test evidence, because removing the Complete-only guard
requires explicit user approval for that exact data-integrity-sensitive action.
Neither rejected command executed. A review-only activation patch is prepared in
/tmp/fireweed-selective-activation.patch and explicit approval is requested.

This commit lands the approved prerequisite tests only. Production remains
Complete-only and partial persistence stays blocked. User approval, activated
source review, public conformance/cancellation/process-loss checks and the shared
benchmark oracle remain required for activation.
