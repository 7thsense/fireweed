# Astra Ultra review — iteration 36

Model: gpt-6-astra, Ultra. Base: d25b0aa8.

APPROVE the asynchronous native-source bridge. It drives a pinned futures stream
through the same admitted accumulator and finalizes only after successful EOF.
Source suspension retains accepted effects and charges; source failure discards
all effects and maps to OutcomeUnknown. Owner-future drop releases admission.
The backend remains responsible for owned caller-cancellation isolation and
retaining the queue fence through final planning and atomic publication.

A manually polled regression suspends after an accepted mutation, verifies no
early clock/result, resumes through success and late error, checks retained output
charges, and verifies owner-drop cleanup. All 124 engine queue tests, the focused
async regression, strict engine library Clippy, formatting and diff checks pass.
Astra independently checked the actual diff and reported no compatibility,
allocation or semantic blocker.

No native selector, public administrative operation or wire format is activated.
The next compiler/provider implementation may be staged privately and physically
tested; public activation must include durable administrative results and replay,
exact size outcomes and preview together.
