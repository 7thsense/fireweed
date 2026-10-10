# Astra Ultra iteration 15 review — 2026-10-10

Reviewer: gpt-6-astra at ultra. Verdict: APPROVE. Base 434b6af1.
Scope: generic indexed additional attributes, native PostgreSQL storage and
versioned definition persistence. No blocking findings remain.

Declared fields remain strict alongside explicit additional namespaces. Full
names and full typed values disambiguate probes, and range predicates constrain
the exact scalar tag. Atomic replacement and deletion regenerate/remove indexes.
Additional payload/header accounting precedes owned hydration; group expansion
includes retained old fields. FWQD4 measures and decodes legacy and modern
layouts without reassigning field/state collection ordinals. Unknown frames and
trailing bytes fail closed; semantic reprovision preserves existing authority.

Astra reviewed final source/docs and independently checked the diff. Root verified
all three native PostgreSQL conformance/reopen scenarios, forced discovery-hash
collisions, acknowledged writer-process-loss through its parent harness, legacy
and nonempty modern namespace header accounting, 23 core and 59 engine tests,
strict library Clippy, TLS compilation, formatting and diff checks. Shared tests
include mixed-type In, one-sided typed ranges, additional-field state queries,
version-fenced deletion, selective claims, long literal names and atomic cleanup.

Approval covers the generic indexed-attribute slice only. Administrative intents,
preview, numeric bucket stats, S3/Turso parity, consumer JSON-pin enforcement,
legacy migration qualification, performance/progress lanes and activation remain
outstanding.
