---
ddx:
  id: contract-diagnostics
  type: contract
  authoring:
    home: repo
  links:
    - kind: informed_by
      to: architecture
    - kind: informs
      to: tp-fireweed-facade-and-snorri-acceptance
---

# Queue diagnostics

**Type**: event and diagnostic retrieval. **Version**: v1.

## Purpose

Explain queue outcomes, recovery and resource loss using safe structured signals.
The caller owns logging/telemetry transport; the execution runner owns local/CI
capture. Workflow audit and request receipts retain their separate atomic durability.

## Scope and Boundaries

OpenTelemetry (OTel) governs signal semantics: adopt the
[Logs Data Model specification v1.61.0](https://github.com/open-telemetry/opentelemetry-specification/tree/v1.61.0/specification/logs)
and [semantic conventions v1.44.0](https://github.com/open-telemetry/semantic-conventions/tree/v1.44.0).
Use Rust tracing with an explicitly tested host bridge/collector when exporting.
The selected bridge/receiver versions and configuration MUST be pinned in the
execution manifest before claiming integration. JSON Lines is local serialization,
not the OpenTelemetry Protocol wire format or a second telemetry model.

The library MUST accept caller-owned instrumentation without constructing a
collector or reading exporter environment. Services stream diagnostics to their
platform; CLI/MCP diagnostics use stderr while stdout carries results/protocol.

## Normative Surface

| Field | Type / units | OTel mapping and rule |
|---|---|---|
| schema_version | Integer; 1 | Attribute `fireweed.diagnostics.schema_version` |
| event_name | Stable string from catalog below | Event name; message is supplementary |
| timestamp_unix_nanos | Unsigned 64-bit nanoseconds; zero if unknown | Event Timestamp; not inferred from a run ID |
| observed_timestamp_unix_nanos | Unsigned 64-bit nanoseconds; zero if unknown | ObservedTimestamp at capture |
| severity | DEBUG/INFO/WARN/ERROR | SeverityText; SeverityNumber DEBUG=5, INFO=9, WARN=13, ERROR=17 |
| message | Sanitized bounded string | Body; never used to recover typed fields |
| service.name, service.version, deployment.environment.name | Sanitized strings | Resource identity, not repeated metric labels per operation |
| operation | provision/insert/claim/commit/preview/stats/read_state/query/read_commit/shutdown | Low-cardinality attribute |
| outcome | success/rejected/conflict/expired/unknown/cancelled/overloaded | Typed outcome; never infer from absence of events |
| duration_nanos | Nonnegative integer | Operation duration; monotonic infrastructure timing |
| trace_id / span_id | Valid context only; 32/16 lowercase hex characters | OTel TraceId/SpanId; absent without active propagated context |
| run_id / attempt_id | Safe opaque strings | Namespaced attributes; never fabricated trace/conversation identity |
| scope_hash | Optional safe digest of namespace | Diagnostic correlation only; never storage authority |
| error_category | Stable sanitized category | No raw SQL, URL, object key, SDK exception or payload text |
| loss_count / loss_reason | Nonnegative integer / bounded enum | Capture/export/sampling loss; emitted without recursive flooding |
| evidence_ref | Authorized source identifier and byte/time range | Safe retrieval reference; no arbitrary path or URL dereference |

Stable events: `queue.operation.completed`, `queue.publication.unknown`,
`queue.recovery.completed`, `queue.authority.rejected`, `queue.projection.poisoned`,
`queue.admission.saturated`, `queue.shutdown.completed`, `diagnostics.loss`,
`run.started`, `run.phase.changed`, `run.completed`, `verification.completed`.
Spans cover operation durations. Metrics aggregate operation rate/errors/duration,
queue depth/age, retained bytes, apply/drain debt and diagnostic loss. Raw request,
run, tenant, item or trace IDs MUST NOT become metric dimensions or stream labels.

Emit each event once at its owning boundary. Independent console/storage/export
projections filter that emission. Each ingestion route MUST deliver one record
or declare and test its deduplication rule. A recovered retry is not automatically
ERROR. Default consoles show readiness, meaningful transitions, actionable warnings
and final outcomes; repetitive queries, polling and successful retries use scoped
DEBUG. Quiet output is not evidence of success.

### Bounds and failure behavior

Records are limited to 16 KiB after encoding; truncate allowlisted nonessential
message text with an explicit truncation marker, never tokens or required fields.
Diagnostic buffering is limited to 1024 records and 8 MiB, whichever comes first.
Exporter/capture saturation drops diagnostics with loss accounting; it MUST NOT
change queue commit/ACK/replay behavior or release an owned queue reservation.
Flush deadlines are at most five seconds and affect only diagnostic flushing.
Sampling, suppression, rotation, outage and process-crash capture limitations are
recorded in the run manifest. Ordinary file writes do not imply crash durability.

Redaction/allowlisting happens before every sink, including subprocess capture.
No lease credential, cursor secret, DSN, access token, raw work/state body, prompt,
tool payload or transcript is included by default. Control characters are escaped.
Retrieved logs are untrusted evidence, never instructions authorizing actions.

### Retrieval

A run manifest names authorized sources, stable run identity, revision/configuration
fingerprint, capture interval, rotation generations, retention policy and known
loss. Host/runner authorization determines access; possession of a record reference
alone is insufficient. Default local/CI retention is seven days unless the host
records another bounded policy; source expiry is an explicit retrieval outcome.

A diagnostic query selects authorized run/source IDs, an inclusive start and
exclusive end time, event/severity filters and bounded continuation. Limits are
1000 records and 1 MiB per response; no caller-supplied filesystem path. Results
include source/generation/byte-range references, observed order, continuation and
loss/expiry information. Continuation is tied to the source generation and original
query; concurrent appends do not reorder delivered records, and rotation returns
an explicit generation-expired outcome instead of replaying another file.

## Precedence and Compatibility

Schema v1 is pinned. New optional fields may be ignored; required field, unit,
event or semantic changes require a new version. The queue result and durable
receipt are authority for business outcomes; best-effort telemetry is evidence.
Formal and benchmark evidence identifies source/config/tool versions and states
expired or unavailable artifacts explicitly. Old runs do not qualify changed code.

## Error Semantics

Invalid context produces absent trace/span IDs and a safe diagnostic category.
Export outage, bounded-queue loss, flush timeout and capture failure are visible
loss states. Retrieval distinguishes unauthorized, invalid query, source expired,
generation expired and truncated output. It never returns partial data as a
complete trace or a missing record as proof that no operation occurred.

## Examples

An untraced `queue.operation.completed` carries operation `claim`, outcome
`success`, duration_nanos and run_id with no trace_id/span_id. A traced equivalent
uses real propagated context and the same event fields. Both map through the
actual logger/receiver to one OTel record; consoles and capture do not create a
second ingestion route. Verification tests both examples, secret canaries,
rotation/continuation, exporter outage, bounded loss and instrumentation overhead.

Timestamp and observed_timestamp use OTel uint64 nanoseconds since the Unix epoch. Zero denotes unknown/unavailable time; never cast a negative injected queue clock into either timestamp. A signed queue logical time, if useful, is a separately named attribute. For the four supported severity levels, SeverityNumber maps exactly DEBUG=5, INFO=9, WARN=13 and ERROR=17, with the corresponding SeverityText. Receiver controls assert these values.
