---
ddx:
  id: build-sp04-object-store-observability
  depends_on: [build-sp03-sequenced-metadata-boundary, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: verified_by, to: tp-scale-substantiation}
  review:
    self_hash: b75fdf641cb7d51d5baedf66abe2569b7ae19d2722fec456710c887204508706
    deps:
      build-sp03-sequenced-metadata-boundary: 6634c5fd29d1980929354abc206f44a274102462a3fb210f9a4842a8e985e280
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
    reviewed_at: "2026-07-20T00:01:31Z"
---

# Implementation Plan: SP-04 Object-Store Observability Below Retries

## Scope

Instrument blob operations at the physical-attempt layer so physical calls and bounded protocol-loop attempts are both visible.
Cover get, put/create, conditional update, delete, list/range-list, and composite stats/head operations. Exclude a
telemetry vendor dependency, runtime metrics endpoint, socket-timeout behavior change, and all tenant/queue/key/URL labels.

## Shared Constraints

- The current `BlobStore` and S3 request path are single-shot; no transport retry wrapper exists below them.
  The instrumented store records one event per trait call. LIST physical request count is derived from
  `list_with_request_count`/`list_from_with_request_count`; aggregate list latency is recorded once, not per page.
  Bounded manifest/fence/acquire protocol loops
  record logical completion and derive retry count from their physical calls. This iteration does not silently
  add network retries or claim a nonexistent layer.
- Constructor types enforce `protocol operation -> instrumented BlobStore -> provider`. A future native-async
  store adapter must use the same recorder vocabulary; this slice does not reopen the whole-transaction
  blocking-adapter decision.
- Stable labels: operation, object class, result class, retryable, backend kind. Error strings are never labels.
- Introduce an object-log-local structured `BlobStoreFault` classified at the provider boundary before mapping
  outward to `EngineError`. Result/retryable/throttle classes come from this type, never parsed strings.
  Timeout is a reserved-zero class in this iteration; an in-flight gauge exposes hung calls without changing
  socket behavior.
- Record latency, request/response bytes where knowable, attempts, retries, and throttling/timeout/error counts.
- Metrics do not allocate on the success hot path beyond the selected recorder and do not change retry behavior.

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 1 | Add net-new schema to TD-004/TP-002: units, label allowlist, method policy, latency semantics, and protocol examples | schema review |
| 2 | Add structured `BlobStoreFault` and provider classification, preserving outward engine errors | exact HTTP/IO/throttle classes; timeout reserved zero |
| 3 | Add recorder/no-op, fixed enums/atomic buckets, wrapper, composite helpers, and in-flight gauge | exact series, pagination, no double counting |
| 4 | Enforce wrapping inside the private segmented-log construction funnel and instrument protocol loops | every root/bypass covered; N iterations/one logical op |
| 5 | Reconcile `SegmentCounters` and retire E3 `MeasuredBlobStore`; add first pull-style shared snapshot and release-ledger feed | equality/cardinality and ledger tests |
| 6 | Benchmark no-op and enabled recording on InMemory/LocalFs group commit | no-op overhead <=2% median; roadmap p99 bar |

## Issue Decomposition

Land the telemetry vocabulary before adapters. Do not add per-backend metric names; backend is a bounded label.
Keep tracing/log correlation optional and outside metric identity. Recorder snapshots become the source of
truth; legacy `SegmentCounters` request/byte fields are derived or equality-checked until consumers migrate,
and the live E3 harness uses the production recorder rather than a third counter wrapper.

### Trait method policy

| Method | Policy |
|---|---|
| `put`, `put_if_absent`, `get`, `delete` | instrument as one physical trait call with bytes/result class |
| `list_with_request_count`, `list_from_with_request_count` | instrument once; charge returned page/request count as physical attempts |
| `list`, `list_from` | wrapper routes through the corresponding count-returning method; no second event |
| `list_page` | instrument as one bounded LIST call; local default and remote override report one request when `limit > 0`, while `limit=0` performs and reports zero physical attempts |
| `stats` | logical span only; delegate to the provider's optimized stats implementation and do not attribute its introspection reads as workload GETs |
| `read_manifest_head` | logical span only; extracted helper invokes instrumented list/get primitives |
| `update_manifest_head_if_version` | logical span only; extracted helper invokes read-head and CAS primitives |

Head composites are extracted into shared helpers and never delegated directly to the inner store, preventing
their primitive calls from bypassing the wrapper or double-attributing bytes. `stats` is the explicit exception:
it preserves LocalFs/provider metadata-walk optimizations and is labeled as introspection, not workload traffic.
Object-class parsing uses path
components and an `other` fallback, so it works above `NamespacedBlobStore` and with hostile keys.

All production roots—server `ObjectLogSpec::open_blob_store`, embedded `open_embedded_object_log`, and direct
`ObjectLog`/SQLite/conformance constructors—reach a private `open_instrumented` segmented-log funnel that
requires the wrapper newtype. Public raw-store constructors wrap exactly once; only a `cfg(test)` raw helper
may bypass it for wrapper tests. The wrapper sits above namespacing, observes logical key components, and
feeds the same provider calls. Dedicated object-log worker queue wait is not physical service latency; TP-002
ack latency continues to measure queueing plus service separately.

## Validation Plan

- [ ] `acquire_epoch`/`fence_epoch`/branch retry counts equal loop iterations minus one; physical call counts
      include their multi-call attempts, while legacy winner mirrors are not mislabeled as CAS retries.
- [ ] Single-shot provider calls never fabricate retries; timeout remains reserved zero and in-flight stays nonzero for a hung call.
- [ ] Bytes and duration are attributed once at the correct layer.
- [ ] Label cardinality is statically bounded and tested against hostile keys/errors.
- [ ] Disabled instrumentation is behaviorally transparent.

## Risks and Rollbacks

Wrong wrapper/composite policy hides calls or double-counts latency. Enforce one private construction funnel and
add bypass/double-count tests. The pull snapshot/ledger feed can be disabled independently; the wrapper can
revert without durable-state changes.

## Exit Criteria

All blob implementations share one instrumented path, recorder snapshots are the reconciled request/byte/cost
source for TP-002 E3, no endpoint/vendor dependency is introduced, and the hot path stays within budget.

## Implementation Status (2026-07-18)

Slices 2–5 are implemented: structured provider-boundary faults, fixed atomic snapshots/deltas, the universal
segmented-log construction funnel, composite head accounting, bounded protocol retry rows, reconciled
`SegmentCounters`, and E3 migration from `MeasuredBlobStore` to a scoped production recorder. Deterministic
tests cover hostile cardinality, disabled/nested transparency, in-flight calls, exact bytes/attempts,
pagination without fabricated retries, S3 HTTP/transport/corruption classes, stats isolation, acquire/fence
CAS retries, bounded branch floor retries, and legacy mirror zero-retry behavior.

The timing gate in slice 6 requires an interleaved same-run disabled-recorder control under representative
load. Functional evidence proves the disabled path uses no clock or atomic recording and allocates no bucket
table, but does not claim the 2% median bar here.

The implemented schema uses counts for completions/attempts/retries/errors/throttles/timeouts, bytes for
request/response payload bodies, and monotonic nanoseconds for summed latency. Request bytes exclude keys,
paths, queries, and headers. S3 response bytes are actual post-header bodies accumulated across all LIST pages
and terminal error pages; generic providers report zero when wire payload size is unknowable. Paged LIST
attempts are not retries, zero-limit pages perform zero attempts, and only physical provider calls affect the
in-flight gauge. GET/DELETE misses and create precondition loss are non-error completions. Logical
head/acquire/fence/branch rows remain separate from billable primitive totals.
