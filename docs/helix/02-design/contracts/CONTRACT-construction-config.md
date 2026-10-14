---
ddx:
  id: contract-construction-config
  type: contract
  authoring:
    home: repo
  links:
    - kind: informed_by
      to: architecture
    - kind: informs
      to: api-queue-claim-commit-contract
---

This is the target design and its acceptance contract. The public PostgreSQL path validates interface limits, pool size, checked admission/scratch multipliers and the required parsed URL before connection or DDL; configuration and connection-setup errors redact credentials. Typed custom operation, retained-byte, scratch-byte and scratch-worker capacities reach PostgreSQL admission and scratch execution; its typed scratch root is resolved and probed once before backing-service access. S3/Turso assembly and the single Snorri adapter cutover remain pending. Existing code is not qualified merely by this specification.


# Queue construction and configuration

**Type**: library configuration. **Version**: v1. **Authority**: ADR-026, API-007.

## Purpose

Construct one concrete queue with identical observable constraints on PostgreSQL
and S3/Turso. Backend choice and resource configuration belong to construction.

## Scope and Boundaries

The host supplies typed options, an operational clock and optional diagnostics.
The library MUST NOT read environment variables or CLI flags in queue operations.
Hosts translate those sources once before construction. Credentials, SDK clients
and database sessions MUST NOT become consumer workflow state or operation DTOs.

## Normative Surface

| Setting | Ownership / default | Validation and semantics |
|---|---|---|
| Backend | Host; explicit PostgreSQL or S3/Turso | No implicit fallback or capability downgrade |
| PostgreSQL URL | Host secret; required | Nonempty; all displays redact credentials; connection failures do not expose it |
| PostgreSQL pool size | Host; 2 | Integer 1..32; no connection before validation |
| S3 endpoint/bucket/region/credential provider | Host; required qualified backing service | Validate endpoint/bucket/credential-source shape without exposing secrets; endpoint qualification remains explicit |
| Projection and scratch roots | Host; explicit local paths for S3/Turso; PostgreSQL typed `scratch_root`, with `None` resolving the OS temporary directory once | Require absolute resolved paths and validate writable/layout ownership during construction before backing-service access; clones share the canonical immutable root and operations never reread process configuration; raw tenant/queue names never form unsafe path components |
| Queue limits | Shared defaults below | Checked arithmetic, positive counts/bytes and consistent nesting; same settings on both adapters |
| Operational clock | Host; production system clock, test clock explicitly injected | Fresh sampling at claim/commit authority linearization; operator evaluated_at is a separate classification input |
| Admission | Shared runtime; QueueRuntimeOptions; 64 operations and 8×request-byte retained budget by default | Checked multiplication before I/O; all clones share capacity; ownership survives caller cancellation |
| Scratch storage | Shared runtime; 16×group-byte logical temporary quota (2 GiB at default limits) | Counts retained data/index/merge files; exhaustion is explicit; no authoritative second log |
| Scratch execution | Shared bounded runtime; explicit worker and owned-operation capacities | PostgreSQL defaults workers to pool size; explicit worker count is positive and at most the owned-operation capacity; queued/started work is bounded; worker drain and final reservation drain remain distinct. Known synchronous scratch stages may reuse their already-owned physical worker without a new task or permit; one worker must support the full operation. Self-drain fails immediately |
| Infrastructure deadlines | Host/runtime; finite documented backend deadlines | Independent of injected logical clock; an expired publication reply may remain OutcomeUnknown |
| Diagnostics | Caller-owned; disabled exporter permitted | No collector is mandatory; redaction and loss behavior follow diagnostics Contract |

Native async read stages enter the existing shared multi-thread runtime only at
newly dispatched owned physical execution. Their future factory runs in that
runtime, with the same admission, cancellation, panic and completion policy as
synchronous stages. A current-thread target or recursive entry into the same
worker pool rejects before registration; caller runtime flavor is unrestricted.

Default interface limits are 4096 addressed records, 64 independent groups,
128 MiB per group, 256 MiB per request, 128 MiB per response, 1 MiB work body and
64 MiB state value. SelectFirst administration has no record-match cap: its full
resolved unit obeys the common publication/result bound. Identifiers, field names,
key sets and payload attributes have no separate backend-specific count/length cap.
All effective limits are immutable for a handle and shared by its clones.

A host MAY select different explicit resource capacities through QueueRuntimeOptions. Optional byte/worker values derive defaults only when absent; zero and inconsistent bounds fail before I/O. Qualification MUST
record them and compare equal interface limits and durability guarantees. An
internal physical frame or parameter limit MUST NOT silently reduce the public
interface, truncate results, or turn native filtering into a client scan.

## Precedence and Compatibility

Explicit typed construction values are authoritative. Host configuration files,
environment and CLI overrides resolve in the host's documented order; the library
has no hidden override layer. Defaults are applied once and validated as a whole.
Queue definitions are immutable separately provisioned schema/progress policies.
Exact semantic ensure is idempotent; incompatible definitions fail before work.

Changing resource tuning cannot change fingerprint formats, claim/replay rules,
state-version semantics or durability. Unsupported combinations fail construction.
Unknown external configuration keys fail host parsing instead of being ignored.

## Error Semantics

| Condition | Outcome | Effects / recovery |
|---|---|---|
| Invalid limits, pool size or option shape | Invalid configuration / Invalid | No DNS, connection, fence, filesystem publication or durable queue effects |
| Missing secret/source | Invalid configuration | Sanitized setting name; never the secret value |
| Backing service unavailable | Sanitized infrastructure error | No fallback; caller may retry construction |
| Existing authoritative namespace/schema mismatch | DefinitionConflict or OutcomeUnknown for corrupt authority | Preserve original authority; no destructive empty initialization |
| Runtime saturation | Explicit admission/backpressure outcome | No unadmitted durable effect; accepted owned work retains its reservation |
| Deadline after possible durable publication | OutcomeUnknown | Resolve original request identity against fresh authority |

## Examples

A host constructs PostgreSQL with a secret URL, pool size 2, default limits and
SystemClock. The same workflow uses S3/Turso with an explicit qualified bucket,
projection/scratch roots and the same limits. A test substitutes TestClock to
exercise exact lease expiry; it does not substitute the infrastructure deadline
clock. A Debug rendering shows the backend and safe limits, never the URL/token.
