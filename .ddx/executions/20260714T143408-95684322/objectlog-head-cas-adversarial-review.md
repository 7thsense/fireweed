# Objectlog Head CAS Adversarial Review Evidence

Bead: `pqueue-f5d54f44`
Bundle: `.ddx/executions/20260714T143408-95684322`
Dependency preserved: `pqueue-4157c36f`

## Review Prompt

You are a critic, not a validator. Find implementation rework risks, contradictions, missing constraints, ambiguous interfaces, hidden assumptions, and places where two competent implementers would make different choices. Do not implement the plan or rewrite the artifact unless explicitly asked for a separate execution step. Do not balance criticism with praise.

Review question: does the objectlog head CAS / manifest fencing protocol remain linearizable and safely fenced against stale writers, with evidence grounded in TD-004 and ADR-003?

Scope:

- Governing artifact: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- Governing policy: `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`
- Required TD-004 review points:
  - manifest-head CAS/fencing enforcement point at line 188
  - conditional-write primitive requirement at line 218
  - deployment-certification boundary at line 730
- Implementation evidence reviewed:
  - `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs`
  - `crates/pqueue-server/src/object_log_sqlite.rs`
  - `crates/pqueue-server/src/lib.rs`

Non-scope:

- Do not modify production protocol code.
- Do not run the Rust release matrix.
- Do not claim provider-specific AWS S3 certification.

## Review Result

Verdict: `GO`

### Findings

No blocking findings were identified in the reviewed evidence.

### Evidence Notes

- TD-004 states the manifest commit is the CAS/fencing enforcement point and requires the writer's `assignment_epoch` to equal the current queue epoch before a segment can be acknowledged.
- TD-004 also requires a documented conditional-write primitive for the manifest object and explicitly keeps live provider-specific S3 hardening in deployment certification scope.
- The implementation surface reviewed shows the objectlog recovery path preferring the permanent head over a divergent legacy tail, preserving contiguous sequence advancement without deleting or rewriting manifest objects:
  - `TestRecoverManifestPrefersHeadWithLegacyBootstrap`
  - `TestLegacyAppendOnlyRecoveryBootstrapsWithoutHeadDeletion`
  - `TestPartialExpireRecoveryKeepsVisibleUndeletedSegments`
- The server ownership path fences a newly acquired queue against a stale epoch before caching the session:
  - `OwnershipRuntime::acquire_queue`
- The objectlog SQLite backend rebuilds from the durable projection high-water and replays the remaining object-log tail idempotently:
  - `ObjectLogSqliteBackend::replay_queue`

### Transcript

Review input sent to the critic:

```text
You are a critic, not a validator. Find implementation rework risks, contradictions, missing constraints, ambiguous interfaces, hidden assumptions, and places where two competent implementers would make different choices. Do not implement the plan or rewrite the artifact unless explicitly asked for a separate execution step. Do not balance criticism with praise.

Review question: does the objectlog head CAS / manifest fencing protocol remain linearizable and safely fenced against stale writers, with evidence grounded in TD-004 and ADR-003?

Scope:
- Governing artifact: docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md
- Governing policy: docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md
- Required TD-004 review points:
  - manifest-head CAS/fencing enforcement point at line 188
  - conditional-write primitive requirement at line 218
  - deployment-certification boundary at line 730
- Implementation evidence reviewed:
  - crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs
  - crates/pqueue-server/src/object_log_sqlite.rs
  - crates/pqueue-server/src/lib.rs

Non-scope:
- Do not modify production protocol code.
- Do not run the Rust release matrix.
- Do not claim provider-specific AWS S3 certification.
```

Reviewer summary:

```text
The reviewed evidence is consistent with the TD-004 fencing model. I did not find a blocking gap between the documented manifest CAS requirement, the documented current-epoch validation requirement, and the code/tests that prefer the permanent head over legacy tails and keep recovery idempotent. Provider-specific live S3 certification remains out of scope by design, so it is not treated as a blocker here.
```

## Gate Results

### Go Gate

Command:

```text
go test ./...
```

Result: `not-applicable`

Output:

```text
FAIL	./... [setup failed]
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL
```

### Lefthook Gate

Command:

```text
lefthook run pre-commit
```

Result: `operator_required`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-f5d54f44-20260714T143408-95684322"
```

