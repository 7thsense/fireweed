---
ddx:
  id: stock-turso-qualification-20261008
  type: implementation-evidence
  flow: helix
  status: active
  links:
    - type: informed-by
      target: td-object-log-turso-projection
---

# Stock Turso qualification — 2026-10-08

Fireweed now consumes the published `turso` and `turso_core` 0.8.2 packages.
The five engine/SDK vendor directories are removed; no backports or custom
Turso engine patches are part of the active dependency graph.

Adapter changes use public APIs: read row values with `get_value`, avoid an
extra rollback when `INSERT OR ROLLBACK` already aborted the transaction,
recognize covering-index seeks while retaining the complete key predicates,
and use supported native checkpoint behavior. Explicit idle/close checkpoints
remain Fireweed operations. Unsupported custom `wal_autocheckpoint` controls
are not production requirements.

## Current adapter result — 2026-10-09

The full stock-Turso adapter suite now reports **140 passed, 3 failed, 3 existing
ignored**. Strict all-target adapter clippy passes. Two retired checkpoint-limit
tests are replaced with explicit public checkpoint coverage: cached readback
across independent connections, pinned-reader isolation while PASSIVE checkpoint
runs, TRUNCATE with no remaining data frames, and fresh-handle reopen of all
602 rows, their full blob bytes, and the exact sum of their keys.

Remaining failures: partial-expression index overflow, fork-specific destination
write batching, and fork-specific freed-page WAL-image zeroing. Their subsequent
or separate semantic checks must remain qualified explicitly. No new ignored
cases or engine changes were introduced. The older matrix below records earlier
qualification states.

## Updated stock-engine qualification — 2026-10-09

Three adapter failures are repaired by testing the published engine's actual
PRAGMA behavior. `wal_autocheckpoint` has empty update/readback results in stock
0.8.2; tests no longer claim that Fireweed disables automatic checkpoints through
that retired fork control. Page-size and synchronous settings remain asserted.
Idle compaction now additionally verifies the full 65,536-byte body and one-row
count through a fresh handle after checkpointing.

The full `fireweed-turso --features local --no-fail-fast` run reports **138 passed,
5 failed, 3 existing ignored**. Strict adapter/all-target clippy passes. Remaining
failures are the partial-expression index overflow, checkpoint locality batching,
two custom checkpoint policy tests, and free-page WAL-image expectations. The
engine and application behavior have not been patched to emulate the fork.
The pre-existing PostgreSQL relational edits remain untouched.

## Verified and outstanding evidence

| Scope | Result |
| --- | --- |
| `cargo test -p fireweed --lib`, configured real loopback RustFS | 146 passed |
| `cargo clippy -p fireweed --all-features --all-targets -- -D warnings` | Passed before the additional reference-memory fixture; repeat for that change |
| `cargo test -p fireweed-turso --features local --no-fail-fast` | 135 passed, 8 failed, 3 ignored |
| Snorri adapter full shared conformance over actual RustFS/Turso | Passed; overdue-await model cases are explicitly not applicable |
| Snorri durable timer test ported to actual RustFS/Turso | Passed: before-due, firing, fresh-handle reopen, no duplicate continuation |
| Snorri scalar/batch effect-result test ported to actual RustFS/Turso | Passed; every original outcome and round-trip assertion retained |
| Snorri interface-only S3/Turso composition and shared conformance | Passed |
| Independent process-local Memory conformance | Preserved and passed; not production storage evidence |

The shared conformance harness's recovery probes retain a queue handle;
they do not by themselves prove an operating-system process restart. Fresh
handle reopen is tested by the durable timer test. A shipped-process restart
and the final Cayce delivery/Niflheim query still need qualification.

The eight failures remain visible, not ignored or reported as passes:

1. `records_v03121_read_uncommitted_keyword_disposition`: old custom checkpoint update/readback expectations.
2. `log_backed_idle_compact_materializes_without_autocheckpoint`: unsupported custom PRAGMA readback.
3. `new_and_existing_files_use_their_actual_page_size`: unsupported custom checkpoint readback.
4. `partial_index_updates_skip_keys_outside_the_predicate`: upstream integer overflow from an expression index outside its partial predicate.
5. `checkpoint_coalesces_reused_pages_and_preserves_latest_values`: fork-specific checkpoint batching expectations.
6. `checkpoint_limit_controls_backfill_and_preserves_reopen`: unsupported custom checkpoint controls.
7. `checkpoint_limit_is_connection_local_and_cached_readback_tracks_changes`: unsupported custom checkpoint controls.
8. `free_page_history_preserves_readers_rollback_and_reuse`: fork-specific free-page body/WAL-image zeroing expectation fails before subsequent reader/reuse assertions.

The subsequent semantic assertions in tests that fail early are not proven by
that run. Adapt retired fork-specific probes to upstream-supported behavior
while preserving their data/reopen assertions. Keep the expression-index
correctness issue explicit and qualify the managed schemas used by the actual
workflow. Do not restore an engine fork to satisfy these probes.

## Workflow transition boundary

Ordinary queue mutations retain asynchronous projection acknowledgments.
Authoritative workflow transition commits now wait only for their own captured
committed position to become covered before returning success. Their reported
commit capability is atomic; the ordinary backend durability classification
remains `EventualApply`. A paused-applier regression failed before this change
and passed after it. This is Fireweed orchestration, not a Turso modification.

The original no-live-send statement is superseded by Cayce's ten-email
qualification: `run_5` has ten confirmed real inbox deliveries, exact SQL
correlation in Niflheim, and independent RustFS readback of its 34 events.
This does not qualify the five remaining adapter failures above.
