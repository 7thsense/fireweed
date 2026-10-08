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

No live SES emails have been sent and no real SES data has yet been verified
queryable in Niflheim for the ten-recipient demonstration.
