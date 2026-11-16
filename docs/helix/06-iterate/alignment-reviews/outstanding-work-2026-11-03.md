---
ddx:
  id: outstanding-work-2026-11-03
  depends_on:
    - bead-rescope-v0.31.30
kind: alignment-review
---

# Outstanding work reconciliation, 2026-11-03

Source revision: `6adcf22cc94afa13f5556ee5e78bceef4887fa13`.
Tracker: `.ddx/beads.jsonl`, 1,254 records: 1,080 closed, 130 cancelled,
43 open, and one blocked. These are tracker states, not assertions that every
open description still matches implementation. The table below is a snapshot;
the tracker remains authoritative for subsequent work.

## Governing scope

The landed R1 decision and `bead-rescope-v0.31.30.md` govern older campaign
wording: the public cell is S3 object-log × Turso × AsyncProjection. Filesystem,
Postgres, memory, and SQLite are not additional public cells. Historical 10,000
and 12,500 items/s numbers are capacity observations, not product pass bars.
Keep the indexed schedule, packed apply, recovery, and bounded resource work;
qualify it against the governing cell and applicable contracts.

## Implementation reconciliation

- `fireweed-025173e0`: commit `67db7c7237763ee7ef939e5b7007cbd173e9b36a`
  added `wait_request_entry_coverage` before `claimed_view` renders leases.
  The shared `exercise_metrics_and_claimed` still immediately checks the leased
  item and bearer token without sleeps or retries. The implementation request
  has landed, but the bead stays open for its dedicated regression,
  parent-failure proof, and live S3 acceptance evidence.
- `fireweed-b0da087c`: commit `ca943e86c66dac57dc254cc4756e22db072a783f`
  removed the application packer. Engine linger controls physical sealing;
  each caller retains its own apply batch and reservation, with no follower
  transfer. The historical force-seal/follower language needs this
  interpretation. This change fixes the duplicate epoch read outside the
  pre-position deadline and strengthens deadline, debt, retry, poison, and
  independent publication tests. Append-tail capacity qualification remains
  open; local adapter tests do not establish public S3 performance.
- `fireweed-5f6ce037`: the requested SPIKE-002 file is still absent from main.
  Keep the external branch prerequisite and blocked state.
- `fireweed-e4e1b053`: `.github/workflows/turso.yml` still selects
  `all_four_log_specs_accept_turso`, whose function is absent. This remains
  actionable CI work.
- `fireweed-a53488bf`: `plan_batch_update_pipelined` and its public re-export
  remain in the engine. The cleanup remains actionable; retain replay support.

No item was closed solely because its implementation appeared in source.
Tracker notes preserve historical observations and identify outstanding proof.

## Execution order

1. Finish pack publication qualification (`fireweed-b0da087c`). This unlocks
   post-position poison (`fireweed-c26beaab`), coordinator shutdown
   (`fireweed-e8e941de`), and part of contiguous apply coalescing
   (`fireweed-d65de382`). Preserve the existing append-tail diagnosis.
2. In parallel, validate Push/FIFO planning (`fireweed-c40ac845`), grouped/cohort
   retained results (`fireweed-f3b13120`), BatchUpdate apply
   (`fireweed-a38111ac`), and the indexed schedule path (`fireweed-5a97fbc4`).
3. Complete admission calibration and committed-read activation, then Claim
   microbatch activation, Complete coalescing, SQL-first retirement, and final
   lifecycle recovery qualification along the recorded dependency graph.
4. Run the dedicated lease visibility proof before closing
   `fireweed-025173e0` or unblocking its dependent workspace test remediation.
5. Repair the zero-test CI selector and stale governance routes independently.
   Run RustFS/kind failover and large recovery/density proofs on provisioned
   hosts. Track upstream object-log release work separately from local fixes.

## Recorded work and unresolved declared prerequisites

An em dash means no unresolved declared blocking dependency. It is not a fresh
readiness review, and does not remove external service or evidence requirements.
Closed and cancelled dependencies are excluded from this table.

| Priority | Bead | State | Work | Unresolved prerequisites |
| --- | --- | --- | --- | --- |
| P0 | `fireweed-5ecf18f2` | open | Epic: Batch transactions, packed apply, and continuous Seventh Sense pipeline | — |
| P0 | `fireweed-b479718a` | open | Epic: 10k settled items/s on every Seventh Sense phase | — |
| P1 | `fireweed-025173e0` | open | Make claimed() show an acknowledged lease on the s3 × turso cell | — |
| P1 | `fireweed-4a8a249c` | open | Align Turso governed faults with post-position poison | `fireweed-c26beaab` |
| P1 | `fireweed-5262d000` | open | Coalesce Complete apply without mixed-vector regression | `fireweed-b0da087c`, `fireweed-f7ee0fa1`, `fireweed-dd77ca5b` |
| P1 | `fireweed-59772572` | open | Calibrate mutation and reader admissions before activation | `fireweed-c40ac845`, `fireweed-8467a171`, `fireweed-b0da087c`, `fireweed-d65de382` |
| P1 | `fireweed-59eae996` | open | Qualify final Turso lifecycle recovery, throughput, and memory | `fireweed-ec528b80` |
| P1 | `fireweed-5a97fbc4` | open | Eliminate the Turso indexed schedule rewrite cliff | — |
| P1 | `fireweed-8467a171` | open | Give callerless reclaim bounded retry and page isolation | `fireweed-c40ac845` |
| P1 | `fireweed-943b132b` | open | HELIX alignment: v0.31.30 spec and implementation gaps | — |
| P1 | `fireweed-a38111ac` | open | Specialize and profile Turso BatchUpdate apply | — |
| P1 | `fireweed-ad7c06e3` | open | Calibrate Claim turns, fences, work, and T2 cycle | `fireweed-f7ee0fa1` |
| P1 | `fireweed-b0da087c` | open | Make force-sealed packs exact and owned through publication | — |
| P1 | `fireweed-c26beaab` | open | Apply post-position poison to every async object-log product | `fireweed-b0da087c` |
| P1 | `fireweed-c40ac845` | open | Split Push reads and prepare FIFO mutation generation planning | — |
| P1 | `fireweed-c83b16e5` | open | Yank object-log 0.3.5 from crates.io | — |
| P1 | `fireweed-d1dee3ca` | open | Activate committed reads, retained grouped results, and exact coverage | `fireweed-f3b13120`, `fireweed-c40ac845`, `fireweed-8467a171`, `fireweed-59772572`, `fireweed-b0da087c`, `fireweed-c26beaab`, `fireweed-4a8a249c` |
| P1 | `fireweed-d65de382` | open | Coalesce underfilled contiguous Turso apply packs | `fireweed-b0da087c`, `fireweed-a38111ac` |
| P1 | `fireweed-dd77ca5b` | open | Activate log-first Claim microbatches and every fence disposition | `fireweed-b0da087c`, `fireweed-c26beaab`, `fireweed-4a8a249c`, `fireweed-c40ac845`, `fireweed-8467a171`, `fireweed-59772572`, `fireweed-d1dee3ca`, `fireweed-f3b13120`, `fireweed-ad7c06e3`, `fireweed-f7ee0fa1`, `fireweed-e8e941de` |
| P1 | `fireweed-e8e941de` | open | Drain owned Turso coordinator work on product shutdown | `fireweed-b0da087c` |
| P1 | `fireweed-ec528b80` | open | Remove obsolete SQL-first Claim serving path after cutover | `fireweed-dd77ca5b`, `fireweed-5262d000` |
| P1 | `fireweed-f3b13120` | open | Prepare grouped and cohort retained Claim results | — |
| P1 | `fireweed-f7ee0fa1` | open | Coalesce authority-first Claim apply into one transaction | `fireweed-d1dee3ca` |
| P2 | `fireweed-0554da2c` | open | Run the TP-002 E2 kind failover proof end to end on RustFS | — |
| P2 | `fireweed-0b5ce9f7` | open | Prove claim replay, cohort expiry, and fence conflict on the governing cell | — |
| P2 | `fireweed-0da0be5a` | open | Verify and hint the grouped-schedule BatchUpdate statements on Turso | — |
| P2 | `fireweed-1a658ec4` | open | Restore the functional-matrix and P2r governance checks after ADR-024 | — |
| P2 | `fireweed-44fddfc5` | open | Repair functional_matrix_route_sources.py for the one-cell storage authority | — |
| P2 | `fireweed-55773f01` | open | Stop two debug-build tests timing out only inside their full suites | — |
| P2 | `fireweed-69335fd4` | open | Retire or rewrite workspace tests that still target retired storage cells | `fireweed-025173e0` |
| P2 | `fireweed-76d6754f` | open | Make object-log BlobStore::compare_and_swap a required method so wrappers cannot drop CAS atomicity | — |
| P2 | `fireweed-a8cc571d` | open | Add postgres committed read-coverage parity | `fireweed-d1dee3ca` |
| P2 | `fireweed-ac58fc8c` | open | Retry transient S3 errors before an object-log position is assigned | — |
| P2 | `fireweed-b8484c6d` | open | Restore object-log CI on main and reconcile the fireweed-v03x release branches | — |
| P2 | `fireweed-e4e1b053` | open | Fix the turso.yml step that runs zero tests for all_four_log_specs_accept_turso | — |
| P3 | `fireweed-0ccf942c` | open | Remove references to retired storage helpers and test targets | — |
| P3 | `fireweed-452e8744` | open | Evidence for 10M recovery and 1000-queue density on the governing cell | — |
| P3 | `fireweed-486cbdf1` | open | Clean up log_engine_store test temp roots even when a test panics | — |
| P3 | `fireweed-5f6ce037` | blocked | Record the traced Turso exposure of the apply.rs client_item_key IN-list in SPIKE-002 | — |
| P3 | `fireweed-6b32e661` | open | Decide whether object-log's fence epoch moves into Sequencer::Meta in 0.4 | — |
| P3 | `fireweed-b8524aa6` | open | Stabilize load-sensitive timing tests on shared development hosts | — |
| P4 | `fireweed-355384aa` | open | Make clippy clean for fireweed built alone with default features | — |
| P4 | `fireweed-a53488bf` | open | Remove the unused plan_batch_update_pipelined BatchUpdate planner | — |
| P4 | `fireweed-d6bf0811` | open | Correct the stale object-log tag in the root Cargo.toml workspace comment | — |

## Validation record

Implementation commit: `e7d3a2d12d8fcff0644a028e078bdbe26622d71d`.

The complete `fireweed-objectlog` library binary passed **195 tests, zero failed,
zero ignored**, including real S3 cases. Reproduction uses
`FIREWEED_RUSTFS_DATA=/home/agent/.cache/fireweed-test-rustfs-data cargo test -p fireweed-objectlog --lib`.
The writable data override is needed on this host because the fixture's default
`/var/tmp` directory is read-only. The initial run without the override failed
in setup for 26 S3 tests; those tests passed after correcting the fixture path.

`bash scripts/ci/pr-gate.sh --mode bootstrap` exits 1 in the unchanged
`scripts/release/build-governed-evidence-bundle-test.sh` fixture: its
`fireweed-build-e3-contract` invocation omits required `--run-id`,
`--composition-fingerprint`, and `--authority-mode`. This is a gate failure,
not a successful bootstrap run. No release fixture was changed in this work.

`cargo clippy -p fireweed-objectlog --all-targets -- -D warnings`,
`cargo fmt --all --check`, and `git diff --check` passed.

Kind failover and campaign throughput remain separate acceptance evidence;
the passing S3 correctness tests do not establish either.

The delayed-epoch regression was run with the original eager epoch read restored:
it failed at `epoch lookup must not wait outside the pre-position budget`.
Restoring the fix made the same exact test pass. The source was restored byte
for byte after this comparison.

`python3 scripts/ci/inventory-storage-remediation.py --write --with-cargo`
failed while listing `fireweed` doctests. A direct rerun of
`cargo test --manifest-path Cargo.toml --locked -p fireweed --doc -- --list --format terse`
also exited 101. Full Cargo route enumeration is therefore incomplete.
The supported `--write` source refresh succeeded, preserving the prior harness
and rustdoc routes. The generated inventory delta is only its source digest;
all debt registries are unchanged.

`bash scripts/ci/storage-remediation-policy.sh --mode-file scripts/ci/storage-remediation-policy.mode`
exits 1: `closure blocked by 32 assigned debt rows`. These rows are unchanged
from the starting inventory. This work does not establish zero-debt closure,
bootstrap success, campaign throughput, or kind failover qualification.

The task remains open with those remaining requirements recorded. This review
and the three tracker note updates are committed separately from the code fix.
