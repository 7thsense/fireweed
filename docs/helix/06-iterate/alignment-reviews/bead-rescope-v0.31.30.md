---
ddx:
  id: bead-rescope-v0.31.30
  depends_on:
    - prd
kind: alignment-review
---

# Bead rescope after v0.31.30

landed-path: R1

v0.31.31 is one public cell: S3 object-log × Turso × AsyncProjection (ADR-024).
Filesystem × Turso, memory, postgres, and SQLite are not public cells. `10000`
and `12500` are capacity observations for a named cell and commit, not product
pass bars. A miss of those script gates is not a product failure.

Beads below were open when `AR-2026-09-21-repo.md` was filed. R2 (restore the
12-cell amendment) is not the product.

| Bead | Disposition |
| --- | --- |
| `fireweed-07d62ef5` | Keep. Object-log lock order is still true on the landed cell. |
| `fireweed-0b5ce9f7` | Keep. Claim replay and Turso cohort expiry are in progress on the public cell. The fence-conflict remainder (`same_fence_second_commit_conflicts`, `other_handle_claim_empty_while_unapplied`) stays open and was not implemented here. |
| `fireweed-452e8744` | Keep. 10M recovery and 1000-queue density are still unmet on s3 × Turso. |
| `fireweed-4a8a249c` | Keep. Turso post-append poison is still true on the landed cell. |
| `fireweed-4befafcb` | Close when the 10000/12500 rule is cited by the campaign plan and TP-002/TP-005. Not a product pass bar. |
| `fireweed-4e6c5284` | Keep. Turso reader pools are still true on the landed cell. |
| `fireweed-5262d000` | Keep. Packed Complete apply is still true on the landed cell. |
| `fireweed-59772572` | Keep. Admission calibration is still true on the landed cell. |
| `fireweed-59eae996` | Keep. Recovery and capacity evidence on Turso is still real work. Throughput numbers in the acceptance are observations, not an already-qualified 10k-every-phase product gate. |
| `fireweed-5a97fbc4` | Retarget. The indexed-schedule fix stays. The `filesystem--turso` N=10000 settled command is not a public-cell pass bar. |
| `fireweed-5ecf18f2` | Keep. Packed apply and the Turso pipeline stay. The description's filesystem × Turso throughput target is not the public cell. |
| `fireweed-8467a171` | Keep. Bounded reclaim retry is still true on the landed cell. |
| `fireweed-8a1d1004` | Close when one FEAT file exists per PRD subsystem and user stories name FR-1 through FR-55. |
| `fireweed-90442d07` | This rescope. Close when this file names every open bead from the alignment, including `fireweed-a8cc571d`. |
| `fireweed-943b132b` | Keep as the alignment epic record. R1 landed. R2 is not implemented. |
| `fireweed-a38111ac` | Retarget. Operation-shaped BatchUpdate tests stay. The `SS_CELL=filesystem--turso` 10000 gate is not a public-cell pass bar. |
| `fireweed-a8cc571d` | Keep. Postgres committed-read coverage is a non-public helper, not a public projection and not SQLite. |
| `fireweed-ad7c06e3` | Keep. Claim-turn calibration is still true on the landed cell. |
| `fireweed-b0da087c` | Keep. Exact force-sealed packs are still true on the landed cell. |
| `fireweed-b479718a` | Retarget. 10000 settled items/s on every phase is not an already-qualified product gate, and `filesystem--turso` is not the public cell. |
| `fireweed-b9180a8b` | Keep. The bounded streaming lane is still true on the landed cell. |
| `fireweed-c26beaab` | Retarget the description. Post-position poison stays for the s3 × Turso object log. Memory, SQLite, and postgres are not public cells. |
| `fireweed-c40ac845` | Keep. Push/FIFO planning is still true on the landed cell. |
| `fireweed-d1dee3ca` | Keep. Committed reads and retained grouped results are still true on the landed cell. |
| `fireweed-d65de382` | Retarget. Coalesced apply stays. The S0 N=10k filesystem command is not a product pass bar. |
| `fireweed-dd77ca5b` | Keep. Log-first claim microbatches are still true on the landed cell. |
| `fireweed-e2cd5913` | Cancel. Acceptance is only `filesystem--turso` at 10000 on every phase, which is not a public-cell qualification gate. |
| `fireweed-e8e941de` | Keep. Turso coordinator shutdown is still true on the landed cell. |
| `fireweed-ec528b80` | Keep. Removing SQL-first claim serving is still true on the landed cell. |
| `fireweed-f3b13120` | Keep. Grouped and cohort retained claim results are still true on the landed cell. |
| `fireweed-f7ee0fa1` | Keep. Packed authority-first claim apply is still true on the landed cell. |
