# CAS-safe manifest compaction vs. the O(1) seal hot path — DESIGN ANALYSIS

Bead: `pqueue-8928baec` · Scope: `crates/pqueue-objectlog/src/segmented.rs` · HEAD: `v0.13.0`
Status: **DESIGN ONLY** (no code changed). All citations are `file:line` at `v0.13.0`.

---

## 0. The problem in one paragraph

`expire_segments_through` (segmented.rs:1830) reclaims the *segment objects* below the durable
retention floor but deliberately **keeps the manifest entries as tombstones** (segmented.rs:1826-1829):
tombstone accumulation is called out as risk R5 and deferred *precisely because* "compacting the
manifest prefix needs its own CAS-safe rewrite so it does not race the append-only manifest
invariant." This doc decides that rewrite. The tension is that the seal hot path is intentionally
O(1) and trusts an **in-memory** tail cache `(next_seq, next_manifest_index, committed_epoch)`
(segmented.rs:1146-1156), committing with a create-only CAS at the cached index
(`put_if_absent`, segmented.rs:1185) and **never listing the manifest before a seal**
(the load-bearing comment at segmented.rs:1144-1145). The **only** intrinsic fence against a
stale/superseded writer is the **index collision** on the write-once address space: a stale
writer's cached index is already occupied → CAS fails → it re-reads via `recover_manifest`
(segmented.rs:1188) → observes the newer epoch → self-fences `EpochFenced`. **Physically deleting a
below-floor index frees that address**, so a writer that stayed stale long enough could wake, WIN
`put_if_absent` at the freed index, and false-ack a phantom entry below the live epoch tail that
tail-only `recover_manifest` never replays. That is a lost-write / split-brain ack — unacceptable
for a durability substrate.

---

## 1. Q1 — Is writer-ownership staleness BOUNDED in wall-clock time?

### 1.1 The full writer-ownership / epoch lifecycle (traced)

**In-memory tail cache is the fence input.** Every seal reads its fence tuple from the in-memory
`ShardBuf` under the lock and compares the caller's `expected_epoch` against `buf.committed_epoch`
(segmented.rs:1146-1156); on a win it advances `buf.next_manifest_index = cur_index + 1`
(segmented.rs:1222-1223). Nothing on the win path re-reads the store.

**Where `committed_epoch` / `next_manifest_index` are refreshed** (the *only* places
`recover_manifest` — the sole tail re-read — is reached):
- `create_queue` on open (segmented.rs:926-935),
- `acquire_epoch` — writes a fence entry then sets `committed_epoch = new_epoch`,
  `next_manifest_index = next_index + 1` (segmented.rs:1012, 1031-1035),
- the **CAS-lost** branch inside `seal` (segmented.rs:1187-1194),
- `advance_retention_floor` (segmented.rs:1992, 2031-2034),
- branch creation (segmented.rs:1654-1662).

There is **no timer, TTL, heartbeat, or idle-expiry inside `segmented.rs`** that forces
`recover_manifest`. A `SegmentedObjectLog` that neither seals-and-loses, nor acquires, nor trims can
hold `(committed_epoch, next_manifest_index)` frozen indefinitely.

**Where the caller's `expected_epoch` comes from — two very different regimes:**

- **RESP + control-plane path (the intended production wiring).** `expected_epoch_for_write`
  (pqueue-resp/src/lib.rs:485, 652, 953, …) calls `OwnershipRuntime::ensure_epoch`
  (pqueue-server/src/lib.rs:938-1009) on **every write**. `ensure_epoch` does a
  `cp_resolve` against the control plane; if a *different* owner is active it returns
  `Err(Unavailable)` (lib.rs:974) and the write never reaches `seal`. The control-plane lease
  carries a TTL: `lease_ttl_ms` default **15_000**, `heartbeat_ttl_ms` default **5_000**
  (pqueue-engine/src/control_plane.rs:284-285). So a cooperative, CP-reachable owner is
  re-gated per write and cannot serve after a peer takeover propagates.

- **Raw `append` / compose path.** `ObjectLog::append` (compose_log.rs:136-166) forwards whatever
  `expected_epoch` the caller supplies straight into `enqueue`/`seal`. In `object_log_sqlite` the
  hot push uses `cached_epoch` — an in-memory mirror (object_log_sqlite.rs:751, 896-903,
  1283/1341/1386/…) refreshed **only** when *this* node calls `current_epoch`/`acquire_epoch`
  (object_log_sqlite.rs:1198-1199, 1240-1241, 1250-1252). There is **no per-write control-plane
  resolve on this path.**

**The ownership layer itself disclaims the guarantee.** `ownership.rs` states the real data-plane
ports historically "read the queue's CURRENT epoch internally … always-current, NEVER self-fences",
that threading the owner's cached `fence_epoch` through the real ports is the *deferred*
server-wiring follow-up **pqueue-c33c367e** (ownership.rs:13-22), and flags a KNOWN HAZARD of
two-counter non-atomicity where "a crash BETWEEN them can delay fencing" (ownership.rs:24-27).
`compose.rs:1293` likewise notes "the composed UoW lock is process-LOCAL and does not fence a peer
owner" — which is exactly *why* the manifest CAS, not the lock, is the fence.

### 1.2 Is the item `lease`/`lease_expires_at` relevant? No.

Every `lease_expires_at` in `pqueue-projection` / `pqueue-postgres` (e.g. projection lib.rs:76,
102, 2287-2308; relational.rs:271, 652) is the **consumer/item XCLAIM lease** — it governs when a
*claimed item* can be re-delivered, not when a *writer* must re-acquire. It does **not** bound
writer-cache staleness. The writer-relevant TTL is the control-plane `lease_ttl_ms` in §1.1, and it
lives in a layer above the substrate.

### 1.3 Can a superseded writer seal at its stale index arbitrarily later?

Walk the only dangerous shape. For writer A to false-ack at a *freed* index, at the moment its
`put_if_absent` executes A must simultaneously have (a) `expected_epoch == buf.committed_epoch`
(passes the fence, segmented.rs:1149) and (b) `buf.next_manifest_index` pointing at a **below-floor,
now-deleted** index.

- A single, consistent owner never satisfies (b): its `next_manifest_index` tracks the live tail —
  its own seals advance it (segmented.rs:1222-1223) and its own trim's `advance_retention_floor`
  advances it (segmented.rs:2033). The floor is always ≥ `retention_ms` behind the live tail, so a
  live owner's next index is at the tail, never below the floor.
- (b) is reachable **only** by a writer that froze `(committed_epoch=E, next_manifest_index=I)`,
  then stayed dormant while a *peer* B advanced the tail and the floor climbed past `I`, then woke
  and sealed **without** refreshing. By Q2 the floor is ≥ `retention_ms - skew` old, so A's freeze
  must be at least that old in wall-clock.
- In the **RESP+CP path** A's wake-up write must first pass `ensure_epoch`; after B's takeover
  `cp_resolve` returns B (→ `Unavailable`, lib.rs:974) or, if A is partitioned from the CP,
  `cp_resolve` errors and the write fails closed. Either way A never reaches `seal`. So *in that
  fully-wired path* staleness is effectively bounded (and in fact A is stopped at the CP gate rather
  than at the index-collision fence).
- But nothing in the **substrate** enforces that gate, the **raw/compose path** has no such gate,
  and even in the RESP path a GC pause **after** a successful resolve but **before** the in-flight
  `put_if_absent` can stretch arbitrarily — the resolve at time T does not re-run at seal time.

### 1.4 Q1 VERDICT

> **Effectively UNBOUNDED at the substrate layer.** `segmented.rs` contains no lease/TTL/heartbeat
> that forces a stale `(committed_epoch, next_manifest_index)` to be refreshed; the sole intrinsic
> fence is the `put_if_absent` index collision. A wall-clock bound *does* exist in the fully-wired
> RESP+control-plane deployment — a per-write `cp_resolve` gate, backed by `lease_ttl_ms`≈15 s /
> `heartbeat_ttl_ms`≈5 s (control_plane.rs:284-285) — but it lives in a layer the substrate is
> deliberately designed **not** to trust (compose.rs:1293), is documented as only partially wired
> and deferred to pqueue-c33c367e (ownership.rs:13-27), does not cover the raw `append`/compose path,
> and does not survive a GC pause between resolve and the in-flight CAS. **For a durability-substrate
> safety argument, treat writer staleness as UNBOUNDED.**

This verdict is what makes the "cheapest" design (D-A) invalid on its own and pushes the
recommendation toward a design that keeps the index address permanently occupied.

---

## 2. Q2 — Would `retention_ms - skew > W` make DELETE-ONLY safe?

**Floor age.** The floor advances to `trim_through = min(checkpoint_high_water,
max_trimmable_seq_before(cutoff))` with `cutoff = now - request_id_retention_ms -
RETENTION_TRIM_SKEW_MARGIN_MS` (compose.rs:1336-1343; `RETENTION_TRIM_SKEW_MARGIN_MS = 5_000`,
compose.rs:1039). `max_trimmable_seq_before` only returns a seq whose segment's
`committed_at_ms <= cutoff` (segmented.rs:2049-2059). So **any below-floor index is
≥ `request_id_retention_ms + 5_000 ms` old in wall-clock** (default `request_id_retention_ms =
3_600_000`, domain.rs:1409).

**The candidate invariant.** If (and only if) Q1 gave a hard bound `W` on writer-cache staleness,
then `request_id_retention_ms - RETENTION_TRIM_SKEW_MARGIN_MS > W` would mean any writer capable of
targeting a freed index is necessarily older than `W` and thus already forced to
re-acquire/self-fence before its CAS could land — making DELETE-ONLY safe.

**Why it is NOT sufficient as written:**
1. **Q1 supplies no substrate-level `W`.** The only `W` is the CP lease/resolve gate, which is not a
   substrate guarantee (see §1.4). Substituting `W = lease_ttl_ms` (15 s) into the inequality gives
   `3_595_000 > 15_000` — *numerically* true, but the inequality is only meaningful if that `W`
   actually bounds the in-flight-CAS case, which it does not (GC pause after resolve; raw path).
2. **It is not enforced anywhere.** Config validation checks `request_id_retention_ms != 0`
   (domain.rs:684) and `max_lease_duration_ms != 0` (domain.rs:702) — it does **not** relate
   `request_id_retention_ms` to `lease_ttl_ms`. A deployment with a short retention (the reclamation
   tests already use `request_id_retention_ms = 60_000`, objectlog_segment_reclamation_tests.rs:… /
   postgres_constructors.rs:49) narrows the margin toward the lease TTL.

**Q2 verdict.** For DELETE-ONLY to be safe you would need to (a) introduce and *prove* a hard
substrate-level staleness bound `W` (which today does not exist — a paused/partitioned/raw-path
writer has no `W`), and (b) add a **config-validation invariant**
`request_id_retention_ms - RETENTION_TRIM_SKEW_MARGIN_MS > W` enforced in
`QueueDefinition`/`ControlPlaneConfig` validation. Neither is implied by the current code. Absent
both, **DELETE-ONLY is unsafe.**

---

## 3. Design comparison

Reminder on cost accounting: **seal is a GROUP COMMIT** — one seal per *batch* of commands, gated by
`target_bytes` / `max_latency_ms` (enqueue size trigger segmented.rs:1074-1076; latency trigger
flush loop object_log_sqlite.rs:1071 / segmented.rs:1094-1104). A per-seal cost is **amortized over
the whole batch**, so we quantify per-seal, not per-command. Acceptance #1 is "bound per-read cost";
compaction must also bound manifest storage.

| Design | Correctness vs. stale-writer false-ack | Hot-path cost (per **seal**) | Bounds storage? | Bounds per-read? | Complexity / blast radius |
|---|---|---|---|---|---|
| **D-A** DELETE-ONLY below floor | **UNSAFE** unless a proven substrate `W` + enforced `retention-skew > W` invariant exists (§2). Frees the index address → resurrects the false-ack. | Zero change (best) | Yes (fully) | Yes (fully) | Trivial code, but unbounded correctness risk. **Rejected.** |
| **D-B** TAIL-VALIDATE on CAS win | **Safe.** After the winning `put_if_absent`, re-read tail `list().max()`; if a higher index exists, the winner was stale → roll back (delete just-written entry + segment) → `EpochFenced`. Restores a fence that does not depend on the address staying occupied. | **+1 LIST + 1 conditional DELETE** per seal (post-compaction manifest is small → LIST is cheap; amortized over the batch). | Yes (can DELETE) | Yes (can DELETE) | Medium: 3 write paths change (`seal`, `acquire_epoch`, `advance_retention_floor`) + rollback + a same-epoch-concurrent-writer subtlety. |
| **D-C** MARK-DEAD markers | **Safe, structurally.** Compaction overwrites a below-floor entry with a tiny marker **at the same key** so the index stays OCCUPIED → `put_if_absent` still collides → the existing self-fence is untouched. **Zero hot-path change.** | Zero change | Yes (marker ≪ tombstone JSON, but 1 object/index remains) | **Partial** — see §3.3 | Low-medium: a marker entry variant + reader/recovery skip; **no** change to `seal`/`acquire`. |
| **D-D** GENERATION rewrite | Safe only if it *also* adds a hot-path current-generation check (else a stale writer false-acks into the old gen) → inherits D-B-class cost, **plus** an addressing/recovery/branch-copy rewrite. | ≥ D-B (gen check) | Yes (fully) | Yes (fully) | High. Dominated by D-B. |
| **D-E** Durable compacted-through watermark (see §3.5) | Safe *as a complement*, not alone. | +0 hot path (read at recover only) | — | — | Low; a helper, folded into the recommendation. |

### 3.3 D-C honest read-cost analysis (does it meet acceptance #1?)

`read_manifest` / `read_from` LIST the manifest prefix then GET each entry (segmented.rs:975-985,
978-979; read path 1327-1341). With MARK-DEAD, `read_manifest` can recognise a dead-marker key by
suffix/flag and **skip its GET**. Per-read then drops from **O(all indices) GETs** to
**O(all indices) LIST + O(live) GETs**. That removes the dominant cost (the per-tombstone GET and
parse) and is a large real win, **but the LIST itself still grows with total history** — on S3 the
manifest LIST pages at 1000 keys/request (segmented.rs:2452-2456), so a queue with millions of
lifetime seals still pays an unbounded, ever-growing LIST even though every GET is skipped. So D-C
**bounds GET/parse cost but not LIST cost** → it does **not fully** satisfy "bound per-read cost" on
its own for very-long-lived trimmed queues. It is a strict improvement and is CAS-safe with zero
hot-path change, but the LIST floor is its ceiling.

### 3.4 D-B rollback detail

On the winning CAS, before acking, re-read the authoritative tail. If `tail_max_index > cur_index`
the winner extended a *stale* tail (a peer had already written a higher index — only possible if
this writer's cached `next_manifest_index` was behind, i.e. it was stale). Roll back in the
crash-safe reverse of the write order: DELETE the just-written `manifest/{cur_index}.json`, then
DELETE the just-written `seg/{first_seq}.seg` (both are addressable and idempotent), refresh the
in-memory tail via `recover_manifest`, and return `EpochFenced`. The three write paths that create a
manifest entry and therefore need the check: `seal` (segmented.rs:1185), `acquire_epoch`
(segmented.rs:1027), `advance_retention_floor` (segmented.rs:2020). Subtlety: a *same-epoch*
concurrent legitimate writer would also trip the tail check; that case must map to the existing
`Conflict` (transient retry, segmented.rs:1196-1198), **not** `EpochFenced` — the discriminator is
whether the observed tail epoch exceeds `expected_epoch`, exactly as the current CAS-lost branch
already decides (segmented.rs:1193-1198).

### 3.5 D-E — durable compacted-through watermark (complement)

Record a durable monotonic `compacted_through_index` (a tiny CAS-guarded marker at a fixed key, or a
field folded into the floor-advance manifest entry). Recovery/readers treat indices `<
compacted_through_index` as intentionally-absent (not "missing/torn"). This does **not** by itself
stop a stale-writer false-ack (a freed address is still writable), so it is not a standalone design;
its value is (a) letting readers **lower-bound** the LIST scan start (helps D-C's residual LIST
cost) and (b) giving recovery an unambiguous "these indices were reclaimed on purpose" signal.

---

## 4. RECOMMENDATION

**Adopt D-C (MARK-DEAD, index stays occupied) as the base, hardened with D-E (durable
`compacted_through_index` watermark), and hold the option to layer D-B's tail-validate only if/when
true index DELETION becomes necessary.**

### 4.1 Why

Given the Q1 verdict (writer staleness is **unbounded** at the substrate layer), the safest and
cheapest correct move is the one that **never frees a write-once address**: MARK-DEAD keeps every
below-floor index OCCUPIED, so the sole intrinsic fence — the `put_if_absent` index collision
(segmented.rs:1185-1198) — is preserved **byte-for-byte with zero hot-path change**. The seal path
keeps its O(1), no-LIST-before-seal property (segmented.rs:1144-1145) intact; correctness does not
depend on any lease, TTL, or the deferred pqueue-c33c367e wiring. This directly respects the R5
constraint that the manifest rewrite "must not race the append-only manifest invariant"
(segmented.rs:1826-1829): a same-key overwrite of a *below-floor, already-superseded* entry cannot
race a live seal (live seals only ever target the *tail*, never a below-floor index), and the
overwrite target is chosen from the durable floor, itself set by an epoch-fenced CAS
(advance_retention_floor, segmented.rs:1985-2036).

The D-E watermark closes D-C's honest gap (§3.3): readers/recovery start their manifest scan at
`compacted_through_index` instead of index 0, so the residual LIST cost is bounded by *live* history
rather than *total* history — which is what "bound per-read cost" actually requires. Marker bytes
(≪ the data-tombstone JSON) bound manifest **storage** to O(live entries) + O(dead markers), and a
follow-up can shrink even the markers by range-coalescing them once D-E's watermark makes a
contiguous dead prefix self-describing.

D-B is the correct answer **iff** the product later needs the manifest object *count* itself bounded
(true index deletion). Its cost is real but amortized: +1 LIST + a conditional DELETE per *seal*
(i.e. per batch, not per command), and the post-compaction manifest is small so the LIST is cheap.
It is strictly more invasive (3 write paths + rollback + the same-epoch discriminator) and should be
deferred until the marker-count actually bites. D-D is dominated by D-B (it needs the same hot-path
guard *plus* an addressing/recovery/branch rewrite) and is not recommended.

### 4.2 Correctness argument for the recommendation (written out)

Claim: with MARK-DEAD + watermark, no stale/superseded writer can false-ack, and no live read
returns a torn/missing entry.

1. **Fence preserved.** Compaction only ever *overwrites in place* (same key) or, in a later
   coalescing step, leaves a contiguous dead prefix; it **never deletes a manifest index address**.
   Therefore for every historical index, `put_if_absent(manifest/{i})` still collides
   (segmented.rs:150-153 InMemory CAS; 291-305 LocalFs `O_EXCL`; 2409 S3). A stale writer with
   cached `next_manifest_index = I` that seals: if `I` is at/below the live tail its CAS collides →
   CAS-lost branch → `recover_manifest` → observes `epoch > expected` → `EpochFenced`
   (segmented.rs:1187-1194). The unbounded-staleness hazard of §1 is neutralised structurally
   because its precondition — a *freed* address — never occurs.
2. **Compaction is epoch-safe.** The target ceiling is the durable retention floor, advanced only by
   the epoch-fenced manifest CAS (advance_retention_floor, segmented.rs:1985-2036;
   compose.rs:1358-1368 treats a fenced/raced advance as a benign skip). A superseded owner cannot
   move the floor, hence cannot authorise a marker below a live index.
3. **Reads stay coherent.** `recover_manifest` derives the tail from `list().max()`
   (segmented.rs:955) — overwriting or coalescing *low* indices never changes the max, so tail
   recovery is invariant under compaction regardless of LIST snapshot semantics. `read_manifest`
   skips dead markers by flag and GETs only live segment keys; the watermark lower-bounds the scan.
   A marker names no segment (`segment_key = None`) so the existing `entry.fence ||
   segment_key.is_none()` skips (segmented.rs:1272-1275, 1336-1338, 2056) already handle it — the
   marker is read-transparent by construction.
4. **Recovery next-seq unaffected.** A marker, like a fence/floor entry, carries the live next-seq in
   `first_seq` so `recover_manifest`'s `tail.fence || retention_floor_through.is_some()` next-seq rule
   (segmented.rs:966-969) stays exact; markers are only ever written *below* the tail, so they never
   become the tail whose `first_seq` is consulted.

---

## 5. Residual risks & secondary-constraint handling

### 5.1 (i) BlobStore LIST-consistency contract

What compaction + reads require:
- **Recovery tail (`recover_manifest`, `list().max()`, segmented.rs:955)** needs only that a
  *newly-PUT tail index becomes visible before a writer relies on not seeing it*. It is robust to a
  stale LIST that still shows *deleted/low* keys (max is unaffected) — which is exactly why the
  recommendation avoids deleting the tail.
- **Full reads (`read_manifest`/`read_from`, segmented.rs:975-985, 1327)** need
  list-then-get to not error on a listed-but-absent key. This already holds: a GET of a
  listed-but-gone manifest key returns `None` and is silently skipped (segmented.rs:979); only a
  missing *segment* GET errors (segmented.rs:1279, 1341), and compaction never deletes a live
  segment.

Per impl:
- **InMemoryBlobStore** — `list` is a locked snapshot of the map (segmented.rs:177-186): strongly
  consistent. ✅
- **LocalFsBlobStore** — `list` is a non-snapshot `read_dir` walk (segmented.rs:247-277, 323): it can
  interleave with a concurrent `put`(rename)/`delete`, so a single walk is *not* a point-in-time
  snapshot. Adequate for the recommendation because (a) tail recovery uses `max` and compaction never
  touches the tail, and (b) a transiently-missing low key only causes a skipped GET, never a torn
  read. It would **not** be adequate for a design that deleted tail-adjacent indices. ✅ for D-C.
- **S3BlobStore** — `list_with_request_count` pages `ListObjectsV2` to completion
  (segmented.rs:2452-2489). Modern S3 is strongly read-after-write and strongly list-consistent, so a
  freshly-PUT tail index is visible; the >1000-key paging fix (segmented.rs:2452-2456) prevents a
  truncated stale tail. Eventual-consistency-only S3-compatible stores would violate the tail-visible
  requirement for *any* design that relies on LIST to observe recent writes — but the recommendation
  relies on **`put_if_absent` (create-only CAS)**, not LIST, to observe a concurrent tail write, so
  it degrades safely: a stale LIST at worst causes an extra `Conflict`/`EpochFenced` retry, never a
  false-ack. ✅ with the caveat documented.

**Required contract to document alongside compaction:** *the manifest LIST must be
read-after-write consistent for newly created tail indices (so `recover_manifest` never derives a
tail below a durably-committed index); it need NOT be snapshot-consistent for deletions/overwrites of
below-floor indices.* InMemory and modern S3 satisfy the first clause; LocalFs satisfies it for the
single-writer-per-shard invariant the substrate already assumes.

### 5.2 (ii) Empty-seed-tail branch edge

Branch creation seeds a floor entry with `retention_floor_through = Some(f)` and `first_seq = f`
(segmented.rs:1585-1597), then `recover_manifest` derives the branch's next-seq as `first_seq = f`
for a floor/fence tail (segmented.rs:966-969). So when the source floor `f` is the last record, the
branch's **first append acks at seq `f`** — a value a naïve "hide everything at/below the floor"
read filter would wrongly suppress. The chosen design must therefore keep **branch seed floors** and
**reclamation floors** distinct: the retention floor is an *exclusive* lower bound for
*reclamation/recovery-resume* (segmented.rs:1946-1947, "resume at `floor + 1`"), but it is **not** a
read-visibility filter. The recommendation honours this automatically: MARK-DEAD/watermark only
governs which *manifest indices* are compacted/skipped, never which *sequences* are visible — read
visibility remains driven by `visible_last_seq` per entry (segmented.rs:939-940, 1276, 1332), which
correctly admits a branch's seq-`f` first append. **Do not** implement compaction as a
`seq <= floor` read filter; implement it as an index-level marker/skip keyed on
`retention_floor_through`/marker flags, leaving the branch-seed next-seq derivation
(segmented.rs:966-969) untouched.

### 5.3 Other residual risks

- **Marker LIST floor (D-C).** Per §3.3 the LIST still grows with lifetime seals; the D-E watermark
  mitigates by lower-bounding the scan, and a follow-up range-coalescing of contiguous dead markers
  can drop the LIST cost toward O(live). Track as the remaining piece of acceptance #1 if very-long-
  lived queues need a hard bound.
- **If DELETE is ever required (D-B).** Re-open this doc: the tail-validate guard (+1 LIST +
  conditional DELETE per seal) is the price, and the same-epoch-vs-fenced discriminator
  (segmented.rs:1193-1198) must be reused verbatim so a legitimate same-epoch race stays a retryable
  `Conflict`.
- **Coupling to pqueue-c33c367e.** The recommendation explicitly does **not** rely on the deferred
  data-plane fence wiring; the permanent head CAS remains the stale-writer fence, and the read-horizon
  watermark is only a read-cost helper, not an ownership fence. If a future design *does* choose to lean
  on the lease bound (D-A), it must first land the §2 config invariant AND a proven substrate `W`, and
  should be gated on pqueue-c33c367e closing.

---

## 6. CODEX STRESS-TEST CORRECTIONS + REVISED RECOMMENDATION (post-review)

An adversarial codex pass against the code corrected three load-bearing claims:

- **C1 (unbounded staleness): CONFIRMED.** (Minor: `ensure_shard` also reaches `recover_manifest` at segmented.rs:2176; does not change the verdict.)
- **C2/C3 (read-cost mechanism): the doc was WRONG that D-E "lower-bounds the LIST scan."** `BlobStore::list` (segmented.rs:71) returns only key strings and has **no `start_after`/range parameter**; S3 sends only prefix + continuation (segmented.rs:2451-2489). So a watermark can only filter *after* enumerating every key, and a same-key mark-dead overwrite cannot be recognized from LIST alone (read_manifest must GET each key, 975-983). ⇒ D-C+D-E bounds GET/parse and shrinks per-entry bytes, but **object count, object metadata, and LIST enumeration still grow with TOTAL history.** Mark-dead is only a payload-byte shrink.
- **C4 (D-B tail-validate): REFUTED — unsafe as written.** `tail_max_index > cur_index` does not prove the higher tail existed when this writer won its CAS: a legitimate writer W wins index `I`; a peer's `recover_manifest` (953) sees `I` as tail, derives and commits `I+1` (e.g. via acquire_epoch); W's post-win LIST then misclassifies its **valid** entry `I` as stale and deletes it — corrupting a committed entry a successor built on. Plus: segment keys are deterministic from `first_seq` and written unconditionally before the CAS (1163), so rollback-deleting "its" segment can delete a **branch-pinned shared source object** (1844/1631); and the entry is durable+reader-visible immediately after put_if_absent (1201), so "delete manifest then segment" is **not crash-safe**. True physical deletion needs a redesigned atomic fencing/commit protocol (a permanent linearizable head object with conditional update/CAS + unique per-attempt segment keys) — a new store primitive, not a small amendment.

### The design the original doc missed: RANGE-LIST + watermark (addresses kept)

codex noted S3 LIST natively supports `StartAfter` — the `BlobStore` trait simply doesn't expose it. Adding `list_from(prefix, start_after_key)` (S3-native; trivial filter for InMemory/LocalFs) unlocks a design that **fully bounds per-read cost with ZERO hot-path change and provable safety**:

- Keep every below-floor manifest index **OCCUPIED** (never freed) ⇒ the `put_if_absent` index-collision fence is intact byte-for-byte ⇒ the O(1) seal (no list-before-seal, 1144-1145) is untouched ⇒ **no stale-writer false-ack is even possible** (correctness never depends on the lease/TTL or the deferred pqueue-c33c367e wiring).
- Maintain a durable monotonic **`compacted_through_index` watermark**, advanced by compaction only up to the highest index fully below the **epoch-fenced retention floor** (so W < lowest LIVE index always; a stale/low W only costs a few extra enumerated keys, never skips live data; W can never exceed the floor so it can never hide a live entry).
- Treat `read_horizon.json` as a cache and the append-only `manifest_head/*~watermark.json` marker history as the durable source of truth, so restart/reload reconstructs the highest contiguous reclaimed index even if a stale writer races a lower blob write.
- `recover_manifest` and `read_manifest` call `list_from(manifest/, {W})` ⇒ enumerate **only indices > W** ⇒ **O(live) LIST + O(live) GET**. Recovery tail is still `max` of the ranged list (the tail is always > floor > W, so it is never skipped).
- Optional **mark-dead** overwrite of below-W entries shrinks per-entry storage BYTES (not count).
- Owner-fence evaluation for `pqueue-c33c367e`: the current index-CAS protocol still must keep below-floor manifest addresses occupied, so it cannot support delete-only compaction safely. The permanent head stays the stale-writer fence; the watermark never becomes the ownership fence. Any cheaper delete-only variant is therefore gated on the post-head-CAS redesign above, not on the current protocol.

**Residual:** object COUNT still grows (addresses are never freed — the price of the write-once fence), but as tiny, never-read, never-listed markers ⇒ a slow, modest storage cost. Fully bounding object COUNT requires the redesigned fencing/commit protocol above (Option 2), a much larger change.

### Revised options for the decision
1. **RANGE-LIST + watermark (+ optional mark-dead) — RECOMMENDED.** Provably safe (fence never weakened), zero seal-path change, **fully bounds per-READ cost to live history** (the bead's primary operational pain: recovery/reads LISTing the whole manifest). Adds `list_from` to BlobStore (3 impls) + a durable watermark + read-path changes. Leaves slow tiny-marker storage-count growth.
2. **Redesigned fencing/commit protocol** (permanent head object + CAS + unique per-attempt segment keys) to allow true physical deletion ⇒ bounds read AND storage-count fully. High blast radius (core commit + recovery + branch rewrite + a new store primitive); another multi-round hardening effort. Only warranted if tiny-marker storage growth is unacceptable.
3. **Minimal (original D-C+D-E only)** — bounds GET/parse + shrinks bytes but NOT LIST/count (codex C2/C3). Dominated by Option 1; not worth it alone.
