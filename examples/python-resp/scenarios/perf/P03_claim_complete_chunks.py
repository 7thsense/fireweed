"""INTENT: Drain due work in claim/complete chunks as fast as possible.

QUEUE MANAGEMENT: Workers pull COUNT chunks (request 1000; bootstrap may cap at
100) and XACK to complete. That is the RESP 'update state after processing' path.

RESP_COMMANDS: XREADGROUP, XACK, XLEN

ASSERTS: Processes items until empty or target; records effective chunk size.

NOT_ON_RESP: fail/retry finalize; claim filters.
"""

from __future__ import annotations

import time

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "P03_claim_complete_chunks"
TITLE = "Claim and complete in chunks"
TAGS = ("perf", "claim", "complete", "chunks")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    want = ctx.perf_claim_count
    # Bootstrap default max_claim_batch_size is 100 — probe effective size.
    probe = R.claim_batch(r, q, group=ctx.group, consumer=ctx.consumer, count=want)
    # Put probe items back into circulation by completing only if not ours? Completing is fine for drain.
    if probe:
        R.complete(r, q, *[i for i, _ in probe], group=ctx.group)

    # Seed a modest due set if queue is empty so the scenario is self-contained when run alone.
    depth = int(r.xlen(q))
    seed_n = 0
    if depth < 1000:
        seed_n = min(ctx.perf_n, 5000)
        base = ctx.now_ms() - seed_n - 1
        batch: list[R.WorkItem] = []
        for i in range(seed_n):
            p = base + i
            batch.append(
                R.WorkItem(
                    client_item_key=ctx.key(f"c{i}"),
                    priority=p,
                    not_before=p,
                    payload="d",
                )
            )
            if len(batch) >= ctx.perf_pipeline:
                R.pipeline_xadd(r, q, batch, batch_size=ctx.perf_pipeline)
                batch.clear()
        if batch:
            R.pipeline_xadd(r, q, batch, batch_size=ctx.perf_pipeline)
        ctx.log(f"seeded {seed_n} due items for drain")

    t0 = time.perf_counter()
    processed = 0
    chunks = 0
    chunk_sizes: list[int] = []
    claim_s = 0.0
    ack_s = 0.0
    empty_streak = 0
    # Drain until empty or we have processed a large amount (cap wall for safety).
    target = ctx.perf_n if seed_n == 0 else seed_n
    while processed < target and empty_streak < 5:
        tc0 = time.perf_counter()
        batch = R.claim_batch(
            r, q, group=ctx.group, consumer=ctx.consumer, count=want
        )
        claim_s += time.perf_counter() - tc0
        if not batch:
            empty_streak += 1
            continue
        empty_streak = 0
        ids = [iid for iid, _ in batch]
        chunk_sizes.append(len(ids))
        chunks += 1
        ta0 = time.perf_counter()
        n_ack = R.complete(r, q, *ids, group=ctx.group)
        ack_s += time.perf_counter() - ta0
        ctx.check(n_ack == len(ids), f"XACK {n_ack} != chunk {len(ids)}")
        processed += n_ack
        if chunks % 50 == 0:
            ctx.log(f"  progress processed={processed} chunks={chunks}")

    wall = time.perf_counter() - t0
    effective = max(chunk_sizes) if chunk_sizes else 0
    ctx.log(
        f"CLAIM_ACK processed={processed} chunks={chunks} "
        f"chunk_max={effective} claim_count_requested={want} "
        f"wall_s={wall:.3f} claim_s={claim_s:.3f} ack_s={ack_s:.3f} "
        f"ops_per_s={processed / wall if wall else 0:.1f}"
    )
    ctx.check(processed > 0, "expected to process at least one chunk")
    # Document bootstrap cap: often 100 when want=1000.
    return ScenarioResult.ok(
        processed=processed,
        chunks=chunks,
        claim_count_requested=want,
        claim_count_effective_max=effective,
        wall_s=wall,
        claim_s=claim_s,
        ack_s=ack_s,
        ops_per_s=processed / wall if wall else 0.0,
        xlen=int(r.xlen(q)),
    )
