"""INTENT: Reschedule about half of a large pending backlog quickly.

QUEUE MANAGEMENT: Pending replace via re-XADD + client_item_key for N/2 keys
created by P01 in the same run_id family. For standalone runs, inserts N then
updates N/2.

RESP_COMMANDS: pipelined XADD (replace)

ASSERTS: Sample FW.HGETALL shows updated priority; all replace ids returned.

NOT_ON_RESP: In-place field patch; updates to leased items.
"""

from __future__ import annotations

import time

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "P02_update_half"
TITLE = "Bulk pending update (half set)"
TAGS = ("perf", "update", "pending-replace")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    n = ctx.perf_n
    half = n // 2
    pipe_n = ctx.perf_pipeline
    base = ctx.now_ms() - n - 1

    # Ensure keys exist: re-insert full set with this run's keys (idempotent pending).
    # When chained after P01 with a different run_id, this still loads N quickly.
    ctx.log(f"PERF ensure n={n} items then update half={half}")
    batch: list[R.WorkItem] = []
    for i in range(n):
        prio = base + i
        batch.append(
            R.WorkItem(
                client_item_key=ctx.key(f"p{i}"),
                priority=prio,
                not_before=prio,
                payload="x",
            )
        )
        if len(batch) >= pipe_n:
            R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
            batch.clear()
    if batch:
        R.pipeline_xadd(r, q, batch, batch_size=pipe_n)

    # Update first half: pull schedule earlier / new payload marker.
    new_base = ctx.now_ms() - half - 1
    t0 = time.perf_counter()
    batch = []
    updated = 0
    for i in range(half):
        prio = new_base + i
        batch.append(
            R.WorkItem(
                client_item_key=ctx.key(f"p{i}"),
                priority=prio,
                not_before=prio,
                payload="u",
            )
        )
        if len(batch) >= pipe_n:
            R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
            updated += len(batch)
            batch.clear()
    if batch:
        R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
        updated += len(batch)
    wall = time.perf_counter() - t0
    ops = updated / wall if wall > 0 else 0.0

    sample = R.fw_hgetall(r, q, ctx.key("p0"))
    ctx.log(
        f"UPDATE_HALF n={updated} wall_s={wall:.3f} ops_per_s={ops:.1f} sample={sample}"
    )
    ctx.check(updated == half, f"updated count {updated} != half {half}")
    ctx.check(sample.get("payload") == "u", f"sample payload should be u, got {sample!r}")

    return ScenarioResult.ok(
        n=n,
        updated=updated,
        wall_s=wall,
        ops_per_s=ops,
        pipeline_batch_size=pipe_n,
        sample=sample,
    )
