"""INTENT: Bulk-insert a large backlog as fast as the local profile allows.

QUEUE MANAGEMENT: Load N work items (default 1_000_000) via pipelined XADD.
Host-bound evidence only — not a product SLA.

RESP_COMMANDS: pipelined XADD, XLEN

ASSERTS: All pipeline replies are ids; XLEN grows by about N for a fresh queue.

NOT_ON_RESP: Multi-item single XADD; durable profile cost models.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "P01_insert_1m"
TITLE = "Bulk insert (pipelined XADD)"
TAGS = ("perf", "insert", "pipeline")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    n = ctx.perf_n
    pipe_n = ctx.perf_pipeline
    base = ctx.now_ms() - n - 1  # all immediately eligible

    before = int(r.xlen(q))
    ctx.log(f"PERF insert n={n} pipeline={pipe_n} queue={q} xlen_before={before}")

    # Compact items: small payload, sequential keys, past not_before.
    def gen():
        for i in range(n):
            prio = base + i
            yield R.WorkItem(
                client_item_key=ctx.key(f"p{i}"),
                priority=prio,
                not_before=prio,
                payload="x",
            )

    # Stream generation in pipeline batches without holding 1M WorkItem objects if possible.
    ids: list[str] = []
    t0 = __import__("time").perf_counter()
    batch: list[R.WorkItem] = []
    for item in gen():
        batch.append(item)
        if len(batch) >= pipe_n:
            ids.extend(R.pipeline_xadd(r, q, batch, batch_size=pipe_n))
            batch.clear()
            if len(ids) % (pipe_n * 50) == 0:
                ctx.log(f"  progress inserted={len(ids)}")
    if batch:
        ids.extend(R.pipeline_xadd(r, q, batch, batch_size=pipe_n))
    wall = __import__("time").perf_counter() - t0

    ctx.check(len(ids) == n, f"expected {n} ids, got {len(ids)}")
    after = int(r.xlen(q))
    ops = n / wall if wall > 0 else 0.0
    ctx.log(
        f"INSERT n={n} pipeline={pipe_n} wall_s={wall:.3f} ops_per_s={ops:.1f} "
        f"xlen_before={before} xlen_after={after}"
    )
    ctx.check(after >= before + n, f"XLEN should rise by ~{n}: {before} -> {after}")

    # Stash path prefix for ordered perf suite consumers via details only.
    return ScenarioResult.ok(
        n=n,
        wall_s=wall,
        ops_per_s=ops,
        pipeline_batch_size=pipe_n,
        xlen_before=before,
        xlen_after=after,
        key_prefix=ctx.key("p"),
    )
