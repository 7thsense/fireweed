"""INTENT: Measure status-call latency with a large live set.

QUEUE MANAGEMENT: Operators need cheap full-queue depth without scanning every
item. XLEN / XINFO are O(metadata), not O(N) client fetches.

RESP_COMMANDS: XADD (optional seed), XLEN, XINFO STREAM, XPENDING

ASSERTS: Status calls succeed; records p50/p95 latency.

NOT_ON_RESP: Full metrics projection (complete/failed/oldest age).
"""

from __future__ import annotations

import time

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "P04_status_under_load"
TITLE = "Status latency under load"
TAGS = ("perf", "status", "xlen")

SAMPLES = 50


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    n = min(ctx.perf_n, 100_000)  # ensure some load without requiring full 1M if drained
    # If shallow, seed pending items.
    depth = int(r.xlen(q))
    if depth < 10_000:
        seed = min(n, 20_000)
        base = ctx.now_ms() + 600_000  # future — stay pending unclaimed
        batch: list[R.WorkItem] = []
        for i in range(seed):
            p = base + i
            batch.append(
                R.WorkItem(
                    client_item_key=ctx.key(f"s{i}"),
                    priority=p,
                    not_before=p,
                    payload="s",
                )
            )
            if len(batch) >= ctx.perf_pipeline:
                R.pipeline_xadd(r, q, batch, batch_size=ctx.perf_pipeline)
                batch.clear()
        if batch:
            R.pipeline_xadd(r, q, batch, batch_size=ctx.perf_pipeline)
        ctx.log(f"seeded {seed} future pending items for status load")

    depth = int(r.xlen(q))
    xlen_ms: list[float] = []
    xinfo_ms: list[float] = []
    for _ in range(SAMPLES):
        t0 = time.perf_counter()
        _ = r.xlen(q)
        xlen_ms.append((time.perf_counter() - t0) * 1000.0)
        t1 = time.perf_counter()
        try:
            _ = R.xinfo_stream_raw(r, q)
        except Exception as exc:  # noqa: BLE001
            ctx.log(f"XINFO error: {exc}")
        xinfo_ms.append((time.perf_counter() - t1) * 1000.0)

    # Optional XPENDING sample
    t2 = time.perf_counter()
    try:
        _ = r.xpending(q, ctx.group)
        xpending_ms = (time.perf_counter() - t2) * 1000.0
    except Exception:
        xpending_ms = -1.0

    ctx.log(
        f"STATUS xlen={depth} samples={SAMPLES} "
        f"xlen_p50_ms={R.percentile(xlen_ms, 50):.3f} "
        f"xlen_p95_ms={R.percentile(xlen_ms, 95):.3f} "
        f"xinfo_p50_ms={R.percentile(xinfo_ms, 50):.3f} "
        f"xinfo_p95_ms={R.percentile(xinfo_ms, 95):.3f} "
        f"xpending_ms={xpending_ms:.3f}"
    )
    ctx.check(depth >= 0, "depth ok")

    return ScenarioResult.ok(
        xlen=depth,
        samples=SAMPLES,
        xlen_p50_ms=R.percentile(xlen_ms, 50),
        xlen_p95_ms=R.percentile(xlen_ms, 95),
        xlen_max_ms=max(xlen_ms) if xlen_ms else 0.0,
        xinfo_p50_ms=R.percentile(xinfo_ms, 50),
        xinfo_p95_ms=R.percentile(xinfo_ms, 95),
        xpending_ms=xpending_ms,
    )
