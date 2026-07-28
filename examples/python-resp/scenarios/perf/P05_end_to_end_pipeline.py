"""INTENT: One rollup narrative — insert, update half, status, drain chunks.

QUEUE MANAGEMENT: End-to-end RESP worker path under load with timing rollup.
Uses PERF_N (default 1e6 for full runs; use PERF_N=10000 for smoke).

RESP_COMMANDS: XADD pipeline, XREADGROUP, XACK, XLEN, XINFO

ASSERTS: Each phase completes; prints a single summary table.

NOT_ON_RESP: Library finalize variants; universal throughput claims.
"""

from __future__ import annotations

import time

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "P05_end_to_end_pipeline"
TITLE = "End-to-end perf pipeline rollup"
TAGS = ("perf", "rollup", "e2e")


def run(ctx: ScenarioContext) -> ScenarioResult:
    # Prefer smaller default when env still at 1M but operator wants one-shot demo:
    # honor ctx.perf_n exactly.
    r = ctx.redis
    q = ctx.queue
    n = ctx.perf_n
    pipe_n = ctx.perf_pipeline
    want_claim = ctx.perf_claim_count
    half = n // 2
    base = ctx.now_ms() - n - 1
    phases: dict[str, dict] = {}

    # --- insert ---
    t0 = time.perf_counter()
    batch: list[R.WorkItem] = []
    for i in range(n):
        p = base + i
        batch.append(
            R.WorkItem(
                client_item_key=ctx.key(f"e{i}"),
                priority=p,
                not_before=p,
                payload="x",
            )
        )
        if len(batch) >= pipe_n:
            R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
            batch.clear()
    if batch:
        R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
    wall = time.perf_counter() - t0
    phases["insert"] = {"n": n, "wall_s": wall, "ops_per_s": n / wall if wall else 0.0}
    ctx.log(f"INSERT n={n} wall_s={wall:.3f} ops_per_s={phases['insert']['ops_per_s']:.1f}")

    # --- update half ---
    t0 = time.perf_counter()
    batch = []
    new_base = ctx.now_ms() - half - 1
    for i in range(half):
        p = new_base + i
        batch.append(
            R.WorkItem(
                client_item_key=ctx.key(f"e{i}"),
                priority=p,
                not_before=p,
                payload="u",
            )
        )
        if len(batch) >= pipe_n:
            R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
            batch.clear()
    if batch:
        R.pipeline_xadd(r, q, batch, batch_size=pipe_n)
    wall = time.perf_counter() - t0
    phases["update_half"] = {
        "n": half,
        "wall_s": wall,
        "ops_per_s": half / wall if wall else 0.0,
    }
    ctx.log(
        f"UPDATE_HALF n={half} wall_s={wall:.3f} ops_per_s={phases['update_half']['ops_per_s']:.1f}"
    )

    # --- status ---
    samples = []
    for _ in range(20):
        t0 = time.perf_counter()
        depth = int(r.xlen(q))
        samples.append((time.perf_counter() - t0) * 1000.0)
    phases["status"] = {
        "xlen": depth,
        "xlen_p50_ms": R.percentile(samples, 50),
        "xlen_p95_ms": R.percentile(samples, 95),
    }
    ctx.log(
        f"STATUS xlen={depth} xlen_p50_ms={phases['status']['xlen_p50_ms']:.3f} "
        f"xlen_p95_ms={phases['status']['xlen_p95_ms']:.3f}"
    )

    # --- drain in chunks ---
    t0 = time.perf_counter()
    processed = 0
    chunks = 0
    max_chunk = 0
    empty = 0
    while processed < n and empty < 8:
        batch_c = R.claim_batch(
            r, q, group=ctx.group, consumer=ctx.consumer, count=want_claim
        )
        if not batch_c:
            empty += 1
            continue
        empty = 0
        ids = [i for i, _ in batch_c]
        max_chunk = max(max_chunk, len(ids))
        R.complete(r, q, *ids, group=ctx.group)
        processed += len(ids)
        chunks += 1
    wall = time.perf_counter() - t0
    phases["drain"] = {
        "processed": processed,
        "chunks": chunks,
        "claim_count_requested": want_claim,
        "claim_count_effective_max": max_chunk,
        "wall_s": wall,
        "ops_per_s": processed / wall if wall else 0.0,
    }
    ctx.log(
        f"CLAIM_ACK processed={processed} chunks={chunks} chunk_max={max_chunk} "
        f"requested={want_claim} wall_s={wall:.3f} ops_per_s={phases['drain']['ops_per_s']:.1f}"
    )

    ctx.log("ROLLUP " + " | ".join(f"{k}={v}" for k, v in phases.items()))
    ctx.check(processed > 0, "drain should process items")

    return ScenarioResult.ok(phases=phases, n=n)
