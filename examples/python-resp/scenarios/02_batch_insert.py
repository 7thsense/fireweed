"""INTENT: Batch-insert work items with stable keys and schedule fields.

QUEUE MANAGEMENT: Producers enqueue many items early. Each item has a stable
client_item_key, an ordering key (priority), and optional eligibility (not_before).

RESP_COMMANDS: pipelined XADD

ASSERTS: N distinct item ids returned; live depth increases by N.

NOT_ON_RESP: Multi-item single command (batch = many XADDs); request_id replay.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "02_batch_insert"
TITLE = "Batch insert work items"
TAGS = ("queue", "batch-insert", "xadd")

N = 20


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    base = ctx.now_ms()

    before = int(r.xlen(q))
    ctx.log(f"XLEN before insert = {before}")

    # Step 1 — Build N work items.
    # priority / not_before use epoch millis; bootstrap queue is Int64 ascending.
    items = [
        R.WorkItem(
            client_item_key=ctx.key(f"ins-{i}"),
            priority=base + i,
            not_before=base + i,  # eligible immediately for later scenarios if past
            payload=f"body-{i}",
            group_key="job-demo",
            extra={"kind": "work"},
        )
        for i in range(N)
    ]

    # Step 2 — Pipeline XADD (transaction=False: Fireweed is not Redis MULTI/EXEC).
    ctx.log(f"pipeline XADD n={N} queue={q}")
    ids = R.pipeline_xadd(r, q, items, batch_size=10)
    ctx.check(len(ids) == N, f"expected {N} ids, got {len(ids)}")
    ctx.check(len(set(ids)) == N, "item ids must be distinct")
    for i, item_id in enumerate(ids):
        ctx.log(f"INSERT id={item_id} key={items[i].client_item_key} priority={items[i].priority}")

    # Step 3 — Live depth counts pending+leased (terminals excluded).
    after = int(r.xlen(q))
    ctx.log(f"XLEN after insert = {after}")
    ctx.check(after >= before + N, f"XLEN should grow by ~{N}: before={before} after={after}")

    return ScenarioResult.ok(n=N, ids=ids, xlen_before=before, xlen_after=after)
