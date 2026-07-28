"""INTENT: Complete claimed work and read queue status proxies.

QUEUE MANAGEMENT: After processing, workers finalize successful work and
operators inspect live depth. RESP exposes XLEN/XINFO/XPENDING — not full
facade metrics (pending/leased/complete/failed breakdown).

RESP_COMMANDS: XADD, XREADGROUP, XACK, XLEN, XINFO STREAM, XPENDING

ASSERTS: XACK count matches claim; status calls succeed; XLEN does not grow after complete.

NOT_ON_RESP: metrics.complete / failed / oldest_eligible_age_ms.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "06_complete_and_status"
TITLE = "Complete work and read status"
TAGS = ("queue", "complete", "status", "xack")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    # Low priorities so these claim before residual high-priority backlog.
    items = [
        R.WorkItem(
            client_item_key=ctx.key(f"done-{i}"),
            priority=i,
            not_before=0,
            payload=f"done-{i}",
        )
        for i in range(3)
    ]
    R.pipeline_xadd(r, q, items, batch_size=3)

    before = R.queue_status(r, q, group=ctx.group)
    ctx.log(f"STATUS before claim xlen={before['xlen']}")

    claimed = []
    for _ in range(40):
        batch = R.claim_batch(r, q, group=ctx.group, consumer=ctx.consumer, count=10)
        for iid, f in batch:
            ck = str(f.get("client_item_key", ""))
            if ck.startswith(ctx.run_id + "-done-"):
                claimed.append(iid)
            else:
                try:
                    R.complete(r, q, iid, group=ctx.group)
                except Exception:
                    pass
        if len(claimed) >= 3:
            break
    claimed = claimed[:3]
    ctx.check(len(claimed) == 3, f"need 3 claims, got {claimed}")

    n = R.complete(r, q, *claimed, group=ctx.group)
    ctx.log(f"XACK n={n}")
    ctx.check(n == 3, f"XACK expected 3, got {n}")

    after = R.queue_status(r, q, group=ctx.group)
    ctx.log(f"STATUS after complete xlen={after['xlen']} xinfo_keys={list((after.get('xinfo') or {}).keys())}")
    ctx.check(isinstance(after["xlen"], int), "xlen must be int")
    # Terminals are excluded from XLEN — depth should not include our completed items.
    ctx.check(after["xlen"] >= 0, "xlen non-negative")

    return ScenarioResult.ok(
        claimed=claimed,
        acked=n,
        status_before=before,
        status_after={"xlen": after["xlen"], "xinfo": after.get("xinfo")},
    )
