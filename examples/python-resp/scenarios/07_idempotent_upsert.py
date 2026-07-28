"""INTENT: Producer retries converge on client_item_key while pending.

QUEUE MANAGEMENT: Re-sending the same logical work id should replace pending
state, not create unbounded duplicates.

RESP_COMMANDS: XADD, XADD (same key), FW.HGETALL

ASSERTS: Second XADD yields a new id; live payload is the latest version.

NOT_ON_RESP: request_id-based unknown-outcome replay on the stock Streams path.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "07_idempotent_upsert"
TITLE = "Idempotent pending upsert"
TAGS = ("queue", "idempotency", "client_item_key")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    key = ctx.key("stable-1")
    t = ctx.now_ms() - 100

    # Step 1 — First insert.
    id1 = R.xadd(
        r,
        q,
        R.WorkItem(
            client_item_key=key,
            priority=t,
            not_before=t,
            payload="first",
        ).fields(),
    )
    ctx.log(f"XADD first id={id1} key={key}")

    # Step 2 — Retry / upsert while still pending.
    id2 = R.xadd(
        r,
        q,
        R.WorkItem(
            client_item_key=key,
            priority=t + 1,
            not_before=t + 1,
            payload="second",
        ).fields(),
    )
    ctx.log(f"XADD second id={id2} key={key}")
    ctx.check(id1 != id2, "pending replace must mint a new item id")

    live = R.fw_hgetall(r, q, key)
    ctx.log(f"FW.HGETALL => {live}")
    ctx.check(live.get("payload") == "second", f"expected latest payload, got {live!r}")

    return ScenarioResult.ok(key=key, id1=id1, id2=id2, live=live)
