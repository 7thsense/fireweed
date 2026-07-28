"""INTENT: Reclaim work after lease expiry (worker loss).

QUEUE MANAGEMENT: If a worker dies without completing, leases expire and another
worker may reclaim. Bootstrap max_lease_duration_ms is 60s — this scenario is
opt-in via --full.

RESP_COMMANDS: XADD, XREADGROUP, XAUTOCLAIM

ASSERTS: After waiting past lease expiry, XAUTOCLAIM returns the item (or skip
if timing cannot be guaranteed).

NOT_ON_RESP: Operator force-reassign with custom policies.
"""

from __future__ import annotations

import time

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "09_lease_reclaim"
TITLE = "Reclaim after lease expiry"
TAGS = ("queue", "lease", "reclaim", "xautoclaim")


def run(ctx: ScenarioContext) -> ScenarioResult:
    if not ctx.full:
        return ScenarioResult.skip("pass --full to wait for lease expiry (~60s)")

    r = ctx.redis
    q = ctx.queue
    t = ctx.now_ms() - 50
    key = ctx.key("reclaim-1")

    R.xadd(
        r,
        q,
        R.WorkItem(
            client_item_key=key,
            priority=t,
            not_before=t,
            payload="will-expire",
        ).fields(),
    )

    target = None
    for _ in range(30):
        batch = R.claim_batch(r, q, group=ctx.group, consumer="worker-crash", count=10)
        for iid, fields in batch:
            if str(fields.get("client_item_key", "")) == key:
                target = (iid, fields)
                break
        if target:
            break
    ctx.check(target is not None, "failed to claim reclaim item")
    item_id, fields = target
    expires = int(fields.get("lease_expires_at") or "0")
    wait_s = max(1.0, (expires - ctx.now_ms()) / 1000.0 + 1.5)
    ctx.log(f"CLAIM id={item_id}; sleeping {wait_s:.1f}s for lease expiry")
    time.sleep(wait_s)

    # XAUTOCLAIM ignores min-idle; gates on lease expiry.
    reply = R.xautoclaim(
        r, q, group=ctx.group, consumer="worker-recovery", count=20, start="0-0"
    )
    ctx.log(f"XAUTOCLAIM => {reply!r}")

    # Best-effort complete if we can find the id in the reply structure.
    recovered = False
    try:
        # redis-py / raw: [next_cursor, entries, deleted]
        entries = reply[1] if isinstance(reply, (list, tuple)) and len(reply) > 1 else []
        for ent in entries or []:
            if ent and ent[0] == item_id:
                recovered = True
                R.complete(r, q, item_id, group=ctx.group)
                break
    except Exception as exc:  # noqa: BLE001
        ctx.log(f"parse reclaim reply: {exc}")

    ctx.check(recovered, f"expected to reclaim item_id={item_id}")
    return ScenarioResult.ok(item_id=item_id, wait_s=wait_s, recovered=recovered)
