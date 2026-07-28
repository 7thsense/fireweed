"""INTENT: Renew a lease without completing work.

QUEUE MANAGEMENT: Long processing holds a claim; workers renew so another
consumer does not take the item after expiry.

RESP_COMMANDS: XADD, XREADGROUP, XCLAIM (consumer = lease_token)

ASSERTS: Claim returns lease_token; XCLAIM JUSTID with that token succeeds.

NOT_ON_RESP: Explicit lease-duration override on renew (queue default TTL applies).
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "08_lease_renew"
TITLE = "Renew a held lease"
TAGS = ("queue", "lease", "renew", "xclaim")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue

    R.xadd(
        r,
        q,
        R.WorkItem(
            client_item_key=ctx.key("lease-1"),
            priority=0,
            not_before=0,
            payload="hold-me",
        ).fields(),
    )

    # Step 1 — Claim our item (low priority so it surfaces first).
    target = None
    for _ in range(40):
        batch = R.claim_batch(r, q, group=ctx.group, consumer=ctx.consumer, count=10)
        for iid, fields in batch:
            if str(fields.get("client_item_key", "")) == ctx.key("lease-1"):
                target = (iid, fields)
                break
            try:
                R.complete(r, q, iid, group=ctx.group)
            except Exception:
                pass
        if target:
            break
    ctx.check(target is not None, "failed to claim lease-1 item")
    item_id, fields = target
    lease_token = fields.get("lease_token")
    ctx.log(
        f"CLAIM id={item_id} lease_token={lease_token} lease_expires_at={fields.get('lease_expires_at')}"
    )
    ctx.check(bool(lease_token), "claim must return lease_token")

    # Step 2 — Renew: XCLAIM with consumer name equal to the current lease_token.
    renewed = R.xclaim_renew(r, q, item_id, str(lease_token), group=ctx.group)
    ctx.log(f"XCLAIM renew => {renewed!r}")

    # Complete to avoid leaving leased junk for other scenarios.
    n = R.complete(r, q, item_id, group=ctx.group)
    ctx.check(n == 1, f"XACK expected 1, got {n}")

    return ScenarioResult.ok(item_id=item_id, lease_token=lease_token, renew=str(renewed))
