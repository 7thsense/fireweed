"""INTENT: Reschedule / replace pending items in batch.

QUEUE MANAGEMENT: Before claim, producers may change send time or payload.
Over RESP this is a pending replace: re-XADD the same client_item_key.

RESP_COMMANDS: XADD (insert), XADD (replace), FW.HGETALL

ASSERTS: Replace returns new ids; live read shows updated priority.

NOT_ON_RESP: In-place update_fields / BatchUpdate; replace fails if leased/terminal.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "03_batch_update_pending"
TITLE = "Update pending items (reschedule)"
TAGS = ("queue", "batch-update", "pending-replace")

N = 5


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    base = ctx.now_ms()

    # Step 1 — Insert pending items with known keys.
    original = [
        R.WorkItem(
            client_item_key=ctx.key(f"upd-{i}"),
            priority=base + 10_000 + i,
            not_before=base + 10_000 + i,
            payload=f"v1-{i}",
        )
        for i in range(N)
    ]
    ids_v1 = R.pipeline_xadd(r, q, original, batch_size=N)
    ctx.log(f"inserted v1 ids={ids_v1}")

    # Step 2 — Pending replace: same client_item_key, new priority/payload.
    # Server assigns a new item_id; the old id is superseded.
    updated = [
        R.WorkItem(
            client_item_key=ctx.key(f"upd-{i}"),
            priority=base + i,  # pull earlier
            not_before=base + i,
            payload=f"v2-{i}",
        )
        for i in range(N)
    ]
    ids_v2 = R.pipeline_xadd(r, q, updated, batch_size=N)
    ctx.log(f"replaced v2 ids={ids_v2}")
    ctx.check(len(ids_v2) == N, "replace should return N ids")
    ctx.check(set(ids_v1).isdisjoint(set(ids_v2)), "replace must mint new item ids")

    # Step 3 — Live read by client_item_key (Pending/Leased only).
    sample_key = ctx.key("upd-0")
    live = R.fw_hgetall(r, q, sample_key)
    ctx.log(f"FW.HGETALL {sample_key} => {live}")
    ctx.check(live.get("payload") == "v2-0", f"payload should be v2, got {live!r}")
    ctx.check(
        live.get("priority") == str(base),
        f"priority should be updated to {base}, got {live.get('priority')!r}",
    )
    state = str(live.get("lifecycle_state", ""))
    ctx.check(
        state.lower() == "pending",
        f"expected Pending lifecycle, got {live.get('lifecycle_state')!r}",
    )

    return ScenarioResult.ok(
        n=N,
        ids_v1=ids_v1,
        ids_v2=ids_v2,
        sample=live,
    )
