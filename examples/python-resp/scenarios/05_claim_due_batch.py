"""INTENT: Claim the next due batch in schedule (priority) order.

QUEUE MANAGEMENT: Workers pull eligible items. Ordering is by ascending priority
(bootstrap: Int64 ascending). Putting schedule millis in priority yields
earliest-due-first delivery. There is no separate 'get by timestamp' command.

RESP_COMMANDS: XADD, XREADGROUP, XACK

ASSERTS: Claim order matches ascending priorities for this run's items.

NOT_ON_RESP: Filtered claim by group_key; rich metrics.oldest_eligible_age_ms.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "05_claim_due_batch"
TITLE = "Claim due batch in priority order"
TAGS = ("queue", "claim", "priority", "schedule")

N = 5


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    # Use tiny priorities so ascending claim surfaces these first even if the
    # shared demo:work stream has residual higher-priority load from other runs.
    # not_before=0 => eligible immediately.
    # Step 1 — Insert in scrambled push order; priorities define claim order.
    push_order = [3, 1, 4, 0, 2]
    items = []
    for rank in push_order:
        prio = rank  # 0..4
        items.append(
            R.WorkItem(
                client_item_key=ctx.key(f"due-{rank}"),
                priority=prio,
                not_before=0,
                payload=f"rank-{rank}",
            )
        )
    ids = R.pipeline_xadd(r, q, items, batch_size=N)
    ctx.log(f"INSERT scrambled push ranks={push_order} ids={ids}")

    # Step 2 — Claim until we hold this run's N items (may interleave others).
    ours: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for _ in range(50):
        more = R.claim_batch(r, q, group=ctx.group, consumer=ctx.consumer, count=10)
        if not more:
            break
        for iid, f in more:
            ck = str(f.get("client_item_key", ""))
            if ck.startswith(ctx.run_id + "-due-") and iid not in seen:
                ours.append((iid, f))
                seen.add(iid)
            elif iid not in seen:
                # Complete foreign items we accidentally claimed so we do not leak leases.
                try:
                    R.complete(r, q, iid, group=ctx.group)
                except Exception:
                    pass
        if len(ours) >= N:
            break

    ctx.check(len(ours) >= N, f"expected at least {N} claimed ours, got {len(ours)}")
    ours = ours[:N]
    priorities = [int(f["priority"]) for _, f in ours]
    ctx.log(f"CLAIM order priorities={priorities}")
    ctx.check(
        priorities == sorted(priorities),
        f"priorities must be ascending, got {priorities}",
    )
    expected = list(range(N))
    ctx.check(
        priorities == expected,
        f"expected schedule order {expected}, got {priorities}",
    )

    # Step 3 — Complete so we leave the queue clean for this batch.
    ack_ids = [iid for iid, _ in ours]
    n_ack = R.complete(r, q, *ack_ids, group=ctx.group)
    ctx.log(f"XACK n={n_ack} ids={ack_ids}")
    ctx.check(n_ack == N, f"XACK should complete {N}, got {n_ack}")

    return ScenarioResult.ok(priorities=priorities, claimed_ids=ack_ids, n=N)
