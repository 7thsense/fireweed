"""INTENT: Claiming before eligibility returns no work.

QUEUE MANAGEMENT: Items may sit in the queue with a future not_before. Workers
must not receive them until the eligibility floor is reached.

RESP_COMMANDS: XADD, XREADGROUP

ASSERTS: XREADGROUP returns an empty batch for future-dated items.

NOT_ON_RESP: Injected test clocks — wall clock only on the service.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "04_claim_before_due"
TITLE = "Empty claim before eligibility"
TAGS = ("queue", "claim", "not_before")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue
    # Far enough that wall-clock jitter cannot make them due mid-scenario.
    due = ctx.now_ms() + 120_000

    # Step 1 — Enqueue items that are not yet eligible.
    items = [
        R.WorkItem(
            client_item_key=ctx.key(f"future-{i}"),
            priority=due + i,
            not_before=due + i,
            payload=f"later-{i}",
        )
        for i in range(5)
    ]
    ids = R.pipeline_xadd(r, q, items, batch_size=5)
    ctx.log(f"INSERT future ids={ids} not_before>={due}")

    # Step 2 — Claim should be empty (only '>' is supported; priority-ordered eligibles).
    claimed = R.claim_batch(r, q, group=ctx.group, consumer=ctx.consumer, count=10)
    # Filter to our keys in case other pending work exists on a shared queue.
    ours = [
        (iid, f)
        for iid, f in claimed
        if str(f.get("client_item_key", "")).startswith(ctx.run_id)
    ]
    ctx.log(f"XREADGROUP ours={len(ours)} total_reply={len(claimed)}")
    ctx.check(len(ours) == 0, f"expected no due items for this run, got {ours!r}")

    return ScenarioResult.ok(inserted=ids, claimed_ours=0, not_before_min=due)
