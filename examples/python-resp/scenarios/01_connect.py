"""INTENT: Verify RESP connectivity and queue stream addressing.

QUEUE MANAGEMENT: Before any work is enqueued, the client confirms the service
is reachable and uses the tenant:queue stream key convention.

RESP_COMMANDS: PING, XLEN

ASSERTS: PING is true; XLEN on the demo queue is an integer (queue exists).

NOT_ON_RESP: Queue create/configure — use FIREWEED_BOOTSTRAP_QUEUES at service start.
"""

from __future__ import annotations

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "01_connect"
TITLE = "Connect and address a queue"
TAGS = ("queue", "connect", "ping")


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    q = ctx.queue

    # Step 1 — PING must succeed (Fireweed speaks RESP like Redis for this command).
    ctx.log(f"PING {ctx.queue!r} via live connection")
    pong = r.ping()
    ctx.check(pong is True, "PING should return True")

    # Step 2 — Stream key is tenant:queue (e.g. demo:work). XLEN is live pending+leased.
    depth = int(r.xlen(q))
    ctx.log(f"XLEN {q} = {depth}")
    ctx.check(depth >= 0, "XLEN should be non-negative")

    return ScenarioResult.ok(ping=True, xlen=depth, queue=q)
