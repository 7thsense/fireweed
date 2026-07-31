"""INTENT: Parameterized full lifecycle via the shared RESP workflow framework.

Exercises the exact demo loop on one queue (default ss:actions, overridable):

    for cycle in 1..y:
        insert N
        mutate floor(N / x)
        get status
    claim/complete in chunks of Z until empty

Framework: lib/workflow.py (LifecycleParams + run_lifecycle)

Env knobs (WF_* preferred; SS_* aliases for SS suite muscle-memory):

    WF_N / SS_N                 N  (default 5000)
    WF_MUTATE_DIVISOR / SS_X    x  (default 2 → mutate N/2)
    WF_CYCLES / SS_Y            y  (default 1)
    WF_CLAIM_CHUNK / SS_Z       Z  (default 100; server may cap)
    WF_PIPELINE / SS_PIPELINE   XADD pipeline (default 1000)
    WF_DRAIN_TIMEOUT_S          drain wall budget
    WF_QUEUE                    stream key (default ss:actions)
    WF_STATUS_EVERY_CHUNKS      mid-drain status cadence (0 = open/close only)

RESP_COMMANDS: XADD pipeline, XREADGROUP, XACK, XLEN, XINFO, XPENDING, FW.HGETALL

ASSERTS: mutate visible; drain empties queue; PING before/after.

NOT_ON_RESP: queue create, fail/retry/release, progress_bound read, range_scan.
"""

from __future__ import annotations

import os

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib.workflow import LifecycleParams, run_lifecycle

SCENARIO_ID = "SS02_lifecycle"
TITLE = "Parameterized insert/mutate/status × y + drain Z lifecycle"
TAGS = ("ss", "lifecycle", "framework", "resp", "seventh-sense")


def _env_int(*names: str, default: int) -> int:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and raw != "":
            return int(raw)
    return default


def _env_float(*names: str, default: float) -> float:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and raw != "":
            return float(raw)
    return default


def run(ctx: ScenarioContext) -> ScenarioResult:
    # Resolve N with SS smoke default when operator has not set scale knobs.
    default_n = 5_000
    if "WF_N" not in os.environ and "SS_N" not in os.environ:
        if 0 < ctx.perf_n < 1_000_000:
            default_n = ctx.perf_n
        elif ctx.perf_n >= 1_000_000:
            default_n = 5_000

    n = _env_int("WF_N", "SS_N", default=default_n)
    x = _env_int("WF_MUTATE_DIVISOR", "SS_X", default=2)
    y = _env_int("WF_CYCLES", "SS_Y", default=1)
    z = _env_int("WF_CLAIM_CHUNK", "SS_Z", "SS_CLAIM_COUNT", default=100)
    pipeline = _env_int(
        "WF_PIPELINE", "SS_PIPELINE", default=ctx.perf_pipeline or 1_000
    )
    drain_timeout = _env_float(
        "WF_DRAIN_TIMEOUT_S",
        "SS_DRAIN_TIMEOUT_S",
        default=120.0 if n * y <= 20_000 else 3600.0,
    )
    status_every = _env_int("WF_STATUS_EVERY_CHUNKS", default=0)

    queue = os.environ.get("WF_QUEUE") or os.environ.get("SS_QUEUE") or "ss:actions"

    params = LifecycleParams(
        n=n,
        mutate_divisor=x,
        cycles=y,
        claim_chunk=z,
        pipeline=pipeline,
        drain_timeout_s=drain_timeout,
        status_every_chunks=status_every,
    )

    ctx.log(
        f"SS02 params N={params.n} x={params.mutate_divisor} "
        f"(mutate={params.mutate_count}/cycle) y={params.cycles} "
        f"Z={params.claim_chunk} pipeline={params.pipeline} queue={queue}"
    )

    report = run_lifecycle(
        ctx.redis,
        queue,
        params,
        group=ctx.group,
        consumer=ctx.consumer,
        key_fn=ctx.key,
        log=ctx.log,
        check=ctx.check,
    )

    details = report.to_dict()
    details["checks"] = list(ctx._checks)
    details["scenario"] = SCENARIO_ID
    return ScenarioResult.ok(**details)
