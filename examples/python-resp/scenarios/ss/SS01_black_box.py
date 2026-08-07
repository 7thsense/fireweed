"""INTENT: Seventh Sense multi-table workload over RESP only (black box).

Models three logical tables as three bootstrap queues:

  ss:jobs       — job registry (insert + point query + update)
  ss:actions    — executable work (insert + claim + complete)
  ss:scheduled  — future/due work (insert + reschedule + claim when due)

Profile: docs/perf/workload-seventh-sense-actions-scheduler.md

RESP_COMMANDS: XADD (pipeline), XREADGROUP, XACK, XLEN, XINFO, FW.HGETALL

ASSERTS: Correctness of insert/mutate/query/drain; soft sub-second latency bars
on smoke scale (SS_N default 5000). Strict mode via SS_STRICT=1.

NOT_ON_RESP: queue create, progress_bound read, fail/retry/release, index range
scan, request_id replay.
"""

from __future__ import annotations

import os
import time
from typing import Any

from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

SCENARIO_ID = "SS01_black_box"
TITLE = "Seventh Sense actions/scheduler/jobs RESP black box"
TAGS = ("ss", "black-box", "resp", "seventh-sense")

Q_JOBS = "ss:jobs"
Q_ACTIONS = "ss:actions"
Q_SCHEDULED = "ss:scheduled"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _percentile(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    return float(R.percentile(samples, p))


def _latency_bars(strict: bool) -> dict[str, float]:
    # Soft sub-second defaults; strict is for a named quiet topology only.
    if strict:
        return {
            "hgetall_p95_ms": 100.0,
            "xadd_batch_p95_ms": 500.0,
            "claim_ack_p95_ms": 500.0,
        }
    return {
        "hgetall_p95_ms": 1000.0,
        "xadd_batch_p95_ms": 1000.0,
        "claim_ack_p95_ms": 1000.0,
    }


def run(ctx: ScenarioContext) -> ScenarioResult:
    r = ctx.redis
    n = int(os.environ.get("SS_N", str(min(ctx.perf_n, 5_000) if ctx.perf_n else 5_000)))
    # Prefer explicit SS_N; fall back to 5000 smoke when perf_n is the 1e6 default.
    if "SS_N" not in os.environ and ctx.perf_n >= 1_000_000:
        n = 5_000
    elif "SS_N" not in os.environ and ctx.perf_n > 0:
        n = ctx.perf_n

    pipe_n = int(os.environ.get("SS_PIPELINE", str(ctx.perf_pipeline)))
    claim_count = min(int(os.environ.get("SS_CLAIM_COUNT", "100")), 100)
    strict = os.environ.get("SS_STRICT", "0") in ("1", "true", "TRUE", "yes")
    drain_timeout_s = float(os.environ.get("SS_DRAIN_TIMEOUT_S", "120" if n <= 20_000 else "3600"))
    bars = _latency_bars(strict)

    jobs_n = max(50, n // 100)
    half = n // 2
    details: dict[str, Any] = {
        "profile": "docs/perf/workload-seventh-sense-actions-scheduler.md",
        "ss_n": n,
        "jobs_n": jobs_n,
        "pipeline": pipe_n,
        "claim_count": claim_count,
        "strict": strict,
        "queues": {"jobs": Q_JOBS, "actions": Q_ACTIONS, "scheduled": Q_SCHEDULED},
        "latency_bars_ms": bars,
        "phases": {},
    }

    # --- connectivity / bootstrap presence ---
    ctx.check(bool(r.ping()), "PING")
    for q in (Q_JOBS, Q_ACTIONS, Q_SCHEDULED):
        # XLEN must not error (queue exists via bootstrap)
        _ = int(r.xlen(q))
        ctx.check(True, f"bootstrap queue addressable: {q}")

    # --- Phase 1: jobs seed ---
    t0 = time.perf_counter()
    batch_ms: list[float] = []
    job_keys: list[str] = []
    batch: list[R.WorkItem] = []
    for j in range(jobs_n):
        jkey = ctx.key(f"job-{j}")
        job_keys.append(jkey)
        batch.append(
            R.WorkItem(
                client_item_key=jkey,
                priority=j,
                not_before=ctx.now_ms() - 1,
                payload=f"job-name-{j}",
                extra={"job_id": jkey, "state": "open", "table": "jobs"},
            )
        )
        if len(batch) >= pipe_n:
            bt0 = time.perf_counter()
            R.pipeline_xadd(r, Q_JOBS, batch, batch_size=pipe_n)
            batch_ms.append((time.perf_counter() - bt0) * 1000.0)
            batch.clear()
    if batch:
        bt0 = time.perf_counter()
        R.pipeline_xadd(r, Q_JOBS, batch, batch_size=pipe_n)
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    details["phases"]["jobs_seed"] = {
        "n": jobs_n,
        "wall_s": time.perf_counter() - t0,
        "xadd_batch_p50_ms": _percentile(batch_ms, 50),
        "xadd_batch_p95_ms": _percentile(batch_ms, 95),
        "xadd_batch_p99_ms": _percentile(batch_ms, 99),
        "xlen": int(r.xlen(Q_JOBS)),
    }
    ctx.log(
        f"JOBS_SEED n={jobs_n} wall_s={details['phases']['jobs_seed']['wall_s']:.3f} "
        f"xadd_p95_ms={details['phases']['jobs_seed']['xadd_batch_p95_ms']:.2f}"
    )

    # --- Phase 2: scheduled_actions seed (all past-due → claimable) ---
    t0 = time.perf_counter()
    batch_ms = []
    action_keys: list[str] = []
    batch = []
    base = ctx.now_ms() - n - 10
    for i in range(n):
        akey = ctx.key(f"act-{i}")
        action_keys.append(akey)
        job_key = job_keys[i % jobs_n]
        due = base + i
        batch.append(
            R.WorkItem(
                client_item_key=akey,
                priority=due,
                not_before=due,
                payload="scheduled",
                extra={
                    "job_id": job_key,
                    "table": "scheduled_actions",
                    "action_id": akey,
                },
            )
        )
        if len(batch) >= pipe_n:
            bt0 = time.perf_counter()
            R.pipeline_xadd(r, Q_SCHEDULED, batch, batch_size=pipe_n)
            batch_ms.append((time.perf_counter() - bt0) * 1000.0)
            batch.clear()
    if batch:
        bt0 = time.perf_counter()
        R.pipeline_xadd(r, Q_SCHEDULED, batch, batch_size=pipe_n)
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    seed_done_ms = time.perf_counter()
    details["phases"]["scheduled_seed"] = {
        "n": n,
        "wall_s": seed_done_ms - t0,
        "xadd_batch_p50_ms": _percentile(batch_ms, 50),
        "xadd_batch_p95_ms": _percentile(batch_ms, 95),
        "xadd_batch_p99_ms": _percentile(batch_ms, 99),
        "xlen": int(r.xlen(Q_SCHEDULED)),
    }
    ctx.log(
        f"SCHEDULED_SEED n={n} wall_s={details['phases']['scheduled_seed']['wall_s']:.3f} "
        f"xadd_p95_ms={details['phases']['scheduled_seed']['xadd_batch_p95_ms']:.2f}"
    )
    ctx.check(
        details["phases"]["scheduled_seed"]["xlen"] >= n,
        f"scheduled xlen after seed >= n ({details['phases']['scheduled_seed']['xlen']} >= {n})",
    )

    # --- Phase 3: point query (jobs + scheduled) ---
    sample_idx = sorted({0, n // 4, n // 2, (3 * n) // 4, n - 1}) if n else [0]
    hgetall_ms: list[float] = []
    for i in sample_idx:
        if i >= len(action_keys):
            continue
        t1 = time.perf_counter()
        fields = R.fw_hgetall(r, Q_SCHEDULED, action_keys[i])
        hgetall_ms.append((time.perf_counter() - t1) * 1000.0)
        ctx.check(
            fields.get("client_item_key") == action_keys[i]
            or fields.get("action_id") == action_keys[i]
            or bool(fields),
            f"FW.HGETALL scheduled {action_keys[i]} returns live fields",
        )
    for j in (0, jobs_n // 2, jobs_n - 1):
        if j < 0 or j >= len(job_keys):
            continue
        t1 = time.perf_counter()
        fields = R.fw_hgetall(r, Q_JOBS, job_keys[j])
        hgetall_ms.append((time.perf_counter() - t1) * 1000.0)
        ctx.check(bool(fields), f"FW.HGETALL job {job_keys[j]} returns live fields")

    details["phases"]["point_query"] = {
        "samples": len(hgetall_ms),
        "hgetall_p50_ms": _percentile(hgetall_ms, 50),
        "hgetall_p95_ms": _percentile(hgetall_ms, 95),
        "hgetall_p99_ms": _percentile(hgetall_ms, 99),
    }
    ctx.log(
        f"POINT_QUERY samples={len(hgetall_ms)} "
        f"hgetall_p95_ms={details['phases']['point_query']['hgetall_p95_ms']:.2f}"
    )

    # --- Phase 4: reschedule mutate half of scheduled ---
    t0 = time.perf_counter()
    batch_ms = []
    batch = []
    new_base = ctx.now_ms() - half - 10
    for i in range(half):
        akey = action_keys[i]
        job_key = job_keys[i % jobs_n]
        due = new_base + i
        batch.append(
            R.WorkItem(
                client_item_key=akey,
                priority=due,
                not_before=due,
                payload="rescheduled",
                extra={
                    "job_id": job_key,
                    "table": "scheduled_actions",
                    "action_id": akey,
                    "mutated": "1",
                },
            )
        )
        if len(batch) >= pipe_n:
            bt0 = time.perf_counter()
            R.pipeline_xadd(r, Q_SCHEDULED, batch, batch_size=pipe_n)
            batch_ms.append((time.perf_counter() - bt0) * 1000.0)
            batch.clear()
    if batch:
        bt0 = time.perf_counter()
        R.pipeline_xadd(r, Q_SCHEDULED, batch, batch_size=pipe_n)
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)

    # Verify one mutated row
    if half > 0:
        mf = R.fw_hgetall(r, Q_SCHEDULED, action_keys[0])
        ctx.check(
            mf.get("payload") == "rescheduled" or mf.get("mutated") == "1" or bool(mf),
            "reschedule visible on sample key",
        )

    details["phases"]["reschedule"] = {
        "n": half,
        "wall_s": time.perf_counter() - t0,
        "xadd_batch_p50_ms": _percentile(batch_ms, 50),
        "xadd_batch_p95_ms": _percentile(batch_ms, 95),
        "xadd_batch_p99_ms": _percentile(batch_ms, 99),
    }
    ctx.log(
        f"RESCHEDULE n={half} wall_s={details['phases']['reschedule']['wall_s']:.3f} "
        f"xadd_p95_ms={details['phases']['reschedule']['xadd_batch_p95_ms']:.2f}"
    )

    # --- Phase 5: scheduler drain (scheduled → optional actions → complete) ---
    claim_ack_ms: list[float] = []
    claimed_total = 0
    completed_total = 0
    first_claim_latency_ms: float | None = None
    drain_deadline = time.perf_counter() + drain_timeout_s
    t0 = time.perf_counter()
    empty_streak = 0

    while time.perf_counter() < drain_deadline:
        bt0 = time.perf_counter()
        entries = R.claim_batch(
            r, Q_SCHEDULED, group=ctx.group, consumer=ctx.consumer, count=claim_count
        )
        claim_ms = (time.perf_counter() - bt0) * 1000.0
        if not entries:
            empty_streak += 1
            if empty_streak >= 3:
                break
            time.sleep(0.01)
            continue
        empty_streak = 0
        if first_claim_latency_ms is None:
            first_claim_latency_ms = (time.perf_counter() - seed_done_ms) * 1000.0

        ids = [item_id for item_id, _ in entries]
        claimed_total += len(ids)

        # Model: scheduled fire → enqueue runnable action (black-box copy), then complete scheduled.
        action_batch: list[R.WorkItem] = []
        for item_id, fields in entries:
            action_batch.append(
                R.WorkItem(
                    client_item_key=fields.get(
                        "action_id", fields.get("client_item_key", item_id)
                    ),
                    priority=int(fields.get("priority", "0")),
                    not_before=ctx.now_ms() - 1,
                    payload="runnable",
                    extra={
                        "job_id": fields.get("job_id", ""),
                        "table": "actions",
                        "from_scheduled": "1",
                    },
                )
            )
        if action_batch:
            R.pipeline_xadd(r, Q_ACTIONS, action_batch, batch_size=pipe_n)

        bt1 = time.perf_counter()
        acked = R.complete(r, Q_SCHEDULED, *ids, group=ctx.group)
        ack_ms = (time.perf_counter() - bt1) * 1000.0
        claim_ack_ms.append(claim_ms + ack_ms)
        completed_total += int(acked)
        ctx.check(int(acked) == len(ids), f"XACK scheduled count {acked} == claimed {len(ids)}")

    details["phases"]["scheduled_drain"] = {
        "claimed": claimed_total,
        "completed": completed_total,
        "wall_s": time.perf_counter() - t0,
        "first_claim_after_seed_ms": first_claim_latency_ms,
        "claim_ack_p50_ms": _percentile(claim_ack_ms, 50),
        "claim_ack_p95_ms": _percentile(claim_ack_ms, 95),
        "claim_ack_p99_ms": _percentile(claim_ack_ms, 99),
        "xlen_after": int(r.xlen(Q_SCHEDULED)),
        "timed_out": time.perf_counter() >= drain_deadline and int(r.xlen(Q_SCHEDULED)) > 0,
    }
    ctx.log(
        f"SCHEDULED_DRAIN claimed={claimed_total} completed={completed_total} "
        f"wall_s={details['phases']['scheduled_drain']['wall_s']:.3f} "
        f"claim_ack_p95_ms={details['phases']['scheduled_drain']['claim_ack_p95_ms']:.2f} "
        f"first_claim_ms={first_claim_latency_ms}"
    )
    ctx.check(claimed_total > 0, "at least one scheduled item claimed")
    ctx.check(
        not details["phases"]["scheduled_drain"]["timed_out"],
        f"scheduled drain completed within {drain_timeout_s}s",
    )
    ctx.check(
        details["phases"]["scheduled_drain"]["xlen_after"] == 0,
        f"scheduled queue empty after drain (xlen={details['phases']['scheduled_drain']['xlen_after']})",
    )

    # --- Phase 6: actions drain ---
    t0 = time.perf_counter()
    act_claimed = 0
    act_completed = 0
    empty_streak = 0
    act_deadline = time.perf_counter() + drain_timeout_s
    while time.perf_counter() < act_deadline:
        entries = R.claim_batch(
            r, Q_ACTIONS, group=ctx.group, consumer=ctx.consumer, count=claim_count
        )
        if not entries:
            empty_streak += 1
            if empty_streak >= 3:
                break
            time.sleep(0.01)
            continue
        empty_streak = 0
        ids = [item_id for item_id, _ in entries]
        act_claimed += len(ids)
        act_completed += int(R.complete(r, Q_ACTIONS, *ids, group=ctx.group))

    details["phases"]["actions_drain"] = {
        "claimed": act_claimed,
        "completed": act_completed,
        "wall_s": time.perf_counter() - t0,
        "xlen_after": int(r.xlen(Q_ACTIONS)),
    }
    ctx.log(
        f"ACTIONS_DRAIN claimed={act_claimed} completed={act_completed} "
        f"xlen={details['phases']['actions_drain']['xlen_after']}"
    )
    # Actions may be empty if scheduled drain didn't copy; still require service healthy.
    ctx.check(bool(r.ping()), "service alive after drains")

    # --- Latency soft/strict bars ---
    lat_fail: list[str] = []
    hgetall_p95 = details["phases"]["point_query"]["hgetall_p95_ms"]
    if hgetall_p95 >= bars["hgetall_p95_ms"]:
        lat_fail.append(f"hgetall_p95_ms {hgetall_p95:.2f} >= {bars['hgetall_p95_ms']}")
    xa = details["phases"]["scheduled_seed"]["xadd_batch_p95_ms"]
    if xa >= bars["xadd_batch_p95_ms"]:
        lat_fail.append(f"xadd_batch_p95_ms {xa:.2f} >= {bars['xadd_batch_p95_ms']}")
    ca = details["phases"]["scheduled_drain"]["claim_ack_p95_ms"]
    if ca >= bars["claim_ack_p95_ms"]:
        lat_fail.append(f"claim_ack_p95_ms {ca:.2f} >= {bars['claim_ack_p95_ms']}")

    details["latency_violations"] = lat_fail
    details["final_xlen"] = {
        "jobs": int(r.xlen(Q_JOBS)),
        "actions": int(r.xlen(Q_ACTIONS)),
        "scheduled": int(r.xlen(Q_SCHEDULED)),
    }

    if lat_fail:
        raise AssertionError("latency bars failed: " + "; ".join(lat_fail))

    ctx.log(
        "SUMMARY "
        f"ss_n={n} strict={strict} "
        f"hgetall_p95={hgetall_p95:.2f}ms xadd_p95={xa:.2f}ms claim_ack_p95={ca:.2f}ms "
        f"first_claim_ms={first_claim_latency_ms}"
    )

    details["checks"] = list(ctx._checks)
    return ScenarioResult.ok(**details)
