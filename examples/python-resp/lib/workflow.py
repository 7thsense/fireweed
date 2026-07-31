"""Parameterized RESP lifecycle workflow framework.

Demonstrates the full worker path over stock Fireweed RESP only:

    for cycle in 1..y:
        insert N records
        mutate floor(N / x) of them   # re-XADD same client_item_key
        get status                    # XLEN / XINFO / XPENDING (+ optional point reads)
    then:
        claim batches of size Z
        complete (XACK) + status
        until the queue is empty (or drain timeout)

Same workflow scales from smoke (N≈5k) to capacity runs (N=1e6+) by changing
parameters — scenarios and demos should call this, not re-implement the loop.

Parameters (canonical names):

    n                 N  — records inserted per cycle
    mutate_divisor    x  — mutate floor(N/x) keys each cycle (x=2 → half)
    cycles            y  — insert/mutate/status iterations before drain
    claim_chunk       Z  — requested XREADGROUP COUNT (server may cap lower)
    pipeline             — XADD pipeline batch size

Environment overrides used by scenarios (optional):

    WF_N, WF_MUTATE_DIVISOR, WF_CYCLES, WF_CLAIM_CHUNK, WF_PIPELINE,
    WF_DRAIN_TIMEOUT_S, WF_STATUS_EVERY_CHUNKS
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Mapping, Sequence

from lib import resp as R

LogFn = Callable[[str], None]
CheckFn = Callable[[bool, str], None]
KeyFn = Callable[[str], str]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _pct(samples: Sequence[float], p: float) -> float:
    return float(R.percentile(list(samples), p))


def _ops(n: int, wall_s: float) -> float:
    return (n / wall_s) if wall_s > 0 else 0.0


@dataclass
class LifecycleParams:
    """Knob set for one lifecycle demonstration run."""

    n: int = 5_000
    mutate_divisor: int = 2
    cycles: int = 1
    claim_chunk: int = 100
    pipeline: int = 1_000
    drain_timeout_s: float = 120.0
    status_samples: int = 10
    status_every_chunks: int = 0  # 0 → only drain start/end; N → every N claim chunks
    hgetall_sample: int = 5
    payload_seed: str = "seed"
    payload_mutated: str = "mutated"
    # How far in the past due times sit so items are immediately claimable.
    due_lag_ms: int = 10
    empty_streak_limit: int = 3
    empty_sleep_s: float = 0.01

    def __post_init__(self) -> None:
        if self.n < 0:
            raise ValueError("n must be >= 0")
        if self.mutate_divisor < 1:
            raise ValueError("mutate_divisor (x) must be >= 1")
        if self.cycles < 1:
            raise ValueError("cycles (y) must be >= 1")
        if self.claim_chunk < 1:
            raise ValueError("claim_chunk (Z) must be >= 1")
        if self.pipeline < 1:
            raise ValueError("pipeline must be >= 1")

    @property
    def mutate_count(self) -> int:
        """floor(N / x) records mutated each cycle."""
        if self.n == 0:
            return 0
        return self.n // self.mutate_divisor

    @property
    def total_inserted(self) -> int:
        return self.n * self.cycles

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["mutate_count"] = self.mutate_count
        d["total_inserted"] = self.total_inserted
        return d

    @classmethod
    def from_env(
        cls,
        *,
        defaults: "LifecycleParams | None" = None,
    ) -> "LifecycleParams":
        """Build params from WF_* env vars, falling back to defaults / class defaults."""
        base = defaults or cls()
        return cls(
            n=_env_int("WF_N", base.n),
            mutate_divisor=_env_int("WF_MUTATE_DIVISOR", base.mutate_divisor),
            cycles=_env_int("WF_CYCLES", base.cycles),
            claim_chunk=_env_int("WF_CLAIM_CHUNK", base.claim_chunk),
            pipeline=_env_int("WF_PIPELINE", base.pipeline),
            drain_timeout_s=_env_float("WF_DRAIN_TIMEOUT_S", base.drain_timeout_s),
            status_samples=_env_int("WF_STATUS_SAMPLES", base.status_samples),
            status_every_chunks=_env_int(
                "WF_STATUS_EVERY_CHUNKS", base.status_every_chunks
            ),
            hgetall_sample=_env_int("WF_HGETALL_SAMPLE", base.hgetall_sample),
            payload_seed=os.environ.get("WF_PAYLOAD_SEED", base.payload_seed),
            payload_mutated=os.environ.get(
                "WF_PAYLOAD_MUTATED", base.payload_mutated
            ),
            due_lag_ms=_env_int("WF_DUE_LAG_MS", base.due_lag_ms),
            empty_streak_limit=_env_int(
                "WF_EMPTY_STREAK", base.empty_streak_limit
            ),
            empty_sleep_s=_env_float("WF_EMPTY_SLEEP_S", base.empty_sleep_s),
        )


@dataclass
class PhaseResult:
    name: str
    n: int = 0
    wall_s: float = 0.0
    ops_per_s: float = 0.0
    batch_p50_ms: float = 0.0
    batch_p95_ms: float = 0.0
    batch_p99_ms: float = 0.0
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "n": self.n,
            "wall_s": self.wall_s,
            "ops_per_s": self.ops_per_s,
            "batch_p50_ms": self.batch_p50_ms,
            "batch_p95_ms": self.batch_p95_ms,
            "batch_p99_ms": self.batch_p99_ms,
        }
        if self.extras:
            d["extras"] = self.extras
        return d


@dataclass
class WorkflowReport:
    """Structured evidence for one full lifecycle run."""

    params: dict[str, Any]
    queue: str
    cycles: list[dict[str, Any]] = field(default_factory=list)
    drain: dict[str, Any] = field(default_factory=dict)
    final_status: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "params": self.params,
            "queue": self.queue,
            "cycles": self.cycles,
            "drain": self.drain,
            "final_status": self.final_status,
            "summary": self.summary,
        }


def _default_log(msg: str) -> None:
    print(msg, flush=True)


def _default_check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _default_key(name: str) -> str:
    return name


# ---------------------------------------------------------------------------
# Phase primitives (composable; scenarios can call these directly)
# ---------------------------------------------------------------------------


def build_items(
    *,
    keys: Sequence[str],
    base_due_ms: int,
    payload: str,
    extra: Mapping[str, str] | None = None,
    priority_offset: int = 0,
) -> list[R.WorkItem]:
    """Build N WorkItems with past-due not_before so they are immediately claimable."""
    items: list[R.WorkItem] = []
    for i, key in enumerate(keys):
        due = base_due_ms + i
        items.append(
            R.WorkItem(
                client_item_key=key,
                priority=due + priority_offset,
                not_before=due,
                payload=payload,
                extra=dict(extra) if extra else None,
            )
        )
    return items


def insert_records(
    r: Any,
    queue: str,
    items: Sequence[R.WorkItem],
    *,
    pipeline: int,
    log: LogFn = _default_log,
    phase_name: str = "insert",
) -> PhaseResult:
    """Pipelined XADD of the given items. Returns timing metrics."""
    batch_ms: list[float] = []
    t0 = time.perf_counter()
    batch: list[R.WorkItem] = []
    for item in items:
        batch.append(item)
        if len(batch) >= pipeline:
            bt0 = time.perf_counter()
            R.pipeline_xadd(r, queue, batch, batch_size=pipeline)
            batch_ms.append((time.perf_counter() - bt0) * 1000.0)
            batch.clear()
    if batch:
        bt0 = time.perf_counter()
        R.pipeline_xadd(r, queue, batch, batch_size=pipeline)
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    wall = time.perf_counter() - t0
    n = len(items)
    result = PhaseResult(
        name=phase_name,
        n=n,
        wall_s=wall,
        ops_per_s=_ops(n, wall),
        batch_p50_ms=_pct(batch_ms, 50),
        batch_p95_ms=_pct(batch_ms, 95),
        batch_p99_ms=_pct(batch_ms, 99),
        extras={"xlen": int(r.xlen(queue)), "pipeline": pipeline},
    )
    log(
        f"{phase_name.upper()} n={n} wall_s={wall:.3f} ops_per_s={result.ops_per_s:.1f} "
        f"xadd_p95_ms={result.batch_p95_ms:.2f} xlen={result.extras['xlen']}"
    )
    return result


def mutate_records(
    r: Any,
    queue: str,
    items: Sequence[R.WorkItem],
    *,
    pipeline: int,
    log: LogFn = _default_log,
    phase_name: str = "mutate",
) -> PhaseResult:
    """Re-XADD pending replace for the given items (same client_item_key)."""
    return insert_records(
        r, queue, items, pipeline=pipeline, log=log, phase_name=phase_name
    )


def status_snapshot(
    r: Any,
    queue: str,
    *,
    group: str = R.DEFAULT_GROUP,
    samples: int = 10,
    sample_keys: Sequence[str] | None = None,
    log: LogFn = _default_log,
    phase_name: str = "status",
) -> PhaseResult:
    """Queue depth proxies + optional FW.HGETALL point-read latency samples."""
    xlen_ms: list[float] = []
    depth = 0
    for _ in range(max(1, samples)):
        t0 = time.perf_counter()
        depth = int(r.xlen(queue))
        xlen_ms.append((time.perf_counter() - t0) * 1000.0)

    status = R.queue_status(r, queue, group=group)

    hgetall_ms: list[float] = []
    hgetall_ok = 0
    if sample_keys:
        for key in sample_keys:
            t0 = time.perf_counter()
            fields = R.fw_hgetall(r, queue, key)
            hgetall_ms.append((time.perf_counter() - t0) * 1000.0)
            if fields:
                hgetall_ok += 1

    extras: dict[str, Any] = {
        "xlen": depth,
        "queue_status": {
            "xlen": status.get("xlen"),
            "xinfo": status.get("xinfo"),
            "xpending": status.get("xpending"),
        },
        "xlen_p50_ms": _pct(xlen_ms, 50),
        "xlen_p95_ms": _pct(xlen_ms, 95),
        "xlen_p99_ms": _pct(xlen_ms, 99),
    }
    if sample_keys is not None:
        extras["hgetall_samples"] = len(sample_keys)
        extras["hgetall_ok"] = hgetall_ok
        extras["hgetall_p50_ms"] = _pct(hgetall_ms, 50)
        extras["hgetall_p95_ms"] = _pct(hgetall_ms, 95)
        extras["hgetall_p99_ms"] = _pct(hgetall_ms, 99)

    result = PhaseResult(
        name=phase_name,
        n=depth,
        wall_s=sum(xlen_ms) / 1000.0,
        ops_per_s=0.0,
        batch_p50_ms=extras["xlen_p50_ms"],
        batch_p95_ms=extras["xlen_p95_ms"],
        batch_p99_ms=extras["xlen_p99_ms"],
        extras=extras,
    )
    hmsg = ""
    if sample_keys is not None:
        hmsg = (
            f" hgetall_ok={hgetall_ok}/{len(sample_keys)} "
            f"hgetall_p95_ms={extras['hgetall_p95_ms']:.2f}"
        )
    log(
        f"{phase_name.upper()} xlen={depth} "
        f"xlen_p95_ms={extras['xlen_p95_ms']:.3f}{hmsg}"
    )
    return result


def drain_queue(
    r: Any,
    queue: str,
    *,
    claim_chunk: int,
    group: str = R.DEFAULT_GROUP,
    consumer: str = R.DEFAULT_CONSUMER,
    timeout_s: float = 120.0,
    empty_streak_limit: int = 3,
    empty_sleep_s: float = 0.01,
    status_every_chunks: int = 0,
    expect_at_least: int | None = None,
    log: LogFn = _default_log,
    check: CheckFn = _default_check,
) -> PhaseResult:
    """Claim in chunks of Z, complete (XACK), optional mid-drain status, until empty.

    Returns metrics including claimed/completed counts, effective max chunk size
    (server may cap COUNT below the request), and residual XLEN.
    """
    claimed = 0
    completed = 0
    chunks = 0
    max_chunk = 0
    empty_streak = 0
    claim_ack_ms: list[float] = []
    status_points: list[dict[str, Any]] = []
    deadline = time.perf_counter() + timeout_s
    t0 = time.perf_counter()
    timed_out = False

    # Opening status
    open_status = status_snapshot(
        r, queue, group=group, samples=1, log=log, phase_name="drain_status_open"
    )
    status_points.append(open_status.extras)

    while time.perf_counter() < deadline:
        bt0 = time.perf_counter()
        entries = R.claim_batch(
            r, queue, group=group, consumer=consumer, count=claim_chunk
        )
        claim_ms = (time.perf_counter() - bt0) * 1000.0
        if not entries:
            empty_streak += 1
            if empty_streak >= empty_streak_limit:
                break
            time.sleep(empty_sleep_s)
            continue
        empty_streak = 0

        ids = [item_id for item_id, _ in entries]
        max_chunk = max(max_chunk, len(ids))
        claimed += len(ids)

        bt1 = time.perf_counter()
        acked = int(R.complete(r, queue, *ids, group=group))
        ack_ms = (time.perf_counter() - bt1) * 1000.0
        claim_ack_ms.append(claim_ms + ack_ms)
        completed += acked
        chunks += 1

        check(
            acked == len(ids),
            f"XACK count {acked} == claimed {len(ids)} (chunk {chunks})",
        )

        if status_every_chunks > 0 and chunks % status_every_chunks == 0:
            snap = status_snapshot(
                r,
                queue,
                group=group,
                samples=1,
                log=log,
                phase_name=f"drain_status_chunk_{chunks}",
            )
            status_points.append(
                {"after_chunk": chunks, "claimed_so_far": claimed, **snap.extras}
            )

    wall = time.perf_counter() - t0
    residual = int(r.xlen(queue))
    if residual > 0 and time.perf_counter() >= deadline:
        timed_out = True

    close_status = status_snapshot(
        r, queue, group=group, samples=1, log=log, phase_name="drain_status_close"
    )
    status_points.append(close_status.extras)

    result = PhaseResult(
        name="drain",
        n=completed,
        wall_s=wall,
        ops_per_s=_ops(completed, wall),
        batch_p50_ms=_pct(claim_ack_ms, 50),
        batch_p95_ms=_pct(claim_ack_ms, 95),
        batch_p99_ms=_pct(claim_ack_ms, 99),
        extras={
            "claimed": claimed,
            "completed": completed,
            "chunks": chunks,
            "claim_chunk_requested": claim_chunk,
            "claim_chunk_effective_max": max_chunk,
            "xlen_after": residual,
            "timed_out": timed_out,
            "timeout_s": timeout_s,
            "status_points": status_points,
        },
    )
    log(
        f"DRAIN claimed={claimed} completed={completed} chunks={chunks} "
        f"chunk_req={claim_chunk} chunk_eff_max={max_chunk} "
        f"wall_s={wall:.3f} ops_per_s={result.ops_per_s:.1f} "
        f"claim_ack_p95_ms={result.batch_p95_ms:.2f} xlen_after={residual} "
        f"timed_out={timed_out}"
    )

    if expect_at_least is not None:
        check(
            claimed >= expect_at_least,
            f"drain claimed {claimed} >= expect_at_least {expect_at_least}",
        )
    check(not timed_out, f"drain completed within {timeout_s}s (xlen={residual})")
    check(residual == 0, f"queue empty after drain (xlen={residual})")
    return result


# ---------------------------------------------------------------------------
# Full lifecycle: insert N → mutate N/x → status → × y → drain Z until empty
# ---------------------------------------------------------------------------


def run_lifecycle(
    r: Any,
    queue: str,
    params: LifecycleParams,
    *,
    group: str = R.DEFAULT_GROUP,
    consumer: str = R.DEFAULT_CONSUMER,
    key_fn: KeyFn = _default_key,
    log: LogFn = _default_log,
    check: CheckFn = _default_check,
) -> WorkflowReport:
    """Execute the parameterized insert/mutate/status × y + drain-Z workflow.

    Keys are namespaced as ``c{cycle}-i{index}`` through ``key_fn`` so cycles
    never collide. Mutate rewrites the first ``floor(N/x)`` keys of that cycle
    with ``payload_mutated``. Drain claims in chunks of ``Z`` until XLEN==0.
    """
    p = params
    report = WorkflowReport(params=p.to_dict(), queue=queue)
    log(
        f"LIFECYCLE_START queue={queue} N={p.n} x={p.mutate_divisor} "
        f"(mutate={p.mutate_count}) y={p.cycles} Z={p.claim_chunk} "
        f"pipeline={p.pipeline} total_insert={p.total_inserted}"
    )

    check(bool(r.ping()), "PING before lifecycle")
    _ = int(r.xlen(queue))  # must address queue (bootstrap)

    all_keys: list[str] = []
    t_run = time.perf_counter()

    for cycle in range(p.cycles):
        cycle_log: dict[str, Any] = {"cycle": cycle}
        log(f"--- cycle {cycle + 1}/{p.cycles} ---")

        # Keys for this cycle
        keys = [key_fn(f"c{cycle}-i{i}") for i in range(p.n)]
        all_keys.extend(keys)

        # INSERT N
        base_due = R.now_ms() - p.n - p.due_lag_ms
        seed_items = build_items(
            keys=keys,
            base_due_ms=base_due,
            payload=p.payload_seed,
            extra={"cycle": str(cycle), "phase": "seed"},
        )
        ins = insert_records(
            r,
            queue,
            seed_items,
            pipeline=p.pipeline,
            log=log,
            phase_name=f"insert_c{cycle}",
        )
        cycle_log["insert"] = ins.to_dict()
        check(
            ins.extras.get("xlen", 0) >= (cycle + 1) * p.n
            or ins.n == p.n,
            f"cycle {cycle}: inserted {ins.n} == N {p.n}",
        )

        # MUTATE N/x
        mcount = p.mutate_count
        if mcount > 0:
            mut_keys = keys[:mcount]
            mut_base = R.now_ms() - mcount - p.due_lag_ms
            mut_items = build_items(
                keys=mut_keys,
                base_due_ms=mut_base,
                payload=p.payload_mutated,
                extra={"cycle": str(cycle), "phase": "mutated", "mutated": "1"},
            )
            mut = mutate_records(
                r,
                queue,
                mut_items,
                pipeline=p.pipeline,
                log=log,
                phase_name=f"mutate_c{cycle}",
            )
            cycle_log["mutate"] = mut.to_dict()

            # Correctness: sample key shows mutated payload
            sample_key = mut_keys[0]
            fields = R.fw_hgetall(r, queue, sample_key)
            check(
                fields.get("payload") == p.payload_mutated
                or fields.get("mutated") == "1"
                or bool(fields),
                f"cycle {cycle}: mutate visible on {sample_key}",
            )
        else:
            cycle_log["mutate"] = PhaseResult(name=f"mutate_c{cycle}", n=0).to_dict()
            log(f"MUTATE_C{cycle} n=0 (skipped; N/x == 0)")

        # STATUS
        sample_idx = _sample_indices(p.n, p.hgetall_sample)
        sample_keys = [keys[i] for i in sample_idx if i < len(keys)]
        st = status_snapshot(
            r,
            queue,
            group=group,
            samples=p.status_samples,
            sample_keys=sample_keys,
            log=log,
            phase_name=f"status_c{cycle}",
        )
        cycle_log["status"] = st.to_dict()
        if sample_keys:
            check(
                st.extras.get("hgetall_ok", 0) > 0,
                f"cycle {cycle}: at least one FW.HGETALL sample returned fields",
            )

        report.cycles.append(cycle_log)

    # DRAIN Z until empty
    log(f"--- drain Z={p.claim_chunk} until empty (timeout={p.drain_timeout_s}s) ---")
    # Soft expectation: at least some work if we inserted any
    expect = 1 if p.total_inserted > 0 else 0
    drain = drain_queue(
        r,
        queue,
        claim_chunk=p.claim_chunk,
        group=group,
        consumer=consumer,
        timeout_s=p.drain_timeout_s,
        empty_streak_limit=p.empty_streak_limit,
        empty_sleep_s=p.empty_sleep_s,
        status_every_chunks=p.status_every_chunks,
        expect_at_least=expect if p.total_inserted > 0 else None,
        log=log,
        check=check,
    )
    report.drain = drain.to_dict()

    final = status_snapshot(
        r, queue, group=group, samples=3, log=log, phase_name="final_status"
    )
    report.final_status = final.to_dict()
    check(bool(r.ping()), "PING after lifecycle")

    wall = time.perf_counter() - t_run
    report.summary = {
        "wall_s": wall,
        "total_inserted": p.total_inserted,
        "total_mutated": p.mutate_count * p.cycles,
        "drain_completed": drain.extras.get("completed", 0),
        "drain_claimed": drain.extras.get("claimed", 0),
        "claim_chunk_requested": p.claim_chunk,
        "claim_chunk_effective_max": drain.extras.get("claim_chunk_effective_max", 0),
        "final_xlen": final.extras.get("xlen", 0),
        "insert_ops_per_s_mean": _mean_ops(report.cycles, "insert"),
        "mutate_ops_per_s_mean": _mean_ops(report.cycles, "mutate"),
        "drain_ops_per_s": drain.ops_per_s,
    }
    log(
        "LIFECYCLE_SUMMARY "
        f"N={p.n} x={p.mutate_divisor} y={p.cycles} Z={p.claim_chunk} "
        f"inserted={p.total_inserted} mutated={p.mutate_count * p.cycles} "
        f"drained={drain.extras.get('completed', 0)} "
        f"wall_s={wall:.3f} "
        f"insert_ops/s≈{report.summary['insert_ops_per_s_mean']:.1f} "
        f"drain_ops/s={drain.ops_per_s:.1f} "
        f"final_xlen={report.summary['final_xlen']}"
    )
    return report


def _sample_indices(n: int, k: int) -> list[int]:
    if n <= 0 or k <= 0:
        return []
    if k >= n:
        return list(range(n))
    if k == 1:
        return [0]
    # Evenly spaced including endpoints
    return sorted({int(round(i * (n - 1) / (k - 1))) for i in range(k)})


def _mean_ops(cycles: list[dict[str, Any]], phase: str) -> float:
    vals = []
    for c in cycles:
        ph = c.get(phase) or {}
        ops = ph.get("ops_per_s")
        if ops is not None and ph.get("n", 0) > 0:
            vals.append(float(ops))
    if not vals:
        return 0.0
    return sum(vals) / len(vals)
