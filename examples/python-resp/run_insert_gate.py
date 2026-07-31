#!/usr/bin/env python3
"""Focused insert gate: one log×projection cell, N records, verify, report vs goals.

Default cell: filesystem log × sqlite projection (object log on --object-log-root).

Goals (product intent / RESP capacity bars — host-bound, not release SLA):
  insert ops/s  >= 10_000   for N in {1e6, 1e7}
  bulk floor    >= 1_000    (hard fail floor)
  verify        XLEN == N and sample FW.HGETALL succeeds

Example:
  python run_insert_gate.py \\
    --n 1000000 \\
    --log filesystem --projection sqlite \\
    --object-log-root /tank/home/erik/fireweed-olog \\
    --projection-path /tmp/fw-proj.db
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lib import resp as R  # noqa: E402

REPO = _ROOT.resolve().parents[1]
SERVICE_BIN = REPO / "target" / "release" / "fireweed-service"

# Product capacity intent (conversation / SS profile). Host-bound observations.
GOAL_INSERT_OPS = 10_000.0
GOAL_BULK_FLOOR = 1_000.0


def _stop(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            proc.kill()
        proc.wait(timeout=5)


def _wait_ready(url: str, timeout_s: float = 90.0) -> None:
    deadline = time.perf_counter() + timeout_s
    last: Exception | None = None
    while time.perf_counter() < deadline:
        try:
            r = R.connect(url)
            r.ping()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.25)
    raise RuntimeError(f"service not ready: {last}")


def insert_verify(
    *,
    url: str,
    queue: str,
    n: int,
    pipeline: int,
    key_prefix: str,
    progress_every: int,
    append_only: bool = False,
) -> dict[str, Any]:
    r = R.connect(url)
    before = int(r.xlen(queue))
    base = R.now_ms() - n - 1
    ids: list[str] = []
    batch_ms: list[float] = []
    t0 = time.perf_counter()
    # WorkItem path (upsert keys) vs raw field maps (append-only PushPort).
    batch_items: list[R.WorkItem] = []
    batch_fields: list[dict[str, str]] = []
    for i in range(n):
        prio = base + i
        if append_only:
            batch_fields.append(
                {
                    "priority": str(prio),
                    "not_before": str(prio),
                    "payload": "x",
                }
            )
            if len(batch_fields) >= pipeline:
                bt0 = time.perf_counter()
                pipe = r.pipeline(transaction=False)
                for fields in batch_fields:
                    pipe.xadd(queue, fields)
                ids.extend(pipe.execute())
                batch_ms.append((time.perf_counter() - bt0) * 1000.0)
                batch_fields.clear()
                if progress_every and len(ids) % progress_every == 0:
                    elapsed = time.perf_counter() - t0
                    rate = len(ids) / elapsed if elapsed else 0.0
                    print(
                        f"  progress inserted={len(ids)} wall_s={elapsed:.1f} "
                        f"ops_per_s≈{rate:.0f}",
                        flush=True,
                    )
        else:
            batch_items.append(
                R.WorkItem(
                    client_item_key=f"{key_prefix}{i}",
                    priority=prio,
                    not_before=prio,
                    payload="x",
                )
            )
            if len(batch_items) >= pipeline:
                bt0 = time.perf_counter()
                ids.extend(R.pipeline_xadd(r, queue, batch_items, batch_size=pipeline))
                batch_ms.append((time.perf_counter() - bt0) * 1000.0)
                batch_items.clear()
                if progress_every and len(ids) % progress_every == 0:
                    elapsed = time.perf_counter() - t0
                    rate = len(ids) / elapsed if elapsed else 0.0
                    print(
                        f"  progress inserted={len(ids)} wall_s={elapsed:.1f} "
                        f"ops_per_s≈{rate:.0f}",
                        flush=True,
                    )
    if batch_fields:
        bt0 = time.perf_counter()
        pipe = r.pipeline(transaction=False)
        for fields in batch_fields:
            pipe.xadd(queue, fields)
        ids.extend(pipe.execute())
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    if batch_items:
        bt0 = time.perf_counter()
        ids.extend(R.pipeline_xadd(r, queue, batch_items, batch_size=pipeline))
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    wall = time.perf_counter() - t0
    ops = n / wall if wall > 0 else 0.0

    tq0 = time.perf_counter()
    after = int(r.xlen(queue))
    status = R.queue_status(r, queue)
    sample_idx = sorted({0, n // 4, n // 2, (3 * n) // 4, max(0, n - 1)}) if n else []
    h_ok = 0
    h_ms: list[float] = []
    if not append_only:
        for i in sample_idx:
            key = f"{key_prefix}{i}"
            ht0 = time.perf_counter()
            fields = R.fw_hgetall(r, queue, key)
            h_ms.append((time.perf_counter() - ht0) * 1000.0)
            if fields.get("client_item_key") == key or bool(fields):
                h_ok += 1
    else:
        # Point-read by client key is N/A; sample claim proves live work exists.
        claim_t0 = time.perf_counter()
        claimed = R.claim_batch(r, queue, count=min(5, n) if n else 0)
        h_ms.append((time.perf_counter() - claim_t0) * 1000.0)
        h_ok = len(claimed)
        sample_idx = list(range(h_ok))
    verify_s = time.perf_counter() - tq0

    verified = len(ids) == n and after == before + n and (h_ok > 0 if n > 0 else True)
    return {
        "n": n,
        "pipeline": pipeline,
        "append_only": append_only,
        "wall_s": wall,
        "ops_per_s": ops,
        "xadd_batch_p50_ms": float(R.percentile(batch_ms, 50)),
        "xadd_batch_p95_ms": float(R.percentile(batch_ms, 95)),
        "xadd_batch_p99_ms": float(R.percentile(batch_ms, 99)),
        "xlen_before": before,
        "xlen_after": after,
        "ids_returned": len(ids),
        "verify": {
            "wall_s": verify_s,
            "xlen": after,
            "expected": before + n,
            "hgetall_ok": h_ok if not append_only else None,
            "hgetall_samples": len(sample_idx) if not append_only else None,
            "hgetall_p95_ms": float(R.percentile(h_ms, 95)) if not append_only else None,
            "claim_sample": h_ok if append_only else None,
            "queue_status_xinfo": status.get("xinfo"),
            "verified": verified,
        },
        "goals": {
            "insert_ops_target": GOAL_INSERT_OPS,
            "bulk_floor": GOAL_BULK_FLOOR,
            "insert_ops_met": ops >= GOAL_INSERT_OPS,
            "bulk_floor_met": ops >= GOAL_BULK_FLOOR,
        },
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--n", type=int, required=True)
    p.add_argument("--pipeline", type=int, default=1000)
    p.add_argument("--log", default="filesystem")
    p.add_argument("--projection", default="sqlite")
    p.add_argument("--listen", default="127.0.0.1:18080")
    p.add_argument("--queue", default="gate:insert")
    p.add_argument("--object-log-root", required=True, help="Directory for filesystem object log")
    p.add_argument("--projection-path", default=None, help="SQLite projection db path")
    p.add_argument("--sqlite-log-path", default=None, help="Only when --log=sqlite")
    p.add_argument("--evidence-dir", default=None)
    p.add_argument("--progress-every", type=int, default=100_000)
    p.add_argument(
        "--append-only",
        action="store_true",
        help=(
            "Omit client_item_key (PushPort append only). Default uses client_item_key "
            "upsert (insert + pending replace) — required for SS-style mutate."
        ),
    )
    p.add_argument(
        "--segment-target-bytes",
        type=int,
        default=int(os.environ.get("FIREWEED_SEGMENT_TARGET_BYTES", "262144")),
    )
    p.add_argument(
        "--segment-max-latency-ms",
        type=int,
        default=int(os.environ.get("FIREWEED_SEGMENT_MAX_LATENCY_MS", "20")),
    )
    args = p.parse_args(argv)

    url = f"redis://{args.listen}"
    run_id = uuid.uuid4().hex[:10]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    evidence = Path(
        args.evidence_dir
        or (REPO / "target" / "python-resp-insert-gate" / f"{ts}-{args.log}x{args.projection}-n{args.n}")
    )
    evidence.mkdir(parents=True, exist_ok=True)

    olog = Path(args.object_log_root)
    olog.mkdir(parents=True, exist_ok=True)
    proj_path = Path(
        args.projection_path
        or (evidence / "projection.db")
    )
    proj_path.parent.mkdir(parents=True, exist_ok=True)

    if not SERVICE_BIN.is_file():
        raise SystemExit(f"missing {SERVICE_BIN}; cargo build -p fireweed-server --release --bin fireweed-service")

    env = os.environ.copy()
    # Drop ambient FIREWEED_* so only this cell's knobs apply.
    for k in list(env):
        if k.startswith("FIREWEED_") or k.startswith("DATABRICKS_"):
            del env[k]
    env.update(
        {
            "FIREWEED_LISTEN_ADDR": args.listen,
            "FIREWEED_LOG_BACKEND": args.log,
            "FIREWEED_PROJECTION_BACKEND": args.projection,
            "FIREWEED_BOOTSTRAP_QUEUES": args.queue,
            "FIREWEED_SEGMENT_TARGET_BYTES": str(args.segment_target_bytes),
            "FIREWEED_SEGMENT_MAX_LATENCY_MS": str(args.segment_max_latency_ms),
            "RUST_LOG": os.environ.get("RUST_LOG", "warn"),
        }
    )
    if args.log == "filesystem":
        env["FIREWEED_OBJECT_LOG_ROOT"] = str(olog)
    if args.log == "sqlite":
        env["FIREWEED_SQLITE_LOG_PATH"] = str(
            args.sqlite_log_path or (evidence / "log.db")
        )
    if args.projection == "sqlite":
        env["FIREWEED_SQLITE_PROJECTION_PATH"] = str(proj_path)

    svc_log = evidence / "service.log"
    print(
        f"INSERT GATE cell={args.log}x{args.projection} N={args.n} "
        f"pipeline={args.pipeline} listen={args.listen}\n"
        f"  object_log_root={olog}\n"
        f"  projection_path={proj_path}\n"
        f"  evidence={evidence}\n"
        f"  goals: insert>={GOAL_INSERT_OPS:.0f}/s bulk_floor>={GOAL_BULK_FLOOR:.0f}/s",
        flush=True,
    )

    log_f = svc_log.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [str(SERVICE_BIN)],
        cwd=str(REPO),
        env=env,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    record: dict[str, Any] = {
        "cell": f"{args.log}x{args.projection}",
        "n": args.n,
        "run_id": run_id,
        "object_log_root": str(olog),
        "projection_path": str(proj_path),
        "segment_target_bytes": args.segment_target_bytes,
        "segment_max_latency_ms": args.segment_max_latency_ms,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        _wait_ready(url)
        print("  service ready", flush=True)
        metrics = insert_verify(
            url=url,
            queue=args.queue,
            n=args.n,
            pipeline=args.pipeline,
            key_prefix=f"{run_id}-",
            progress_every=args.progress_every,
            append_only=args.append_only,
        )
        record["insert"] = metrics
        g = metrics["goals"]
        v = metrics["verify"]
        print(
            f"\n  RESULT wall_s={metrics['wall_s']:.3f} ops_per_s={metrics['ops_per_s']:.1f}\n"
            f"  xadd_p50/p95/p99_ms="
            f"{metrics['xadd_batch_p50_ms']:.1f}/"
            f"{metrics['xadd_batch_p95_ms']:.1f}/"
            f"{metrics['xadd_batch_p99_ms']:.1f}\n"
            f"  xlen={metrics['xlen_after']} verified={v['verified']} "
            f"hgetall_p95_ms={v['hgetall_p95_ms']:.2f}\n"
            f"  GOAL insert>={GOAL_INSERT_OPS:.0f}/s: "
            f"{'MET' if g['insert_ops_met'] else 'MISS'} "
            f"(bulk_floor {'MET' if g['bulk_floor_met'] else 'MISS'})",
            flush=True,
        )
        if not v["verified"]:
            record["status"] = "fail"
            record["error"] = "verify failed"
        elif not g["bulk_floor_met"]:
            record["status"] = "fail"
            record["error"] = f"below bulk floor {GOAL_BULK_FLOOR}/s"
        elif not g["insert_ops_met"]:
            record["status"] = "gap"  # correctness OK, capacity goal missed
            record["error"] = f"below insert goal {GOAL_INSERT_OPS}/s"
        else:
            record["status"] = "pass"
    except Exception as exc:  # noqa: BLE001
        record["status"] = "fail"
        record["error"] = f"{type(exc).__name__}: {exc}"
        print(f"  FAIL: {record['error']}", flush=True)
        try:
            tail = svc_log.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]
            record["service_log_tail"] = tail
            for line in tail:
                print(f"  | {line}", flush=True)
        except Exception:  # noqa: BLE001
            pass
    finally:
        _stop(proc)
        log_f.close()

    record["finished_at"] = datetime.now(timezone.utc).isoformat()
    (evidence / "result.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    print(f"  status={record['status']} evidence={evidence}", flush=True)
    # exit 0 for pass or gap (capacity miss still produces evidence); 1 only on hard fail
    return 0 if record.get("status") in ("pass", "gap") else 1


if __name__ == "__main__":
    raise SystemExit(main())
