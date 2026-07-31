#!/usr/bin/env python3
"""Gate 1: insert N records for every public log × projection cell.

For each StorageConfig cell (log × projection):

  1. Start a fresh fireweed-service with that backend pair
  2. Time pipelined XADD of N work items (default 1_000_000)
  3. Verify with a simple RESP query that all N are present (XLEN + sample FW.HGETALL)
  4. Tear down and free the listen port

Evidence: target/python-resp-insert-matrix/<UTC>/summary.json

Examples:

  # full 1M matrix (needs postgres + minio for those cells)
  python run_insert_matrix.py --url-base redis://127.0.0.1:18080

  # local-only cells (memory/sqlite/filesystem × memory/sqlite)
  python run_insert_matrix.py --cells local

  # smoke
  INSERT_MATRIX_N=10000 python run_insert_matrix.py --cells local
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow `python run_insert_matrix.py` from this directory.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lib import resp as R  # noqa: E402

REPO = _ROOT.resolve().parents[1]
SERVICE_BIN = REPO / "target" / "release" / "fireweed-service"

LOGS = ("memory", "sqlite", "postgres", "filesystem", "s3")
PROJECTIONS = ("memory", "sqlite", "postgres")

# Cells that need only local disk / RAM.
LOCAL_CELLS = [
    (log, proj)
    for log in ("memory", "sqlite", "filesystem")
    for proj in ("memory", "sqlite")
]


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_cells(spec: str) -> list[tuple[str, str]]:
    if spec == "all":
        return [(log, proj) for log in LOGS for proj in PROJECTIONS]
    if spec == "local":
        return list(LOCAL_CELLS)
    cells: list[tuple[str, str]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "x" not in part and "*" not in part:
            raise SystemExit(f"bad cell {part!r}; want logxproj e.g. memoryxsqlite")
        sep = "x" if "x" in part else "*"
        log, proj = part.split(sep, 1)
        log, proj = log.strip(), proj.strip()
        if log not in LOGS or proj not in PROJECTIONS:
            raise SystemExit(f"unknown cell {part!r}; logs={LOGS} projections={PROJECTIONS}")
        cells.append((log, proj))
    if not cells:
        raise SystemExit("no cells selected")
    return cells


def _cell_dir(root: Path, log: str, proj: str) -> Path:
    d = root / f"{log}x{proj}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _build_env(
    *,
    log: str,
    proj: str,
    work: Path,
    listen: str,
    queue: str,
    pg_url: str | None,
    s3: dict[str, str] | None,
    run_id: str,
) -> dict[str, str]:
    env = os.environ.copy()
    # Isolate from ambient storage knobs that could pollute a cell.
    for k in list(env):
        if k.startswith("FIREWEED_") or k.startswith("DATABRICKS_"):
            if k in (
                "FIREWEED_RESP_URL",
                "FIREWEED_RESP_QUEUE",
            ):
                continue
            del env[k]

    env["FIREWEED_LISTEN_ADDR"] = listen
    env["FIREWEED_LOG_BACKEND"] = log
    env["FIREWEED_PROJECTION_BACKEND"] = proj
    env["FIREWEED_BOOTSTRAP_QUEUES"] = queue
    env["RUST_LOG"] = env.get("RUST_LOG", "warn")

    if log == "sqlite":
        env["FIREWEED_SQLITE_LOG_PATH"] = str(work / "log.db")
    if proj == "sqlite":
        env["FIREWEED_SQLITE_PROJECTION_PATH"] = str(work / "projection.db")
    if log == "filesystem":
        env["FIREWEED_OBJECT_LOG_ROOT"] = str(work / "object-log")
        (work / "object-log").mkdir(parents=True, exist_ok=True)

    if log == "postgres":
        if not pg_url:
            raise RuntimeError("postgres log requires --pg-url")
        # Prefer dedicated DB when operator passed a base URL; cell suffix via options.
        env["FIREWEED_PG_URL"] = _cell_pg_url(pg_url, f"fw_im_{run_id}_{log}_{proj}")
        env["FIREWEED_POSTGRES_LOG_DATABASE_URL"] = env["FIREWEED_PG_URL"]
    if proj == "postgres":
        if not pg_url:
            raise RuntimeError("postgres projection requires --pg-url")
        # Distinct DB when log is also postgres so axes do not share one catalog by accident.
        db_name = f"fw_im_{run_id}_{log}_{proj}"
        if log == "postgres":
            # Same DB is the product "postgres×postgres" relational pairing.
            env["FIREWEED_PG_PROJECTION_URL"] = env["FIREWEED_PG_URL"]
            env["FIREWEED_POSTGRES_PROJECTION_DATABASE_URL"] = env["FIREWEED_PG_URL"]
        else:
            env["FIREWEED_PG_PROJECTION_URL"] = _cell_pg_url(pg_url, db_name)
            env["FIREWEED_POSTGRES_PROJECTION_DATABASE_URL"] = env[
                "FIREWEED_PG_PROJECTION_URL"
            ]

    if log == "s3":
        if not s3:
            raise RuntimeError("s3 log requires S3 endpoint configuration")
        bucket = f"fw-im-{run_id}-{log}-{proj}".replace("_", "-")[:63]
        env["FIREWEED_OBJECT_LOG_S3_ENDPOINT"] = s3["endpoint"]
        env["FIREWEED_OBJECT_LOG_S3_BUCKET"] = bucket
        env["FIREWEED_OBJECT_LOG_S3_REGION"] = s3.get("region", "us-east-1")
        env["FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE"] = "static"
        env["FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID"] = s3["access_key"]
        env["FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY"] = s3["secret_key"]
        env["FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP"] = (
            "true" if s3.get("insecure_http", "true") in ("1", "true", True) else "false"
        )
        env["_CELL_S3_BUCKET"] = bucket  # runner bookkeeping (not read by service)

    return env


def _cell_pg_url(base: str, db_name: str) -> str:
    """Replace the path database name in a libpq URL."""
    # postgres://user:pass@host:port/dbname
    if "://" not in base:
        return base
    scheme, rest = base.split("://", 1)
    if "/" in rest:
        hostpart, _old = rest.split("/", 1)
        # strip query
        return f"{scheme}://{hostpart}/{db_name}"
    return f"{base.rstrip('/')}/{db_name}"


def _ensure_pg_database(base_url: str, db_name: str) -> None:
    """CREATE DATABASE if missing (connects to 'postgres' maintenance DB)."""
    maint = _cell_pg_url(base_url, "postgres")
    try:
        import psycopg2  # type: ignore
    except ImportError:
        # Fall back to psql CLI
        env = os.environ.copy()
        # parse password from URL if present
        subprocess.run(
            [
                "psql",
                maint,
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"SELECT 1 FROM pg_database WHERE datname='{db_name}'",
            ],
            check=False,
            capture_output=True,
        )
        exists = subprocess.run(
            [
                "psql",
                maint,
                "-tAc",
                f"SELECT 1 FROM pg_database WHERE datname='{db_name}'",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if exists.stdout.strip() == "1":
            return
        r = subprocess.run(
            ["psql", maint, "-v", "ON_ERROR_STOP=1", "-c", f'CREATE DATABASE "{db_name}"'],
            check=False,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0 and "already exists" not in (r.stderr or ""):
            raise RuntimeError(f"CREATE DATABASE {db_name} failed: {r.stderr}")
        return

    conn = psycopg2.connect(maint)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()


def _ensure_s3_bucket(s3: dict[str, str], bucket: str) -> None:
    """Best-effort bucket create via docker exec mc (MinIO) or skip if already there."""
    # Prefer docker exec on known container if set.
    container = s3.get("mc_container")
    if container:
        endpoint_in = s3.get("mc_endpoint_in_container", "http://127.0.0.1:9000")
        cmd = (
            f"mc alias set local {endpoint_in} {s3['access_key']} {s3['secret_key']} >/dev/null "
            f"&& mc mb -p local/{bucket} 2>/dev/null || true"
        )
        subprocess.run(
            ["docker", "exec", container, "sh", "-c", cmd],
            check=False,
            capture_output=True,
        )
        return
    # Fallback: aws cli if present
    if shutil.which("aws"):
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = s3["access_key"]
        env["AWS_SECRET_ACCESS_KEY"] = s3["secret_key"]
        subprocess.run(
            [
                "aws",
                "--endpoint-url",
                s3["endpoint"],
                "s3",
                "mb",
                f"s3://{bucket}",
            ],
            check=False,
            capture_output=True,
            env=env,
        )


def _start_service(env: dict[str, str], log_path: Path) -> subprocess.Popen:
    if not SERVICE_BIN.is_file():
        raise SystemExit(
            f"missing {SERVICE_BIN}; build with:\n"
            "  cargo build -p fireweed-server --release --bin fireweed-service\n"
            "(default features include the full public log×projection matrix)"
        )
    log_f = log_path.open("w", encoding="utf-8")
    # Strip runner-only keys
    child_env = {k: v for k, v in env.items() if not k.startswith("_CELL_")}
    proc = subprocess.Popen(
        [str(SERVICE_BIN)],
        cwd=str(REPO),
        env=child_env,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    proc._log_f = log_f  # type: ignore[attr-defined]
    return proc


def _stop_service(proc: subprocess.Popen | None) -> None:
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
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            proc.kill()
        proc.wait(timeout=5)
    log_f = getattr(proc, "_log_f", None)
    if log_f is not None:
        try:
            log_f.close()
        except Exception:  # noqa: BLE001
            pass


def _wait_ready(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.perf_counter() + timeout_s
    last_err: Exception | None = None
    while time.perf_counter() < deadline:
        try:
            r = R.connect(url)
            r.ping()
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(0.25)
    raise RuntimeError(f"service not ready at {url}: {last_err}")


def _percentile(samples: list[float], p: float) -> float:
    return float(R.percentile(samples, p))


def insert_and_verify(
    *,
    url: str,
    queue: str,
    n: int,
    pipeline: int,
    run_id: str,
    progress_every: int,
) -> dict[str, Any]:
    r = R.connect(url)
    r.ping()
    before = int(r.xlen(queue))
    base = R.now_ms() - n - 1
    key_prefix = f"{run_id}-p"

    ids: list[str] = []
    batch: list[R.WorkItem] = []
    batch_ms: list[float] = []
    t0 = time.perf_counter()
    for i in range(n):
        prio = base + i
        batch.append(
            R.WorkItem(
                client_item_key=f"{key_prefix}{i}",
                priority=prio,
                not_before=prio,
                payload="x",
            )
        )
        if len(batch) >= pipeline:
            bt0 = time.perf_counter()
            ids.extend(R.pipeline_xadd(r, queue, batch, batch_size=pipeline))
            batch_ms.append((time.perf_counter() - bt0) * 1000.0)
            batch.clear()
            if progress_every and len(ids) % progress_every == 0:
                print(f"    progress inserted={len(ids)}", flush=True)
    if batch:
        bt0 = time.perf_counter()
        ids.extend(R.pipeline_xadd(r, queue, batch, batch_size=pipeline))
        batch_ms.append((time.perf_counter() - bt0) * 1000.0)
    wall = time.perf_counter() - t0

    # --- simple verification query ---
    tq0 = time.perf_counter()
    after = int(r.xlen(queue))
    status = R.queue_status(r, queue)
    sample_idx = sorted({0, n // 4, n // 2, (3 * n) // 4, max(0, n - 1)}) if n else []
    hgetall_ok = 0
    hgetall_ms: list[float] = []
    for i in sample_idx:
        key = f"{key_prefix}{i}"
        ht0 = time.perf_counter()
        fields = R.fw_hgetall(r, queue, key)
        hgetall_ms.append((time.perf_counter() - ht0) * 1000.0)
        if fields.get("client_item_key") == key or bool(fields):
            hgetall_ok += 1
    verify_wall = time.perf_counter() - tq0

    ids_ok = len(ids) == n
    xlen_ok = after >= before + n
    # Fresh queue expectation: exact N when before==0
    exact_ok = (before == 0 and after == n) or (before > 0 and after >= before + n)
    samples_ok = hgetall_ok == len(sample_idx) if sample_idx else True
    verified = ids_ok and xlen_ok and exact_ok and samples_ok

    ops = n / wall if wall > 0 else 0.0
    result = {
        "n": n,
        "pipeline": pipeline,
        "wall_s": wall,
        "ops_per_s": ops,
        "xadd_batch_p50_ms": _percentile(batch_ms, 50),
        "xadd_batch_p95_ms": _percentile(batch_ms, 95),
        "xadd_batch_p99_ms": _percentile(batch_ms, 99),
        "xlen_before": before,
        "xlen_after": after,
        "ids_returned": len(ids),
        "verify": {
            "wall_s": verify_wall,
            "xlen": after,
            "xlen_delta": after - before,
            "expected_delta": n,
            "ids_ok": ids_ok,
            "xlen_ok": xlen_ok,
            "exact_ok": exact_ok,
            "hgetall_samples": len(sample_idx),
            "hgetall_ok": hgetall_ok,
            "hgetall_p95_ms": _percentile(hgetall_ms, 95),
            "queue_status_xinfo": status.get("xinfo"),
            "verified": verified,
        },
    }
    return result


def run_cell(
    *,
    log: str,
    proj: str,
    n: int,
    pipeline: int,
    listen: str,
    url: str,
    queue: str,
    evidence_root: Path,
    pg_url: str | None,
    s3: dict[str, str] | None,
    run_id: str,
    progress_every: int,
    ready_timeout_s: float,
) -> dict[str, Any]:
    cell = f"{log}x{proj}"
    work = _cell_dir(evidence_root / "cells", log, proj)
    svc_log = work / "service.log"
    print(f"\n== CELL {cell} N={n} ==", flush=True)

    record: dict[str, Any] = {
        "cell": cell,
        "log": log,
        "projection": proj,
        "n": n,
        "status": "fail",
        "error": None,
    }
    proc: subprocess.Popen | None = None
    t_cell = time.perf_counter()
    try:
        env = _build_env(
            log=log,
            proj=proj,
            work=work,
            listen=listen,
            queue=queue,
            pg_url=pg_url,
            s3=s3,
            run_id=run_id.replace("-", "")[:8],
        )
        record["env_snapshot"] = {
            k: ("***" if "SECRET" in k or "PASSWORD" in k or "KEY" in k else v)
            for k, v in env.items()
            if k.startswith("FIREWEED_") or k.startswith("_CELL_")
        }

        # Provision external resources
        if log == "postgres" or proj == "postgres":
            if not pg_url:
                raise RuntimeError("postgres cell but no --pg-url")
            # DB names embedded in env URLs
            for key in (
                "FIREWEED_PG_URL",
                "FIREWEED_PG_PROJECTION_URL",
                "FIREWEED_POSTGRES_LOG_DATABASE_URL",
                "FIREWEED_POSTGRES_PROJECTION_DATABASE_URL",
            ):
                u = env.get(key)
                if u and "/" in u:
                    db = u.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
                    if db and db != "postgres":
                        print(f"  ensure db {db}", flush=True)
                        _ensure_pg_database(pg_url, db)
        if log == "s3":
            bucket = env.get("_CELL_S3_BUCKET") or env["FIREWEED_OBJECT_LOG_S3_BUCKET"]
            print(f"  ensure s3 bucket {bucket}", flush=True)
            assert s3 is not None
            _ensure_s3_bucket(s3, bucket)

        print(f"  starting service listen={listen}", flush=True)
        proc = _start_service(env, svc_log)
        _wait_ready(url, timeout_s=ready_timeout_s)
        # Confirm bootstrap queue addressable
        rr = R.connect(url)
        _ = int(rr.xlen(queue))

        print(f"  inserting N={n} pipeline={pipeline}", flush=True)
        metrics = insert_and_verify(
            url=url,
            queue=queue,
            n=n,
            pipeline=pipeline,
            run_id=f"{run_id}-{cell}",
            progress_every=progress_every,
        )
        record["insert"] = metrics
        v = metrics["verify"]
        print(
            f"  INSERT wall_s={metrics['wall_s']:.3f} ops_per_s={metrics['ops_per_s']:.1f} "
            f"xlen={metrics['xlen_after']} verified={v['verified']}",
            flush=True,
        )
        if not v["verified"]:
            record["status"] = "fail"
            record["error"] = (
                f"verify failed: ids_ok={v['ids_ok']} xlen_ok={v['xlen_ok']} "
                f"exact_ok={v['exact_ok']} hgetall={v['hgetall_ok']}/{v['hgetall_samples']}"
            )
            print(f"  FAIL: {record['error']}", flush=True)
        else:
            record["status"] = "pass"
            print("  PASS", flush=True)
    except Exception as exc:  # noqa: BLE001
        record["status"] = "fail"
        record["error"] = f"{type(exc).__name__}: {exc}"
        print(f"  FAIL: {record['error']}", flush=True)
        # Tail service log for diagnosis
        if svc_log.is_file():
            try:
                tail = svc_log.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]
                record["service_log_tail"] = tail
                print("  --- service log tail ---", flush=True)
                for line in tail:
                    print(f"  | {line}", flush=True)
            except Exception:  # noqa: BLE001
                pass
    finally:
        _stop_service(proc)
        # Brief pause so the port is released before the next cell.
        time.sleep(0.5)

    record["cell_wall_s"] = time.perf_counter() - t_cell
    (work / "result.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return record


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--n",
        type=int,
        default=int(os.environ.get("INSERT_MATRIX_N", "1000000")),
        help="Insert volume (default INSERT_MATRIX_N or 1000000)",
    )
    p.add_argument(
        "--pipeline",
        type=int,
        default=int(os.environ.get("INSERT_MATRIX_PIPELINE", "1000")),
    )
    p.add_argument(
        "--cells",
        default=os.environ.get("INSERT_MATRIX_CELLS", "all"),
        help="all | local | comma list of logxproj (e.g. memoryxmemory,filesystemxsqlite)",
    )
    p.add_argument("--listen", default=os.environ.get("FIREWEED_LISTEN_ADDR", "127.0.0.1:18080"))
    p.add_argument(
        "--url",
        default=None,
        help="RESP URL (default redis://<listen>)",
    )
    p.add_argument("--queue", default=os.environ.get("FIREWEED_RESP_QUEUE", "matrix:insert"))
    p.add_argument(
        "--pg-url",
        default=os.environ.get(
            "FIREWEED_PG_URL",
            os.environ.get(
                "INSERT_MATRIX_PG_URL",
                "postgres://postgres:postgres@127.0.0.1:33120/postgres",
            ),
        ),
    )
    p.add_argument(
        "--s3-endpoint",
        default=os.environ.get(
            "FIREWEED_OBJECT_LOG_S3_ENDPOINT",
            os.environ.get("INSERT_MATRIX_S3_ENDPOINT", "http://127.0.0.1:19000"),
        ),
    )
    p.add_argument(
        "--s3-access-key",
        default=os.environ.get("INSERT_MATRIX_S3_ACCESS_KEY", "minioadmin"),
    )
    p.add_argument(
        "--s3-secret-key",
        default=os.environ.get("INSERT_MATRIX_S3_SECRET_KEY", "minioadmin"),
    )
    p.add_argument(
        "--s3-region",
        default=os.environ.get("INSERT_MATRIX_S3_REGION", "us-east-1"),
    )
    p.add_argument(
        "--s3-mc-container",
        default=os.environ.get("INSERT_MATRIX_S3_MC_CONTAINER", "fireweed-e3-minio"),
        help="Docker container with mc for bucket create (empty to skip)",
    )
    p.add_argument(
        "--evidence-dir",
        default=None,
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=int(os.environ.get("INSERT_MATRIX_PROGRESS", "100000")),
    )
    p.add_argument("--ready-timeout", type=float, default=90.0)
    args = p.parse_args(argv)

    cells = _parse_cells(args.cells)
    url = args.url or f"redis://{args.listen}"
    run_id = uuid.uuid4().hex[:10]
    ts = _utc()
    evidence = (
        Path(args.evidence_dir)
        if args.evidence_dir
        else REPO / "target" / "python-resp-insert-matrix" / ts
    )
    evidence = evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)

    s3 = {
        "endpoint": args.s3_endpoint,
        "access_key": args.s3_access_key,
        "secret_key": args.s3_secret_key,
        "region": args.s3_region,
        "insecure_http": "true",
        "mc_container": args.s3_mc_container or "",
        "mc_endpoint_in_container": "http://127.0.0.1:9000",
    }

    print(
        f"INSERT MATRIX run_id={run_id} N={args.n} cells={len(cells)} "
        f"listen={args.listen} evidence={evidence}",
        flush=True,
    )
    for log, proj in cells:
        print(f"  - {log}x{proj}", flush=True)

    # Ensure no stale listener
    subprocess.run(["pkill", "-f", "target/release/fireweed-service"], check=False)
    time.sleep(0.5)

    results: list[dict[str, Any]] = []
    t0 = time.perf_counter()
    for log, proj in cells:
        # Skip postgres/s3 early if deps clearly unusable? Let run_cell report fail.
        needs_pg = log == "postgres" or proj == "postgres"
        needs_s3 = log == "s3"
        if needs_pg and not args.pg_url:
            results.append(
                {
                    "cell": f"{log}x{proj}",
                    "log": log,
                    "projection": proj,
                    "status": "skip",
                    "error": "no --pg-url",
                }
            )
            continue
        if needs_s3 and not args.s3_endpoint:
            results.append(
                {
                    "cell": f"{log}x{proj}",
                    "log": log,
                    "projection": proj,
                    "status": "skip",
                    "error": "no --s3-endpoint",
                }
            )
            continue

        rec = run_cell(
            log=log,
            proj=proj,
            n=args.n,
            pipeline=args.pipeline,
            listen=args.listen,
            url=url,
            queue=args.queue,
            evidence_root=evidence,
            pg_url=args.pg_url if needs_pg or True else None,
            s3=s3 if needs_s3 or True else None,
            run_id=run_id,
            progress_every=args.progress_every,
            ready_timeout_s=args.ready_timeout,
        )
        results.append(rec)
        # Incremental summary so a long run still leaves evidence if interrupted
        _write_summary(evidence, run_id, args, results, time.perf_counter() - t0)

    wall = time.perf_counter() - t0
    summary = _write_summary(evidence, run_id, args, results, wall)
    _print_table(results)
    print(f"\nevidence: {evidence}", flush=True)
    print(
        f"passed={summary['passed']} failed={summary['failed']} skipped={summary['skipped']} "
        f"wall_s={wall:.1f}",
        flush=True,
    )
    return 0 if summary["failed"] == 0 else 1


def _write_summary(
    evidence: Path,
    run_id: str,
    args: argparse.Namespace,
    results: list[dict[str, Any]],
    wall: float,
) -> dict[str, Any]:
    passed = sum(1 for r in results if r.get("status") == "pass")
    failed = sum(1 for r in results if r.get("status") == "fail")
    skipped = sum(1 for r in results if r.get("status") == "skip")
    rows = []
    for r in results:
        ins = r.get("insert") or {}
        ver = (ins.get("verify") or {}) if ins else {}
        rows.append(
            {
                "cell": r.get("cell"),
                "status": r.get("status"),
                "wall_s": ins.get("wall_s"),
                "ops_per_s": ins.get("ops_per_s"),
                "xlen_after": ins.get("xlen_after"),
                "verified": ver.get("verified"),
                "error": r.get("error"),
            }
        )
    summary = {
        "run_id": run_id,
        "n": args.n,
        "pipeline": args.pipeline,
        "listen": args.listen,
        "queue": args.queue,
        "wall_s": wall,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "rows": rows,
        "results": results,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    (evidence / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    # Compact markdown table
    lines = [
        "# Insert matrix gate (1M records)",
        "",
        f"- run_id: `{run_id}`",
        f"- N: **{args.n}**",
        f"- pipeline: {args.pipeline}",
        "",
        "| cell | status | wall_s | ops/s | xlen | verified |",
        "|------|--------|--------|------:|-----:|----------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['cell']} | {row['status']} | "
            f"{_fmt(row['wall_s'])} | {_fmt(row['ops_per_s'], 1)} | "
            f"{row['xlen_after'] if row['xlen_after'] is not None else '—'} | "
            f"{row['verified']} |"
        )
    (evidence / "SUMMARY.md").write_text("\n".join(lines) + "\n")
    return summary


def _fmt(v: Any, digits: int = 3) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return str(v)


def _print_table(results: list[dict[str, Any]]) -> None:
    print("\n=== INSERT MATRIX RESULTS ===", flush=True)
    print(
        f"{'cell':<22} {'status':<7} {'wall_s':>10} {'ops/s':>12} {'xlen':>10} verified",
        flush=True,
    )
    for r in results:
        ins = r.get("insert") or {}
        ver = ins.get("verify") or {}
        print(
            f"{r.get('cell', '?'):<22} {r.get('status', '?'):<7} "
            f"{_fmt(ins.get('wall_s')):>10} {_fmt(ins.get('ops_per_s'), 1):>12} "
            f"{str(ins.get('xlen_after', '—')):>10} {ver.get('verified')}",
            flush=True,
        )


if __name__ == "__main__":
    raise SystemExit(main())
