from __future__ import annotations

import importlib
import json
import platform
import time
import traceback
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from harness.capture import capture_transcript
from harness.context import ScenarioContext
from harness.result import ScenarioResult
from lib import resp as R

# Functional suite (default)
FUNCTIONAL_SCENARIOS = [
    "scenarios.01_connect",
    "scenarios.02_batch_insert",
    "scenarios.03_batch_update_pending",
    "scenarios.04_claim_before_due",
    "scenarios.05_claim_due_batch",
    "scenarios.06_complete_and_status",
    "scenarios.07_idempotent_upsert",
    "scenarios.08_lease_renew",
]

FUNCTIONAL_FULL_EXTRA = [
    "scenarios.09_lease_reclaim",
]

PERF_SCENARIOS = [
    "scenarios.perf.P01_insert_1m",
    "scenarios.perf.P02_update_half",
    "scenarios.perf.P03_claim_complete_chunks",
    "scenarios.perf.P04_status_under_load",
    "scenarios.perf.P05_end_to_end_pipeline",
]

# Seventh Sense multi-queue black box (docs/perf/workload-seventh-sense-actions-scheduler.md)
# SS02 is the parameterized lifecycle framework demo (N, N/x, y, Z).
SS_SCENARIOS = [
    "scenarios.ss.SS01_black_box",
    "scenarios.ss.SS02_lifecycle",
]


def _load(module_path: str) -> Any:
    return importlib.import_module(module_path)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_suite(
    *,
    url: str,
    queue: str,
    suite: str,
    scenario_filter: str | None,
    full: bool,
    evidence_root: Path | None,
    perf_n: int,
    perf_pipeline: int,
    perf_claim_count: int,
) -> int:
    if suite == "functional":
        modules = list(FUNCTIONAL_SCENARIOS)
        if full:
            modules.extend(FUNCTIONAL_FULL_EXTRA)
        kind = "e2e"
    elif suite == "perf":
        modules = list(PERF_SCENARIOS)
        kind = "perf"
    elif suite == "ss":
        modules = list(SS_SCENARIOS)
        kind = "ss"
    elif suite == "all":
        modules = list(FUNCTIONAL_SCENARIOS)
        if full:
            modules.extend(FUNCTIONAL_FULL_EXTRA)
        modules.extend(PERF_SCENARIOS)
        modules.extend(SS_SCENARIOS)
        kind = "all"
    else:
        raise SystemExit(f"unknown suite: {suite}")

    if scenario_filter:
        modules = [m for m in modules if scenario_filter in m]
        if not modules:
            raise SystemExit(f"no scenario matching {scenario_filter!r}")

    run_id = uuid.uuid4().hex[:12]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = _repo_root()
    if evidence_root is None:
        evidence_root = root / "target" / f"python-resp-{kind}" / ts
    evidence_root = evidence_root.resolve()
    evidence_root.mkdir(parents=True, exist_ok=True)
    scen_dir = evidence_root / "scenarios"
    scen_dir.mkdir(parents=True, exist_ok=True)

    try:
        r = R.connect(url)
        r.ping()
    except Exception as exc:  # noqa: BLE001
        print(
            f"FAIL: cannot connect to {url}: {exc}\n"
            "Start the service, e.g.:\n"
            "  ./examples/python-resp/scripts/start_dev_service.sh",
            flush=True,
        )
        return 2

    summary: dict[str, Any] = {
        "suite": suite,
        "run_id": run_id,
        "url": url,
        "queue": queue,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "perf_n": perf_n if suite in ("perf", "all") else None,
        "results": [],
    }

    jsonl_path = evidence_root / "all.jsonl"
    failed = 0
    skipped = 0
    passed = 0

    with jsonl_path.open("w", encoding="utf-8") as jsonl:
        for mod_path in modules:
            mod = _load(mod_path)
            sid = getattr(mod, "SCENARIO_ID", mod_path.rsplit(".", 1)[-1])
            title = getattr(mod, "TITLE", sid)
            tags = list(getattr(mod, "TAGS", ()))
            run_fn: Callable[[ScenarioContext], ScenarioResult] = mod.run

            print(f"\n== {sid}: {title} ==", flush=True)
            log_path = scen_dir / f"{sid}.log"
            json_path = scen_dir / f"{sid}.json"
            ctx = ScenarioContext(
                redis=r,
                queue=queue,
                run_id=f"{run_id}-{sid}",
                evidence_dir=str(evidence_root),
                full=full,
                perf_n=perf_n,
                perf_pipeline=perf_pipeline,
                perf_claim_count=perf_claim_count,
            )

            t0 = time.perf_counter()
            status = "fail"
            error: str | None = None
            details: dict[str, Any] = {}
            with capture_transcript(log_path):
                print(f"INTENT module={mod_path}", flush=True)
                try:
                    result = run_fn(ctx)
                    status = result.status
                    details = result.details
                    error = result.error
                    if status == "fail":
                        print(f"FAIL: {error}", flush=True)
                    elif status == "skip":
                        print(f"SKIP: {error}", flush=True)
                    else:
                        print("PASS", flush=True)
                except Exception as exc:  # noqa: BLE001
                    status = "fail"
                    error = f"{type(exc).__name__}: {exc}"
                    details = {"traceback": traceback.format_exc()}
                    print(f"FAIL: {error}", flush=True)
                    print(details["traceback"], flush=True)

            duration_ms = int((time.perf_counter() - t0) * 1000)
            record = {
                "id": sid,
                "title": title,
                "module": mod_path,
                "tags": tags,
                "status": status,
                "duration_ms": duration_ms,
                "details": details,
                "error": error,
            }
            json_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
            jsonl.write(json.dumps(record, sort_keys=True) + "\n")
            summary["results"].append(
                {"id": sid, "status": status, "duration_ms": duration_ms, "error": error}
            )
            if status == "pass":
                passed += 1
            elif status == "skip":
                skipped += 1
            else:
                failed += 1

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    summary["passed"] = passed
    summary["failed"] = failed
    summary["skipped"] = skipped
    (evidence_root / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(
        f"\n--- summary: passed={passed} failed={failed} skipped={skipped} "
        f"evidence={evidence_root} ---",
        flush=True,
    )
    return 1 if failed else 0
