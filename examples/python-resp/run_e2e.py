#!/usr/bin/env python3
"""Run Fireweed Python RESP scenarios (functional and/or performance).

Examples:
  python run_e2e.py
  python run_e2e.py --suite perf
  PERF_N=10000 python run_e2e.py --suite perf
  python run_e2e.py --scenario 05_claim_due_batch
  python run_e2e.py --full
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow `python run_e2e.py` from this directory.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from harness.runner import run_suite  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--url",
        default=os.environ.get("FIREWEED_RESP_URL", "redis://127.0.0.1:8080"),
        help="RESP URL (default FIREWEED_RESP_URL or redis://127.0.0.1:8080)",
    )
    p.add_argument(
        "--queue",
        default=os.environ.get("FIREWEED_RESP_QUEUE", "demo:work"),
        help="Stream key tenant:queue (default demo:work)",
    )
    p.add_argument(
        "--suite",
        choices=("functional", "perf", "all"),
        default="functional",
        help="functional (default), perf, or all",
    )
    p.add_argument(
        "--scenario",
        default=None,
        help="Substring filter on module path / scenario id",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="Include slow scenarios (e.g. lease reclaim wait)",
    )
    p.add_argument(
        "--evidence-dir",
        default=None,
        help="Evidence output directory (default target/python-resp-*)",
    )
    p.add_argument(
        "--perf-n",
        type=int,
        default=int(os.environ.get("PERF_N", "1000000")),
        help="Perf insert volume (default PERF_N or 1000000)",
    )
    p.add_argument(
        "--perf-pipeline",
        type=int,
        default=int(os.environ.get("PERF_PIPELINE", "1000")),
        help="XADD pipeline batch size",
    )
    p.add_argument(
        "--perf-claim-count",
        type=int,
        default=int(os.environ.get("PERF_CLAIM_COUNT", "1000")),
        help="Requested XREADGROUP COUNT (server may cap lower)",
    )
    args = p.parse_args(argv)

    evidence = Path(args.evidence_dir) if args.evidence_dir else None
    return run_suite(
        url=args.url,
        queue=args.queue,
        suite=args.suite,
        scenario_filter=args.scenario,
        full=args.full,
        evidence_root=evidence,
        perf_n=args.perf_n,
        perf_pipeline=args.perf_pipeline,
        perf_claim_count=args.perf_claim_count,
    )


if __name__ == "__main__":
    raise SystemExit(main())
