#!/usr/bin/env python3
"""
Repeat-suite report formatter and flaky-rate gate (TP-003 §5).

Modes
-----
  --parse-suites FILE
      Parse a [[suites]] TOML file; emit one JSON object per line.
      Each object: {"name": "...", "command": ["arg0", ...]}.
      Used by repeat-suite.sh to enumerate suites without a shell TOML parser.

  --report FILE [--max-flaky-rate R]
      Read the TSV results file written by repeat-suite.sh (lines:
        suite_name<TAB>pass|fail
      ), compute per-suite and overall stats, print the report, and exit
      nonzero when overall flaky_rate > R (default R=1.0).

Report fields (AC-4 requirement):
  run_count, failures, flaky_rate, failing_selectors.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict


def parse_suites(toml_path: str) -> list[dict]:
    with open(toml_path, "rb") as fh:
        import tomllib  # Python 3.11+
        data = tomllib.load(fh)
    suites = data.get("suites", [])
    for entry in suites:
        if "name" not in entry:
            print(f"ERROR: suite entry missing 'name': {entry}", file=sys.stderr)
            sys.exit(1)
        if "command" not in entry or not isinstance(entry["command"], list):
            print(
                f"ERROR: suite '{entry.get('name', '?')}' missing list 'command'",
                file=sys.stderr,
            )
            sys.exit(1)
    return suites


def read_results(results_file: str) -> dict[str, list[str]]:
    """Return {suite_name: ['pass'|'fail', ...]}."""
    results: dict[str, list[str]] = defaultdict(list)
    with open(results_file) as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line:
                continue
            name, outcome = line.split("\t", 1)
            results[name].append(outcome.strip())
    return dict(results)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parse-suites", metavar="FILE",
                        help="parse TOML suite list; emit JSON lines")
    parser.add_argument("--report", metavar="FILE",
                        help="read TSV results; format report")
    parser.add_argument("--max-flaky-rate", type=float, default=1.0,
                        metavar="R",
                        help="max allowed flaky rate [0.0, 1.0] (default 1.0)")
    args = parser.parse_args()

    if args.parse_suites:
        suites = parse_suites(args.parse_suites)
        for suite in suites:
            print(json.dumps(suite))
        return

    if args.report:
        results = read_results(args.report)

        if not results:
            print("ERROR: results file is empty; no runs recorded", file=sys.stderr)
            sys.exit(1)

        total_runs = 0
        total_failures = 0
        failing_selectors: list[str] = []

        print("=== repeat-suite report ===")
        print()

        for suite_name, outcomes in results.items():
            run_count = len(outcomes)
            failures = outcomes.count("fail")
            total_runs += run_count
            total_failures += failures
            rate = failures / run_count if run_count > 0 else 0.0
            status = "FAIL" if failures > 0 else "PASS"

            print(f"  suite           : {suite_name}")
            print(f"  run_count       : {run_count}")
            print(f"  failures        : {failures}")
            print(f"  flaky_rate      : {rate:.6f}")
            print(f"  status          : {status}")
            print()

            if failures > 0:
                failing_selectors.append(suite_name)

        overall_rate = total_failures / total_runs if total_runs > 0 else 0.0
        max_rate = args.max_flaky_rate

        print("--- overall ---")
        print(f"  run_count       : {total_runs}")
        print(f"  total_failures  : {total_failures}")
        print(f"  flaky_rate      : {overall_rate:.6f}")
        print(f"  max_flaky_rate  : {max_rate:.6f}")
        if failing_selectors:
            print("  failing_selectors:")
            for name in failing_selectors:
                print(f"    - {name}")
        else:
            print("  failing_selectors: (none)")
        print()

        passed = overall_rate <= max_rate
        if passed:
            print(
                f"=== PASSED (flaky_rate={overall_rate:.6f} <= max={max_rate:.6f}) ==="
            )
        else:
            print(
                f"=== FAILED (flaky_rate={overall_rate:.6f} > max={max_rate:.6f}) ==="
            )
            sys.exit(1)
        return

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
