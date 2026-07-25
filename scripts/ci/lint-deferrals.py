#!/usr/bin/env python3
import re
import sys
from pathlib import Path

EXPECTED_OWNER = "Erik"
EXPECTED_REASON = "external production SLA input pending"
EXPECTED_RECHECK = "2026-07-15"

MARKER_RE = re.compile(
    r"fireweed-deferral:\s*progress_bound_ms\s*;"
    r"\s*owner=(?P<owner>[^;]+)\s*;"
    r"\s*reason=\"(?P<reason>[^\"]+)\"\s*;"
    r"\s*recheck=(?P<recheck>\d{4}-\d{2}-\d{2})"
)


def lint(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    mentions_progress_bound = "progress_bound_ms" in text
    mentions_deferral = re.search(r"\bdefer(?:red|ral)?\b", text, re.IGNORECASE) is not None
    markers = list(MARKER_RE.finditer(text))

    errors: list[str] = []
    if mentions_progress_bound and mentions_deferral and not markers:
        errors.append(
            "progress_bound_ms deferral requires marker "
            "fireweed-deferral: progress_bound_ms; owner=Erik; "
            'reason="external production SLA input pending"; recheck=2026-07-15'
        )

    for marker in markers:
        owner = marker.group("owner").strip()
        reason = marker.group("reason").strip()
        recheck = marker.group("recheck").strip()
        if owner != EXPECTED_OWNER:
            errors.append(f"progress_bound_ms deferral owner must be {EXPECTED_OWNER}")
        if reason != EXPECTED_REASON:
            errors.append(f"progress_bound_ms deferral reason must be {EXPECTED_REASON!r}")
        if recheck != EXPECTED_RECHECK:
            errors.append(f"progress_bound_ms deferral recheck must be {EXPECTED_RECHECK}")

    return errors


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: lint-deferrals.py <file> [<file>...]", file=sys.stderr)
        return 2

    failed = False
    for raw_path in argv:
        path = Path(raw_path)
        if not path.is_file():
            print(f"{path}: not a file", file=sys.stderr)
            failed = True
            continue
        errors = lint(path)
        for error in errors:
            print(f"{path}: {error}", file=sys.stderr)
        failed = failed or bool(errors)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
