#!/usr/bin/env python3
"""Fail if committed example excerpts drift from source symbols."""

from __future__ import annotations

import sys
from pathlib import Path

# Reuse extractor
sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_examples import (  # noqa: E402
    MANIFEST,
    OUT_DIR,
    ROOT,
    load_manifest,
    render_excerpt,
)


def main() -> int:
    examples = load_manifest(MANIFEST)
    failed = 0
    for example in examples:
        slug = example["slug"]
        expected = render_excerpt(example)
        path = OUT_DIR / f"{slug}.rs"
        if not path.is_file():
            print(f"missing excerpt: {path.relative_to(ROOT)}", file=sys.stderr)
            failed += 1
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != expected:
            print(
                f"stale excerpt: {path.relative_to(ROOT)} "
                f"(regenerate with python3 scripts/site/extract_examples.py)",
                file=sys.stderr,
            )
            failed += 1
    if failed:
        print(f"{failed} provenance check(s) failed", file=sys.stderr)
        return 1
    print(f"provenance ok for {len(examples)} example(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
