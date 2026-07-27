#!/usr/bin/env python3
"""Extract curated Rust symbols into docs/site/examples/src/ for the microsite."""

from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # stdlib fallback: minimal YAML subset for this manifest
    yaml = None


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "docs/site/_meta/example-manifest.yaml"
OUT_DIR = ROOT / "docs/site/examples/src"

FN_RE = re.compile(
    r"(?m)^(?:pub\s+)?(?:async\s+)?fn\s+(?P<name>[A-Za-z0-9_]+)\s*"
    r"(?P<generics><[^;{]*>)?\s*"
    r"(?P<sig>\([^;{]*\))\s*"
    r"(?:->\s*[^;{]+)?\s*"
    r"(?:where\s+[^;{]+)?\s*\{"
)


def load_manifest(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text)
        return list(data["examples"])

    # Minimal parser: only handles this repo's flat manifest shape.
    examples: list[dict] = []
    current: dict | None = None
    sources: list[dict] | None = None
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        if stripped == "examples:":
            continue
        if stripped.startswith("- slug:"):
            if current:
                examples.append(current)
            current = {
                "slug": stripped.split(":", 1)[1].strip(),
                "sources": [],
            }
            sources = current["sources"]
            continue
        if current is None:
            continue
        if stripped.startswith("title:"):
            current["title"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("summary:"):
            current["summary"] = stripped.split(":", 1)[1].strip().strip(">")
        elif stripped.startswith("tags:"):
            tags = stripped.split(":", 1)[1].strip()
            current["tags"] = [t.strip() for t in tags.strip("[]").split(",") if t.strip()]
        elif stripped.startswith("category:"):
            current["category"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- path:"):
            sources.append({"path": stripped.split(":", 1)[1].strip()})
        elif stripped.startswith("symbol:") and sources:
            sources[-1]["symbol"] = stripped.split(":", 1)[1].strip()
    if current:
        examples.append(current)
    return examples


def extract_function(source: str, symbol: str) -> str:
    for match in FN_RE.finditer(source):
        if match.group("name") != symbol:
            continue
        start = match.start()
        # Walk braces from the opening '{' found by the regex end.
        brace_at = source.find("{", match.end() - 1)
        if brace_at < 0:
            raise ValueError(f"no body for {symbol}")
        depth = 0
        i = brace_at
        in_str = False
        in_char = False
        in_line_comment = False
        in_block_comment = False
        while i < len(source):
            ch = source[i]
            nxt = source[i + 1] if i + 1 < len(source) else ""
            if in_line_comment:
                if ch == "\n":
                    in_line_comment = False
                i += 1
                continue
            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if in_str:
                if ch == "\\" and nxt:
                    i += 2
                    continue
                if ch == '"':
                    in_str = False
                i += 1
                continue
            if in_char:
                if ch == "\\" and nxt:
                    i += 2
                    continue
                if ch == "'":
                    in_char = False
                i += 1
                continue
            if ch == "/" and nxt == "/":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue
            if ch == '"':
                in_str = True
                i += 1
                continue
            if ch == "'":
                in_char = True
                i += 1
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return source[start : i + 1].rstrip() + "\n"
            i += 1
        raise ValueError(f"unbalanced braces for {symbol}")
    raise ValueError(f"symbol not found: {symbol}")


def render_excerpt(example: dict) -> str:
    chunks: list[str] = []
    for src in example["sources"]:
        path = ROOT / src["path"]
        symbol = src["symbol"]
        body = extract_function(path.read_text(encoding="utf-8"), symbol)
        header = (
            f"// Provenance: {src['path']}::{symbol}\n"
            f"// Do not edit by hand — regenerate with scripts/site/extract_examples.py\n"
        )
        chunks.append(header + body)
    return "\n".join(chunks)


def main() -> int:
    if not MANIFEST.is_file():
        print(f"missing manifest: {MANIFEST}", file=sys.stderr)
        return 1
    examples = load_manifest(MANIFEST)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    for example in examples:
        slug = example["slug"]
        try:
            text = render_excerpt(example)
        except Exception as exc:  # noqa: BLE001 - surface extract failures clearly
            print(f"extract failed for {slug}: {exc}", file=sys.stderr)
            return 1
        out = OUT_DIR / f"{slug}.rs"
        out.write_text(text, encoding="utf-8")
        written += 1
        print(f"wrote {out.relative_to(ROOT)}")
    print(f"extracted {written} example(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
