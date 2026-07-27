#!/usr/bin/env python3
"""Validate local href targets for the product microsite and operator shim."""

from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "docs/site"
OPERATOR = ROOT / "docs/operator/index.html"


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)


def check_html(path: Path) -> list[str]:
    errors: list[str] = []
    parser = LinkParser()
    parser.feed(path.read_text(encoding="utf-8"))
    for href in parser.hrefs:
        parsed = urlparse(href)
        if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
            continue
        local = unquote(parsed.path)
        if not local:
            continue
        target = (path.parent / local).resolve()
        try:
            target.relative_to(ROOT.resolve())
        except ValueError:
            errors.append(f"{path.relative_to(ROOT)} escapes repo: {href}")
            continue
        if not target.is_file():
            errors.append(f"{path.relative_to(ROOT)} broken local link: {href}")
    return errors


def main() -> int:
    pages = sorted(SITE.rglob("*.html"))
    if OPERATOR.is_file():
        pages.append(OPERATOR)
    if not pages:
        print("no HTML pages found", file=sys.stderr)
        return 1
    errors: list[str] = []
    for page in pages:
        errors.extend(check_html(page))
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        print(f"{len(errors)} link error(s)", file=sys.stderr)
        return 1
    print(f"validated {len(pages)} HTML page(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
