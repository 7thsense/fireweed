#!/usr/bin/env python3
"""Stage the microsite for GitHub Pages.

Published layout (https://<org>.github.io/<repo>/):

  <out>/
    index.html          # redirect → site/
    .nojekyll
    site/               # from docs/site

The site links to repository files outside docs/site by their GitHub URLs,
so the staged tree is docs/site alone. Every relative href in the staged
HTML must resolve inside it.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = ROOT / "target" / "site-pages"

HREF_RE = re.compile(r'(?:href|src)="([^"]+)"')

REDIRECT = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=site/" />
    <title>Fireweed</title>
    <link rel="canonical" href="site/" />
  </head>
  <body>
    <p>Redirecting to the <a href="site/">Fireweed microsite</a>.</p>
  </body>
</html>
"""


def validate_staged_links(out: Path) -> list[str]:
    """Filesystem link check for staged site HTML (relative links only)."""
    errors: list[str] = []
    site = (out / "site").resolve()
    for page in (out / "site").rglob("*.html"):
        for href in HREF_RE.findall(page.read_text(encoding="utf-8")):
            parsed = urlparse(href)
            if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
                continue
            path = unquote(parsed.path)
            if not path:
                continue
            target = (page.parent / path).resolve()
            try:
                target.relative_to(site)
            except ValueError:
                errors.append(f"{page.relative_to(out)} links outside the site: {href}")
                continue
            if not (target.is_file() or (target.is_dir() and (target / "index.html").is_file())):
                errors.append(f"{page.relative_to(out)} broken: {href}")
    return errors


def stage(out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    shutil.copytree(ROOT / "docs" / "site", out / "site")
    (out / ".nojekyll").write_text("", encoding="utf-8")
    (out / "index.html").write_text(REDIRECT, encoding="utf-8")

    errors = validate_staged_links(out)
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        raise SystemExit(f"stage validation failed: {len(errors)} error(s)")
    pages = sum(1 for _ in (out / "site").rglob("*.html"))
    print(f"staged {pages} page(s) at {out}")


def main(argv: list[str]) -> int:
    out = Path(argv[1]).resolve() if len(argv) > 1 else DEFAULT_OUT
    stage(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
