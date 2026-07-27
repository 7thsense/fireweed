#!/usr/bin/env python3
"""Stage the microsite and link targets for GitHub Pages.

Published layout (https://<org>.github.io/<repo>/):

  <out>/
    index.html          # redirect → site/
    .nojekyll
    site/               # from docs/site
    helix/              # from docs/helix
    deployment/         # from docs/deployment
    operator/           # from docs/operator
    SECURITY.md …       # repo-root policy files

Href values authored for the repo docs/ tree are rewritten so they resolve
under the Pages project base path instead of escaping to the github.io root.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = ROOT / "target" / "site-pages"

ROOT_POLICY_FILES = [
    "SECURITY.md",
    "CONTRIBUTING.md",
    "SUPPORT.md",
    "LICENSE-MIT",
    "LICENSE-APACHE",
    "CODE_OF_CONDUCT.md",
    "NOTICE",
]

HREF_RE = re.compile(r'href="([^"]+)"')


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def map_to_staged(target: Path, out: Path) -> Path | None:
    """Map a repo-absolute path to its staged location, if published."""
    target = target.resolve()
    root = ROOT.resolve()
    out = out.resolve()

    try:
        rel = target.relative_to(root)
    except ValueError:
        return None

    parts = rel.parts
    if parts[:2] == ("docs", "site"):
        return out.joinpath("site", *parts[2:])
    if parts[:2] == ("docs", "helix"):
        return out.joinpath("helix", *parts[2:])
    if parts[:2] == ("docs", "deployment"):
        return out.joinpath("deployment", *parts[2:])
    if parts[:2] == ("docs", "operator"):
        return out.joinpath("operator", *parts[2:])
    if len(parts) == 1 and parts[0] in ROOT_POLICY_FILES:
        return out / parts[0]
    return None


def rewrite_href(original_page: Path, href: str, staged_page: Path, out: Path) -> str:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:", "javascript:")):
        return href
    path = unquote(parsed.path)
    if not path:
        return href

    resolved = (original_page.parent / path).resolve()
    staged_target = map_to_staged(resolved, out)
    if staged_target is None:
        return href
    if not staged_target.exists() and not staged_target.is_symlink():
        # Still rewrite if parent mapping is correct (e.g. optional NOTICE).
        pass

    new_path = os.path.relpath(staged_target, staged_page.parent).replace("\\", "/")
    return urlunparse(("", "", new_path, "", parsed.query, parsed.fragment))


def rewrite_html_file(original_page: Path, staged_page: Path, out: Path) -> bool:
    text = staged_page.read_text(encoding="utf-8")
    changed = False

    def repl(match: re.Match[str]) -> str:
        nonlocal changed
        href = match.group(1)
        new = rewrite_href(original_page, href, staged_page, out)
        if new != href:
            changed = True
            return f'href="{new}"'
        return match.group(0)

    new_text = HREF_RE.sub(repl, text)
    if changed:
        staged_page.write_text(new_text, encoding="utf-8")
    return changed


def validate_staged_links(out: Path) -> list[str]:
    """Filesystem link check for staged site HTML (same-origin relative only)."""
    errors: list[str] = []
    for page in (out / "site").rglob("*.html"):
        text = page.read_text(encoding="utf-8")
        for href in HREF_RE.findall(text):
            parsed = urlparse(href)
            if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
                continue
            path = unquote(parsed.path)
            if not path:
                continue
            target = (page.parent / path).resolve()
            try:
                target.relative_to(out.resolve())
            except ValueError:
                errors.append(f"{page.relative_to(out)} escapes stage: {href}")
                continue
            if not target.exists():
                errors.append(f"{page.relative_to(out)} broken: {href}")
    return errors


def stage(out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    copy_tree(ROOT / "docs" / "site", out / "site")
    copy_tree(ROOT / "docs" / "helix", out / "helix")
    copy_tree(ROOT / "docs" / "deployment", out / "deployment")
    copy_tree(ROOT / "docs" / "operator", out / "operator")

    for name in ROOT_POLICY_FILES:
        src = ROOT / name
        if src.is_file():
            shutil.copy2(src, out / name)

    rewritten = 0
    site_src = ROOT / "docs" / "site"
    for staged_page in (out / "site").rglob("*.html"):
        rel = staged_page.relative_to(out / "site")
        original_page = site_src / rel
        if rewrite_html_file(original_page, staged_page, out):
            rewritten += 1

    (out / ".nojekyll").write_text("", encoding="utf-8")
    (out / "index.html").write_text(
        """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=site/" />
    <title>Fireweed Queue</title>
    <link rel="canonical" href="site/" />
  </head>
  <body>
    <p>Redirecting to the <a href="site/">Fireweed Queue microsite</a>.</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    errors = validate_staged_links(out)
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        raise SystemExit(f"stage validation failed: {len(errors)} error(s)")

    print(f"staged pages at {out}")
    print(f"rewrote links in {rewritten} HTML page(s)")


def main(argv: list[str]) -> int:
    out = Path(argv[1]).resolve() if len(argv) > 1 else DEFAULT_OUT
    stage(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
