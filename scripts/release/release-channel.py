#!/usr/bin/env python3
"""Resolve an explicit source-preview pin, otherwise retain governed publication.

A preview is a source distribution, never a governed deployment attestation.
The tag points at evidence E and the archive comes from its measured ancestor S.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import tomllib


def resolve(root: Path, tag: str) -> dict[str, str]:
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        raise ValueError("tag must be vMAJOR.MINOR.PATCH")

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

    evidence = git("rev-parse", f"refs/tags/{tag}^{{commit}}")
    if git("rev-parse", "HEAD") != evidence:
        raise ValueError("checkout must equal the resolved tag")
    result = {"channel": "governed", "tag": tag, "version": tag[1:], "evidence_commit": evidence}
    relative = f"docs/releases/{tag}.source-preview.json"
    if not (root / relative).exists():
        return result
    # Read committed bytes, not an untracked/modified manifest.
    manifest = json.loads(git("show", f"{evidence}:{relative}"))
    if manifest.get("schema") != "fireweed.source-preview-release.v1" or manifest.get("version") != tag[1:]:
        raise ValueError("invalid source-preview release manifest")
    source = manifest.get("measured_source", "")
    if not re.fullmatch(r"[0-9a-f]{40}", source) or source == evidence:
        raise ValueError("preview requires distinct full source and evidence commits")
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", source, evidence], check=True)
    source_version = tomllib.loads(git("show", f"{source}:Cargo.toml"))["workspace"]["package"]["version"]
    if source_version != tag[1:]:
        raise ValueError("measured source package version differs from tag")
    if manifest.get("claims") != {"governed_product_ready": False, "signed": False}:
        raise ValueError("source preview cannot claim governed readiness or signatures")
    git("show", f"{evidence}:docs/releases/{tag}.md")
    result.update(channel="source-preview", measured_source=source)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    for key, value in resolve(args.repo, args.tag).items():
        print(f"{key}={value}")
