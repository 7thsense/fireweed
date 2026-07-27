#!/usr/bin/env python3
"""Run a versioned public-release check manifest and record revision/tool evidence."""
import argparse, json, pathlib, subprocess, sys

parser = argparse.ArgumentParser()
parser.add_argument("--manifest", default="scripts/ci/public-release-gates.json")
parser.add_argument("--evidence", default="target/public-release-gate.json")
parser.add_argument("--repo", default=".")
args = parser.parse_args()
repo = pathlib.Path(args.repo).resolve()
manifest_path = (repo / args.manifest).resolve()
evidence_path = (repo / args.evidence).resolve()
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("schema_version") != 1 or not manifest.get("gates"):
    raise SystemExit("invalid public-release gate manifest")
ids = [gate.get("id") for gate in manifest["gates"]]
if None in ids or len(ids) != len(set(ids)):
    raise SystemExit("gate IDs must be present and unique")

def output(command):
    try:
        value = subprocess.check_output(command, cwd=repo, text=True, stderr=subprocess.STDOUT)
        return value.splitlines()[0] if value else ""
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        return f"unavailable: {error}"

revision = output(["git", "rev-parse", "HEAD"])
if len(revision) != 40:
    raise SystemExit("checked-out revision is not a full commit")
tools = {
    "bash": output(["bash", "--version"]),
    "cargo": output(["cargo", "--version"]),
    "cargo-deny": output(["cargo", "deny", "--version"]),
    "git": output(["git", "--version"]),
    "gitleaks": output(["gitleaks", "version"]),
    "python": sys.version.splitlines()[0],
    "rustc": output(["rustc", "--version"]),
}
results = []
passed = True
for gate in manifest["gates"]:
    command = gate.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(v, str) for v in command):
        raise SystemExit(f"gate {gate['id']} has an invalid command array")
    print(f"--- public-release gate: {gate['id']} ---", flush=True)
    status = subprocess.run(command, cwd=repo, check=False).returncode
    results.append({"id": gate["id"], "command": command, "exit_status": status})
    if status != 0:
        passed = False
        break
evidence = {
    "schema": "fireweed.public_release_gate.v1",
    "manifest": {"path": str(manifest_path.relative_to(repo)), "version": manifest.get("version")},
    "revision": revision,
    "tool_versions": tools,
    "results": results,
    "passed": passed,
}
evidence_path.parent.mkdir(parents=True, exist_ok=True)
evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"public-release evidence: {evidence_path}")
raise SystemExit(0 if passed else 1)
