#!/usr/bin/env python3
"""Record a public-API capacity run, including exact child resource usage (Linux)."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

repo = Path(__file__).resolve().parents[2]
binary = repo / "target/release/fireweed-workload"
args = sys.argv[1:]
if not args or args == ["--help"]:
    print("Build with cargo build -p fireweed-workload --release, then:\n"
          "scripts/perf/workflow-capacity.py --profile primitives --items 10000 --batch 100 --deadline-seconds 600")
    raise SystemExit(0)

def git(*args):
    return subprocess.check_output(["git", "-C", str(repo), *args])

head = git("rev-parse", "HEAD").decode().strip()
status = git("status", "--porcelain").decode()
digest = hashlib.sha256(git("diff", "--binary", "HEAD"))
for path in sorted((repo / "crates/fireweed-workload").rglob("*")):
    if path.is_file():
        digest.update(str(path.relative_to(repo)).encode())
        digest.update(path.read_bytes())
with binary.open("rb") as f:
    binary_sha = hashlib.file_digest(f, "sha256").hexdigest()
# /tmp may be tmpfs. Default capacity data to the repository filesystem;
# correctness tests may still deliberately use ephemeral temporary storage.
capacity_base = repo / "target/workflow-capacity"
capacity_base.mkdir(parents=True, exist_ok=True)
data_directory = None
if "--root" in args:
    data_root = Path(args[args.index("--root") + 1]).resolve()
else:
    data_directory = tempfile.TemporaryDirectory(dir=capacity_base)
    data_root = Path(data_directory.name)
    args = [*args, "--root", str(data_root)]
command = [str(binary), *args]
mount = json.loads(subprocess.check_output([
    "findmnt", "--json", "--target", str(data_root if data_root.exists() else data_root.parent)
]))

def storage_usage(root):
    groups = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        parts = path.relative_to(root).parts
        family = "/".join(parts[:2])
        bucket = groups.setdefault(family, {"files": 0, "bytes": 0, "allocated_bytes": 0})
        stat = path.stat()
        bucket["files"] += 1
        bucket["bytes"] += stat.st_size
        bucket["allocated_bytes"] += stat.st_blocks * 512
    return groups

with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
    started = time.monotonic()
    child = subprocess.Popen(command, cwd=repo, stdout=stdout, stderr=stderr)
    _, status_code, usage = os.wait4(child.pid, 0)
    child.returncode = os.waitstatus_to_exitcode(status_code)
    wall = time.monotonic() - started
    stdout.seek(0)
    stderr.seek(0)
    raw = stdout.read().decode()
    errors = stderr.read().decode()
    report = {
        "command": command, "head": head, "dirty": bool(status),
        "filesystem": mount,
        "storage": storage_usage(data_root),
        "source_diff_and_workload_sha256": digest.hexdigest(),
        "binary_sha256": binary_sha, "exit_code": child.returncode,
        "process_wall_s": wall, "user_cpu_s": usage.ru_utime,
        "system_cpu_s": usage.ru_stime, "max_rss_kib": usage.ru_maxrss,
        "filesystem_input_blocks": usage.ru_inblock,
        "filesystem_output_blocks": usage.ru_oublock,
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
    }
    if child.returncode == 0:
        report["result"] = json.loads(raw)
    else:
        report["stdout"] = raw
    if errors:
        report["stderr"] = errors
    print(json.dumps(report, indent=2))
    if data_directory is not None:
        data_directory.cleanup()
    raise SystemExit(child.returncode)
