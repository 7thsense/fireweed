#!/usr/bin/env python3
"""Record a public-API capacity run, including exact child resource usage (Linux)."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from workflow_storage_monitor import WalMonitor

repo = Path(__file__).resolve().parents[2]
binary = repo / "target/release/fireweed-workload"
args = sys.argv[1:]
campaign_target = 12_500 if "--stretch" in args else 10_000
qualification_requested = "--qualify" in args or "--stretch" in args
args = [arg for arg in args if arg not in ("--qualify", "--stretch")]
diagnostic_provenance = None
if "--diagnostic-provenance" in args:
    index = args.index("--diagnostic-provenance")
    diagnostic_provenance = json.loads(Path(args[index + 1]).read_text())
    del args[index:index + 2]
if not args or args == ["--help"]:
    print("Build with cargo build -p fireweed-workload --release, then:\n"
          "scripts/perf/workflow-capacity.py --profile primitives --items 10000 --batch 100 --deadline-seconds 600\n"
          "Add --qualify for the million-row 10k primitive/campaign or historical 9.5k workflow gates.\n"
          "Use --profile campaign --recycle --cycles 3 --items 1000000; --stretch requires 12.5k campaign recipients/sec.")
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
        # Post-run publication counts expose log grouping without tracing the
        # timed workload. Ignore temporary files and mutable metadata keys.
        if len(parts[-1]) == 20 and parts[-1].isdigit():
            if len(parts) >= 5 and parts[-2] == "fwlog":
                bucket["log_data_objects"] = bucket.get("log_data_objects", 0) + 1
            elif len(parts) >= 6 and parts[-3:-1] == ("fwmeta", "manifest"):
                bucket["log_manifests"] = bucket.get("log_manifests", 0) + 1
    return groups


def file_attributes(root):
    """Capture projection COW/compression flags as well as mount provenance.

    A projection directory may inherit Btrfs attributes different from its
    mount defaults. Record the actual DB/WAL flags, without traversing log
    objects or requiring lsattr on non-Linux/unsupported filesystems.
    """
    executable = shutil.which("lsattr")
    if executable is None:
        return {"available": False}
    paths = [root] if root.exists() else []
    for pattern in ("projection.db", "projection.db-wal"):
        paths.extend(sorted(root.glob("shard-*/" + pattern)))
    if not paths:
        return {"available": True, "paths": []}
    result = subprocess.run(
        [executable, "-d", "--", *map(str, paths)],
        capture_output=True, text=True, check=False,
    )
    return {"available": True, "exit_code": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}

rustfs_data = repo / "target" / "rustfs-data"
rustfs_data.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("FIREWEED_RUSTFS_DATA", str(rustfs_data))

with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
    started = time.monotonic()
    projection_root = (Path(args[args.index("--projection-root") + 1]).resolve()
                       if "--projection-root" in args else data_root)
    child = subprocess.Popen(command, cwd=repo, stdout=stdout, stderr=stderr)
    monitor = WalMonitor(projection_root, pid=child.pid)
    monitor.start()
    try:
        _, status_code, usage = os.wait4(child.pid, 0)
    finally:
        wal_observation = monitor.finish()
    child.returncode = os.waitstatus_to_exitcode(status_code)
    wall = time.monotonic() - started
    stdout.seek(0)
    stderr.seek(0)
    raw = stdout.read().decode()
    errors = stderr.read().decode()
    report = {
        "command": command, "head": head, "dirty": bool(status),
        "diagnostics": {key: os.environ[key] for key in (
            "FIREWEED_SQL_TRACE", "FIREWEED_APPLY_TRACE", "FIREWEED_WORKLOAD_TIMING",
            "FIREWEED_METRICS_TRACE",
            "FIREWEED_LOG_TRACE", "FIREWEED_WORKLOAD_DEBUG", "LD_PRELOAD",
            "FIREWEED_PROJECTION_IO_TRACE", "OBJECT_LOG_LOCAL_PUBLISH_TRACE",
        ) if key in os.environ},
        "runtime_configuration": {
            "OBJECT_LOG_FLUSH_RUNTIME_THREADS": os.environ.get("OBJECT_LOG_FLUSH_RUNTIME_THREADS"),
            "FIREWEED_RUSTFS_DATA": os.environ.get("FIREWEED_RUSTFS_DATA"),
        },
        "filesystem": mount,
        "storage": storage_usage(data_root),
        "projection_wal_observation": wal_observation,
        "file_attributes": file_attributes(data_root),
        "source_diff_and_workload_sha256": digest.hexdigest(),
        "binary_sha256": binary_sha, "exit_code": child.returncode,
        "process_wall_s": wall, "user_cpu_s": usage.ru_utime,
        "system_cpu_s": usage.ru_stime, "max_rss_kib": usage.ru_maxrss,
        "filesystem_input_blocks": usage.ru_inblock,
        "filesystem_output_blocks": usage.ru_oublock,
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
    }
    if "--projection-root" in args:
        projection_root = Path(args[args.index("--projection-root") + 1]).resolve()
        report["projection_storage"] = storage_usage(projection_root)
        report["projection_file_attributes"] = file_attributes(projection_root)
        report["projection_filesystem"] = json.loads(subprocess.check_output([
            "findmnt", "--json", "--target", str(next(path for path in [projection_root, *projection_root.parents] if path.exists()))
        ]))
    if child.returncode == 0:
        report["result"] = json.loads(raw)
    else:
        report["stdout"] = raw
    if errors:
        report["stderr"] = errors
    if diagnostic_provenance is not None:
        report["diagnostic_provenance"] = diagnostic_provenance
    if qualification_requested:
        from workflow_capacity_gate import qualify
        report["qualification"] = qualify(report, campaign_target=campaign_target)
    if child.returncode != 0:
        if data_directory is not None:
            # Move the failed run out of TemporaryDirectory's cleanup path.
            # Keep the authoritative log for recovery / timeout investigation;
            # successful runs retain the existing automatic cleanup behavior.
            retained_root = Path(tempfile.mkdtemp(prefix="failed-run-", dir=capacity_base))
            data_root.rename(retained_root)
            report["original_data_root"] = str(data_root)
            report["retained_data_root"] = str(retained_root)
        else:
            report["retained_data_root"] = str(data_root)
    print(json.dumps(report, indent=2))
    if data_directory is not None:
        data_directory.cleanup()
    raise SystemExit(child.returncode or (1 if qualification_requested and not report["qualification"]["passed"] else 0))
